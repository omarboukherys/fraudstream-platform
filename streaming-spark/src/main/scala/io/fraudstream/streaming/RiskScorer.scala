package io.fraudstream.streaming

/**
 * Moteur de scoring : fonctions pures (sans Spark) pour être testées unitairement.
 *
 * Le score est additif et borné à 1.0. Chaque règle ajoute une "raison" lisible : ce sont ces raisons
 * que l'agent IA analyste utilise comme point de départ de son enquête.
 * Les seuils sont volontairement explicites (pas de modèle ML) pour que la démo soit explicable ;
 * un modèle (ex. XGBoost entraîné sur le lac) pourrait remplacer `score` sans toucher au reste du job.
 */
object RiskScorer {

  val WindowMs: Long      = 10 * 60 * 1000L   // fenêtre glissante
  val AlertThreshold      = 0.6
  val HighRiskMcc: Set[String] = Set("CRYPTO_EXCHANGE", "GAMBLING")

  final case class Features(
      txCount10m: Int,
      amountSum10m: Double,
      amountAvg10m: Double,
      distinctCountries10m: Int,
      distinctMerchants10m: Int,
      isNewDevice: Boolean,
      secondsSinceLastTx: Long,
      lastCountry: Option[String],
      lifetimeAvgAmount: Double,
      lifetimeCount: Long,
      homeCountry: Option[String]
  )

  /** Calcule les features de la transaction `tx` à partir de l'état *avant* cette transaction. */
  def features(tx: Tx, state: CustomerState): Features = {
    val nowMs  = tx.eventTime.getTime
    val recent = state.recent.filter(r => nowMs - r.eventTimeMs <= WindowMs && r.eventTimeMs <= nowMs)
    val last   = state.recent.sortBy(-_.eventTimeMs).headOption
    Features(
      txCount10m           = recent.size,
      amountSum10m         = recent.map(_.amount).sum,
      amountAvg10m         = if (recent.isEmpty) 0.0 else recent.map(_.amount).sum / recent.size,
      distinctCountries10m = recent.map(_.country).distinct.size,
      distinctMerchants10m = recent.map(_.merchantId).distinct.size,
      isNewDevice          = isNewDevice(tx.deviceId, nowMs, state),
      secondsSinceLastTx   = last.map(l => (nowMs - l.eventTimeMs) / 1000).getOrElse(-1L),
      lastCountry          = last.map(_.country),
      lifetimeAvgAmount    = state.lifetimeAvgAmount,
      lifetimeCount        = state.lifetimeCount,
      homeCountry          = Option(state.homeCountry)
    )
  }

  val NewDevicePeriodMs: Long = 24 * 3600 * 1000L

  /**
   * Un device est "nouveau" s'il n'a jamais été vu, ou s'il est apparu il y a moins de 24 h.
   * Le tout premier device d'un client est enregistré avec firstSeen = 0 : il est de confiance d'emblée.
   */
  def isNewDevice(deviceId: String, nowMs: Long, state: CustomerState): Boolean =
    state.knownDevices.nonEmpty && state.knownDevices.get(deviceId).forall(first => nowMs - first < NewDevicePeriodMs)

  /** Applique les règles. Renvoie (score borné, raisons). */
  def score(tx: Tx, f: Features): (Double, Seq[String]) = {
    val amount = tx.amount.doubleValue()
    val rules = Seq.newBuilder[(Double, String)]

    // 1. Card testing : rafale de petits montants en ligne
    if (f.txCount10m >= 5 && f.amountAvg10m < 15 && amount < 15 && tx.channel == "ONLINE")
      rules += 0.55 -> s"VELOCITY_SMALL_AMOUNTS(${f.txCount10m + 1} tx / 10 min, avg ${"%.1f".format(f.amountAvg10m)})"
    else if (f.txCount10m >= 8)
      rules += 0.30 -> s"HIGH_VELOCITY(${f.txCount10m + 1} tx / 10 min)"

    // 2. Voyage impossible : changement de pays en < 30 min sur canal physique
    f.lastCountry.foreach { lc =>
      if (lc != tx.country && f.secondsSinceLastTx >= 0 && f.secondsSinceLastTx < 1800
          && (tx.channel == "CARD_PRESENT" || tx.channel == "ATM"))
        rules += 0.65 -> s"IMPOSSIBLE_TRAVEL($lc -> ${tx.country} in ${f.secondsSinceLastTx}s)"
    }

    // 3. Montant anormal vs habitude du client (il faut un minimum d'historique)
    if (f.lifetimeCount >= 5 && amount > 8 * f.lifetimeAvgAmount)
      rules += 0.45 -> s"AMOUNT_ANOMALY(${"%.0f".format(amount)} vs avg ${"%.0f".format(f.lifetimeAvgAmount)})"

    // 4. Nouveau device
    if (f.isNewDevice) {
      val w = if (tx.channel == "TRANSFER" || HighRiskMcc.contains(tx.merchantCategory)) 0.50 else 0.25
      rules += w -> s"NEW_DEVICE(${tx.deviceId})"
    }

    // 5. Catégorie marchand à risque
    if (HighRiskMcc.contains(tx.merchantCategory))
      rules += 0.20 -> s"HIGH_RISK_MCC(${tx.merchantCategory})"

    // 6. Transaction à l'étranger
    f.homeCountry.foreach { hc => if (hc != tx.country) rules += 0.10 -> s"FOREIGN_COUNTRY(${tx.country})" }

    val built = rules.result()
    (math.min(1.0, built.map(_._1).sum), built.map(_._2))
  }

  /** Nouvel état après prise en compte de `tx` (fenêtre purgée, moyenne incrémentale, devices connus). */
  def updateState(tx: Tx, state: CustomerState): CustomerState = {
    val nowMs   = tx.eventTime.getTime
    val amount  = tx.amount.doubleValue()
    val recent  = (state.recent :+ RecentTx(nowMs, amount, tx.country, tx.deviceId, tx.merchantId))
      .filter(r => nowMs - r.eventTimeMs <= WindowMs)
      .takeRight(200) // garde-fou mémoire
    val n       = state.lifetimeCount + 1
    val avg     = state.lifetimeAvgAmount + (amount - state.lifetimeAvgAmount) / n
    val devices =
      if (state.knownDevices.contains(tx.deviceId)) state.knownDevices
      else {
        val firstSeen = if (state.knownDevices.isEmpty) 0L else nowMs
        val updated = state.knownDevices + (tx.deviceId -> firstSeen)
        // garde-fou mémoire : le device de confiance + les 9 plus récents
        if (updated.size <= 10) updated
        else updated.toSeq.sortBy { case (_, t) => if (t == 0L) Long.MaxValue else t }.takeRight(10).toMap
      }
    CustomerState(
      recent            = recent,
      knownDevices      = devices,
      homeCountry       = if (state.homeCountry == null) tx.country else state.homeCountry,
      lifetimeCount     = n,
      lifetimeAvgAmount = avg
    )
  }

  /** Pipeline complet pour une transaction : features -> score -> ligne enrichie + nouvel état. */
  def process(tx: Tx, state: CustomerState): (ScoredTx, CustomerState) = {
    val f = features(tx, state)
    val (s, reasons) = score(tx, f)
    val newState = updateState(tx, state)
    val win = newState.recent // fenêtre 10 min incluant la transaction courante
    val scored = ScoredTx(
      tx.transactionId, tx.customerId, tx.accountId, tx.eventTime, tx.amount, tx.currency,
      tx.merchantId, tx.merchantCategory, tx.channel, tx.country, tx.city, tx.deviceId,
      tx.isFraudLabel, tx.fraudScenario,
      txCount10m = win.size,
      amountSum10m = win.map(_.amount).sum,
      amountAvg10m = win.map(_.amount).sum / win.size,
      distinctCountries10m = win.map(_.country).distinct.size,
      distinctMerchants10m = win.map(_.merchantId).distinct.size,
      isNewDevice = f.isNewDevice,
      secondsSinceLastTx = f.secondsSinceLastTx,
      riskScore = BigDecimal(s).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble,
      riskReasons = reasons,
      isAlert = s >= AlertThreshold
    )
    (scored, newState)
  }
}
