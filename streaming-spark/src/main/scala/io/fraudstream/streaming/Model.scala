package io.fraudstream.streaming

import java.sql.Timestamp

/** Transaction parsée depuis Kafka (après validation du JSON et conversion de l'horodatage). */
final case class Tx(
    transactionId: String,
    customerId: String,
    accountId: String,
    eventTime: Timestamp,
    amount: java.math.BigDecimal,
    currency: String,
    merchantId: String,
    merchantCategory: String,
    channel: String,
    country: String,
    city: String,
    deviceId: String,
    ipAddress: String,
    isFraudLabel: Boolean,
    fraudScenario: String
)

/** Résumé compact d'une transaction récente, conservé dans l'état par client. */
final case class RecentTx(eventTimeMs: Long, amount: Double, country: String, deviceId: String, merchantId: String)

/**
 * État conservé par client dans `flatMapGroupsWithState`.
 * Volontairement petit : fenêtre glissante de 10 minutes + quelques agrégats "vie entière" (moyenne incrémentale).
 */
final case class CustomerState(
    recent: Seq[RecentTx],
    /** device -> horodatage (ms) de première apparition ; 0 pour le tout premier device, considéré de confiance. */
    knownDevices: Map[String, Long],
    homeCountry: String,
    lifetimeCount: Long,
    lifetimeAvgAmount: Double
)

object CustomerState {
  val empty: CustomerState = CustomerState(Seq.empty, Map.empty, homeCountry = null, lifetimeCount = 0L, lifetimeAvgAmount = 0.0)
}

/** Transaction enrichie : features fenêtrées + score + raisons. C'est la ligne écrite dans Cassandra et le lac. */
final case class ScoredTx(
    transactionId: String,
    customerId: String,
    accountId: String,
    eventTime: Timestamp,
    amount: java.math.BigDecimal,
    currency: String,
    merchantId: String,
    merchantCategory: String,
    channel: String,
    country: String,
    city: String,
    deviceId: String,
    isFraudLabel: Boolean,
    fraudScenario: String,
    // features
    txCount10m: Int,
    amountSum10m: Double,
    amountAvg10m: Double,
    distinctCountries10m: Int,
    distinctMerchants10m: Int,
    isNewDevice: Boolean,
    secondsSinceLastTx: Long,
    // scoring
    riskScore: Double,
    riskReasons: Seq[String],
    isAlert: Boolean
)

/** Message publié dans `fraud.alerts` et consommé par l'agent IA analyste. */
final case class Alert(
    alertId: String,
    transactionId: String,
    customerId: String,
    eventTime: Timestamp,
    amount: java.math.BigDecimal,
    currency: String,
    country: String,
    channel: String,
    merchantCategory: String,
    riskScore: Double,
    riskReasons: Seq[String],
    status: String
)
