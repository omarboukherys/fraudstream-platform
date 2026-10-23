package io.fraudstream.streaming

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class RiskScorerSpec extends AnyFunSuite with Matchers {

  private def tx(id: String, t: Long, amount: Double, country: String = "MA", device: String = "dev-1",
                 channel: String = "CARD_PRESENT", mcc: String = "GROCERY", merchant: String = "M1") =
    Tx(id, "C1", "ACC1", new Timestamp(t), new java.math.BigDecimal(amount), "MAD", merchant, mcc, channel,
       country, "Casablanca", device, "1.1.1.1", isFraudLabel = false, fraudScenario = "")

  /** Rejoue une séquence et renvoie la liste des transactions scorées. */
  private def replay(txs: Seq[Tx]): Seq[ScoredTx] = {
    var state = CustomerState.empty
    txs.map { t => val (s, ns) = RiskScorer.process(t, state); state = ns; s }
  }

  private val t0 = 1700000000000L

  test("une transaction ordinaire n'est pas une alerte") {
    val history = (1 to 10).map(i => tx(s"h$i", t0 + i * 3600000L, 100 + i))
    val scored = replay(history :+ tx("x", t0 + 11 * 3600000L, 120))
    scored.last.riskScore should be < 0.3
    scored.last.isAlert shouldBe false
  }

  test("card testing : rafale de petits montants en ligne sur nouveau device -> alerte") {
    val history = (1 to 5).map(i => tx(s"h$i", t0 + i * 3600000L, 200))
    val burst = (1 to 7).map(i => tx(s"b$i", t0 + 6 * 3600000L + i * 3000L, 3, device = "dev-new", channel = "ONLINE"))
    val scored = replay(history ++ burst)
    val last = scored.last
    last.isAlert shouldBe true
    last.riskReasons.exists(_.startsWith("VELOCITY_SMALL_AMOUNTS")) shouldBe true
    last.riskReasons.exists(_.startsWith("NEW_DEVICE")) shouldBe true
    last.txCount10m shouldBe 7
  }

  test("impossible travel : changement de pays en 4 minutes sur canal physique -> alerte") {
    val scored = replay(Seq(
      tx("a", t0, 150, country = "MA"),
      tx("b", t0 + 240000L, 400, country = "FR")))
    scored.last.isAlert shouldBe true
    scored.last.riskReasons.exists(_.startsWith("IMPOSSIBLE_TRAVEL(MA -> FR")) shouldBe true
    scored.last.riskReasons.exists(_.startsWith("FOREIGN_COUNTRY")) shouldBe true
  }

  test("montant anormal : 12x la moyenne historique, à l'étranger, en ligne") {
    val history = (1 to 8).map(i => tx(s"h$i", t0 + i * 3600000L, 100))
    val scored = replay(history :+ tx("big", t0 + 9 * 3600000L, 1200, country = "US", channel = "ONLINE"))
    scored.last.riskReasons.exists(_.startsWith("AMOUNT_ANOMALY")) shouldBe true
    scored.last.riskScore shouldBe 0.55 +- 0.001
  }

  test("account takeover : nouveau device + virement vers crypto -> alerte") {
    val history = (1 to 3).map(i => tx(s"h$i", t0 + i * 3600000L, 100))
    val scored = replay(history :+ tx("ato", t0 + 4 * 3600000L, 500, device = "dev-evil",
      channel = "TRANSFER", mcc = "CRYPTO_EXCHANGE"))
    scored.last.isAlert shouldBe true
    scored.last.riskScore shouldBe 0.7 +- 0.001
  }

  test("l'état purge la fenêtre de 10 minutes et plafonne la taille") {
    val many = (1 to 300).map(i => tx(s"m$i", t0 + i * 1000L, 10))
    var state = CustomerState.empty
    many.foreach { t => state = RiskScorer.updateState(t, state) }
    state.recent.size should be <= 200
    state.recent.forall(r => many.last.eventTime.getTime - r.eventTimeMs <= RiskScorer.WindowMs) shouldBe true
    state.lifetimeCount shouldBe 300
    state.lifetimeAvgAmount shouldBe 10.0 +- 1e-9
    state.homeCountry shouldBe "MA"
  }
}
