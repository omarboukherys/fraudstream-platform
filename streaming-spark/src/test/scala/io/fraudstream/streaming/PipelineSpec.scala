package io.fraudstream.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test d'intégration du pipeline Spark sans Kafka ni Cassandra : on injecte du JSON via MemoryStream
 * et on vérifie que l'état par client détecte bien un scénario de fraude multi-événements.
 */
class PipelineSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[2]").appName("pipeline-spec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def json(id: String, customer: String, time: String, amount: Double, country: String,
                   device: String = "dev-1", channel: String = "CARD_PRESENT"): String =
    s"""{"transactionId":"$id","customerId":"$customer","accountId":"A1","eventTime":"$time","amount":$amount,
       |"currency":"MAD","merchantId":"M1","merchantCategory":"GROCERY","channel":"$channel","country":"$country",
       |"city":"Casablanca","deviceId":"$device","ipAddress":"1.1.1.1","isFraudLabel":false,"fraudScenario":null}"""
      .stripMargin.replace("\n", "")

  test("le pipeline score un voyage impossible à partir du flux JSON") {
    import spark.implicits._
    implicit val sqlCtx: org.apache.spark.sql.SQLContext = spark.sqlContext
    val cfg = Config.fromEnv().copy(watermark = "1 minute")
    val input = MemoryStream[String]
    val raw = input.toDF() // colonne "value", comme la source Kafka

    val scored = FraudStreamingJob.buildPipeline(spark, raw, cfg)
    val query = scored.writeStream.format("memory").queryName("scored").outputMode(OutputMode.Append()).start()

    input.addData(
      json("a", "C1", "2026-10-23T10:00:00.000Z", 150, "MA"),
      json("b", "C1", "2026-10-23T10:04:00.000Z", 400, "FR"),
      json("c", "C2", "2026-10-23T10:05:00.000Z", 90, "MA"))
    query.processAllAvailable()
    // Un événement bien plus tard fait avancer le watermark et libère les sorties append
    input.addData(json("d", "C3", "2026-10-23T12:00:00.000Z", 50, "MA"))
    query.processAllAvailable()

    val rows = spark.table("scored").as[ScoredTx].collect().sortBy(_.transactionId)
    rows.map(_.transactionId).toSeq should contain allOf ("a", "b", "c")
    val b = rows.find(_.transactionId == "b").get
    b.isAlert shouldBe true
    b.riskReasons.exists(_.startsWith("IMPOSSIBLE_TRAVEL")) shouldBe true
    rows.find(_.transactionId == "a").get.isAlert shouldBe false
    rows.find(_.transactionId == "c").get.isAlert shouldBe false
    query.stop()
  }
}
