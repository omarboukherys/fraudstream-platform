package io.fraudstream.streaming

import org.apache.spark.sql.types._

/**
 * Contrat du message JSON publié par le producteur Spring Boot dans `transactions.raw`.
 * Toute évolution ici doit être synchronisée avec producer-springboot/.../model/Transaction.java.
 */
object TransactionSchema {

  val schema: StructType = StructType(Seq(
    StructField("transactionId",    StringType,  nullable = false),
    StructField("customerId",       StringType,  nullable = false),
    StructField("accountId",        StringType),
    StructField("eventTime",        StringType), // ISO-8601, converti en timestamp dans le job
    StructField("amount",           DecimalType(14, 2)),
    StructField("currency",         StringType),
    StructField("merchantId",       StringType),
    StructField("merchantCategory", StringType),
    StructField("channel",          StringType),
    StructField("country",          StringType),
    StructField("city",             StringType),
    StructField("deviceId",         StringType),
    StructField("ipAddress",        StringType),
    StructField("isFraudLabel",     BooleanType),
    StructField("fraudScenario",    StringType)
  ))
}
