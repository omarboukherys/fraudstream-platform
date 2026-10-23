package io.fraudstream.streaming

/** Configuration lue dans l'environnement (12-factor) : identique en local, sur kind et sur GKE. */
final case class Config(
    kafkaBootstrap: String,
    inputTopic: String,
    alertTopic: String,
    cassandraHost: String,
    cassandraKeyspace: String,
    lakePath: String,
    checkpointPath: String,
    triggerSeconds: Int,
    watermark: String,
    bqDirectWrite: Boolean,
    bqProject: String,
    bqDataset: String,
    gcsTempBucket: String
)

object Config {
  private def env(name: String, default: String): String = sys.env.getOrElse(name, default)

  def fromEnv(): Config = Config(
    kafkaBootstrap    = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
    inputTopic        = env("INPUT_TOPIC", "transactions.raw"),
    alertTopic        = env("ALERT_TOPIC", "fraud.alerts"),
    cassandraHost     = env("CASSANDRA_HOST", "localhost"),
    cassandraKeyspace = env("CASSANDRA_KEYSPACE", "fraudstream"),
    lakePath          = env("LAKE_PATH", "./data/lake"),          // ou gs://<bucket>/lake
    checkpointPath    = env("CHECKPOINT_PATH", "./data/checkpoints"),
    triggerSeconds    = env("TRIGGER_SECONDS", "5").toInt,
    watermark         = env("WATERMARK", "2 minutes"),
    bqDirectWrite     = env("BQ_DIRECT_WRITE", "false").toBoolean,
    bqProject         = env("GCP_PROJECT_ID", ""),
    bqDataset         = env("BQ_DATASET", "fraudstream"),
    gcsTempBucket     = env("GCS_TEMP_BUCKET", "")
  )
}
