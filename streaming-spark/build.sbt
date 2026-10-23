ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "io.fraudstream"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "fraud-streaming",
    libraryDependencies ++= Seq(
      // Spark : fourni par l'image apache/spark au runtime -> "provided"
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      // Connecteurs
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
      // BigQuery direct (BQ_DIRECT_WRITE=true) : non embarqué dans le jar, à passer à spark-submit :
      //   --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0
      // Par défaut le lac Parquet/GCS alimente BigQuery via Airflow (gratuit).
      // Tests
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test
    ),
    // Java 17 : accès aux internals requis par Spark 3.5
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    ),
    assembly / assemblyJarName := "fraud-streaming-assembly.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case "reference.conf"                      => MergeStrategy.concat  // driver DataStax
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "module-info.class"                   => MergeStrategy.discard
      case x if x.endsWith(".proto")             => MergeStrategy.first
      case _                                     => MergeStrategy.first
    }
  )
