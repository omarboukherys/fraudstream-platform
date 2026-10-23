#!/usr/bin/env bash
# Soumission Spark native Kubernetes sur GKE (driver + executors en pods), à la place du Deployment "local[*]".
# Pré-requis : image poussée dans Artifact Registry, bucket GCS, Workload Identity configuré (gcp/setup.sh).
set -euo pipefail
: "${PROJECT_ID:?}" "${REGION:=europe-west1}" "${K8S_MASTER:?ex: https://34.x.x.x}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/fraudstream/spark-streaming:0.1.0"

spark-submit \
  --master "k8s://${K8S_MASTER}" \
  --deploy-mode cluster \
  --name fraudstream-streaming \
  --class io.fraudstream.streaming.FraudStreamingJob \
  --conf spark.kubernetes.namespace=fraudstream \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=fraudstream-ksa \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  --conf spark.kubernetes.driverEnv.CASSANDRA_HOST=cassandra \
  --conf spark.kubernetes.driverEnv.LAKE_PATH="gs://${PROJECT_ID}-fraudstream-lake/lake" \
  --conf spark.kubernetes.driverEnv.CHECKPOINT_PATH="gs://${PROJECT_ID}-fraudstream-lake/checkpoints" \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25 \
  local:///opt/app/fraud-streaming.jar
