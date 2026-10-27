#!/usr/bin/env bash
# Provisionne le strict nécessaire sur GCP pour le mode hybride (local + BigQuery/GCS/Gemini).
# Coût attendu : 0 € dans le palier gratuit (BigQuery 10 Go stockage / 1 To requêtes par mois, GCS 5 Go).
# GKE n'est PAS créé ici : voir gke.sh (crédits d'essai).
#
# Usage : PROJECT_ID=mon-projet ./gcp/setup.sh
set -euo pipefail
: "${PROJECT_ID:?définir PROJECT_ID}"
REGION="${REGION:-europe-west1}"
DATASET="${BQ_DATASET:-fraudstream}"
BUCKET="${PROJECT_ID}-fraudstream-lake"
SA="fraudstream-sa"

gcloud config set project "$PROJECT_ID"
gcloud services enable bigquery.googleapis.com storage.googleapis.com aiplatform.googleapis.com \
  artifactregistry.googleapis.com iam.googleapis.com

# 1. Dataset BigQuery (localisation EU) + table d'exemple avec partitionnement par jour et expiration 60 j (coût)
bq --location=EU mk --dataset --default_partition_expiration=5184000 "${PROJECT_ID}:${DATASET}" 2>/dev/null || true
bq query --use_legacy_sql=false < "$(dirname "$0")/bigquery_schema.sql"

# 2. Bucket GCS pour le lac Parquet (mode GKE) + staging BigQuery, cycle de vie 30 j
gcloud storage buckets create "gs://${BUCKET}" --location="$REGION" --uniform-bucket-level-access 2>/dev/null || true
cat > /tmp/lifecycle.json <<'EOF'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}
EOF
gcloud storage buckets update "gs://${BUCKET}" --lifecycle-file=/tmp/lifecycle.json

# 3. Compte de service moindre privilège (Airflow local -> BigQuery ; agents -> BigQuery lecture ; Vertex AI)
gcloud iam service-accounts create "$SA" --display-name="FraudStream platform" 2>/dev/null || true
SA_EMAIL="${SA}@${PROJECT_ID}.iam.gserviceaccount.com"
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA_EMAIL}" --role="$ROLE" --quiet >/dev/null
done

# 4. Clé JSON pour le mode local/hybride (jamais commitée : gcp/*.json est dans .gitignore)
if [ ! -f "$(dirname "$0")/sa-key.json" ]; then
  gcloud iam service-accounts keys create "$(dirname "$0")/sa-key.json" --iam-account="$SA_EMAIL"
fi

# 5. Budget d'alerte : 5 $ (≈ 50 MAD) pour ne jamais dépasser l'enveloppe
BILLING=$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName)')
gcloud billing budgets create --billing-account="${BILLING##*/}" --display-name="fraudstream-cap" \
  --budget-amount=5USD --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0 2>/dev/null || true

cat <<EOF

OK. À mettre dans votre .env :
  USE_BIGQUERY=true
  GCP_PROJECT_ID=${PROJECT_ID}
  BQ_DATASET=${DATASET}
  GCP_SA_KEY_PATH=./gcp/sa-key.json
  GOOGLE_API_KEY=<clé Gemini AI Studio>   # ou USE_VERTEX=true
EOF
