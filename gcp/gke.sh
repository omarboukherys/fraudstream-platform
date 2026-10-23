#!/usr/bin/env bash
# Cluster GKE Autopilot minimal (facturé à l'usage : à n'utiliser qu'avec les crédits d'essai, puis à supprimer !).
# Usage : PROJECT_ID=... ./gcp/gke.sh create | delete
set -euo pipefail
: "${PROJECT_ID:?}"; REGION="${REGION:-europe-west1}"; CLUSTER="fraudstream"
case "${1:-create}" in
  create)
    gcloud services enable container.googleapis.com
    gcloud container clusters create-auto "$CLUSTER" --region "$REGION" --project "$PROJECT_ID"
    gcloud artifacts repositories create fraudstream --repository-format=docker --location="$REGION" 2>/dev/null || true
    # Workload Identity : KSA fraudstream/fraudstream-ksa -> GSA fraudstream-sa
    gcloud iam service-accounts add-iam-policy-binding "fraudstream-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
      --role roles/iam.workloadIdentityUser \
      --member "serviceAccount:${PROJECT_ID}.svc.id.goog[fraudstream/fraudstream-ksa]"
    gcloud container clusters get-credentials "$CLUSTER" --region "$REGION"
    echo "Puis : make build-images && make push-images PROJECT_ID=$PROJECT_ID && kubectl apply -k k8s/overlays/gke"
    ;;
  delete)
    gcloud container clusters delete "$CLUSTER" --region "$REGION" --quiet
    ;;
esac
