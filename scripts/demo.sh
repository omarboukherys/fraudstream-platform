#!/usr/bin/env bash
# Scénario de démo de bout en bout (à lancer quand `docker compose ps` montre tout en "healthy"/"running").
# 1. injecte 4 scénarios de fraude via l'API du producteur
# 2. attend que Spark les score et publie les alertes
# 3. affiche les alertes du jour (Cassandra) et les rapports d'enquête de l'agent IA
set -euo pipefail
PRODUCER=${PRODUCER_URL:-http://localhost:8090}
AGENTS=${AGENTS_URL:-http://localhost:8000}

say() { printf "\n\033[1;34m▶ %s\033[0m\n" "$*"; }

say "État du producteur"
curl -s "$PRODUCER/api/producer/status"; echo

say "Injection de 4 scénarios de fraude sur le client C00042"
for S in CARD_TESTING IMPOSSIBLE_TRAVEL HIGH_AMOUNT ACCOUNT_TAKEOVER; do
  printf "  %-18s -> " "$S"; curl -s -X POST "$PRODUCER/api/producer/inject/$S?customerId=C00042"; echo
done

say "Attente du scoring Spark (trigger 5 s) et des enquêtes de l'agent (~10-30 s selon Gemini)..."
sleep 25

say "Alertes du jour (Cassandra via l'API agents)"
curl -s "$AGENTS/alerts/today?limit=10" | python3 -m json.tool | head -60

say "Dernier rapport d'enquête produit par l'agent"
curl -s "$AGENTS/metrics" | python3 -m json.tool

say "Question en langage naturel sur les données (NL -> SQL)"
curl -s -X POST "$AGENTS/ask-data" -H 'Content-Type: application/json' \
  -d '{"question": "Quel est le taux d alerte par canal aujourd hui, et quel canal est le plus risqué ?"}' | python3 -m json.tool

say "Diagnostic d'exploitation du pipeline"
curl -s -X POST "$AGENTS/ops" -H 'Content-Type: application/json' \
  -d '{"question": "Le pipeline est-il en bonne santé ? Y a-t-il du lag sur fraud.alerts ?"}' | python3 -m json.tool

say "Terminé. UIs : Kafka http://localhost:8080 · Spark http://localhost:4040 · Airflow http://localhost:8081 (admin/admin) · Agents http://localhost:8000/docs"
