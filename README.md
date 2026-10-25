# FraudStream Platform

**Détection de fraude bancaire en temps réel + agents IA d'investigation.**
Kafka · Spark Structured Streaming (Scala) · Cassandra · Airflow / PySpark · BigQuery · Kubernetes (kind / GKE) · Java Spring Boot · Gemini + LangGraph.

> POC personnel : une plateforme Big Data de bout en bout, exécutable **gratuitement en local** en une commande,
> avec un mode **hybride GCP** (BigQuery, GCS, Gemini) dans le palier gratuit et des manifests **GKE** prêts.
> Le fil rouge : des transactions synthétiques de clients d'une banque marocaine, un moteur de scoring temps réel,
> et un **agent IA** qui enquête sur chaque alerte comme le ferait un analyste — avec preuves et garde-fous.

```
Spring Boot ──▶ Kafka ──▶ Spark Streaming (Scala) ──▶ Cassandra (serving)
                                   │                    ├─▶ Lac Parquet ──▶ Airflow/PySpark ──▶ BigQuery
                                   └─▶ fraud.alerts ──▶ Agent IA (Gemini + LangGraph) ──▶ rapport d'enquête
```

## Genèse : de l'inférence serverless à la plateforme Big Data

Ce projet est la **v2** de deux projets AWS que j'ai construits auparavant sur le même cas d'usage :

- [`aws-sagemaker-mlops-pipeline`](https://github.com/omarboukherys/aws-sagemaker-mlops-pipeline) — entraînement XGBoost sur données déséquilibrées, Model Registry, endpoint temps réel, Model Monitor.
- [`aws-realtime-ml-inference`](https://github.com/omarboukherys/aws-realtime-ml-inference) — Kinesis → Lambda → endpoint SageMaker → alertes SNS.

La v1 répondait à « comment scorer une transaction en temps réel avec un modèle en production ». La v2 répond à la question suivante, celle d'une équipe data platform : « comment faire tenir ça à l'échelle, avec de l'état, plusieurs consommateurs, de l'analytique et une boucle d'évaluation ».

| | v1 (AWS serverless) | v2 (ce repo) |
|---|---|---|
| Ingestion | Kinesis | Kafka (partitionnement par client, plusieurs consommateurs) |
| Traitement | Lambda sans état, 1 transaction à la fois | Spark Structured Streaming avec état par client (fenêtres, devices, moyenne historique) |
| Scoring | Modèle XGBoost sur endpoint SageMaker | Règles explicables en Scala, conçues pour être remplacées par le modèle de la v1 (`RiskScorer.process`) |
| Suite d'une alerte | E-mail SNS | Agent IA qui enquête, rend un verdict et une action, avec trace d'audit |
| Stockage | Logs CloudWatch | Cassandra (serving) + lac Parquet + BigQuery (analytique) |
| Évaluation | — | Précision / rappel quotidiens et rappel par scénario de fraude (Airflow) |
| Déploiement | Lambda | Kubernetes (kind / GKE), portabilité Azure documentée |

## Ce que montre ce projet

| Compétence | Où la voir |
|---|---|
| Streaming avec état, exactly-once, watermark, RocksDB | [`streaming-spark/.../FraudStreamingJob.scala`](streaming-spark/src/main/scala/io/fraudstream/streaming/FraudStreamingJob.scala) |
| Logique métier Scala pure, testée sans Spark | [`RiskScorer.scala`](streaming-spark/src/main/scala/io/fraudstream/streaming/RiskScorer.scala) · [`RiskScorerSpec`](streaming-spark/src/test/scala/io/fraudstream/streaming/RiskScorerSpec.scala) |
| Kafka : partitionnement par clé, topics, lag, KRaft | [`docker-compose.yml`](docker-compose.yml) · [`kafka_ops.py`](agents/fraudstream_agents/kafka_ops.py) |
| Modélisation Cassandra query-first (TTL, clustering) | [`cassandra/schema.cql`](cassandra/schema.cql) |
| Batch PySpark + Airflow idempotent + contrôles qualité | [`batch-airflow/`](batch-airflow) |
| BigQuery : partitionnement, clustering, coûts bornés | [`gcp/bigquery_schema.sql`](gcp/bigquery_schema.sql) · [`analytics.py`](agents/fraudstream_agents/analytics.py) |
| Java 21 / Spring Boot 3 / Kafka producer idempotent / Actuator | [`producer-springboot/`](producer-springboot) |
| Kubernetes : kustomize, StatefulSets, Workload Identity | [`k8s/`](k8s) |
| Agents IA : ReAct, outils, NL→SQL avec garde-fous, auditabilité | [`agents/fraudstream_agents/agents.py`](agents/fraudstream_agents/agents.py) |
| Portabilité Azure (Event Hubs, Databricks, ADLS, AKS) | [`docs/azure-portability.md`](docs/azure-portability.md) |
| Exploitation : incidents réels, diagnostic, correctifs | [`docs/runbook.md`](docs/runbook.md) |

## Démarrage rapide (local, 0 €)

Pré-requis : Docker Desktop (≥ 8 Go alloués), `make`, une clé Gemini gratuite ([AI Studio](https://aistudio.google.com/apikey)).

```bash
cp .env.example .env            # renseigner GOOGLE_API_KEY
make up                         # build + démarrage (première fois : ~10 min, le build sbt/Maven est long)
make ps                         # attendre que tout soit healthy / running
make demo                       # injecte 4 fraudes, affiche alertes + enquêtes de l'agent
```

Interfaces (ports modifiables dans `.env`) : Kafka UI `:18085` · Spark UI `:14040` · Airflow `:18081` (admin/admin) · Agents API `:18800/docs` · Producteur `:18090/actuator`.

Déclencher le batch quotidien : `make airflow-run` (ou depuis l'UI Airflow).

## Mode hybride GCP (BigQuery + GCS + Gemini, palier gratuit)

```bash
make gcp-setup PROJECT_ID=mon-projet     # dataset BigQuery, bucket, compte de service, budget d'alerte 5 $
# .env : USE_BIGQUERY=true  GCP_PROJECT_ID=...  GCP_SA_KEY_PATH=./gcp/sa-key.json
make up && make airflow-run              # le DAG charge les marts dans BigQuery
```

L'agent `ask-data` interroge alors BigQuery (avec `maximum_bytes_billed`) au lieu de DuckDB sur le lac local.

## Kubernetes

```bash
make build-images && make kind-up        # cluster kind local
cp k8s/overlays/kind/secret.example.yaml k8s/overlays/kind/secret.yaml   # puis renseigner GOOGLE_API_KEY
make kind-deploy
```

GKE Autopilot (crédits d'essai) : `PROJECT_ID=... ./gcp/gke.sh create`, `make push-images`, `kubectl apply -k k8s/overlays/gke`,
puis `k8s/overlays/gke/spark-submit.sh` pour le mode driver + executors. **Supprimer le cluster après la démo** (`./gcp/gke.sh delete`).

## Tests

```bash
make test            # agents (LLM simulé + API, 20 tests) + batch PySpark (2 tests) — sans Docker
make test-spark      # Scala : règles de scoring + pipeline sur MemoryStream (sbt)
make test-producer   # Java : générateur de scénarios (Maven)
```

## Les agents IA en détail

**Agent analyste fraude** (`/investigate`, et automatiquement sur chaque message de `fraud.alerts`) :
boucle ReAct LangGraph avec 4 outils — `explain_risk_reasons`, `get_customer_history`, `get_customer_profile`,
`query_analytics` — et une sortie JSON validée (`verdict`, `confidence`, `recommended_action`, `key_evidence`).
Chaque appel d'outil et son résultat sont stockés dans `investigations.evidence` : un analyste humain peut rejouer le raisonnement.

**Agent Data Ops** (`/ops`) : lag des consommateurs, offsets, volumes par jour → diagnostic en français.

**Ask-data** (`/ask-data`) : question en langage naturel → SQL BigQuery → réponse chiffrée + SQL utilisé.

Garde-fous (tous testés) : SQL `SELECT` uniquement, tables en liste blanche, `LIMIT` forcé, plafond d'octets facturés ;
`isFraudLabel` (vérité terrain synthétique) retiré du contexte avant le LLM ; sortie illisible ⇒ verdict `SUSPICIOUS` / `MONITOR`, jamais `BLOCK_CARD`.

## Scénarios de fraude simulés et règles

| Scénario injecté | Règle(s) qui le détectent | Score |
|---|---|---|
| `CARD_TESTING` — rafale de 6-12 micro-paiements en ligne, nouveau device | `VELOCITY_SMALL_AMOUNTS` + `NEW_DEVICE` | 0.80 |
| `IMPOSSIBLE_TRAVEL` — 2 achats physiques MA → étranger en 4 min | `IMPOSSIBLE_TRAVEL` + `FOREIGN_COUNTRY` | 0.75 |
| `HIGH_AMOUNT` — 12-30× la moyenne du client, en ligne, à l'étranger | `AMOUNT_ANOMALY` + `FOREIGN_COUNTRY` | 0.55 → **non alerté** (volontaire : montre une limite, voir ci-dessous) |
| `ACCOUNT_TAKEOVER` — nouveau device + virement crypto | `NEW_DEVICE` (poids renforcé sur virement) + `HIGH_RISK_MCC` | 0.70 |

Le DAG batch calcule chaque jour précision / rappel global et **rappel par scénario** (`scenario_recall`) :
c'est ce tableau qui dit quelle règle manque. Exercice naturel de suite : ajouter une règle ou un modèle pour `HIGH_AMOUNT`.

## Résultats mesurés

_À compléter après la première exécution (débit soutenu, latence Kafka → alerte, précision/rappel du jour, latence moyenne des enquêtes)._

## Limites connues et suite

- Démo mono-broker / mono-nœud : en prod, Strimzi (3 brokers), Cassandra 3 réplicas, Spark en mode cluster (script fourni).
- Règles à seuils fixes : étape suivante = modèle entraîné sur le lac (features déjà calculées en streaming) servi dans le même `process(tx, state)`.
- Le job Spark tourne en `local[*]` dans un pod : suffisant pour kind ; sur GKE utiliser `spark-submit.sh`.
- Pas de schéma registry (JSON brut) : Avro + Schema Registry serait le choix de prod.
- Airflow en `standalone` dans un seul conteneur : Cloud Composer ou Astronomer en prod.

## Arborescence

```
producer-springboot/   Java 21 · Spring Boot 3 · générateur de transactions + API de pilotage
streaming-spark/       Scala 2.12 · Spark 3.5 · scoring avec état · 4 sinks
batch-airflow/         DAG Airflow + job PySpark d'agrégats, contrôles qualité, chargement BigQuery
agents/                Python 3.11 · FastAPI · LangGraph · Gemini · DuckDB/BigQuery · consommateur Kafka
cassandra/             schéma CQL
k8s/                   kustomize : base + overlays kind / gke
gcp/                   scripts gcloud/bq, schéma BigQuery, GKE
docs/                  architecture, runbook de dépannage, portabilité Azure, notes d'entretien
```

## Licence

MIT.
