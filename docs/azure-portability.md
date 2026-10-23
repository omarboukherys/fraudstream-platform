# Portabilité Azure

Le POC tourne sur GCP, mais chaque brique a été choisie pour être **remplaçable sans réécrire le code métier**.
L'annonce cible un profil GCP *et* Azure : ce document montre la correspondance et ce qu'il faudrait changer.

| Brique FraudStream | GCP (ce repo) | Azure (équivalent) | Impact sur le code |
|---|---|---|---|
| Bus d'événements | Kafka (KRaft) / Confluent | **Azure Event Hubs** (endpoint compatible Kafka 1.0+) | Aucun : changer `bootstrap.servers` + SASL (voir ci-dessous) |
| Streaming | Spark Structured Streaming sur GKE | **Azure Databricks** ou Spark sur **AKS** | Aucun : même jar, `spark-submit` ou Databricks Jobs |
| Serving temps réel | Cassandra | **Azure Cosmos DB for Apache Cassandra** (API CQL) | Aucun : même driver, `CASSANDRA_HOST` + TLS |
| Lac de données | GCS (Parquet) | **ADLS Gen2** (`abfss://`) | Aucun : `LAKE_PATH=abfss://lake@<compte>.dfs.core.windows.net/` + connecteur hadoop-azure |
| Entrepôt analytique | BigQuery | **Azure Synapse / Microsoft Fabric** (ou Databricks SQL) | `analytics.py` : ajouter une classe `SynapseAnalytics` (pyodbc), dialecte sqlglot `tsql` |
| Orchestration | Airflow (Cloud Composer) | **Azure Data Factory** ou Airflow managé (Astronomer / Fabric) | DAG réutilisable tel quel sur Airflow ; sinon réécriture en pipeline ADF |
| Kubernetes | GKE Autopilot | **AKS** | Aucun : manifests kustomize identiques, overlay `aks` avec ACR |
| LLM | Gemini (Vertex AI) | **Azure OpenAI** (GPT-4o) via `langchain-openai` | `llm.py` : une branche `use_azure_openai` ; les agents LangGraph ne changent pas |
| Secrets / identité | Workload Identity + Secret Manager | **Workload Identity AKS** + **Key Vault** | Overlay kustomize |

## Event Hubs en pratique (producteur Spring Boot)

```yaml
spring.kafka:
  bootstrap-servers: <namespace>.servicebus.windows.net:9093
  properties:
    security.protocol: SASL_SSL
    sasl.mechanism: PLAIN
    sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="${EVENTHUBS_CONNECTION_STRING}";
```

Côté Spark, les mêmes options sont passées en `kafka.*` sur la source/sink. Limites à connaître : Event Hubs ne
supporte pas les transactions Kafka ni la compaction de topics ; le partitionnement par clé reste garanti.

## Ce qui ne bouge pas

- Le modèle de données (`cassandra/schema.cql`, `gcp/bigquery_schema.sql`, schéma Parquet du lac).
- Le moteur de scoring (`RiskScorer.scala`, 100 % pur, testé sans Spark).
- Les agents LangGraph et leurs garde-fous SQL (`analytics.guard_sql`), indépendants du moteur SQL cible.

## Ce que je ferais en premier pour un client Azure

1. Event Hubs + Databricks (Jobs + Unity Catalog) : 80 % de la valeur pour le moins d'effort.
2. ADLS Gen2 + Delta Lake à la place du Parquet brut (ACID, time travel, `OPTIMIZE`).
3. Azure OpenAI pour les agents, avec Content Safety et journalisation des prompts dans Log Analytics.
