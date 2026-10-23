"""Configuration 12-factor : tout vient de l'environnement (identique en local, kind et GKE)."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM
    gemini_model: str = "gemini-2.5-flash"
    google_api_key: str = ""            # clé AI Studio (palier gratuit) ...
    use_vertex: bool = False            # ... ou Vertex AI avec le compte de service GCP
    gcp_project_id: str = ""
    gcp_location: str = "europe-west1"
    llm_temperature: float = 0.1

    # Données
    kafka_bootstrap_servers: str = "localhost:9094"
    alert_topic: str = "fraud.alerts"
    report_topic: str = "agent.reports"
    alert_consumer_group: str = "fraud-analyst-agent"
    alert_consumer_enabled: bool = False
    cassandra_host: str = "localhost"
    cassandra_keyspace: str = "fraudstream"
    lake_path: str = "./data/lake"
    use_bigquery: bool = False
    bq_dataset: str = "fraudstream"
    bq_max_bytes_billed: int = 1_000_000_000   # 1 Go max par requête : garde-fou coût

    # Agent
    max_tool_calls: int = 8
    sql_row_limit: int = 200


settings = Settings()
