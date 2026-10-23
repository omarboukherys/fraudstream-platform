"""
Couche analytique interrogée par les agents (NL -> SQL).

  - USE_BIGQUERY=true  : BigQuery (dataset `fraudstream`), avec plafond d'octets facturés par requête.
  - sinon              : DuckDB sur le lac Parquet local, exposé sous les MÊMES noms de tables.

Les garde-fous SQL (SELECT uniquement, tables autorisées, LIMIT forcé) s'appliquent dans les deux cas :
un LLM ne doit jamais pouvoir modifier ou exfiltrer des données via cet outil.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from .config import Settings, settings as default_settings

ALLOWED_TABLES = {
    "transactions_scored", "daily_customer_metrics", "daily_fraud_kpis", "detection_quality", "scenario_recall",
}

SCHEMA_DOC = """\
Tables disponibles (toutes partitionnées par event_date DATE) :
- transactions_scored(transactionId STRING, customerId STRING, eventTime TIMESTAMP, amount NUMERIC, currency STRING,
    merchantId STRING, merchantCategory STRING, channel STRING [CARD_PRESENT|ONLINE|ATM|TRANSFER], country STRING,
    city STRING, deviceId STRING, isFraudLabel BOOL, fraudScenario STRING, txCount10m INT, amountSum10m FLOAT,
    amountAvg10m FLOAT, distinctCountries10m INT, distinctMerchants10m INT, isNewDevice BOOL, secondsSinceLastTx INT,
    riskScore FLOAT, riskReasons ARRAY<STRING>, isAlert BOOL, event_date DATE)
- daily_customer_metrics(event_date, customerId, tx_count, amount_sum, amount_avg, amount_max, alert_count,
    max_risk_score, distinct_countries, distinct_devices, online_tx_count)
- daily_fraud_kpis(event_date, channel, country, tx_count, amount_sum, alert_count, alert_rate, avg_risk_score)
- detection_quality(event_date, tp, fp, fn, tn, precision, recall)
- scenario_recall(event_date, fraudScenario, injected, detected, recall)
Dialecte : SQL standard (BigQuery). Toujours filtrer sur event_date quand c'est possible (coût)."""


class SqlGuardError(ValueError):
    pass


@dataclass
class QueryResult:
    sql: str
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    bytes_processed: int | None = None


def guard_sql(sql: str, row_limit: int, dialect: str = "bigquery") -> str:
    """Valide et normalise une requête générée par le LLM. Lève SqlGuardError sinon."""
    sql = sql.strip().rstrip(";")
    if not sql:
        raise SqlGuardError("requête vide")
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.ParseError as e:
        raise SqlGuardError(f"SQL invalide : {e}") from e
    if len(statements) != 1 or statements[0] is None:
        raise SqlGuardError("une seule instruction autorisée")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union)):
        raise SqlGuardError(f"seules les requêtes SELECT sont autorisées (reçu {type(tree).__name__})")
    for node in tree.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Merge, exp.Command)):
            raise SqlGuardError("instruction d'écriture détectée")
    tables = {t.name for t in tree.find_all(exp.Table)}
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    forbidden = tables - ALLOWED_TABLES - ctes
    if forbidden:
        raise SqlGuardError(f"tables non autorisées : {sorted(forbidden)}")
    # LIMIT forcé (ou abaissé) pour borner la taille de la réponse renvoyée au LLM
    limit = tree.args.get("limit")
    if limit is None or int(limit.expression.this) > row_limit:
        tree = tree.limit(row_limit)
    return tree.sql(dialect=dialect)


class DuckDbAnalytics:
    """Exécute les requêtes sur le lac Parquet local, avec les mêmes noms de tables qu'en BigQuery."""

    def __init__(self, lake_path: str):
        import duckdb

        self.conn = duckdb.connect(database=":memory:")
        self.lake_path = lake_path
        self.conn.execute(
            "CREATE OR REPLACE VIEW transactions_scored AS "
            f"SELECT * FROM read_parquet('{lake_path}/transactions_scored/*/*.parquet', hive_partitioning = true)")
        for mart in ("daily_customer_metrics", "daily_fraud_kpis", "detection_quality", "scenario_recall"):
            # Les marts n'existent qu'après un run Airflow : vue tolérante si le dossier est absent
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {mart} AS SELECT * FROM read_parquet("
                f"'{lake_path}/marts/{mart}/*/*.parquet', hive_partitioning = true, union_by_name = true)"
            ) if self._exists(f"{lake_path}/marts/{mart}") else None

    @staticmethod
    def _exists(path: str) -> bool:
        import glob
        return bool(glob.glob(f"{path}/*/*.parquet"))

    def query(self, sql: str, row_limit: int) -> QueryResult:
        safe = guard_sql(sql, row_limit, dialect="bigquery")
        duck_sql = sqlglot.transpile(safe, read="bigquery", write="duckdb")[0]
        cur = self.conn.execute(duck_sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return QueryResult(sql=safe, rows=rows, row_count=len(rows), truncated=len(rows) >= row_limit)


class BigQueryAnalytics:
    def __init__(self, project: str, dataset: str, max_bytes_billed: int):
        from google.cloud import bigquery

        self.client = bigquery.Client(project=project)
        self.project, self.dataset, self.max_bytes = project, dataset, max_bytes_billed
        self._bq = bigquery

    def _qualify(self, sql: str) -> str:
        tree = sqlglot.parse_one(sql, read="bigquery")
        for t in tree.find_all(exp.Table):
            if t.name in ALLOWED_TABLES and not t.db:
                t.set("db", exp.to_identifier(self.dataset))
                t.set("catalog", exp.to_identifier(self.project))
        return tree.sql(dialect="bigquery")

    def query(self, sql: str, row_limit: int) -> QueryResult:
        safe = guard_sql(sql, row_limit)
        qualified = self._qualify(safe)
        job_config = self._bq.QueryJobConfig(maximum_bytes_billed=self.max_bytes, use_query_cache=True)
        job = self.client.query(qualified, job_config=job_config)
        rows = [dict(r) for r in job.result()]
        return QueryResult(sql=safe, rows=rows, row_count=len(rows), truncated=len(rows) >= row_limit,
                           bytes_processed=job.total_bytes_processed)


def build_analytics(cfg: Settings = default_settings):
    if cfg.use_bigquery:
        return BigQueryAnalytics(cfg.gcp_project_id, cfg.bq_dataset, cfg.bq_max_bytes_billed)
    return DuckDbAnalytics(cfg.lake_path)


def strip_code_fences(text: str) -> str:
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    return (m.group(1) if m else text).strip()
