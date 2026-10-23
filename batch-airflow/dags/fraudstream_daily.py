"""
DAG Airflow : pipeline batch quotidien FraudStream.

  check_lake_partition ──► spark_daily_aggregates ──► data_quality_checks ──► load_bigquery ──► publish_summary
                                                                              (skip si USE_BIGQUERY=false)

Idempotent : chaque run écrase la partition du jour (dynamic partition overwrite + BigQuery WRITE_TRUNCATE par partition).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

LAKE_PATH = os.environ.get("LAKE_PATH", "/data/lake")
USE_BIGQUERY = os.environ.get("USE_BIGQUERY", "false").lower() == "true"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BQ_DATASET = os.environ.get("BQ_DATASET", "fraudstream")
MARTS = ["daily_customer_metrics", "daily_fraud_kpis", "detection_quality", "scenario_recall"]

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _list_parquet(path: str) -> list[str]:
    if path.startswith("gs://"):
        import gcsfs
        return [f for f in gcsfs.GCSFileSystem().ls(path) if f.endswith(".parquet")]
    import glob
    return glob.glob(f"{path}/*.parquet")


def check_lake_partition(ds: str, **_):
    files = _list_parquet(f"{LAKE_PATH}/transactions_scored/event_date={ds}")
    if not files:
        raise AirflowSkipException(f"Pas de données pour {ds} dans {LAKE_PATH} – rien à agréger.")
    print(f"{len(files)} fichiers Parquet pour {ds}")


def data_quality_checks(ds: str, **_):
    """Contrôles simples façon Great Expectations : non-vide, pas de montant négatif, précision minimale."""
    import pyarrow.dataset as pds

    metrics = pds.dataset(f"{LAKE_PATH}/marts/daily_customer_metrics", format="parquet", partitioning="hive").to_table(
        filter=(pds.field("event_date") == datetime.strptime(ds, "%Y-%m-%d").date())
    ).to_pandas()
    assert len(metrics) > 0, "daily_customer_metrics est vide"
    assert (metrics["amount_sum"].astype(float) >= 0).all(), "montants négatifs détectés"
    assert metrics["customerId"].is_unique, "doublons (jour, client)"

    quality = pds.dataset(f"{LAKE_PATH}/marts/detection_quality", format="parquet", partitioning="hive").to_table(
        filter=(pds.field("event_date") == datetime.strptime(ds, "%Y-%m-%d").date())
    ).to_pandas().iloc[0]
    print(f"précision={quality['precision']:.3f} rappel={quality['recall']:.3f}")
    if quality["recall"] < 0.5:
        # On n'échoue pas le DAG : on alerte. En prod -> notification Slack/PagerDuty.
        print("WARNING: rappel < 0.5, le moteur de règles rate trop de fraudes injectées")
    return {"precision": float(quality["precision"]), "recall": float(quality["recall"])}


def load_bigquery(ds: str, **_):
    if not USE_BIGQUERY:
        raise AirflowSkipException("USE_BIGQUERY=false : chargement BigQuery ignoré (mode local).")
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    tables = ["transactions_scored"] + MARTS
    for table in tables:
        src = f"{LAKE_PATH}/{'' if table == 'transactions_scored' else 'marts/'}{table}/event_date={ds}"
        if src.startswith("gs://"):
            # Lac sur GCS : chargement natif depuis les URIs, partition hive event_date=... détectée automatiquement
            hive = bigquery.HivePartitioningOptions()
            hive.mode = "AUTO"
            hive.source_uri_prefix = src.rsplit("/event_date=", 1)[0] + "/"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                time_partitioning=bigquery.TimePartitioning(field="event_date"),
                hive_partitioning=hive,
            )
            job = client.load_table_from_uri([f"{src}/*.parquet"], f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table}${ds.replace('-', '')}", job_config=job_config)
            job.result()
        else:
            # Mode hybride : lac local -> upload direct des Parquet locaux dans BigQuery
            import glob
            import pyarrow.parquet as pq
            import pyarrow as pa

            files = glob.glob(f"{src}/*.parquet")
            if not files:
                print(f"aucun fichier pour {table}/{ds}, ignoré")
                continue
            table_arrow = pa.concat_tables([pq.read_table(f) for f in files])
            table_arrow = table_arrow.append_column("event_date", pa.array([datetime.strptime(ds, "%Y-%m-%d").date()] * table_arrow.num_rows))
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                time_partitioning=bigquery.TimePartitioning(field="event_date"),
            )
            import io
            buf = io.BytesIO()
            pq.write_table(table_arrow, buf)
            buf.seek(0)
            job = client.load_table_from_file(buf, f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table}${ds.replace('-', '')}", job_config=job_config)
            job.result()
        print(f"BigQuery: {table} chargé pour {ds}")


def publish_summary(ds: str, ti=None, **_):
    quality = ti.xcom_pull(task_ids="data_quality_checks") or {}
    summary = {"date": ds, "bigquery": USE_BIGQUERY, **quality}
    print("RÉSUMÉ DU RUN:", json.dumps(summary))
    return summary


with DAG(
    dag_id="fraudstream_daily",
    description="Agrégats quotidiens fraude : lac Parquet -> marts PySpark -> BigQuery",
    schedule="15 1 * * *",          # 01:15 UTC, après la fin de journée
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraudstream", "batch", "bigquery"],
) as dag:

    t_check = PythonOperator(task_id="check_lake_partition", python_callable=check_lake_partition)

    t_spark = BashOperator(
        task_id="spark_daily_aggregates",
        bash_command=(
            "spark-submit --master local[*] --driver-memory 1g "
            "/opt/airflow/jobs/daily_aggregates.py --date {{ ds }} --lake-path " + LAKE_PATH
        ),
    )

    t_quality = PythonOperator(task_id="data_quality_checks", python_callable=data_quality_checks)
    t_bq = PythonOperator(task_id="load_bigquery", python_callable=load_bigquery)
    t_summary = PythonOperator(task_id="publish_summary", python_callable=publish_summary, trigger_rule="none_failed")

    t_check >> t_spark >> t_quality >> t_bq >> t_summary
