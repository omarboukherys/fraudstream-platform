"""
Job PySpark batch : agrégats quotidiens à partir du lac Parquet produit par le streaming.

Entrée  : {LAKE_PATH}/transactions_scored/event_date=YYYY-MM-DD/
Sorties : {LAKE_PATH}/marts/daily_customer_metrics/  (Parquet)
          {LAKE_PATH}/marts/daily_fraud_kpis/
          {LAKE_PATH}/marts/detection_quality/        (précision/rappel vs label synthétique)
          + chargement BigQuery si USE_BIGQUERY=true (via le DAG, pas ici : séparation des responsabilités)

Usage : spark-submit daily_aggregates.py --date 2026-10-23
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "fraudstream-daily-aggregates") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def compute_daily_customer_metrics(tx):
    """Une ligne par (jour, client) : volumes, montants, nb d'alertes, pays distincts."""
    return tx.groupBy("event_date", "customerId").agg(
        F.count("*").alias("tx_count"),
        F.sum("amount").cast("decimal(16,2)").alias("amount_sum"),
        F.avg("amount").cast("decimal(16,2)").alias("amount_avg"),
        F.max("amount").cast("decimal(16,2)").alias("amount_max"),
        F.sum(F.col("isAlert").cast("int")).alias("alert_count"),
        F.max("riskScore").alias("max_risk_score"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("deviceId").alias("distinct_devices"),
        F.sum(F.when(F.col("channel") == "ONLINE", 1).otherwise(0)).alias("online_tx_count"),
    )


def compute_daily_fraud_kpis(tx):
    """KPIs globaux du jour, par canal et pays : ce que suit un responsable fraude."""
    return tx.groupBy("event_date", "channel", "country").agg(
        F.count("*").alias("tx_count"),
        F.sum("amount").cast("decimal(16,2)").alias("amount_sum"),
        F.sum(F.col("isAlert").cast("int")).alias("alert_count"),
        (F.sum(F.col("isAlert").cast("int")) / F.count("*")).alias("alert_rate"),
        F.avg("riskScore").alias("avg_risk_score"),
    )


def compute_detection_quality(tx):
    """
    Qualité de détection du moteur de règles vs vérité terrain synthétique (isFraudLabel).
    En production, le label viendrait des retours des analystes (table investigations).
    """
    confusion = tx.groupBy("event_date").agg(
        F.sum(F.when(F.col("isAlert") & F.col("isFraudLabel"), 1).otherwise(0)).alias("tp"),
        F.sum(F.when(F.col("isAlert") & ~F.col("isFraudLabel"), 1).otherwise(0)).alias("fp"),
        F.sum(F.when(~F.col("isAlert") & F.col("isFraudLabel"), 1).otherwise(0)).alias("fn"),
        F.sum(F.when(~F.col("isAlert") & ~F.col("isFraudLabel"), 1).otherwise(0)).alias("tn"),
    )
    return confusion.select(
        "event_date", "tp", "fp", "fn", "tn",
        (F.col("tp") / F.greatest(F.col("tp") + F.col("fp"), F.lit(1))).alias("precision"),
        (F.col("tp") / F.greatest(F.col("tp") + F.col("fn"), F.lit(1))).alias("recall"),
    )


def compute_scenario_recall(tx):
    """Rappel par scénario de fraude injecté : quel pattern le moteur rate-t-il ?"""
    return (
        tx.filter(F.col("isFraudLabel"))
        .groupBy("event_date", "fraudScenario")
        .agg(
            F.count("*").alias("injected"),
            F.sum(F.col("isAlert").cast("int")).alias("detected"),
            (F.sum(F.col("isAlert").cast("int")) / F.count("*")).alias("recall"),
        )
    )


def run(spark: SparkSession, lake_path: str, date: str) -> dict:
    src = f"{lake_path}/transactions_scored"
    tx = spark.read.parquet(src).filter(F.col("event_date") == F.lit(date))
    n = tx.count()
    if n == 0:
        raise ValueError(f"Aucune transaction dans le lac pour {date} ({src})")

    marts = {
        "daily_customer_metrics": compute_daily_customer_metrics(tx),
        "daily_fraud_kpis": compute_daily_fraud_kpis(tx),
        "detection_quality": compute_detection_quality(tx),
        "scenario_recall": compute_scenario_recall(tx),
    }
    for name, df in marts.items():
        (df.write.mode("overwrite").partitionBy("event_date").parquet(f"{lake_path}/marts/{name}"))

    quality = marts["detection_quality"].collect()[0].asDict()
    return {"date": date, "transactions": n, **{k: (float(v) if v is not None else None) for k, v in quality.items() if k != "event_date"}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--lake-path", default=os.environ.get("LAKE_PATH", "/data/lake"))
    args = parser.parse_args()
    spark = build_spark()
    try:
        print(run(spark, args.lake_path, args.date))
    finally:
        spark.stop()
