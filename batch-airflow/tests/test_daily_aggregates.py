"""Tests du job batch sur un mini-lac Parquet généré à la volée (pytest -q batch-airflow/tests)."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import daily_aggregates as job  # noqa: E402
from pyspark.sql import Row  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (job.build_spark("test-daily")
         .builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _row(i, customer, amount, alert, fraud, scenario, channel="CARD_PRESENT", country="MA"):
    return Row(
        transactionId=f"t{i}", customerId=customer, accountId="A", eventTime=datetime(2026, 10, 23, 10, i % 60),
        amount=float(amount), currency="MAD", merchantId="M1", merchantCategory="GROCERY", channel=channel,
        country=country, city="Casablanca", deviceId="d1", isFraudLabel=fraud, fraudScenario=scenario,
        txCount10m=1, amountSum10m=float(amount), amountAvg10m=float(amount), distinctCountries10m=1,
        distinctMerchants10m=1, isNewDevice=False, secondsSinceLastTx=-1, riskScore=0.9 if alert else 0.1,
        riskReasons=["X"] if alert else [], isAlert=alert,
    )


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    path = tmp_path_factory.mktemp("lake")
    rows = [
        _row(1, "C1", 100, False, False, ""),
        _row(2, "C1", 120, False, False, ""),
        _row(3, "C2", 3, True, True, "CARD_TESTING"),
        _row(4, "C2", 4, True, True, "CARD_TESTING"),
        _row(5, "C3", 5000, False, True, "HIGH_AMOUNT"),   # fraude ratée
        _row(6, "C4", 200, True, False, ""),               # faux positif
    ]
    df = spark.createDataFrame(rows).withColumn("event_date", job.F.to_date("eventTime"))
    df.write.partitionBy("event_date").parquet(f"{path}/transactions_scored")
    return str(path)


def test_run_produces_marts_and_quality(spark, lake):
    result = job.run(spark, lake, "2026-10-23")
    assert result["transactions"] == 6
    assert result["tp"] == 2 and result["fp"] == 1 and result["fn"] == 1 and result["tn"] == 2
    assert result["precision"] == pytest.approx(2 / 3)
    assert result["recall"] == pytest.approx(2 / 3)

    metrics = spark.read.parquet(f"{lake}/marts/daily_customer_metrics").collect()
    by_customer = {r.customerId: r for r in metrics}
    assert by_customer["C1"].tx_count == 2 and float(by_customer["C1"].amount_sum) == 220.0
    assert by_customer["C2"].alert_count == 2

    recall = {r.fraudScenario: r.recall for r in spark.read.parquet(f"{lake}/marts/scenario_recall").collect()}
    assert recall["CARD_TESTING"] == 1.0 and recall["HIGH_AMOUNT"] == 0.0


def test_run_fails_on_empty_day(spark, lake):
    with pytest.raises(ValueError):
        job.run(spark, lake, "2000-01-01")
