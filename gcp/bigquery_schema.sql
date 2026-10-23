-- Schéma BigQuery de FraudStream (dataset `fraudstream`). Les tables sont (re)créées par Airflow au chargement ;
-- ce script fixe le partitionnement/clustering pour maîtriser les coûts de scan.
CREATE TABLE IF NOT EXISTS fraudstream.transactions_scored (
  transactionId STRING, customerId STRING, accountId STRING, eventTime TIMESTAMP, amount NUMERIC, currency STRING,
  merchantId STRING, merchantCategory STRING, channel STRING, country STRING, city STRING, deviceId STRING,
  isFraudLabel BOOL, fraudScenario STRING, txCount10m INT64, amountSum10m FLOAT64, amountAvg10m FLOAT64,
  distinctCountries10m INT64, distinctMerchants10m INT64, isNewDevice BOOL, secondsSinceLastTx INT64,
  riskScore FLOAT64, riskReasons ARRAY<STRING>, isAlert BOOL, ingested_at TIMESTAMP, event_date DATE
)
PARTITION BY event_date
CLUSTER BY customerId, isAlert
OPTIONS (partition_expiration_days = 60, require_partition_filter = FALSE);

CREATE TABLE IF NOT EXISTS fraudstream.daily_customer_metrics (
  event_date DATE, customerId STRING, tx_count INT64, amount_sum NUMERIC, amount_avg NUMERIC, amount_max NUMERIC,
  alert_count INT64, max_risk_score FLOAT64, distinct_countries INT64, distinct_devices INT64, online_tx_count INT64
) PARTITION BY event_date CLUSTER BY customerId;

CREATE TABLE IF NOT EXISTS fraudstream.daily_fraud_kpis (
  event_date DATE, channel STRING, country STRING, tx_count INT64, amount_sum NUMERIC, alert_count INT64,
  alert_rate FLOAT64, avg_risk_score FLOAT64
) PARTITION BY event_date;

CREATE TABLE IF NOT EXISTS fraudstream.detection_quality (
  event_date DATE, tp INT64, fp INT64, fn INT64, tn INT64, precision FLOAT64, recall FLOAT64
) PARTITION BY event_date;

CREATE TABLE IF NOT EXISTS fraudstream.scenario_recall (
  event_date DATE, fraudScenario STRING, injected INT64, detected INT64, recall FLOAT64
) PARTITION BY event_date;

-- Vue "tableau de bord" : alertes du jour avec rappel/précision
CREATE OR REPLACE VIEW fraudstream.v_daily_overview AS
SELECT k.event_date,
       SUM(k.tx_count) AS tx_count, SUM(k.alert_count) AS alert_count,
       SAFE_DIVIDE(SUM(k.alert_count), SUM(k.tx_count)) AS alert_rate,
       ANY_VALUE(q.precision) AS precision, ANY_VALUE(q.recall) AS recall
FROM fraudstream.daily_fraud_kpis k
LEFT JOIN fraudstream.detection_quality q USING (event_date)
GROUP BY k.event_date;
