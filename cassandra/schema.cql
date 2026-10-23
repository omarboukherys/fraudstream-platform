-- FraudStream – modèle de données Cassandra (couche "serving" temps réel)
-- Principe : une table par pattern d'accès (query-first design).

CREATE KEYSPACE IF NOT EXISTS fraudstream
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fraudstream;

-- Historique des transactions par client, du plus récent au plus ancien.
-- Pattern : "les N dernières transactions du client X" (utilisé par l'agent analyste).
CREATE TABLE IF NOT EXISTS transactions_by_customer (
  customer_id   text,
  event_time    timestamp,
  transaction_id text,
  amount        decimal,
  currency      text,
  merchant_id   text,
  merchant_category text,
  channel       text,          -- CARD_PRESENT | ONLINE | ATM | TRANSFER
  country       text,
  city          text,
  device_id     text,
  risk_score    double,
  risk_reasons  list<text>,
  is_fraud_label boolean,      -- vérité terrain synthétique (démo uniquement)
  PRIMARY KEY ((customer_id), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND default_time_to_live = 2592000;   -- 30 jours

-- Profil courant du client, mis à jour par le streaming (agrégats fenêtrés).
-- Pattern : "quel est le comportement habituel du client X ?"
CREATE TABLE IF NOT EXISTS customer_profile (
  customer_id       text PRIMARY KEY,
  window_end        timestamp,
  tx_count_10m      int,
  amount_sum_10m    decimal,
  amount_avg_10m    decimal,
  distinct_countries_10m int,
  distinct_merchants_10m int,
  last_country      text,
  last_device_id    text,
  updated_at        timestamp
);

-- Alertes de fraude, partitionnées par jour pour les listes "alertes du jour".
CREATE TABLE IF NOT EXISTS alerts_by_day (
  alert_day      date,
  event_time     timestamp,
  alert_id       text,
  transaction_id text,
  customer_id    text,
  amount         decimal,
  risk_score     double,
  risk_reasons   list<text>,
  status         text,         -- OPEN | INVESTIGATING | CONFIRMED | DISMISSED
  PRIMARY KEY ((alert_day), event_time, alert_id)
) WITH CLUSTERING ORDER BY (event_time DESC, alert_id ASC);

-- Rapports produits par l'agent IA analyste.
CREATE TABLE IF NOT EXISTS investigations (
  alert_id       text PRIMARY KEY,
  transaction_id text,
  customer_id    text,
  created_at     timestamp,
  model          text,
  verdict        text,         -- FRAUD_LIKELY | SUSPICIOUS | LEGITIMATE
  confidence     double,
  summary        text,
  recommended_action text,
  evidence       text,         -- JSON des appels d'outils (traçabilité)
  latency_ms     int
);
