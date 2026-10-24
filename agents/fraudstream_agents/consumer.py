"""
Consommateur Kafka du topic `fraud.alerts` : chaque alerte déclenche une enquête de l'agent analyste.
Le rapport est écrit dans Cassandra (investigations), le statut de l'alerte mis à jour, et un message publié
dans `agent.reports` (pour un dashboard, une notification ou un autre agent).

Idempotence : alertId est déterministe (alert-<transactionId>) ; une alerte déjà enquêtée est ignorée.
Commit des offsets après traitement => at-least-once, dédoublonné par la vérification ci-dessus.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime

from kafka import KafkaConsumer, KafkaProducer

from .agents import FraudAnalystAgent
from .config import Settings, settings as default_settings
from .store import Store

log = logging.getLogger(__name__)


class AlertConsumer(threading.Thread):
    def __init__(self, agent: FraudAnalystAgent, store: Store, cfg: Settings = default_settings):
        super().__init__(name="alert-consumer", daemon=True)
        self.agent, self.store, self.cfg = agent, store, cfg
        self._stop = threading.Event()
        self.processed = 0
        self.failed = 0
        self.last_report: dict | None = None

    def stop(self):
        self._stop.set()

    def run(self):
        consumer = KafkaConsumer(
            self.cfg.alert_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap_servers,
            group_id=self.cfg.alert_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            max_poll_records=1,                    # une alerte à la fois : une enquête LLM dure plusieurs minutes
            max_poll_interval_ms=30 * 60 * 1000,   # ceinture : tolère 30 min entre deux poll()
            session_timeout_ms=45_000,
        )
        producer = KafkaProducer(bootstrap_servers=self.cfg.kafka_bootstrap_servers,
                                 value_serializer=lambda d: json.dumps(d, ensure_ascii=False, default=str).encode())
        log.info("alert consumer started on %s", self.cfg.alert_topic)
        try:
            while not self._stop.is_set():
                batch = consumer.poll(timeout_ms=1000)
                for _tp, records in batch.items():
                    for record in records:
                        self._process_keeping_alive(consumer, record.value, producer)
                        if self._stop.is_set():
                            return
        finally:
            consumer.close()
            producer.flush()
            producer.close()

    def _process_keeping_alive(self, consumer: KafkaConsumer, alert: dict, producer: KafkaProducer):
        """
        Lance l'enquête dans un thread de travail et continue d'appeler poll() pendant ce temps, partitions en pause :
        sans cela le consommateur dépasse max.poll.interval.ms, est exclu du groupe et le commit échoue
        (bug observé : enquête de 11 min > 5 min par défaut). Pattern classique "pause / poll / resume".
        """
        assigned = consumer.assignment()
        consumer.pause(*assigned)
        worker = threading.Thread(target=self._handle, args=(alert, producer), name="investigation", daemon=True)
        worker.start()
        while worker.is_alive():
            consumer.poll(timeout_ms=1000)      # ne renvoie rien (partitions en pause) mais maintient l'adhésion au groupe
        consumer.resume(*consumer.assignment())
        try:
            consumer.commit()
        except Exception:  # CommitFailedError après un rebalance : l'alerte sera rejouée, dédoublonnée sur alertId
            log.exception("commit impossible pour %s (rebalance ?) : rejouée au prochain poll", alert.get("alertId"))

    def _handle(self, alert: dict, producer: KafkaProducer):
        alert_id = alert.get("alertId")
        if not alert_id:
            log.warning("alerte sans alertId ignorée: %s", alert)
            return
        if self.store.get_investigation(alert_id):
            log.info("alerte %s déjà enquêtée, ignorée", alert_id)
            return
        log.info("enquête démarrée pour %s (client %s, score %s, raisons %s)", alert_id, alert.get("customerId"),
                 alert.get("riskScore"), alert.get("riskReasons"))
        try:
            report = self.agent.investigate(alert)
            self.store.save_investigation(report.model_dump())
            event_time = datetime.fromisoformat(str(alert["eventTime"]).replace("Z", "+00:00"))
            new_status = {"FRAUD_LIKELY": "CONFIRMED", "LEGITIMATE": "DISMISSED"}.get(report.verdict, "INVESTIGATING")
            self.store.update_alert_status(event_time.date(), event_time, alert_id, new_status)
            producer.send(self.cfg.report_topic, key=report.customer_id.encode(),
                          value=report.model_dump(exclude={"evidence"}))
            self.processed += 1
            self.last_report = report.model_dump(exclude={"evidence"})
            log.info("alerte %s -> %s (%.2f) en %d ms, %d outils", alert_id, report.verdict, report.confidence,
                     report.latency_ms, report.tool_calls)
        except Exception:  # on log et on continue : une alerte en échec ne doit pas bloquer le flux
            self.failed += 1
            log.exception("échec de l'enquête pour %s", alert_id)


def parse_alert_day(alert: dict) -> date:
    return datetime.fromisoformat(str(alert["eventTime"]).replace("Z", "+00:00")).date()
