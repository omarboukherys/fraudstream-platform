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
import re
import threading
import time
from datetime import date, datetime, timezone

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
        self.skipped_budget = 0
        self.last_report: dict | None = None
        # Garde-fous quota LLM (palier gratuit Gemini : N requêtes/jour/modèle)
        self._budget_day: date = datetime.now(timezone.utc).date()
        self.investigations_today = 0
        self.paused_until: float = 0.0          # epoch ; > now => on ne consomme pas (429 reçu)

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
            assigned_paused = False
            while not self._stop.is_set():
                # Pause quota : on garde l'adhésion au groupe (poll) mais sans consommer (partitions en pause)
                if time.time() < self.paused_until or self._budget_exhausted():
                    if not assigned_paused and consumer.assignment():
                        consumer.pause(*consumer.assignment())
                        assigned_paused = True
                    consumer.poll(timeout_ms=1000)
                    continue
                if assigned_paused:
                    consumer.resume(*consumer.assignment())
                    assigned_paused = False
                    log.info("reprise de la consommation des alertes")
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

    def _budget_exhausted(self) -> bool:
        today = datetime.now(timezone.utc).date()
        if today != self._budget_day:                 # nouveau jour : compteur remis à zéro
            self._budget_day, self.investigations_today = today, 0
        if self.investigations_today >= self.cfg.max_investigations_per_day:
            if self.investigations_today == self.cfg.max_investigations_per_day:
                log.warning("budget quotidien atteint (%d enquêtes) : alertes laissées dans Kafka jusqu'à demain",
                            self.cfg.max_investigations_per_day)
                self.investigations_today += 1        # pour ne journaliser qu'une fois
            return True
        return False

    @staticmethod
    def _retry_delay_from_error(exc: Exception, default_s: int) -> int:
        """Extrait 'retryDelay': '63377s' ou 'Please retry in 17h36m17s' d'une erreur 429 Google."""
        msg = str(exc)
        m = re.search(r"retryDelay['\"]?:\s*['\"]?(\d+)s", msg)
        if m:
            return int(m.group(1))
        m = re.search(r"retry in (?:(\d+)h)?(?:(\d+)m)?(?:([\d.]+)s)?", msg)
        if m and any(m.groups()):
            h, mi, se = (int(m.group(1) or 0), int(m.group(2) or 0), float(m.group(3) or 0))
            return int(h * 3600 + mi * 60 + se) or default_s
        return default_s

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
            self.investigations_today += 1
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
        except Exception as exc:  # on log et on continue : une alerte en échec ne doit pas bloquer le flux
            self.failed += 1
            if "429" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc) or "RateLimit" in type(exc).__name__:
                delay = min(self._retry_delay_from_error(exc, self.cfg.rate_limit_pause_s), 24 * 3600)
                self.paused_until = time.time() + delay
                log.warning("quota LLM dépassé (429) : consommation des alertes en pause %d s", delay)
            log.exception("échec de l'enquête pour %s", alert_id)


def parse_alert_day(alert: dict) -> date:
    return datetime.fromisoformat(str(alert["eventTime"]).replace("Z", "+00:00")).date()
