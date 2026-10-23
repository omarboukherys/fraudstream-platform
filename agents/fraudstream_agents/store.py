"""Accès Cassandra (couche serving). Une interface abstraite permet de substituer un store en mémoire dans les tests."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Protocol

from .config import Settings, settings as default_settings


class Store(Protocol):
    def customer_history(self, customer_id: str, limit: int = 20) -> list[dict[str, Any]]: ...
    def customer_profile(self, customer_id: str) -> dict[str, Any] | None: ...
    def alerts_of_day(self, day: date, limit: int = 50) -> list[dict[str, Any]]: ...
    def save_investigation(self, inv: dict[str, Any]) -> None: ...
    def get_investigation(self, alert_id: str) -> dict[str, Any] | None: ...
    def update_alert_status(self, day: date, event_time: datetime, alert_id: str, status: str) -> None: ...


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            out[k] = float(v)  # Decimal
        else:
            out[k] = v
    return out


class CassandraStore:
    def __init__(self, cfg: Settings = default_settings):
        from cassandra.cluster import Cluster
        from cassandra.query import dict_factory

        self._cluster = Cluster([cfg.cassandra_host])
        self._session = self._cluster.connect(cfg.cassandra_keyspace)
        self._session.row_factory = dict_factory
        self._ps = {
            "history": self._session.prepare(
                "SELECT * FROM transactions_by_customer WHERE customer_id = ? LIMIT ?"),
            "profile": self._session.prepare("SELECT * FROM customer_profile WHERE customer_id = ?"),
            "alerts": self._session.prepare("SELECT * FROM alerts_by_day WHERE alert_day = ? LIMIT ?"),
            "inv_get": self._session.prepare("SELECT * FROM investigations WHERE alert_id = ?"),
            "inv_put": self._session.prepare(
                "INSERT INTO investigations (alert_id, transaction_id, customer_id, created_at, model, verdict, "
                "confidence, summary, recommended_action, evidence, latency_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
            "alert_status": self._session.prepare(
                "UPDATE alerts_by_day SET status = ? WHERE alert_day = ? AND event_time = ? AND alert_id = ?"),
        }

    def customer_history(self, customer_id: str, limit: int = 20) -> list[dict[str, Any]]:
        return [_jsonable(r) for r in self._session.execute(self._ps["history"], [customer_id, limit])]

    def customer_profile(self, customer_id: str) -> dict[str, Any] | None:
        row = self._session.execute(self._ps["profile"], [customer_id]).one()
        return _jsonable(row) if row else None

    def alerts_of_day(self, day: date, limit: int = 50) -> list[dict[str, Any]]:
        return [_jsonable(r) for r in self._session.execute(self._ps["alerts"], [day, limit])]

    def save_investigation(self, inv: dict[str, Any]) -> None:
        self._session.execute(self._ps["inv_put"], [
            inv["alert_id"], inv["transaction_id"], inv["customer_id"],
            datetime.now(timezone.utc), inv["model"], inv["verdict"], float(inv["confidence"]),
            inv["summary"], inv["recommended_action"], json.dumps(inv.get("evidence", []), ensure_ascii=False),
            int(inv["latency_ms"]),
        ])

    def get_investigation(self, alert_id: str) -> dict[str, Any] | None:
        row = self._session.execute(self._ps["inv_get"], [alert_id]).one()
        return _jsonable(row) if row else None

    def update_alert_status(self, day: date, event_time: datetime, alert_id: str, status: str) -> None:
        self._session.execute(self._ps["alert_status"], [status, day, event_time, alert_id])


class InMemoryStore:
    """Store de test / démo sans Cassandra."""

    def __init__(self, history: dict[str, list[dict]] | None = None, profiles: dict[str, dict] | None = None):
        self.history = history or {}
        self.profiles = profiles or {}
        self.investigations: dict[str, dict] = {}
        self.alert_status: dict[str, str] = {}

    def customer_history(self, customer_id, limit=20):
        return self.history.get(customer_id, [])[:limit]

    def customer_profile(self, customer_id):
        return self.profiles.get(customer_id)

    def alerts_of_day(self, day, limit=50):
        return []

    def save_investigation(self, inv):
        self.investigations[inv["alert_id"]] = inv

    def get_investigation(self, alert_id):
        return self.investigations.get(alert_id)

    def update_alert_status(self, day, event_time, alert_id, status):
        self.alert_status[alert_id] = status
