"""
API FastAPI des agents.

  POST /investigate            enquête à la demande sur une alerte (corps = message fraud.alerts)
  GET  /investigations/{id}    rapport stocké
  POST /ask-data               question en langage naturel -> SQL -> réponse
  POST /ops                    question d'exploitation (lag, volumes, santé)
  GET  /alerts/today           alertes du jour (Cassandra)
  GET  /health, /metrics       supervision

Lancer : uvicorn fraudstream_agents.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .agents import AskDataAgent, DataOpsAgent, FraudAnalystAgent, InvestigationReport
from .analytics import build_analytics
from .config import settings
from .consumer import AlertConsumer
from .kafka_ops import KafkaOps
from .llm import build_llm
from .store import CassandraStore
from .tools import build_tools

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("fraudstream.api")

state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    llm = build_llm(settings)
    store = CassandraStore(settings)
    analytics = build_analytics(settings)
    kafka_ops = KafkaOps(settings)
    tools = build_tools(store, analytics, kafka_ops, settings)
    state["store"] = store
    state["analyst"] = FraudAnalystAgent(llm, [t for t in tools if not t.name.startswith(("list_topics", "get_consumer", "get_topic"))], settings)
    state["ops"] = DataOpsAgent(llm, tools, settings)
    state["ask"] = AskDataAgent(llm, tools, settings)
    state["started_at"] = datetime.now(timezone.utc)
    if settings.alert_consumer_enabled:
        consumer = AlertConsumer(state["analyst"], store, settings)
        consumer.start()
        state["consumer"] = consumer
    log.info("agents prêts (modèle=%s, bigquery=%s)", settings.gemini_model, settings.use_bigquery)
    yield
    if "consumer" in state:
        state["consumer"].stop()


app = FastAPI(title="FraudStream Agents", version="0.1.0", lifespan=lifespan)


class Question(BaseModel):
    question: str


@app.get("/health")
def health():
    return {"status": "ok", "model": settings.gemini_model, "bigquery": settings.use_bigquery,
            "consumer": settings.alert_consumer_enabled}


@app.get("/metrics")
def metrics():
    c: AlertConsumer | None = state.get("consumer")
    return {"started_at": state.get("started_at"), "alerts_processed": c.processed if c else 0,
            "alerts_failed": c.failed if c else 0, "last_report": c.last_report if c else None}


@app.post("/investigate", response_model=InvestigationReport)
def investigate(alert: dict[str, Any]):
    for k in ("alertId", "transactionId", "customerId"):
        if k not in alert:
            raise HTTPException(400, f"champ manquant : {k}")
    report = state["analyst"].investigate(alert)
    state["store"].save_investigation(report.model_dump())
    return report


@app.get("/investigations/{alert_id}")
def get_investigation(alert_id: str):
    inv = state["store"].get_investigation(alert_id)
    if not inv:
        raise HTTPException(404, "enquête inconnue")
    return inv


@app.get("/alerts/today")
def alerts_today(limit: int = 50):
    return state["store"].alerts_of_day(date.today(), limit)


@app.post("/ask-data")
def ask_data(q: Question):
    return state["ask"].ask(q.question)


@app.post("/ops")
def ops(q: Question):
    return state["ops"].ask(q.question)
