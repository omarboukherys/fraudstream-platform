"""Tests de l'API FastAPI avec l'état injecté (pas de Cassandra, pas de Kafka, LLM scripté)."""
from fastapi.testclient import TestClient

from fraudstream_agents import api
from fraudstream_agents.agents import AskDataAgent, FraudAnalystAgent
from fraudstream_agents.analytics import DuckDbAnalytics
from fraudstream_agents.tools import build_tools
from tests.conftest import ScriptedChatModel, final_json_msg, tool_call_msg


def _client(cfg, store, lake):
    tools = build_tools(store, DuckDbAnalytics(lake), None, cfg)
    analyst_llm = ScriptedChatModel.build([
        tool_call_msg("get_customer_history", {"customer_id": "C00042"}, "c1"),
        final_json_msg({"verdict": "SUSPICIOUS", "confidence": 0.6, "summary": "Voyage possible mais inhabituel.",
                        "recommended_action": "CONTACT_CUSTOMER", "key_evidence": ["premier achat en FR"]}),
    ])
    ask_llm = ScriptedChatModel.build([
        tool_call_msg("query_analytics", {"sql": "SELECT channel, COUNT(*) n FROM transactions_scored GROUP BY channel"}, "q1"),
        __import__("langchain_core.messages", fromlist=["AIMessage"]).AIMessage(
            content="3 transactions en CARD_PRESENT, 1 en ONLINE.\n```sql\nSELECT channel, COUNT(*) n FROM transactions_scored GROUP BY channel\n```"),
    ])
    api.state.clear()
    api.state.update({
        "store": store,
        "analyst": FraudAnalystAgent(analyst_llm, tools, cfg),
        "ask": AskDataAgent(ask_llm, tools, cfg),
    })
    return TestClient(api.app)   # pas de lifespan : on n'entre pas dans le context manager


def test_investigate_then_fetch(cfg, store, lake, alert):
    c = _client(cfg, store, lake)
    r = c.post("/investigate", json=alert)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["verdict"] == "SUSPICIOUS" and body["tool_calls"] == 1
    r2 = c.get(f"/investigations/{alert['alertId']}")
    assert r2.status_code == 200 and r2.json()["recommended_action"] == "CONTACT_CUSTOMER"
    assert c.get("/investigations/nope").status_code == 404


def test_investigate_requires_ids(cfg, store, lake):
    c = _client(cfg, store, lake)
    assert c.post("/investigate", json={"foo": "bar"}).status_code == 400


def test_ask_data_returns_sql_and_answer(cfg, store, lake):
    c = _client(cfg, store, lake)
    r = c.post("/ask-data", json={"question": "Combien de transactions par canal ?"})
    assert r.status_code == 200
    body = r.json()
    assert "CARD_PRESENT" in body["answer"]
    assert body["sql"].startswith("SELECT channel")
    assert body["evidence"][0]["tool"] == "query_analytics"


def test_health(cfg, store, lake):
    c = _client(cfg, store, lake)
    assert c.get("/health").json()["status"] == "ok"
