"""Tests de l'agent analyste avec un LLM scripté : on vérifie l'orchestration outils -> verdict -> traçabilité."""
import json

from fraudstream_agents.agents import FraudAnalystAgent, extract_json
from fraudstream_agents.analytics import DuckDbAnalytics
from fraudstream_agents.tools import build_tools
from tests.conftest import ScriptedChatModel, final_json_msg, tool_call_msg


def test_investigation_uses_tools_and_returns_structured_verdict(cfg, store, lake, alert):
    llm = ScriptedChatModel.build([
        tool_call_msg("explain_risk_reasons", {"reasons": alert["riskReasons"]}, "c1"),
        tool_call_msg("get_customer_history", {"customer_id": "C00042", "limit": 10}, "c2"),
        tool_call_msg("query_analytics", {"sql": "SELECT country, AVG(amount) AS avg_amount FROM transactions_scored GROUP BY country"}, "c3"),
        final_json_msg({"verdict": "FRAUD_LIKELY", "confidence": 0.86,
                        "summary": "Achat de 2400 MAD en France 4 minutes après un achat physique au Maroc, sans historique à l'étranger.",
                        "recommended_action": "BLOCK_CARD", "key_evidence": ["IMPOSSIBLE_TRAVEL", "montant 16x la moyenne"]}),
    ])
    tools = build_tools(store, DuckDbAnalytics(lake), None, cfg)
    report = FraudAnalystAgent(llm, tools, cfg).investigate(alert)

    assert report.verdict == "FRAUD_LIKELY"
    assert report.recommended_action == "BLOCK_CARD"
    assert report.confidence == 0.86
    assert report.tool_calls == 3
    tools_used = [e["tool"] for e in report.evidence]
    assert tools_used == ["explain_risk_reasons", "get_customer_history", "query_analytics"]
    # les résultats d'outils sont tracés (auditabilité)
    history = json.loads(report.evidence[1]["result"])
    assert history[0]["transaction_id"] == "t9"
    analytics = json.loads(report.evidence[2]["result"])
    assert analytics["row_count"] == 2 and "LIMIT 50" in analytics["sql"]
    assert report.latency_ms >= 0


def test_isfraudlabel_is_stripped_before_the_llm_sees_it(cfg, store, lake, alert):
    captured = {}

    class Spy(ScriptedChatModel.build([final_json_msg({"verdict": "LEGITIMATE", "confidence": 0.5, "summary": "ok",
                                                      "recommended_action": "DISMISS"})]).__class__):
        def _generate(self, messages, **kw):
            captured["prompt"] = "".join(str(m.content) for m in messages)
            return super()._generate(messages, **kw)

    llm = Spy(responses=[final_json_msg({"verdict": "LEGITIMATE", "confidence": 0.5, "summary": "ok",
                                        "recommended_action": "DISMISS"})])
    tools = build_tools(store, DuckDbAnalytics(lake), None, cfg)
    FraudAnalystAgent(llm, tools, cfg).investigate({**alert, "isFraudLabel": True})
    assert '"isFraudLabel": true' not in captured["prompt"]
    assert "alert-t9" in captured["prompt"]


def test_malformed_llm_output_falls_back_to_safe_verdict(cfg, store, lake, alert):
    from langchain_core.messages import AIMessage

    llm = ScriptedChatModel.build([AIMessage(content="Je pense que c'est louche mais je ne sais pas formater.")])
    tools = build_tools(store, DuckDbAnalytics(lake), None, cfg)
    report = FraudAnalystAgent(llm, tools, cfg).investigate(alert)
    assert report.verdict == "SUSPICIOUS"
    assert report.recommended_action == "MONITOR"   # jamais BLOCK_CARD sur une sortie illisible
    assert "non structurée" in report.summary


def test_extract_json_handles_fences_and_raw():
    assert extract_json('```json\n{"a": 1}\n```')["a"] == 1
    assert extract_json('blabla {"b": [1,2]} fin')["b"] == [1, 2]
