"""
Agents LangGraph.

  FraudAnalystAgent : reçoit une alerte, enquête avec ses outils (historique client, profil, entrepôt analytique),
                      rend un verdict structuré + action recommandée. Toutes les preuves (appels d'outils) sont
                      conservées pour l'auditabilité : un analyste humain doit pouvoir rejouer le raisonnement.
  DataOpsAgent      : diagnostique la santé du pipeline (lag Kafka, fraîcheur du lac, volumes) et répond aux
                      questions d'exploitation.
  AskDataAgent      : NL -> SQL sur BigQuery/DuckDB avec garde-fous, pour les questions ad hoc d'un responsable fraude.
"""
from __future__ import annotations

import json
import re
import time
from typing import Any, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.prebuilt import create_react_agent
from pydantic import BaseModel, Field, ValidationError

from .config import Settings, settings as default_settings

# ----------------------------------------------------------------------------- modèles de sortie

class Investigation(BaseModel):
    verdict: Literal["FRAUD_LIKELY", "SUSPICIOUS", "LEGITIMATE"]
    confidence: float = Field(ge=0, le=1)
    summary: str = Field(description="2 à 4 phrases, en français, factuelles, citant les éléments vérifiés")
    recommended_action: Literal["BLOCK_CARD", "CONTACT_CUSTOMER", "MONITOR", "DISMISS"]
    key_evidence: list[str] = Field(default_factory=list)


class InvestigationReport(BaseModel):
    alert_id: str
    transaction_id: str
    customer_id: str
    model: str
    verdict: str
    confidence: float
    summary: str
    recommended_action: str
    key_evidence: list[str]
    evidence: list[dict[str, Any]]      # trace des appels d'outils
    latency_ms: int
    tool_calls: int


# ----------------------------------------------------------------------------- prompts

ANALYST_SYSTEM = """Tu es un analyste senior de la lutte anti-fraude d'une banque marocaine.
Tu reçois une ALERTE produite par un moteur de règles temps réel (Spark) et tu dois rendre un verdict argumenté.

Méthode (ReAct) :
1. Comprends les raisons de l'alerte (outil explain_risk_reasons).
2. Vérifie l'historique récent du client (get_customer_history) et son profil (get_customer_profile) :
   l'alerte est-elle cohérente avec ses habitudes (pays, appareils, montants, canaux) ?
3. Si utile, compare à la population avec query_analytics (ex. montant moyen pour ce pays/canal, taux d'alerte).
4. Conclus. Ne confonds pas "inhabituel" et "frauduleux" : un voyage réel, un nouvel appareil déclaré, un achat
   exceptionnel plausible => SUSPICIOUS + CONTACT_CUSTOMER plutôt que BLOCK_CARD.

Règles : maximum {max_tools} appels d'outils. N'invente aucune donnée : si un outil échoue, dis-le.
Le champ isFraudLabel, s'il apparaît, est une étiquette de simulation : IGNORE-LE, ne t'en sers jamais pour conclure.

Réponse finale OBLIGATOIREMENT au format JSON suivant, sans texte autour :
```json
{{"verdict": "FRAUD_LIKELY|SUSPICIOUS|LEGITIMATE", "confidence": 0.0-1.0, "summary": "...",
  "recommended_action": "BLOCK_CARD|CONTACT_CUSTOMER|MONITOR|DISMISS", "key_evidence": ["...", "..."]}}
```"""

DATA_OPS_SYSTEM = """Tu es l'ingénieur d'astreinte de la plateforme de données FraudStream
(Kafka -> Spark Structured Streaming -> Cassandra / lac Parquet -> Airflow -> BigQuery).
Diagnostique l'état du pipeline avec tes outils (topics, lag des consommateurs, offsets, requêtes analytiques
sur les volumes par event_date) et réponds de façon concise et actionnable, en français.
Groupes de consommateurs connus : "fraud-analyst-agent" (agent IA), le job Spark n'utilise pas de groupe committé.
Topics : transactions.raw (entrée), fraud.alerts (alertes), agent.reports (rapports d'enquête)."""

ASK_DATA_SYSTEM = """Tu es un assistant analytique pour un responsable fraude. Tu réponds à ses questions en
générant UNE requête SQL (SELECT uniquement, dialecte BigQuery) via l'outil query_analytics, puis en interprétant
le résultat en français, chiffres à l'appui. Si la question est ambiguë, choisis l'interprétation la plus utile et
dis laquelle tu as retenue. Termine toujours par la requête SQL utilisée entre balises ```sql```."""


# ----------------------------------------------------------------------------- utilitaires

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```|(\{.*\})", re.S)


def extract_json(text: str) -> dict[str, Any]:
    m = _JSON_RE.search(text)
    if not m:
        raise ValueError("aucun JSON dans la réponse")
    return json.loads(m.group(1) or m.group(2))


def _text(msg: AIMessage) -> str:
    c = msg.content
    if isinstance(c, list):  # Gemini peut renvoyer des blocs
        return "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in c)
    return str(c)


def _trace(messages: list) -> list[dict[str, Any]]:
    """Reconstitue la trace outil -> résultat à partir des messages du graphe."""
    calls = {}
    for m in messages:
        if isinstance(m, AIMessage):
            for tc in m.tool_calls:
                calls[tc["id"]] = {"tool": tc["name"], "args": tc["args"], "result": None}
        elif isinstance(m, ToolMessage) and m.tool_call_id in calls:
            raw = m.content if isinstance(m.content, str) else str(m.content)
            calls[m.tool_call_id]["result"] = raw[:2000]
    return list(calls.values())


# ----------------------------------------------------------------------------- agents

class FraudAnalystAgent:
    def __init__(self, llm: BaseChatModel, tools: list, cfg: Settings = default_settings):
        self.cfg = cfg
        self.model_name = getattr(llm, "model", None) or getattr(llm, "model_name", None) or type(llm).__name__
        self.graph = create_react_agent(llm, tools, prompt=ANALYST_SYSTEM.format(max_tools=cfg.max_tool_calls))

    def investigate(self, alert: dict[str, Any]) -> InvestigationReport:
        t0 = time.perf_counter()
        alert_clean = {k: v for k, v in alert.items() if k != "isFraudLabel"}
        result = self.graph.invoke(
            {"messages": [HumanMessage(content="ALERTE À ANALYSER :\n" + json.dumps(alert_clean, ensure_ascii=False, default=str))]},
            config={"recursion_limit": 2 * self.cfg.max_tool_calls + 4},
        )
        messages = result["messages"]
        final = _text(messages[-1])
        try:
            inv = Investigation.model_validate(extract_json(final))
        except (ValueError, ValidationError) as e:
            # Repli sûr : on ne bloque jamais une carte sur une sortie mal formée
            inv = Investigation(verdict="SUSPICIOUS", confidence=0.3, recommended_action="MONITOR",
                                summary=f"Sortie du modèle non structurée ({e}). Réponse brute : {final[:300]}",
                                key_evidence=[])
        trace = _trace(messages)
        return InvestigationReport(
            alert_id=alert["alertId"], transaction_id=alert["transactionId"], customer_id=alert["customerId"],
            model=str(self.model_name), verdict=inv.verdict, confidence=inv.confidence, summary=inv.summary,
            recommended_action=inv.recommended_action, key_evidence=inv.key_evidence, evidence=trace,
            latency_ms=int((time.perf_counter() - t0) * 1000), tool_calls=len(trace),
        )


class DataOpsAgent:
    def __init__(self, llm: BaseChatModel, tools: list, cfg: Settings = default_settings):
        self.graph = create_react_agent(llm, tools, prompt=DATA_OPS_SYSTEM)
        self.cfg = cfg

    def ask(self, question: str) -> dict[str, Any]:
        result = self.graph.invoke({"messages": [HumanMessage(content=question)]},
                                   config={"recursion_limit": 2 * self.cfg.max_tool_calls + 4})
        return {"answer": _text(result["messages"][-1]), "evidence": _trace(result["messages"])}


class AskDataAgent:
    def __init__(self, llm: BaseChatModel, tools: list, cfg: Settings = default_settings):
        query_tools = [t for t in tools if t.name == "query_analytics"]
        self.graph = create_react_agent(llm, query_tools, prompt=ASK_DATA_SYSTEM)
        self.cfg = cfg

    def ask(self, question: str) -> dict[str, Any]:
        result = self.graph.invoke({"messages": [HumanMessage(content=question)]},
                                   config={"recursion_limit": 10})
        trace = _trace(result["messages"])
        sql = next((t["args"].get("sql") for t in reversed(trace) if t["tool"] == "query_analytics"), None)
        return {"answer": _text(result["messages"][-1]), "sql": sql, "evidence": trace}
