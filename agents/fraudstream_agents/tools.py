"""Outils exposés aux agents (LangChain StructuredTool). Chaque outil est une fermeture sur ses dépendances."""
from __future__ import annotations

import functools
import json
import logging
import time
from typing import Any

from langchain_core.tools import StructuredTool

log = logging.getLogger(__name__)

from .analytics import SCHEMA_DOC, SqlGuardError, strip_code_fences
from .config import Settings, settings as default_settings

RULE_EXPLANATIONS = {
    "VELOCITY_SMALL_AMOUNTS": "Rafale de petits paiements en ligne : typique d'un test de carte volée avant un gros achat.",
    "HIGH_VELOCITY": "Nombre de transactions anormalement élevé sur 10 minutes.",
    "IMPOSSIBLE_TRAVEL": "Deux transactions physiques dans deux pays différents en un temps incompatible avec un déplacement réel.",
    "AMOUNT_ANOMALY": "Montant très supérieur à la moyenne historique du client.",
    "NEW_DEVICE": "Appareil jamais vu (ou vu depuis < 24 h) pour ce client.",
    "HIGH_RISK_MCC": "Catégorie de marchand à risque élevé (crypto, jeux d'argent).",
    "FOREIGN_COUNTRY": "Transaction hors du pays habituel du client.",
}


def _dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _timed(fn):
    """Journalise la durée et la taille de chaque appel d'outil : indispensable pour localiser une enquête lente."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            out = fn(*args, **kwargs)
            return out
        finally:
            ms = int((time.perf_counter() - t0) * 1000)
            size = len(out) if "out" in locals() and isinstance(out, str) else -1
            log.info("outil %s : %d ms, %d caractères, args=%s", fn.__name__, ms, size, _dump(kwargs or args)[:200])
    return wrapper


def build_tools(store, analytics, kafka_ops=None, cfg: Settings = default_settings) -> list[StructuredTool]:
    def get_customer_history(customer_id: str, limit: int = 20) -> str:
        """Dernières transactions du client (les plus récentes d'abord) avec score et raisons de risque."""
        return _dump(store.customer_history(customer_id, min(limit, cfg.history_limit)))

    def get_customer_profile(customer_id: str) -> str:
        """Profil courant du client : agrégats sur 10 min, dernier pays et dernier appareil vus."""
        return _dump(store.customer_profile(customer_id) or {"note": "profil inconnu"})

    def explain_risk_reasons(reasons: list[str]) -> str:
        """Explique en langage métier les codes de règles présents dans riskReasons (ex. IMPOSSIBLE_TRAVEL(MA -> FR in 240s))."""
        out = []
        for r in reasons:
            code = r.split("(")[0]
            out.append({"rule": r, "meaning": RULE_EXPLANATIONS.get(code, "règle inconnue")})
        return _dump(out)

    def query_analytics(sql: str) -> str:
        """Exécute une requête SELECT (SQL standard BigQuery) sur l'entrepôt analytique. Lecture seule, LIMIT forcé.
        Utile pour comparer le client à la population (ex. montant moyen par pays, taux d'alerte par canal)."""
        try:
            res = analytics.query(strip_code_fences(sql), cfg.sql_row_limit)
        except SqlGuardError as e:
            return _dump({"error": f"requête refusée : {e}"})
        except Exception as e:  # erreur moteur : on la renvoie au LLM pour qu'il corrige sa requête
            return _dump({"error": str(e)[:500]})
        return _dump({"sql": res.sql, "row_count": res.row_count, "truncated": res.truncated,
                      "bytes_processed": res.bytes_processed, "rows": res.rows[:cfg.sql_row_limit]})

    query_analytics.__doc__ += "\n\n" + SCHEMA_DOC

    tools = [
        StructuredTool.from_function(_timed(get_customer_history)),
        StructuredTool.from_function(_timed(get_customer_profile)),
        StructuredTool.from_function(_timed(explain_risk_reasons)),
        StructuredTool.from_function(_timed(query_analytics)),
    ]

    if kafka_ops is not None:
        def list_topics() -> str:
            """Liste les topics Kafka de la plateforme."""
            return _dump(kafka_ops.topics())

        def get_consumer_lag(group_id: str) -> str:
            """Lag (messages non encore traités) d'un groupe de consommateurs Kafka, par partition."""
            return _dump(kafka_ops.consumer_lag(group_id))

        def get_topic_offsets(topic: str) -> str:
            """Nombre de messages et offsets de fin d'un topic Kafka."""
            return _dump(kafka_ops.topic_offsets(topic))

        tools += [StructuredTool.from_function(_timed(list_topics)), StructuredTool.from_function(_timed(get_consumer_lag)),
                  StructuredTool.from_function(_timed(get_topic_offsets))]
    return tools
