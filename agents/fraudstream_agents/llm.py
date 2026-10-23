"""Fabrique du modèle : Gemini via AI Studio (clé API, gratuit) ou via Vertex AI (compte de service GCP)."""
from __future__ import annotations

from langchain_core.language_models import BaseChatModel

from .config import Settings, settings as default_settings


def build_llm(cfg: Settings = default_settings) -> BaseChatModel:
    if cfg.use_vertex:
        from langchain_google_vertexai import ChatVertexAI  # dépendance optionnelle [vertex]

        return ChatVertexAI(model_name=cfg.gemini_model, project=cfg.gcp_project_id,
                            location=cfg.gcp_location, temperature=cfg.llm_temperature)
    if not cfg.google_api_key:
        raise RuntimeError("GOOGLE_API_KEY manquant (ou USE_VERTEX=true avec un compte de service).")
    from langchain_google_genai import ChatGoogleGenerativeAI

    return ChatGoogleGenerativeAI(model=cfg.gemini_model, google_api_key=cfg.google_api_key,
                                  temperature=cfg.llm_temperature)
