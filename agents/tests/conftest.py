import json
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage

from fraudstream_agents.config import Settings
from fraudstream_agents.store import InMemoryStore


class ScriptedChatModel:
    """Factory de FakeMessagesListChatModel avec support bind_tools (requis par create_react_agent)."""

    @staticmethod
    def build(responses: list[AIMessage]):
        from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

        class _Fake(FakeMessagesListChatModel):
            def bind_tools(self, tools, **kwargs):
                return self

        return _Fake(responses=responses)


@pytest.fixture
def cfg(tmp_path):
    return Settings(google_api_key="fake", lake_path=str(tmp_path / "lake"), max_tool_calls=4, sql_row_limit=50,
                    _env_file=None)


@pytest.fixture
def store():
    return InMemoryStore(
        history={"C00042": [
            {"transaction_id": "t9", "event_time": "2026-10-23T10:04:00+00:00", "amount": 2400.0, "country": "FR",
             "channel": "CARD_PRESENT", "device_id": "dev-a1", "risk_score": 0.75,
             "risk_reasons": ["IMPOSSIBLE_TRAVEL(MA -> FR in 240s)", "FOREIGN_COUNTRY(FR)"]},
            {"transaction_id": "t8", "event_time": "2026-10-23T10:00:00+00:00", "amount": 150.0, "country": "MA",
             "channel": "CARD_PRESENT", "device_id": "dev-a1", "risk_score": 0.0, "risk_reasons": []},
        ]},
        profiles={"C00042": {"customer_id": "C00042", "tx_count_10m": 2, "last_country": "FR", "last_device_id": "dev-a1"}},
    )


@pytest.fixture
def lake(tmp_path):
    """Mini-lac Parquet pour DuckDB."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    d = tmp_path / "lake" / "transactions_scored" / "event_date=2026-10-23"
    d.mkdir(parents=True)
    table = pa.table({
        "transactionId": ["t1", "t2", "t3", "t4"],
        "customerId": ["C1", "C2", "C00042", "C00042"],
        "amount": [100.0, 20.0, 150.0, 2400.0],
        "country": ["MA", "MA", "MA", "FR"],
        "channel": ["CARD_PRESENT", "ONLINE", "CARD_PRESENT", "CARD_PRESENT"],
        "riskScore": [0.1, 0.2, 0.0, 0.75],
        "isAlert": [False, False, False, True],
    })
    pq.write_table(table, d / "part-0.parquet")
    return str(tmp_path / "lake")


@pytest.fixture
def alert():
    return {
        "alertId": "alert-t9", "transactionId": "t9", "customerId": "C00042", "eventTime": "2026-10-23T10:04:00.000Z",
        "amount": 2400.0, "currency": "MAD", "country": "FR", "channel": "CARD_PRESENT", "merchantCategory": "ELECTRONICS",
        "riskScore": 0.75, "riskReasons": ["IMPOSSIBLE_TRAVEL(MA -> FR in 240s)", "FOREIGN_COUNTRY(FR)"], "status": "OPEN",
    }


def tool_call_msg(name: str, args: dict, call_id: str) -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id, "type": "tool_call"}])


def final_json_msg(payload: dict) -> AIMessage:
    return AIMessage(content="Voici mon analyse.\n```json\n" + json.dumps(payload, ensure_ascii=False) + "\n```")
