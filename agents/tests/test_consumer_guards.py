"""Garde-fous quota du consommateur d'alertes (sans Kafka)."""
from fraudstream_agents.consumer import AlertConsumer


def test_retry_delay_parsing():
    err = Exception("429 RESOURCE_EXHAUSTED ... 'Please retry in 17h36m17.27s' ... 'retryDelay': '63377s'")
    assert AlertConsumer._retry_delay_from_error(err, 900) == 63377
    assert AlertConsumer._retry_delay_from_error(Exception("Please retry in 2m5s"), 900) == 125
    assert AlertConsumer._retry_delay_from_error(Exception("boom"), 900) == 900


def test_budget_exhaustion_flag(cfg, store):
    cfg2 = cfg.model_copy(update={"max_investigations_per_day": 2})
    c = AlertConsumer(agent=None, store=store, cfg=cfg2)
    assert c._budget_exhausted() is False
    c.investigations_today = 2
    assert c._budget_exhausted() is True
    assert c._budget_exhausted() is True   # idempotent, journalise une seule fois
