"""Garde-fous SQL : le LLM ne peut ni écrire, ni sortir des tables autorisées, ni ramener des résultats illimités."""
import pytest

from fraudstream_agents.analytics import DuckDbAnalytics, SqlGuardError, guard_sql, strip_code_fences


def test_select_is_normalised_with_limit():
    sql = guard_sql("SELECT customerId, amount FROM transactions_scored WHERE isAlert", row_limit=100)
    assert sql.upper().startswith("SELECT")
    assert "LIMIT 100" in sql


def test_existing_higher_limit_is_lowered():
    sql = guard_sql("SELECT * FROM transactions_scored LIMIT 5000", row_limit=200)
    assert "LIMIT 200" in sql


def test_existing_lower_limit_is_kept():
    sql = guard_sql("SELECT * FROM transactions_scored LIMIT 5", row_limit=200)
    assert "LIMIT 5" in sql


@pytest.mark.parametrize("bad", [
    "DELETE FROM transactions_scored",
    "DROP TABLE transactions_scored",
    "INSERT INTO transactions_scored SELECT * FROM transactions_scored",
    "SELECT * FROM transactions_scored; DROP TABLE transactions_scored",
    "SELECT * FROM customers_pii",
    "UPDATE transactions_scored SET amount = 0",
])
def test_writes_and_unknown_tables_are_refused(bad):
    with pytest.raises(SqlGuardError):
        guard_sql(bad, row_limit=10)


def test_ctes_on_allowed_tables_are_accepted():
    sql = guard_sql("WITH a AS (SELECT country, COUNT(*) n FROM transactions_scored GROUP BY country) SELECT * FROM a", 10)
    assert "WITH a AS" in sql


def test_duckdb_runs_bigquery_dialect_over_parquet(lake):
    res = DuckDbAnalytics(lake).query(
        "SELECT country, COUNT(*) AS n, SUM(CAST(isAlert AS INT64)) AS alerts FROM transactions_scored "
        "WHERE event_date = '2026-10-23' GROUP BY country ORDER BY n DESC", row_limit=10)
    by_country = {r["country"]: r for r in res.rows}
    assert by_country["MA"]["n"] == 3 and by_country["FR"]["alerts"] == 1
    assert res.truncated is False


def test_strip_code_fences():
    assert strip_code_fences("```sql\nSELECT 1\n```") == "SELECT 1"
    assert strip_code_fences("SELECT 2") == "SELECT 2"
