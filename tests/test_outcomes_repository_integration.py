from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.outcomes_repository import MarketOutcomeRecord, OutcomesRepository
from tests.db_helpers import alembic_upgrade_head


def test_outcomes_repository_upsert_is_idempotent() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    first_ingested_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
    second_ingested_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
    first_resolved_ts = datetime(2026, 2, 12, 8, 30, tzinfo=UTC)
    second_resolved_ts = datetime(2026, 2, 12, 9, 30, tzinfo=UTC)

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "Outcome repo test market", "resolved", False),
            )

        repository = OutcomesRepository(connection)
        repository.upsert_market_outcome(
            MarketOutcomeRecord(
                market_id=market_id,
                resolved=True,
                outcome="YES",
                resolved_ts=first_resolved_ts,
                resolver_source="explicit:winningOutcome",
                ingested_at=first_ingested_at,
            )
        )
        repository.upsert_market_outcome(
            MarketOutcomeRecord(
                market_id=market_id,
                resolved=True,
                outcome="NO",
                resolved_ts=second_resolved_ts,
                resolver_source="inferred:outcomePrices_unique_max_ge_0.99",
                ingested_at=second_ingested_at,
            )
        )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT resolved, outcome, resolved_ts, resolver_source, ingested_at
                FROM market_outcomes
                WHERE market_id = %s
                """,
                (market_id,),
            )
            row = cursor.fetchone()

    assert row is not None
    assert row[0] is True
    assert row[1] == "NO"
    assert row[2] == second_resolved_ts
    assert row[3] == "inferred:outcomePrices_unique_max_ge_0.99"
    assert row[4] == second_ingested_at
