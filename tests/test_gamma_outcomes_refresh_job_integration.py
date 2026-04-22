from __future__ import annotations

from typing import Any
from uuid import uuid4

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.jobs.gamma_outcomes_refresh import GammaOutcomesRefreshConfig, run_gamma_outcomes_refresh
from tests.db_helpers import alembic_upgrade_head


class _FakeGammaClient:
    market_id: str = ""

    def __init__(self, config: Any) -> None:
        self.config = config

    def iter_markets(self, **_: Any) -> list[dict[str, Any]]:
        return [
            {
                "id": self.market_id,
                "title": "Integration outcome market",
                "outcomes": '["Yes","No"]',
                "outcomePrices": '["1.00","0.00"]',
                "resolvedAt": "2026-02-12T10:00:00Z",
            }
        ]


def test_gamma_outcomes_refresh_job_populates_market_outcomes(monkeypatch: Any) -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    _FakeGammaClient.market_id = market_id

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "Integration outcome market", "resolved", False),
            )

    monkeypatch.setattr("pmx.jobs.gamma_outcomes_refresh.GammaClient", _FakeGammaClient)
    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.get_database_url",
        lambda: database_url,
    )

    stats = run_gamma_outcomes_refresh(
        config=GammaOutcomesRefreshConfig(
            gamma_base_url="https://gamma-api.polymarket.com",
            gamma_timeout_seconds=20,
            gamma_page_size=200,
            ingest_epsilon_seconds=300,
        ),
        max_markets=10,
        only_resolved=True,
    )

    assert stats["errors"] == 0
    assert stats["outcomes_upserted"] == 1
    assert stats["resolved_upserted"] == 1

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM market_outcomes
                WHERE market_id = %s
                """,
                (market_id,),
            )
            count = cursor.fetchone()[0]

    assert count == 1
