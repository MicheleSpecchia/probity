from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.gamma_catalog_repository import GammaCatalogRepository
from pmx.ingest.gamma_catalog import normalize_market_payload
from tests.db_helpers import alembic_upgrade_head


def test_gamma_catalog_upsert_is_idempotent() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    token_id = f"t-{uuid4().hex[:12]}"
    now = datetime(2026, 2, 10, 0, 0, tzinfo=UTC)

    payload_v1 = {
        "id": market_id,
        "title": "First title",
        "slug": "first-title",
        "status": "active",
        "rule_text": "Resolves YES if event occurs.",
        "updatedAt": "2026-02-10T00:00:00Z",
        "tokens": [{"outcome": "YES", "tokenId": token_id}],
    }
    payload_v2 = {
        "id": market_id,
        "title": "Updated title",
        "slug": "updated-title",
        "status": "closed",
        "rule_text": "Resolves NO if event does not occur.",
        "updatedAt": "2026-02-10T01:00:00Z",
        "tokens": [{"outcome": "YES", "tokenId": token_id}],
    }

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        repository = GammaCatalogRepository(connection)

        market_v1, tokens_v1 = normalize_market_payload(payload_v1, ingested_at=now)
        assert market_v1 is not None
        repository.upsert_market(market_v1)
        for token in tokens_v1:
            conflict = repository.upsert_market_token(token)
            assert conflict is None

        market_v2, tokens_v2 = normalize_market_payload(payload_v2, ingested_at=now)
        assert market_v2 is not None
        repository.upsert_market(market_v2)
        for token in tokens_v2:
            conflict = repository.upsert_market_token(token)
            assert conflict is None

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT title, slug, status, rule_parse_ok
                FROM markets
                WHERE market_id = %s
                """,
                (market_id,),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Updated title"
            assert row[1] == "updated-title"
            assert row[2] == "closed"
            assert row[3] is False

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM market_tokens
                WHERE market_id = %s
                """,
                (market_id,),
            )
            token_count = cursor.fetchone()[0]
            assert token_count == 1


def test_gamma_catalog_token_conflict_keeps_existing_mapping() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    shared_token_id = f"t-{uuid4().hex[:12]}"
    market_a_id = f"m-{uuid4().hex[:12]}"
    market_b_id = f"m-{uuid4().hex[:12]}"
    now = datetime(2026, 2, 10, 0, 0, tzinfo=UTC)

    market_a_payload = {
        "id": market_a_id,
        "title": "Market A",
        "status": "active",
        "tokens": [{"outcome": "YES", "tokenId": shared_token_id}],
    }
    market_b_payload = {
        "id": market_b_id,
        "title": "Market B",
        "status": "active",
        "tokens": [{"outcome": "YES", "tokenId": shared_token_id}],
    }

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        repository = GammaCatalogRepository(connection)

        market_a, tokens_a = normalize_market_payload(market_a_payload, ingested_at=now)
        assert market_a is not None
        repository.upsert_market(market_a)
        for token in tokens_a:
            conflict = repository.upsert_market_token(token)
            assert conflict is None

        market_b, tokens_b = normalize_market_payload(market_b_payload, ingested_at=now)
        assert market_b is not None
        repository.upsert_market(market_b)
        assert len(tokens_b) == 1
        conflict = repository.upsert_market_token(tokens_b[0])

        assert conflict is not None
        assert conflict.token_id == shared_token_id
        assert conflict.existing_market_id == market_a_id
        assert conflict.action == "kept_existing"

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT market_id, outcome, token_id
                FROM market_tokens
                WHERE token_id = %s
                """,
                (shared_token_id,),
            )
            rows = cursor.fetchall()

        assert rows == [(market_a_id, "YES", shared_token_id)]
