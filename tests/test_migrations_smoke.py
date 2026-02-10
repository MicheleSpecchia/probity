from __future__ import annotations

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url
from tests.db_helpers import alembic_upgrade_head, to_psycopg_dsn

REQUIRED_TABLES = {
    "articles",
    "candles",
    "claims",
    "feature_snapshots",
    "forecasts",
    "markets",
    "orderbook_snapshots",
    "trades",
}

REQUIRED_INDEXES = {
    "articles_pub_idx",
    "markets_status_idx",
    "trades_token_event_idx",
}

REQUIRED_PARTITIONS = {
    "candles_p_default",
    "orderbook_snapshots_p_default",
    "trades_p_default",
}


def test_migrations_smoke_upgrade_head_creates_expected_objects() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.relname
                FROM pg_class AS c
                JOIN pg_namespace AS n
                  ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                  AND c.relname = ANY(%s)
                """,
                (sorted(REQUIRED_TABLES),),
            )
            found_tables = {row[0] for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = ANY(%s)
                """,
                (sorted(REQUIRED_INDEXES),),
            )
            found_indexes = {row[0] for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT relname
                FROM pg_class
                WHERE relkind = 'r'
                  AND relname = ANY(%s)
                """,
                (sorted(REQUIRED_PARTITIONS),),
            )
            found_partitions = {row[0] for row in cursor.fetchall()}

    assert REQUIRED_TABLES.issubset(found_tables)
    assert REQUIRED_INDEXES.issubset(found_indexes)
    assert REQUIRED_PARTITIONS.issubset(found_partitions)
