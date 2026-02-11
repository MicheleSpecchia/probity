from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from pmx.db.clob_repository import ClobRepository
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.ingest.clob_client import CandleRecord, OrderbookSnapshot, TradeRecord
from tests.db_helpers import alembic_upgrade_head


def test_clob_repository_upserts_are_idempotent() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    token_id = f"t-{uuid4().hex[:12]}"
    now = datetime(2026, 2, 10, 0, 0, tzinfo=UTC)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = ClobRepository(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "CLOB Test Market", "active", False),
            )
            cursor.execute(
                """
                INSERT INTO market_tokens (market_id, outcome, token_id)
                VALUES (%s, %s, %s)
                """,
                (market_id, "YES", token_id),
            )

        snapshot = OrderbookSnapshot(
            token_id=token_id,
            event_ts=datetime(2026, 2, 10, 0, 0, tzinfo=UTC),
            bids=[{"price": "0.50000000", "size": "10.00000000"}],
            asks=[{"price": "0.51000000", "size": "10.00000000"}],
            mid=Decimal("0.50500000"),
        )
        trade = TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 2, 10, 0, 0, 1, tzinfo=UTC),
            price=Decimal("0.50000000"),
            size=Decimal("2.00000000"),
            side="buy",
            trade_hash="trade-hash-1",
            seq=1,
        )
        candle = CandleRecord(
            token_id=token_id,
            interval="1m",
            start_ts=datetime(2026, 2, 10, 0, 0, tzinfo=UTC),
            end_ts=datetime(2026, 2, 10, 0, 1, tzinfo=UTC),
            o=Decimal("0.40000000"),
            h=Decimal("0.60000000"),
            low=Decimal("0.39000000"),
            c=Decimal("0.50000000"),
            v=Decimal("5.00000000"),
        )

        repository.upsert_orderbook_snapshot(snapshot, ingested_at=now)
        repository.upsert_trade(trade, ingested_at=now)
        repository.upsert_candle(candle, ingested_at=now)

        repository.upsert_orderbook_snapshot(snapshot, ingested_at=now)
        repository.upsert_trade(trade, ingested_at=now)
        repository.upsert_candle(candle, ingested_at=now)

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM orderbook_snapshots WHERE token_id = %s",
                (token_id,),
            )
            snapshot_count = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM trades WHERE token_id = %s",
                (token_id,),
            )
            trade_count = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM candles WHERE token_id = %s AND interval = '1m'",
                (token_id,),
            )
            candle_count = cursor.fetchone()[0]

        assert snapshot_count == 1
        assert trade_count == 1
        assert candle_count == 1


def test_clob_repository_trade_fallback_hash_keeps_distinct_trades() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    token_id = f"t-{uuid4().hex[:12]}"
    now = datetime(2026, 2, 10, 1, 0, tzinfo=UTC)
    trade_ts = datetime(2026, 2, 10, 1, 0, 1, tzinfo=UTC)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = ClobRepository(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "CLOB Trade Hash Test Market", "active", False),
            )
            cursor.execute(
                """
                INSERT INTO market_tokens (market_id, outcome, token_id)
                VALUES (%s, %s, %s)
                """,
                (market_id, "YES", token_id),
            )

        trade_one = TradeRecord(
            token_id=token_id,
            event_ts=trade_ts,
            price=Decimal("0.50000000"),
            size=Decimal("2.00000000"),
            side="buy",
            trade_hash=None,
            seq=None,
        )
        trade_two = TradeRecord(
            token_id=token_id,
            event_ts=trade_ts,
            price=Decimal("0.60000000"),
            size=Decimal("2.00000000"),
            side="buy",
            trade_hash=None,
            seq=None,
        )

        repository.upsert_trade(trade_one, ingested_at=now)
        repository.upsert_trade(trade_two, ingested_at=now)
        repository.upsert_trade(trade_one, ingested_at=now)
        repository.upsert_trade(trade_two, ingested_at=now)

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM trades WHERE token_id = %s",
                (token_id,),
            )
            trade_count = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(DISTINCT trade_hash_norm) FROM trades WHERE token_id = %s",
                (token_id,),
            )
            distinct_hash_count = cursor.fetchone()[0]

        assert trade_count == 2
        assert distinct_hash_count == 2
