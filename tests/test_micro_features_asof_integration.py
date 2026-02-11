from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.feature_repository import FeatureRepository
from pmx.features.microstore import MicroFeatureStore
from pmx.features.spec_micro_v1 import FEATURE_SET_VERSION
from tests.db_helpers import alembic_upgrade_head


def test_micro_feature_store_asof_gating_and_idempotent_upsert() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:10]}"
    token_id = f"t-{uuid4().hex[:10]}"
    run_id = uuid4()
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    epsilon_seconds = 300
    ingest_bound = decision_ts + timedelta(seconds=epsilon_seconds)
    late_ingested = ingest_bound + timedelta(seconds=1)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = FeatureRepository(connection)
        store = MicroFeatureStore()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "Feature Test Market", "active", False),
            )
            cursor.execute(
                """
                INSERT INTO market_tokens (market_id, outcome, token_id)
                VALUES (%s, %s, %s)
                """,
                (market_id, "YES", token_id),
            )

            cursor.execute(
                """
                INSERT INTO orderbook_snapshots (token_id, event_ts, ingested_at, bids, asks, mid)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(minutes=2),
                    ingest_bound,
                    Jsonb([{"price": "0.39000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.41000000", "size": "9.00000000"}]),
                    Decimal("0.40000000"),
                ),
            )
            cursor.execute(
                """
                INSERT INTO orderbook_snapshots (token_id, event_ts, ingested_at, bids, asks, mid)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(minutes=1),
                    late_ingested,
                    Jsonb([{"price": "0.89000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.91000000", "size": "9.00000000"}]),
                    Decimal("0.90000000"),
                ),
            )

            cursor.execute(
                """
                INSERT INTO trades (
                    token_id, event_ts, ingested_at, price, size, side, trade_hash, seq
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(minutes=6),
                    ingest_bound,
                    Decimal("0.35000000"),
                    Decimal("1.00000000"),
                    "buy",
                    "trade-anchor",
                    1,
                ),
            )
            cursor.execute(
                """
                INSERT INTO trades (
                    token_id, event_ts, ingested_at, price, size, side, trade_hash, seq
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(minutes=2),
                    ingest_bound,
                    Decimal("0.40000000"),
                    Decimal("2.00000000"),
                    "buy",
                    "trade-allowed",
                    2,
                ),
            )
            cursor.execute(
                """
                INSERT INTO trades (
                    token_id, event_ts, ingested_at, price, size, side, trade_hash, seq
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(minutes=1),
                    late_ingested,
                    Decimal("0.90000000"),
                    Decimal("2.00000000"),
                    "buy",
                    "trade-late",
                    3,
                ),
            )
            cursor.execute(
                """
                INSERT INTO candles (
                    token_id, interval, start_ts, end_ts, ingested_at, o, h, l, c, v
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    token_id,
                    "1m",
                    decision_ts - timedelta(minutes=3),
                    decision_ts - timedelta(minutes=2),
                    ingest_bound,
                    Decimal("0.39000000"),
                    Decimal("0.40500000"),
                    Decimal("0.38500000"),
                    Decimal("0.40000000"),
                    Decimal("10.00000000"),
                ),
            )
            cursor.execute(
                """
                INSERT INTO candles (
                    token_id, interval, start_ts, end_ts, ingested_at, o, h, l, c, v
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    token_id,
                    "1m",
                    decision_ts - timedelta(minutes=2),
                    decision_ts - timedelta(minutes=1),
                    ingest_bound,
                    Decimal("0.40000000"),
                    Decimal("0.41000000"),
                    Decimal("0.39500000"),
                    Decimal("0.40500000"),
                    Decimal("11.00000000"),
                ),
            )

        repository.insert_run(
            run_id=run_id,
            run_type="compute_micro_features",
            decision_ts=decision_ts,
            ingest_epsilon_seconds=epsilon_seconds,
            code_version="test",
            config_hash="test",
        )

        features = store.compute_features(connection, token_id, decision_ts, epsilon_seconds)
        assert features["last_trade_price"] == pytest.approx(0.4, abs=1e-8)
        assert features["trade_count_5m"] == 1
        assert features["stale_seconds_last_book"] == 120
        assert features["token_id"] == token_id

        repository.upsert_feature_snapshot(
            run_id=run_id,
            market_id=market_id,
            token_id=token_id,
            decision_ts=decision_ts,
            feature_set=FEATURE_SET_VERSION,
            features_json=features,
            computed_at=decision_ts,
        )
        repository.upsert_feature_snapshot(
            run_id=run_id,
            market_id=market_id,
            token_id=token_id,
            decision_ts=decision_ts,
            feature_set=FEATURE_SET_VERSION,
            features_json=features,
            computed_at=decision_ts,
        )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM feature_snapshots
                WHERE run_id = %s
                  AND market_id = %s
                  AND asof_ts = %s
                """,
                (run_id, market_id, decision_ts),
            )
            snapshot_count = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT features
                FROM feature_snapshots
                WHERE run_id = %s
                  AND market_id = %s
                  AND asof_ts = %s
                LIMIT 1
                """,
                (run_id, market_id, decision_ts),
            )
            stored = cursor.fetchone()

    assert snapshot_count == 1
    assert stored is not None
    payload = stored[0] if isinstance(stored[0], dict) else {}
    meta = payload.get("_meta")
    assert isinstance(meta, dict)
    assert meta.get("token_id") == token_id
