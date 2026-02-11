from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pmx.backtest.asof_dataset import build_asof_dataset
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.models.baselines import baseline_a_price, baseline_b_micro
from tests.db_helpers import alembic_upgrade_head


def test_backtest_asof_dataset_excludes_leaky_rows_and_scores_baselines() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:10]}"
    token_id = f"t-{uuid4().hex[:10]}"
    run_id = uuid4()
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    future_decision_ts = decision_ts + timedelta(hours=1)
    epsilon_seconds = 300
    ingest_bound = decision_ts + timedelta(seconds=epsilon_seconds)
    late_ingest = ingest_bound + timedelta(seconds=1)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "Backtest Market", "resolved", False),
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
                INSERT INTO market_outcomes (
                    market_id, resolved, outcome, resolved_ts, resolver_source, ingested_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    market_id,
                    True,
                    "YES",
                    decision_ts + timedelta(minutes=30),
                    "test",
                    decision_ts + timedelta(hours=2),
                ),
            )
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id, run_type, decision_ts, ingest_epsilon_seconds, code_version, config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (run_id, "compute_micro_features", decision_ts, epsilon_seconds, "test", "test"),
            )
            cursor.execute(
                """
                INSERT INTO feature_snapshots (
                    run_id, market_id, asof_ts, feature_set_version, features
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    market_id,
                    decision_ts,
                    f"micro_v1:token:{token_id}",
                    Jsonb(
                        {
                            "mid_price": 0.42,
                            "spread_bps": 200.0,
                            "book_imbalance_1": 0.1,
                            "trade_count_5m": 3,
                            "volume_5m": 12.0,
                            "return_5m": 0.03,
                            "realized_vol_1h": 0.02,
                            "stale_seconds_last_trade": 30,
                            "stale_seconds_last_book": 15,
                        }
                    ),
                ),
            )
            cursor.execute(
                """
                INSERT INTO feature_snapshots (
                    run_id, market_id, asof_ts, feature_set_version, features
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    market_id,
                    future_decision_ts,
                    f"micro_v1:token:{token_id}",
                    Jsonb({"mid_price": 0.99}),
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
                    ingest_bound,
                    Jsonb([{"price": "0.39000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.41000000", "size": "10.00000000"}]),
                    0.4,
                ),
            )
            cursor.execute(
                """
                INSERT INTO orderbook_snapshots (token_id, event_ts, ingested_at, bids, asks, mid)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    token_id,
                    decision_ts - timedelta(seconds=30),
                    late_ingest,
                    Jsonb([{"price": "0.89000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.91000000", "size": "10.00000000"}]),
                    0.9,
                ),
            )

        dataset = build_asof_dataset(
            connection,
            token_ids=[token_id],
            decision_ts_list=[decision_ts, future_decision_ts],
            epsilon_s=epsilon_seconds,
            feature_set="micro_v1",
            outcome_provider=None,
        )

    assert len(dataset.examples) == 1
    assert dataset.skipped_no_outcome == 1
    assert dataset.skipped_missing_features == 0

    example = dataset.examples[0]
    assert example.token_id == token_id
    assert example.price_prob == pytest.approx(0.4, abs=1e-8)
    assert example.features_json["mid_price"] == pytest.approx(0.42, abs=1e-8)
    assert baseline_a_price(example.price_prob) == pytest.approx(example.price_prob, abs=1e-12)
    prob_b = baseline_b_micro(example.features_json)
    assert 0.0 <= prob_b <= 1.0
