from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.jobs.forecast_baseline_ensemble import (
    ForecastBaselineEnsembleConfig,
    run_forecast_baseline_ensemble,
)
from tests.db_helpers import alembic_upgrade_head


def test_forecast_baseline_ensemble_is_asof_safe_and_deterministic(
    tmp_path: Path,
) -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:10]}"
    token_id = f"t-{uuid4().hex[:10]}"
    feature_run_id = uuid4()
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    epsilon_seconds = 300
    ingest_bound = decision_ts + timedelta(seconds=epsilon_seconds)
    late_ingested = ingest_bound + timedelta(seconds=1)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "Forecast Integration Market", "resolved", False),
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
                    decision_ts + timedelta(hours=2),
                    "test",
                    decision_ts + timedelta(hours=3),
                ),
            )
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id, run_type, decision_ts, ingest_epsilon_seconds, code_version, config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    feature_run_id,
                    "compute_micro_features",
                    decision_ts,
                    epsilon_seconds,
                    "test",
                    "test",
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
                    feature_run_id,
                    market_id,
                    decision_ts,
                    f"micro_v1:token:{token_id}",
                    Jsonb(
                        {
                            "mid_price": 0.42,
                            "spread_bps": 120.0,
                            "top_depth_bid": 20.0,
                            "top_depth_ask": 18.0,
                            "book_imbalance_1": 0.05,
                            "last_trade_price": 0.41,
                            "last_trade_size": 5.0,
                            "trade_count_5m": 7,
                            "volume_5m": 35.0,
                            "return_5m": 0.02,
                            "realized_vol_1h": 0.06,
                            "stale_seconds_last_trade": 25,
                            "stale_seconds_last_book": 11,
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
                    feature_run_id,
                    market_id,
                    decision_ts + timedelta(hours=1),
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
                    late_ingested,
                    Jsonb([{"price": "0.89000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.91000000", "size": "10.00000000"}]),
                    0.9,
                ),
            )

    config = ForecastBaselineEnsembleConfig(
        feature_set="micro_v1",
        ingest_epsilon_seconds=epsilon_seconds,
        artifacts_root=str(tmp_path),
        min_isotonic_samples=30,
        min_conformal_samples=20,
        driver_top_k=5,
        calibration_n_bins=10,
        calibration_min_eval=40,
        calibration_ece_threshold=0.08,
    )
    artifact_a = run_forecast_baseline_ensemble(
        config=config,
        token_ids=[token_id],
        max_tokens=None,
        from_ts=decision_ts,
        to_ts=decision_ts,
        step_hours=4,
        nonce="forecast-integration-a",
    )
    artifact_b = run_forecast_baseline_ensemble(
        config=config,
        token_ids=[token_id],
        max_tokens=None,
        from_ts=decision_ts,
        to_ts=decision_ts,
        step_hours=4,
        nonce="forecast-integration-b",
    )

    assert artifact_a["counts"]["examples"] == 1
    assert artifact_a["counts"]["forecasts"] == 1
    assert artifact_a["forecasts"][0]["p_a"] == pytest.approx(0.4, abs=1e-8)
    assert artifact_a["dataset_hash"] == artifact_b["dataset_hash"]
    assert artifact_a["model_hash"] == artifact_b["model_hash"]
    assert artifact_a["forecast_payload_hash"] == artifact_b["forecast_payload_hash"]
    assert Path(artifact_a["artifact_path"]).exists()
    assert Path(artifact_b["artifact_path"]).exists()
