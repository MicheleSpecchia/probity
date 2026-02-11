from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.jobs.select_markets import SelectMarketsConfig, run_select_markets
from pmx.selector.spec import SelectorConfig
from tests.db_helpers import alembic_upgrade_head


def test_selector_job_persists_runs_and_items(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)

    market_id = f"m-{uuid4().hex[:10]}"
    token_id = f"t-{uuid4().hex[:10]}"
    feature_run_id = uuid4()
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    ingest_bound = decision_ts + timedelta(seconds=300)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (
                    market_id, title, description, category, status,
                    updated_ts, rule_text, rule_parse_json, rule_parse_ok, resolution_ts
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (
                    market_id,
                    "Selector Integration Market",
                    "Clear description",
                    "politics",
                    "active",
                    decision_ts - timedelta(hours=1),
                    "Resolves YES if condition holds",
                    Jsonb({"version": "stub_v1"}),
                    True,
                    decision_ts + timedelta(days=3),
                ),
            )
            cursor.execute(
                "INSERT INTO market_tokens (market_id, outcome, token_id) VALUES (%s, %s, %s)",
                (market_id, "YES", token_id),
            )
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id, run_type, decision_ts, ingest_epsilon_seconds, code_version, config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (feature_run_id, "compute_micro_features", decision_ts, 300, "test", "test"),
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
                            "spread_bps": 120.0,
                            "top_depth_bid": 12.0,
                            "top_depth_ask": 11.0,
                            "book_imbalance_1": 0.04,
                            "return_5m": 0.03,
                            "realized_vol_1h": 0.02,
                            "stale_seconds_last_trade": 20,
                            "stale_seconds_last_book": 10,
                            "ingest_bound_ts": ingest_bound.isoformat(),
                        }
                    ),
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
                    Jsonb([{"price": "0.49000000", "size": "15.00000000"}]),
                    Jsonb([{"price": "0.51000000", "size": "14.00000000"}]),
                    0.5,
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
                    0.5,
                    10.0,
                    "buy",
                    f"tr-{uuid4().hex[:10]}",
                    1,
                ),
            )

    config = SelectMarketsConfig(
        ingest_epsilon_seconds=300,
        max_candidates=10,
        k_deep=1,
        feature_set="micro_v1",
        max_per_category=5,
        max_per_group=5,
        target_bucket_mix={
            "0_24h": 0.0,
            "1_7d": 1.0,
            "7_30d": 0.0,
            "30d_plus": 0.0,
            "unknown": 0.0,
        },
        artifacts_root=str(tmp_path),
        selector_config=SelectorConfig(),
    )
    summary = run_select_markets(decision_ts=decision_ts, config=config)
    run_uuid = UUID(summary["run_id"])

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM selection_runs WHERE run_id = %s",
                (run_uuid,),
            )
            run_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM selection_items si
                     WHERE si.selection_run_id IN (
                         SELECT selection_run_id FROM selection_runs WHERE run_id = %s
                     )) AS total_rows,
                    (SELECT COUNT(*) FROM (
                        SELECT si.selection_run_id, si.market_id
                        FROM selection_items si
                        WHERE si.selection_run_id IN (
                            SELECT selection_run_id FROM selection_runs WHERE run_id = %s
                        )
                        GROUP BY si.selection_run_id, si.market_id
                    ) AS deduped) AS dedup_rows
                """,
                (run_uuid, run_uuid),
            )
            counts = cursor.fetchone()

    assert run_count == 3
    assert counts is not None
    assert int(counts[0]) == int(counts[1])
    assert summary["counts"]["selected_main"] == 1
    assert summary["candidate_fallback_artifact"] is not None
