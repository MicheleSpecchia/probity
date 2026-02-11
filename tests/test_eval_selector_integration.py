from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.jobs.eval_selector import EvalSelectorConfig, run_eval_selector
from tests.db_helpers import alembic_upgrade_head


def test_eval_selector_reports_all_versions_and_respects_asof(
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
    selector_run_id = uuid4()
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    epsilon_seconds = 300
    ingest_bound = decision_ts + timedelta(seconds=epsilon_seconds)
    late_ingest = ingest_bound + timedelta(seconds=1)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (
                    market_id, title, description, category, status, updated_ts,
                    rule_text, rule_parse_ok, resolution_ts
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    market_id,
                    "Selector eval market",
                    "Test market for selector evaluation",
                    "politics",
                    "active",
                    decision_ts - timedelta(hours=1),
                    "Resolves YES if event happens.",
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
                    "integration_test",
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
                            "spread_bps": 150.0,
                            "book_imbalance_1": 0.04,
                            "trade_count_5m": 4,
                            "volume_5m": 25.0,
                            "return_5m": 0.01,
                            "realized_vol_1h": 0.03,
                            "stale_seconds_last_trade": 20,
                            "stale_seconds_last_book": 12,
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
                    decision_ts - timedelta(seconds=20),
                    late_ingest,
                    Jsonb([{"price": "0.89000000", "size": "10.00000000"}]),
                    Jsonb([{"price": "0.91000000", "size": "10.00000000"}]),
                    0.9,
                ),
            )
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id, run_type, decision_ts, ingest_epsilon_seconds, code_version, config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (selector_run_id, "select_markets", decision_ts, epsilon_seconds, "test", "test"),
            )

            selector_versions = (
                "selector_v1",
                "baseline_top_volume",
                "baseline_random_stratified",
            )
            selection_run_ids: dict[str, int] = {}
            for version in selector_versions:
                cursor.execute(
                    """
                    INSERT INTO selection_runs (
                        run_id, stage, universe_size, selected_size, selector_version, params
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING selection_run_id
                    """,
                    (
                        selector_run_id,
                        "deep_dive" if version == "selector_v1" else "deep_dive_baseline",
                        1,
                        1,
                        version,
                        Jsonb({"decision_ts": decision_ts.isoformat()}),
                    ),
                )
                selection_run_ids[version] = int(cursor.fetchone()[0])

            for version, selection_run_id in selection_run_ids.items():
                cursor.execute(
                    """
                    INSERT INTO selection_items (
                        selection_run_id, market_id, score, rank, reason
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        selection_run_id,
                        market_id,
                        0.75 if version == "selector_v1" else 0.55,
                        1,
                        Jsonb({"token_id": token_id, "ttr_bucket": "1_7d"}),
                    ),
                )

    config = EvalSelectorConfig(
        feature_set="micro_v1",
        ingest_epsilon_seconds=epsilon_seconds,
        window_hours=72,
        artifacts_root=str(tmp_path / "selector_eval"),
        selector_versions=(
            "selector_v1",
            "baseline_top_volume",
            "baseline_random_stratified",
        ),
    )
    result = run_eval_selector(decision_ts=decision_ts, config=config)
    report = result["report"]

    for version in config.selector_versions:
        payload = report["selectors"][version]
        assert payload["status"] == "ok"
        assert payload["counts"]["selected_tokens"] == 1
        assert payload["counts"]["examples"] >= 1

    selector_payload = report["selectors"]["selector_v1"]
    assert selector_payload["price_prob_stats"]["max"] == pytest.approx(0.4, abs=1e-8)

    artifact_dir = tmp_path / "selector_eval"
    artifacts = sorted(artifact_dir.glob("*.json"))
    assert len(artifacts) == 1
