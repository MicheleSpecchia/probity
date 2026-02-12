from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.trade_plan_from_decision import (
    load_trade_plan_from_decision_config,
    run_trade_plan_from_decision,
)
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact


def test_trade_plan_hashing_is_deterministic_for_same_input_and_args() -> None:
    decision_path = (
        Path(__file__).with_name("fixtures") / "decisions" / "decision_artifact_sample.json"
    )
    artifacts_root = Path("tmp_trade_plan_job_artifacts")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    config = load_trade_plan_from_decision_config(
        max_orders=200,
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=2000.0,
        sizing_mode="fixed_notional",
        fixed_notional_usd=100.0,
        base_notional_usd=25.0,
        target_edge_bps=100.0,
        min_scale=0.5,
        max_scale=2.0,
        dry_run=True,
        artifacts_root=str(artifacts_root),
    )

    try:
        first = run_trade_plan_from_decision(
            decision_artifact_path=decision_path,
            config=config,
            nonce="trade-plan-hash-test",
        )
        second = run_trade_plan_from_decision(
            decision_artifact_path=decision_path,
            config=config,
            nonce="trade-plan-hash-test",
        )

        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["policy_hash"] == second["policy_hash"]
        assert first["orders_hash"] == second["orders_hash"]
        assert first["trade_plan_payload_hash"] == second["trade_plan_payload_hash"]
        assert validate_trade_plan_artifact(first) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
