from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.risk_from_trade_plan import (
    load_risk_from_trade_plan_config,
    run_risk_from_trade_plan,
)
from pmx.risk.validate_artifact import validate_risk_artifact


def test_risk_hashing_is_deterministic_for_same_input_and_args() -> None:
    trade_plan_path = (
        Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    )
    artifacts_root = Path("tmp_risk_determinism")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    config = load_risk_from_trade_plan_config(
        artifacts_root=str(artifacts_root),
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=2000.0,
        top1_share_cap=0.5,
        top3_share_cap=0.8,
        performance_top1_cap=0.5,
        performance_top3_cap=0.8,
        allow_downsize=True,
        min_notional_usd=5.0,
        blocking_quality_flags=None,
        cooldown_block_flags=None,
    )
    try:
        first = run_risk_from_trade_plan(
            trade_plan_artifact_path=trade_plan_path,
            config=config,
            nonce=None,
        )
        second = run_risk_from_trade_plan(
            trade_plan_artifact_path=trade_plan_path,
            config=config,
            nonce=None,
        )

        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["run_id"] == second["run_id"]
        assert first["policy_hash"] == second["policy_hash"]
        assert first["items_hash"] == second["items_hash"]
        assert first["risk_payload_hash"] == second["risk_payload_hash"]
        assert first["artifact_path"] == second["artifact_path"]
        assert validate_risk_artifact(first) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
