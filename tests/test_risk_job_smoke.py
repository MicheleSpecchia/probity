from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.jobs.risk_from_trade_plan import (
    load_risk_from_trade_plan_config,
    run_risk_from_trade_plan,
)
from pmx.risk.validate_artifact import validate_risk_artifact


def test_risk_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    trade_plan_path = (
        Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    )
    hooks_path = Path(__file__).with_name("fixtures") / "risk" / "risk_hooks_sample.json"
    artifacts_root = Path("tmp_risk_job_smoke")
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
    captured_logs: list[dict[str, Any]] = []

    def _capture_log(
        logger: Any,
        level: int,
        message: str,
        run_context: Any,
        **extra_fields: Any,
    ) -> None:
        captured_logs.append({"msg": message, "extra_fields": dict(extra_fields)})

    monkeypatch.setattr("pmx.jobs.risk_from_trade_plan._log", _capture_log)

    try:
        result = run_risk_from_trade_plan(
            trade_plan_artifact_path=trade_plan_path,
            hooks_json_path=hooks_path,
            config=config,
            nonce="risk-job-smoke",
        )
        completed_logs = [
            entry for entry in captured_logs if entry["msg"] == "risk_from_trade_plan_completed"
        ]

        assert Path(result["artifact_path"]).exists()
        assert validate_risk_artifact(result) == []
        assert result["counts"]["n_total"] == 4
        assert len(completed_logs) >= 1
        latest = completed_logs[-1]["extra_fields"]
        assert "policy_hash" in latest
        assert "items_hash" in latest
        assert "risk_payload_hash" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
