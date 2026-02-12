from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.jobs.execute_trade_plan_stub import (
    load_execute_trade_plan_stub_config,
    run_execute_trade_plan_stub,
)


def test_execute_trade_plan_stub_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    trade_plan_path = (
        Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    )
    artifacts_root = Path("tmp_execution_job_artifacts")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    config = load_execute_trade_plan_stub_config(
        mode="simulate_submit",
        max_orders=None,
        simulate_reject_modulo=2,
        simulate_reject_remainder=0,
        artifacts_root=str(artifacts_root),
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

    monkeypatch.setattr("pmx.jobs.execute_trade_plan_stub._log", _capture_log)

    try:
        first = run_execute_trade_plan_stub(
            trade_plan_artifact_path=trade_plan_path,
            config=config,
            nonce="execution-job-test",
        )
        second = run_execute_trade_plan_stub(
            trade_plan_artifact_path=trade_plan_path,
            config=config,
            nonce="execution-job-test",
        )
        completed_logs = [
            log for log in captured_logs if log["msg"] == "execute_trade_plan_stub_completed"
        ]

        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["execution_payload_hash"] == second["execution_payload_hash"]
        assert first["orders_hash"] == second["orders_hash"]
        assert first["counts"]["n_orders"] == 4
        assert first["counts"]["n_skipped"] >= 2
        assert validate_execution_artifact(first) == []

        assert len(completed_logs) >= 1
        latest = completed_logs[-1]
        extra_fields = latest["extra_fields"]
        assert "input_trade_plan_hash" in extra_fields
        assert "execution_policy_hash" in extra_fields
        assert "execution_payload_hash" in extra_fields
        assert "orders_hash" in extra_fields
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
