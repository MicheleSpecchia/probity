from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.jobs.monitor_from_pipeline import (
    load_monitor_from_pipeline_config,
    run_monitor_from_pipeline,
)
from pmx.jobs.run_pipeline_stub import RunPipelineStubConfig, run_pipeline_stub
from pmx.monitoring.validate_artifact import validate_monitoring_report_artifact


def _forecast_fixture_path() -> Path:
    return Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"


def _pipeline_config(artifacts_root: Path) -> RunPipelineStubConfig:
    return RunPipelineStubConfig(
        artifacts_root=str(artifacts_root),
        min_edge_bps=50.0,
        robust_mode="require_positive_low90",
        max_items=200,
        sizing_mode="fixed_notional",
        fixed_notional_usd=25.0,
        base_notional_usd=25.0,
        target_edge_bps=100.0,
        min_scale=0.5,
        max_scale=2.0,
        max_orders=200,
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=2000.0,
        execution_mode="dry_run",
        fee_bps=0.0,
        fee_usd=0.0,
        mark_source="execution_price",
        reference_prices_json=None,
    )


def test_monitoring_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    artifacts_root = Path("tmp_monitoring_smoke")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    captured_logs: list[dict[str, Any]] = []

    def _capture_log(
        logger: Any,
        level: int,
        message: str,
        run_context: Any,
        **extra_fields: Any,
    ) -> None:
        captured_logs.append({"msg": message, "extra_fields": dict(extra_fields)})

    monkeypatch.setattr("pmx.jobs.monitor_from_pipeline._log", _capture_log)

    try:
        pipeline = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_pipeline_config(artifacts_root),
            nonce="monitoring-smoke:pipeline",
        )
        config = load_monitor_from_pipeline_config(
            artifacts_root=str(artifacts_root),
            fail_on_critical_block=True,
            warn_on_any_quality_signal=True,
        )
        result = run_monitor_from_pipeline(
            pipeline_artifact_path=Path(pipeline["artifact_path"]),
            config=config,
            nonce="monitoring-smoke:monitor",
        )
        completed_logs = [
            entry for entry in captured_logs if entry["msg"] == "monitor_from_pipeline_completed"
        ]

        assert Path(result["artifact_path"]).exists()
        assert validate_monitoring_report_artifact(result) == []
        assert result["health_status"] in {"OK", "WARN", "FAIL"}
        assert len(completed_logs) >= 1
        latest = completed_logs[-1]["extra_fields"]
        assert "monitoring_policy_hash" in latest
        assert "monitoring_inputs_hash" in latest
        assert "monitoring_payload_hash" in latest
        assert "health_status" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
