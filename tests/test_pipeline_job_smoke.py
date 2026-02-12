from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.jobs.run_pipeline_stub import RunPipelineStubConfig, run_pipeline_stub
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact


def _forecast_fixture_path() -> Path:
    return Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"


def _config(artifacts_root: Path) -> RunPipelineStubConfig:
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


def test_pipeline_stub_job_writes_all_artifacts_and_logs_hashes(
    monkeypatch: Any,
) -> None:
    artifacts_root = Path("tmp_pipeline_smoke")
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

    monkeypatch.setattr("pmx.jobs.run_pipeline_stub._log", _capture_log)

    try:
        result = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_config(artifacts_root),
            nonce="pipeline-smoke-test",
        )

        assert Path(result["artifact_path"]).exists()
        assert validate_pipeline_run_artifact(result) == []

        for stage_name in ("decision", "trade_plan", "execution", "portfolio"):
            stage_path = Path(result["outputs"][stage_name]["artifact_path"])
            assert stage_path.exists()

        completed = [entry for entry in captured_logs if entry["msg"] == "pipeline_stub_completed"]
        assert len(completed) >= 1
        latest = completed[-1]["extra_fields"]
        assert "decision_payload_hash" in latest
        assert "trade_plan_payload_hash" in latest
        assert "execution_payload_hash" in latest
        assert "portfolio_payload_hash" in latest
        assert "pipeline_payload_hash" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
