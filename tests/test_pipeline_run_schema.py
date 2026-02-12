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


def test_pipeline_run_artifact_schema_valid_payload() -> None:
    artifacts_root = Path("tmp_pipeline_schema_valid")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        summary = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_config(artifacts_root),
            nonce="pipeline-schema-valid",
        )
        assert validate_pipeline_run_artifact(summary) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)


def test_pipeline_run_artifact_schema_missing_required_field() -> None:
    artifacts_root = Path("tmp_pipeline_schema_invalid")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        summary = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_config(artifacts_root),
            nonce="pipeline-schema-invalid",
        )
        summary.pop("outputs")

        errors = validate_pipeline_run_artifact(summary)
        assert len(errors) >= 1
        first: dict[str, Any] = errors[0]
        assert first["code"] == "schema:required"
        assert first["path"] == "$"
        assert "outputs" in str(first["reason"])
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
