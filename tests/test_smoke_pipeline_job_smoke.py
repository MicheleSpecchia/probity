from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pmx.audit_bundle.validate_artifact import validate_audit_bundle_artifact
from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.jobs.smoke_pipeline_artifact_only import (
    STEP_ORDER,
    SmokePipelineArtifactOnlyConfig,
    run_smoke_pipeline_artifact_only,
)
from pmx.monitoring.validate_artifact import validate_monitoring_report_artifact
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact
from pmx.portfolio.validate_artifact import validate_portfolio_artifact
from pmx.risk.validate_artifact import validate_risk_artifact
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact


def _forecast_fixture_path() -> Path:
    return Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"


def _config(artifacts_root: Path) -> SmokePipelineArtifactOnlyConfig:
    return SmokePipelineArtifactOnlyConfig(
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
        fail_on_critical_block=True,
        warn_on_any_quality_signal=True,
    )


def _validators() -> dict[str, Any]:
    return {
        "decision": validate_decision_artifact,
        "trade_plan": validate_trade_plan_artifact,
        "execution": validate_execution_artifact,
        "portfolio": validate_portfolio_artifact,
        "pipeline": validate_pipeline_run_artifact,
        "performance": validate_performance_report_artifact,
        "risk": validate_risk_artifact,
        "audit_bundle": validate_audit_bundle_artifact,
        "monitoring": validate_monitoring_report_artifact,
    }


def test_smoke_pipeline_job_writes_summary_and_validates_all_step_artifacts() -> None:
    artifacts_root = Path("tmp_smoke_pipeline_job")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        summary = run_smoke_pipeline_artifact_only(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_config(artifacts_root),
            nonce="smoke-job",
        )

        summary_path = Path(summary["artifact_path"])
        assert summary_path.exists()
        assert summary["counts"]["n_steps_fail"] == 0
        assert set(summary["outputs"].keys()) == set(STEP_ORDER)
        assert summary["overall_status"] in {"OK", "WARN"}

        validators = _validators()
        for step in STEP_ORDER:
            output = summary["outputs"][step]
            assert output["status"] == "OK"
            assert output["artifact_path"] is not None
            assert output["payload_hash"] is not None
            assert len(output["payload_hash"]) == 64

            artifact_path = Path(output["artifact_path"])
            assert artifact_path.exists()

            with artifact_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            assert validators[step](payload) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
