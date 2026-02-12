from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.audit_bundle.validate_artifact import validate_audit_bundle_artifact
from pmx.jobs.build_audit_bundle import load_build_audit_bundle_config, run_build_audit_bundle
from pmx.jobs.performance_from_portfolio import (
    load_performance_from_portfolio_config,
    run_performance_from_portfolio,
)
from pmx.jobs.risk_from_trade_plan import load_risk_from_trade_plan_config, run_risk_from_trade_plan
from pmx.jobs.run_pipeline_stub import RunPipelineStubConfig, run_pipeline_stub


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


def test_audit_bundle_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    artifacts_root = Path("tmp_audit_bundle_smoke")
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

    monkeypatch.setattr("pmx.jobs.build_audit_bundle._log", _capture_log)

    try:
        pipeline = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_pipeline_config(artifacts_root),
            nonce="audit-bundle-smoke:pipeline",
        )
        pipeline_path = Path(pipeline["artifact_path"])
        portfolio_path = Path(pipeline["outputs"]["portfolio"]["artifact_path"])
        trade_plan_path = Path(pipeline["outputs"]["trade_plan"]["artifact_path"])

        performance_config = load_performance_from_portfolio_config(
            artifacts_root=str(artifacts_root),
            window_from=None,
            window_to=None,
        )
        performance = run_performance_from_portfolio(
            portfolio_artifact_paths=[portfolio_path],
            config=performance_config,
            nonce="audit-bundle-smoke:performance",
        )
        risk_config = load_risk_from_trade_plan_config(
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
        risk = run_risk_from_trade_plan(
            trade_plan_artifact_path=trade_plan_path,
            performance_artifact_path=Path(performance["artifact_path"]),
            config=risk_config,
            nonce="audit-bundle-smoke:risk",
        )

        config = load_build_audit_bundle_config(artifacts_root=str(artifacts_root))
        result = run_build_audit_bundle(
            pipeline_artifact_path=pipeline_path,
            performance_artifact_path=Path(performance["artifact_path"]),
            risk_artifact_path=Path(risk["artifact_path"]),
            config=config,
            nonce="audit-bundle-smoke:bundle",
        )
        completed_logs = [
            entry for entry in captured_logs if entry["msg"] == "build_audit_bundle_completed"
        ]

        assert Path(result["artifact_path"]).exists()
        assert validate_audit_bundle_artifact(result) == []
        assert len(result["inputs"]) >= 7
        assert len(completed_logs) >= 1
        latest = completed_logs[-1]["extra_fields"]
        assert "bundle_hash" in latest
        assert "audit_bundle_payload_hash" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)


def test_audit_bundle_job_allows_missing_optional_performance_and_risk() -> None:
    artifacts_root = Path("tmp_audit_bundle_optional")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        pipeline = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=_pipeline_config(artifacts_root),
            nonce="audit-bundle-optional:pipeline",
        )
        config = load_build_audit_bundle_config(artifacts_root=str(artifacts_root))
        result = run_build_audit_bundle(
            pipeline_artifact_path=Path(pipeline["artifact_path"]),
            config=config,
            nonce="audit-bundle-optional:bundle",
        )
        assert Path(result["artifact_path"]).exists()
        assert validate_audit_bundle_artifact(result) == []
        stages = {entry["stage"] for entry in result["inputs"]}
        assert {"pipeline", "decision", "trade_plan", "execution", "portfolio"}.issubset(stages)
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
