from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.smoke_pipeline_artifact_only import (
    SmokePipelineArtifactOnlyConfig,
    run_smoke_pipeline_artifact_only,
)


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


def test_smoke_pipeline_payload_hashes_are_deterministic_for_same_nonce() -> None:
    artifacts_root = Path("tmp_smoke_pipeline_determinism")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        config = _config(artifacts_root)
        first = run_smoke_pipeline_artifact_only(
            forecast_artifact_path=_forecast_fixture_path(),
            config=config,
            nonce="smoke-deterministic",
        )
        second = run_smoke_pipeline_artifact_only(
            forecast_artifact_path=_forecast_fixture_path(),
            config=config,
            nonce="smoke-deterministic",
        )

        assert (
            first["outputs"]["decision"]["payload_hash"]
            == second["outputs"]["decision"]["payload_hash"]
        )
        assert (
            first["outputs"]["trade_plan"]["payload_hash"]
            == second["outputs"]["trade_plan"]["payload_hash"]
        )
        assert (
            first["outputs"]["execution"]["payload_hash"]
            == second["outputs"]["execution"]["payload_hash"]
        )
        assert (
            first["outputs"]["portfolio"]["payload_hash"]
            == second["outputs"]["portfolio"]["payload_hash"]
        )
        assert (
            first["outputs"]["performance"]["payload_hash"]
            == second["outputs"]["performance"]["payload_hash"]
        )
        assert first["outputs"]["risk"]["payload_hash"] == second["outputs"]["risk"]["payload_hash"]
        assert (
            first["outputs"]["audit_bundle"]["payload_hash"]
            == second["outputs"]["audit_bundle"]["payload_hash"]
        )
        assert (
            first["outputs"]["monitoring"]["payload_hash"]
            == second["outputs"]["monitoring"]["payload_hash"]
        )
        assert first["smoke_outputs_hash"] == second["smoke_outputs_hash"]
        assert first["smoke_payload_hash"] == second["smoke_payload_hash"]
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
