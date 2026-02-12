from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.run_pipeline_stub import RunPipelineStubConfig, run_pipeline_stub


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


def test_pipeline_run_is_deterministic_for_same_input_and_params() -> None:
    artifacts_root = Path("tmp_pipeline_determinism")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        config = _config(artifacts_root)
        first = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=config,
            nonce=None,
        )
        second = run_pipeline_stub(
            forecast_artifact_path=_forecast_fixture_path(),
            config=config,
            nonce=None,
        )

        assert first["run_id"] == second["run_id"]
        assert first["pipeline_policy_hash"] == second["pipeline_policy_hash"]
        assert first["pipeline_outputs_hash"] == second["pipeline_outputs_hash"]
        assert first["pipeline_payload_hash"] == second["pipeline_payload_hash"]
        assert first["artifact_path"] == second["artifact_path"]

        for stage_name in ("decision", "trade_plan", "execution", "portfolio"):
            assert first["outputs"][stage_name]["run_id"] == second["outputs"][stage_name]["run_id"]
            assert (
                first["outputs"][stage_name]["payload_hash"]
                == second["outputs"][stage_name]["payload_hash"]
            )
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
