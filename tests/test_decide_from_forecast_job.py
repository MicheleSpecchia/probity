from __future__ import annotations

import shutil
from pathlib import Path

from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.jobs.decide_from_forecast import (
    load_decide_from_forecast_config,
    run_decide_from_forecast,
)


def test_decide_from_forecast_job_is_artifact_only_and_deterministic() -> None:
    forecast_path = (
        Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"
    )
    artifacts_root = Path("tmp_decision_job_artifacts")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    config = load_decide_from_forecast_config(
        min_edge_bps=50.0,
        robust_mode="require_positive_low90",
        max_items=200,
        artifacts_root=str(artifacts_root),
    )

    try:
        first = run_decide_from_forecast(
            forecast_artifact_path=forecast_path,
            config=config,
            nonce="job-test-deterministic",
        )
        second = run_decide_from_forecast(
            forecast_artifact_path=forecast_path,
            config=config,
            nonce="job-test-deterministic",
        )

        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["counts"] == {"n_total": 6, "n_trade": 3, "n_no_trade": 3}
        assert first["decision_payload_hash"] == second["decision_payload_hash"]
        assert first["decision_items_hash"] == second["decision_items_hash"]
        assert validate_decision_artifact(first) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
