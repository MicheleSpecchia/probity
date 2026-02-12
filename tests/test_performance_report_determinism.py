from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.performance_from_portfolio import (
    load_performance_from_portfolio_config,
    run_performance_from_portfolio,
)
from pmx.performance.validate_artifact import validate_performance_report_artifact


def _fixture_paths() -> list[Path]:
    root = Path(__file__).with_name("fixtures") / "portfolio"
    return [
        root / "portfolio_artifact_sample_A.json",
        root / "portfolio_artifact_sample_B.json",
    ]


def test_performance_report_is_deterministic_for_same_inputs() -> None:
    artifacts_root = Path("tmp_performance_determinism")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        config = load_performance_from_portfolio_config(
            artifacts_root=str(artifacts_root),
            window_from=None,
            window_to=None,
        )
        first = run_performance_from_portfolio(
            portfolio_artifact_paths=_fixture_paths(),
            config=config,
            nonce=None,
        )
        second = run_performance_from_portfolio(
            portfolio_artifact_paths=_fixture_paths(),
            config=config,
            nonce=None,
        )

        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["run_id"] == second["run_id"]
        assert first["performance_policy_hash"] == second["performance_policy_hash"]
        assert first["performance_inputs_hash"] == second["performance_inputs_hash"]
        assert first["performance_payload_hash"] == second["performance_payload_hash"]
        assert first["artifact_path"] == second["artifact_path"]
        assert validate_performance_report_artifact(first) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
