from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

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


def test_performance_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    artifacts_root = Path("tmp_performance_job_smoke")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    config = load_performance_from_portfolio_config(
        artifacts_root=str(artifacts_root),
        window_from=None,
        window_to=None,
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

    monkeypatch.setattr("pmx.jobs.performance_from_portfolio._log", _capture_log)

    try:
        report = run_performance_from_portfolio(
            portfolio_artifact_paths=_fixture_paths(),
            config=config,
            nonce="performance-smoke-test",
        )
        completed_logs = [
            entry
            for entry in captured_logs
            if entry["msg"] == "performance_from_portfolio_completed"
        ]

        assert Path(report["artifact_path"]).exists()
        assert report["aggregate_metrics"]["n_inputs"] == 2
        assert validate_performance_report_artifact(report) == []
        assert len(completed_logs) >= 1
        latest = completed_logs[-1]["extra_fields"]
        assert "performance_policy_hash" in latest
        assert "performance_inputs_hash" in latest
        assert "performance_payload_hash" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
