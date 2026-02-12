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


def test_performance_report_schema_valid_payload() -> None:
    artifacts_root = Path("tmp_performance_schema_valid")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        config = load_performance_from_portfolio_config(
            artifacts_root=str(artifacts_root),
            window_from=None,
            window_to=None,
        )
        report = run_performance_from_portfolio(
            portfolio_artifact_paths=_fixture_paths(),
            config=config,
            nonce="performance-schema-valid",
        )
        assert validate_performance_report_artifact(report) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)


def test_performance_report_schema_missing_required_field() -> None:
    artifacts_root = Path("tmp_performance_schema_invalid")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    try:
        config = load_performance_from_portfolio_config(
            artifacts_root=str(artifacts_root),
            window_from=None,
            window_to=None,
        )
        report = run_performance_from_portfolio(
            portfolio_artifact_paths=_fixture_paths(),
            config=config,
            nonce="performance-schema-invalid",
        )
        report.pop("aggregate_metrics")

        errors = validate_performance_report_artifact(report)
        assert len(errors) >= 1
        first: dict[str, Any] = errors[0]
        assert first["code"] == "schema:required"
        assert first["path"] == "$"
        assert "aggregate_metrics" in str(first["reason"])
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
