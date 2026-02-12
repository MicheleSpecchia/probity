from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pmx.jobs.portfolio_from_execution import (
    load_portfolio_from_execution_config,
    run_portfolio_from_execution,
)
from pmx.portfolio.validate_artifact import validate_portfolio_artifact


def test_portfolio_job_writes_artifact_and_logs_hashes(monkeypatch: Any) -> None:
    execution_path = (
        Path(__file__).with_name("fixtures") / "executions" / "execution_artifact_sample.json"
    )
    artifacts_root = Path("tmp_portfolio_job_smoke")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    config = load_portfolio_from_execution_config(
        artifacts_root=str(artifacts_root),
        fee_bps=0.0,
        fee_usd=0.0,
        mark_source="execution_price",
        reference_prices_json=None,
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

    monkeypatch.setattr("pmx.jobs.portfolio_from_execution._log", _capture_log)

    try:
        result = run_portfolio_from_execution(
            execution_artifact_paths=[execution_path],
            config=config,
            nonce="portfolio-smoke-test",
        )
        completed_logs = [
            entry for entry in captured_logs if entry["msg"] == "portfolio_from_execution_completed"
        ]

        assert Path(result["artifact_path"]).exists()
        assert result["counts"]["n_execution_inputs"] == 1
        assert result["counts"]["n_ledger_entries"] == 3
        assert result["counts"]["n_positions"] == 3
        assert validate_portfolio_artifact(result) == []

        assert len(completed_logs) >= 1
        latest = completed_logs[-1]["extra_fields"]
        assert "ledger_hash" in latest
        assert "positions_hash" in latest
        assert "valuation_hash" in latest
        assert "portfolio_payload_hash" in latest
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
