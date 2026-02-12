from __future__ import annotations

import shutil
from pathlib import Path

from pmx.jobs.portfolio_from_execution import (
    load_portfolio_from_execution_config,
    run_portfolio_from_execution,
)
from pmx.portfolio.validate_artifact import validate_portfolio_artifact


def test_portfolio_hashing_is_deterministic_for_same_input_and_args() -> None:
    execution_path = (
        Path(__file__).with_name("fixtures") / "executions" / "execution_artifact_sample.json"
    )
    artifacts_root = Path("tmp_portfolio_job_artifacts")
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    config = load_portfolio_from_execution_config(
        artifacts_root=str(artifacts_root),
        fee_bps=0.0,
        fee_usd=0.0,
        mark_source="execution_price",
        reference_prices_json=None,
    )

    try:
        first = run_portfolio_from_execution(
            execution_artifact_paths=[execution_path],
            config=config,
            nonce="portfolio-hash-test",
        )
        second = run_portfolio_from_execution(
            execution_artifact_paths=[execution_path],
            config=config,
            nonce="portfolio-hash-test",
        )
        assert Path(first["artifact_path"]).exists()
        assert Path(second["artifact_path"]).exists()
        assert first["portfolio_policy_hash"] == second["portfolio_policy_hash"]
        assert first["ledger_hash"] == second["ledger_hash"]
        assert first["positions_hash"] == second["positions_hash"]
        assert first["valuation_hash"] == second["valuation_hash"]
        assert first["portfolio_payload_hash"] == second["portfolio_payload_hash"]
        assert validate_portfolio_artifact(first) == []
    finally:
        if artifacts_root.exists():
            shutil.rmtree(artifacts_root)
