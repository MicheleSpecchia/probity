from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.portfolio.artifact import PORTFOLIO_POLICY_VERSION, build_portfolio_artifact
from pmx.portfolio.ledger import LedgerConfig, build_ledger
from pmx.portfolio.positions import apply_ledger_to_positions
from pmx.portfolio.validate_artifact import validate_portfolio_artifact
from pmx.portfolio.valuation import build_reference_prices, mark_to_model


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "executions" / "execution_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    execution_artifact = _load_fixture()
    config = LedgerConfig(fee_bps=0.0, fee_usd=0.0)
    ledger = build_ledger([execution_artifact], config)
    positions = apply_ledger_to_positions(ledger.entries)
    prices, warnings = build_reference_prices(
        [execution_artifact],
        mark_source="execution_price",
        external_prices=None,
    )
    valuation = mark_to_model(positions, reference_prices=prices, mark_source="execution_price")
    run_context = build_run_context(
        "portfolio_from_execution",
        {
            "input_execution_hashes": [execution_artifact["execution_payload_hash"]],
            "policy_version": PORTFOLIO_POLICY_VERSION,
            "params": {
                "artifacts_root": "artifacts",
                "fee_bps": 0.0,
                "fee_usd": 0.0,
                "mark_source": "execution_price",
                "reference_prices_json": None,
            },
        },
        started_at=datetime(2026, 2, 5, tzinfo=UTC),
        nonce="portfolio-artifact-schema-test",
    )
    return build_portfolio_artifact(
        run_context=run_context,
        execution_artifacts=[execution_artifact],
        params={
            "artifacts_root": "artifacts",
            "fee_bps": 0.0,
            "fee_usd": 0.0,
            "mark_source": "execution_price",
            "reference_prices_json": None,
        },
        ledger_entries=ledger.entries,
        positions=positions,
        valuation=valuation,
        quality_flags=list(ledger.quality_flags),
        quality_warnings=[*ledger.quality_warnings, *warnings],
        policy_version=PORTFOLIO_POLICY_VERSION,
    )


def test_portfolio_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    errors = validate_portfolio_artifact(artifact)
    assert errors == []


def test_portfolio_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("ledger_entries")

    errors = validate_portfolio_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "ledger_entries" in str(errors[0]["reason"])
