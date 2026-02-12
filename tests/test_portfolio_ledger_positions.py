from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from pmx.portfolio.ledger import LedgerConfig, build_ledger
from pmx.portfolio.positions import apply_ledger_to_positions


def _load_execution_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "executions" / "execution_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def test_portfolio_ledger_and_positions_are_deterministic() -> None:
    execution_artifact = _load_execution_fixture()
    ledger_result = build_ledger([execution_artifact], LedgerConfig(fee_bps=0.0, fee_usd=0.0))

    assert ledger_result.counts["n_input_orders"] == 4
    assert ledger_result.counts["n_ledger_entries"] == 3
    assert ledger_result.counts["n_rejected"] == 1
    assert ledger_result.quality_flags == ()
    assert any(warning["code"] == "rejected_order" for warning in ledger_result.quality_warnings)
    assert [entry["token_id"] for entry in ledger_result.entries] == ["tok-b", "tok-a", "tok-e"]

    positions = apply_ledger_to_positions(ledger_result.entries)
    assert [position["token_id"] for position in positions] == ["tok-a", "tok-b", "tok-e"]
    assert [position["side"] for position in positions] == ["BUY_YES", "BUY_NO", "BUY_YES"]
    assert positions[0]["quantity"] == pytest.approx(192.31)
    assert positions[0]["avg_cost"] == pytest.approx(0.51999376)
    assert positions[1]["avg_cost"] == pytest.approx(0.5499945)
    assert positions[2]["avg_cost"] == pytest.approx(0.61001647)


def test_portfolio_ledger_ignores_duplicate_client_order_ids() -> None:
    execution_artifact = _load_execution_fixture()
    duplicate_artifact = copy.deepcopy(execution_artifact)
    duplicate_order = copy.deepcopy(duplicate_artifact["orders"][0])
    duplicate_order["market_id"] = "mkt-duplicate"
    duplicate_order["token_id"] = "tok-duplicate"
    duplicate_artifact["orders"].append(duplicate_order)

    ledger_result = build_ledger([duplicate_artifact], LedgerConfig(fee_bps=0.0, fee_usd=0.0))

    assert ledger_result.counts["n_input_orders"] == 5
    assert ledger_result.counts["n_duplicates"] == 1
    assert ledger_result.counts["n_ledger_entries"] == 3
    assert "duplicate_client_order_id" in ledger_result.quality_flags
    assert any(
        warning["code"] == "duplicate_client_order_id" for warning in ledger_result.quality_warnings
    )
