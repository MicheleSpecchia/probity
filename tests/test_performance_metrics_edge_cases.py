from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pmx.performance.metrics import compute_performance_metrics


def _load_fixture(name: str) -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "portfolio" / name
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _clone(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload))


def test_performance_metrics_flags_insufficient_inputs_and_zero_notional() -> None:
    payload = _clone(_load_fixture("portfolio_artifact_sample_A.json"))
    valuation = payload["valuation"]["summary"]
    valuation["total_notional_exposure_usd"] = 0.0
    valuation["total_unrealized_pnl_usd"] = 0.0

    _, aggregate, flags, warnings = compute_performance_metrics([payload])

    assert "insufficient_inputs" in flags
    assert "zero_notional" in flags
    assert aggregate["n_inputs"] == 1
    assert aggregate["coverage"]["zero_notional_inputs"] == 1
    assert any(item["code"] == "zero_notional" for item in warnings)


def test_performance_metrics_flags_concentration_and_large_negative_pnl() -> None:
    first = _load_fixture("portfolio_artifact_sample_A.json")
    second = _load_fixture("portfolio_artifact_sample_B.json")

    per_run, _, flags, warnings = compute_performance_metrics([first, second])
    second_metrics = next(
        item for item in per_run if item["portfolio_run_id"] == "portfolio-run-sample-b"
    )

    assert second_metrics["concentration"]["top1_notional_share"] > 0.5
    assert second_metrics["concentration"]["top3_notional_share"] > 0.8
    assert "extreme_concentration_top1" in flags
    assert "extreme_concentration_top3" in flags
    assert "negative_pnl_large" in flags
    assert any(item["code"] == "negative_pnl_large" for item in warnings)
