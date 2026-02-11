from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.selector.ttr import estimate_resolution_ts, estimate_ttr_bucket


def test_ttr_cases_fixture() -> None:
    fixture_path = Path("tests/fixtures/selector/ttr_cases.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)

    for case in payload:
        assert isinstance(case, dict)
        market_payload = case["market_payload"]
        decision_ts = _parse_datetime(case["decision_ts"])
        expected_bucket = str(case["expected_bucket"])
        expected_resolution = case.get("expected_resolution_ts")

        resolution = estimate_resolution_ts(_as_mapping(market_payload))
        if expected_resolution is None:
            assert resolution is None, case["name"]
        else:
            assert resolution is not None, case["name"]
            assert resolution.isoformat() == str(expected_resolution), case["name"]

        bucket = estimate_ttr_bucket(_as_mapping(market_payload), decision_ts)
        assert bucket == expected_bucket, case["name"]


def _parse_datetime(raw: Any) -> datetime:
    text = str(raw).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _as_mapping(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return {str(key): value for key, value in raw.items()}
    return {}
