from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def _load_probe_module() -> Any:
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "wss_probe.py"
    spec = importlib.util.spec_from_file_location("wss_probe", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load scripts/wss_probe.py module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_fixtures() -> list[tuple[str, Any]]:
    fixtures_dir = Path(__file__).with_name("fixtures") / "wss"
    fixtures: list[tuple[str, Any]] = []
    for path in sorted(fixtures_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            fixtures.append((path.name, json.load(handle)))
    return fixtures


def test_wss_fixtures_detect_message_types_and_seq_support() -> None:
    module = _load_probe_module()
    fixture_items = _load_fixtures()
    messages = [payload for _, payload in fixture_items]

    summary = module.analyze_messages(messages)
    expected_supports_seq = True

    seq_fields_by_fixture: dict[str, list[str]] = {}
    for filename, payload in fixture_items:
        fields = sorted(module.collect_seq_like_fields(payload))
        if fields:
            seq_fields_by_fixture[filename] = fields

    assert summary.message_count == len(messages)
    assert len(summary.message_types) >= 1
    assert "trade" in summary.message_types
    assert summary.supports_seq is expected_supports_seq, (
        "supports_seq mismatch: "
        f"expected={expected_supports_seq}, actual={summary.supports_seq}, "
        f"seq_like_fields={list(summary.seq_like_fields)}, "
        f"seq_fields_by_fixture={seq_fields_by_fixture}"
    )
    assert "seq" in summary.seq_like_fields
    assert "sequence" in summary.seq_like_fields
    assert "offset" in summary.seq_like_fields
