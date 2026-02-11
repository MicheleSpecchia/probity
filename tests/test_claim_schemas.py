from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pmx.claims.validate import (
    PayloadValidationError,
    validate_claim_extract,
    validate_evidence_checklist,
)


def _load_fixture(name: str) -> dict[str, Any]:
    fixture_path = Path(__file__).with_name("fixtures") / "claims" / name
    with fixture_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def test_validate_claim_extract_accepts_valid_fixture() -> None:
    payload = _load_fixture("valid_claim_extract.json")

    validated = validate_claim_extract(payload)

    assert validated.schema_version == "claim_extract.v1"
    assert validated.raw_claim_count == 1
    assert validated.canonical_claim_count == 1
    assert list(validated.payload.keys()) == sorted(validated.payload.keys())


def test_validate_claim_extract_rejects_too_many_sources() -> None:
    payload = _load_fixture("invalid_claim_extract_too_many_sources.json")

    with pytest.raises(PayloadValidationError) as exc_info:
        validate_claim_extract(payload)

    issues = list(exc_info.value.issues)
    assert len(issues) >= 1
    assert any(
        issue.code == "max_sources_per_claim_exceeded"
        and issue.path == "$.claims[0].sources"
        and issue.reason == "source count 11 exceeds 10"
        for issue in issues
    )


def test_validate_claim_extract_rejects_duplicate_source_url_after_canonicalization() -> None:
    payload = _load_fixture("valid_claim_extract.json")
    payload["claims"][0]["sources"].append(
        {"url": ("https://www.reuters.com/world/us/example-election-story?utm_source=email")}
    )

    with pytest.raises(PayloadValidationError) as exc_info:
        validate_claim_extract(payload)

    issues = list(exc_info.value.issues)
    assert any(
        issue.code == "duplicate_source_url" and issue.path == "$.claims[0].sources[2].url"
        for issue in issues
    )


def test_validate_evidence_checklist_accepts_valid_fixture() -> None:
    payload = _load_fixture("valid_evidence_checklist.json")

    validated = validate_evidence_checklist(payload)

    assert validated.schema_version == "evidence_checklist.v1"
    assert validated.item_count == 1
    assert list(validated.payload.keys()) == sorted(validated.payload.keys())


def test_validate_evidence_checklist_rejects_missing_required_fields() -> None:
    payload = _load_fixture("invalid_checklist_missing_required.json")

    with pytest.raises(PayloadValidationError) as exc_info:
        validate_evidence_checklist(payload)

    issues = list(exc_info.value.issues)
    assert any(
        issue.code == "schema:required"
        and issue.path == "$"
        and "'items' is a required property" in issue.reason
        for issue in issues
    )
