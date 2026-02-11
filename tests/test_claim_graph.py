from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from pmx.claims.canonicalize import canonicalize_claims
from pmx.claims.graph import build_claim_graph


def _load_fixture(name: str) -> dict[str, Any]:
    fixture_path = Path(__file__).with_name("fixtures") / "claims" / name
    with fixture_path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    if not isinstance(loaded, dict):
        raise ValueError(f"Invalid fixture payload: {name}")
    return loaded


def _result_digest(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def test_build_claim_graph_clusters_similar_claims_deterministically() -> None:
    fixture = _load_fixture("graph_input_claims.json")
    claims_raw = fixture["claims_raw"]

    result_a = build_claim_graph(claims_raw, similarity_threshold=0.5)
    result_b = build_claim_graph(list(reversed(claims_raw)), similarity_threshold=0.5)

    assert result_a.as_dict() == result_b.as_dict()
    assert len(result_a.clusters) == 31
    assert result_a.claim_to_cluster[1] == 1
    assert result_a.claim_to_cluster[2] == 1
    assert result_a.claim_to_cluster[3] == 1
    assert result_a.clusters[0].claim_ids == (1, 2, 3)


def test_canonicalize_claims_enforces_cap_and_stable_dropped_ids() -> None:
    fixture = _load_fixture("graph_input_claims.json")
    claims_raw = fixture["claims_raw"]

    result = canonicalize_claims(claims_raw, max_canonical=25, similarity_threshold=0.5)

    assert len(result.canonical_claims) == 25
    assert result.canonical_claims[0].claim_ids == (1, 2, 3)
    assert result.dropped_claim_ids == (28, 29, 30, 31, 32, 33)
    assert result.claim_to_canonical[1] == 1
    assert 28 not in result.claim_to_canonical


def test_canonicalize_claims_echo_and_diversity_metrics() -> None:
    fixture = _load_fixture("graph_input_claims.json")
    claims_raw = fixture["claims_raw"]

    result = canonicalize_claims(claims_raw, max_canonical=25, similarity_threshold=0.5)
    first = result.canonical_claims[0]

    assert first.metrics.unique_domains == 2
    assert first.metrics.primary_domains == 2
    assert first.metrics.diversity_score == pytest.approx(2 / 3, abs=1e-9)
    assert first.metrics.echo_penalty == pytest.approx(1 / 3, abs=1e-9)


def test_canonicalize_claims_is_fully_reproducible() -> None:
    fixture = _load_fixture("graph_input_claims.json")
    claims_raw = fixture["claims_raw"]

    result_a = canonicalize_claims(claims_raw, max_canonical=25, similarity_threshold=0.5)
    result_b = canonicalize_claims(claims_raw, max_canonical=25, similarity_threshold=0.5)

    digest_a = _result_digest(result_a.as_dict())
    digest_b = _result_digest(result_b.as_dict())
    assert digest_a == digest_b
