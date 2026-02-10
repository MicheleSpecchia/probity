from __future__ import annotations

from datetime import UTC, datetime

from pmx.audit.run_context import build_run_context, compute_config_hash


def test_compute_config_hash_is_order_independent() -> None:
    left = {"b": 2, "a": {"d": [3, 4], "c": 1}}
    right = {"a": {"c": 1, "d": [3, 4]}, "b": 2}

    assert compute_config_hash(left) == compute_config_hash(right)


def test_build_run_context_is_deterministic_with_fixed_inputs() -> None:
    config = {"market_scope": "screening", "cadence_hours": 4}
    started_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)

    first = build_run_context(
        "screening",
        config,
        started_at=started_at,
        nonce="fixed-seed",
        code_version="abcdef123456",
    )
    second = build_run_context(
        "screening",
        config,
        started_at=started_at,
        nonce="fixed-seed",
        code_version="abcdef123456",
    )

    assert first.run_id == second.run_id
    assert first.config_hash == second.config_hash
    assert first.code_version == "abcdef123456"
    assert len(first.run_id) == 32
