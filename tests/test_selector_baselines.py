from __future__ import annotations

from datetime import UTC, datetime

from pmx.selector.baselines import deterministic_seed, select_random_stratified, select_top_volume
from pmx.selector.compute import CandidateScore


def _candidate(market_id: str, *, volume: float, bucket: str, category: str) -> CandidateScore:
    return CandidateScore(
        market_id=market_id,
        token_id=f"token-{market_id}",
        category=category,
        group_id=f"group-{category}",
        ttr_bucket=bucket,
        screen_score=0.5,
        lq=0.5,
        volume_24h=volume,
        price_prob=0.5,
        components={"lq": 0.5},
        flags=(),
        penalties={},
        include_reasons=("by_volume",),
    )


def test_select_top_volume_orders_deterministically() -> None:
    candidates = [
        _candidate("m2", volume=10.0, bucket="1_7d", category="c1"),
        _candidate("m1", volume=30.0, bucket="0_24h", category="c1"),
        _candidate("m3", volume=20.0, bucket="7_30d", category="c2"),
    ]
    selected = select_top_volume(candidates, k=2)
    assert [item.market_id for item in selected] == ["m1", "m3"]


def test_random_stratified_is_seed_deterministic() -> None:
    candidates = [
        _candidate("m1", volume=11.0, bucket="0_24h", category="c1"),
        _candidate("m2", volume=12.0, bucket="0_24h", category="c1"),
        _candidate("m3", volume=13.0, bucket="1_7d", category="c2"),
        _candidate("m4", volume=14.0, bucket="1_7d", category="c2"),
        _candidate("m5", volume=15.0, bucket="7_30d", category="c3"),
    ]
    seed = deterministic_seed(datetime(2026, 2, 11, 12, 0, tzinfo=UTC), "cfg")
    first = select_random_stratified(candidates, k=3, seed=seed)
    second = select_random_stratified(candidates, k=3, seed=seed)
    third = select_random_stratified(candidates, k=3, seed=seed + 1)

    first_ids = [item.market_id for item in first]
    second_ids = [item.market_id for item in second]
    third_ids = [item.market_id for item in third]
    assert first_ids == second_ids
    assert first_ids != third_ids
