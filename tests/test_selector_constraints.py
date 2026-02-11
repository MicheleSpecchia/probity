from __future__ import annotations

from pmx.selector.compute import CandidateScore
from pmx.selector.constraints import enforce_constraints


def _score(
    market_id: str,
    *,
    score: float,
    lq: float,
    volume: float,
    bucket: str,
    category: str,
    group_id: str,
) -> CandidateScore:
    return CandidateScore(
        market_id=market_id,
        token_id=f"token-{market_id}",
        category=category,
        group_id=group_id,
        ttr_bucket=bucket,
        screen_score=score,
        lq=lq,
        volume_24h=volume,
        price_prob=0.5,
        components={"lq": lq},
        flags=(),
        penalties={},
        include_reasons=("by_volume",),
    )


def test_enforce_constraints_is_deterministic_and_respects_caps() -> None:
    scored = [
        _score(
            "m1", score=0.99, lq=0.9, volume=100, bucket="0_24h", category="sports", group_id="g1"
        ),
        _score(
            "m2", score=0.95, lq=0.8, volume=90, bucket="0_24h", category="sports", group_id="g1"
        ),
        _score(
            "m3", score=0.94, lq=0.7, volume=80, bucket="1_7d", category="politics", group_id="g2"
        ),
        _score(
            "m4", score=0.93, lq=0.75, volume=70, bucket="1_7d", category="politics", group_id="g2"
        ),
        _score(
            "m5", score=0.92, lq=0.6, volume=60, bucket="7_30d", category="crypto", group_id="g3"
        ),
        _score(
            "m6",
            score=0.91,
            lq=0.55,
            volume=50,
            bucket="30d_plus",
            category="crypto",
            group_id="g4",
        ),
    ]
    mix = {
        "0_24h": 0.4,
        "1_7d": 0.4,
        "7_30d": 0.1,
        "30d_plus": 0.1,
        "unknown": 0.0,
    }

    first = enforce_constraints(
        scored,
        k=4,
        target_bucket_mix=mix,
        max_per_category=2,
        max_per_group=1,
    )
    second = enforce_constraints(
        scored,
        k=4,
        target_bucket_mix=mix,
        max_per_category=2,
        max_per_group=1,
    )

    first_ids = [item.market_id for item in first.selected]
    second_ids = [item.market_id for item in second.selected]
    assert first_ids == second_ids
    assert len(first.selected) == 4
    assert all(count <= 2 for count in first.selected_by_category.values())
    assert all(count <= 1 for count in first.selected_by_group.values())
