from __future__ import annotations

from pmx.selector.compute import CandidateScore, compute_deep_scores
from pmx.selector.spec import compute_deep_score, compute_screen_score


def test_compute_deep_score_is_deterministic() -> None:
    screen = compute_screen_score(
        features={
            "spread_bps": 120.0,
            "top_depth_bid": 80.0,
            "top_depth_ask": 75.0,
            "book_imbalance_1": 0.08,
            "return_5m": 0.03,
            "realized_vol_1h": 0.05,
            "stale_seconds_last_trade": 20.0,
            "stale_seconds_last_book": 10.0,
        },
        price_prob=0.52,
        market_payload={
            "title": "Will event happen by 2026-02-20?",
            "description": "Binary market with clear rule text.",
            "rule_text": "Resolves YES if event happens.",
            "rule_parse_ok": True,
        },
    )
    first = compute_deep_score(score_result=screen, ttr_bucket="1_7d", price_prob=0.52)
    second = compute_deep_score(score_result=screen, ttr_bucket="1_7d", price_prob=0.52)

    assert first.deep_score == second.deep_score
    assert first.components == second.components
    assert first.penalties == second.penalties
    assert first.reason_hash == second.reason_hash


def test_compute_deep_scores_uses_stable_tiebreaks() -> None:
    candidates = [
        _candidate("m2", deep_score=0.8, screen_score=0.7, volume=100.0),
        _candidate("m1", deep_score=0.8, screen_score=0.7, volume=100.0),
        _candidate("m3", deep_score=0.9, screen_score=0.4, volume=1.0),
    ]
    ranked = compute_deep_scores(candidates)
    assert [item.market_id for item in ranked] == ["m3", "m1", "m2"]


def _candidate(
    market_id: str,
    *,
    deep_score: float,
    screen_score: float,
    volume: float,
) -> CandidateScore:
    return CandidateScore(
        market_id=market_id,
        token_id=f"token-{market_id}",
        category="test",
        group_id="group",
        ttr_bucket="1_7d",
        screen_score=screen_score,
        lq=0.5,
        volume_24h=volume,
        price_prob=0.5,
        components={"lq": 0.5, "pq": 0.5, "vo": 0.5, "dc": 0.5, "rc": 0.5, "penalty_sum": 0.0},
        flags=(),
        penalties={},
        include_reasons=("by_volume",),
        deep_score=deep_score,
        deep_components={"screen_score": screen_score},
        deep_flags=(),
        deep_penalties={},
        deep_reason_hash="hash",
    )
