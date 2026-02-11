from __future__ import annotations

from pmx.selector.spec import SelectorConfig, compute_screen_score


def test_selector_score_hard_flag_illiquid_sets_zero() -> None:
    result = compute_screen_score(
        features={
            "spread_bps": 5000.0,
            "top_depth_bid": 0.1,
            "top_depth_ask": 0.1,
            "stale_seconds_last_trade": 10.0,
            "stale_seconds_last_book": 10.0,
        },
        price_prob=0.5,
        market_payload={"title": "A", "rule_parse_ok": True},
        config=SelectorConfig(),
    )
    assert result.screen_score == 0.0
    assert "illiquid" in result.flags


def test_selector_score_is_deterministic() -> None:
    kwargs = {
        "features": {
            "spread_bps": 100.0,
            "top_depth_bid": 50.0,
            "top_depth_ask": 60.0,
            "stale_seconds_last_trade": 30.0,
            "stale_seconds_last_book": 15.0,
            "book_imbalance_1": 0.1,
            "return_5m": 0.02,
            "realized_vol_1h": 0.03,
        },
        "price_prob": 0.52,
        "market_payload": {
            "title": "Election market",
            "description": "Clear binary rule",
            "rule_text": "Resolves YES if event happens.",
            "rule_parse_ok": True,
        },
    }
    first = compute_screen_score(**kwargs)
    second = compute_screen_score(**kwargs)

    assert first.screen_score == second.screen_score
    assert first.components == second.components
    assert first.reason_hash == second.reason_hash
