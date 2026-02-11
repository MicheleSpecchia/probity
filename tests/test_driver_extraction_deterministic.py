from __future__ import annotations

import pytest

from pmx.forecast.models import build_model_hash, compute_probabilities, extract_top_drivers


def test_driver_extraction_is_deterministic() -> None:
    features = {
        "mid_price": 0.46,
        "spread_bps": 120.0,
        "top_depth_bid": 15.0,
        "top_depth_ask": 11.0,
        "book_imbalance_1": 0.12,
        "last_trade_price": 0.45,
        "last_trade_size": 8.0,
        "trade_count_5m": 12,
        "volume_5m": 84.0,
        "return_5m": 0.03,
        "realized_vol_1h": 0.09,
        "stale_seconds_last_trade": 15,
        "stale_seconds_last_book": 8,
    }

    drivers_a = extract_top_drivers(features=features, price_prob=0.47, top_k=5)
    drivers_b = extract_top_drivers(features=features, price_prob=0.47, top_k=5)

    assert drivers_a == drivers_b
    assert len(drivers_a) == 5

    contributions = [abs(float(item["contribution"])) for item in drivers_a]
    assert contributions == sorted(contributions, reverse=True)


def test_probability_stack_outputs_are_bounded_and_hash_is_stable() -> None:
    features = {
        "mid_price": 0.52,
        "spread_bps": 80.0,
        "book_imbalance_1": 0.08,
        "trade_count_5m": 6,
        "volume_5m": 40.0,
        "return_5m": -0.01,
        "realized_vol_1h": 0.07,
        "stale_seconds_last_trade": 12,
        "stale_seconds_last_book": 5,
    }
    p_a, p_b, p_raw = compute_probabilities(price_prob=0.51, features=features)

    assert p_a == pytest.approx(0.51, abs=1e-12)
    assert 0.0 <= p_b <= 1.0
    assert 0.0 <= p_raw <= 1.0

    hash_a = build_model_hash()
    hash_b = build_model_hash()
    assert hash_a == hash_b
