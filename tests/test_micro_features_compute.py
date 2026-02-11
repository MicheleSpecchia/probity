from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256

import pytest

from pmx.features.spec_micro_v1 import (
    BookLevel,
    BookSnapshotInput,
    CandleInput,
    TradeInput,
    compute_micro_v1_features,
)


def test_compute_micro_v1_features_expected_values() -> None:
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    snapshot = BookSnapshotInput(
        event_ts=decision_ts - timedelta(seconds=60),
        bids=(
            BookLevel(price=Decimal("0.45000000"), size=Decimal("5.00000000")),
            BookLevel(price=Decimal("0.44000000"), size=Decimal("2.00000000")),
        ),
        asks=(
            BookLevel(price=Decimal("0.47000000"), size=Decimal("3.00000000")),
            BookLevel(price=Decimal("0.48000000"), size=Decimal("1.00000000")),
        ),
        mid=None,
    )
    trades_5m = (
        TradeInput(
            event_ts=decision_ts - timedelta(minutes=4),
            price=Decimal("0.44000000"),
            size=Decimal("2.00000000"),
        ),
        TradeInput(
            event_ts=decision_ts - timedelta(minutes=2),
            price=Decimal("0.46000000"),
            size=Decimal("3.00000000"),
        ),
    )
    last_trade = trades_5m[-1]
    candles_1h = (
        CandleInput(
            start_ts=decision_ts - timedelta(minutes=3),
            end_ts=decision_ts - timedelta(minutes=2),
            close=Decimal("0.40000000"),
        ),
        CandleInput(
            start_ts=decision_ts - timedelta(minutes=2),
            end_ts=decision_ts - timedelta(minutes=1),
            close=Decimal("0.42000000"),
        ),
        CandleInput(
            start_ts=decision_ts - timedelta(minutes=1),
            end_ts=decision_ts,
            close=Decimal("0.41000000"),
        ),
    )

    features = compute_micro_v1_features(
        decision_ts=decision_ts,
        book_snapshot=snapshot,
        last_trade=last_trade,
        trades_5m=trades_5m,
        candles_1h=candles_1h,
        anchor_price_5m=Decimal("0.44000000"),
    )

    assert features["mid_price"] == pytest.approx(0.46, abs=1e-8)
    assert features["spread_bps"] == pytest.approx(434.782609, abs=1e-6)
    assert features["top_depth_bid"] == pytest.approx(5.0, abs=1e-8)
    assert features["top_depth_ask"] == pytest.approx(3.0, abs=1e-8)
    assert features["book_imbalance_1"] == pytest.approx(0.25, abs=1e-8)
    assert features["last_trade_price"] == pytest.approx(0.46, abs=1e-8)
    assert features["last_trade_size"] == pytest.approx(3.0, abs=1e-8)
    assert features["trade_count_5m"] == 2
    assert features["volume_5m"] == pytest.approx(5.0, abs=1e-8)
    assert features["return_5m"] == pytest.approx(0.04445176, abs=1e-8)
    assert features["realized_vol_1h"] is not None
    assert features["stale_seconds_last_trade"] == 120
    assert features["stale_seconds_last_book"] == 60


def test_compute_micro_v1_features_is_deterministic() -> None:
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    snapshot = BookSnapshotInput(
        event_ts=decision_ts,
        bids=(BookLevel(price=Decimal("0.50000000"), size=Decimal("1.00000000")),),
        asks=(BookLevel(price=Decimal("0.51000000"), size=Decimal("1.00000000")),),
        mid=Decimal("0.50500000"),
    )
    trades_5m = (
        TradeInput(
            event_ts=decision_ts - timedelta(seconds=1),
            price=Decimal("0.50000000"),
            size=Decimal("1.50000000"),
        ),
    )
    candles_1h: tuple[CandleInput, ...] = ()

    features_a = compute_micro_v1_features(
        decision_ts=decision_ts,
        book_snapshot=snapshot,
        last_trade=trades_5m[0],
        trades_5m=trades_5m,
        candles_1h=candles_1h,
        anchor_price_5m=Decimal("0.50000000"),
    )
    features_b = compute_micro_v1_features(
        decision_ts=decision_ts,
        book_snapshot=snapshot,
        last_trade=trades_5m[0],
        trades_5m=trades_5m,
        candles_1h=candles_1h,
        anchor_price_5m=Decimal("0.50000000"),
    )

    digest_a = sha256(json.dumps(features_a, sort_keys=True).encode("utf-8")).hexdigest()
    digest_b = sha256(json.dumps(features_b, sort_keys=True).encode("utf-8")).hexdigest()
    assert digest_a == digest_b
