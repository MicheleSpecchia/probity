from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

FEATURE_SET_VERSION = "micro_v1"

# Fixed quantization for reproducible floating outputs.
_PRICE_QUANT = Decimal("0.00000001")
_SIZE_QUANT = Decimal("0.00000001")
_RATIO_QUANT = Decimal("0.00000001")
_BPS_QUANT = Decimal("0.000001")
_RETURN_QUANT = Decimal("0.00000001")
_VOL_QUANT = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class BookSnapshotInput:
    event_ts: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    mid: Decimal | None


@dataclass(frozen=True, slots=True)
class TradeInput:
    event_ts: datetime
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class CandleInput:
    start_ts: datetime
    end_ts: datetime
    close: Decimal


def compute_micro_v1_features(
    *,
    decision_ts: datetime,
    book_snapshot: BookSnapshotInput | None,
    last_trade: TradeInput | None,
    trades_5m: Sequence[TradeInput],
    candles_1h: Sequence[CandleInput],
    anchor_price_5m: Decimal | None,
) -> dict[str, Any]:
    """Compute deterministic microstructure features for one token at one decision_ts.

    Feature formulas:
    - mid_price: snapshot mid if available, otherwise (best_bid + best_ask) / 2
    - spread_bps: ((best_ask - best_bid) / mid_price) * 10_000
    - top_depth_bid/top_depth_ask: sum of size at best bid/ask level
    - book_imbalance_1: (bid_depth_1 - ask_depth_1) / (bid_depth_1 + ask_depth_1)
    - last_trade_*: latest trade as-of
    - trade_count_5m / volume_5m: count and sum(size) in [t-5m, t]
    - return_5m: ln(reference_now / anchor_price_5m),
      reference_now = mid_price else last_trade_price
    - realized_vol_1h: stdev of 1m log returns in [t-1h, t]
    - stale_seconds_*: decision_ts - last_event_ts
    """

    decision_ts_utc = _as_utc_datetime(decision_ts)
    bids = (
        tuple(sorted(book_snapshot.bids, key=lambda level: level.price, reverse=True))
        if book_snapshot
        else ()
    )
    asks = tuple(sorted(book_snapshot.asks, key=lambda level: level.price)) if book_snapshot else ()

    best_bid = bids[0].price if bids else None
    best_ask = asks[0].price if asks else None

    mid_price_decimal = _resolve_mid_price(book_snapshot, best_bid, best_ask)
    spread_bps_decimal = _compute_spread_bps(best_bid, best_ask, mid_price_decimal)
    top_depth_bid_decimal = _top_depth_at_best_level(bids, descending=True)
    top_depth_ask_decimal = _top_depth_at_best_level(asks, descending=False)
    book_imbalance_decimal = _book_imbalance(top_depth_bid_decimal, top_depth_ask_decimal)

    sorted_trades = tuple(sorted(trades_5m, key=lambda trade: trade.event_ts))
    trade_count_5m = len(sorted_trades)
    volume_5m_decimal = sum((trade.size for trade in sorted_trades), Decimal("0"))
    last_trade_price_decimal = last_trade.price if last_trade is not None else None
    last_trade_size_decimal = last_trade.size if last_trade is not None else None

    reference_now = mid_price_decimal if mid_price_decimal is not None else last_trade_price_decimal
    return_5m_decimal = _compute_log_return(reference_now, anchor_price_5m)

    sorted_candles = tuple(sorted(candles_1h, key=lambda candle: candle.start_ts))
    realized_vol_decimal = _realized_volatility(sorted_candles)

    stale_seconds_last_trade = (
        _stale_seconds(decision_ts_utc, last_trade.event_ts) if last_trade is not None else None
    )
    stale_seconds_last_book = (
        _stale_seconds(decision_ts_utc, book_snapshot.event_ts)
        if book_snapshot is not None
        else None
    )

    return {
        "mid_price": _decimal_to_float(mid_price_decimal, _PRICE_QUANT),
        "spread_bps": _decimal_to_float(spread_bps_decimal, _BPS_QUANT),
        "top_depth_bid": _decimal_to_float(top_depth_bid_decimal, _SIZE_QUANT),
        "top_depth_ask": _decimal_to_float(top_depth_ask_decimal, _SIZE_QUANT),
        "book_imbalance_1": _decimal_to_float(book_imbalance_decimal, _RATIO_QUANT),
        "last_trade_price": _decimal_to_float(last_trade_price_decimal, _PRICE_QUANT),
        "last_trade_size": _decimal_to_float(last_trade_size_decimal, _SIZE_QUANT),
        "trade_count_5m": trade_count_5m,
        "volume_5m": _decimal_to_float(volume_5m_decimal, _SIZE_QUANT),
        "return_5m": _decimal_to_float(return_5m_decimal, _RETURN_QUANT),
        "realized_vol_1h": _decimal_to_float(realized_vol_decimal, _VOL_QUANT),
        "stale_seconds_last_trade": stale_seconds_last_trade,
        "stale_seconds_last_book": stale_seconds_last_book,
    }


def _resolve_mid_price(
    snapshot: BookSnapshotInput | None,
    best_bid: Decimal | None,
    best_ask: Decimal | None,
) -> Decimal | None:
    if snapshot is not None and snapshot.mid is not None and snapshot.mid > 0:
        return snapshot.mid
    if best_bid is None or best_ask is None:
        return None
    if best_bid <= 0 or best_ask <= 0:
        return None
    return (best_bid + best_ask) / Decimal("2")


def _compute_spread_bps(
    best_bid: Decimal | None,
    best_ask: Decimal | None,
    mid_price: Decimal | None,
) -> Decimal | None:
    if best_bid is None or best_ask is None or mid_price is None:
        return None
    if mid_price <= 0:
        return None
    return ((best_ask - best_bid) / mid_price) * Decimal("10000")


def _top_depth_at_best_level(levels: Sequence[BookLevel], *, descending: bool) -> Decimal:
    if not levels:
        return Decimal("0")
    best_price = levels[0].price
    depth = Decimal("0")
    for level in levels:
        if level.price != best_price:
            break
        depth += level.size
    if depth < 0:
        return Decimal("0")
    return depth


def _book_imbalance(bid_depth: Decimal, ask_depth: Decimal) -> Decimal | None:
    denom = bid_depth + ask_depth
    if denom <= 0:
        return None
    return (bid_depth - ask_depth) / denom


def _compute_log_return(
    current_price: Decimal | None, anchor_price: Decimal | None
) -> Decimal | None:
    if current_price is None or anchor_price is None:
        return None
    if current_price <= 0 or anchor_price <= 0:
        return None
    raw = math.log(float(current_price / anchor_price))
    return Decimal(str(raw))


def _realized_volatility(candles: Sequence[CandleInput]) -> Decimal | None:
    closes = [candle.close for candle in candles if candle.close > 0]
    if len(closes) < 2:
        return None

    returns: list[float] = []
    for index in range(1, len(closes)):
        ratio = closes[index] / closes[index - 1]
        if ratio <= 0:
            continue
        returns.append(math.log(float(ratio)))
    if not returns:
        return None

    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / len(returns)
    return Decimal(str(math.sqrt(variance)))


def _stale_seconds(decision_ts: datetime, event_ts: datetime) -> int:
    event_utc = _as_utc_datetime(event_ts)
    delta = int((decision_ts - event_utc).total_seconds())
    return delta if delta >= 0 else 0


def _decimal_to_float(value: Decimal | None, quant: Decimal) -> float | None:
    if value is None:
        return None
    try:
        quantized = value.quantize(quant)
    except (InvalidOperation, ValueError):
        return None
    return float(quantized)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
