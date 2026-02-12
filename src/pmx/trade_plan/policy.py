from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Literal, cast

SizingMode = Literal["fixed_notional", "scaled_by_edge"]
TradeSide = Literal["BUY_YES", "BUY_NO"]

_TRADABLE_ACTIONS = frozenset({"TRADE", "BUY_YES", "BUY_NO"})
_BLOCKING_QUALITY_FLAGS = frozenset(
    {
        "conformal_degenerate_intervals",
        "conformal_invalid_intervals",
        "illiquid",
        "insufficient_calibration_data",
        "insufficient_data",
        "insufficient_uncertainty_data",
        "poor_calibration",
        "stale",
    }
)


@dataclass(frozen=True, slots=True)
class TradePlanPolicyConfig:
    max_orders: int = 200
    max_total_notional_usd: float = 5000.0
    max_notional_per_market_usd: float = 500.0
    max_notional_per_category_usd: float = 2000.0
    sizing_mode: SizingMode = "fixed_notional"
    fixed_notional_usd: float = 25.0
    base_notional_usd: float = 25.0
    target_edge_bps: float = 100.0
    min_scale: float = 0.5
    max_scale: float = 2.0

    def __post_init__(self) -> None:
        if self.max_orders <= 0:
            raise ValueError("max_orders must be > 0")
        if self.max_total_notional_usd <= 0.0:
            raise ValueError("max_total_notional_usd must be > 0")
        if self.max_notional_per_market_usd <= 0.0:
            raise ValueError("max_notional_per_market_usd must be > 0")
        if self.max_notional_per_category_usd <= 0.0:
            raise ValueError("max_notional_per_category_usd must be > 0")
        if self.sizing_mode not in {"fixed_notional", "scaled_by_edge"}:
            raise ValueError(f"Unsupported sizing_mode: {self.sizing_mode!r}")
        if self.fixed_notional_usd <= 0.0:
            raise ValueError("fixed_notional_usd must be > 0")
        if self.base_notional_usd <= 0.0:
            raise ValueError("base_notional_usd must be > 0")
        if self.target_edge_bps <= 0.0:
            raise ValueError("target_edge_bps must be > 0")
        if self.min_scale <= 0.0:
            raise ValueError("min_scale must be > 0")
        if self.max_scale <= 0.0:
            raise ValueError("max_scale must be > 0")
        if self.min_scale > self.max_scale:
            raise ValueError("min_scale must be <= max_scale")

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "base_notional_usd": self.base_notional_usd,
            "fixed_notional_usd": self.fixed_notional_usd,
            "max_notional_per_category_usd": self.max_notional_per_category_usd,
            "max_notional_per_market_usd": self.max_notional_per_market_usd,
            "max_orders": self.max_orders,
            "max_scale": self.max_scale,
            "max_total_notional_usd": self.max_total_notional_usd,
            "min_scale": self.min_scale,
            "sizing_mode": self.sizing_mode,
            "target_edge_bps": self.target_edge_bps,
        }


@dataclass(frozen=True, slots=True)
class TradePlanResult:
    orders: tuple[dict[str, Any], ...]
    skipped: tuple[dict[str, Any], ...]
    counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class _DecisionItem:
    market_id: str
    token_id: str
    action: str
    category: str | None
    price_prob: float
    p_raw: float
    p_cal: float
    interval_50: tuple[float, float]
    interval_90: tuple[float, float]
    edge: float
    edge_bps: float
    quality_flags: tuple[str, ...]
    quality_warnings: tuple[dict[str, str], ...]
    no_trade_reasons: tuple[str, ...]


def build_trade_plan(
    decision_artifact: Mapping[str, Any],
    config: TradePlanPolicyConfig,
) -> TradePlanResult:
    raw_items_obj = decision_artifact.get("items")
    if not _is_sequence(raw_items_obj):
        raise ValueError("Decision artifact must include an 'items' list")
    raw_items = cast(Sequence[Any], raw_items_obj)

    parsed_items = [_parse_decision_item(item, index=index) for index, item in enumerate(raw_items)]
    ranked_items = sorted(
        parsed_items,
        key=lambda item: (-abs(item.edge_bps), item.market_id, item.token_id),
    )

    max_total_notional = _to_decimal(config.max_total_notional_usd)
    max_market_notional = _to_decimal(config.max_notional_per_market_usd)
    max_category_notional = _to_decimal(config.max_notional_per_category_usd)

    total_notional = Decimal("0")
    market_notional: dict[str, Decimal] = {}
    category_notional: dict[str, Decimal] = {}
    orders: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for item in ranked_items:
        precheck_reason = _precheck_reason(item)
        if precheck_reason is not None:
            skipped.append(_build_skipped_item(item, precheck_reason))
            continue

        notional = _compute_notional_usd(item.edge_bps, config)
        if notional <= Decimal("0"):
            skipped.append(_build_skipped_item(item, "invalid_notional"))
            continue

        if len(orders) >= config.max_orders:
            skipped.append(_build_skipped_item(item, "cap_exceeded:max_orders"))
            continue
        if total_notional + notional > max_total_notional:
            skipped.append(_build_skipped_item(item, "cap_exceeded:max_total_notional_usd"))
            continue

        current_market_notional = market_notional.get(item.market_id, Decimal("0"))
        if current_market_notional + notional > max_market_notional:
            skipped.append(_build_skipped_item(item, "cap_exceeded:max_notional_per_market_usd"))
            continue

        if item.category is not None:
            current_category_notional = category_notional.get(item.category, Decimal("0"))
            if current_category_notional + notional > max_category_notional:
                skipped.append(
                    _build_skipped_item(item, "cap_exceeded:max_notional_per_category_usd")
                )
                continue

        side = _trade_side(item.edge_bps)
        if side is None:
            skipped.append(_build_skipped_item(item, "zero_edge"))
            continue

        price = _execution_price(side, item.price_prob)
        if price <= Decimal("0"):
            skipped.append(_build_skipped_item(item, "invalid_price"))
            continue

        quantity = _round_money_decimal(notional / price)
        if quantity <= Decimal("0"):
            skipped.append(_build_skipped_item(item, "invalid_quantity"))
            continue

        order = {
            "rank": len(orders) + 1,
            "market_id": item.market_id,
            "token_id": item.token_id,
            "category": item.category,
            "action": "TRADE",
            "decision_action": item.action,
            "side": side,
            "notional_usd": float(notional),
            "price": _round_probability(float(price)),
            "quantity": float(quantity),
            "price_prob": _round_probability(item.price_prob),
            "p_raw": _round_probability(item.p_raw),
            "p_cal": _round_probability(item.p_cal),
            "interval_50": {
                "low": _round_probability(item.interval_50[0]),
                "high": _round_probability(item.interval_50[1]),
            },
            "interval_90": {
                "low": _round_probability(item.interval_90[0]),
                "high": _round_probability(item.interval_90[1]),
            },
            "edge": _round_probability(item.edge),
            "edge_bps": _round_bps(item.edge_bps),
            "quality_flags": list(item.quality_flags),
            "quality_warnings": [dict(entry) for entry in item.quality_warnings],
            "no_trade_reasons": [],
        }
        orders.append(order)

        total_notional += notional
        market_notional[item.market_id] = current_market_notional + notional
        if item.category is not None:
            category_notional[item.category] = category_notional.get(
                item.category, Decimal("0")
            ) + (notional)

    skipped_sorted = sorted(
        skipped,
        key=lambda entry: (
            str(entry.get("reason_code", "")),
            str(entry.get("market_id", "")),
            str(entry.get("token_id", "")),
        ),
    )
    counts = {
        "n_total": len(parsed_items),
        "n_orders": len(orders),
        "n_skipped": len(skipped_sorted),
    }
    return TradePlanResult(orders=tuple(orders), skipped=tuple(skipped_sorted), counts=counts)


def _parse_decision_item(raw_item: Any, *, index: int) -> _DecisionItem:
    if not isinstance(raw_item, Mapping):
        raise ValueError(f"items[{index}] must be an object")
    prefix = f"items[{index}]"
    market_id = _require_text(raw_item.get("market_id"), f"{prefix}.market_id")
    token_id = _require_text(raw_item.get("token_id"), f"{prefix}.token_id")
    action = _require_text(raw_item.get("action"), f"{prefix}.action")
    price_prob = _require_float(raw_item.get("price_prob"), f"{prefix}.price_prob")
    p_raw = _require_float(raw_item.get("p_raw"), f"{prefix}.p_raw")
    p_cal = _require_float(raw_item.get("p_cal"), f"{prefix}.p_cal")
    interval_50 = _require_interval(raw_item.get("interval_50"), f"{prefix}.interval_50")
    interval_90 = _require_interval(raw_item.get("interval_90"), f"{prefix}.interval_90")

    edge_raw = _optional_float(raw_item.get("edge"))
    edge_bps_raw = _optional_float(raw_item.get("edge_bps"))
    edge = edge_raw if edge_raw is not None else (p_cal - price_prob)
    edge_bps = edge_bps_raw if edge_bps_raw is not None else (edge * 10_000.0)

    return _DecisionItem(
        market_id=market_id,
        token_id=token_id,
        action=action,
        category=_optional_text(raw_item.get("category")),
        price_prob=price_prob,
        p_raw=p_raw,
        p_cal=p_cal,
        interval_50=interval_50,
        interval_90=interval_90,
        edge=edge,
        edge_bps=edge_bps,
        quality_flags=_normalize_string_list(raw_item.get("quality_flags")),
        quality_warnings=_normalize_warning_list(raw_item.get("quality_warnings")),
        no_trade_reasons=_normalize_string_list(raw_item.get("no_trade_reasons")),
    )


def _precheck_reason(item: _DecisionItem) -> str | None:
    if item.action == "NO_TRADE":
        if item.no_trade_reasons:
            return f"decision_no_trade:{item.no_trade_reasons[0]}"
        return "decision_no_trade"
    if item.action not in _TRADABLE_ACTIONS:
        return "invalid_decision_action"

    blocking_flags = [flag for flag in item.quality_flags if flag in _BLOCKING_QUALITY_FLAGS]
    if blocking_flags:
        return f"blocked_by_quality_flag:{blocking_flags[0]}"
    return None


def _compute_notional_usd(edge_bps: float, config: TradePlanPolicyConfig) -> Decimal:
    if config.sizing_mode == "fixed_notional":
        return _round_money_decimal(_to_decimal(config.fixed_notional_usd))

    scale = abs(edge_bps) / config.target_edge_bps
    bounded_scale = min(config.max_scale, max(config.min_scale, scale))
    notional = config.base_notional_usd * bounded_scale
    return _round_money_decimal(_to_decimal(notional))


def _trade_side(edge_bps: float) -> TradeSide | None:
    if edge_bps > 0.0:
        return "BUY_YES"
    if edge_bps < 0.0:
        return "BUY_NO"
    return None


def _execution_price(side: TradeSide, price_prob: float) -> Decimal:
    if side == "BUY_YES":
        return _to_decimal(price_prob)
    return _to_decimal(1.0 - price_prob)


def _build_skipped_item(item: _DecisionItem, reason_code: str) -> dict[str, Any]:
    return {
        "market_id": item.market_id,
        "token_id": item.token_id,
        "category": item.category,
        "decision_action": item.action,
        "edge_bps": _round_bps(item.edge_bps),
        "reason_code": reason_code,
        "quality_flags": list(item.quality_flags),
        "quality_warnings": [dict(entry) for entry in item.quality_warnings],
        "no_trade_reasons": list(item.no_trade_reasons),
    }


def _normalize_string_list(raw: Any) -> tuple[str, ...]:
    if not _is_sequence(raw):
        return ()
    values = [str(item).strip() for item in raw]
    return tuple(sorted({value for value in values if value}))


def _normalize_warning_list(raw: Any) -> tuple[dict[str, str], ...]:
    if not _is_sequence(raw):
        return ()
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("code", "")).strip() or "unknown_warning"
        message_raw = item.get("message", item.get("detail"))
        message = str(message_raw).strip() if message_raw is not None else ""
        payload: dict[str, str] = {"code": code}
        if message:
            payload["message"] = message
        deduped[(code, message)] = payload

    keys = sorted(deduped.keys(), key=lambda key: (key[0], key[1]))
    return tuple(deduped[key] for key in keys)


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _require_text(raw: Any, path: str) -> str:
    text = _optional_text(raw)
    if text is None:
        raise ValueError(f"{path} must be a non-empty string")
    return text


def _optional_float(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _require_float(raw: Any, path: str) -> float:
    value = _optional_float(raw)
    if value is None:
        raise ValueError(f"{path} must be a finite number")
    return value


def _require_interval(raw: Any, path: str) -> tuple[float, float]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must be an object with low/high")
    low = _require_float(raw.get("low"), f"{path}.low")
    high = _require_float(raw.get("high"), f"{path}.high")
    if low > high:
        raise ValueError(f"{path} invalid interval: low > high")
    return low, high


def _round_probability(value: float) -> float:
    return float(_to_decimal(value).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))


def _round_bps(value: float) -> float:
    return float(_to_decimal(value).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _round_money_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _to_decimal(value: float | int | str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"Decimal value must be finite: {value!r}")
    return result


def _is_sequence(raw: Any) -> bool:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
