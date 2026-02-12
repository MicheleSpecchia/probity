from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Literal, TypeGuard

RiskVerdict = Literal["ALLOW", "BLOCK", "DOWNSIZE"]

DEFAULT_BLOCKING_QUALITY_FLAGS: tuple[str, ...] = (
    "conformal_degenerate_intervals",
    "conformal_invalid_intervals",
    "illiquid",
    "insufficient_calibration_data",
    "insufficient_data",
    "insufficient_uncertainty_data",
    "poor_calibration",
    "stale",
)
DEFAULT_COOLDOWN_BLOCK_FLAGS: tuple[str, ...] = (
    "critical_drawdown",
    "critical_liquidity",
    "critical_loss",
)


@dataclass(frozen=True, slots=True)
class RiskPolicyConfig:
    max_total_notional_usd: float = 5000.0
    max_notional_per_market_usd: float = 500.0
    max_notional_per_category_usd: float = 2000.0
    top1_share_cap: float = 0.50
    top3_share_cap: float = 0.80
    performance_top1_cap: float = 0.50
    performance_top3_cap: float = 0.80
    allow_downsize: bool = True
    min_notional_usd: float = 5.0
    blocking_quality_flags: tuple[str, ...] = DEFAULT_BLOCKING_QUALITY_FLAGS
    cooldown_block_flags: tuple[str, ...] = DEFAULT_COOLDOWN_BLOCK_FLAGS

    def __post_init__(self) -> None:
        if self.max_total_notional_usd <= 0.0:
            raise ValueError("max_total_notional_usd must be > 0")
        if self.max_notional_per_market_usd <= 0.0:
            raise ValueError("max_notional_per_market_usd must be > 0")
        if self.max_notional_per_category_usd <= 0.0:
            raise ValueError("max_notional_per_category_usd must be > 0")
        if not 0.0 < self.top1_share_cap <= 1.0:
            raise ValueError("top1_share_cap must be in (0,1]")
        if not 0.0 < self.top3_share_cap <= 1.0:
            raise ValueError("top3_share_cap must be in (0,1]")
        if self.top3_share_cap < self.top1_share_cap:
            raise ValueError("top3_share_cap must be >= top1_share_cap")
        if not 0.0 < self.performance_top1_cap <= 1.0:
            raise ValueError("performance_top1_cap must be in (0,1]")
        if not 0.0 < self.performance_top3_cap <= 1.0:
            raise ValueError("performance_top3_cap must be in (0,1]")
        if self.performance_top3_cap < self.performance_top1_cap:
            raise ValueError("performance_top3_cap must be >= performance_top1_cap")
        if self.min_notional_usd <= 0.0:
            raise ValueError("min_notional_usd must be > 0")
        if not self.blocking_quality_flags:
            raise ValueError("blocking_quality_flags must be non-empty")

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "allow_downsize": self.allow_downsize,
            "blocking_quality_flags": sorted(set(self.blocking_quality_flags)),
            "cooldown_block_flags": sorted(set(self.cooldown_block_flags)),
            "max_notional_per_category_usd": self.max_notional_per_category_usd,
            "max_notional_per_market_usd": self.max_notional_per_market_usd,
            "max_total_notional_usd": self.max_total_notional_usd,
            "min_notional_usd": self.min_notional_usd,
            "performance_top1_cap": self.performance_top1_cap,
            "performance_top3_cap": self.performance_top3_cap,
            "top1_share_cap": self.top1_share_cap,
            "top3_share_cap": self.top3_share_cap,
        }


@dataclass(frozen=True, slots=True)
class RiskHooks:
    current_total_notional_usd: Decimal
    current_notional_by_market: Mapping[str, Decimal]
    current_notional_by_category: Mapping[str, Decimal]
    cooldown_tokens: Mapping[str, tuple[str, ...]]
    cooldown_markets: Mapping[str, tuple[str, ...]]

    @classmethod
    def empty(cls) -> RiskHooks:
        return cls(
            current_total_notional_usd=Decimal("0"),
            current_notional_by_market={},
            current_notional_by_category={},
            cooldown_tokens={},
            cooldown_markets={},
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> RiskHooks:
        if payload is None:
            return cls.empty()
        current_market_raw = payload.get("current_notional_by_market")
        current_category_raw = payload.get("current_notional_by_category")
        cooldown_tokens_raw = payload.get("cooldown_tokens")
        cooldown_markets_raw = payload.get("cooldown_markets")
        return cls(
            current_total_notional_usd=_round_money_decimal(
                _to_decimal(payload.get("current_total_notional_usd", 0.0))
            ),
            current_notional_by_market=_decimal_map(current_market_raw),
            current_notional_by_category=_decimal_map(current_category_raw),
            cooldown_tokens=_cooldown_map(cooldown_tokens_raw),
            cooldown_markets=_cooldown_map(cooldown_markets_raw),
        )


@dataclass(frozen=True, slots=True)
class RiskPolicyResult:
    items: tuple[dict[str, Any], ...]
    counts: dict[str, int]
    notional_summary: dict[str, float]
    quality_flags: tuple[str, ...]
    quality_warnings: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class _OrderInput:
    rank: int
    market_id: str
    token_id: str
    category: str | None
    side: str
    requested_notional: Decimal
    quality_flags: tuple[str, ...]
    quality_warnings: tuple[dict[str, str], ...]


def evaluate_risk_policy(
    trade_plan_artifact: Mapping[str, Any],
    config: RiskPolicyConfig,
    *,
    performance_artifact: Mapping[str, Any] | None = None,
    hooks: RiskHooks | None = None,
) -> RiskPolicyResult:
    hooks_obj = hooks or RiskHooks.empty()
    parsed_orders = _parse_trade_plan_orders(trade_plan_artifact)
    performance_reasons, performance_flags, performance_warnings = _performance_guards(
        performance_artifact,
        config=config,
    )

    max_total = _to_decimal(config.max_total_notional_usd)
    max_market = _to_decimal(config.max_notional_per_market_usd)
    max_category = _to_decimal(config.max_notional_per_category_usd)
    min_notional = _to_decimal(config.min_notional_usd)
    total_notional = hooks_obj.current_total_notional_usd
    market_notional: dict[str, Decimal] = dict(hooks_obj.current_notional_by_market)
    category_notional: dict[str, Decimal] = dict(hooks_obj.current_notional_by_category)

    items: list[dict[str, Any]] = []
    quality_flags: set[str] = set(performance_flags)
    warning_map: dict[tuple[str, str], dict[str, str]] = {}
    for entry in performance_warnings:
        warning_map[(entry["code"], entry.get("message", ""))] = dict(entry)

    for order in parsed_orders:
        reasons: list[str] = []
        reasons.extend(performance_reasons)
        reasons.extend(_blocking_flag_reasons(order, config=config))
        reasons.extend(_cooldown_reasons(order, hooks_obj=hooks_obj, config=config))

        if reasons:
            items.append(
                _build_item(
                    order=order,
                    verdict="BLOCK",
                    approved_notional=Decimal("0"),
                    reason_codes=tuple(sorted(set(reasons))),
                )
            )
            continue

        requested = order.requested_notional
        if requested <= Decimal("0"):
            items.append(
                _build_item(
                    order=order,
                    verdict="BLOCK",
                    approved_notional=Decimal("0"),
                    reason_codes=("invalid_notional",),
                )
            )
            continue

        total_remaining = max_total - total_notional
        market_remaining = max_market - market_notional.get(order.market_id, Decimal("0"))
        category_remaining = (
            max_category - category_notional.get(order.category, Decimal("0"))
            if order.category is not None
            else requested
        )
        allowed = min(requested, total_remaining, market_remaining, category_remaining)
        cap_reasons = _cap_reasons(
            total_remaining=total_remaining,
            market_remaining=market_remaining,
            category_remaining=category_remaining,
            category=order.category,
        )
        if allowed <= Decimal("0"):
            items.append(
                _build_item(
                    order=order,
                    verdict="BLOCK",
                    approved_notional=Decimal("0"),
                    reason_codes=cap_reasons or ("cap_exceeded:unknown",),
                )
            )
            continue

        concentration_reason = _concentration_reason(
            market_id=order.market_id,
            add_notional=allowed,
            total_notional=total_notional,
            market_notional=market_notional,
            top1_cap=config.top1_share_cap,
            top3_cap=config.top3_share_cap,
        )
        if concentration_reason is not None:
            items.append(
                _build_item(
                    order=order,
                    verdict="BLOCK",
                    approved_notional=Decimal("0"),
                    reason_codes=(concentration_reason,),
                )
            )
            quality_flags.add(concentration_reason)
            _add_warning(
                warning_map,
                code=concentration_reason,
                message=(
                    f"Order {order.market_id}/{order.token_id} blocked by concentration guard."
                ),
            )
            continue

        if allowed < requested:
            if config.allow_downsize and allowed >= min_notional:
                reason = cap_reasons[0] if cap_reasons else "downsized_by_cap:unknown"
                items.append(
                    _build_item(
                        order=order,
                        verdict="DOWNSIZE",
                        approved_notional=allowed,
                        reason_codes=(reason.replace("cap_exceeded", "downsized_by_cap"),),
                    )
                )
                total_notional += allowed
                market_notional[order.market_id] = (
                    market_notional.get(order.market_id, Decimal("0")) + allowed
                )
                if order.category is not None:
                    category_notional[order.category] = (
                        category_notional.get(order.category, Decimal("0")) + allowed
                    )
                continue

            items.append(
                _build_item(
                    order=order,
                    verdict="BLOCK",
                    approved_notional=Decimal("0"),
                    reason_codes=cap_reasons or ("cap_exceeded:unknown",),
                )
            )
            continue

        items.append(
            _build_item(
                order=order,
                verdict="ALLOW",
                approved_notional=requested,
                reason_codes=(),
            )
        )
        total_notional += requested
        market_notional[order.market_id] = market_notional.get(order.market_id, Decimal("0")) + (
            requested
        )
        if order.category is not None:
            category_notional[order.category] = category_notional.get(
                order.category, Decimal("0")
            ) + (requested)

    items_sorted = sorted(
        items,
        key=lambda entry: (
            _coerce_int(entry.get("rank"), default=0),
            str(entry.get("market_id", "")),
            str(entry.get("token_id", "")),
        ),
    )
    n_allow = sum(1 for item in items_sorted if item.get("verdict") == "ALLOW")
    n_block = sum(1 for item in items_sorted if item.get("verdict") == "BLOCK")
    n_downsize = sum(1 for item in items_sorted if item.get("verdict") == "DOWNSIZE")
    requested_total = sum(
        _coerce_float(item.get("requested_notional_usd"), default=0.0) for item in items_sorted
    )
    approved_total = sum(
        _coerce_float(item.get("approved_notional_usd"), default=0.0) for item in items_sorted
    )

    warning_keys = sorted(warning_map.keys(), key=lambda key: key)
    return RiskPolicyResult(
        items=tuple(items_sorted),
        counts={
            "n_total": len(items_sorted),
            "n_allow": n_allow,
            "n_block": n_block,
            "n_downsize": n_downsize,
        },
        notional_summary={
            "requested_total_usd": _round_money(requested_total),
            "approved_total_usd": _round_money(approved_total),
            "blocked_total_usd": _round_money(max(requested_total - approved_total, 0.0)),
        },
        quality_flags=tuple(sorted(quality_flags)),
        quality_warnings=tuple(warning_map[key] for key in warning_keys),
    )


def _parse_trade_plan_orders(trade_plan_artifact: Mapping[str, Any]) -> tuple[_OrderInput, ...]:
    orders_obj = trade_plan_artifact.get("orders")
    if not _is_sequence(orders_obj):
        raise ValueError("trade_plan_artifact.orders must be a list")
    orders = orders_obj
    parsed: list[_OrderInput] = []
    for index, raw in enumerate(orders):
        if not isinstance(raw, Mapping):
            raise ValueError(f"orders[{index}] must be an object")
        market_id = _require_text(raw.get("market_id"), f"orders[{index}].market_id")
        token_id = _require_text(raw.get("token_id"), f"orders[{index}].token_id")
        rank = _coerce_int(raw.get("rank"), default=index + 1)
        side = _require_text(raw.get("side"), f"orders[{index}].side")
        requested_notional = _round_money_decimal(
            _to_decimal(_require_float(raw.get("notional_usd"), f"orders[{index}].notional_usd"))
        )
        parsed.append(
            _OrderInput(
                rank=rank,
                market_id=market_id,
                token_id=token_id,
                category=_optional_text(raw.get("category")),
                side=side,
                requested_notional=requested_notional,
                quality_flags=_normalize_string_list(raw.get("quality_flags")),
                quality_warnings=_normalize_warning_list(raw.get("quality_warnings")),
            )
        )
    parsed.sort(key=lambda item: (item.rank, item.market_id, item.token_id))
    return tuple(parsed)


def _performance_guards(
    performance_artifact: Mapping[str, Any] | None,
    *,
    config: RiskPolicyConfig,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[dict[str, str], ...]]:
    if performance_artifact is None:
        return (), (), ()

    top1_observed = 0.0
    top3_observed = 0.0
    per_run_obj = performance_artifact.get("per_run_metrics")
    if _is_sequence(per_run_obj):
        for raw in per_run_obj:
            if not isinstance(raw, Mapping):
                continue
            concentration = _mapping(raw.get("concentration"))
            top1_observed = max(
                top1_observed,
                _coerce_float(concentration.get("top1_notional_share"), default=0.0),
            )
            top3_observed = max(
                top3_observed,
                _coerce_float(concentration.get("top3_notional_share"), default=0.0),
            )

    reasons: list[str] = []
    flags: list[str] = []
    warnings: list[dict[str, str]] = []
    if top1_observed > config.performance_top1_cap:
        reasons.append("performance_concentration_top1_breach")
        flags.append("performance_concentration_top1_breach")
        warnings.append(
            {
                "code": "performance_concentration_top1_breach",
                "message": (
                    f"Historical top1 share {top1_observed:.6f} exceeds "
                    f"cap {config.performance_top1_cap:.6f}."
                ),
            }
        )
    if top3_observed > config.performance_top3_cap:
        reasons.append("performance_concentration_top3_breach")
        flags.append("performance_concentration_top3_breach")
        warnings.append(
            {
                "code": "performance_concentration_top3_breach",
                "message": (
                    f"Historical top3 share {top3_observed:.6f} exceeds "
                    f"cap {config.performance_top3_cap:.6f}."
                ),
            }
        )
    return tuple(reasons), tuple(sorted(set(flags))), tuple(warnings)


def _blocking_flag_reasons(order: _OrderInput, *, config: RiskPolicyConfig) -> tuple[str, ...]:
    block_set = set(config.blocking_quality_flags)
    reasons: list[str] = []
    for flag in order.quality_flags:
        if flag in block_set or flag.startswith("critical_"):
            reasons.append(f"blocked_by_quality_flag:{flag}")
    return tuple(sorted(set(reasons)))


def _cooldown_reasons(
    order: _OrderInput, *, hooks_obj: RiskHooks, config: RiskPolicyConfig
) -> tuple[str, ...]:
    reasons: list[str] = []
    cooldown_set = set(config.cooldown_block_flags)
    for flag in hooks_obj.cooldown_tokens.get(order.token_id, ()):
        if flag in cooldown_set or flag.startswith("critical_"):
            reasons.append(f"cooldown_active:token:{flag}")
    for flag in hooks_obj.cooldown_markets.get(order.market_id, ()):
        if flag in cooldown_set or flag.startswith("critical_"):
            reasons.append(f"cooldown_active:market:{flag}")
    return tuple(sorted(set(reasons)))


def _cap_reasons(
    *,
    total_remaining: Decimal,
    market_remaining: Decimal,
    category_remaining: Decimal,
    category: str | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if total_remaining <= Decimal("0"):
        reasons.append("cap_exceeded:max_total_notional_usd")
    if market_remaining <= Decimal("0"):
        reasons.append("cap_exceeded:max_notional_per_market_usd")
    if category is not None and category_remaining <= Decimal("0"):
        reasons.append("cap_exceeded:max_notional_per_category_usd")
    if not reasons:
        min_remaining = min(total_remaining, market_remaining, category_remaining)
        if min_remaining < total_remaining:
            reasons.append("cap_exceeded:max_notional_per_market_usd")
        elif category is not None and min_remaining < market_remaining:
            reasons.append("cap_exceeded:max_notional_per_category_usd")
        elif min_remaining < category_remaining:
            reasons.append("cap_exceeded:max_total_notional_usd")
    return tuple(reasons)


def _concentration_reason(
    *,
    market_id: str,
    add_notional: Decimal,
    total_notional: Decimal,
    market_notional: Mapping[str, Decimal],
    top1_cap: float,
    top3_cap: float,
) -> str | None:
    if add_notional <= Decimal("0"):
        return None
    prospective_total = total_notional + add_notional
    if prospective_total <= Decimal("0"):
        return None

    prospective_market = dict(market_notional)
    prospective_market[market_id] = prospective_market.get(market_id, Decimal("0")) + add_notional
    values = sorted(
        (value for value in prospective_market.values() if value > Decimal("0")), reverse=True
    )
    if not values:
        return None

    top1_share = float(values[0] / prospective_total)
    if top1_share > top1_cap:
        return "concentration_cap:top1"
    top3_share = float(sum(values[:3]) / prospective_total)
    if top3_share > top3_cap:
        return "concentration_cap:top3"
    return None


def _build_item(
    *,
    order: _OrderInput,
    verdict: RiskVerdict,
    approved_notional: Decimal,
    reason_codes: Sequence[str],
) -> dict[str, Any]:
    return {
        "rank": order.rank,
        "market_id": order.market_id,
        "token_id": order.token_id,
        "category": order.category,
        "side": order.side,
        "requested_notional_usd": _round_money(float(order.requested_notional)),
        "approved_notional_usd": _round_money(float(approved_notional)),
        "verdict": verdict,
        "reason_codes": sorted({code for code in reason_codes if code}),
        "quality_flags": list(order.quality_flags),
        "quality_warnings": [dict(item) for item in order.quality_warnings],
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


def _decimal_map(raw: Any) -> dict[str, Decimal]:
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, Decimal] = {}
    for key, value in raw.items():
        text_key = str(key).strip()
        if not text_key:
            continue
        out[text_key] = _round_money_decimal(_to_decimal(value))
    return out


def _cooldown_map(raw: Any) -> dict[str, tuple[str, ...]]:
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, tuple[str, ...]] = {}
    for key, value in raw.items():
        text_key = str(key).strip()
        if not text_key:
            continue
        out[text_key] = _normalize_string_list(value)
    return out


def _mapping(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    return {}


def _add_warning(
    warning_map: dict[tuple[str, str], dict[str, str]],
    *,
    code: str,
    message: str,
) -> None:
    normalized_code = code.strip() or "unknown_warning"
    normalized_message = message.strip()
    warning_map[(normalized_code, normalized_message)] = {
        "code": normalized_code,
        "message": normalized_message,
    }


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _require_text(raw: Any, path: str) -> str:
    value = _optional_text(raw)
    if value is None:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _require_float(raw: Any, path: str) -> float:
    value = _coerce_optional_float(raw)
    if value is None:
        raise ValueError(f"{path} must be a finite number")
    return value


def _coerce_optional_float(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _coerce_float(raw: Any, *, default: float) -> float:
    value = _coerce_optional_float(raw)
    return value if value is not None else default


def _coerce_int(raw: Any, *, default: int) -> int:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _to_decimal(raw: Any) -> Decimal:
    if raw is None:
        return Decimal("0")
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value: {raw!r}") from exc
    if not value.is_finite():
        raise ValueError(f"Decimal value must be finite: {raw!r}")
    return value


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _round_money_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _is_sequence(raw: Any) -> TypeGuard[Sequence[Any]]:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
