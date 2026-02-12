from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

RobustMode = Literal["require_positive_low90", "require_negative_high90", "none"]
DecisionAction = Literal["BUY_YES", "BUY_NO", "NO_TRADE"]

BLOCKING_QUALITY_FLAGS = frozenset(
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
class DecisionPolicyConfig:
    min_edge_bps: float = 50.0
    robust_mode: RobustMode = "require_positive_low90"
    max_items: int = 200

    def __post_init__(self) -> None:
        if self.min_edge_bps < 0.0:
            raise ValueError("min_edge_bps must be >= 0")
        if self.max_items <= 0:
            raise ValueError("max_items must be > 0")
        if self.robust_mode not in {"require_positive_low90", "require_negative_high90", "none"}:
            raise ValueError(f"Unsupported robust_mode: {self.robust_mode!r}")

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "max_items": self.max_items,
            "min_edge_bps": self.min_edge_bps,
            "robust_mode": self.robust_mode,
        }


def decide_from_forecast_artifact(
    forecast_artifact: Mapping[str, Any],
    config: DecisionPolicyConfig,
) -> tuple[dict[str, Any], ...]:
    raw_items_obj = forecast_artifact.get("forecasts")
    if not _is_sequence(raw_items_obj):
        raise ValueError("forecast artifact must include a 'forecasts' list")
    raw_items = cast(Sequence[Any], raw_items_obj)

    global_flags = _normalize_string_list(forecast_artifact.get("quality_flags"))
    global_warnings = _normalize_warning_list(forecast_artifact.get("quality_warnings"))

    parsed_items: list[dict[str, Any]] = []
    for index, raw_item in enumerate(raw_items):
        if not isinstance(raw_item, Mapping):
            raise ValueError(f"forecasts[{index}] must be an object")
        parsed_items.append(
            _build_decision_item(
                raw_item,
                index=index,
                global_flags=global_flags,
                global_warnings=global_warnings,
                config=config,
            )
        )

    trades = sorted(
        (item for item in parsed_items if item["action"] != "NO_TRADE"),
        key=lambda item: (
            -abs(float(item["edge_bps"])),
            str(item["market_id"]),
            str(item["token_id"]),
        ),
    )
    no_trades = sorted(
        (item for item in parsed_items if item["action"] == "NO_TRADE"),
        key=lambda item: (str(item["market_id"]), str(item["token_id"])),
    )
    ordered = trades + no_trades
    return tuple(ordered[: config.max_items])


def _build_decision_item(
    item: Mapping[str, Any],
    *,
    index: int,
    global_flags: tuple[str, ...],
    global_warnings: tuple[dict[str, str], ...],
    config: DecisionPolicyConfig,
) -> dict[str, Any]:
    prefix = f"forecasts[{index}]"
    market_id = _require_text(item.get("market_id"), f"{prefix}.market_id")
    token_id = _require_text(item.get("token_id"), f"{prefix}.token_id")
    price_prob = _require_float(
        item.get("price_prob", item.get("p_a")),
        f"{prefix}.price_prob|p_a",
    )
    p_raw = _require_float(item.get("p_raw"), f"{prefix}.p_raw")
    p_cal = _require_float(item.get("p_cal"), f"{prefix}.p_cal")
    lo50, hi50 = _require_interval(item.get("interval_50"), f"{prefix}.interval_50")
    lo90, hi90 = _require_interval(item.get("interval_90"), f"{prefix}.interval_90")

    edge = p_cal - price_prob
    edge_bps = edge * 10_000.0
    edge_low_90 = lo90 - price_prob
    edge_high_90 = hi90 - price_prob

    item_flags = _normalize_string_list(item.get("quality_flags"))
    item_no_trade_flags = _normalize_string_list(item.get("no_trade_flags"))
    quality_flags = tuple(sorted({*global_flags, *item_flags, *item_no_trade_flags}))

    item_warnings = _normalize_warning_list(item.get("quality_warnings"))
    quality_warnings = item_warnings if item_warnings else global_warnings

    reasons: list[str] = []
    for flag in quality_flags:
        if flag in BLOCKING_QUALITY_FLAGS:
            reasons.append(f"flag:{flag}")

    action: DecisionAction = "NO_TRADE"
    if not reasons:
        if edge_bps >= config.min_edge_bps:
            if _robust_buy_yes_passes(config.robust_mode, edge_low_90):
                action = "BUY_YES"
            else:
                reasons.append("robust_check_failed")
        elif edge_bps <= (-config.min_edge_bps):
            if _robust_buy_no_passes(config.robust_mode, edge_high_90):
                action = "BUY_NO"
            else:
                reasons.append("robust_check_failed")
        else:
            reasons.append("edge_below_threshold")

    no_trade_reasons = [] if action != "NO_TRADE" else sorted(set(reasons))
    return {
        "market_id": market_id,
        "token_id": token_id,
        "price_prob": _round_value(price_prob),
        "p_raw": _round_value(p_raw),
        "p_cal": _round_value(p_cal),
        "interval_50": {"low": _round_value(lo50), "high": _round_value(hi50)},
        "interval_90": {"low": _round_value(lo90), "high": _round_value(hi90)},
        "edge": _round_value(edge),
        "edge_bps": _round_bps(edge_bps),
        "edge_low_90": _round_value(edge_low_90),
        "edge_high_90": _round_value(edge_high_90),
        "action": action,
        "no_trade_reasons": no_trade_reasons,
        "quality_flags": list(quality_flags),
        "quality_warnings": [dict(warning) for warning in quality_warnings],
    }


def _robust_buy_yes_passes(mode: RobustMode, edge_low_90: float) -> bool:
    if mode == "require_positive_low90":
        return edge_low_90 > 0.0
    return True


def _robust_buy_no_passes(mode: RobustMode, edge_high_90: float) -> bool:
    if mode == "require_negative_high90":
        return edge_high_90 < 0.0
    return True


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


def _require_text(raw: Any, path: str) -> str:
    value = str(raw).strip() if raw is not None else ""
    if not value:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _require_float(raw: Any, path: str) -> float:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{path} must be a number")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a number") from exc
    return value


def _require_interval(raw: Any, path: str) -> tuple[float, float]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must be an object with low/high")
    low = _require_float(raw.get("low"), f"{path}.low")
    high = _require_float(raw.get("high"), f"{path}.high")
    if low > high:
        raise ValueError(f"{path} invalid interval: low > high")
    return low, high


def _round_value(value: float) -> float:
    return round(float(value), 8)


def _round_bps(value: float) -> float:
    return round(float(value), 4)


def _is_sequence(raw: Any) -> bool:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
