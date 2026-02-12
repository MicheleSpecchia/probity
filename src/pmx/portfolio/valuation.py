from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

MarkSource = Literal["execution_price", "execution_p_cal", "execution_price_prob"]
OrderSide = Literal["BUY_YES", "BUY_NO"]


def build_reference_prices(
    execution_artifacts: Sequence[Mapping[str, Any]],
    *,
    mark_source: MarkSource,
    external_prices: Mapping[str, Any] | None = None,
) -> tuple[dict[tuple[str, str], float], tuple[dict[str, str], ...]]:
    price_map: dict[tuple[str, str], float] = {}
    warnings: list[dict[str, str]] = []

    ordered_artifacts = sorted(
        execution_artifacts,
        key=lambda artifact: (
            str(artifact.get("generated_at_utc", "")),
            str(artifact.get("run_id", "")),
        ),
    )
    for artifact_index, artifact in enumerate(ordered_artifacts):
        raw_orders = artifact.get("orders")
        if not _is_sequence(raw_orders):
            continue
        orders_sequence = cast(Sequence[Any], raw_orders)
        orders = sorted(
            orders_sequence,
            key=lambda order: (
                int(_coerce_int(_extract_mapping(order).get("rank"), default=0)),
                str(_extract_mapping(order).get("market_id", "")),
                str(_extract_mapping(order).get("token_id", "")),
            ),
        )
        for order_index, raw_order in enumerate(orders):
            order = _extract_mapping(raw_order)
            token_id = _optional_text(order.get("token_id"))
            side = _optional_text(order.get("side"))
            if token_id is None or side not in {"BUY_YES", "BUY_NO"}:
                continue

            resolved = _reference_from_order(
                order,
                mark_source=mark_source,
                side=cast(OrderSide, side),
            )
            if resolved is None:
                continue
            key = (token_id, side)
            price_map[key] = _round_price(resolved)

            if not math.isfinite(resolved) or resolved <= 0.0:
                warnings.append(
                    {
                        "code": "invalid_reference_price",
                        "token_id": token_id,
                        "side": side,
                        "source": mark_source,
                        "message": (
                            "Reference price is non-finite or non-positive; "
                            f"artifact_index={artifact_index}, order_index={order_index}"
                        ),
                    }
                )

    external_map, external_warnings = _parse_external_prices(external_prices)
    for key, value in external_map.items():
        if key not in price_map:
            price_map[key] = value

    warnings.extend(external_warnings)
    normalized_warnings = _normalize_warning_records(warnings)
    return price_map, normalized_warnings


def missing_reference_keys(
    positions: Sequence[Mapping[str, Any]],
    reference_prices: Mapping[tuple[str, str], float],
) -> tuple[str, ...]:
    missing: list[str] = []
    for index, position in enumerate(positions):
        token_id = _require_text(position.get("token_id"), f"positions[{index}].token_id")
        side = _require_side(position.get("side"), f"positions[{index}].side")
        key = (token_id, side)
        if key not in reference_prices:
            missing.append(f"{token_id}|{side}")
    return tuple(sorted(set(missing)))


def mark_to_model(
    positions: Sequence[Mapping[str, Any]],
    *,
    reference_prices: Mapping[tuple[str, str], float],
    mark_source: MarkSource,
) -> dict[str, Any]:
    ordered_positions = sorted(
        positions,
        key=lambda position: (str(position.get("token_id", "")), str(position.get("side", ""))),
    )
    per_position: list[dict[str, Any]] = []
    total_cost_basis = 0.0
    total_market_value = 0.0
    total_unrealized_pnl = 0.0
    total_notional_exposure = 0.0

    for index, position in enumerate(ordered_positions):
        token_id = _require_text(position.get("token_id"), f"positions[{index}].token_id")
        side = _require_side(position.get("side"), f"positions[{index}].side")
        quantity = _require_positive_float(position.get("quantity"), f"positions[{index}].quantity")
        avg_cost = _require_positive_float(position.get("avg_cost"), f"positions[{index}].avg_cost")
        fallback_cost_basis = quantity * avg_cost
        raw_cost_basis = position.get("cost_basis_usd")
        cost_basis = (
            _require_non_negative_float(raw_cost_basis, f"positions[{index}].cost_basis_usd")
            if raw_cost_basis is not None
            else fallback_cost_basis
        )

        key = (token_id, side)
        if key not in reference_prices:
            raise ValueError(
                "Missing reference price for position key "
                f"{token_id}|{side}; provide compatible mark source data"
            )
        mark_price = _require_positive_float(reference_prices[key], f"reference_prices[{key}]")

        market_value = mark_price * quantity
        unrealized_pnl = market_value - cost_basis
        notional_exposure = abs(market_value)

        total_cost_basis += cost_basis
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        total_notional_exposure += notional_exposure

        per_position.append(
            {
                "position_id": _optional_text(position.get("position_id")),
                "token_id": token_id,
                "side": side,
                "quantity": _round_quantity(quantity),
                "avg_cost": _round_price(avg_cost),
                "mark_price": _round_price(mark_price),
                "cost_basis_usd": _round_money(cost_basis),
                "market_value_usd": _round_money(market_value),
                "unrealized_pnl_usd": _round_money(unrealized_pnl),
                "notional_exposure_usd": _round_money(notional_exposure),
            }
        )

    summary = {
        "n_positions": len(per_position),
        "total_cost_basis_usd": _round_money(total_cost_basis),
        "total_market_value_usd": _round_money(total_market_value),
        "total_unrealized_pnl_usd": _round_money(total_unrealized_pnl),
        "total_notional_exposure_usd": _round_money(total_notional_exposure),
    }
    return {
        "mark_source": mark_source,
        "summary": summary,
        "per_position": per_position,
    }


def _reference_from_order(
    order: Mapping[str, Any],
    *,
    mark_source: MarkSource,
    side: OrderSide,
) -> float | None:
    if mark_source == "execution_price":
        value = _coerce_float(order.get("price"))
        if value is None or value <= 0.0:
            return None
        return value

    source_field = "p_cal" if mark_source == "execution_p_cal" else "price_prob"
    yes_value = _coerce_float(order.get(source_field))
    if yes_value is None or not math.isfinite(yes_value):
        return None
    if yes_value < 0.0 or yes_value > 1.0:
        return None
    if side == "BUY_NO":
        return 1.0 - yes_value
    return yes_value


def _parse_external_prices(
    external_prices: Mapping[str, Any] | None,
) -> tuple[dict[tuple[str, str], float], list[dict[str, str]]]:
    if external_prices is None:
        return {}, []
    resolved: dict[tuple[str, str], float] = {}
    warnings: list[dict[str, str]] = []
    for raw_key, raw_value in external_prices.items():
        key = str(raw_key).strip()
        if not key:
            continue
        parsed_value = _coerce_float(raw_value)
        if parsed_value is None or not math.isfinite(parsed_value):
            warnings.append(
                {
                    "code": "invalid_external_reference_price",
                    "message": f"Invalid external reference price for key {key!r}.",
                }
            )
            continue

        if "|" in key:
            token_id, raw_side = key.split("|", maxsplit=1)
            token = token_id.strip()
            side = raw_side.strip()
            if token and side in {"BUY_YES", "BUY_NO"} and parsed_value > 0.0:
                resolved[(token, side)] = _round_price(parsed_value)
                continue
            warnings.append(
                {
                    "code": "invalid_external_reference_key",
                    "message": f"Invalid external side-specific key {key!r}.",
                }
            )
            continue

        token_id = key
        if parsed_value < 0.0 or parsed_value > 1.0:
            warnings.append(
                {
                    "code": "invalid_external_reference_probability",
                    "message": f"Token-level reference for {key!r} must be in [0,1].",
                }
            )
            continue
        resolved[(token_id, "BUY_YES")] = _round_price(parsed_value)
        resolved[(token_id, "BUY_NO")] = _round_price(1.0 - parsed_value)
    return resolved, warnings


def _normalize_warning_records(records: Sequence[Mapping[str, str]]) -> tuple[dict[str, str], ...]:
    deduped: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for record in records:
        code = str(record.get("code", "")).strip() or "unknown_warning"
        message = str(record.get("message", "")).strip()
        token_id = str(record.get("token_id", "")).strip()
        side = str(record.get("side", "")).strip()
        key = (code, message, token_id, side)
        payload: dict[str, str] = {"code": code}
        if message:
            payload["message"] = message
        if token_id:
            payload["token_id"] = token_id
        if side:
            payload["side"] = side
        deduped[key] = payload
    sorted_keys = sorted(deduped.keys(), key=lambda item: item)
    return tuple(deduped[key] for key in sorted_keys)


def _extract_mapping(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    return {}


def _coerce_int(raw: Any, *, default: int) -> int:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _coerce_float(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


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


def _require_side(raw: Any, path: str) -> OrderSide:
    value = _require_text(raw, path)
    if value not in {"BUY_YES", "BUY_NO"}:
        raise ValueError(f"{path} must be BUY_YES or BUY_NO")
    return cast(OrderSide, value)


def _require_positive_float(raw: Any, path: str) -> float:
    value = _coerce_float(raw)
    if value is None or value <= 0.0:
        raise ValueError(f"{path} must be a positive number")
    return value


def _require_non_negative_float(raw: Any, path: str) -> float:
    value = _coerce_float(raw)
    if value is None or value < 0.0:
        raise ValueError(f"{path} must be a non-negative number")
    return value


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _round_quantity(value: float) -> float:
    return round(float(value), 2)


def _round_price(value: float) -> float:
    return round(float(value), 8)


def _is_sequence(raw: Any) -> bool:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
