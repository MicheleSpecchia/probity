from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

OrderSide = Literal["BUY_YES", "BUY_NO"]


@dataclass(slots=True)
class _PositionAccumulator:
    token_id: str
    side: OrderSide
    quantity: float = 0.0
    total_cost_usd: float = 0.0
    total_notional_usd: float = 0.0
    total_fees_usd: float = 0.0
    market_ids: set[str] = field(default_factory=set)


def apply_ledger_to_positions(entries: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    ordered_entries = sorted(
        entries,
        key=lambda entry: (
            str(entry.get("executed_at_utc", "")),
            str(entry.get("client_order_id", "")),
        ),
    )

    accumulators: dict[tuple[str, str], _PositionAccumulator] = {}
    for index, entry in enumerate(ordered_entries):
        token_id = _require_text(entry.get("token_id"), f"entries[{index}].token_id")
        side = _parse_side(entry.get("side"), f"entries[{index}].side")
        quantity = _require_positive_float(entry.get("quantity"), f"entries[{index}].quantity")
        total_cost_usd = _require_non_negative_float(
            entry.get("total_cost_usd"),
            f"entries[{index}].total_cost_usd",
        )
        notional_usd = _require_non_negative_float(
            entry.get("notional_usd"),
            f"entries[{index}].notional_usd",
        )
        fee_usd = _require_non_negative_float(entry.get("fee_usd"), f"entries[{index}].fee_usd")
        market_id = _require_text(entry.get("market_id"), f"entries[{index}].market_id")

        key = (token_id, side)
        if key not in accumulators:
            accumulators[key] = _PositionAccumulator(token_id=token_id, side=side)
        accumulator = accumulators[key]
        accumulator.quantity += quantity
        accumulator.total_cost_usd += total_cost_usd
        accumulator.total_notional_usd += notional_usd
        accumulator.total_fees_usd += fee_usd
        accumulator.market_ids.add(market_id)

    positions: list[dict[str, Any]] = []
    for token_id, raw_side in sorted(accumulators.keys(), key=lambda item: (item[0], item[1])):
        side = cast(OrderSide, raw_side)
        accumulator = accumulators[(token_id, raw_side)]
        if accumulator.quantity <= 0.0:
            continue
        avg_cost = accumulator.total_cost_usd / accumulator.quantity
        position_id_seed = f"{token_id}|{side}"
        position_id = hashlib.sha256(position_id_seed.encode("utf-8")).hexdigest()
        positions.append(
            {
                "position_id": position_id,
                "token_id": token_id,
                "side": side,
                "quantity": _round_quantity(accumulator.quantity),
                "avg_cost": _round_price(avg_cost),
                "cost_basis_usd": _round_money(accumulator.total_cost_usd),
                "notional_usd": _round_money(accumulator.total_notional_usd),
                "fees_usd": _round_money(accumulator.total_fees_usd),
                "market_ids": sorted(accumulator.market_ids),
            }
        )

    return tuple(positions)


def _require_text(raw: Any, path: str) -> str:
    value = str(raw).strip() if raw is not None else ""
    if not value:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _parse_side(raw: Any, path: str) -> OrderSide:
    value = _require_text(raw, path)
    if value not in {"BUY_YES", "BUY_NO"}:
        raise ValueError(f"{path} must be BUY_YES or BUY_NO")
    return cast(OrderSide, value)


def _require_positive_float(raw: Any, path: str) -> float:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{path} must be a positive number")
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise ValueError(f"{path} must be a positive number")
    return parsed


def _require_non_negative_float(raw: Any, path: str) -> float:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{path} must be a non-negative number")
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"{path} must be a non-negative number")
    return parsed


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _round_quantity(value: float) -> float:
    return round(float(value), 2)


def _round_price(value: float) -> float:
    return round(float(value), 8)
