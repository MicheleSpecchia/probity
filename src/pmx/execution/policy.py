from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

ExecutionMode = Literal["dry_run", "simulate_submit"]
SimulatedOrderStatus = Literal["SIMULATED_SUBMITTED", "SIMULATED_REJECTED"]


@dataclass(frozen=True, slots=True)
class ExecutionPolicyConfig:
    mode: ExecutionMode = "simulate_submit"
    max_orders: int = 200
    simulate_reject_modulo: int = 0
    simulate_reject_remainder: int = 0

    def __post_init__(self) -> None:
        if self.mode not in {"dry_run", "simulate_submit"}:
            raise ValueError(f"Unsupported mode: {self.mode!r}")
        if self.max_orders <= 0:
            raise ValueError("max_orders must be > 0")
        if self.simulate_reject_modulo < 0:
            raise ValueError("simulate_reject_modulo must be >= 0")
        if self.simulate_reject_modulo == 0 and self.simulate_reject_remainder != 0:
            raise ValueError("simulate_reject_remainder must be 0 when simulate_reject_modulo=0")
        if self.simulate_reject_modulo > 0 and not (
            0 <= self.simulate_reject_remainder < self.simulate_reject_modulo
        ):
            raise ValueError(
                "simulate_reject_remainder must satisfy 0 <= remainder < simulate_reject_modulo"
            )

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "max_orders": self.max_orders,
            "mode": self.mode,
            "simulate_reject_modulo": self.simulate_reject_modulo,
            "simulate_reject_remainder": self.simulate_reject_remainder,
        }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    orders: tuple[dict[str, Any], ...]
    skipped: tuple[dict[str, Any], ...]
    counts: dict[str, int]
    idempotency_key: str


def apply_execution_policy(
    *,
    trade_plan_artifact: Mapping[str, Any],
    config: ExecutionPolicyConfig,
) -> ExecutionResult:
    raw_orders_obj = trade_plan_artifact.get("orders")
    if not _is_sequence(raw_orders_obj):
        raise ValueError("Trade-plan artifact must include an 'orders' list")
    raw_orders = cast(Sequence[Any], raw_orders_obj)

    raw_skipped_obj = trade_plan_artifact.get("skipped")
    if not _is_sequence(raw_skipped_obj):
        raise ValueError("Trade-plan artifact must include a 'skipped' list")
    raw_skipped = cast(Sequence[Any], raw_skipped_obj)

    input_run_id = _require_text(trade_plan_artifact.get("run_id"), "run_id")
    trade_plan_payload_hash = _require_hash(
        trade_plan_artifact.get("trade_plan_payload_hash"),
        "trade_plan_payload_hash",
    )
    trade_plan_policy_hash = _require_hash(
        trade_plan_artifact.get("policy_hash"),
        "policy_hash",
    )

    parsed_orders = [_parse_order(raw, index=index) for index, raw in enumerate(raw_orders)]
    ordered_orders = sorted(
        parsed_orders,
        key=lambda order: (
            int(order["rank"]),
            str(order["market_id"]),
            str(order["token_id"]),
        ),
    )

    executed: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for order in ordered_orders:
        reason = _precheck_order_reason(order)
        if reason is not None:
            blocked.append(_to_skipped(order, reason))
            continue

        if len(executed) >= config.max_orders:
            blocked.append(_to_skipped(order, "cap_exceeded:max_orders"))
            continue

        status, reject_reason = _simulated_status(order, config)
        executed_item = {
            "rank": int(order["rank"]),
            "client_order_id": _client_order_id(
                input_run_id=input_run_id,
                market_id=str(order["market_id"]),
                token_id=str(order["token_id"]),
                side=str(order["side"]),
                notional_usd=float(order["notional_usd"]),
                price=float(order["price"]),
                quantity=float(order["quantity"]),
            ),
            "market_id": str(order["market_id"]),
            "token_id": str(order["token_id"]),
            "side": str(order["side"]),
            "price": _round_price(float(order["price"])),
            "quantity": _round_money(float(order["quantity"])),
            "notional_usd": _round_money(float(order["notional_usd"])),
            "status": status,
            "reject_reason_code": reject_reason,
        }
        executed.append(executed_item)

    copied_skipped = [_copy_skipped(raw, index=index) for index, raw in enumerate(raw_skipped)]
    merged_skipped = copied_skipped + blocked
    sorted_skipped = sorted(
        merged_skipped,
        key=lambda item: (
            str(item.get("reason_code", "")),
            str(item.get("market_id", "")),
            str(item.get("token_id", "")),
        ),
    )

    idempotency_key = f"{trade_plan_payload_hash}|{trade_plan_policy_hash}"
    counts = {
        "n_total": len(executed) + len(sorted_skipped),
        "n_orders": len(executed),
        "n_rejected": sum(1 for order in executed if order["status"] == "SIMULATED_REJECTED"),
        "n_skipped": len(sorted_skipped),
    }
    return ExecutionResult(
        orders=tuple(executed),
        skipped=tuple(sorted_skipped),
        counts=counts,
        idempotency_key=idempotency_key,
    )


def _parse_order(raw: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"orders[{index}] must be an object")
    prefix = f"orders[{index}]"
    return {
        "rank": _require_int(raw.get("rank"), f"{prefix}.rank"),
        "market_id": _require_text(raw.get("market_id"), f"{prefix}.market_id"),
        "token_id": _require_text(raw.get("token_id"), f"{prefix}.token_id"),
        "action": _require_text(raw.get("action"), f"{prefix}.action"),
        "side": _require_text(raw.get("side"), f"{prefix}.side"),
        "price": _require_float(raw.get("price"), f"{prefix}.price"),
        "quantity": _require_float(raw.get("quantity"), f"{prefix}.quantity"),
        "notional_usd": _require_float(raw.get("notional_usd"), f"{prefix}.notional_usd"),
        "quality_flags": _normalize_string_list(raw.get("quality_flags")),
        "quality_warnings": _normalize_warning_list(raw.get("quality_warnings")),
        "no_trade_reasons": _normalize_string_list(raw.get("no_trade_reasons")),
    }


def _precheck_order_reason(order: Mapping[str, Any]) -> str | None:
    action = str(order.get("action", ""))
    if action != "TRADE":
        return "invalid_order_action"
    side = str(order.get("side", ""))
    if side not in {"BUY_YES", "BUY_NO"}:
        return "invalid_order_side"
    if float(order.get("quantity", 0.0)) <= 0.0:
        return "invalid_quantity"
    if float(order.get("notional_usd", 0.0)) <= 0.0:
        return "invalid_notional"
    if float(order.get("price", 0.0)) <= 0.0:
        return "invalid_price"
    return None


def _simulated_status(
    order: Mapping[str, Any],
    config: ExecutionPolicyConfig,
) -> tuple[SimulatedOrderStatus, str | None]:
    if config.mode == "dry_run":
        return ("SIMULATED_SUBMITTED", None)
    if config.simulate_reject_modulo <= 0:
        return ("SIMULATED_SUBMITTED", None)

    token_id = str(order["token_id"])
    token_hash = hashlib.sha256(token_id.encode("utf-8")).hexdigest()
    modulo_value = int(token_hash[:16], 16) % config.simulate_reject_modulo
    if modulo_value == config.simulate_reject_remainder:
        return ("SIMULATED_REJECTED", "simulated_reject_hash_mod")
    return ("SIMULATED_SUBMITTED", None)


def _client_order_id(
    *,
    input_run_id: str,
    market_id: str,
    token_id: str,
    side: str,
    notional_usd: float,
    price: float,
    quantity: float,
) -> str:
    payload = (
        f"{input_run_id}|{market_id}|{token_id}|{side}|"
        f"{_round_money(notional_usd):.2f}|{_round_price(price):.8f}|{_round_money(quantity):.2f}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _copy_skipped(raw: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"skipped[{index}] must be an object")
    prefix = f"skipped[{index}]"
    return {
        "market_id": _require_text(raw.get("market_id"), f"{prefix}.market_id"),
        "token_id": _require_text(raw.get("token_id"), f"{prefix}.token_id"),
        "reason_code": _require_text(raw.get("reason_code"), f"{prefix}.reason_code"),
        "quality_flags": _normalize_string_list(raw.get("quality_flags")),
        "quality_warnings": _normalize_warning_list(raw.get("quality_warnings")),
        "no_trade_reasons": _normalize_string_list(raw.get("no_trade_reasons")),
    }


def _to_skipped(order: Mapping[str, Any], reason_code: str) -> dict[str, Any]:
    return {
        "market_id": str(order["market_id"]),
        "token_id": str(order["token_id"]),
        "reason_code": reason_code,
        "quality_flags": list(cast(tuple[str, ...], order["quality_flags"])),
        "quality_warnings": [
            dict(entry) for entry in cast(tuple[dict[str, str], ...], order["quality_warnings"])
        ],
        "no_trade_reasons": list(cast(tuple[str, ...], order["no_trade_reasons"])),
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


def _require_text(raw: Any, path: str) -> str:
    value = str(raw).strip() if raw is not None else ""
    if not value:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _require_int(raw: Any, path: str) -> int:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{path} must be an integer")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{path} must be > 0")
    return value


def _require_float(raw: Any, path: str) -> float:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{path} must be a number")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a number") from exc
    return value


def _require_hash(raw: Any, path: str) -> str:
    value = _require_text(raw, path)
    if len(value) != 64 or not all(char in "0123456789abcdef" for char in value):
        raise ValueError(f"{path} must be a lowercase sha256 hex string")
    return value


def _round_price(value: float) -> float:
    return round(float(value), 8)


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _is_sequence(raw: Any) -> bool:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
