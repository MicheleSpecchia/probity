from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

OrderSide = Literal["BUY_YES", "BUY_NO"]
OrderStatus = Literal["SIMULATED_SUBMITTED", "SIMULATED_REJECTED"]


@dataclass(frozen=True, slots=True)
class LedgerConfig:
    fee_bps: float = 0.0
    fee_usd: float = 0.0

    def __post_init__(self) -> None:
        if self.fee_bps < 0.0:
            raise ValueError("fee_bps must be >= 0")
        if self.fee_usd < 0.0:
            raise ValueError("fee_usd must be >= 0")

    def as_hash_dict(self) -> dict[str, float]:
        return {
            "fee_bps": self.fee_bps,
            "fee_usd": self.fee_usd,
        }


@dataclass(frozen=True, slots=True)
class LedgerBuildResult:
    entries: tuple[dict[str, Any], ...]
    quality_flags: tuple[str, ...]
    quality_warnings: tuple[dict[str, str], ...]
    counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class _OrderCandidate:
    executed_at_utc: str
    execution_run_id: str
    execution_artifact_hash: str
    client_order_id: str
    market_id: str
    token_id: str
    side: OrderSide
    price: float
    quantity: float
    notional_usd: float
    status: OrderStatus | str
    reject_reason_code: str | None
    artifact_index: int
    order_index: int


def build_ledger(
    execution_artifacts: Sequence[Mapping[str, Any]],
    config: LedgerConfig,
) -> LedgerBuildResult:
    candidates: list[_OrderCandidate] = []
    n_input_orders = 0
    for artifact_index, execution_artifact in enumerate(execution_artifacts):
        execution_run_id = _require_text(execution_artifact.get("run_id"), "run_id")
        execution_artifact_hash = _resolve_execution_artifact_hash(execution_artifact)
        executed_at_utc = _require_utc_timestamp(
            execution_artifact.get("generated_at_utc"),
            "generated_at_utc",
        )
        raw_orders_obj = execution_artifact.get("orders")
        if not _is_sequence(raw_orders_obj):
            raise ValueError("Execution artifact must include an 'orders' list")
        raw_orders = cast(Sequence[Any], raw_orders_obj)
        n_input_orders += len(raw_orders)
        for order_index, raw_order in enumerate(raw_orders):
            candidates.append(
                _parse_order_candidate(
                    raw_order,
                    execution_run_id=execution_run_id,
                    execution_artifact_hash=execution_artifact_hash,
                    executed_at_utc=executed_at_utc,
                    artifact_index=artifact_index,
                    order_index=order_index,
                )
            )

    ordered_candidates = sorted(
        candidates,
        key=lambda candidate: (
            candidate.executed_at_utc,
            candidate.client_order_id,
            candidate.artifact_index,
            candidate.order_index,
        ),
    )

    entries: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    flags: set[str] = set()
    seen_client_order_ids: set[str] = set()
    n_duplicates = 0
    n_rejected = 0

    for candidate in ordered_candidates:
        if candidate.client_order_id in seen_client_order_ids:
            n_duplicates += 1
            flags.add("duplicate_client_order_id")
            warnings.append(
                {
                    "code": "duplicate_client_order_id",
                    "client_order_id": candidate.client_order_id,
                    "market_id": candidate.market_id,
                    "token_id": candidate.token_id,
                    "message": "Duplicate client_order_id ignored for ledger accounting.",
                }
            )
            continue
        seen_client_order_ids.add(candidate.client_order_id)

        if candidate.status == "SIMULATED_REJECTED":
            n_rejected += 1
            warnings.append(
                {
                    "code": "rejected_order",
                    "client_order_id": candidate.client_order_id,
                    "market_id": candidate.market_id,
                    "token_id": candidate.token_id,
                    "reason_code": candidate.reject_reason_code or "unknown_reject_reason",
                    "message": "Rejected order produced no ledger entry.",
                }
            )
            continue
        if candidate.status != "SIMULATED_SUBMITTED":
            flags.add("unsupported_order_status")
            warnings.append(
                {
                    "code": "unsupported_order_status",
                    "client_order_id": candidate.client_order_id,
                    "market_id": candidate.market_id,
                    "token_id": candidate.token_id,
                    "reason_code": str(candidate.status),
                    "message": "Order status is unsupported for ledger accounting.",
                }
            )
            continue

        fee_total = _round_money(
            (candidate.notional_usd * config.fee_bps / 10_000.0) + config.fee_usd
        )
        total_cost = _round_money(candidate.notional_usd + fee_total)
        entry_id_seed = f"{candidate.client_order_id}|{candidate.executed_at_utc}"
        entry_id = hashlib.sha256(entry_id_seed.encode("utf-8")).hexdigest()
        entries.append(
            {
                "entry_id": entry_id,
                "executed_at_utc": candidate.executed_at_utc,
                "execution_run_id": candidate.execution_run_id,
                "execution_artifact_hash": candidate.execution_artifact_hash,
                "client_order_id": candidate.client_order_id,
                "market_id": candidate.market_id,
                "token_id": candidate.token_id,
                "side": candidate.side,
                "status": "SIMULATED_FILLED",
                "quantity": _round_quantity(candidate.quantity),
                "price": _round_price(candidate.price),
                "notional_usd": _round_money(candidate.notional_usd),
                "fee_usd": fee_total,
                "total_cost_usd": total_cost,
            }
        )

    entries_sorted = tuple(
        sorted(
            entries,
            key=lambda entry: (
                str(entry["executed_at_utc"]),
                str(entry["client_order_id"]),
            ),
        )
    )
    warnings_sorted = _normalize_warning_records(warnings)
    counts = {
        "n_input_orders": n_input_orders,
        "n_ledger_entries": len(entries_sorted),
        "n_rejected": n_rejected,
        "n_duplicates": n_duplicates,
        "n_warnings": len(warnings_sorted),
    }
    return LedgerBuildResult(
        entries=entries_sorted,
        quality_flags=tuple(sorted(flags)),
        quality_warnings=warnings_sorted,
        counts=counts,
    )


def _parse_order_candidate(
    raw_order: Any,
    *,
    execution_run_id: str,
    execution_artifact_hash: str,
    executed_at_utc: str,
    artifact_index: int,
    order_index: int,
) -> _OrderCandidate:
    if not isinstance(raw_order, Mapping):
        raise ValueError("orders entries must be objects")
    prefix = f"orders[{order_index}]"
    side = _parse_side(raw_order.get("side"), f"{prefix}.side")
    status = _parse_status(raw_order.get("status"), f"{prefix}.status")
    return _OrderCandidate(
        executed_at_utc=executed_at_utc,
        execution_run_id=execution_run_id,
        execution_artifact_hash=execution_artifact_hash,
        client_order_id=_require_hash(
            raw_order.get("client_order_id"), f"{prefix}.client_order_id"
        ),
        market_id=_require_text(raw_order.get("market_id"), f"{prefix}.market_id"),
        token_id=_require_text(raw_order.get("token_id"), f"{prefix}.token_id"),
        side=side,
        price=_require_positive_float(raw_order.get("price"), f"{prefix}.price"),
        quantity=_require_positive_float(raw_order.get("quantity"), f"{prefix}.quantity"),
        notional_usd=_require_positive_float(
            raw_order.get("notional_usd"), f"{prefix}.notional_usd"
        ),
        status=status,
        reject_reason_code=_optional_text(raw_order.get("reject_reason_code")),
        artifact_index=artifact_index,
        order_index=order_index,
    )


def _parse_side(raw: Any, path: str) -> OrderSide:
    value = _require_text(raw, path)
    if value not in {"BUY_YES", "BUY_NO"}:
        raise ValueError(f"{path} must be BUY_YES or BUY_NO")
    return cast(OrderSide, value)


def _parse_status(raw: Any, path: str) -> OrderStatus | str:
    value = _require_text(raw, path)
    if value in {"SIMULATED_SUBMITTED", "SIMULATED_REJECTED"}:
        return cast(OrderStatus, value)
    return value


def _resolve_execution_artifact_hash(execution_artifact: Mapping[str, Any]) -> str:
    direct = _optional_text(execution_artifact.get("execution_payload_hash"))
    if direct is not None and _is_hash(direct):
        return direct
    hash_seed = str(execution_artifact)
    return hashlib.sha256(hash_seed.encode("utf-8")).hexdigest()


def _normalize_warning_records(records: Sequence[Mapping[str, str]]) -> tuple[dict[str, str], ...]:
    deduped: dict[tuple[str, str, str, str, str, str], dict[str, str]] = {}
    for record in records:
        code = str(record.get("code", "")).strip() or "unknown_warning"
        message = str(record.get("message", "")).strip()
        client_order_id = str(record.get("client_order_id", "")).strip()
        token_id = str(record.get("token_id", "")).strip()
        market_id = str(record.get("market_id", "")).strip()
        reason_code = str(record.get("reason_code", "")).strip()
        key = (code, client_order_id, token_id, market_id, reason_code, message)
        payload: dict[str, str] = {"code": code}
        if message:
            payload["message"] = message
        if client_order_id:
            payload["client_order_id"] = client_order_id
        if token_id:
            payload["token_id"] = token_id
        if market_id:
            payload["market_id"] = market_id
        if reason_code:
            payload["reason_code"] = reason_code
        deduped[key] = payload

    keys = sorted(deduped.keys(), key=lambda item: item)
    return tuple(deduped[key] for key in keys)


def _require_utc_timestamp(raw: Any, path: str) -> str:
    value = _require_text(raw, path)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{path} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _require_hash(raw: Any, path: str) -> str:
    value = _require_text(raw, path)
    if not _is_hash(value):
        raise ValueError(f"{path} must be a lowercase sha256 hash")
    return value


def _is_hash(raw: str) -> bool:
    return len(raw) == 64 and all(char in "0123456789abcdef" for char in raw)


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


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _round_quantity(value: float) -> float:
    return round(float(value), 2)


def _round_price(value: float) -> float:
    return round(float(value), 8)


def _is_sequence(raw: Any) -> bool:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
