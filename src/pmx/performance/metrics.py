from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from statistics import median
from typing import Any, TypeGuard

from pmx.portfolio.canonical import canonical_hash

DEFAULT_MIN_INPUTS_WARNING = 2
DEFAULT_TOP1_SHARE_THRESHOLD = 0.50
DEFAULT_TOP3_SHARE_THRESHOLD = 0.80
DEFAULT_NEGATIVE_PNL_USD_THRESHOLD = -250.0
DEFAULT_NEGATIVE_PNL_BPS_THRESHOLD = -500.0


def compute_performance_metrics(
    portfolio_artifacts: Sequence[Mapping[str, Any]],
    *,
    min_inputs_warning: int = DEFAULT_MIN_INPUTS_WARNING,
    top1_share_threshold: float = DEFAULT_TOP1_SHARE_THRESHOLD,
    top3_share_threshold: float = DEFAULT_TOP3_SHARE_THRESHOLD,
    negative_pnl_usd_threshold: float = DEFAULT_NEGATIVE_PNL_USD_THRESHOLD,
    negative_pnl_bps_threshold: float = DEFAULT_NEGATIVE_PNL_BPS_THRESHOLD,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any], tuple[str, ...], tuple[dict[str, str], ...]]:
    ordered_artifacts = sorted(
        portfolio_artifacts,
        key=lambda artifact: (
            _optional_text(artifact.get("run_id")) or "",
            _resolve_payload_hash(artifact),
        ),
    )

    per_run_metrics: list[dict[str, Any]] = []
    quality_flags: set[str] = set()
    warnings: dict[tuple[str, str], dict[str, str]] = {}

    if len(ordered_artifacts) < min_inputs_warning:
        quality_flags.add("insufficient_inputs")
        _add_warning(
            warnings,
            code="insufficient_inputs",
            message=(
                "Performance report built with fewer than two portfolio artifacts; "
                "aggregate stability is limited."
            ),
        )

    for artifact in ordered_artifacts:
        run_metric = _build_per_run_metric(artifact)
        per_run_metrics.append(run_metric)

        run_id = str(run_metric["portfolio_run_id"])
        total_notional = _coerce_float(run_metric.get("total_notional_usd"), default=0.0)
        pnl_bps = _coerce_float(run_metric.get("pnl_bps"), default=0.0)
        unrealized_pnl = _coerce_float(run_metric.get("unrealized_pnl_usd"), default=0.0)
        concentration = _extract_mapping(run_metric.get("concentration"))
        top1_share = _coerce_float(concentration.get("top1_notional_share"), default=0.0)
        top3_share = _coerce_float(concentration.get("top3_notional_share"), default=0.0)

        if total_notional <= 0.0:
            quality_flags.add("zero_notional")
            _add_warning(
                warnings,
                code="zero_notional",
                message=f"Portfolio run {run_id} has zero notional exposure.",
            )
        if top1_share > top1_share_threshold:
            quality_flags.add("extreme_concentration_top1")
            _add_warning(
                warnings,
                code="extreme_concentration_top1",
                message=(
                    f"Portfolio run {run_id} top1 notional share {top1_share:.6f} "
                    f"exceeds threshold {top1_share_threshold:.6f}."
                ),
            )
        if top3_share > top3_share_threshold:
            quality_flags.add("extreme_concentration_top3")
            _add_warning(
                warnings,
                code="extreme_concentration_top3",
                message=(
                    f"Portfolio run {run_id} top3 notional share {top3_share:.6f} "
                    f"exceeds threshold {top3_share_threshold:.6f}."
                ),
            )
        if unrealized_pnl <= negative_pnl_usd_threshold or pnl_bps <= negative_pnl_bps_threshold:
            quality_flags.add("negative_pnl_large")
            _add_warning(
                warnings,
                code="negative_pnl_large",
                message=(
                    f"Portfolio run {run_id} unrealized_pnl_usd={unrealized_pnl:.2f}, "
                    f"pnl_bps={pnl_bps:.6f} breached negative PnL threshold."
                ),
            )

    aggregate_metrics = _aggregate_metrics(per_run_metrics)
    ordered_warning_keys = sorted(warnings.keys(), key=lambda item: item)
    return (
        tuple(per_run_metrics),
        aggregate_metrics,
        tuple(sorted(quality_flags)),
        tuple(warnings[key] for key in ordered_warning_keys),
    )


def _build_per_run_metric(artifact: Mapping[str, Any]) -> dict[str, Any]:
    run_id = _optional_text(artifact.get("run_id")) or "unknown_run"
    payload_hash = _resolve_payload_hash(artifact)
    generated_at_utc = _optional_text(artifact.get("generated_at_utc"))

    counts_obj = _extract_mapping(artifact.get("counts"))
    n_ledger = _coerce_int(
        counts_obj.get("n_ledger_entries"),
        default=_sequence_len(artifact.get("ledger_entries")),
    )
    n_positions = _coerce_int(
        counts_obj.get("n_positions"),
        default=_sequence_len(artifact.get("positions")),
    )
    n_duplicates = _coerce_int(
        counts_obj.get("n_duplicate_client_order_id_ignored"),
        default=_coerce_int(counts_obj.get("n_duplicates"), default=0),
    )

    total_notional = _resolve_total_notional(artifact)
    unrealized_pnl = _resolve_unrealized_pnl(artifact)
    pnl_bps = (
        _round_metric((unrealized_pnl / total_notional) * 10_000.0) if total_notional > 0 else 0.0
    )

    notionals = _position_notionals(artifact)
    top1_share = 0.0
    top3_share = 0.0
    if total_notional > 0.0 and notionals:
        notionals_desc = sorted(notionals, reverse=True)
        top1_share = _round_metric(notionals_desc[0] / total_notional)
        top3_share = _round_metric(sum(notionals_desc[:3]) / total_notional)

    category_share = _category_share(artifact, total_notional=total_notional)
    exposure = _exposure_by_token_side(artifact)

    metric: dict[str, Any] = {
        "portfolio_run_id": run_id,
        "portfolio_payload_hash": payload_hash,
        "generated_at_utc": generated_at_utc,
        "counts": {
            "n_ledger": n_ledger,
            "n_positions": n_positions,
        },
        "n_duplicate_client_order_id_ignored": n_duplicates,
        "total_notional_usd": _round_money(total_notional),
        "unrealized_pnl_usd": _round_money(unrealized_pnl),
        "pnl_bps": pnl_bps,
        "concentration": {
            "top1_notional_share": top1_share,
            "top3_notional_share": top3_share,
            "by_category_share": category_share,
        },
        "exposure_by_token_side": exposure,
    }
    return metric


def _aggregate_metrics(per_run_metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl_values = [_coerce_float(metric.get("pnl_bps"), default=0.0) for metric in per_run_metrics]
    unrealized_values = [
        _coerce_float(metric.get("unrealized_pnl_usd"), default=0.0) for metric in per_run_metrics
    ]
    zero_notional_count = sum(
        1
        for metric in per_run_metrics
        if _coerce_float(metric.get("total_notional_usd"), default=0.0) <= 0.0
    )
    total = len(per_run_metrics)

    return {
        "n_inputs": total,
        "mean_pnl_bps": _round_metric(_mean(pnl_values)),
        "median_pnl_bps": _round_metric(_median(pnl_values)),
        "worst_pnl_bps": _round_metric(min(pnl_values)) if pnl_values else 0.0,
        "best_pnl_bps": _round_metric(max(pnl_values)) if pnl_values else 0.0,
        "mean_unrealized_pnl_usd": _round_money(_mean(unrealized_values)),
        "median_unrealized_pnl_usd": _round_money(_median(unrealized_values)),
        "coverage": {
            "zero_notional_inputs": zero_notional_count,
            "nonzero_notional_inputs": max(total - zero_notional_count, 0),
            "zero_notional_rate": _round_metric(zero_notional_count / total if total > 0 else 0.0),
        },
    }


def _resolve_total_notional(artifact: Mapping[str, Any]) -> float:
    valuation = _extract_mapping(artifact.get("valuation"))
    summary = _extract_mapping(valuation.get("summary"))
    direct = _coerce_optional_float(summary.get("total_notional_exposure_usd"))
    if direct is not None:
        return max(direct, 0.0)
    notionals = _position_notionals(artifact)
    return max(sum(notionals), 0.0)


def _resolve_unrealized_pnl(artifact: Mapping[str, Any]) -> float:
    valuation = _extract_mapping(artifact.get("valuation"))
    summary = _extract_mapping(valuation.get("summary"))
    direct = _coerce_optional_float(summary.get("total_unrealized_pnl_usd"))
    if direct is not None:
        return direct

    per_position_obj = valuation.get("per_position")
    if _is_sequence(per_position_obj):
        total = 0.0
        for raw in per_position_obj:
            entry = _extract_mapping(raw)
            total += _coerce_float(entry.get("unrealized_pnl_usd"), default=0.0)
        return total
    return 0.0


def _position_notionals(artifact: Mapping[str, Any]) -> list[float]:
    valuation = _extract_mapping(artifact.get("valuation"))
    per_position_obj = valuation.get("per_position")
    values: list[float] = []
    if _is_sequence(per_position_obj):
        for raw in per_position_obj:
            entry = _extract_mapping(raw)
            value = _coerce_optional_float(entry.get("notional_exposure_usd"))
            if value is not None and value > 0.0:
                values.append(value)
    if values:
        return values

    positions_obj = artifact.get("positions")
    if _is_sequence(positions_obj):
        for raw in positions_obj:
            entry = _extract_mapping(raw)
            value = _coerce_optional_float(entry.get("notional_usd"))
            if value is not None and value > 0.0:
                values.append(value)
    return values


def _category_share(artifact: Mapping[str, Any], *, total_notional: float) -> dict[str, float]:
    if total_notional <= 0.0:
        return {}

    category_notional: dict[str, float] = {}
    valuation = _extract_mapping(artifact.get("valuation"))
    per_position_obj = valuation.get("per_position")
    if _is_sequence(per_position_obj):
        for raw in per_position_obj:
            entry = _extract_mapping(raw)
            category = _optional_text(entry.get("category"))
            if category is None:
                continue
            value = _coerce_optional_float(entry.get("notional_exposure_usd"))
            if value is None or value <= 0.0:
                continue
            category_notional[category] = category_notional.get(category, 0.0) + value
    if not category_notional:
        return {}

    out: dict[str, float] = {}
    for category in sorted(category_notional.keys()):
        out[category] = _round_metric(category_notional[category] / total_notional)
    return out


def _exposure_by_token_side(artifact: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    positions_obj = artifact.get("positions")
    if not _is_sequence(positions_obj):
        return ()

    out: list[dict[str, Any]] = []
    for raw in positions_obj:
        entry = _extract_mapping(raw)
        token_id = _optional_text(entry.get("token_id"))
        side = _optional_text(entry.get("side"))
        if token_id is None or side not in {"BUY_YES", "BUY_NO"}:
            continue
        quantity = _coerce_float(entry.get("quantity"), default=0.0)
        notional = _coerce_float(entry.get("notional_usd"), default=0.0)
        signed_notional = notional if side == "BUY_YES" else -notional
        out.append(
            {
                "token_id": token_id,
                "side": side,
                "quantity": _round_quantity(quantity),
                "notional_usd": _round_money(notional),
                "signed_notional_usd": _round_money(signed_notional),
            }
        )
    out.sort(key=lambda item: (str(item["token_id"]), str(item["side"])))
    return tuple(out)


def _resolve_payload_hash(artifact: Mapping[str, Any]) -> str:
    value = _optional_text(artifact.get("portfolio_payload_hash"))
    if value is not None and _is_sha256(value):
        return value
    return canonical_hash(artifact)


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


def _extract_mapping(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    return {}


def _is_sequence(raw: Any) -> TypeGuard[Sequence[Any]]:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))


def _sequence_len(raw: Any) -> int:
    if _is_sequence(raw):
        return len(raw)
    return 0


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _coerce_int(raw: Any, *, default: int) -> int:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


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


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(median(values))


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _round_metric(value: float) -> float:
    return round(float(value), 6)


def _round_quantity(value: float) -> float:
    return round(float(value), 2)
