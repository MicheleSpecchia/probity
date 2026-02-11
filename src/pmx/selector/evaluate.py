from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from pmx.backtest.asof_dataset import Example, build_asof_dataset
from pmx.backtest.metrics import aggregate_metrics
from pmx.models.baselines import baseline_a_price, baseline_b_micro

DEFAULT_SELECTOR_VERSIONS: tuple[str, ...] = (
    "selector_v1",
    "baseline_top_volume",
    "baseline_random_stratified",
)
DEFAULT_STEP_HOURS = 4


@dataclass(frozen=True, slots=True)
class SelectionRunInfo:
    selector_version: str
    selection_run_id: int
    decision_ts: datetime


@dataclass(frozen=True, slots=True)
class SelectedToken:
    market_id: str
    token_id: str
    ttr_bucket: str


def evaluate_selector_runs(
    conn: psycopg.Connection,
    *,
    decision_ts: datetime,
    epsilon_s: int,
    window_hours: int,
    feature_set: str = "micro_v1",
    selector_versions: Sequence[str] = DEFAULT_SELECTOR_VERSIONS,
    selection_run_ids: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    if epsilon_s <= 0:
        raise ValueError("epsilon_s must be > 0")
    if window_hours <= 0:
        raise ValueError("window_hours must be > 0")

    decision = _as_utc_datetime(decision_ts)
    lower_bound = decision - timedelta(hours=window_hours)
    versions = tuple(sorted({str(item) for item in selector_versions if str(item).strip()}))
    if not versions:
        raise ValueError("selector_versions cannot be empty")

    run_map = _resolve_selection_runs(
        conn,
        versions=versions,
        decision_ts=decision,
        lower_bound=lower_bound,
        explicit_selection_run_ids=selection_run_ids,
    )
    decision_grid = _build_decision_grid(lower_bound, decision, step_hours=DEFAULT_STEP_HOURS)

    by_selector: dict[str, Any] = {}
    for version in versions:
        run_info = run_map.get(version)
        if run_info is None:
            by_selector[version] = _missing_selector_report()
            continue

        selected_tokens = _load_selected_tokens(conn, run_info.selection_run_id)
        token_ids = sorted({row.token_id for row in selected_tokens})
        if not token_ids:
            by_selector[version] = _empty_selector_report(run_info.selection_run_id)
            continue

        dataset = build_asof_dataset(
            conn,
            token_ids=token_ids,
            decision_ts_list=decision_grid,
            epsilon_s=epsilon_s,
            feature_set=feature_set,
            outcome_provider=None,
        )
        metrics = _evaluate_dataset(dataset.examples)
        entropy_summary = _entropy_summary([item.price_prob for item in dataset.examples])
        ttr_distribution = dict(
            sorted(Counter(item.ttr_bucket for item in selected_tokens).items())
        )

        by_selector[version] = {
            "status": "ok",
            "selection_run_id": run_info.selection_run_id,
            "selection_decision_ts": run_info.decision_ts.isoformat(),
            "token_ids": token_ids,
            "token_ids_hash": _stable_hash(token_ids),
            "counts": {
                "selected_tokens": len(token_ids),
                "examples": len(dataset.examples),
                "skipped_no_outcome": dataset.skipped_no_outcome,
                "skipped_missing_features": dataset.skipped_missing_features,
                "skipped_missing_price": dataset.skipped_missing_price,
            },
            "dataset_hash": _dataset_hash(dataset.examples),
            "avg_brier_a": metrics["baseline_a"]["brier"],
            "avg_brier_b": metrics["baseline_b"]["brier"],
            "ece_a": metrics["baseline_a"]["ece"],
            "ece_b": metrics["baseline_b"]["ece"],
            "baseline_a": metrics["baseline_a"],
            "baseline_b": metrics["baseline_b"],
            "pq_entropy_distribution": entropy_summary,
            "price_prob_stats": _float_summary([item.price_prob for item in dataset.examples]),
            "ttr_bucket_distribution": ttr_distribution,
        }

    report = {
        "decision_ts": decision.isoformat(),
        "window_hours": window_hours,
        "epsilon_seconds": epsilon_s,
        "step_hours": DEFAULT_STEP_HOURS,
        "feature_set": feature_set,
        "selector_versions": list(versions),
        "selection_runs": {
            version: run_map[version].selection_run_id if version in run_map else None
            for version in versions
        },
        "selectors": by_selector,
    }
    report["report_hash"] = _stable_hash(report)
    return report


def _resolve_selection_runs(
    conn: psycopg.Connection,
    *,
    versions: tuple[str, ...],
    decision_ts: datetime,
    lower_bound: datetime,
    explicit_selection_run_ids: Mapping[str, int] | None,
) -> dict[str, SelectionRunInfo]:
    if explicit_selection_run_ids:
        out: dict[str, SelectionRunInfo] = {}
        for version in versions:
            raw_id = explicit_selection_run_ids.get(version)
            if raw_id is None:
                continue
            selection_run_id = int(raw_id)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT r.decision_ts
                    FROM selection_runs sr
                    JOIN runs r ON r.run_id = sr.run_id
                    WHERE sr.selection_run_id = %s
                      AND sr.selector_version = %s
                    LIMIT 1
                    """,
                    (selection_run_id, version),
                )
                row = cursor.fetchone()
            if row is None:
                continue
            out[version] = SelectionRunInfo(
                selector_version=version,
                selection_run_id=selection_run_id,
                decision_ts=_as_utc_datetime(row[0]),
            )
        return out

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                sr.selector_version,
                sr.selection_run_id,
                r.decision_ts
            FROM selection_runs sr
            JOIN runs r ON r.run_id = sr.run_id
            WHERE sr.selector_version = ANY(%s)
              AND r.decision_ts <= %s
              AND r.decision_ts >= %s
            ORDER BY sr.selector_version ASC, r.decision_ts DESC, sr.selection_run_id DESC
            """,
            (list(versions), decision_ts, lower_bound),
        )
        rows = cursor.fetchall()

    run_infos: dict[str, SelectionRunInfo] = {}
    for row in rows:
        version = str(row[0])
        if version in run_infos:
            continue
        run_infos[version] = SelectionRunInfo(
            selector_version=version,
            selection_run_id=int(row[1]),
            decision_ts=_as_utc_datetime(row[2]),
        )
    return run_infos


def _load_selected_tokens(conn: psycopg.Connection, selection_run_id: int) -> list[SelectedToken]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                si.market_id,
                COALESCE(NULLIF(si.reason->>'token_id', ''), mt.token_id) AS token_id,
                COALESCE(NULLIF(si.reason->>'ttr_bucket', ''), 'unknown') AS ttr_bucket
            FROM selection_items si
            LEFT JOIN LATERAL (
                SELECT token_id
                FROM market_tokens
                WHERE market_id = si.market_id
                ORDER BY token_id ASC
                LIMIT 1
            ) mt ON TRUE
            WHERE si.selection_run_id = %s
            ORDER BY si.rank ASC, si.market_id ASC
            """,
            (selection_run_id,),
        )
        rows = cursor.fetchall()

    selected: list[SelectedToken] = []
    for row in rows:
        token_id = _as_text(row[1])
        market_id = _as_text(row[0])
        if token_id is None or market_id is None:
            continue
        selected.append(
            SelectedToken(
                market_id=market_id,
                token_id=token_id,
                ttr_bucket=_as_text(row[2]) or "unknown",
            )
        )
    return selected


def _evaluate_dataset(examples: tuple[Example, ...]) -> dict[str, Any]:
    y_true: list[int] = []
    pred_a: list[float] = []
    pred_b: list[float] = []

    for item in examples:
        y_true.append(item.outcome_y)
        pred_a.append(baseline_a_price(item.price_prob))
        pred_b.append(baseline_b_micro(item.features_json))

    return {
        "baseline_a": aggregate_metrics(y_true, pred_a),
        "baseline_b": aggregate_metrics(y_true, pred_b),
    }


def _dataset_hash(examples: tuple[Example, ...]) -> str:
    payload = [
        {
            "token_id": item.token_id,
            "market_id": item.market_id,
            "decision_ts": item.decision_ts.isoformat(),
            "price_prob": round(item.price_prob, 8),
            "outcome_y": item.outcome_y,
            "features_json": item.features_json,
        }
        for item in examples
    ]
    return _stable_hash(payload)


def _entropy_summary(probabilities: Sequence[float]) -> dict[str, float]:
    entropy_values = [_binary_entropy(item) for item in probabilities]
    return _float_summary(entropy_values)


def _binary_entropy(probability: float) -> float:
    p = _clamp(float(probability), 1e-6, 1.0 - 1e-6)
    entropy = -(p * math.log(p) + (1.0 - p) * math.log(1.0 - p)) / math.log(2.0)
    return round(_clamp(entropy, 0.0, 1.0), 6)


def _float_summary(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {
            "mean": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    ordered = sorted(float(item) for item in values)
    return {
        "mean": round(sum(ordered) / len(ordered), 6),
        "p50": round(_percentile(ordered, 0.50), 6),
        "p90": round(_percentile(ordered, 0.90), 6),
        "min": round(ordered[0], 6),
        "max": round(ordered[-1], 6),
    }


def _percentile(sorted_values: Sequence[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    index = round((len(sorted_values) - 1) * quantile)
    clamped_index = max(0, min(index, len(sorted_values) - 1))
    return float(sorted_values[clamped_index])


def _build_decision_grid(
    lower_bound: datetime,
    upper_bound: datetime,
    *,
    step_hours: int,
) -> list[datetime]:
    out: list[datetime] = []
    step = timedelta(hours=step_hours)
    current = lower_bound
    while current <= upper_bound:
        out.append(current)
        current = current + step
    return out


def _missing_selector_report() -> dict[str, Any]:
    return {
        "status": "missing_selection",
        "selection_run_id": None,
        "selection_decision_ts": None,
        "token_ids": [],
        "token_ids_hash": _stable_hash([]),
        "counts": {
            "selected_tokens": 0,
            "examples": 0,
            "skipped_no_outcome": 0,
            "skipped_missing_features": 0,
            "skipped_missing_price": 0,
        },
        "dataset_hash": _stable_hash([]),
        "avg_brier_a": 0.0,
        "avg_brier_b": 0.0,
        "ece_a": 0.0,
        "ece_b": 0.0,
        "baseline_a": aggregate_metrics([], []),
        "baseline_b": aggregate_metrics([], []),
        "pq_entropy_distribution": _float_summary([]),
        "price_prob_stats": _float_summary([]),
        "ttr_bucket_distribution": {},
    }


def _empty_selector_report(selection_run_id: int) -> dict[str, Any]:
    payload = _missing_selector_report()
    payload["status"] = "empty_selection"
    payload["selection_run_id"] = selection_run_id
    return payload


def _stable_hash(value: Any) -> str:
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
