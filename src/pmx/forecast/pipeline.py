from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pmx.backtest.asof_dataset import Example
from pmx.backtest.metrics import aggregate_metrics
from pmx.forecast.calibration import (
    Calibrator,
    calibrate_probabilities,
    calibrator_hash,
    fit_calibrator,
)
from pmx.forecast.models import build_model_hash, compute_probabilities, extract_top_drivers
from pmx.forecast.uncertainty import (
    ConformalIntervalModel,
    build_intervals,
    conformal_hash,
    fit_split_conformal,
)


@dataclass(frozen=True, slots=True)
class ForecastRecord:
    token_id: str
    market_id: str
    decision_ts: datetime
    p_raw: float
    p_cal: float
    p_a: float
    p_b: float
    interval_50: tuple[float, float]
    interval_90: tuple[float, float]
    drivers: tuple[dict[str, float | str], ...]
    no_trade_flags: tuple[str, ...]
    calibration_hash: str
    uncertainty_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "market_id": self.market_id,
            "decision_ts": self.decision_ts.isoformat(),
            "p_raw": round(self.p_raw, 8),
            "p_cal": round(self.p_cal, 8),
            "p_a": round(self.p_a, 8),
            "p_b": round(self.p_b, 8),
            "interval_50": {
                "low": round(self.interval_50[0], 8),
                "high": round(self.interval_50[1], 8),
            },
            "interval_90": {
                "low": round(self.interval_90[0], 8),
                "high": round(self.interval_90[1], 8),
            },
            "drivers": list(self.drivers),
            "no_trade_flags": list(self.no_trade_flags),
            "calibration_hash": self.calibration_hash,
            "uncertainty_hash": self.uncertainty_hash,
        }


@dataclass(frozen=True, slots=True)
class CalibrationWindow:
    decision_ts: datetime
    train_count: int
    calibrator: dict[str, Any]
    uncertainty: dict[str, Any]
    calibration_hash: str
    uncertainty_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision_ts": self.decision_ts.isoformat(),
            "train_count": self.train_count,
            "calibrator": self.calibrator,
            "uncertainty": self.uncertainty,
            "calibration_hash": self.calibration_hash,
            "uncertainty_hash": self.uncertainty_hash,
        }


@dataclass(frozen=True, slots=True)
class ForecastRunResult:
    forecasts: tuple[ForecastRecord, ...]
    metrics: dict[str, Any]
    interval_report: dict[str, float]
    dataset_hash: str
    model_hash: str
    calibration_hash: str
    uncertainty_hash: str
    forecast_payload_hash: str
    calibration_windows: tuple[CalibrationWindow, ...]
    example_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "forecasts": [item.as_dict() for item in self.forecasts],
            "metrics": self.metrics,
            "interval_report": self.interval_report,
            "dataset_hash": self.dataset_hash,
            "model_hash": self.model_hash,
            "calibration_hash": self.calibration_hash,
            "uncertainty_hash": self.uncertainty_hash,
            "forecast_payload_hash": self.forecast_payload_hash,
            "calibration_windows": [item.as_dict() for item in self.calibration_windows],
            "example_count": self.example_count,
        }


@dataclass(frozen=True, slots=True)
class _WindowModels:
    calibrator: Calibrator
    uncertainty_model: ConformalIntervalModel
    calibration_hash: str
    uncertainty_hash: str


def build_train_eval_sets(
    examples: Sequence[Example],
    *,
    decision_ts: datetime,
) -> tuple[tuple[Example, ...], tuple[Example, ...]]:
    train = tuple(
        sorted(
            (item for item in examples if item.decision_ts < decision_ts),
            key=lambda item: (item.decision_ts, item.token_id, item.market_id),
        )
    )
    evaluate = tuple(
        sorted(
            (item for item in examples if item.decision_ts == decision_ts),
            key=lambda item: (item.token_id, item.market_id),
        )
    )
    return train, evaluate


def run_forecast_pipeline(
    examples: Sequence[Example],
    *,
    min_isotonic_samples: int = 30,
    min_conformal_samples: int = 20,
    driver_top_k: int = 5,
) -> ForecastRunResult:
    ordered_examples = tuple(
        sorted(
            examples,
            key=lambda item: (item.decision_ts, item.token_id, item.market_id),
        )
    )
    if not ordered_examples:
        empty_metrics = {
            "raw": aggregate_metrics([], []),
            "calibrated": aggregate_metrics([], []),
        }
        return ForecastRunResult(
            forecasts=(),
            metrics=empty_metrics,
            interval_report=_empty_interval_report(),
            dataset_hash=_stable_hash([]),
            model_hash=build_model_hash(),
            calibration_hash=_stable_hash([]),
            uncertainty_hash=_stable_hash([]),
            forecast_payload_hash=_stable_hash([]),
            calibration_windows=(),
            example_count=0,
        )

    decisions = sorted({item.decision_ts for item in ordered_examples})
    forecasts_with_label: list[tuple[ForecastRecord, int]] = []
    windows: list[CalibrationWindow] = []
    raw_all: list[float] = []
    calibrated_all: list[float] = []
    labels_all: list[int] = []

    for decision_ts in decisions:
        train_examples, eval_examples = build_train_eval_sets(
            ordered_examples,
            decision_ts=decision_ts,
        )
        window_models = _fit_window_models(
            train_examples=train_examples,
            min_isotonic_samples=min_isotonic_samples,
            min_conformal_samples=min_conformal_samples,
        )
        windows.append(
            CalibrationWindow(
                decision_ts=decision_ts,
                train_count=len(train_examples),
                calibrator=window_models.calibrator.as_dict(),
                uncertainty=window_models.uncertainty_model.as_dict(),
                calibration_hash=window_models.calibration_hash,
                uncertainty_hash=window_models.uncertainty_hash,
            )
        )

        for example in eval_examples:
            p_a, p_b, p_raw = compute_probabilities(
                price_prob=example.price_prob,
                features=example.features_json,
            )
            p_cal = window_models.calibrator.predict(p_raw)
            intervals = build_intervals(window_models.uncertainty_model, p_cal)
            drivers = extract_top_drivers(
                features=example.features_json,
                price_prob=example.price_prob,
                top_k=driver_top_k,
            )
            forecast = ForecastRecord(
                token_id=example.token_id,
                market_id=example.market_id,
                decision_ts=example.decision_ts,
                p_raw=p_raw,
                p_cal=p_cal,
                p_a=p_a,
                p_b=p_b,
                interval_50=intervals["interval_50"],
                interval_90=intervals["interval_90"],
                drivers=tuple(drivers),
                no_trade_flags=infer_no_trade_flags(example.features_json),
                calibration_hash=window_models.calibration_hash,
                uncertainty_hash=window_models.uncertainty_hash,
            )
            forecasts_with_label.append((forecast, example.outcome_y))
            labels_all.append(example.outcome_y)
            raw_all.append(p_raw)
            calibrated_all.append(p_cal)

    forecasts = tuple(item[0] for item in forecasts_with_label)
    metrics = {
        "raw": aggregate_metrics(labels_all, raw_all),
        "calibrated": aggregate_metrics(labels_all, calibrated_all),
    }
    interval_report = _interval_report(forecasts_with_label)

    dataset_hash = _dataset_hash(ordered_examples)
    model_hash = build_model_hash()
    calibration_hash_value = _stable_hash(
        [
            {
                "decision_ts": item.decision_ts.isoformat(),
                "calibration_hash": item.calibration_hash,
                "calibrator": item.calibrator,
                "train_count": item.train_count,
            }
            for item in windows
        ]
    )
    uncertainty_hash_value = _stable_hash(
        [
            {
                "decision_ts": item.decision_ts.isoformat(),
                "uncertainty_hash": item.uncertainty_hash,
                "uncertainty": item.uncertainty,
                "train_count": item.train_count,
            }
            for item in windows
        ]
    )
    payload_hash = _stable_hash([item.as_dict() for item in forecasts])
    return ForecastRunResult(
        forecasts=forecasts,
        metrics=metrics,
        interval_report=interval_report,
        dataset_hash=dataset_hash,
        model_hash=model_hash,
        calibration_hash=calibration_hash_value,
        uncertainty_hash=uncertainty_hash_value,
        forecast_payload_hash=payload_hash,
        calibration_windows=tuple(windows),
        example_count=len(ordered_examples),
    )


def infer_no_trade_flags(features: dict[str, Any]) -> tuple[str, ...]:
    required = (
        "spread_bps",
        "top_depth_bid",
        "top_depth_ask",
        "stale_seconds_last_trade",
        "stale_seconds_last_book",
    )
    flags: set[str] = set()
    missing = [key for key in required if features.get(key) is None]
    if missing:
        flags.add("insufficient_data")

    spread = _as_float(features.get("spread_bps"), default=0.0)
    depth_bid = _as_float(features.get("top_depth_bid"), default=0.0)
    depth_ask = _as_float(features.get("top_depth_ask"), default=0.0)
    if spread >= 1500.0 or (depth_bid + depth_ask) < 1.0:
        flags.add("illiquid")

    stale_trade = _as_float(features.get("stale_seconds_last_trade"), default=0.0)
    stale_book = _as_float(features.get("stale_seconds_last_book"), default=0.0)
    if max(stale_trade, stale_book) >= 14_400.0:
        flags.add("stale")

    return tuple(sorted(flags))


def _fit_window_models(
    *,
    train_examples: tuple[Example, ...],
    min_isotonic_samples: int,
    min_conformal_samples: int,
) -> _WindowModels:
    raw_probs: list[float] = []
    labels: list[int] = []
    for example in train_examples:
        _, _, p_raw = compute_probabilities(
            price_prob=example.price_prob,
            features=example.features_json,
        )
        raw_probs.append(p_raw)
        labels.append(example.outcome_y)

    calibrator = fit_calibrator(
        raw_probs,
        labels,
        min_isotonic_samples=min_isotonic_samples,
    )
    calibrated = calibrate_probabilities(calibrator, raw_probs)
    uncertainty_model = fit_split_conformal(
        labels,
        calibrated,
        min_calibration=min_conformal_samples,
    )
    return _WindowModels(
        calibrator=calibrator,
        uncertainty_model=uncertainty_model,
        calibration_hash=calibrator_hash(calibrator),
        uncertainty_hash=conformal_hash(uncertainty_model),
    )


def _interval_report(
    forecasts_with_label: Sequence[tuple[ForecastRecord, int]],
) -> dict[str, float]:
    if not forecasts_with_label:
        return _empty_interval_report()

    hit_50 = 0
    hit_90 = 0
    width_50_sum = 0.0
    width_90_sum = 0.0
    total = float(len(forecasts_with_label))

    for forecast, y_value in forecasts_with_label:
        y_float = float(y_value)
        lo50, hi50 = forecast.interval_50
        lo90, hi90 = forecast.interval_90
        if lo50 <= y_float <= hi50:
            hit_50 += 1
        if lo90 <= y_float <= hi90:
            hit_90 += 1
        width_50_sum += hi50 - lo50
        width_90_sum += hi90 - lo90

    return {
        "coverage_50": hit_50 / total,
        "coverage_90": hit_90 / total,
        "sharpness_50": width_50_sum / total,
        "sharpness_90": width_90_sum / total,
    }


def _empty_interval_report() -> dict[str, float]:
    return {
        "coverage_50": 0.0,
        "coverage_90": 0.0,
        "sharpness_50": 0.0,
        "sharpness_90": 0.0,
    }


def _dataset_hash(examples: Sequence[Example]) -> str:
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


def _stable_hash(payload: Any) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _as_float(raw: Any, *, default: float) -> float:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default
