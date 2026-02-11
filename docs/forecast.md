# Forecast Core v1 (Milestone 10.1)

## Scope
- Baseline A: `p_raw_a = price_prob` from as-of market data.
- Baseline B: deterministic logistic score on `micro_v1` features.
- Ensemble stacker: deterministic logistic combiner over A/B outputs.
- Calibration: isotonic by default, Platt fallback for small sample windows.
- Uncertainty: deterministic split-conformal intervals (50/90).

## As-of contract
- Forecast pipeline consumes examples built by `pmx.backtest.asof_dataset`.
- Input gating is mandatory:
  - `event_ts <= decision_ts`
  - `ingested_at <= decision_ts + ingest_epsilon_seconds`
- Labels are included only when `decision_ts < resolved_ts`.

## Run
```powershell
python -m pmx.jobs.forecast_baseline_ensemble `
  --token-ids tokenA,tokenB `
  --from 2026-01-01T00:00:00Z `
  --to 2026-01-03T00:00:00Z `
  --step-hours 4 `
  --epsilon-seconds 300 `
  --feature-set micro_v1 `
  --max-tokens 200
```

## Artifact
- Path: `artifacts/forecasts/<run_id>.json`
- Includes:
  - schema/version:
    - `artifact_schema_version = "forecast_artifact.v1"`
  - run metadata: `run_id`, `code_version`, `config_hash`
  - reproducibility hashes:
    - `dataset_hash`
    - `model_hash`
    - `calibration_hash`
    - `uncertainty_hash`
    - `calibration_report_hash`
    - `uncertainty_report_hash`
    - `forecast_payload_hash`
  - per-forecast outputs:
    - `p_raw`, `p_cal`
    - `interval_50`, `interval_90`
    - deterministic `drivers`
    - `no_trade_flags` (`illiquid`, `stale`, `insufficient_data`)
  - aggregate metrics and interval quality report.
  - uncertainty diagnostics:
    - `uncertainty_report` with observed coverage/width by level (`0.5`, `0.9`)
    - sanity checks (`invalid_interval`, `degenerate_interval`, monotonic width check)
    - additive soft `quality_flags` / `quality_warnings` (no crash-only gates)
  - artifact contract can be validated via:
    - `pmx.forecast.validate_artifact.validate_forecast_artifact(...)`

## Determinism policy
- Canonical JSON hashing is centralized in `pmx.forecast.canonical`:
  - sorted object keys
  - stable separators
  - float normalization with rounding to 6 decimals before hash
- Quality metadata ordering:
  - `quality_flags`: unique + sorted
  - `quality_warnings`: sorted by `(code, message/detail)`
- Report ordering:
  - levels are fixed `[0.5, 0.9]`
  - bins use deterministic index order.

## Interpretation
- `p_raw`: uncalibrated ensemble probability.
- `p_cal`: calibrated probability for decisioning.
- `interval_50` / `interval_90`: uncertainty bands from conformal residual quantiles.
- `drivers`: deterministic contribution ranking (`coefficient * value`).
- `uncertainty_report`: post-hoc diagnostic report; it does not change generated
  intervals and is additive for audit/quality monitoring.
