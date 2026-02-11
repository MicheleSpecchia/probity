# Backtest Baselines (Milestone 8.2)

## Scope
- Baseline A: `p = price_prob` as-of (`mid` from book or last trade price).
- Baseline B: deterministic logistic score over `micro_v1` features.
- Walk-forward dataset builder with strict as-of gating.

## As-of contract
- For every decision timestamp `t`, examples are built only from rows satisfying:
  - `event_ts <= t`
  - `ingested_at <= t + epsilon`
- Feature rows are selected from `feature_snapshots` with:
  - `asof_ts <= t`
  - deterministic latest tie-break (`asof_ts DESC`, id DESC)
- Labels use resolved market outcome only when `t < resolved_ts`.

## Run
```powershell
python -m pmx.jobs.backtest_baselines `
  --token-ids tokenA,tokenB `
  --from 2026-01-01T00:00:00Z `
  --to 2026-01-03T00:00:00Z `
  --step-hours 4 `
  --epsilon-seconds 300 `
  --feature-set micro_v1
```

Alternative token selection:
```powershell
python -m pmx.jobs.backtest_baselines `
  --max-tokens 200 `
  --from 2026-01-01T00:00:00Z `
  --to 2026-01-03T00:00:00Z
```

## Output artifact
- Path: `artifacts/backtests/<run_id>.json`
- Includes:
  - `dataset_hash`, `config_hash`, `code_version`, `feature_set`
  - aggregate metrics (`brier`, `ece`, `sharpness`)
  - per-token metrics
  - counts:
    - `examples`
    - `skipped_no_outcome`
    - `skipped_missing_features`
    - `skipped_missing_price`

## Metric interpretation
- `brier`: lower is better.
- `ece`: lower is better calibration.
- `sharpness`: variance of probabilities; higher means more confident predictions.
- `coverage`: placeholder until interval models are added.
