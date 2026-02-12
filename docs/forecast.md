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

## Decision Layer v1 (Milestone 10.2)
- Decision job is artifact-only and does not require DB access:
  - input: forecast artifact JSON
  - output: `artifacts/decisions/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.decide_from_forecast `
  --forecast-artifact artifacts/forecasts/<forecast_run_id>.json `
  --min-edge-bps 50 `
  --robust-mode require_positive_low90 `
  --max-items 200 `
  --artifacts-root artifacts
```
- Policy (`decision_policy.v1`) per forecast row:
  - `edge = p_cal - price_prob`
  - `edge_bps = 10000 * edge`
  - robust checks:
    - `require_positive_low90`: BUY_YES requires `interval_90.low - price_prob > 0`
    - `require_negative_high90`: BUY_NO requires `interval_90.high - price_prob < 0`
    - `none`: threshold-only checks
  - blocking flags force `NO_TRADE` with deterministic reason codes:
    - `illiquid`, `stale`, `insufficient_data`
    - `poor_calibration`, `insufficient_calibration_data`
    - `conformal_invalid_intervals`, `conformal_degenerate_intervals`
    - `insufficient_uncertainty_data`
- Decision artifact schema:
  - `decision_schema_version = "decision_artifact.v1"`
  - validator:
    - `pmx.decisions.validate_artifact.validate_decision_artifact(...)`
- Decision artifact hashes:
  - `policy_hash`
  - `decision_items_hash`
  - `decision_payload_hash`

## Trade Plan Layer v1 (Milestone 10.3)
- Trade-plan job is artifact-only and does not require DB access:
  - input: decision artifact JSON
  - output: `artifacts/trade_plans/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.trade_plan_from_decision `
  --decision-artifact artifacts/decisions/<decision_run_id>.json `
  --max-orders 200 `
  --max-total-notional-usd 5000 `
  --max-notional-per-market-usd 500 `
  --max-notional-per-category-usd 2000 `
  --sizing-mode fixed_notional `
  --fixed-notional-usd 25 `
  --artifacts-root artifacts
```
- Policy (`trade_plan_policy.v1`) transforms decision rows into:
  - `orders`: deterministic paper execution stubs for tradable items.
  - `skipped`: deterministic exclusions for `NO_TRADE`, quality blocks, and cap
    limits.
- Sizing modes:
  - `fixed_notional`: constant `fixed_notional_usd` per order.
  - `scaled_by_edge`: `base_notional_usd * clamp(abs(edge_bps)/target_edge_bps, min_scale, max_scale)`.
- Risk caps (no partial fills in v1):
  - `max_orders`
  - `max_total_notional_usd`
  - `max_notional_per_market_usd`
  - `max_notional_per_category_usd`
- Trade-plan artifact schema:
  - `artifact_schema_version = "trade_plan_artifact.v1"`
  - validator:
    - `pmx.trade_plan.validate_artifact.validate_trade_plan_artifact(...)`
- Trade-plan hashes:
  - `policy_hash`
  - `orders_hash`
  - `trade_plan_payload_hash`

## Execution Stub v1 (Milestone 10.4)
- Execution job is artifact-only and does not require DB or network access:
  - input: trade-plan artifact JSON
  - output: `artifacts/executions/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.execute_trade_plan_stub `
  --trade-plan-artifact artifacts/trade_plans/<trade_plan_run_id>.json `
  --mode simulate_submit `
  --max-orders 200 `
  --simulate-reject-modulo 0 `
  --simulate-reject-remainder 0 `
  --artifacts-root artifacts
```
- Execution policy (`execution_policy.v1`) behavior:
  - accepts only `action="TRADE"` orders.
  - deterministic `client_order_id` from:
    `input_run_id|market_id|token_id|side|notional_usd|price|quantity`.
  - deterministic idempotency key:
    `sha256(trade_plan_payload_hash + "|" + trade_plan_policy_hash)`.
  - simulated statuses:
    - default `SIMULATED_SUBMITTED`
    - optional deterministic rejection via token hash modulo rule.
- Execution artifact schema:
  - `artifact_schema_version = "execution_artifact.v1"`
  - validator:
    - `pmx.execution.validate_artifact.validate_execution_artifact(...)`
- Execution hashes:
  - `execution_policy_hash`
  - `orders_hash`
  - `execution_payload_hash`

## Portfolio Accounting v1 (Milestone 10.5)
- Portfolio job is artifact-only and offline:
  - input: one or more execution artifacts
  - output: `artifacts/portfolios/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.portfolio_from_execution `
  --execution-artifacts artifacts/executions/<execution_run_id>.json `
  --artifacts-root artifacts `
  --fee-bps 0 `
  --fee-usd 0 `
  --mark-source execution_price
```
- Multiple inputs can be provided by repeating `--execution-artifacts` or with
  comma-separated paths.
- Policy (`portfolio_accounting.v1`) behavior:
  - deterministic ledger from `SIMULATED_SUBMITTED` orders only.
  - deterministic duplicate handling on `client_order_id` (first wins, later
    duplicates ignored with warning).
  - deterministic position aggregation (`token_id`, `side`) with VWAP
    `avg_cost`.
  - deterministic mark-to-model valuation with configurable mark source:
    - `execution_price`
    - `execution_p_cal`
    - `execution_price_prob`
    - optional fallback map: `--reference-prices-json`.
- Portfolio artifact schema:
  - `artifact_schema_version = "portfolio_artifact.v1"`
  - validator:
    - `pmx.portfolio.validate_artifact.validate_portfolio_artifact(...)`
- Portfolio hashes:
  - `portfolio_policy_hash`
  - `ledger_hash`
  - `positions_hash`
  - `valuation_hash`
  - `portfolio_payload_hash`

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
