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

## End-to-End Pipeline Runner v1 (Milestone 10.6)
- Pipeline runner is artifact-only and offline:
  - input: forecast artifact JSON
  - outputs: decision, trade plan, execution, portfolio, and pipeline summary artifacts
  - no DB/network dependency for the orchestration step.
- Run:
```powershell
python -m pmx.jobs.run_pipeline_stub `
  --forecast-artifact tests/fixtures/forecast/forecast_artifact_sample.json `
  --artifacts-root artifacts `
  --min-edge-bps 50 `
  --robust-mode require_positive_low90 `
  --max-items 200 `
  --sizing-mode fixed_notional `
  --fixed-notional-usd 25 `
  --max-orders 200 `
  --max-total-notional-usd 5000 `
  --max-notional-per-market-usd 500 `
  --max-notional-per-category-usd 2000 `
  --execution-mode dry_run `
  --mark-source execution_price `
  --fee-bps 0 `
  --fee-usd 0
```
- Summary artifact:
  - `artifacts/pipeline_runs/<run_id>.json`
  - `artifact_schema_version = "pipeline_run_artifact.v1"`
  - rerun with the same input artifact + params reproduces the same stage payload hashes
    and pipeline payload hash.
  - contains:
    - input forecast hash/run id
    - output stage run ids + payload hashes
    - aggregate KPIs (`n_decisions`, `n_orders`, `n_rejected`, `n_positions`,
      planned/executed notional, unrealized PnL)
    - merged `quality_flags` / `quality_warnings`
    - reproducibility hashes:
      - `pipeline_policy_hash`
      - `pipeline_outputs_hash`
      - `pipeline_payload_hash`

## Performance Report v1 (Milestone 10.7)
- Performance report is artifact-only and offline:
  - input: one or more portfolio artifacts
  - output: `artifacts/performance/<run_id>.json`
  - no DB/network dependency.
- Run:
```powershell
python -m pmx.jobs.performance_from_portfolio `
  --portfolio-artifacts tests/fixtures/portfolio/portfolio_artifact_sample_A.json,tests/fixtures/portfolio/portfolio_artifact_sample_B.json `
  --artifacts-root artifacts
```
- Optional windowing (inclusive on `generated_at_utc` when present):
```powershell
python -m pmx.jobs.performance_from_portfolio `
  --portfolio-artifacts artifacts/portfolios/run_a.json,artifacts/portfolios/run_b.json `
  --window-from 2026-02-01T00:00:00Z `
  --window-to 2026-02-28T23:59:59Z
```
- Report includes deterministic per-run and aggregate metrics:
  - counts (`n_ledger`, `n_positions`)
  - notional/PnL (`total_notional_usd`, `unrealized_pnl_usd`, `pnl_bps`)
  - concentration (`top1_notional_share`, `top3_notional_share`, category shares)
  - exposure by token/side
  - aggregate stats (mean/median/worst/best PnL, zero-notional coverage)
  - soft quality flags/warnings.
- Reproducibility hashes:
  - `performance_policy_hash`
  - `performance_inputs_hash`
  - `performance_payload_hash`

## Risk Policy v1 (Milestone 11)
- Risk job is artifact-only and offline:
  - input: trade-plan artifact JSON
  - optional input: performance report artifact JSON
  - optional input: hooks JSON (current exposure/cooldown state)
  - output: `artifacts/risks/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.risk_from_trade_plan `
  --trade-plan-artifact artifacts/trade_plans/<trade_plan_run_id>.json `
  --performance-artifact artifacts/performance/<performance_run_id>.json `
  --hooks-json tests/fixtures/risk/risk_hooks_sample.json `
  --artifacts-root artifacts
```
- Policy (`risk_policy.v1`) evaluates each trade-plan order with deterministic verdicts:
  - `ALLOW`
  - `BLOCK`
  - `DOWNSIZE` (when partial room exists and downsize is enabled)
- Deterministic rule families:
  - hard quality blocks (`illiquid`, `stale`, `insufficient_data`, etc.)
  - global / per-market / per-category notional caps
  - concentration caps (`top1`, `top3`)
  - optional cooldown hooks (`token`, `market`)
  - optional performance concentration guards.
- Risk artifact schema:
  - `artifact_schema_version = "risk_artifact.v1"`
  - validator:
    - `pmx.risk.validate_artifact.validate_risk_artifact(...)`
- Risk hashes:
  - `policy_hash`
  - `items_hash`
  - `risk_payload_hash`

## Audit Bundle v1 (Milestone 11)
- Audit-bundle job is artifact-only and offline:
  - required input: pipeline run artifact JSON
  - optional inputs: forecast override, performance artifact, risk artifact
  - output: `artifacts/audit_bundles/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.build_audit_bundle `
  --pipeline-artifact artifacts/pipeline_runs/<pipeline_run_id>.json `
  --performance-artifact artifacts/performance/<performance_run_id>.json `
  --risk-artifact artifacts/risks/<risk_run_id>.json `
  --artifacts-root artifacts
```
- Bundle contract:
  - `artifact_schema_version = "audit_bundle_artifact.v1"`
  - ordered lineage over stages (`forecast`, `decision`, `trade_plan`, `execution`,
    `portfolio`, `pipeline`, optional `performance`, optional `risk`)
  - carries per-stage hashes, run IDs, code/config identifiers when available.
- Audit bundle hashes:
  - `bundle_hash`
  - `audit_bundle_policy_hash`
  - `audit_bundle_payload_hash`

## Monitoring v1 (Milestone 11)
- Monitoring job is artifact-only and offline:
  - required input: pipeline run artifact JSON
  - optional inputs: forecast/performance/risk artifacts
  - output: `artifacts/monitoring/<run_id>.json`
- Run:
```powershell
python -m pmx.jobs.monitor_from_pipeline `
  --pipeline-artifact artifacts/pipeline_runs/<pipeline_run_id>.json `
  --performance-artifact artifacts/performance/<performance_run_id>.json `
  --risk-artifact artifacts/risks/<risk_run_id>.json `
  --artifacts-root artifacts
```
- Health semantics (`monitoring_policy.v1`):
  - `FAIL` when critical block reasons are detected (e.g. `critical_*` in risk
    block reasons / flags).
  - `WARN` when quality flags/warnings are present without critical failures.
  - `OK` otherwise.
- Monitoring artifact schema:
  - `artifact_schema_version = "monitoring_report_artifact.v1"`
  - validator:
    - `pmx.monitoring.validate_artifact.validate_monitoring_report_artifact(...)`
- Monitoring hashes:
  - `monitoring_policy_hash`
  - `monitoring_inputs_hash`
  - `monitoring_payload_hash`

## E2E Smoke Runner v1 (Milestone 12)
- Smoke runner is artifact-only and offline:
  - input: forecast artifact JSON (fixture by default)
  - output: `artifacts/smoke/<run_id>.json`
  - validates each stage artifact immediately after generation.
- Run:
```powershell
python -m pmx.jobs.smoke_pipeline_artifact_only `
  --forecast-artifact tests/fixtures/forecast/forecast_artifact_sample.json `
  --artifacts-root artifacts `
  --nonce smoke
```
- Strict mode (`exit != 0` only on `overall_status=FAIL`):
```powershell
python -m pmx.jobs.smoke_pipeline_artifact_only `
  --forecast-artifact tests/fixtures/forecast/forecast_artifact_sample.json `
  --artifacts-root artifacts `
  --nonce smoke `
  --strict
```
- Summary includes:
  - input path/hash
  - per-step artifact paths + payload/policy hashes
  - `overall_status` (`OK` / `WARN` / `FAIL`)
  - deterministic counts of flags/warnings and step outcomes
  - reproducibility hashes:
    - `smoke_policy_hash`
    - `smoke_outputs_hash`
    - `smoke_payload_hash`
- See `docs/runbook.md` for operational instructions and replay workflow.

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
