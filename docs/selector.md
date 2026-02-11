# Selector v1 (Milestone 9.1)

## Objective
- Build deterministic deep-dive selection (`k=200`) from at most `1500` candidates.
- Prioritize forecasting accuracy with diversification constraints and bias-control baselines.

## Score components (accuracy-first)
- `LQ` liquidity quality:
  - spread, top depth, staleness (`micro_v1`).
- `PQ` price entropy:
  - binary entropy from as-of `price_prob`.
- `VO` volatility/opportunity:
  - `realized_vol_1h` and `abs(return_5m)`.
- `DC` data completeness:
  - feature presence + price presence.
- `RC` rule clarity proxy:
  - `rule_parse_ok`, text completeness, ambiguity heuristic.

Formula:
```text
screen_score = 0.35*LQ + 0.25*PQ + 0.20*VO + 0.10*DC + 0.10*RC - penalties
```

Hard flags:
- `illiquid` or `stale_data` => score forced to `0`.

## TTR buckets
- `0_24h`
- `1_7d`
- `7_30d`
- `30d_plus`
- `unknown`

Deterministic estimation pipeline:
1. direct timestamps (in order): `resolution_ts`, `close_ts`, `end_ts`, `resolved_ts`
2. nested payload keys (for example `metadata.*`, `raw.*`, `rule_parse_json.*`)
3. regex date extraction from `question`, `title`, `slug`, `description`
4. fallback `unknown` (or `0_24h` if status is already resolved/closed/ended)

`estimate_resolution_ts(...)` is deterministic and timezone-normalized to UTC.

## Deep-dive score (`deep_score`)
After candidate constraints, deep-dive ranking uses `deep_score`:

```text
deep_score =
  0.40*PQ + 0.25*VO + 0.15*DC + 0.10*TTR + 0.10*screen_score - penalties
```

Where:
- `PQ`: entropy from as-of `price_prob`
- `VO`: volatility/opportunity term
- `DC`: data completeness
- `TTR`: bucket opportunity prior (`0_24h` highest, `unknown` lowest)

Penalties:
- `illiquid` / `stale_data` -> `no_trade_candidate` hard penalty
- ambiguity and missing-data penalties inherited from screen scoring

Selection ordering is deterministic:
1. `deep_score DESC`
2. `screen_score DESC`
3. `volume DESC`
4. `market_id ASC`

## Constraints
- Deterministic greedy ranking:
  - `screen_score DESC`
  - `LQ DESC`
  - `volume DESC`
  - `market_id ASC`
- Then enforce:
  - target bucket mix
  - `max_per_category`
  - `max_per_group`
- Fill remaining slots deterministically from global ranking.

## Baseline selectors (bias control)
- `baseline_top_volume`
- `baseline_random_stratified`
  - stratifies by `(ttr_bucket, category)`
  - deterministic seed from `sha256(decision_ts + config_hash)`

## Persistence and audit
- `selection_runs` + `selection_items` are persisted for:
  - `selector_v1`
  - `baseline_top_volume`
  - `baseline_random_stratified`
- Candidate score table is optional:
  - if `selector_candidates` does not exist, candidate scores are persisted in artifacts fallback:
    - `artifacts/selector/selector/<run_id>.json`

## CLI
```powershell
python -m pmx.jobs.select_markets `
  --decision-ts 2026-02-11T12:00:00Z `
  --epsilon-seconds 300 `
  --max-candidates 1500 `
  --k-deep 200
```

Selector-vs-baseline evaluation:
```powershell
python -m pmx.jobs.eval_selector `
  --decision-ts 2026-02-11T12:00:00Z `
  --epsilon-seconds 300 `
  --window-hours 72
```
