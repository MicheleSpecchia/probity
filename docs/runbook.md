# Runbook (Artifact-Only Offline)

## Purpose
- Run the full artifact-only pipeline end-to-end with one command.
- Validate every generated artifact schema.
- Replay deterministically with the same input + nonce.

## One-command smoke run
```powershell
python -m pmx.jobs.smoke_pipeline_artifact_only `
  --forecast-artifact tests/fixtures/forecast/forecast_artifact_sample.json `
  --artifacts-root artifacts `
  --nonce smoke
```

- Output summary:
  - `artifacts/smoke/<run_id>.json`
- Per-step artifacts are written under:
  - `artifacts/decisions/`
  - `artifacts/trade_plans/`
  - `artifacts/executions/`
  - `artifacts/portfolios/`
  - `artifacts/pipeline_runs/`
  - `artifacts/performance/`
  - `artifacts/risks/`
  - `artifacts/audit_bundles/`
  - `artifacts/monitoring/`

## Strict mode
```powershell
python -m pmx.jobs.smoke_pipeline_artifact_only `
  --forecast-artifact tests/fixtures/forecast/forecast_artifact_sample.json `
  --artifacts-root artifacts `
  --nonce smoke `
  --strict
```

- `--strict` exits non-zero only when `overall_status == "FAIL"`.
- Without `--strict`, the job writes summary even on failures.

## Deterministic replay
- Use the same:
  - forecast artifact file content
  - CLI params
  - `--nonce`
- Expect stable payload hashes in summary:
  - decision/trade_plan/execution/portfolio/performance/risk/audit_bundle/monitoring
  - `smoke_outputs_hash`
  - `smoke_payload_hash`

## Summary interpretation
- `overall_status`:
  - `OK`: no failures and no warning-level quality signals.
  - `WARN`: no step failure, but quality flags/warnings or monitor WARN.
  - `FAIL`: at least one failed step or monitor FAIL.
- `outputs.<step>.status`:
  - `OK` / `FAIL` / `SKIPPED`
- `outputs.<step>.errors`:
  - deterministic shape: `{step, code, path, reason}`.

## Change forecast fixture/input
- Pass a different forecast artifact:
```powershell
python -m pmx.jobs.smoke_pipeline_artifact_only `
  --forecast-artifact artifacts/forecasts/<forecast_run_id>.json `
  --artifacts-root artifacts `
  --nonce smoke
```

## Windows note
- This runbook is offline and does not require DB or Docker.
- If local DB/Docker is unavailable, artifact-only smoke is still fully supported.
