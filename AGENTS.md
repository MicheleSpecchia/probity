# AGENTS

## Default Execution Rules
- Always read repository context before editing files.
- Keep one PR focused on one small, verifiable change.
- Never introduce data leakage: enforce as-of time in every ingest, feature, and backtest job.
- Always log `run_id`, `code_version`, and `config_hash` for every forecasting run.

## Guardrail Expectations
- Respect deterministic execution paths in tests and jobs.
- Preserve idempotency boundaries on job re-runs.
- Block outputs with explicit no-trade flags when guardrails fail:
  leak, echo, illiquid, ambiguous rules, overconfidence.

## Verification Contract
- If tools are unavailable locally, mark checks as Not run or Skipped.
- Do not claim checks passed unless output is observed in the current environment.
- CI is required before merge: lint, typecheck, and tests must be green.
