# Contributing

## Verification contract
- Run checks locally when tools are available:
  - `ruff format --check .`
  - `ruff check .`
  - `mypy src`
  - `pytest -q`
- If tools are unavailable locally, mark checks as `Not run` or `Skipped`.
- Never claim local success without command output.
- CI in `.github/workflows/ci.yml` is the source of truth.
- Merge requires CI green (lint, typecheck, tests).

## Install
- Default: `python -m pip install -e ".[dev]"`
- Alternative: `uv pip install --system -e ".[dev]"`

## Unified verify command
- `python scripts/verify.py`
- In CI mode (`CI=true`), missing tools are treated as failure.
