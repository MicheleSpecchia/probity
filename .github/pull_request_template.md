# Title

## Intended in this PR
- 

## Verifica

### Verified in this environment
- [ ] Locale: toolchain disponibile e comandi eseguiti
- [ ] Locale: toolchain NON disponibile (spiegare cosa manca)
- Output (se rilevante):

### Verified by CI
- [ ] CI is the source of truth (merge solo con CI green)

## Checklist
- [ ] As-of / anti-leak: rispettato (nessun uso di dati futuri)
- [ ] Determinismo: hashing/config/versioning ok
- [ ] Idempotenza: job/scripts safe to retry
- [ ] Logging/audit: run_id + config_hash + code_version presenti
- [ ] Test aggiunti/aggiornati
- [ ] Docs aggiornate (README/runbook) se cambia runtime/config

## Come verificare
1. Install:
   - `python -m pip install -e ".[dev]"`
   - (alt) `uv pip install --system -e ".[dev]"`
2. `python scripts/verify.py`
3. (opzionale) comandi diretti: ruff/mypy/pytest
4. (opzionale) DB:
   - `docker compose up -d`
   - `docker compose ps`
