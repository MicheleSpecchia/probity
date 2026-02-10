install:
    uv pip install --system -e ".[dev]"

install-pip:
    python -m pip install -e ".[dev]"

lint:
    ruff format --check .
    ruff check .

typecheck:
    mypy src

test:
    pytest -q

verify:
    python scripts/verify.py

check: lint typecheck test
