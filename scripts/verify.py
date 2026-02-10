from __future__ import annotations

import os
import shutil
import subprocess
import sys

CHECKS = [
    ("ruff format", ["ruff", "format", "--check", "."]),
    ("ruff lint", ["ruff", "check", "."]),
    ("mypy", ["mypy", "src"]),
    ("pytest", ["pytest", "-q"]),
]


def is_ci() -> bool:
    value = os.getenv("CI", "")
    return value.strip().lower() in {"1", "true", "yes"}


def main() -> int:
    ci_mode = is_ci()
    missing_tools: list[str] = []
    failed_checks: list[str] = []

    for check_name, command in CHECKS:
        tool = command[0]
        if shutil.which(tool) is None:
            print(f"SKIPPED: {check_name} (tool not found: {tool})")
            missing_tools.append(tool)
            continue

        print(f"RUN: {' '.join(command)}")
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            failed_checks.append(check_name)

    if failed_checks:
        print(f"FAILED checks: {', '.join(failed_checks)}")
        return 1

    if ci_mode and missing_tools:
        unique_tools = sorted(set(missing_tools))
        print(f"FAILED in CI due to missing tools: {', '.join(unique_tools)}")
        return 1

    if missing_tools:
        unique_tools = sorted(set(missing_tools))
        print(f"Completed with skipped checks (missing tools): {', '.join(unique_tools)}")
    else:
        print("All checks passed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
