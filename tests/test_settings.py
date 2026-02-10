from __future__ import annotations

import pytest

from pmx.config.settings import SettingsError, load_settings


BASE_ENV = {
    "APP_DATABASE_URL": "postgresql+psycopg://pmx:pmx_dev_password@localhost:5432/pmx",
    "POSTGRES_USER": "pmx",
    "POSTGRES_PASSWORD": "pmx_dev_password",
    "POSTGRES_DB": "pmx",
}


def test_load_settings_reads_required_and_default_values() -> None:
    settings = load_settings(BASE_ENV)

    assert settings.app_database_url == BASE_ENV["APP_DATABASE_URL"]
    assert settings.postgres_user == "pmx"
    assert settings.decision_cadence_hours == 4
    assert settings.screening_budget_per_cycle == 1500
    assert settings.deep_dive_budget_per_cycle == 200


def test_load_settings_fails_when_required_values_are_missing() -> None:
    broken_env = {key: value for key, value in BASE_ENV.items() if key != "APP_DATABASE_URL"}

    with pytest.raises(SettingsError, match="APP_DATABASE_URL"):
        load_settings(broken_env)
