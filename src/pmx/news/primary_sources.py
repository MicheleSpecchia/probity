from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True, slots=True)
class PrimarySourceDefaults:
    is_primary: bool
    trust_score: int
    rps: float
    allow_subdomains: bool


@dataclass(frozen=True, slots=True)
class PrimarySourcePolicy:
    domain: str
    name: str
    is_primary: bool
    trust_score: int
    rps: float
    allow_subdomains: bool


@dataclass(frozen=True, slots=True)
class PrimarySourceConfig:
    version: int
    defaults: PrimarySourceDefaults
    domains: tuple[PrimarySourcePolicy, ...]

    def by_domain(self) -> dict[str, PrimarySourcePolicy]:
        return {policy.domain: policy for policy in self.domains}


def load_primary_sources_config(path: str | Path) -> PrimarySourceConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    if not isinstance(payload, Mapping):
        raise ValueError(f"Invalid primary source config format in {config_path}")

    version = int(payload.get("version", 1))
    defaults_payload = _as_mapping(payload.get("defaults"))
    defaults = PrimarySourceDefaults(
        is_primary=bool(defaults_payload.get("is_primary", True)),
        trust_score=_coerce_trust_score(defaults_payload.get("trust_score", 70)),
        rps=_coerce_positive_float(defaults_payload.get("rps", 0.5), fallback=0.5),
        allow_subdomains=bool(defaults_payload.get("allow_subdomains", False)),
    )

    domains_payload = payload.get("domains")
    if not isinstance(domains_payload, list):
        raise ValueError(f"Invalid domains list in {config_path}")

    policies: list[PrimarySourcePolicy] = []
    for item in domains_payload:
        item_map = _as_mapping(item)
        domain = _normalize_domain(item_map.get("domain"))
        if domain is None:
            continue

        policy = PrimarySourcePolicy(
            domain=domain,
            name=_as_text(item_map.get("name")) or domain,
            is_primary=bool(item_map.get("is_primary", defaults.is_primary)),
            trust_score=_coerce_trust_score(item_map.get("trust_score", defaults.trust_score)),
            rps=_coerce_positive_float(item_map.get("rps", defaults.rps), fallback=defaults.rps),
            allow_subdomains=bool(item_map.get("allow_subdomains", defaults.allow_subdomains)),
        )
        policies.append(policy)

    policies.sort(key=lambda item: item.domain)
    return PrimarySourceConfig(
        version=version,
        defaults=defaults,
        domains=tuple(policies),
    )


def match_primary_source_policy(
    domain: str,
    policies: tuple[PrimarySourcePolicy, ...],
) -> PrimarySourcePolicy | None:
    normalized_domain = _normalize_domain(domain)
    if normalized_domain is None:
        return None

    exact = [policy for policy in policies if policy.domain == normalized_domain]
    if exact:
        return exact[0]

    matching_subdomains = [
        policy
        for policy in policies
        if policy.allow_subdomains and normalized_domain.endswith(f".{policy.domain}")
    ]
    if not matching_subdomains:
        return None

    matching_subdomains.sort(key=lambda policy: (-len(policy.domain), policy.domain))
    return matching_subdomains[0]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_domain(value: Any) -> str | None:
    text = _as_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered.startswith("www."):
        lowered = lowered[4:]
    return lowered


def _coerce_positive_float(value: Any, *, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if parsed <= 0:
        return fallback
    return parsed


def _coerce_trust_score(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 50
    if parsed < 0:
        return 0
    if parsed > 100:
        return 100
    return parsed
