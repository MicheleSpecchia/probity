from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from pmx.claims.graph import SourceRecord


@dataclass(frozen=True, slots=True)
class EchoMetrics:
    unique_domains: int
    primary_domains: int
    diversity_score: float
    echo_penalty: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "unique_domains": self.unique_domains,
            "primary_domains": self.primary_domains,
            "diversity_score": self.diversity_score,
            "echo_penalty": self.echo_penalty,
        }


def source_diversity_score(sources: Sequence[SourceRecord]) -> float:
    total_sources = len(sources)
    if total_sources == 0:
        return 0.0
    unique_domains = len(_domain_counts(sources))
    return _clamp_01(unique_domains / total_sources)


def echo_penalty(sources: Sequence[SourceRecord]) -> float:
    total_sources = len(sources)
    if total_sources <= 1:
        return 0.0

    counts = _domain_counts(sources)
    unique_domains = len(counts)
    if unique_domains <= 1:
        return 1.0

    max_share = max(counts.values()) / total_sources
    uniform_share = 1.0 / unique_domains
    normalized = (max_share - uniform_share) / (1.0 - uniform_share)
    return _clamp_01(normalized)


def compute_echo_metrics(sources: Sequence[SourceRecord]) -> EchoMetrics:
    counts = _domain_counts(sources)
    unique_domains = len(counts)
    primary_domains = len(
        {source.domain for source in sources if source.domain and source.source_type == "primary"}
    )
    return EchoMetrics(
        unique_domains=unique_domains,
        primary_domains=primary_domains,
        diversity_score=source_diversity_score(sources),
        echo_penalty=echo_penalty(sources),
    )


def _domain_counts(sources: Sequence[SourceRecord]) -> Counter[str]:
    return Counter(source.domain for source in sources if source.domain)


def _clamp_01(value: float) -> float:
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return value
