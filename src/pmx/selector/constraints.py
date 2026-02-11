from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from pmx.selector.compute import CandidateScore
from pmx.selector.ttr import ALL_BUCKETS, BUCKET_UNKNOWN

DEFAULT_TARGET_BUCKET_MIX: dict[str, float] = {
    "0_24h": 0.25,
    "1_7d": 0.35,
    "7_30d": 0.25,
    "30d_plus": 0.10,
    "unknown": 0.05,
}


@dataclass(frozen=True, slots=True)
class ConstraintResult:
    selected: tuple[CandidateScore, ...]
    quota_by_bucket: dict[str, int]
    selected_by_bucket: dict[str, int]
    selected_by_category: dict[str, int]
    selected_by_group: dict[str, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "selected_count": len(self.selected),
            "quota_by_bucket": self.quota_by_bucket,
            "selected_by_bucket": self.selected_by_bucket,
            "selected_by_category": self.selected_by_category,
            "selected_by_group": self.selected_by_group,
        }


def enforce_constraints(
    scored: list[CandidateScore],
    *,
    k: int = 200,
    target_bucket_mix: dict[str, float] | None = None,
    max_per_category: int = 45,
    max_per_group: int = 20,
) -> ConstraintResult:
    if k <= 0:
        raise ValueError("k must be > 0")
    if max_per_category <= 0 or max_per_group <= 0:
        raise ValueError("max_per_category and max_per_group must be > 0")

    ranked = sorted(
        scored,
        key=lambda item: (
            -item.screen_score,
            -item.lq,
            -item.volume_24h,
            item.market_id,
        ),
    )
    if not ranked:
        return ConstraintResult(
            selected=(),
            quota_by_bucket={bucket: 0 for bucket in ALL_BUCKETS},
            selected_by_bucket={bucket: 0 for bucket in ALL_BUCKETS},
            selected_by_category={},
            selected_by_group={},
        )

    mix = dict(DEFAULT_TARGET_BUCKET_MIX if target_bucket_mix is None else target_bucket_mix)
    normalized_mix = _normalized_mix(mix)
    quotas = _compute_bucket_quotas(k=k, mix=normalized_mix)

    selected: list[CandidateScore] = []
    used_market_ids: set[str] = set()
    by_category: Counter[str] = Counter()
    by_group: Counter[str] = Counter()
    by_bucket: Counter[str] = Counter()

    for bucket in ALL_BUCKETS:
        target = quotas.get(bucket, 0)
        if target <= 0:
            continue
        for item in ranked:
            if len(selected) >= k or by_bucket[bucket] >= target:
                break
            if item.market_id in used_market_ids:
                continue
            if _bucket(item.ttr_bucket) != bucket:
                continue
            if not _can_select(item, by_category, by_group, max_per_category, max_per_group):
                continue
            _select_item(item, selected, used_market_ids, by_category, by_group, by_bucket)

    for item in ranked:
        if len(selected) >= k:
            break
        if item.market_id in used_market_ids:
            continue
        if not _can_select(item, by_category, by_group, max_per_category, max_per_group):
            continue
        _select_item(item, selected, used_market_ids, by_category, by_group, by_bucket)

    selected.sort(
        key=lambda item: (
            -item.screen_score,
            -item.lq,
            -item.volume_24h,
            item.market_id,
        )
    )
    return ConstraintResult(
        selected=tuple(selected),
        quota_by_bucket=quotas,
        selected_by_bucket={bucket: by_bucket.get(bucket, 0) for bucket in ALL_BUCKETS},
        selected_by_category=dict(sorted(by_category.items())),
        selected_by_group=dict(sorted(by_group.items())),
    )


def _normalized_mix(mix: dict[str, float]) -> dict[str, float]:
    out = {bucket: max(float(mix.get(bucket, 0.0)), 0.0) for bucket in ALL_BUCKETS}
    total = sum(out.values())
    if total <= 0:
        uniform = 1.0 / len(ALL_BUCKETS)
        return {bucket: uniform for bucket in ALL_BUCKETS}
    return {bucket: value / total for bucket, value in out.items()}


def _compute_bucket_quotas(*, k: int, mix: dict[str, float]) -> dict[str, int]:
    base: dict[str, int] = {}
    remainders: list[tuple[float, str]] = []
    assigned = 0
    for bucket in ALL_BUCKETS:
        raw = mix[bucket] * k
        floor_value = int(raw)
        base[bucket] = floor_value
        assigned += floor_value
        remainders.append((raw - floor_value, bucket))

    remaining = k - assigned
    remainders.sort(key=lambda item: (-item[0], item[1]))
    for _, bucket in remainders[:remaining]:
        base[bucket] += 1
    return base


def _bucket(raw: str) -> str:
    value = raw.strip() if raw else ""
    return value if value in ALL_BUCKETS else BUCKET_UNKNOWN


def _can_select(
    item: CandidateScore,
    by_category: Counter[str],
    by_group: Counter[str],
    max_per_category: int,
    max_per_group: int,
) -> bool:
    category = item.category or "unknown"
    group = item.group_id or "unknown"
    if by_category[category] >= max_per_category:
        return False
    if by_group[group] >= max_per_group:
        return False
    return True


def _select_item(
    item: CandidateScore,
    selected: list[CandidateScore],
    used_market_ids: set[str],
    by_category: Counter[str],
    by_group: Counter[str],
    by_bucket: Counter[str],
) -> None:
    selected.append(item)
    used_market_ids.add(item.market_id)
    by_category[item.category or "unknown"] += 1
    by_group[item.group_id or "unknown"] += 1
    by_bucket[_bucket(item.ttr_bucket)] += 1
