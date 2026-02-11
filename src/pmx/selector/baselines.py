from __future__ import annotations

import hashlib
import random
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime

from pmx.selector.compute import CandidateScore


def select_top_volume(
    candidates: Sequence[CandidateScore], *, k: int = 200
) -> list[CandidateScore]:
    if k <= 0:
        return []
    ranked = sorted(
        candidates,
        key=lambda item: (
            -item.volume_24h,
            -item.screen_score,
            item.market_id,
        ),
    )
    return list(ranked[:k])


def deterministic_seed(decision_ts: datetime, config_hash: str) -> int:
    payload = f"{decision_ts.isoformat()}|{config_hash}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def select_random_stratified(
    candidates: Sequence[CandidateScore],
    *,
    k: int = 200,
    seed: int,
) -> list[CandidateScore]:
    if k <= 0:
        return []
    groups: dict[tuple[str, str], list[CandidateScore]] = defaultdict(list)
    for item in sorted(candidates, key=lambda row: (row.ttr_bucket, row.category, row.market_id)):
        groups[(item.ttr_bucket, item.category)].append(item)

    quotas = _compute_group_quotas(groups, k)
    rng = random.Random(seed)

    selected: list[CandidateScore] = []
    used: set[str] = set()
    for group_key in sorted(groups.keys()):
        items = list(groups[group_key])
        rng.shuffle(items)
        quota = quotas.get(group_key, 0)
        for item in items[:quota]:
            if item.market_id in used:
                continue
            selected.append(item)
            used.add(item.market_id)

    if len(selected) < k:
        pool = [item for item in candidates if item.market_id not in used]
        pool = sorted(pool, key=lambda row: row.market_id)
        rng.shuffle(pool)
        missing = k - len(selected)
        selected.extend(pool[:missing])

    selected.sort(key=lambda item: item.market_id)
    return selected[:k]


def _compute_group_quotas(
    groups: dict[tuple[str, str], list[CandidateScore]],
    k: int,
) -> dict[tuple[str, str], int]:
    total = sum(len(values) for values in groups.values())
    if total <= 0:
        return {key: 0 for key in groups}

    quotas: dict[tuple[str, str], int] = {}
    remainders: list[tuple[float, tuple[str, str]]] = []
    assigned = 0
    for key in sorted(groups.keys()):
        size = len(groups[key])
        raw = (size / total) * k
        floor_value = int(raw)
        quotas[key] = floor_value
        assigned += floor_value
        remainders.append((raw - floor_value, key))

    remaining = k - assigned
    remainders.sort(key=lambda item: (-item[0], item[1]))
    for _, key in remainders[:remaining]:
        quotas[key] += 1
    return quotas
