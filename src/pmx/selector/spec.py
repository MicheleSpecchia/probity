from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SelectorConfig:
    lq_threshold: float = 0.35
    max_spread_bps_hard: float = 1500.0
    min_top_depth_hard: float = 1.0
    stale_seconds_hard: float = 14_400.0


@dataclass(frozen=True, slots=True)
class ScoreResult:
    screen_score: float
    components: dict[str, float]
    flags: tuple[str, ...]
    penalties: dict[str, float]
    reason_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "screen_score": self.screen_score,
            "components": self.components,
            "flags": list(self.flags),
            "penalties": self.penalties,
            "reason_hash": self.reason_hash,
        }


@dataclass(frozen=True, slots=True)
class DeepScoreResult:
    deep_score: float
    components: dict[str, float]
    flags: tuple[str, ...]
    penalties: dict[str, float]
    reason_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "deep_score": self.deep_score,
            "components": self.components,
            "flags": list(self.flags),
            "penalties": self.penalties,
            "reason_hash": self.reason_hash,
        }


_TTR_COMPONENT_BY_BUCKET: dict[str, float] = {
    "0_24h": 1.00,
    "1_7d": 0.85,
    "7_30d": 0.55,
    "30d_plus": 0.30,
    "unknown": 0.25,
}


def compute_screen_score(
    *,
    features: dict[str, Any] | None,
    price_prob: float | None,
    market_payload: dict[str, Any],
    config: SelectorConfig | None = None,
) -> ScoreResult:
    cfg = config or SelectorConfig()
    feature_map = dict(features or {})

    lq, hard_flags = _liquidity_quality(feature_map, cfg)
    pq = _price_entropy_component(price_prob)
    vo = _volatility_opportunity_component(feature_map)
    dc = _data_completeness_component(feature_map, price_prob)
    rc, rc_penalty = _rule_clarity_component(market_payload)

    penalties: dict[str, float] = {}
    if rc_penalty > 0:
        penalties["ambiguous_rule"] = rc_penalty
    if not feature_map:
        penalties["missing_features"] = 0.20
    if price_prob is None:
        penalties["missing_price_prob"] = 0.20
    if "illiquid" in hard_flags:
        penalties["illiquid"] = 1.0
    if "stale_data" in hard_flags:
        penalties["stale_data"] = 1.0

    base_score = 0.35 * lq + 0.25 * pq + 0.20 * vo + 0.10 * dc + 0.10 * rc
    penalty_sum = sum(penalties.values())
    raw_score = base_score - penalty_sum
    if hard_flags:
        raw_score = 0.0
    screen_score = round(_clamp(raw_score, 0.0, 1.0), 6)

    flags = tuple(sorted(set(hard_flags + tuple(sorted(penalties.keys())))))
    components = {
        "lq": round(lq, 6),
        "pq": round(pq, 6),
        "vo": round(vo, 6),
        "dc": round(dc, 6),
        "rc": round(rc, 6),
        "penalty_sum": round(penalty_sum, 6),
    }
    reason_hash = _build_reason_hash(components=components, flags=flags, penalties=penalties)

    return ScoreResult(
        screen_score=screen_score,
        components=components,
        flags=flags,
        penalties={key: round(value, 6) for key, value in sorted(penalties.items())},
        reason_hash=reason_hash,
    )


def liquidity_quality_from_features(features: dict[str, Any] | None) -> float:
    lq, _ = _liquidity_quality(dict(features or {}), SelectorConfig())
    return round(lq, 6)


def compute_deep_score(
    *,
    score_result: ScoreResult,
    ttr_bucket: str,
    price_prob: float | None,
) -> DeepScoreResult:
    del price_prob
    pq = score_result.components.get("pq", 0.0)
    vo = score_result.components.get("vo", 0.0)
    dc = score_result.components.get("dc", 0.0)
    screen = score_result.screen_score
    ttr_component = _ttr_component(ttr_bucket)

    base_score = 0.40 * pq + 0.25 * vo + 0.15 * dc + 0.10 * ttr_component + 0.10 * screen

    penalties: dict[str, float] = {}
    if "illiquid" in score_result.flags or "stale_data" in score_result.flags:
        penalties["no_trade_candidate"] = 1.0
    ambiguous_rule = score_result.penalties.get("ambiguous_rule", 0.0)
    if ambiguous_rule > 0:
        penalties["ambiguous_rule"] = round(_clamp(ambiguous_rule * 0.75, 0.0, 0.30), 6)
    if "missing_features" in score_result.penalties:
        penalties["missing_features"] = 0.15
    if "missing_price_prob" in score_result.penalties:
        penalties["missing_price_prob"] = 0.15

    penalty_sum = sum(penalties.values())
    deep_score = round(_clamp(base_score - penalty_sum, 0.0, 1.0), 6)

    flags: set[str] = set(score_result.flags)
    if "no_trade_candidate" in penalties:
        flags.add("no_trade_candidate")
    components = {
        "screen_score": round(screen, 6),
        "pq": round(pq, 6),
        "vo": round(vo, 6),
        "dc": round(dc, 6),
        "ttr": round(ttr_component, 6),
        "penalty_sum": round(penalty_sum, 6),
    }
    ordered_flags = tuple(sorted(flags))
    ordered_penalties = {key: penalties[key] for key in sorted(penalties.keys())}
    reason_hash = _build_reason_hash(
        components=components,
        flags=ordered_flags,
        penalties=ordered_penalties,
    )
    return DeepScoreResult(
        deep_score=deep_score,
        components=components,
        flags=ordered_flags,
        penalties=ordered_penalties,
        reason_hash=reason_hash,
    )


def _liquidity_quality(
    features: dict[str, Any],
    config: SelectorConfig,
) -> tuple[float, tuple[str, ...]]:
    spread_bps = _as_float(features.get("spread_bps"), default=10_000.0)
    depth_bid = max(_as_float(features.get("top_depth_bid"), default=0.0), 0.0)
    depth_ask = max(_as_float(features.get("top_depth_ask"), default=0.0), 0.0)
    stale_trade = max(_as_float(features.get("stale_seconds_last_trade"), default=1e9), 0.0)
    stale_book = max(_as_float(features.get("stale_seconds_last_book"), default=1e9), 0.0)

    spread_term = 1.0 - _clamp(spread_bps / 1000.0, 0.0, 1.0)
    depth_total = depth_bid + depth_ask
    depth_term = _clamp(math.log1p(depth_total) / math.log1p(500.0), 0.0, 1.0)
    stale_term_trade = 1.0 - _clamp(stale_trade / 7200.0, 0.0, 1.0)
    stale_term_book = 1.0 - _clamp(stale_book / 7200.0, 0.0, 1.0)

    lq = 0.35 * spread_term + 0.35 * depth_term + 0.15 * stale_term_trade + 0.15 * stale_term_book

    hard_flags: list[str] = []
    if spread_bps >= config.max_spread_bps_hard or depth_total < config.min_top_depth_hard:
        hard_flags.append("illiquid")
    if max(stale_trade, stale_book) >= config.stale_seconds_hard:
        hard_flags.append("stale_data")
    return _clamp(lq, 0.0, 1.0), tuple(hard_flags)


def _price_entropy_component(price_prob: float | None) -> float:
    if price_prob is None:
        return 0.0
    p = _clamp(float(price_prob), 1e-6, 1.0 - 1e-6)
    entropy = -(p * math.log(p) + (1.0 - p) * math.log(1.0 - p)) / math.log(2.0)
    return _clamp(entropy, 0.0, 1.0)


def _volatility_opportunity_component(features: dict[str, Any]) -> float:
    abs_return = abs(_as_float(features.get("return_5m"), default=0.0))
    realized_vol = _as_float(features.get("realized_vol_1h"), default=0.0)
    ret_term = _clamp(abs_return / 0.20, 0.0, 1.0)
    vol_term = _clamp(realized_vol / 0.20, 0.0, 1.0)
    return _clamp(0.6 * ret_term + 0.4 * vol_term, 0.0, 1.0)


def _data_completeness_component(features: dict[str, Any], price_prob: float | None) -> float:
    required_fields = (
        "spread_bps",
        "top_depth_bid",
        "top_depth_ask",
        "book_imbalance_1",
        "return_5m",
        "realized_vol_1h",
        "stale_seconds_last_trade",
        "stale_seconds_last_book",
    )
    present = 0
    total = len(required_fields) + 1
    for field in required_fields:
        value = features.get(field)
        if value is not None:
            present += 1
    if price_prob is not None:
        present += 1
    return _clamp(present / total, 0.0, 1.0)


def _rule_clarity_component(market_payload: dict[str, Any]) -> tuple[float, float]:
    rule_parse_ok = bool(market_payload.get("rule_parse_ok"))
    text_parts = [
        _as_text(market_payload.get("rule_text")),
        _as_text(market_payload.get("description")),
        _as_text(market_payload.get("title")),
    ]
    merged = " ".join(part for part in text_parts if part)
    length_term = _clamp(len(merged) / 400.0, 0.0, 1.0)
    base = 0.6 * (1.0 if rule_parse_ok else 0.0) + 0.4 * length_term

    lowered = merged.lower()
    ambiguity_hits = sum(
        1
        for token in ("maybe", "depends", "subject to", "discretion", "ambiguous", "unclear")
        if token in lowered
    )
    ambiguity_penalty = min(0.30, 0.05 * ambiguity_hits)
    rc = _clamp(base - ambiguity_penalty, 0.0, 1.0)
    return rc, ambiguity_penalty


def _build_reason_hash(
    *,
    components: dict[str, float],
    flags: tuple[str, ...],
    penalties: dict[str, float],
) -> str:
    payload = {
        "components": components,
        "flags": list(flags),
        "penalties": penalties,
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _as_float(raw: Any, *, default: float) -> float:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def _ttr_component(bucket: str) -> float:
    normalized = (bucket or "").strip()
    return _TTR_COMPONENT_BY_BUCKET.get(normalized, _TTR_COMPONENT_BY_BUCKET["unknown"])
