from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "this",
        "to",
        "was",
        "were",
        "with",
        "will",
    }
)


@dataclass(frozen=True, slots=True)
class MarketLexiconEntry:
    market_id: str
    title_tokens: tuple[str, ...]
    slug_tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LinkedMarketScore:
    market_id: str
    score: float
    title_hits: int
    body_hits: int
    slug_hits: int


def build_market_lexicon(rows: Iterable[Mapping[str, str | None]]) -> list[MarketLexiconEntry]:
    output: list[MarketLexiconEntry] = []
    for row in rows:
        market_id = _as_text(row.get("market_id"))
        if not market_id:
            continue
        title_tokens = tuple(sorted(_tokenize(_as_text(row.get("title")))))
        slug_tokens = tuple(sorted(_tokenize(_as_text(row.get("slug")))))
        output.append(
            MarketLexiconEntry(
                market_id=market_id,
                title_tokens=title_tokens,
                slug_tokens=slug_tokens,
            )
        )

    output.sort(key=lambda entry: entry.market_id)
    return output


def link_article_markets(
    *,
    title: str | None,
    body: str | None,
    lexicon: Sequence[MarketLexiconEntry],
    top_k: int = 5,
) -> list[LinkedMarketScore]:
    if top_k <= 0:
        return []

    title_tokens = _tokenize(title)
    body_tokens = _tokenize(body)
    combined_tokens = title_tokens | body_tokens

    scores: list[LinkedMarketScore] = []
    for entry in lexicon:
        title_set = set(entry.title_tokens)
        slug_set = set(entry.slug_tokens)

        title_hits = len(title_tokens & title_set)
        body_hits = len(body_tokens & title_set)
        slug_hits = len(combined_tokens & slug_set)
        score = (2.0 * title_hits) + float(body_hits) + (0.5 * slug_hits)
        if score <= 0:
            continue

        scores.append(
            LinkedMarketScore(
                market_id=entry.market_id,
                score=score,
                title_hits=title_hits,
                body_hits=body_hits,
                slug_hits=slug_hits,
            )
        )

    scores.sort(key=lambda item: (-item.score, item.market_id))
    return scores[:top_k]


def _tokenize(raw_text: str | None) -> set[str]:
    if raw_text is None:
        return set()
    return {
        token
        for token in _TOKEN_RE.findall(raw_text.lower())
        if token and token not in _STOPWORDS
    }


def _as_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
