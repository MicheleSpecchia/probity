from __future__ import annotations

from pmx.news.linking import build_market_lexicon, link_article_markets


def test_market_linking_is_deterministic_with_stable_tie_break() -> None:
    markets = [
        {"market_id": "m-2", "title": "US Senate election", "slug": "us-senate-election"},
        {"market_id": "m-1", "title": "US Senate vote", "slug": "us-senate-vote"},
    ]
    lexicon = build_market_lexicon(markets)

    linked = link_article_markets(
        title="US senate vote update",
        body="The senate election result may change.",
        lexicon=lexicon,
        top_k=5,
    )

    assert [item.market_id for item in linked] == ["m-1", "m-2"]
    assert linked[0].score >= linked[1].score
