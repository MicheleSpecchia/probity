from __future__ import annotations

from pmx.news.normalize import canonicalize_url, extract_domain, normalize_text


def test_canonicalize_url_strips_tracking_and_sorts_query() -> None:
    canonical = canonicalize_url(
        "HTTPS://www.Reuters.com/path?q=1&utm_source=x&b=2&fbclid=abc&a=3#fragment"
    )

    assert canonical == "https://reuters.com/path?a=3&b=2&q=1"


def test_extract_domain_drops_www_and_lowercases() -> None:
    domain = extract_domain("https://WWW.APNEWS.com/article/id-1")
    assert domain == "apnews.com"


def test_normalize_text_is_deterministic() -> None:
    assert normalize_text("  Hello   WORLD  ") == "hello world"
