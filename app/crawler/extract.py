from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Optional

from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from .normalize import normalize_text


CAPITAL_PATTERNS = [
    re.compile(r"(?:資本金)[:：]?\s*([0-9,\.]+(?:万円|円)?)"),
    re.compile(r"(?:capital)[:：]?\s*([0-9,\.]+(?:\s*(?:yen|jpy))?)", re.IGNORECASE),
]

INDUSTRY_PATTERNS = [
    re.compile(r"(?:業種)[:：]?\s*([\w一-龠ぁ-んァ-ンー・、\s]+)"),
    re.compile(r"(?:business)[:：]?\s*(.+)", re.IGNORECASE),
]

POSTAL_RE = re.compile(r"〒?\s?\d{3}-?\d{4}")
PHONE_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
CORP_KEYWORDS = [
    "会社概要", "会社案内", "企業情報", "採用情報", "お問い合わせ", "プライバシー", "個人情報", "特定商取引法", "サイトマップ",
]
NEWS_HOSTS = {
    "toonippo.co.jp",
    "yahoo.co.jp",
    "asahi.com",
    "mainichi.jp",
    "yomiuri.co.jp",
    "nikkei.com",
    "nhk.or.jp",
}


@dataclass
class ExtractionResult:
    matched: bool
    homepage_url: Optional[str]
    capital: Optional[str]
    industry: Optional[str]


def _match_company_text(page_text: str, company_name: str, address: Optional[str]) -> bool:
    normalized_page = normalize_text(page_text)
    name_score = fuzz.partial_ratio(normalize_text(company_name), normalized_page)
    addr_norm = normalize_text(address)
    if not addr_norm:
        return name_score > 80
    address_score = fuzz.partial_ratio(addr_norm, normalized_page)
    return name_score > 80 and address_score > 75


def _extract_field(patterns: list[re.Pattern[str]], text: str) -> Optional[str]:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group(1).strip()
    return None


def analyze_page(soup: BeautifulSoup, *, url: str, company_name: str, address: Optional[str]) -> ExtractionResult:
    page_text = soup.get_text(separator=" ")
    # Heuristics: corporate signals and news-like page detection
    signals = 0
    if POSTAL_RE.search(page_text):
        signals += 1
    if PHONE_RE.search(page_text):
        signals += 1
    for kw in CORP_KEYWORDS:
        if kw in page_text:
            signals += 1
            break
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    news_like = any(host.endswith(h) for h in NEWS_HOSTS) or any(seg in path for seg in ("/article", "/articles", "/news/"))
    capital = _extract_field(CAPITAL_PATTERNS, page_text)
    industry = _extract_field(INDUSTRY_PATTERNS, page_text)
    matched = _match_company_text(page_text, company_name, address)
    if news_like and signals < 2:
        matched = False
    return ExtractionResult(
        matched=matched,
        homepage_url=url if matched else None,
        capital=capital,
        industry=industry,
    )
