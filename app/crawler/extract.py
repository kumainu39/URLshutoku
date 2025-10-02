from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from .normalize import normalize_text


CAPITAL_PATTERNS = [
    re.compile(r"資本金[:：]?\s*([0-9０-９,，\.億万円]+)", re.IGNORECASE),
    re.compile(r"capital\s*[:：]?\s*([0-9,\.]+\s*(yen|円|jpy))", re.IGNORECASE),
]

INDUSTRY_PATTERNS = [
    re.compile(r"業種[:：]?\s*([\w一-龠ぁ-んァ-ンー・／/\s]+)"),
    re.compile(r"business\s*[:：]?\s*(.+)"),
]


@dataclass
class ExtractionResult:
    matched: bool
    homepage_url: Optional[str]
    capital: Optional[str]
    industry: Optional[str]


def _match_company_text(page_text: str, company_name: str, address: str) -> bool:
    normalized_page = normalize_text(page_text)
    name_score = fuzz.partial_ratio(normalize_text(company_name), normalized_page)
    address_score = fuzz.partial_ratio(normalize_text(address), normalized_page)
    return name_score > 80 and address_score > 75


def _extract_field(patterns: list[re.Pattern[str]], text: str) -> Optional[str]:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group(1).strip()
    return None


def analyze_page(soup: BeautifulSoup, *, url: str, company_name: str, address: str) -> ExtractionResult:
    page_text = soup.get_text(separator=" ")
    capital = _extract_field(CAPITAL_PATTERNS, page_text)
    industry = _extract_field(INDUSTRY_PATTERNS, page_text)
    matched = _match_company_text(page_text, company_name, address)
    return ExtractionResult(
        matched=matched,
        homepage_url=url if matched else None,
        capital=capital,
        industry=industry,
    )
