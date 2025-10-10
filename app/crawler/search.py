from __future__ import annotations

import asyncio
import re
import html as _html
from typing import List, Tuple

import httpx
from loguru import logger

from ..config import get_settings
from .normalize import normalize_text

DUCKDUCKGO_SEARCH_URL = "https://html.duckduckgo.com/html/"


async def duckduckgo_search(query: str, *, limit: int) -> List[str]:
    params = {"q": query, "kl": "jp-jp", "df": "y"}
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(DUCKDUCKGO_SEARCH_URL, params=params)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("DuckDuckGo search failed: {exc}", exc=exc)
            return []
        urls = re.findall(r"<a[^>]+class=\"result__a\"[^>]*href=\"(.*?)\"", response.text)
        cleaned: List[str] = []
        for href in urls:
            href = _html.unescape(href)
            # Normalize protocol-relative and relative links
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://duckduckgo.com" + href

            try:
                u = httpx.URL(href)
            except Exception:
                continue

            # Unwrap DuckDuckGo redirector /l/?uddg=...
            if u.host and "duckduckgo.com" in u.host and u.path.startswith("/l/"):
                target = u.params.get("uddg", "")
                if target:
                    try:
                        href = str(httpx.URL(target))
                    except Exception:
                        href = target

            cleaned.append(href)
            if len(cleaned) >= limit:
                break
        # Deduplicate while preserving order
        seen = set()
        unique: List[str] = []
        for u in cleaned:
            if u and u not in seen:
                seen.add(u)
                unique.append(u)
        logger.debug(
            "DuckDuckGo returned {count} results for query '{query}'",
            count=len(unique),
            query=query,
        )
        return unique


_BLOCKLIST_HOSTS = {
    # Aggregators / directories / number lookups / news
    "houjin.info",
    "houjin.jp",
    "corporatedb.jp",
    "baseconnect.in",
    "jpnumber.com",
    "prtimes.jp",
    "irbank.net",
    "maonline.jp",
    "data-link-plus.com",
    # News/media
    "toonippo.co.jp",
    "yahoo.co.jp",
    "asahi.com",
    "mainichi.jp",
    "yomiuri.co.jp",
    "nikkei.com",
    "nhk.or.jp",
}

# Explicit exclusion by substring match (case-insensitive)
EXCLUDE_KEYWORDS = [
    'cnavi.g-search.or.jp',
    'detail/',
    'mynavi.jp',
    'fumadata.com',
    'salesnow.jp',
    'hpsm.noor.jp',
    'www.nakai-seika.co.jp',
    'www.dreamnews.jp',
    'en-gage.net',
    'alarmbox.jp',
    'shoku-bank.jp',
    'ameblo.jp',
    'houjin.goo.to',
    'khn-messe.jp',
    'www.ekiten.jp',
    'tabelog.com',
    'prtimes.jp',
    'gmo-connect.com',
    'www.hatomarksite.com',
    'map.yahoo.co.jp',
    'jbplt.jp',
    'akala.ai',
    'www.suinaka.or.jp',
    'itp.ne.jp',
    'caretaxi-net.com',
    'takunavi.jp',
    'www.tdb.co.jp',
    'www.buffett-code.com',
    'mado2.jp',
    'toukibo.ai-con.lawyer',
    'www.osakataxi.or.jp',
    'doda.jp',
    'info.gbiz.go.jp',
    'www.nikkei.com',
    'www.facebook.com',
    'baseconnect.in',
    'www.b-mall.ne.jp',
    'www.seino.co.jp',
    '?',
    '%',
    'x-work.jp',
    'www.akabou.ne.jp',
    'www2.akabou.ne.jp',
    'www.hellowork.careers',
    'drivers-job.biz',
    'sline.co.jp',
    'korps.jp',
    'www.big-advance.site',
    'www.cookdoor.jp',
    'www.nohhi.co.jp',
    'job.trck.jp',
    'job.goo.to',
    'www.yabashi.co.jp',
    'offerers.jp',
    'curama.jp',
    'www.marutokusangyo.co.jp',
    'www.daidolife.com',
    'www.introcompa.com',
    'truckaichi.com',
    'doraever.jp',
    'search?',
    'jp.indeed.com',
    'www.mapion.co.jp',
]


def _score_url(url: str) -> Tuple[int, int]:
    """Lower score is prioritized. Heuristics:
    - Penalize known aggregator hosts.
    - Prefer .co.jp /.ne.jp /.or.jp /.jp, then .com; others after.
    """
    try:
        u = httpx.URL(url)
    except Exception:
        return (100, 100)
    host = (u.host or "").lower()
    if host.startswith("www."):
        host = host[4:]
    tld_penalty = 5
    if host.endswith(".co.jp") or host.endswith(".ne.jp") or host.endswith(".or.jp"):
        tld_penalty = 0
    elif host.endswith(".jp"):
        tld_penalty = 1
    elif host.endswith(".com"):
        tld_penalty = 2
    block_penalty = 10 if any(host == b or host.endswith("." + b) for b in _BLOCKLIST_HOSTS) else 0
    return (block_penalty + tld_penalty, len(url))


async def search_company(name: str, address: str | None) -> List[str]:
    settings = get_settings()
    # Normalize company name and address to improve search robustness
    name_n = normalize_text(name) or name
    addr = (address or "").strip()
    addr_n = normalize_text(addr) if addr else ""
    query = f"{name_n} {addr_n}" if addr_n else name_n
    # Bias search towards company profile pages
    query = f"{query} 企業概要"
    if settings.search_engine == "duckduckgo":
        urls = await duckduckgo_search(query, limit=settings.search_result_limit)
        # Exclude URLs by explicit keywords
        lowers = []
        filtered: List[str] = []
        for u in urls:
            lu = u.lower()
            if any(key in lu for key in EXCLUDE_KEYWORDS):
                continue
            filtered.append(u)
        # Sort with heuristic to prioritize likely official sites
        filtered.sort(key=_score_url)
        logger.debug("Candidates for '{q}': {urls}", q=query, urls=filtered)
        return filtered
    raise ValueError(f"Unsupported search engine: {settings.search_engine}")


async def gather_searches(companies: List[dict]) -> List[List[str]]:
    semaphore = asyncio.Semaphore(get_settings().concurrency_limit)

    async def _search(company: dict) -> List[str]:
        async with semaphore:
            try:
                return await search_company(company["name"], company["address"])
            except Exception as exc:
                logger.exception("Search failed for company_id={id}: {exc}", id=company["id"], exc=exc)
                return []

    return await asyncio.gather(*[_search(company) for company in companies])
