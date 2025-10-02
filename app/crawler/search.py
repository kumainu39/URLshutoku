from __future__ import annotations

import asyncio
import re
from typing import List

import httpx
from loguru import logger

from ..config import get_settings

DUCKDUCKGO_SEARCH_URL = "https://duckduckgo.com/html/"


async def duckduckgo_search(query: str, *, limit: int) -> List[str]:
    params = {"q": query, "kl": "jp-jp", "df": "y"}
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, headers=headers) as client:
        response = await client.get(DUCKDUCKGO_SEARCH_URL, params=params)
        response.raise_for_status()
        urls = re.findall(r"<a rel=\"nofollow\" class=\"result__a\" href=\"(.*?)\"", response.text)
        cleaned = []
        for url in urls:
            if url.startswith("https://duckduckgo.com/l/?uddg="):
                url = httpx.URL(url).params.get("uddg", "")
            cleaned.append(url)
            if len(cleaned) >= limit:
                break
        logger.debug("DuckDuckGo returned {count} results for query '{query}'", count=len(cleaned), query=query)
        return cleaned


async def search_company(name: str, address: str) -> List[str]:
    settings = get_settings()
    query = f"{name} {address}"
    if settings.search_engine == "duckduckgo":
        return await duckduckgo_search(query, limit=settings.search_result_limit)
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
