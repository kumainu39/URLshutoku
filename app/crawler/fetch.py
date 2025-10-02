from __future__ import annotations

import asyncio
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from ..config import get_settings


async def fetch_html(url: str) -> Optional[BeautifulSoup]:
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Failed to fetch {url}: {exc}", url=url, exc=exc)
            return None
    return BeautifulSoup(response.text, "html.parser")


async def fetch_multiple(urls: list[str]) -> list[Optional[BeautifulSoup]]:
    semaphore = asyncio.Semaphore(get_settings().concurrency_limit)

    async def _fetch(url: str) -> Optional[BeautifulSoup]:
        async with semaphore:
            return await fetch_html(url)

    return await asyncio.gather(*[_fetch(url) for url in urls])
