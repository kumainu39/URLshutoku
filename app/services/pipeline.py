from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from bs4 import BeautifulSoup
from loguru import logger

from .. import db
from ..crawler import extract, fetch, search
from .llm import LLMRequest, LLMVerifier


@dataclass
class JobConfig:
    prefecture: Optional[str]
    limit: Optional[int]
    chunk_size: int
    concurrency: int
    skip_existing: bool


@dataclass
class JobLog:
    messages: List[str] = field(default_factory=list)

    def add(self, message: str) -> None:
        logger.info(message)
        self.messages.append(message)


@dataclass
class JobState:
    job_id: str
    config: JobConfig
    total: int = 0
    processed: int = 0
    successes: int = 0
    failures: int = 0
    skipped: int = 0
    log: JobLog = field(default_factory=JobLog)


class CrawlPipeline:
    def __init__(self, *, engine=None) -> None:
        self.engine = engine or db.get_engine()
        db.ensure_schema(self.engine)
        self.llm = LLMVerifier()
        self._state_lock = asyncio.Lock()

    async def run(self, state: JobState, *, on_update: Optional[Callable[[JobState], None]] = None) -> JobState:
        offset = 0
        fetched = 0
        semaphore = asyncio.Semaphore(state.config.concurrency)
        scheduled = 0

        async def _run_company(company: dict) -> None:
            async with semaphore:
                await self._process_company(state, company)
                if on_update:
                    on_update(state)

        pending: List[asyncio.Task[None]] = []

        while True:
            companies_batch = db.fetch_companies(
                self.engine,
                prefecture=state.config.prefecture,
                limit=state.config.chunk_size,
                skip_existing=state.config.skip_existing,
                offset=offset,
            )
            offset += state.config.chunk_size
            if not companies_batch:
                break
            fetched += len(companies_batch)
            state.total = fetched if state.config.limit is None else min(fetched, state.config.limit)

            for company in companies_batch:
                if state.config.limit is not None and scheduled >= state.config.limit:
                    break
                pending.append(asyncio.create_task(_run_company(company)))
                scheduled += 1

            if pending:
                await asyncio.gather(*pending)
                pending.clear()

            if state.config.limit is not None and scheduled >= state.config.limit:
                break

        async with self._state_lock:
            state.log.add("ジョブが完了しました。")
        return state

    async def _process_company(self, state: JobState, company: dict) -> None:
        if state.config.skip_existing and company.get("homepage_url"):
            async with self._state_lock:
                state.skipped += 1
                state.processed += 1
                state.log.add(
                    f"既存URLのためスキップ: {company['name']} ({company['corporate_number']})"
                )
            return

        async with self._state_lock:
            state.log.add(f"検索開始: {company['name']} ({company['corporate_number']})")
        candidate_urls = await search.search_company(company["name"], company["address"])

        if not candidate_urls:
            async with self._state_lock:
                state.failures += 1
                state.processed += 1
            db.update_company(
                self.engine,
                company["id"],
                homepage_url=None,
                capital=None,
                industry=None,
                status="no_candidates",
            )
            async with self._state_lock:
                state.log.add("候補URLが見つかりませんでした。")
            return

        for url in candidate_urls:
            soup = await fetch.fetch_html(url)
            if soup is None:
                continue
            result = await self._verify_candidate(soup, company, url)
            if result is None:
                continue
            async with self._state_lock:
                state.successes += 1
                state.processed += 1
            db.update_company(
                self.engine,
                company["id"],
                homepage_url=result.homepage_url,
                capital=result.capital,
                industry=result.industry,
                status="matched",
            )
            async with self._state_lock:
                state.log.add(f"一致: {url}")
            return

        async with self._state_lock:
            state.failures += 1
            state.processed += 1
        db.update_company(
            self.engine,
            company["id"],
            homepage_url=None,
            capital=None,
            industry=None,
            status="no_match",
        )
        async with self._state_lock:
            state.log.add("一致するページがありませんでした。")

    async def _verify_candidate(
        self, soup: BeautifulSoup, company: dict, url: str
    ) -> Optional[extract.ExtractionResult]:
        result = extract.analyze_page(
            soup,
            url=url,
            company_name=company["name"],
            address=company["address"],
        )
        if result.matched:
            return result
        if self.llm.enabled:
            llm_result = self.llm.validate(
                LLMRequest(
                    company_name=company["name"],
                    address=company["address"],
                    page_text=soup.get_text(separator=" "),
                )
            )
            if llm_result:
                return extract.ExtractionResult(
                    matched=True,
                    homepage_url=url,
                    capital=result.capital,
                    industry=result.industry,
                )
        return None


class JobManager:
    def __init__(self) -> None:
        self.jobs: Dict[str, JobState] = {}
        self._lock = asyncio.Lock()

    async def create_job(self, state: JobState) -> JobState:
        async with self._lock:
            self.jobs[state.job_id] = state
        return state

    def get(self, job_id: str) -> Optional[JobState]:
        return self.jobs.get(job_id)

    async def update(self, job_id: str, updater: Callable[[JobState], None]) -> None:
        async with self._lock:
            if job_id in self.jobs:
                updater(self.jobs[job_id])
