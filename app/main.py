from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from .config import Settings, get_settings
from .services.pipeline import CrawlPipeline, JobConfig, JobManager, JobState

app = FastAPI(title="URL取得支援ツール")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

job_manager = JobManager()
pipeline = CrawlPipeline()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    settings: Settings = get_settings()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "jobs": job_manager.jobs,
            "settings": settings,
        },
    )


@app.post("/jobs", response_class=HTMLResponse)
async def create_job(
    request: Request,
    prefecture: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    chunk_size: int = Form(default=100),
    concurrency: int = Form(default=5),
    skip_existing: Optional[str] = Form(default=None),
) -> HTMLResponse:
    job_id = str(uuid.uuid4())
    chunk_size = max(1, chunk_size)
    concurrency = max(1, concurrency)
    config = JobConfig(
        prefecture=prefecture or None,
        limit=limit,
        chunk_size=chunk_size,
        concurrency=concurrency,
        skip_existing=bool(skip_existing),
    )
    settings = get_settings()
    settings.concurrency_limit = concurrency  # type: ignore[attr-defined]
    state = JobState(job_id=job_id, config=config)
    await job_manager.create_job(state)

    async def _run_job() -> None:
        logger.info("ジョブ開始: {job_id}", job_id=job_id)
        await pipeline.run(state, on_update=lambda _: None)
        logger.info("ジョブ終了: {job_id}", job_id=job_id)

    asyncio.create_task(_run_job())

    return templates.TemplateResponse(
        "job.html",
        {"request": request, "job": state},
    )


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str) -> HTMLResponse:
    job = job_manager.get(job_id)
    if job is None:
        return HTMLResponse(content="ジョブが見つかりません", status_code=404)
    return templates.TemplateResponse("job.html", {"request": request, "job": job})
