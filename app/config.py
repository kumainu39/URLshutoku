from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class Settings(BaseModel):
    database_url: str = Field(
        default="postgresql+psycopg2://masaki:39masaki@localhost:5432/companyinfo",
        description="SQLAlchemy database URL where company records are stored.",
    )
    search_engine: str = Field(
        default="duckduckgo",
        description="Identifier of the search backend to use for candidate URL discovery.",
    )
    search_result_limit: int = Field(
        default=5,
        description="Maximum number of search engine results to consider per company.",
    )
    http_timeout_seconds: float = Field(default=15.0)
    concurrency_limit: int = Field(default=5)
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
    )
    llm_enabled: bool = False
    llm_model_path: Optional[Path] = None
    llm_gpu_layers: int = 0
    llm_context_window: int = 4096

    class Config:
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    from dotenv import load_dotenv

    load_dotenv()
    return Settings()  # type: ignore[arg-type]
