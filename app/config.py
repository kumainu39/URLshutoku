from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


# Preferred path: pydantic-settings is available
try:  # pragma: no cover - runtime check
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class Settings(BaseSettings):
        database_url: str = Field(
            default="postgresql+psycopg2://masaki:39masaki@localhost:5432/companyinfo",
            description="SQLAlchemy database URL where company records are stored.",
        )
        search_engine: str = Field(
            default="duckduckgo",
            description="Identifier of the search backend to use for candidate URL discovery.",
        )
        search_result_limit: int = Field(
            default=10,
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
        # Recheck window for companies whose homepage was not found
        recheck_not_found_days: int = Field(
            default=30,
            description="After this many days, re-queue companies marked NOT_FOUND for another search.",
        )

        model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            extra="ignore",
        )

    @lru_cache
    def get_settings() -> Settings:
        return Settings()  # type: ignore[arg-type]

except Exception:
    # Fallback path: pydantic-settings not installed, use BaseModel with dotenv
    import os
    from dotenv import load_dotenv

    class Settings(BaseModel):
        database_url: str = Field(
            default="postgresql+psycopg2://masaki:39masaki@localhost:5432/companyinfo",
            description="SQLAlchemy database URL where company records are stored.",
        )
        search_engine: str = Field(default="duckduckgo")
        search_result_limit: int = Field(default=10)
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
        recheck_not_found_days: int = Field(default=30)

        class Config:
            extra = "ignore"

    @lru_cache
    def get_settings() -> Settings:
        load_dotenv()

        def _get_bool(name: str, default: bool) -> bool:
            v = os.getenv(name)
            if v is None:
                return default
            return v.strip().lower() in {"1", "true", "yes", "on"}

        def _get_int(name: str, default: int) -> int:
            v = os.getenv(name)
            try:
                return int(v) if v is not None else default
            except Exception:
                return default

        def _get_float(name: str, default: float) -> float:
            v = os.getenv(name)
            try:
                return float(v) if v is not None else default
            except Exception:
                return default

        def _get_path(name: str) -> Optional[Path]:
            v = os.getenv(name)
            return Path(v) if v else None

        return Settings(
            database_url=os.getenv("DATABASE_URL", Settings().database_url),
            search_engine=os.getenv("SEARCH_ENGINE", Settings().search_engine),
            search_result_limit=_get_int("SEARCH_RESULT_LIMIT", Settings().search_result_limit),
            http_timeout_seconds=_get_float("HTTP_TIMEOUT_SECONDS", Settings().http_timeout_seconds),
            concurrency_limit=_get_int("CONCURRENCY_LIMIT", Settings().concurrency_limit),
            user_agent=os.getenv("USER_AGENT", Settings().user_agent),
            llm_enabled=_get_bool("LLM_ENABLED", Settings().llm_enabled),
            llm_model_path=_get_path("LLM_MODEL_PATH"),
            llm_gpu_layers=_get_int("LLM_GPU_LAYERS", Settings().llm_gpu_layers),
            llm_context_window=_get_int("LLM_CONTEXT_WINDOW", Settings().llm_context_window),
            recheck_not_found_days=_get_int("RECHECK_NOT_FOUND_DAYS", Settings().recheck_not_found_days),
        )
