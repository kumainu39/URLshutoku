from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
    text,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import get_settings

metadata = MetaData()

companies = Table(
    "companies",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("corporate_number", String, nullable=False, unique=True),
    Column("name", String, nullable=False),
    Column("address", String, nullable=False),
    Column("homepage_url", String, nullable=True),
    Column("capital", String, nullable=True),
    Column("industry", String, nullable=True),
    Column("last_checked_at", DateTime, nullable=True),
    Column("last_status", String, nullable=True),
    Column("skip", Boolean, nullable=False, server_default=text("0")),
)


def get_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url, future=True)


@contextmanager
def session_scope() -> Iterator[Engine]:
    engine = get_engine()
    try:
        yield engine
    finally:
        engine.dispose()


def ensure_schema(engine: Engine) -> None:
    metadata.create_all(engine)


def fetch_companies(
    engine: Engine,
    *,
    prefecture: Optional[str] = None,
    limit: Optional[int] = None,
    skip_existing: bool = True,
    offset: int = 0,
) -> List[dict]:
    query = select(companies).where(companies.c.skip.is_(False))
    if skip_existing:
        query = query.where(companies.c.homepage_url.is_(None))
    if prefecture:
        query = query.where(companies.c.address.contains(prefecture))
    query = query.order_by(companies.c.id).offset(offset)
    if limit is not None:
        query = query.limit(limit)

    with engine.begin() as conn:
        result = conn.execute(query)
        return [dict(row._mapping) for row in result]


def update_company(
    engine: Engine,
    company_id: int,
    *,
    homepage_url: Optional[str],
    capital: Optional[str],
    industry: Optional[str],
    status: str,
) -> None:
    stmt = (
        update(companies)
        .where(companies.c.id == company_id)
        .values(
            homepage_url=homepage_url,
            capital=capital,
            industry=industry,
            last_status=status,
            last_checked_at=datetime.utcnow(),
        )
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def bulk_upsert(engine: Engine, rows: Iterable[dict]) -> None:
    with engine.begin() as conn:
        for row in rows:
            try:
                conn.execute(companies.insert().values(**row))
            except SQLAlchemyError:
                stmt = (
                    update(companies)
                    .where(companies.c.corporate_number == row["corporate_number"])
                    .values({k: v for k, v in row.items() if k != "corporate_number"})
                )
                conn.execute(stmt)
