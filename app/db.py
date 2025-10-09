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
    inspect,
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

    inspector = inspect(engine)
    if not inspector.has_table("companies"):
        return

    existing_columns = {col["name"] for col in inspector.get_columns("companies")}
    dialect = engine.dialect.name

    added_id = False

    with engine.begin() as conn:
        if "id" not in existing_columns:
            added_id = True
            if dialect == "postgresql":
                conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS id BIGSERIAL"))
            else:
                conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS id INTEGER"))
        if "homepage_url" not in existing_columns:
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS homepage_url TEXT"))
        if "capital" not in existing_columns:
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS capital TEXT"))
        if "industry" not in existing_columns:
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry TEXT"))
        if "last_checked_at" not in existing_columns:
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP"))
        if "last_status" not in existing_columns:
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_status TEXT"))
        if "skip" not in existing_columns:
            if dialect == "postgresql":
                conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS skip BOOLEAN DEFAULT FALSE"))
            else:
                conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS skip BOOLEAN DEFAULT 0"))

    if added_id:
        with engine.begin() as conn:
            if dialect == "postgresql":
                conn.execute(
                    text(
                        "UPDATE companies "
                        "SET id = nextval(pg_get_serial_sequence('companies', 'id')) "
                        "WHERE id IS NULL",
                    )
                )
                conn.execute(text("ALTER TABLE companies ALTER COLUMN id SET NOT NULL"))
            else:
                conn.execute(text("UPDATE companies SET id = rowid WHERE id IS NULL"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_id ON companies(id)"))

    with engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(text("ALTER TABLE companies ALTER COLUMN skip SET DEFAULT FALSE"))
            conn.execute(text("UPDATE companies SET skip = FALSE WHERE skip IS NULL"))
        else:
            conn.execute(text("UPDATE companies SET skip = 0 WHERE skip IS NULL"))


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


def fetch_prefectures(engine: Engine) -> List[str]:
    inspector = inspect(engine)
    if not inspector.has_table("companies"):
        return []
    columns = {col["name"] for col in inspector.get_columns("companies")}
    prefecture_column: Optional[str] = None
    for candidate in ("prefecture_name", "prefecture"):
        if candidate in columns:
            prefecture_column = candidate
            break
    if prefecture_column is None:
        return []
    stmt = text(
        f"SELECT DISTINCT {prefecture_column} AS prefecture "
        f"FROM companies "
        f"WHERE {prefecture_column} IS NOT NULL AND {prefecture_column} <> '' "
        f"ORDER BY {prefecture_column}"
    )
    prefectures: List[str] = []
    seen = set()
    with engine.begin() as conn:
        result = conn.execute(stmt)
        for row in result:
            value = getattr(row, "prefecture", row[0])
            if value is None:
                continue
            name = value.strip() if isinstance(value, str) else value
            if not name or name in seen:
                continue
            seen.add(name)
            prefectures.append(name)
    return prefectures


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
