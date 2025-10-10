from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator, List, Optional, Tuple

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
    case,
    func,
    or_,
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
    Column("prefecture_name", String, nullable=True),
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
    prioritize_missing: bool = True,
) -> List[dict]:
    query = select(companies).where(companies.c.skip.is_(False))
    if skip_existing:
        query = query.where(or_(companies.c.homepage_url.is_(None), companies.c.homepage_url == ""))
    if prefecture:
        # Filter by dedicated prefecture_name column when available
        if "prefecture_name" in companies.c:
            query = query.where(companies.c.prefecture_name == prefecture)
        else:
            query = query.where(companies.c.address.contains(prefecture))
    order_cols = []
    if prioritize_missing:
        # 0 for missing (NULL or empty), 1 for existing -> missing first
        order_cols.append(
            case((or_(companies.c.homepage_url.is_(None), companies.c.homepage_url == ""), 0), else_=1)
        )
    order_cols.append(companies.c.id)
    query = query.order_by(*order_cols).offset(offset)
    if limit is not None:
        query = query.limit(limit)

    with engine.begin() as conn:
        result = conn.execute(query)
        rows: List[dict] = []
        for r in result:
            rec = dict(r._mapping)
            # Prefer structured components when available to build a clean address
            parts = [
                (rec.get("prefecture_name") or "").strip(),
                (rec.get("city_name") or "").strip(),
                (rec.get("street_number") or "").strip(),
            ]
            composed = "".join([p for p in parts if p])
            if composed:
                rec["address"] = composed
            rows.append(rec)
        return rows


def count_missing_by_prefecture(engine: Engine, prefecture: Optional[str] = None) -> Tuple[int, int]:
    """Return (missing_count, total_count) optionally filtered by prefecture.

    Excludes rows marked as skip.
    """
    where_parts = [companies.c.skip.is_(False)]
    if prefecture:
        if "prefecture_name" in companies.c:
            where_parts.append(companies.c.prefecture_name == prefecture)
        else:
            where_parts.append(companies.c.address.contains(prefecture))

    missing_clause = or_(companies.c.homepage_url.is_(None), companies.c.homepage_url == "")
    with engine.begin() as conn:
        total = conn.execute(select(func.count()).select_from(companies).where(*where_parts)).scalar_one()
        missing = conn.execute(
            select(func.count()).select_from(companies).where(*where_parts, missing_clause)
        ).scalar_one()
    return int(missing), int(total)


def fetch_prefectures(engine: Engine) -> List[str]:
    inspector = inspect(engine)
    if not inspector.has_table("companies"):
        return []
    cols = {col["name"] for col in inspector.get_columns("companies")}
    if "prefecture_name" not in cols:
        return []
    stmt = text(
        "SELECT DISTINCT prefecture_name AS prefecture "
        "FROM companies "
        "WHERE prefecture_name IS NOT NULL AND prefecture_name <> '' "
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
