from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from energywatch.db.models import Base

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DB_PATH = _PROJECT_ROOT / "data" / "energywatch.db"


def get_db_url() -> str:
    return os.environ.get(
        "ENERGYWATCH_DB_URL",
        f"sqlite:///{_DEFAULT_DB_PATH}",
    )


def get_engine(db_url: str | None = None):
    url = db_url or get_db_url()
    engine = create_engine(
        url,
        connect_args={"check_same_thread": False},
        echo=False,
    )

    @event.listens_for(engine, "connect")
    def set_pragmas(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def init_db(engine=None) -> None:
    """Create all tables. Also ensures data/ dir exists."""
    _DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    eng = engine or get_engine()
    Base.metadata.create_all(eng)


_engine = None
_SessionLocal = None


def get_session() -> Session:
    global _engine, _SessionLocal
    if _engine is None:
        _engine = get_engine()
        init_db(_engine)
        _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)
    return _SessionLocal()
