from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Iterator, Optional, Set

from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, declarative_base, sessionmaker

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "sqlite:///db.sqlite3"

# Load environment variables from .env ahead of time so DATABASE_URL is available.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(override=False)
load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=False)


def _get_database_url_from_streamlit() -> Optional[str]:
    try:
        import streamlit as st  # type: ignore
    except Exception:  # pragma: no cover - Streamlit secrets are optional
        return None

    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - secrets may be unavailable locally
        return None

    for key in ("DATABASE_URL", "database_url"):
        try:
            value = secrets.get(key)  # type: ignore[call-arg]
        except Exception:  # pragma: no cover - secrets may be unavailable entirely
            continue
        if value:
            return str(value)
    return None


@lru_cache(maxsize=1)
def get_database_url() -> str:
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    secret_url = _get_database_url_from_streamlit()
    if secret_url:
        return secret_url
    return DEFAULT_DATABASE_URL


def _build_connect_args(database_url: str) -> dict[str, object]:
    url_obj = make_url(database_url)
    connect_args: dict[str, object] = {}
    if url_obj.get_backend_name() == "sqlite":
        if url_obj.database and url_obj.database != ":memory:":
            db_path = Path(url_obj.database)
            if not db_path.is_absolute():
                db_path = _PROJECT_ROOT / db_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
        connect_args["check_same_thread"] = False
    return connect_args


def _configure_sqlite(engine: Engine, database_url: str) -> None:
    url_obj = make_url(database_url)
    if url_obj.get_backend_name() != "sqlite":
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON;")
            cursor.execute("PRAGMA journal_mode=WAL;")
        finally:
            cursor.close()


def _create_engine() -> Engine:
    database_url = get_database_url()
    connect_args = _build_connect_args(database_url)
    engine = create_engine(
        database_url,
        connect_args=connect_args,
        future=True,
        pool_pre_ping=True,
    )
    _configure_sqlite(engine, database_url)
    return engine


def _create_alembic_config() -> "AlembicConfig":
    from alembic.config import Config as AlembicConfig

    ini_path = _PROJECT_ROOT / "alembic.ini"
    script_location = _PROJECT_ROOT / "alembic"

    if not ini_path.exists():
        raise FileNotFoundError("alembic.ini was not found")
    if not script_location.exists():
        raise FileNotFoundError("alembic/ directory was not found")

    alembic_config = AlembicConfig(str(ini_path))
    alembic_config.set_main_option("script_location", str(script_location))
    alembic_config.set_main_option("sqlalchemy.url", get_database_url())
    return alembic_config


_MIGRATIONS_COMPLETED: Set[str] = set()


def run_database_migrations(revision: str = "head") -> None:
    if revision in _MIGRATIONS_COMPLETED:
        return
    try:
        alembic_config = _create_alembic_config()
    except FileNotFoundError as exc:  # pragma: no cover - configuration guard
        raise RuntimeError("Alembic configuration is not available") from exc

    from alembic import command  # Imported lazily to avoid mandatory dependency for utility scripts.

    command.upgrade(alembic_config, revision)
    _MIGRATIONS_COMPLETED.add(revision)


def _create_all_metadata() -> None:
    Base.metadata.create_all(bind=engine, checkfirst=True)


def init_db() -> None:
    try:
        run_database_migrations()
    except Exception as exc:  # noqa: BLE001 - fallback to metadata creation
        logger.warning(
            "Failed to apply Alembic migrations automatically: %s. Falling back to Base.metadata.create_all().",
            exc,
        )
        _create_all_metadata()
    else:
        _create_all_metadata()


engine = _create_engine()
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
Base = declarative_base()


__all__ = [
    "Base",
    "SessionLocal",
    "engine",
    "get_database_url",
    "init_db",
    "run_database_migrations",
    "session_scope",
]


@contextmanager
def session_scope() -> Iterator[Session]:
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
