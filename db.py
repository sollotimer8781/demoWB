from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, declarative_base, sessionmaker

DEFAULT_DATABASE_URL = "sqlite:///data.db"


def _get_database_url_from_streamlit() -> Optional[str]:
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return None
    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None
    for key in ("DATABASE_URL", "database_url"):
        value = secrets.get(key)  # type: ignore[call-arg]
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


def _build_connect_args(database_url: str) -> dict:
    url_obj = make_url(database_url)
    connect_args = {}
    if url_obj.get_backend_name() == "sqlite":
        if url_obj.database and url_obj.database != ":memory:":
            db_path = Path(url_obj.database)
            if not db_path.is_absolute():
                db_path = Path.cwd() / db_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
        connect_args["check_same_thread"] = False
    return connect_args


def _create_engine():
    database_url = get_database_url()
    connect_args = _build_connect_args(database_url)
    return create_engine(
        database_url,
        connect_args=connect_args,
        future=True,
        pool_pre_ping=True,
    )


engine = _create_engine()
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
Base = declarative_base()

__all__ = ["Base", "SessionLocal", "engine", "init_db", "session_scope", "get_database_url"]


def init_db() -> None:
    import models  # noqa: F401

    Base.metadata.create_all(bind=engine)


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
