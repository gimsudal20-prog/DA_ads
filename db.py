import os
import time
from typing import Optional, List
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()

_HASH_FUNCS = {Engine: lambda e: e.url.render_as_string(hide_password=True)}

def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        try:
            db_url = str(st.secrets.get("DATABASE_URL", "")).strip()
        except Exception:
            db_url = ""

    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. (.env env var or Streamlit secrets)")

    if "sslmode=" not in db_url:
        joiner = "&" if "?" in db_url else "?"
        db_url = db_url + f"{joiner}sslmode=require"

    return db_url

@st.cache_resource(show_spinner=False)
def get_engine():
    url = get_database_url()
    connect_args = {
        "sslmode": "require",
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
    return create_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=300,
        pool_use_lifo=True,
        future=True,
    )

def _reset_engine_cache() -> None:
    try:
        get_engine.clear()
    except Exception:
        pass

def sql_read(engine, sql: str, params: Optional[dict] = None, retries: int = 2) -> pd.DataFrame:
    last_err: Exception | None = None
    _engine = engine

    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except Exception as e:
            last_err = e
            try:
                _engine.dispose()
            except Exception:
                pass
            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine
            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err

def sql_exec(engine, sql: str, params: Optional[dict] = None, retries: int = 1) -> None:
    last_err = None
    for i in range(retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), params or {})
            return
        except Exception as e:
            last_err = e
            try:
                engine.dispose()
            except Exception:
                pass
            if i < retries:
                time.sleep(0.25 * (2 ** i))
                continue
            raise last_err

def db_ping(engine, retries: int = 2) -> None:
    last_err: Exception | None = None
    _engine = engine
    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            try:
                _engine.dispose()
            except Exception:
                pass
            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine
            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err

def _get_table_names_cached(engine, schema: str = "public") -> set:
    cache = st.session_state.setdefault("_table_names_cache", {})
    if schema in cache:
        return cache[schema]
    try:
        insp = inspect(engine)
        names = set(insp.get_table_names(schema=schema))
    except Exception:
        names = set()
    cache[schema] = names
    return names

def table_exists(engine, table: str, schema: str = "public") -> bool:
    return table in _get_table_names_cached(engine, schema=schema)

def get_table_columns(engine, table: str, schema: str = "public") -> set:
    cache = st.session_state.setdefault("_table_cols_cache", {})
    key = f"{schema}.{table}"
    if key in cache:
        return cache[key]
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        out = {str(c.get("name", "")).lower() for c in cols}
    except Exception:
        out = set()
    cache[key] = out
    return out

def _sql_in_str_list(values: List[int]) -> str:
    safe = []
    for v in values:
        try:
            safe.append(f"'{int(v)}'")
        except Exception:
            continue
    return ",".join(safe) if safe else "''"
