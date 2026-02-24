import os
import time
import logging
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        db_url = str(st.secrets.get("DATABASE_URL", "")).strip()
    if not db_url:
        st.error("DATABASE_URL 환경변수가 설정되지 않았습니다.")
        raise ValueError("DATABASE_URL is missing.")
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return db_url

@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    try:
        url = get_database_url()
        return create_engine(
            url,
            connect_args={"sslmode": "require", "connect_timeout": 10},
            pool_pre_ping=True, pool_size=2, max_overflow=2,
            pool_recycle=300, future=True
        )
    except Exception as e:
        logger.error(f"Engine creation failed: {e}")
        raise

def sql_read(engine, sql: str, params: dict = None, retries: int = 2) -> pd.DataFrame:
    """구체적인 SQLAlchemyError를 잡고, 로그를 남겨 사일런트 페일러를 개선합니다."""
    last_err = None
    for i in range(retries + 1):
        try:
            with engine.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except SQLAlchemyError as e:
            last_err = e
            logger.warning(f"[DB Read Retry {i}] SQL Execution failed: {e}")
            if i < retries: time.sleep(0.35 * (2 ** i))
            
    logger.error(f"[DB Read Final Fail] Failed to read DB: {last_err}")
    st.error("데이터베이스 조회 중 오류가 발생했습니다. (연결 지연 혹은 쿼리 오류)")
    return pd.DataFrame()

def sql_exec(engine, sql: str, params: dict = None, retries: int = 1) -> None:
    for i in range(retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), params or {})
            return
        except SQLAlchemyError as e:
            logger.warning(f"[DB Exec Retry {i}] {e}")
            if i < retries: time.sleep(0.25 * (2 ** i))
            else:
                logger.error(f"DB Execution Failed: {e}")
                st.error("데이터 저장/수정 중 오류가 발생했습니다.")

def table_exists(engine, table: str, schema: str = "public") -> bool:
    cache = st.session_state.setdefault("_table_names_cache", {})
    if schema not in cache:
        try:
            insp = inspect(engine)
            cache[schema] = set(insp.get_table_names(schema=schema))
        except SQLAlchemyError:
            cache[schema] = set()
    return table in cache[schema]

def get_table_columns(engine, table: str, schema: str = "public") -> set:
    try:
        insp = inspect(engine)
        return {str(c.get("name", "")).lower() for c in insp.get_columns(table, schema=schema)}
    except SQLAlchemyError:
        return set()