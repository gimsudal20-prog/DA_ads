# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from device_collector_helpers import ensure_device_tables


def _log(msg: str) -> None:
    print(msg, flush=True)


def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _safe_rollback(raw_conn, *, ctx: str = ""):
    if not raw_conn:
        return
    try:
        raw_conn.rollback()
    except Exception as rollback_exc:
        extra = f" | {ctx}" if ctx else ""
        _log(f"⚠️ 롤백 실패{extra} | {_exc_label(rollback_exc)}")


def _safe_close(resource, *, label: str, ctx: str = ""):
    if not resource:
        return
    try:
        resource.close()
    except Exception as close_exc:
        extra = f" | {ctx}" if ctx else ""
        _log(f"⚠️ {label} close 실패{extra} | {_exc_label(close_exc)}")


def _log_retry_failure(action: str, attempt: int, total: int, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    _log(f"⚠️ {action} 실패 {attempt}/{total}{extra} | {_exc_label(exc)}")


def _log_best_effort_failure(action: str, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    _log(f"⚠️ {action} 무시됨{extra} | {_exc_label(exc)}")


def _raise_retry_failure(action: str, exc: Exception | None, *, ctx: str = ""):
    msg = f"{action} 최종 실패"
    if ctx:
        msg += f" | {ctx}"
    if exc is not None:
        msg += f" | {_exc_label(exc)}"
        raise RuntimeError(msg) from exc
    raise RuntimeError(msg)


def _filter_nonzero_media_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        imp = int(round(float(r.get("imp", 0) or 0)))
        clk = int(round(float(r.get("clk", 0) or 0)))
        cost = int(round(float(r.get("cost", 0) or 0)))
        conv = float(r.get("conv", 0) or 0.0)
        sales = int(round(float(r.get("sales", 0) or 0)))
        if any([imp != 0, clk != 0, cost != 0, conv != 0.0, sales != 0]):
            out.append(r)
    return out


def get_engine(db_url: str) -> Engine:
    if not db_url:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다. collector.py는 실제 DB 연결이 필요합니다.")
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    import os
    pool_size = max(1, int(os.getenv("COLLECTOR_DB_POOL_SIZE", "6") or 6))
    max_overflow = max(0, int(os.getenv("COLLECTOR_DB_MAX_OVERFLOW", "12") or 12))
    pool_timeout = max(5, int(os.getenv("COLLECTOR_DB_POOL_TIMEOUT", "30") or 30))
    pool_recycle = max(60, int(os.getenv("COLLECTOR_DB_POOL_RECYCLE", "1800") or 1800))
    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=300000"},
        future=True,
    )


def ensure_column(engine: Engine, table: str, column: str, datatype: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {datatype}"))
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate column" in msg:
            return
        _log_best_effort_failure("ensure_column", e, ctx=f"table={table} column={column}")


def ensure_tables(engine: Engine):
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, image_url TEXT, PRIMARY KEY(customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_shopping_query_daily (
                    dt DATE,
                    customer_id TEXT,
                    campaign_id TEXT,
                    adgroup_id TEXT,
                    ad_id TEXT,
                    query_text TEXT,
                    total_conv DOUBLE PRECISION,
                    total_sales BIGINT DEFAULT 0,
                    purchase_conv DOUBLE PRECISION,
                    purchase_sales BIGINT DEFAULT 0,
                    cart_conv DOUBLE PRECISION,
                    cart_sales BIGINT DEFAULT 0,
                    wishlist_conv DOUBLE PRECISION,
                    wishlist_sales BIGINT DEFAULT 0,
                    split_available BOOLEAN,
                    data_source TEXT,
                    PRIMARY KEY(dt, customer_id, adgroup_id, ad_id, query_text)
                )"""))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS fact_campaign_off_log (
                        dt DATE,
                        customer_id TEXT,
                        campaign_id TEXT,
                        off_time TEXT,
                        PRIMARY KEY(dt, customer_id, campaign_id)
                    )
                """))

            ensure_column(engine, "dim_ad", "ad_title", "TEXT")
            ensure_column(engine, "dim_ad", "ad_desc", "TEXT")
            ensure_column(engine, "dim_ad", "pc_landing_url", "TEXT")
            ensure_column(engine, "dim_ad", "mobile_landing_url", "TEXT")
            ensure_column(engine, "dim_ad", "creative_text", "TEXT")
            ensure_column(engine, "dim_ad", "image_url", "TEXT")

            for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily"]:
                ensure_column(engine, table, "purchase_conv", "DOUBLE PRECISION")
                ensure_column(engine, table, "purchase_sales", "BIGINT")
                ensure_column(engine, table, "purchase_roas", "DOUBLE PRECISION")
                ensure_column(engine, table, "cart_conv", "DOUBLE PRECISION")
                ensure_column(engine, table, "cart_sales", "BIGINT")
                ensure_column(engine, table, "cart_roas", "DOUBLE PRECISION")
                ensure_column(engine, table, "wishlist_conv", "DOUBLE PRECISION")
                ensure_column(engine, table, "wishlist_sales", "BIGINT")
                ensure_column(engine, table, "wishlist_roas", "DOUBLE PRECISION")
                ensure_column(engine, table, "primary_conv", "DOUBLE PRECISION")
                ensure_column(engine, table, "primary_sales", "BIGINT")
                ensure_column(engine, table, "primary_roas", "DOUBLE PRECISION")
                ensure_column(engine, table, "split_available", "BOOLEAN")
                ensure_column(engine, table, "data_source", "TEXT")

            with engine.begin() as conn:
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_media_daily (
                    dt DATE,
                    customer_id TEXT,
                    campaign_type TEXT,
                    media_name TEXT,
                    region_name TEXT,
                    device_name TEXT DEFAULT '전체',
                    imp BIGINT,
                    clk BIGINT,
                    cost BIGINT,
                    conv DOUBLE PRECISION,
                    sales BIGINT DEFAULT 0,
                    data_source TEXT,
                    source_report TEXT,
                    PRIMARY KEY(dt, customer_id, campaign_type, media_name, region_name, device_name)
                )"""))
            ensure_column(engine, "fact_media_daily", "data_source", "TEXT")
            ensure_column(engine, "fact_media_daily", "source_report", "TEXT")

            ensure_device_tables(engine)
            break
        except Exception as e:
            time.sleep(3)
            if attempt == 2:
                raise e


def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = (
        f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f'ON CONFLICT ({pk_str}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'

    tuples = list(df.itertuples(index=False, name=None))
    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)} pk={pk_cols}"
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
        except Exception as e:
            last_err = e
            _safe_rollback(raw_conn, ctx=ctx)
            _log_retry_failure("DB 적재", attempt, 3, e, ctx=ctx)
            time.sleep(3)
        finally:
            _safe_close(cur, label="cursor", ctx=ctx)
            _safe_close(raw_conn, label="connection", ctx=ctx)
    _raise_retry_failure("DB 적재", last_err, ctx=ctx)


def clear_fact_range(engine: Engine, table: str, customer_id: str, d1):
    last_err: Exception | None = None
    ctx = f"table={table} cid={customer_id} dt={d1}"
    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            return
        except Exception as e:
            last_err = e
            _log_retry_failure("fact 범위 삭제", attempt, 3, e, ctx=ctx)
            time.sleep(3)
    _raise_retry_failure("fact 범위 삭제", last_err, ctx=ctx)


def clear_fact_scope(engine: Engine, table: str, customer_id: str, d1, pk: str, ids: List[str]):
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    last_err: Exception | None = None
    ctx = f"table={table} cid={customer_id} dt={d1} pk={pk} ids={len(ids)}"
    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f'DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt AND {pk} = ANY(:ids)'),
                    {"cid": str(customer_id), "dt": d1, "ids": ids},
                )
            return
        except Exception as e:
            last_err = e
            _log_retry_failure("fact 범위 삭제(scope)", attempt, 3, e, ctx=ctx)
            time.sleep(3)
    _raise_retry_failure("fact 범위 삭제(scope)", last_err, ctx=ctx)


def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1):
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=["dt", "customer_id", pk], keep="last").sort_values(by=["dt", "customer_id", pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ["dt", "customer_id", pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)}"
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
        except Exception as e:
            last_err = e
            _safe_rollback(raw_conn, ctx=ctx)
            _log_retry_failure("DB 적재", attempt, 3, e, ctx=ctx)
            time.sleep(3)
        finally:
            _safe_close(cur, label="cursor", ctx=ctx)
            _safe_close(raw_conn, label="connection", ctx=ctx)
    _raise_retry_failure("DB 적재", last_err, ctx=ctx)


def replace_query_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1):
    table = "fact_shopping_query_daily"
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk_cols = ["dt", "customer_id", "adgroup_id", "ad_id", "query_text"]
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)}"
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
        except Exception as e:
            last_err = e
            _safe_rollback(raw_conn, ctx=ctx)
            _log_retry_failure("DB 적재", attempt, 3, e, ctx=ctx)
            time.sleep(3)
        finally:
            _safe_close(cur, label="cursor", ctx=ctx)
            _safe_close(raw_conn, label="connection", ctx=ctx)
    _raise_retry_failure("DB 적재", last_err, ctx=ctx)


def replace_fact_scope(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1, pk: str, ids: List[str]):
    clear_fact_scope(engine, table, customer_id, d1, pk, ids)
    if not rows:
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=["dt", "customer_id", pk], keep="last").sort_values(by=["dt", "customer_id", pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ["dt", "customer_id", pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)}"
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
        except Exception as e:
            last_err = e
            _safe_rollback(raw_conn, ctx=ctx)
            _log_retry_failure("DB 적재", attempt, 3, e, ctx=ctx)
            time.sleep(3)
        finally:
            _safe_close(cur, label="cursor", ctx=ctx)
            _safe_close(raw_conn, label="connection", ctx=ctx)
    _raise_retry_failure("DB 적재", last_err, ctx=ctx)


def _get_fact_media_daily_conflict_cols(engine: Engine) -> List[str]:
    expected = ["dt", "customer_id", "campaign_type", "media_name", "region_name", "device_name"]
    legacy = ["dt", "customer_id", "campaign_type", "media_name", "region_name"]
    sql = text("""
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = 'fact_media_daily'
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 0 ELSE 1 END,
            tc.constraint_name,
            kcu.ordinal_position
    """)
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).mappings().all()
    except Exception as e:
        _log(f"⚠️ fact_media_daily 제약조건 조회 실패 → 기본 PK 가정 사용 | {type(e).__name__}: {e}")
        return expected

    grouped: Dict[tuple[str, str], List[str]] = {}
    for row in rows:
        key = (str(row.get("constraint_name") or ""), str(row.get("constraint_type") or ""))
        grouped.setdefault(key, []).append(str(row.get("column_name") or "").strip())

    ordered_candidates: List[List[str]] = []
    for (_, constraint_type), cols in grouped.items():
        if constraint_type == "PRIMARY KEY":
            ordered_candidates.insert(0, cols)
        else:
            ordered_candidates.append(cols)

    for cols in ordered_candidates:
        if cols == expected:
            return expected
    for cols in ordered_candidates:
        if cols == legacy:
            return legacy
    for cols in ordered_candidates:
        if cols and all(c in cols for c in legacy):
            _log(f"⚠️ fact_media_daily 예상 외 제약조건 감지 | columns={cols}")
            return cols

    _log("⚠️ fact_media_daily PK/UNIQUE 제약조건을 찾지 못해 기본 PK 가정 사용")
    return expected


def _prepare_media_fact_rows_for_conflict(df: pd.DataFrame, conflict_cols: List[str]) -> pd.DataFrame:
    expected = ["dt", "customer_id", "campaign_type", "media_name", "region_name", "device_name"]
    legacy = ["dt", "customer_id", "campaign_type", "media_name", "region_name"]
    numeric_cols = ["imp", "clk", "cost", "conv", "sales"]

    if "device_name" not in df.columns:
        df["device_name"] = "전체"
    df["device_name"] = df["device_name"].map(lambda x: str(x).strip() if x is not None else "").replace("", "전체")

    if conflict_cols == expected:
        return df.drop_duplicates(subset=conflict_cols, keep="last").sort_values(by=conflict_cols)

    if conflict_cols == legacy:
        _log("⚠️ fact_media_daily가 구 PK(device_name 제외) 스키마입니다. device_name을 '전체'로 병합해 임시 적재합니다. 스키마 마이그레이션 후 백필이 필요합니다.")
        work = df.copy()
        work["device_name"] = "전체"
        if "data_source" in work.columns:
            work["data_source"] = "legacy_pk_schema_aggregated"
        if "source_report" in work.columns:
            work["source_report"] = work["source_report"].fillna("AD").replace("", "AD")

        agg_spec: Dict[str, Any] = {}
        for col in work.columns:
            if col in legacy:
                continue
            if col in numeric_cols:
                agg_spec[col] = "sum"
            else:
                agg_spec[col] = "last"
        grouped = work.groupby(legacy, dropna=False, as_index=False).agg(agg_spec)
        ordered = legacy + [c for c in work.columns if c not in legacy]
        grouped = grouped[ordered]
        return grouped.sort_values(by=legacy)

    _log(f"⚠️ fact_media_daily 예상 외 충돌키 사용 | {conflict_cols}")
    use_cols = [c for c in conflict_cols if c in df.columns]
    if not use_cols:
        use_cols = expected
    return df.drop_duplicates(subset=use_cols, keep="last").sort_values(by=use_cols)


def replace_media_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1, scoped_campaign_types: List[str] | None = None):
    table = "fact_media_daily"
    pk_cols = _get_fact_media_daily_conflict_cols(engine)
    input_rows = len(rows or [])
    rows = _filter_nonzero_media_rows(rows or [])
    dropped_zero_rows = max(0, input_rows - len(rows))
    if dropped_zero_rows:
        _log(f"ℹ️ fact_media_daily 0성과 행 제외 | cid={customer_id} dt={d1} dropped={dropped_zero_rows} kept={len(rows)}")
    last_delete_err: Exception | None = None
    delete_sql = text(
        f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt" + (" AND campaign_type = ANY(:types)" if scoped_campaign_types else "")
    )

    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(delete_sql, {"cid": str(customer_id), "dt": d1, "types": scoped_campaign_types or []})
            last_delete_err = None
            break
        except Exception as e:
            last_delete_err = e
            _log(f"⚠️ fact_media_daily 삭제 실패 {attempt}/3 | cid={customer_id} dt={d1} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
    if last_delete_err is not None:
        raise RuntimeError(f"fact_media_daily 삭제 실패 | cid={customer_id} dt={d1} pk={pk_cols} | {type(last_delete_err).__name__}: {last_delete_err}") from last_delete_err

    if not rows:
        reason = "all_zero_filtered" if input_rows else "empty"
        _log(f"ℹ️ fact_media_daily 적재 대상 없음 | cid={customer_id} dt={d1} reason={reason} input_rows={input_rows}")
        return 0

    df = pd.DataFrame(rows).astype(object).where(pd.notnull, None)
    df = _prepare_media_fact_rows_for_conflict(df, pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    conflict_cols_sql = ", ".join(pk_cols)
    conflict_clause = (
        f'ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f'ON CONFLICT ({conflict_cols_sql}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_upsert_err: Exception | None = None
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            _log(f"✅ fact_media_daily 적재 완료 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols}")
            return len(df)
        except Exception as e:
            last_upsert_err = e
            if raw_conn:
                _safe_rollback(raw_conn, ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            _log(f"⚠️ fact_media_daily 적재 실패 {attempt}/3 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
        finally:
            _safe_close(cur, label="cursor", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            _safe_close(raw_conn, label="connection", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
    raise RuntimeError(f"fact_media_daily 적재 최종 실패 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(last_upsert_err).__name__}: {last_upsert_err}") from last_upsert_err
