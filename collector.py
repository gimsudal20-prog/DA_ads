# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import hmac
import base64
import hashlib
import argparse
import sys
import io
import random
import re
import csv
import threading
import concurrent.futures
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from device_collector_helpers import (
    DEVICE_PARSER_VERSION,
    ensure_device_tables,
    build_ad_to_campaign_map,
    parse_ad_device_report,
    save_device_stats,
    summarize_stat_res,
)

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None

load_dotenv(override=False)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False
SKIP_KEYWORD_STATS = False  
SKIP_AD_STATS = False       
FAST_MODE = False

CART_ENABLE_DATE = date(2026, 3, 11)
SHOPPING_HINT_KEYS = ('shopping', '쇼핑', 'product', 'productcatalog', 'catalog', 'shop')

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

DEBUG_DIR = Path(os.getenv("DEBUG_REPORT_DIR", "debug_reports"))

def save_debug_report(tp: str, customer_id: str, job_id: str, content: str):
    try:
        if FAST_MODE:
            return
        if not os.getenv("DEBUG_REPORTS", "1") in ["1", "true", "TRUE", "yes", "YES"]:
            return
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = DEBUG_DIR / f"{ts}_{customer_id}_{tp}_{job_id}.txt"
        fname.write_text(content or "", encoding="utf-8")
    except Exception:
        pass

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def now_millis() -> str: return str(int(time.time() * 1000))

def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")

def make_headers(method: str, path: str, customer_id: str) -> Dict[str, str]:
    ts = now_millis()
    sig = sign_path_only(method.upper(), path, ts, API_SECRET)
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sig,
    }

def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    max_retries = 8
    session = get_session()
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error: raise requests.HTTPError(f"403 Forbidden: 권한이 없습니다 ({customer_id})", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt + random.uniform(0.1, 1.5))
                continue
            data = None
            try: data = r.json()
            except Exception: data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            time.sleep(2 + attempt)
    if raise_error: raise Exception(f"최대 재시도 초과: {url}")
    return 0, None

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception:
        return False, None

def lock_key_for_job(customer_id: str, target_date: date, scope: str = "collector_daily") -> int:
    raw = f"{scope}:{str(customer_id).strip()}:{target_date.isoformat()}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big", signed=False) & 0x7FFFFFFFFFFFFFFF

def acquire_job_lock(engine: Engine, customer_id: str, target_date: date):
    if not DB_URL or not str(DB_URL).lower().startswith(("postgresql", "postgres://")):
        return None
    raw_conn = None
    cur = None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        lk = lock_key_for_job(customer_id, target_date)
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lk,))
        row = cur.fetchone()
        locked = bool(row[0]) if row else False
        if not locked:
            try:
                cur.close()
            except Exception:
                pass
            try:
                raw_conn.close()
            except Exception:
                pass
            return False
        return raw_conn
    except Exception as e:
        log(f"⚠️ 락 획득 실패 - 무락 모드로 진행합니다: {e}")
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if raw_conn:
                raw_conn.close()
        except Exception:
            pass
        return None

def release_job_lock(raw_conn, customer_id: str, target_date: date):
    if raw_conn is None:
        return
    cur = None
    try:
        cur = raw_conn.cursor()
        lk = lock_key_for_job(customer_id, target_date)
        cur.execute("SELECT pg_advisory_unlock(%s)", (lk,))
    except Exception:
        pass
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

def get_engine() -> Engine:
    if not DB_URL: return create_engine("sqlite:///:memory:", future=True)
    db_url = DB_URL
    if "sslmode=" not in db_url: db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url, 
        poolclass=NullPool, 
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=300000"}, 
        future=True
    )

def ensure_column(engine: Engine, table: str, column: str, datatype: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {datatype}"))
    except Exception: pass

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
    if not rows: return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last').sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols]) if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    
    tuples = list(df.itertuples(index=False, name=None))
    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception: pass
            time.sleep(3)
            if attempt == 2: log(f"⚠️ DB 적재 에러 (테이블: {table}): {e}")
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if raw_conn:
                try: raw_conn.close()
                except Exception: pass

def clear_fact_range(engine: Engine, table: str, customer_id: str, d1: date):
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            return
        except Exception:
            time.sleep(3)

def clear_fact_scope(engine: Engine, table: str, customer_id: str, d1: date, pk: str, ids: List[str]):
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f'DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt AND {pk} = ANY(:ids)'),
                    {"cid": str(customer_id), "dt": d1, "ids": ids},
                )
            return
        except Exception:
            time.sleep(3)

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last').sort_values(by=['dt', 'customer_id', pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ['dt', 'customer_id', pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])
    
    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception: pass
            time.sleep(3)
            if attempt == 2: log(f"⚠️ DB 적재 에러 (테이블: {table}): {e}")
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if raw_conn:
                try: raw_conn.close()
                except Exception: pass

def replace_query_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    table = "fact_shopping_query_daily"
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk_cols = ['dt', 'customer_id', 'adgroup_id', 'ad_id', 'query_text']
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last').sort_values(by=pk_cols).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    
    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception: pass
            time.sleep(3)
            if attempt == 2: log(f"⚠️ DB 적재 에러 (테이블: {table}): {e}")
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if raw_conn:
                try: raw_conn.close()
                except Exception: pass

def replace_fact_scope(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date, pk: str, ids: List[str]):
    clear_fact_scope(engine, table, customer_id, d1, pk, ids)
    if not rows:
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last').sort_values(by=['dt', 'customer_id', pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ['dt', 'customer_id', pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception: pass
            time.sleep(3)
            if attempt == 2: log(f"⚠️ DB 적재 에러 (테이블: {table}): {e}")
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if raw_conn:
                try: raw_conn.close()
                except Exception: pass


def filter_stat_result(stat_res: dict, allowed_ids: set[str] | None) -> dict:
    if not stat_res or not allowed_ids:
        return stat_res or {}
    allowed = {str(x).strip() for x in allowed_ids if str(x).strip()}
    return {str(k): v for k, v in (stat_res or {}).items() if str(k).strip() in allowed}


def list_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id)
    return data if ok and isinstance(data, list) else []

def list_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": campaign_id})
    return data if ok and isinstance(data, list) else []

def list_keywords(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/keywords", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []

def extract_keyword_text_from_obj(k: dict) -> str:
    return str(
        k.get("keyword")
        or k.get("relKeyword")
        or k.get("keywordPlus")
        or k.get("relKeywordPlus")
        or k.get("keywordName")
        or k.get("name")
        or k.get("userKeyword")
        or k.get("searchKeyword")
        or ""
    ).strip()

def make_live_keyword_resolver(customer_id: str):
    cache: dict[str, dict] = {}

    def _build(adgroup_id: str):
        gid = str(adgroup_id or "").strip()
        if not gid:
            return {"exact": {}, "rows": []}
        if gid in cache:
            return cache[gid]
        exact = {}
        rows = []
        try:
            kws = list_keywords(customer_id, gid)
            for k in kws or []:
                kid = str(k.get("nccKeywordId") or k.get("keywordId") or "").strip()
                kw = extract_keyword_text_from_obj(k)
                if not kid or not kw:
                    continue
                kw_l = kw.lower()
                kw_n = normalize_keyword_text(kw)
                exact[(gid, kw)] = kid
                exact[(gid, kw_l)] = kid
                exact[(gid, kw_n)] = kid
                rows.append((kw_n, kid))
        except Exception:
            pass
        cache[gid] = {"exact": exact, "rows": rows}
        return cache[gid]

    def resolve(adgroup_id: str, keyword_text: str) -> str:
        gid = str(adgroup_id or "").strip()
        kw = str(keyword_text or "").strip()
        if not gid or not kw or kw == '-':
            return ""
        built = _build(gid)
        kw_l = kw.lower()
        kw_n = normalize_keyword_text(kw)
        kid = built["exact"].get((gid, kw)) or built["exact"].get((gid, kw_l)) or built["exact"].get((gid, kw_n))
        if kid:
            return kid
        cands = keyword_text_candidates(kw_n, built.get("rows", []))
        return cands[0] if len(cands) == 1 else ""

    return resolve

def list_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": adgroup_id})
    if ok and isinstance(data, list) and data: return data
    ok_owner, data_owner = safe_call("GET", "/ncc/ads", customer_id, {"ownerId": adgroup_id})
    if ok_owner and isinstance(data_owner, list): return data_owner
    return data if ok and isinstance(data, list) else []

def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad", {})
    image_url, title, desc = "", "", ""
    vd = ad_inner.get("valData")
    val_data = {}
    if isinstance(vd, str):
        try: val_data = json.loads(vd)
        except: pass
    elif isinstance(vd, dict): val_data = vd
        
    if val_data:
        title = title or val_data.get("customProductName") or val_data.get("productName") or val_data.get("title") or ""
        image_url = image_url or val_data.get("imageUrl") or val_data.get("image") or ""
        
    sp = ad_inner.get("shoppingProduct")
    sp_data = {}
    if isinstance(sp, str):
        try: sp_data = json.loads(sp)
        except: pass
    elif isinstance(sp, dict): sp_data = sp
        
    if sp_data:
        title = title or sp_data.get("name") or sp_data.get("productName") or ""
        image_url = image_url or sp_data.get("imageUrl") or ""

    if not image_url: image_url = ad_inner.get("image", {}).get("imageUrl", "") if isinstance(ad_inner.get("image"), dict) else ""
    if not image_url: image_url = ad_inner.get("imageUrl") or ad_inner.get("mobileImageUrl") or ad_inner.get("pcImageUrl") or ""

    title = title or ad_inner.get("headline") or ad_inner.get("title") or ""
    desc = ad_inner.get("description") or ad_inner.get("desc") or ad_inner.get("addPromoText") or ""
    
    pc_url = ad_inner.get("pcLandingUrl") or ad_obj.get("pcLandingUrl") or ""
    m_url = ad_inner.get("mobileLandingUrl") or ad_obj.get("mobileLandingUrl") or ""
    
    creative_text = f"{title} | {desc}".strip(" |")
    if pc_url: creative_text += f" | {pc_url}"
    
    return {"ad_title": str(title)[:200], "ad_desc": str(desc)[:200], "pc_landing_url": str(pc_url)[:500], "mobile_landing_url": str(m_url)[:500], "creative_text": str(creative_text)[:500], "image_url": str(image_url)[:1000]}

def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out = []
    d_str = d1.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt", "avgRnk"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    chunks = [ids[i:i+50] for i in range(0, len(ids), 50)]
    
    def fetch_chunk(chunk):
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data: return data["data"]
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, max(1, len(chunks)))) as executor:
        results = executor.map(fetch_chunk, chunks)
        for res in results: out.extend(res)
    return out

def fetch_stats_fallback(engine: Engine, customer_id: str, target_date: date, ids: List[str], id_key: str, table_name: str, split_map: dict | None = None, scoped_replace: bool = False) -> int:
    if not ids:
        if not scoped_replace:
            clear_fact_range(engine, table_name, customer_id, target_date)
        return 0

    raw_stats = get_stats_range(customer_id, ids, target_date)
    rows = []
    for r in raw_stats or []:
        obj_id = str(r.get("id") or "").strip()
        if not obj_id:
            continue

        imp = int(r.get("impCnt", 0) or 0)
        clk = int(r.get("clkCnt", 0) or 0)
        cost = int(float(r.get("salesAmt", 0) or 0))
        total_conv = float(r.get("ccnt", 0) or 0)
        total_sales = int(float(r.get("convAmt", 0) or 0))

        if imp == 0 and clk == 0 and cost == 0 and total_conv == 0 and total_sales == 0:
            continue

        split = split_map.get(obj_id) if split_map else None
        purchase_conv = split.get("purchase_conv", 0.0) if split else None
        purchase_sales = split.get("purchase_sales", 0) if split else None
        cart_conv = split.get("cart_conv", 0.0) if split else None
        cart_sales = split.get("cart_sales", 0) if split else None
        wishlist_conv = split.get("wishlist_conv", 0.0) if split else None
        wishlist_sales = split.get("wishlist_sales", 0) if split else None

        total_roas = (total_sales / cost * 100.0) if cost > 0 else 0.0
        purchase_roas = None if purchase_sales is None or cost <= 0 else (purchase_sales / cost * 100.0)
        cart_roas = None if cart_sales is None or cost <= 0 else (cart_sales / cost * 100.0)
        wishlist_roas = None if wishlist_sales is None or cost <= 0 else (wishlist_sales / cost * 100.0)

        row = {
            "dt": target_date,
            "customer_id": str(customer_id),
            id_key: obj_id,
            "imp": imp,
            "clk": clk,
            "cost": cost,
            "conv": total_conv,
            "sales": total_sales,
            "roas": total_roas,
            "purchase_conv": purchase_conv,
            "purchase_sales": purchase_sales,
            "purchase_roas": purchase_roas,
            "cart_conv": cart_conv,
            "cart_sales": cart_sales,
            "cart_roas": cart_roas,
            "wishlist_conv": wishlist_conv,
            "wishlist_sales": wishlist_sales,
            "wishlist_roas": wishlist_roas,
            "split_available": bool(split),
            "data_source": "stats_total_plus_split" if split else "stats_total_only",
        }
        if id_key in ["campaign_id", "keyword_id", "ad_id"]:
            row["avg_rnk"] = float(r.get("avgRnk", 0) or 0)
        rows.append(row)

    pk_name = id_key
    if scoped_replace:
        replace_fact_scope(engine, table_name, rows, customer_id, target_date, pk_name, ids)
    else:
        replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def cleanup_ghost_reports(customer_id: str):
    status, data = request_json("GET", "/stat-reports", customer_id, raise_error=False)
    if status == 200 and isinstance(data, list):
        for job in data:
            if job_id := job.get("reportJobId"):
                safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

def resolve_download_url(dl_url: str) -> str:
    if not dl_url:
        return ""
    dl_url = str(dl_url).strip()
    if dl_url.startswith("http://") or dl_url.startswith("https://"):
        return dl_url
    if dl_url.startswith("/"):
        return BASE_URL + dl_url
    return f"{BASE_URL}/{dl_url.lstrip('/')}"

def parse_report_text_to_df(txt: str) -> pd.DataFrame:
    txt = txt.strip()
    if not txt:
        return pd.DataFrame()
    sep = '\t' if '\t' in txt else ','
    return pd.read_csv(io.StringIO(txt), sep=sep, header=None, dtype=str, on_bad_lines='skip')

def download_report_dataframe(customer_id: str, tp: str, job_id: str, initial_url: str) -> pd.DataFrame | None:
    session = get_session()
    current_url = initial_url
    last_error = ""

    for retry in range(3):
        url = resolve_download_url(current_url)
        try:
            r = session.get(url, timeout=60, allow_redirects=True)
            if r.status_code == 200:
                r.encoding = "utf-8"
                save_debug_report(tp, customer_id, job_id, r.text)
                return parse_report_text_to_df(r.text)

            last_error = f"plain HTTP {r.status_code}"

            parsed = urlparse(url)
            if url.startswith(BASE_URL):
                auth_headers = make_headers("GET", parsed.path or "/", customer_id)
                r2 = session.get(url, headers=auth_headers, timeout=60, allow_redirects=True)
                if r2.status_code == 200:
                    r2.encoding = "utf-8"
                    save_debug_report(tp, customer_id, job_id, r2.text)
                    return parse_report_text_to_df(r2.text)
                last_error = f"plain HTTP {r.status_code} / auth HTTP {r2.status_code}"

            s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
            if s_status == 200 and isinstance(s_data, dict) and s_data.get("downloadUrl"):
                current_url = s_data.get("downloadUrl")

            log(f"⚠️ [{tp}] 대용량 리포트 다운로드 실패 {last_error} (재시도 {retry+1}/3)")
            time.sleep(2)
        except Exception as e:
            last_error = str(e)
            log(f"⚠️ [{tp}] 대용량 리포트 처리 중 에러: {e} (재시도 {retry+1}/3)")
            time.sleep(2)

    log(f"⚠️ [{tp}] 다운로드 최종 실패: {last_error}")
    return None

def fetch_multiple_stat_reports(customer_id: str, report_types: List[str], target_date: date) -> Dict[str, pd.DataFrame | None]:
    cleanup_ghost_reports(customer_id)
    results = {tp: None for tp in report_types}

    for i in range(0, len(report_types), 3):
        batch = report_types[i:i+3]
        jobs = {}

        for tp in batch:
            time.sleep(random.uniform(0.1, 0.3) if FAST_MODE else random.uniform(0.5, 1.5))
            payload = {"reportTp": tp, "statDt": target_date.strftime("%Y%m%d")}
            status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
            if status == 200 and data and "reportJobId" in data:
                jobs[tp] = data["reportJobId"]
            else:
                log(f"⚠️ [{tp}] 대용량 리포트 요청 실패: HTTP {status} - {data}")

        max_wait = 120
        while jobs and max_wait > 0:
            for tp, job_id in list(jobs.items()):
                s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
                if s_status == 200 and s_data:
                    stt = s_data.get("status")
                    if stt == "BUILT":
                        dl_url = s_data.get("downloadUrl")
                        if dl_url:
                            results[tp] = download_report_dataframe(customer_id, tp, job_id, dl_url)
                        else:
                            log(f"⚠️ [{tp}] BUILT 상태지만 downloadUrl 이 없습니다.")
                            results[tp] = None
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
                    elif stt in ["NONE", "ERROR"]:
                        if stt == "ERROR":
                            log(f"⚠️ [{tp}] 네이버 API 내부 리포트 생성 ERROR 발생")
                        results[tp] = pd.DataFrame() if stt == "NONE" else None
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
            if jobs:
                time.sleep(0.5 if FAST_MODE else 1.0)
            max_wait -= 1

        for job_id in jobs.values():
            safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

    return results

def normalize_header(v: str) -> str:
    return str(v).lower().replace(" ", "").replace("_", "").replace("-", "").replace('"', '').replace("'", "")

def normalize_keyword_text(v: str) -> str:
    s = str(v or "").strip().lower()
    if not s or s == "-":
        return ""
    out = []
    for ch in s:
        if ch.isalnum() or ('가' <= ch <= '힣'):
            out.append(ch)
    return "".join(out)

def extract_prefixed_token(vals, prefix: str) -> str:
    prefix_l = str(prefix).lower()
    p = re.compile(rf"\b{re.escape(prefix_l)}[a-z0-9-]+", re.I)
    for v in vals:
        s = str(v).strip()
        if s.lower().startswith(prefix_l):
            return s
        m = p.search(s)
        if m:
            return m.group(0)
    return ""

def keyword_text_candidates(kw_norm: str, rows: list[tuple[str, str]]) -> list[str]:
    if not kw_norm:
        return []
    hits = []
    for db_norm, kid in rows:
        if not db_norm or not kid:
            continue
        if db_norm == kw_norm or kw_norm in db_norm or db_norm in kw_norm:
            hits.append(kid)
    seen, out = set(), []
    for kid in hits:
        if kid not in seen:
            seen.add(kid)
            out.append(kid)
    return out

def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [normalize_header(h) for h in headers]
    norm_candidates = [normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h: return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c in h and "그룹" not in h: return i
    return -1

def safe_float(v) -> float:
    if pd.isna(v): return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-": return 0.0
    try: return float(s)
    except Exception: return 0.0

def maybe_swap_imp_clk_indices(data_df: pd.DataFrame, report_tp: str, pk_idx: int, imp_idx: int, clk_idx: int) -> tuple[int, int, dict | None]:
    if data_df is None or data_df.empty or imp_idx == -1 or clk_idx == -1 or "AD" not in report_tp:
        return imp_idx, clk_idx, None

    valid = 0
    bad = 0
    imp_total = 0
    clk_total = 0
    preview = []
    need_idx = max(pk_idx, imp_idx, clk_idx)

    for _, r in data_df.head(80).iterrows():
        if len(r) <= need_idx:
            continue
        obj_id = str(r.iloc[pk_idx]).strip() if pk_idx != -1 and len(r) > pk_idx else ""
        if not obj_id or obj_id == "-" or obj_id.lower() in ["id", "adid", "campaignid", "keywordid"]:
            continue
        imp = int(safe_float(r.iloc[imp_idx])) if len(r) > imp_idx else 0
        clk = int(safe_float(r.iloc[clk_idx])) if len(r) > clk_idx else 0
        if imp == 0 and clk == 0:
            continue
        valid += 1
        imp_total += imp
        clk_total += clk
        if len(preview) < 5:
            preview.append((obj_id, imp, clk))
        if clk > imp:
            bad += 1

    if valid < 5:
        return imp_idx, clk_idx, None

    should_swap = bad >= max(3, int(valid * 0.6)) or (imp_total > 0 and clk_total > imp_total)
    if should_swap:
        return clk_idx, imp_idx, {"valid": valid, "bad": bad, "imp_total": imp_total, "clk_total": clk_total, "preview": preview}

    return imp_idx, clk_idx, None

def split_enabled_for_date(target_date: date) -> bool:
    return target_date >= CART_ENABLE_DATE

def is_shopping_campaign_obj(camp: dict) -> bool:
    hay = " ".join([
        str(camp.get("campaignTp", "")),
        str(camp.get("campaignType", "")),
        str(camp.get("type", "")),
        str(camp.get("name", "")),
    ]).lower()
    return any(k in hay for k in SHOPPING_HINT_KEYS)

def merge_split_maps(*maps: dict) -> dict:
    out = {}
    for mp in maps:
        if not mp:
            continue
        for k, v in mp.items():
            if not k:
                continue
            b = out.setdefault(str(k), {
                "purchase_conv": 0.0,
                "purchase_sales": 0,
                "cart_conv": 0.0,
                "cart_sales": 0,
                "wishlist_conv": 0.0,
                "wishlist_sales": 0,
            })
            b["purchase_conv"] += float(v.get("purchase_conv", 0.0) or 0.0)
            b["purchase_sales"] += int(float(v.get("purchase_sales", 0) or 0))
            b["cart_conv"] += float(v.get("cart_conv", 0.0) or 0.0)
            b["cart_sales"] += int(float(v.get("cart_sales", 0) or 0))
            b["wishlist_conv"] += float(v.get("wishlist_conv", 0.0) or 0.0)
            b["wishlist_sales"] += int(float(v.get("wishlist_sales", 0) or 0))
    return out

def filter_split_map_excluding_ids(split_map: dict, excluded_ids: set[str] | None = None) -> dict:
    if not split_map:
        return {}
    excluded = {str(x).strip() for x in (excluded_ids or set()) if str(x).strip()}
    if not excluded:
        return dict(split_map)
    out = {}
    for k, v in split_map.items():
        ks = str(k).strip()
        if not ks or ks in excluded:
            continue
        out[ks] = v
    return out

def summarize_split_map(split_map: dict) -> dict:
    out = empty_split_summary()
    if not split_map:
        return out
    for v in split_map.values():
        if not isinstance(v, dict):
            continue
        out['purchase_conv'] += float(v.get('purchase_conv', 0.0) or 0.0)
        out['purchase_sales'] += int(float(v.get('purchase_sales', 0) or 0))
        out['cart_conv'] += float(v.get('cart_conv', 0.0) or 0.0)
        out['cart_sales'] += int(float(v.get('cart_sales', 0) or 0))
        out['wishlist_conv'] += float(v.get('wishlist_conv', 0.0) or 0.0)
        out['wishlist_sales'] += int(float(v.get('wishlist_sales', 0) or 0))
    return out

def validate_shopping_split_summary(summary: dict, ad_map: dict) -> tuple[bool, str]:
    if not split_summary_has_values(summary) or not ad_map:
        return True, ''
    map_sum = summarize_split_map(ad_map)
    checks = [
        ('purchase_conv', 0.6),
        ('purchase_sales', 0.15),
        ('cart_conv', 1.5),
        ('cart_sales', 0.20),
        ('wishlist_conv', 1.5),
        ('wishlist_sales', 0.20),
    ]
    mismatches = []
    for key, ratio_tol in checks:
        s_val = float(summary.get(key, 0) or 0)
        m_val = float(map_sum.get(key, 0) or 0)
        if s_val <= 0 and m_val <= 0:
            continue
        diff = abs(s_val - m_val)
        base = max(abs(s_val), abs(m_val), 1.0)
        if diff / base > ratio_tol:
            mismatches.append(f"{key} summary={s_val} ad_map={m_val}")
    return (len(mismatches) == 0, '; '.join(mismatches))

def empty_split_summary() -> dict:
    return {
        "purchase_conv": 0.0,
        "purchase_sales": 0,
        "cart_conv": 0.0,
        "cart_sales": 0,
        "wishlist_conv": 0.0,
        "wishlist_sales": 0,
    }

def add_split_summary(summary: dict, is_purchase: bool, is_cart: bool, is_wishlist: bool, c_val: float, s_val: int):
    if is_purchase:
        summary["purchase_conv"] += float(c_val or 0.0)
        summary["purchase_sales"] += int(s_val or 0)
    elif is_cart:
        summary["cart_conv"] += float(c_val or 0.0)
        summary["cart_sales"] += int(s_val or 0)
    elif is_wishlist:
        summary["wishlist_conv"] += float(c_val or 0.0)
        summary["wishlist_sales"] += int(s_val or 0)

def merge_split_summaries(*summaries: dict) -> dict:
    out = empty_split_summary()
    for s in summaries:
        if not s:
            continue
        out["purchase_conv"] += float(s.get("purchase_conv", 0.0) or 0.0)
        out["purchase_sales"] += int(float(s.get("purchase_sales", 0) or 0))
        out["cart_conv"] += float(s.get("cart_conv", 0.0) or 0.0)
        out["cart_sales"] += int(float(s.get("cart_sales", 0) or 0))
        out["wishlist_conv"] += float(s.get("wishlist_conv", 0.0) or 0.0)
        out["wishlist_sales"] += int(float(s.get("wishlist_sales", 0) or 0))
    return out

def split_summary_has_values(summary: dict) -> bool:
    if not summary:
        return False
    return any(float(summary.get(k, 0) or 0) > 0 for k in ["purchase_conv", "cart_conv", "wishlist_conv"])

def format_split_summary(summary: dict) -> str:
    def fmt(v):
        try:
            fv = float(v or 0)
            return str(int(fv)) if fv.is_integer() else f"{fv:.2f}".rstrip('0').rstrip('.')
        except Exception:
            return str(v)
    return (
        f"구매완료 {fmt(summary.get('purchase_conv', 0))}건 | "
        f"장바구니 {fmt(summary.get('cart_conv', 0))}건 | "
        f"위시리스트 {fmt(summary.get('wishlist_conv', 0))}건"
    )

def process_conversion_report(df: pd.DataFrame, allowed_campaign_ids: set[str] | None = None, report_hint: str = "", keyword_lookup: dict | None = None, keyword_unique_lookup: dict | None = None, live_keyword_resolver=None, debug_account_name: str = "", debug_target_date: str = "") -> Tuple[dict, dict, dict, dict]:
    camp_map, kw_map, ad_map = {}, {}, {}
    summary = empty_split_summary()
    debug_rows = []
    allowed_campaign_ids = set(str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip())
    keyword_lookup = keyword_lookup or {}
    keyword_unique_lookup = keyword_unique_lookup or {}
    if df is None or df.empty:
        return camp_map, kw_map, ad_map, summary

    def ensure_split_bucket(m_dict: dict, obj_id: str):
        if obj_id not in m_dict:
            m_dict[obj_id] = {
                "purchase_conv": 0.0,
                "purchase_sales": 0,
                "cart_conv": 0.0,
                "cart_sales": 0,
                "wishlist_conv": 0.0,
                "wishlist_sales": 0,
            }

    def apply_row(m_dict: dict, obj_id: str, is_purchase: bool, is_cart: bool, is_wishlist: bool, c_val: float, s_val: int):
        obj_id = str(obj_id).strip()
        if not obj_id or obj_id == "-":
            return
        ensure_split_bucket(m_dict, obj_id)
        if is_purchase:
            m_dict[obj_id]["purchase_conv"] += c_val
            m_dict[obj_id]["purchase_sales"] += s_val
        elif is_cart:
            m_dict[obj_id]["cart_conv"] += c_val
            m_dict[obj_id]["cart_sales"] += s_val
        elif is_wishlist:
            m_dict[obj_id]["wishlist_conv"] += c_val
            m_dict[obj_id]["wishlist_sales"] += s_val

    def classify_conversion_value(v) -> tuple[bool, bool, bool]:
        ctype = str(v).strip().lower()
        ctype_norm = ctype.replace('_', '').replace('-', '').replace(' ', '')
        is_purchase = (
            '구매완료' in ctype_norm or ctype_norm == '구매' or ctype_norm in {'1', 'purchase', 'purchasing'}
        )
        is_cart = (
            '장바구니담기' in ctype_norm or '장바구니' in ctype_norm or ctype_norm in {'3', 'cart', 'addtocart', 'addtocarts'}
        )
        is_wishlist = (
            '위시리스트추가' in ctype_norm or '위시리스트' in ctype_norm or '상품찜' in ctype_norm or ctype_norm in {'wishlist', 'addtowishlist', 'wishlistadd', 'wish'}
        )
        return is_purchase, is_cart, is_wishlist

    def maybe_numeric(v: str) -> float | None:
        s = str(v).strip().replace(',', '')
        if not s or s == '-':
            return None
        if re.fullmatch(r'-?\d+(?:\.\d+)?', s):
            try:
                return float(s)
            except Exception:
                return None
        return None

    def looks_like_id(v: str) -> bool:
        s = str(v).strip().lower()
        return s.startswith(('cmp-', 'grp-', 'nkw-', 'nad-', 'bsn-'))

    def row_allowed(row_campaign_id: str | None) -> bool:
        if not allowed_campaign_ids:
            return True
        row_campaign_id = str(row_campaign_id or "").strip()
        return bool(row_campaign_id) and row_campaign_id in allowed_campaign_ids

    def add_debug_row(vals, parsed_type, c_val, s_val, kept, reason, row_cid="", row_gid="", row_kid="", row_adid="", kw_text="", kw_obj_id=""):
        debug_rows.append({
            "report_tp": report_hint,
            "date": str(debug_target_date or ""),
            "account_name": str(debug_account_name or ""),
            "campaign_id": str(row_cid or ""),
            "adgroup_id": str(row_gid or ""),
            "keyword_id": str(row_kid or ""),
            "keyword_text": str(kw_text or ""),
            "keyword_mapped_id": str(kw_obj_id or ""),
            "ad_id": str(row_adid or ""),
            "parsed_type": str(parsed_type or ""),
            "parsed_count": c_val,
            "parsed_sales": s_val,
            "kept": 1 if kept else 0,
            "reason": reason,
            "row": " | ".join([str(x) for x in vals]),
        })

    def flush_debug_rows():
        if FAST_MODE or not debug_rows or not debug_account_name or not debug_target_date:
            return
        dbg_dir = os.path.join(os.getcwd(), "debug_split_rows")
        os.makedirs(dbg_dir, exist_ok=True)
        safe_name = re.sub(r'[^0-9A-Za-z가-힣._-]+', '_', str(debug_account_name))
        out_path = os.path.join(dbg_dir, f"{debug_target_date}_{safe_name}_{report_hint}.csv")
        fields = [
            "report_tp","date","account_name","campaign_id","adgroup_id","keyword_id","keyword_text","keyword_mapped_id","ad_id",
            "parsed_type","parsed_count","parsed_sales","kept","reason","row"
        ]
        with open(out_path, 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(debug_rows)

    def guess_campaign_id_from_row(vals: list[str]) -> str:
        for v in vals:
            s = str(v).strip().lower()
            if s.startswith('cmp-'):
                return str(v).strip()
        return ""

    def first_value_with_prefix(vals: list[str], prefix: str) -> str:
        for v in vals:
            s = str(v).strip()
            if s.lower().startswith(prefix):
                return s
        return ""

    def value_from_idx_or_scan(vals: list[str], idx: int, prefix: str, allow_dash: bool = False) -> str:
        if 0 <= idx < len(vals):
            v = str(vals[idx]).strip()
            if v.lower().startswith(prefix):
                return v
            if allow_dash and v == '-':
                return v
        return first_value_with_prefix(vals, prefix)

    header_idx = -1
    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if (
            'conversiontype' in row_vals or '전환유형' in row_vals or 'convtp' in row_vals or
            '총전환수' in row_vals or 'conversioncount' in row_vals
        ):
            header_idx = i
            break

    if header_idx != -1:
        headers = [normalize_header(str(x)) for x in df.iloc[header_idx].fillna("")]
        cid_idx = get_col_idx(headers, ['캠페인id', 'campaignid', 'ncccampaignid'])
        kid_idx = get_col_idx(headers, ['키워드id', 'keywordid', 'ncckeywordid'])
        adid_idx = get_col_idx(headers, ['광고id', '소재id', 'adid', 'nccadid'])
        type_idx = get_col_idx(headers, ['전환유형', 'conversiontype', 'convtp'])
        cnt_idx = get_col_idx(headers, ['총전환수', '전환수', 'conversions', 'conversioncount', 'ccnt'])
        sales_idx = get_col_idx(headers, ['총전환매출액(원)', '전환매출액', 'conversionvalue', 'sales', 'salesbyconversion', 'convamt'])

        if type_idx != -1 and cnt_idx != -1:
            data_df = df.iloc[header_idx + 1:]
            for _, r in data_df.iterrows():
                if len(r) <= max(type_idx, cnt_idx, sales_idx if sales_idx != -1 else -1):
                    continue
                row_campaign_id = r.iloc[cid_idx] if cid_idx != -1 and len(r) > cid_idx else ''
                if not row_allowed(row_campaign_id):
                    add_debug_row([str(x) for x in r.tolist()], "", 0, 0, False, "campaign_filtered_header")
                    continue
                is_purchase, is_cart, is_wishlist = classify_conversion_value(r.iloc[type_idx])
                if not (is_purchase or is_cart or is_wishlist):
                    continue
                c_val = safe_float(r.iloc[cnt_idx])
                s_val = int(safe_float(r.iloc[sales_idx])) if sales_idx != -1 else 0
                add_split_summary(summary, is_purchase, is_cart, is_wishlist, c_val, s_val)
                add_debug_row([str(x) for x in r.tolist()], "purchase" if is_purchase else ("cart" if is_cart else "wishlist"), c_val, s_val, True, "header_keep")
                if cid_idx != -1 and len(r) > cid_idx:
                    apply_row(camp_map, r.iloc[cid_idx], is_purchase, is_cart, is_wishlist, c_val, s_val)
                if kid_idx != -1 and len(r) > kid_idx:
                    apply_row(kw_map, r.iloc[kid_idx], is_purchase, is_cart, is_wishlist, c_val, s_val)
                if adid_idx != -1 and len(r) > adid_idx:
                    apply_row(ad_map, r.iloc[adid_idx], is_purchase, is_cart, is_wishlist, c_val, s_val)

            if camp_map or kw_map or ad_map:
                flush_debug_rows()
                return camp_map, kw_map, ad_map, summary

    sample_rows = [df.iloc[i].fillna("") for i in range(min(20, len(df)))]

    def best_prefixed_idx(sample_rows, target_prefix: str, allow_dash: bool = False, preferred_after: int = -1) -> int:
        max_cols = max((len(r) for r in sample_rows), default=0)
        best_idx, best_score, best_prefix_hits = -1, -1, 0
        for i in range(max_cols):
            score = 0
            prefix_hits = 0
            dash_hits = 0
            for r in sample_rows:
                if len(r) <= i:
                    continue
                v = str(r.iloc[i]).strip().lower()
                if v.startswith(target_prefix):
                    score += 5
                    prefix_hits += 1
                elif allow_dash and v == '-':
                    dash_hits += 1
            if prefix_hits > 0:
                score += min(dash_hits, prefix_hits)
            if preferred_after >= 0 and i <= preferred_after:
                score -= 2
            if prefix_hits > best_prefix_hits or (prefix_hits == best_prefix_hits and score > best_score):
                best_idx, best_score, best_prefix_hits = i, score, prefix_hits
        return best_idx if best_prefix_hits > 0 else -1

    cid_idx = best_prefixed_idx(sample_rows, 'cmp-')
    gid_idx = best_prefixed_idx(sample_rows, 'grp-', preferred_after=cid_idx)
    kid_idx = best_prefixed_idx(sample_rows, 'nkw-', allow_dash=True, preferred_after=max(cid_idx, gid_idx))
    adid_idx = best_prefixed_idx(sample_rows, 'nad-', preferred_after=max(cid_idx, gid_idx, kid_idx))

    kw_text_idx = -1
    if report_hint.upper() == 'SHOPPINGKEYWORD_CONVERSION_DETAIL':
        candidate = gid_idx + 1 if gid_idx != -1 else -1
        max_cols = max((len(r) for r in sample_rows), default=0)
        if 0 <= candidate < max_cols:
            text_score = 0
            for r in sample_rows:
                if len(r) <= candidate:
                    continue
                v = str(r.iloc[candidate]).strip()
                if v and v != '-' and not looks_like_id(v) and maybe_numeric(v) is None:
                    text_score += 1
            if text_score > 0:
                kw_text_idx = candidate

    for _, r in df.iterrows():
        vals = ["" if pd.isna(x) else str(x).strip() for x in r.tolist()]
        n = len(vals)
        if n < 2:
            continue

        text_type_hits = []
        numeric_type_hits = []
        for idx, v in enumerate(vals):
            s_raw = str(v).strip()
            s = s_raw.lower()
            is_purchase, is_cart, is_wishlist = classify_conversion_value(v)
            if not (is_purchase or is_cart or is_wishlist):
                continue
            if s_raw in {'1', '3'}:
                if idx >= max(0, n - 6):
                    numeric_type_hits.append((idx, is_purchase, is_cart, is_wishlist))
            else:
                text_type_hits.append((idx, is_purchase, is_cart, is_wishlist))

        type_hits = text_type_hits if text_type_hits else numeric_type_hits

        if not type_hits and report_hint.upper() == 'SHOPPINGKEYWORD_CONVERSION_DETAIL':
            pass
        if not type_hits:
            add_debug_row(vals, "", 0, 0, False, "no_type_hit")
            continue

        row_campaign_id = guess_campaign_id_from_row(vals)
        if not row_allowed(row_campaign_id):
            add_debug_row(vals, "", 0, 0, False, "campaign_filtered")
            continue

        picked = None
        for type_idx, is_purchase, is_cart, is_wishlist in type_hits:
            anchor_idx = type_idx
            anchor_is_purchase, anchor_is_cart, anchor_is_wishlist = is_purchase, is_cart, is_wishlist
            raw_tok = str(vals[type_idx]).strip().lower()
            if raw_tok in {'1', '2', '3'} and type_idx + 1 < n:
                n_is_purchase, n_is_cart, n_is_wishlist = classify_conversion_value(vals[type_idx + 1])
                if n_is_purchase or n_is_cart or n_is_wishlist:
                    anchor_idx = type_idx + 1
                    anchor_is_purchase, anchor_is_cart, anchor_is_wishlist = n_is_purchase, n_is_cart, n_is_wishlist

            numeric_right = []
            for j in range(anchor_idx + 1, n):
                vv = vals[j]
                if looks_like_id(vv):
                    continue
                num = maybe_numeric(vv)
                if num is not None:
                    numeric_right.append((j, num))
            if not numeric_right:
                continue

            c_val = float(numeric_right[0][1])
            s_val = int(numeric_right[1][1]) if len(numeric_right) >= 2 else 0

            picked = (anchor_is_purchase, anchor_is_cart, anchor_is_wishlist, c_val, s_val)
            break

        if not picked:
            add_debug_row(vals, "", 0, 0, False, "no_numeric_right")
            continue

        is_purchase, is_cart, is_wishlist, c_val, s_val = picked
        add_split_summary(summary, is_purchase, is_cart, is_wishlist, c_val, s_val)
        row_cid = value_from_idx_or_scan(vals, cid_idx, 'cmp-') or extract_prefixed_token(vals, 'cmp-')
        row_gid = value_from_idx_or_scan(vals, gid_idx, 'grp-') or extract_prefixed_token(vals, 'grp-')
        row_kid = value_from_idx_or_scan(vals, kid_idx, 'nkw-', allow_dash=True)
        if row_kid in {'', '-'}:
            row_kid = extract_prefixed_token(vals, 'nkw-')
        row_adid = value_from_idx_or_scan(vals, adid_idx, 'nad-') or extract_prefixed_token(vals, 'nad-')

        if row_cid:
            apply_row(camp_map, row_cid, is_purchase, is_cart, is_wishlist, c_val, s_val)

        kw_obj_id = ""
        row_kid_s = str(row_kid).strip()
        if row_kid_s not in {"", "-"} and row_kid_s.lower().startswith("nkw-"):
            kw_obj_id = row_kid_s
        elif kw_text_idx != -1 and kw_text_idx < n and row_gid:
            kw_text = str(vals[kw_text_idx]).strip()
            kw_norm = normalize_keyword_text(kw_text)
            kw_obj_id = (
                keyword_lookup.get((row_gid, kw_text), "")
                or keyword_lookup.get((row_gid, kw_text.lower()), "")
                or keyword_lookup.get((row_gid, kw_norm), "")
            )
            if not kw_obj_id:
                group_rows = keyword_lookup.get((row_gid, '__rows__'), [])
                cands = keyword_text_candidates(kw_norm, group_rows)
                if len(cands) == 1:
                    kw_obj_id = cands[0]
            if not kw_obj_id and live_keyword_resolver:
                try:
                    kw_obj_id = live_keyword_resolver(row_gid, kw_text) or ""
                except Exception:
                    kw_obj_id = ""
        if kw_obj_id:
            apply_row(kw_map, kw_obj_id, is_purchase, is_cart, is_wishlist, c_val, s_val)

        if row_adid:
            apply_row(ad_map, row_adid, is_purchase, is_cart, is_wishlist, c_val, s_val)

        add_debug_row(
            vals,
            "purchase" if is_purchase else ("cart" if is_cart else "wishlist"),
            c_val,
            s_val,
            True,
            "keep",
            row_cid=row_cid,
            row_gid=row_gid,
            row_kid=row_kid_s,
            row_adid=row_adid,
            kw_text=(str(vals[kw_text_idx]).strip() if kw_text_idx != -1 and kw_text_idx < n else ""),
            kw_obj_id=kw_obj_id,
        )

    flush_debug_rows()
    return camp_map, kw_map, ad_map, summary

def parse_shopping_query_report(df: pd.DataFrame, target_date: date, customer_id: str) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    rows_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    def classify(v) -> tuple[bool, bool, bool]:
        ctype = str(v).strip().lower()
        ctype_norm = ctype.replace('_', '').replace('-', '').replace(' ', '')
        is_purchase = ('구매완료' in ctype_norm or ctype_norm == '구매' or ctype_norm in {'1', 'purchase', 'purchasing'})
        is_cart = ('장바구니담기' in ctype_norm or '장바구니' in ctype_norm or ctype_norm in {'3', 'cart', 'addtocart', 'addtocarts'})
        is_wishlist = ('위시리스트추가' in ctype_norm or '위시리스트' in ctype_norm or '상품찜' in ctype_norm or ctype_norm in {'wishlist', 'addtowishlist', 'wishlistadd', 'wish'})
        return is_purchase, is_cart, is_wishlist

    sample_rows = [df.iloc[i].fillna("") for i in range(min(20, len(df)))]

    def best_prefixed_idx(sample_rows, target_prefix: str, allow_dash: bool = False, preferred_after: int = -1) -> int:
        max_cols = max((len(r) for r in sample_rows), default=0)
        best_idx, best_score, best_prefix_hits = -1, -1, 0
        for i in range(max_cols):
            score = 0
            prefix_hits = 0
            dash_hits = 0
            for r in sample_rows:
                if len(r) <= i:
                    continue
                v = str(r.iloc[i]).strip().lower()
                if v.startswith(target_prefix):
                    score += 5
                    prefix_hits += 1
                elif allow_dash and v == '-':
                    dash_hits += 1
            if prefix_hits > 0:
                score += min(dash_hits, prefix_hits)
            if preferred_after >= 0 and i <= preferred_after:
                score -= 2
            if prefix_hits > best_prefix_hits or (prefix_hits == best_prefix_hits and score > best_score):
                best_idx, best_score, best_prefix_hits = i, score, prefix_hits
        return best_idx if best_prefix_hits > 0 else -1

    cid_idx = best_prefixed_idx(sample_rows, 'cmp-')
    gid_idx = best_prefixed_idx(sample_rows, 'grp-', preferred_after=cid_idx)
    adid_idx = best_prefixed_idx(sample_rows, 'nad-', preferred_after=max(cid_idx, gid_idx))

    kw_text_idx = -1
    candidate = gid_idx + 1 if gid_idx != -1 else -1
    max_cols = max((len(r) for r in sample_rows), default=0)
    if 0 <= candidate < max_cols:
        text_score = 0
        for r in sample_rows:
            if len(r) <= candidate:
                continue
            v = str(r.iloc[candidate]).strip()
            if v and v != '-' and not v.lower().startswith(('cmp-', 'grp-', 'nkw-', 'nad-', 'bsn-')):
                vv = v.replace(',', '')
                if not re.fullmatch(r'-?\d+(?:\.\d+)?', vv):
                    text_score += 1
        if text_score > 0:
            kw_text_idx = candidate

    for _, r in df.iterrows():
        vals = ["" if pd.isna(x) else str(x).strip() for x in r.tolist()]
        if len(vals) < 2:
            continue

        text_type_hits = []
        numeric_type_hits = []
        n = len(vals)
        for idx, v in enumerate(vals):
            s_raw = str(v).strip()
            is_purchase, is_cart, is_wishlist = classify(v)
            if not (is_purchase or is_cart or is_wishlist):
                continue
            if s_raw in {'1', '3'}:
                if idx >= max(0, n - 6):
                    numeric_type_hits.append((idx, is_purchase, is_cart, is_wishlist))
            else:
                text_type_hits.append((idx, is_purchase, is_cart, is_wishlist))
        type_hits = text_type_hits if text_type_hits else numeric_type_hits
        if not type_hits:
            continue

        anchor_idx, is_purchase, is_cart, is_wishlist = type_hits[-1]
        numeric_right = []
        for j in range(anchor_idx + 1, min(anchor_idx + 4, len(vals))):
            s = str(vals[j]).strip().replace(',', '')
            if re.fullmatch(r'-?\d+(?:\.\d+)?', s):
                try:
                    numeric_right.append((j, float(s)))
                except Exception:
                    pass
        if not numeric_right:
            continue

        c_val = float(numeric_right[0][1])
        s_val = int(numeric_right[1][1]) if len(numeric_right) >= 2 else 0
        row_cid = vals[cid_idx].strip() if 0 <= cid_idx < len(vals) else ""
        row_gid = vals[gid_idx].strip() if 0 <= gid_idx < len(vals) else ""
        row_adid = vals[adid_idx].strip() if 0 <= adid_idx < len(vals) else ""
        query_text = vals[kw_text_idx].strip() if 0 <= kw_text_idx < len(vals) else ""
        if not row_gid or not row_adid or not query_text or query_text == '-':
            continue

        key = (row_cid, row_gid, row_adid, query_text)
        row = rows_map.setdefault(key, {
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": row_cid,
            "adgroup_id": row_gid,
            "ad_id": row_adid,
            "query_text": query_text,
            "total_conv": 0.0,
            "total_sales": 0,
            "purchase_conv": 0.0,
            "purchase_sales": 0,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "split_available": True,
            "data_source": "SHOPPINGKEYWORD_CONVERSION_DETAIL",
        })
        row["total_conv"] += c_val
        row["total_sales"] += s_val
        if is_purchase:
            row["purchase_conv"] += c_val
            row["purchase_sales"] += s_val
        elif is_cart:
            row["cart_conv"] += c_val
            row["cart_sales"] += s_val
        elif is_wishlist:
            row["wishlist_conv"] += c_val
            row["wishlist_sales"] += s_val

    return list(rows_map.values())

def build_keyword_lookup_from_keyword_report(df: pd.DataFrame) -> tuple[dict, dict]:
    lookup = {}
    unique_lookup = {}
    if df is None or df.empty:
        return lookup, unique_lookup

    header_idx = -1
    pk_cands = ["키워드id", "keywordid", "ncckeywordid"]
    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in [normalize_header(x) for x in pk_cands]) or "노출수" in row_vals or "impressions" in row_vals:
            header_idx = i
            break

    if header_idx != -1:
        headers = [normalize_header(str(x)) for x in df.iloc[header_idx].fillna("")]
        data_df = df.iloc[header_idx + 1:]
        kid_idx = get_col_idx(headers, ["키워드id", "keywordid", "ncckeywordid"])
        gid_idx = get_col_idx(headers, ["광고그룹id", "adgroupid", "nccadgroupid"])
        kw_idx = get_col_idx(headers, ["키워드", "keyword", "연관검색어", "relkeyword", "검색어"])
    else:
        data_df = df.iloc[1:] if ("date" in str(df.iloc[0, 0]).lower() or "id" in str(df.iloc[0, 0]).lower()) else df
        gid_idx = 3
        kw_idx = 4
        kid_idx = 5

    rows = []
    text_freq = {}
    group_rows = {}
    for _, r in data_df.iterrows():
        vals = r.fillna("").tolist()
        if len(vals) <= max(kid_idx, gid_idx, kw_idx):
            continue
        kid = str(vals[kid_idx]).strip() if kid_idx != -1 and len(vals) > kid_idx else ""
        gid = str(vals[gid_idx]).strip() if gid_idx != -1 and len(vals) > gid_idx else ""
        kw = str(vals[kw_idx]).strip() if kw_idx != -1 and len(vals) > kw_idx else ""
        if not kid or kid == '-' or not gid or gid == '-' or not kw or kw == '-':
            continue
        kid_s = kid
        gid_s = gid
        kw_s = kw
        kw_l = kw_s.lower()
        kw_n = normalize_keyword_text(kw_s)
        lookup[(gid_s, kw_s)] = kid_s
        lookup[(gid_s, kw_l)] = kid_s
        lookup[(gid_s, kw_n)] = kid_s
        group_rows.setdefault(gid_s, []).append((kw_n, kid_s))
        if kw_n:
            text_freq[kw_n] = text_freq.get(kw_n, 0) + 1
            rows.append((kw_n, kid_s))
    for gid_s, rs in group_rows.items():
        lookup[(gid_s, '__rows__')] = rs
    for kw_n, kid_s in rows:
        if text_freq.get(kw_n) == 1:
            unique_lookup.setdefault(kw_n, []).append(kid_s)
    return lookup, unique_lookup

def parse_base_report(df: pd.DataFrame, report_tp: str, conv_map: dict | None = None, has_conv_report: bool = False) -> dict:
    if df is None or df.empty:
        return {}

    header_idx = -1
    pk_cands = []
    if "CAMPAIGN" in report_tp:
        pk_cands = ["캠페인id", "campaignid"]
    elif "KEYWORD" in report_tp:
        pk_cands = ["키워드id", "keywordid", "ncckeywordid"]
    elif "AD" in report_tp:
        pk_cands = ["광고id", "소재id", "adid"]

    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in [normalize_header(x) for x in pk_cands]) or "노출수" in row_vals or "impressions" in row_vals:
            header_idx = i
            break

    if header_idx != -1:
        headers = [normalize_header(str(x)) for x in df.iloc[header_idx].fillna("")]
        data_df = df.iloc[header_idx + 1:]
        pk_idx = get_col_idx(headers, pk_cands)
        imp_idx = get_col_idx(headers, ["노출수", "impressions", "impcnt"])
        clk_idx = get_col_idx(headers, ["클릭수", "clicks", "clkcnt"])
        cost_idx = get_col_idx(headers, ["총비용", "cost", "salesamt"])
        conv_idx = get_col_idx(headers, ["전환수", "conversions", "ccnt"])
        sales_idx = get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"])
        rank_idx = get_col_idx(headers, ["평균노출순위", "averageposition", "avgrnk"])
    else:
        data_df = df.iloc[1:] if ("date" in str(df.iloc[0, 0]).lower() or "id" in str(df.iloc[0, 0]).lower()) else df
        pk_idx = 2 if "CAMPAIGN" in report_tp else 5
        imp_idx = 5 if "CAMPAIGN" in report_tp else 8
        clk_idx = 6 if "CAMPAIGN" in report_tp else 9
        cost_idx = 7 if "CAMPAIGN" in report_tp else 10
        conv_idx = 8 if "CAMPAIGN" in report_tp else 11
        sales_idx = 9 if "CAMPAIGN" in report_tp else 12
        rank_idx = 11 if "CAMPAIGN" in report_tp else 14

    imp_idx, clk_idx, swap_meta = maybe_swap_imp_clk_indices(data_df, report_tp, pk_idx, imp_idx, clk_idx)
    if swap_meta:
        print(
            f"   ⚠️ {report_tp} 리포트에서 노출/클릭 컬럼 순서가 뒤바뀐 것으로 보여 자동 교정합니다. "
            f"(valid={swap_meta['valid']}, bad={swap_meta['bad']}, imp_total={swap_meta['imp_total']}, clk_total={swap_meta['clk_total']}, preview={swap_meta['preview']})",
            flush=True,
        )

    res = {}
    for _, r in data_df.iterrows():
        if len(r) <= pk_idx:
            continue

        obj_id = str(r.iloc[pk_idx]).strip()
        if not obj_id or obj_id == "-" or obj_id.lower() in ["id", "keywordid", "adid", "campaignid"]:
            continue

        if obj_id not in res:
            res[obj_id] = {
                "imp": 0,
                "clk": 0,
                "cost": 0,
                "conv": 0.0,
                "sales": 0,
                "purchase_conv": 0.0 if has_conv_report else None,
                "purchase_sales": 0 if has_conv_report else None,
                "cart_conv": 0.0 if has_conv_report else None,
                "cart_sales": 0 if has_conv_report else None,
                "wishlist_conv": 0.0 if has_conv_report else None,
                "wishlist_sales": 0 if has_conv_report else None,
                "split_available": bool(has_conv_report),
                "rank_sum": 0.0,
                "rank_cnt": 0,
            }

        imp = int(safe_float(r.iloc[imp_idx])) if imp_idx != -1 and len(r) > imp_idx else 0
        res[obj_id]["imp"] += imp

        if clk_idx != -1 and len(r) > clk_idx:
            res[obj_id]["clk"] += int(safe_float(r.iloc[clk_idx]))
        if cost_idx != -1 and len(r) > cost_idx:
            res[obj_id]["cost"] += int(safe_float(r.iloc[cost_idx]))
        if conv_idx != -1 and len(r) > conv_idx:
            res[obj_id]["conv"] += safe_float(r.iloc[conv_idx])
        if sales_idx != -1 and len(r) > sales_idx:
            res[obj_id]["sales"] += int(safe_float(r.iloc[sales_idx]))

        if rank_idx != -1 and len(r) > rank_idx:
            rnk = safe_float(r.iloc[rank_idx])
            if rnk > 0 and imp > 0:
                res[obj_id]["rank_sum"] += (rnk * imp)
                res[obj_id]["rank_cnt"] += imp

    if has_conv_report and conv_map is not None:
        for obj_id, bucket in res.items():
            split = conv_map.get(obj_id)
            if split:
                bucket["purchase_conv"] = split.get("purchase_conv", 0.0)
                bucket["purchase_sales"] = split.get("purchase_sales", 0)
                bucket["cart_conv"] = split.get("cart_conv", 0.0)
                bucket["cart_sales"] = split.get("cart_sales", 0)
                bucket["wishlist_conv"] = split.get("wishlist_conv", 0.0)
                bucket["wishlist_sales"] = split.get("wishlist_sales", 0)

    return res

def merge_and_save_combined(engine: Engine, customer_id: str, target_date: date, table_name: str, pk_name: str, stat_res: dict, data_source: str, scoped_ids: List[str] | None = None) -> int:
    if not stat_res:
        return 0

    rows = []
    for k, s in stat_res.items():
        cost = s["cost"]
        total_sales = s["sales"]
        total_roas = (total_sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (s.get("rank_sum", 0) / s.get("rank_cnt", 1)) if s.get("rank_cnt", 0) > 0 else 0.0

        purchase_sales = s.get("purchase_sales")
        purchase_roas = None if purchase_sales is None or cost <= 0 else (purchase_sales / cost * 100.0)

        cart_sales = s.get("cart_sales")
        cart_roas = None if cart_sales is None or cost <= 0 else (cart_sales / cost * 100.0)
        wishlist_sales = s.get("wishlist_sales")
        wishlist_roas = None if wishlist_sales is None or cost <= 0 else (wishlist_sales / cost * 100.0)

        primary_conv = s.get("purchase_conv") if s.get("purchase_conv") is not None else s["conv"]
        primary_sales = purchase_sales if purchase_sales is not None else total_sales
        primary_roas = None if primary_sales is None or cost <= 0 else (primary_sales / cost * 100.0)

        row = {
            "dt": target_date,
            "customer_id": str(customer_id),
            pk_name: k,
            "imp": s["imp"],
            "clk": s["clk"],
            "cost": cost,
            "conv": s["conv"],
            "sales": total_sales,
            "roas": total_roas,
            "purchase_conv": s.get("purchase_conv"),
            "purchase_sales": purchase_sales,
            "purchase_roas": purchase_roas,
            "cart_conv": s.get("cart_conv"),
            "cart_sales": cart_sales,
            "cart_roas": cart_roas,
            "wishlist_conv": s.get("wishlist_conv"),
            "wishlist_sales": wishlist_sales,
            "wishlist_roas": wishlist_roas,
            "primary_conv": primary_conv,
            "primary_sales": primary_sales,
            "primary_roas": primary_roas,
            "split_available": s.get("split_available", False),
            "data_source": data_source,
        }
        if pk_name in ["campaign_id", "keyword_id", "ad_id"]:
            row["avg_rnk"] = round(avg_rnk, 2)
        rows.append(row)

    if scoped_ids is not None:
        replace_fact_scope(engine, table_name, rows, customer_id, target_date, pk_name, scoped_ids)
    else:
        replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)



MEDIA_HEADER_CANDIDATES = [
    "매체이름", "매체명", "매체", "노출매체", "지면", "노출지면", "media", "medianame", "mediatype", "placement", "network"
]
REGION_HEADER_CANDIDATES = [
    "지역", "지역명", "노출지역", "시도", "시군구", "행정구역", "region", "regionname", "location"
]
DEVICE_HEADER_CANDIDATES_LOCAL = [
    "pc mobile type", "pc_mobile_type", "pc/mobile type", "pcmobiletype",
    "device", "device_name", "devicename", "platform", "platform type",
    "기기", "디바이스", "노출기기", "노출 기기", "단말기", "플랫폼",
]
AD_HEADER_CANDIDATES_LOCAL = ["광고id", "소재id", "adid"]
IMP_HEADER_CANDIDATES_LOCAL = ["노출수", "impressions", "impcnt"]
CLK_HEADER_CANDIDATES_LOCAL = ["클릭수", "clicks", "clkcnt"]
COST_HEADER_CANDIDATES_LOCAL = ["총비용", "비용", "cost", "salesamt"]
CONV_HEADER_CANDIDATES_LOCAL = ["전환수", "conversions", "ccnt"]
SALES_HEADER_CANDIDATES_LOCAL = ["전환매출액", "전환매출", "conversionvalue", "sales", "convamt"]

def _m_normalize_header(v: Any) -> str:
    return str(v or '').lower().replace(' ', '').replace('_', '').replace('-', '').replace('"', '').replace("'", '')

def _m_get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [_m_normalize_header(h) for h in headers]
    norm_candidates = [_m_normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c and c in h:
                return i
    return -1

def _m_safe_float(v: Any) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(',', '').strip()
    if not s or s == '-':
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def _m_safe_text(v: Any, default: str = '전체') -> str:
    s = str(v or '').strip()
    if not s or s.lower() in {'nan', 'none'} or s == '-':
        return default
    return s

def _map_campaign_type_label(v: Any) -> str:
    s = str(v or '').strip()
    if not s:
        return '기타'
    up = s.upper()
    if up in {'WEB_SITE', 'WEBSITE', 'POWER_LINK'} or s == '파워링크':
        return '파워링크'
    if 'SHOPPING' in up or s == '쇼핑검색':
        return '쇼핑검색'
    if up in {'POWER_CONTENTS'} or s == '파워컨텐츠':
        return '파워컨텐츠'
    if up in {'BRAND_SEARCH'} or s == '브랜드검색':
        return '브랜드검색'
    if up in {'PLACE'} or s == '플레이스':
        return '플레이스'
    return s

def build_campaign_type_map(engine: Engine, customer_id: str) -> Dict[str, str]:
    sql = "SELECT campaign_id, campaign_tp FROM dim_campaign WHERE customer_id = :cid"
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {'cid': str(customer_id)}).fetchall()
        return {str(r[0]).strip(): _map_campaign_type_label(r[1]) for r in rows if str(r[0]).strip()}
    except Exception:
        return {}

def replace_media_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1: date, scoped_campaign_types: List[str] | None = None):
    table = 'fact_media_daily'
    pk_cols = ['dt', 'customer_id', 'campaign_type', 'media_name', 'region_name', 'device_name']
    for _ in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt" + (" AND campaign_type = ANY(:types)" if scoped_campaign_types else "")), {'cid': str(customer_id), 'dt': d1, 'types': scoped_campaign_types or []})
            break
        except Exception:
            time.sleep(2)
    if not rows:
        return 0

    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last').sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    conflict_clause = (
        'ON CONFLICT (dt, customer_id, campaign_type, media_name, region_name, device_name) DO UPDATE SET ' +
        ', '.join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        'ON CONFLICT (dt, customer_id, campaign_type, media_name, region_name, device_name) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    for _ in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return len(df)
        except Exception:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception: pass
            time.sleep(2)
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if raw_conn:
                try: raw_conn.close()
                except Exception: pass
    return 0

def _detect_media_header_idx(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return -1
    scan_limit = min(60, len(df))
    best_idx = -1
    best_score = -1
    for i in range(scan_limit):
        row_vals = [_m_normalize_header(x) for x in df.iloc[i].fillna('').tolist()]
        score = 0
        
        # ✨ 수정 1: 헤더 감지 시 캠페인 ID를 포함시켜, 소재 ID가 생략된 양식의 매체 리포트도 인지하도록 안전망 추가
        if any(c in row_vals for c in [_m_normalize_header(x) for x in AD_HEADER_CANDIDATES_LOCAL + ["캠페인id", "campaignid", "ncccampaignid"]]):
            score += 2
            
        if any(c in row_vals for c in [_m_normalize_header(x) for x in MEDIA_HEADER_CANDIDATES + REGION_HEADER_CANDIDATES + DEVICE_HEADER_CANDIDATES_LOCAL]):
            score += 2
        metric_hits = sum(1 for x in [_m_normalize_header(x) for x in IMP_HEADER_CANDIDATES_LOCAL + CLK_HEADER_CANDIDATES_LOCAL + COST_HEADER_CANDIDATES_LOCAL] if x in row_vals)
        score += min(metric_hits, 3)
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 5:
            return i
    return best_idx if best_score >= 3 else -1



def normalize_device_name(device_value: Any) -> str:
    v = str(device_value or '').strip().upper()
    if not v:
        return ''
    if v in {'M', 'MO', 'MOBILE', '모바일'}:
        return 'MO'
    if v in {'P', 'PC', 'DESKTOP', 'DESK', '컴퓨터'}:
        return 'PC'
    return v

def _build_media_rows_from_noheader(raw_df: pd.DataFrame, target_date: date, customer_id: str, campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None):
    if raw_df is None or raw_df.empty:
        return [], {'status': 'empty'}
    preview = raw_df.head(5).fillna('').astype(str).values.tolist()
    agg = {}
    row_count = 0
    mapped_rows = 0
    for _, row in raw_df.iterrows():
        vals = [str(x).strip() for x in row.fillna('').tolist()]
        # ✨ 수정 2: index 14(전환매출액)까지 안전하게 파싱하도록 최소 길이를 14로 수정 (기존 14 미만 스킵)
        if len(vals) < 14:
            continue
            
        row_count += 1
        campaign_id = vals[2].strip()
        if not campaign_id or not campaign_id.startswith('cmp-'):
            continue
        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
            continue
        mapped_rows += 1
        campaign_type = campaign_type_map.get(campaign_id, '기타')
        media_name = vals[7].strip() or '전체'
        device_name = normalize_device_name(vals[8]) or '전체'
        region_name = '전체'
        clk = int(round(_m_safe_float(vals[10]))) if len(vals) > 10 else 0
        cost = int(round(_m_safe_float(vals[11]))) if len(vals) > 11 else 0
        imp = int(round(_m_safe_float(vals[12]))) if len(vals) > 12 else 0
        conv = float(_m_safe_float(vals[13])) if len(vals) > 13 else 0.0
        
        # ✨ 수정 2: 강제로 0으로 하드코딩되던 sales 부분 파싱 추가
        sales = int(round(_m_safe_float(vals[14]))) if len(vals) > 14 else 0  
        
        key = (campaign_type, media_name, region_name, device_name)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += imp
        bucket['clk'] += clk
        bucket['cost'] += cost
        bucket['conv'] += conv
        bucket['sales'] += sales
        
    rows = []
    for (campaign_type, media_name, region_name, device_name), s in agg.items():
        rows.append({
            'dt': target_date,
            'customer_id': str(customer_id),
            'campaign_type': campaign_type,
            'media_name': media_name,
            'region_name': region_name,
            'device_name': device_name,
            'imp': int(s['imp']),
            'clk': int(s['clk']),
            'cost': int(s['cost']),
            'conv': float(round(s['conv'], 4)),
            'sales': int(s['sales']),
            'data_source': 'ad_report_dimension_noheader',
            'source_report': 'AD',
        })
    detail_rows = sum(1 for r in rows if str(r.get('media_name', '전체')) != '전체' or str(r.get('region_name', '전체')) != '전체')
    summary_rows = len(rows) - detail_rows
    distinct_media = sorted({str(r.get('media_name', '') or '') for r in rows if str(r.get('media_name', '전체')) != '전체'})
    return rows, {
        'status': 'ok_no_header' if rows else 'no_header',
        'row_count': row_count,
        'mapped_rows': mapped_rows,
        'header_preview': preview,
        'detail_rows': detail_rows,
        'summary_rows': summary_rows,
        'distinct_media_count': len(distinct_media),
        'distinct_media_preview': distinct_media[:10],
    }

def parse_media_report_rows(df: pd.DataFrame, target_date: date, customer_id: str, ad_to_campaign: Dict[str, str], campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if df is None or df.empty:
        return [], {'status': 'empty'}

    raw_df = df.reset_index(drop=True).copy()
    header_idx = _detect_media_header_idx(raw_df)
    if header_idx == -1:
        return _build_media_rows_from_noheader(raw_df, target_date, customer_id, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)

    headers_raw = [str(x) for x in raw_df.iloc[header_idx].fillna('').tolist()]
    headers = [_m_normalize_header(x) for x in headers_raw]
    data_df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)

    ad_idx = _m_get_col_idx(headers, AD_HEADER_CANDIDATES_LOCAL)
    camp_idx = _m_get_col_idx(headers, ["캠페인id", "campaignid", "ncccampaignid"])
    media_idx = _m_get_col_idx(headers, MEDIA_HEADER_CANDIDATES)
    region_idx = _m_get_col_idx(headers, REGION_HEADER_CANDIDATES)
    device_idx = _m_get_col_idx(headers, DEVICE_HEADER_CANDIDATES_LOCAL)
    imp_idx = _m_get_col_idx(headers, IMP_HEADER_CANDIDATES_LOCAL)
    clk_idx = _m_get_col_idx(headers, CLK_HEADER_CANDIDATES_LOCAL)
    cost_idx = _m_get_col_idx(headers, COST_HEADER_CANDIDATES_LOCAL)
    conv_idx = _m_get_col_idx(headers, CONV_HEADER_CANDIDATES_LOCAL)
    sales_idx = _m_get_col_idx(headers, SALES_HEADER_CANDIDATES_LOCAL)

    # ✨ 수정 3: ad_id가 무조건 있어야만 통과시키던 로직에서 camp_id가 있는 경우도 허용하도록 완화 (전체 유실 방지)
    if ad_idx == -1 and camp_idx == -1:
        return [], {'status': 'no_ad_or_camp_id', 'header_idx': header_idx, 'headers': headers_raw[:20]}

    if media_idx == -1 and region_idx == -1 and device_idx == -1:
        return [], {'status': 'no_dimension', 'header_idx': header_idx, 'headers': headers_raw[:20]}

    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    row_count = 0
    mapped_rows = 0
    for _, row in data_df.iterrows():
        row_count += 1
        max_idx = max([x for x in [ad_idx, camp_idx, media_idx, region_idx, device_idx, imp_idx, clk_idx, cost_idx, conv_idx, sales_idx] if x != -1], default=0)
        if len(row) <= max_idx:
            continue
            
        campaign_id = ''
        
        # 1. 소재 ID를 통한 캠페인 추적 (기존 로직)
        if ad_idx != -1 and len(row) > ad_idx:
            ad_id = str(row.iloc[ad_idx]).strip()
            campaign_id = str(ad_to_campaign.get(ad_id, '') or '').strip()
            
        # 2. 매체 리포트에 캠페인 ID가 직접 박혀있는 경우의 Fallback
        if not campaign_id and camp_idx != -1 and len(row) > camp_idx:
            campaign_id = str(row.iloc[camp_idx]).strip()
            
        if not campaign_id:
            continue
            
        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
            continue
            
        mapped_rows += 1
        campaign_type = campaign_type_map.get(campaign_id, '기타')
        media_name = _m_safe_text(row.iloc[media_idx] if media_idx != -1 and len(row) > media_idx else '', '전체')
        region_name = _m_safe_text(row.iloc[region_idx] if region_idx != -1 and len(row) > region_idx else '', '전체')
        raw_device = row.iloc[device_idx] if device_idx != -1 and len(row) > device_idx else ''
        device_name = normalize_device_name(raw_device) or _m_safe_text(raw_device, '전체')
        key = (campaign_type, media_name, region_name, device_name)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(round(_m_safe_float(row.iloc[imp_idx]) if imp_idx != -1 and len(row) > imp_idx else 0))
        bucket['clk'] += int(round(_m_safe_float(row.iloc[clk_idx]) if clk_idx != -1 and len(row) > clk_idx else 0))
        bucket['cost'] += int(round(_m_safe_float(row.iloc[cost_idx]) if cost_idx != -1 and len(row) > cost_idx else 0))
        bucket['conv'] += float(_m_safe_float(row.iloc[conv_idx]) if conv_idx != -1 and len(row) > conv_idx else 0)
        bucket['sales'] += int(round(_m_safe_float(row.iloc[sales_idx]) if sales_idx != -1 and len(row) > sales_idx else 0))

    rows = []
    for (campaign_type, media_name, region_name, device_name), s in agg.items():
        rows.append({
            'dt': target_date,
            'customer_id': str(customer_id),
            'campaign_type': campaign_type,
            'media_name': media_name,
            'region_name': region_name,
            'device_name': device_name or '전체',
            'imp': int(s['imp']),
            'clk': int(s['clk']),
            'cost': int(s['cost']),
            'conv': float(round(s['conv'], 4)),
            'sales': int(s['sales']),
            'data_source': 'ad_report_dimension',
            'source_report': 'AD',
        })
    detail_rows = sum(1 for r in rows if str(r.get('media_name', '전체')) != '전체' or str(r.get('region_name', '전체')) != '전체')
    summary_rows = len(rows) - detail_rows
    distinct_media = sorted({str(r.get('media_name', '') or '') for r in rows if str(r.get('media_name', '전체')) != '전체'})
    return rows, {
        'status': 'ok' if rows else 'no_rows',
        'header_idx': header_idx,
        'row_count': row_count,
        'mapped_rows': mapped_rows,
        'dim_cols': {'media': media_idx, 'region': region_idx, 'device': device_idx},
        'detail_rows': detail_rows,
        'summary_rows': summary_rows,
        'distinct_media_count': len(distinct_media),
        'distinct_media_preview': distinct_media[:10],
    }

def build_media_rows_from_campaign_device(target_date: date, customer_id: str, camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]], campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for (campaign_id, device_name), s in (camp_device_stat or {}).items():
        campaign_id = str(campaign_id or '').strip()
        if not campaign_id:
            continue
        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
            continue
        campaign_type = campaign_type_map.get(campaign_id, '기타')
        device_label = normalize_device_name(device_name) or _m_safe_text(device_name, '전체')
        key = (campaign_type, '전체', '전체', device_label)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(s.get('imp', 0) or 0)
        bucket['clk'] += int(s.get('clk', 0) or 0)
        bucket['cost'] += int(s.get('cost', 0) or 0)
        bucket['conv'] += float(s.get('conv', 0.0) or 0.0)
        bucket['sales'] += int(s.get('sales', 0) or 0)
    rows = []
    for (campaign_type, media_name, region_name, device_name), s in agg.items():
        rows.append({
            'dt': target_date,
            'customer_id': str(customer_id),
            'campaign_type': campaign_type,
            'media_name': media_name,
            'region_name': region_name,
            'device_name': device_name,
            'imp': int(s['imp']),
            'clk': int(s['clk']),
            'cost': int(s['cost']),
            'conv': float(round(s['conv'], 4)),
            'sales': int(s['sales']),
            'data_source': 'campaign_device_fallback',
            'source_report': 'AD',
        })
    return rows

def build_media_rows_from_campaign_total_db(engine: Engine, customer_id: str, target_date: date, campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> List[Dict[str, Any]]:
    sql = "SELECT campaign_id, imp, clk, cost, conv, sales FROM fact_campaign_daily WHERE customer_id = :cid AND dt = :dt"
    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {'cid': str(customer_id), 'dt': target_date}).fetchall()
    except Exception:
        rows = []
    for campaign_id, imp, clk, cost, conv, sales in rows:
        camp_id = str(campaign_id or '').strip()
        if not camp_id:
            continue
        if allowed_campaign_ids is not None and camp_id not in allowed_campaign_ids:
            continue
        campaign_type = campaign_type_map.get(camp_id, '기타')
        key = (campaign_type, '전체', '전체', '전체')
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(imp or 0)
        bucket['clk'] += int(clk or 0)
        bucket['cost'] += int(cost or 0)
        bucket['conv'] += float(conv or 0.0)
        bucket['sales'] += int(sales or 0)
    out = []
    for (campaign_type, media_name, region_name, device_name), s in agg.items():
        out.append({
            'dt': target_date,
            'customer_id': str(customer_id),
            'campaign_type': campaign_type,
            'media_name': media_name,
            'region_name': region_name,
            'device_name': device_name,
            'imp': int(s['imp']),
            'clk': int(s['clk']),
            'cost': int(s['cost']),
            'conv': float(round(s['conv'], 4)),
            'sales': int(s['sales']),
            'data_source': 'campaign_total_fallback',
            'source_report': 'STATS',
        })
    return out

def collect_media_fact(engine: Engine, customer_id: str, target_date: date, ad_report_df: pd.DataFrame | None, ad_to_campaign_map: Dict[str, str], campaign_type_map: Dict[str, str], camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]] | None = None, allowed_campaign_ids: set[str] | None = None, scoped_campaign_types: List[str] | None = None) -> Tuple[int, Dict[str, Any]]:
    media_rows, meta = parse_media_report_rows(ad_report_df, target_date, customer_id, ad_to_campaign_map, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
    if media_rows:
        return replace_media_fact_range(engine, media_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types), meta

    if camp_device_stat:
        fb_rows = build_media_rows_from_campaign_device(target_date, customer_id, camp_device_stat, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
        if fb_rows:
            meta = {'status': 'fallback_device', **(meta or {})}
            return replace_media_fact_range(engine, fb_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types), meta

    total_rows = build_media_rows_from_campaign_total_db(engine, customer_id, target_date, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
    if total_rows:
        meta = {'status': 'fallback_total', **(meta or {})}
        return replace_media_fact_range(engine, total_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types), meta

    return replace_media_fact_range(engine, [], customer_id, target_date, scoped_campaign_types=scoped_campaign_types), {'status': 'empty'}

def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool = False, fast_mode: bool = False, collect_mode: str = "sa_with_device", shopping_only: bool = False, sa_scope: str = "full"):
    log(f"▶️ [ {account_name} ] 업체 데이터 조회 시작...")

    job_lock = acquire_job_lock(engine, customer_id, target_date)
    if job_lock is False:
        log(f"⏭️ [ {account_name} ] 동일 날짜/계정 수집이 이미 실행 중이라 건너뜁니다. ({target_date})")
        return

    try:
        collect_mode = (collect_mode or "sa_with_device").strip().lower()
        collect_sa = collect_mode in {"sa_only", "sa_with_device"}
        collect_device = collect_mode in {"device_only", "sa_with_device"}
        target_camp_ids, target_kw_ids, target_ad_ids = [], [], []
        shopping_campaign_ids: set[str] = set()
        shopping_adgroup_ids: set[str] = set()
        shopping_keyword_ids: set[str] = set()
        if shopping_only:
            log(f"   🛍️ [ {account_name} ] 쇼핑검색 전용 수집 모드")

        if not skip_dim:
            log(f"   📥 [ {account_name} ] 구조 데이터 동기화 시작...")
            camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []

            camps = list_campaigns(customer_id)
            for c in camps:
                cid = str(c.get("nccCampaignId"))
                camp_tp = str(c.get("campaignTp", ""))
                is_shopping = is_shopping_campaign_obj(c)
                if shopping_only and not is_shopping:
                    continue

                target_camp_ids.append(cid)
                if is_shopping:
                    shopping_campaign_ids.add(cid)
                camp_rows.append({
                    "customer_id": str(customer_id),
                    "campaign_id": cid,
                    "campaign_name": str(c.get("name", "")),
                    "campaign_tp": camp_tp,
                    "status": str(c.get("status", "")),
                })

                groups = list_adgroups(customer_id, cid)
                for g in groups:
                    gid = str(g.get("nccAdgroupId"))
                    if is_shopping:
                        shopping_adgroup_ids.add(gid)
                    ag_rows.append({
                        "customer_id": str(customer_id),
                        "adgroup_id": gid,
                        "campaign_id": cid,
                        "adgroup_name": str(g.get("name", "")),
                        "status": str(g.get("status", "")),
                    })

                    if collect_sa and not SKIP_KEYWORD_DIM:
                        kws = list_keywords(customer_id, gid)
                        for k in kws:
                            kid = str(k.get("nccKeywordId"))
                            target_kw_ids.append(kid)
                            kw_rows.append({
                                "customer_id": str(customer_id),
                                "keyword_id": kid,
                                "adgroup_id": gid,
                                "keyword": extract_keyword_text_from_obj(k),
                                "status": str(k.get("status", "")),
                            })

                    if (collect_sa or collect_device) and not SKIP_AD_DIM:
                        ads = list_ads(customer_id, gid)
                        for ad in ads:
                            adid = str(ad.get("nccAdId"))
                            target_ad_ids.append(adid)
                            ext = extract_ad_creative_fields(ad)
                            ad_rows.append({
                                "customer_id": str(customer_id),
                                "ad_id": adid,
                                "adgroup_id": gid,
                                "ad_name": str(ad.get("name") or ad.get("adName") or ""),
                                "status": str(ad.get("status", "")),
                                "ad_title": ext["ad_title"],
                                "ad_desc": ext["ad_desc"],
                                "pc_landing_url": ext["pc_landing_url"],
                                "mobile_landing_url": ext["mobile_landing_url"],
                                "creative_text": ext["creative_text"],
                                "image_url": ext["image_url"],
                            })

            upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
            upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
            if not SKIP_KEYWORD_DIM:
                upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
                kw_text_filled = sum(1 for r in kw_rows if str(r.get("keyword") or "").strip())
                log(f"   🔎 [ {account_name} ] 구조 키워드 텍스트 적재: {kw_text_filled}/{len(kw_rows)}")
            if not SKIP_AD_DIM:
                upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
            shopping_keyword_ids = set(target_kw_ids) if shopping_adgroup_ids else set()
            log(f"   ✅ [ {account_name} ] 구조 적재 완료")

        else:
            with engine.connect() as conn:
                shopping_campaign_ids = {str(r[0]) for r in conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid AND lower(coalesce(campaign_tp,'')) LIKE :kw"), {"cid": customer_id, "kw": '%shopping%'})}
                shopping_adgroup_ids = {
                    str(r[0]) for r in conn.execute(
                        text("SELECT adgroup_id FROM dim_adgroup WHERE customer_id = :cid AND campaign_id = ANY(:cids)"),
                        {"cid": customer_id, "cids": list(shopping_campaign_ids)},
                    )
                } if shopping_campaign_ids else set()
                shopping_keyword_ids = {
                    str(r[0]) for r in conn.execute(
                        text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid AND adgroup_id = ANY(:gids)"),
                        {"cid": customer_id, "gids": list(shopping_adgroup_ids)},
                    )
                } if shopping_adgroup_ids else set()

                if shopping_only:
                    target_camp_ids = sorted(shopping_campaign_ids)
                    target_kw_ids = sorted(shopping_keyword_ids) if collect_sa else []
                    target_ad_ids = [
                        str(r[0]) for r in conn.execute(
                            text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid AND adgroup_id = ANY(:gids)"),
                            {"cid": customer_id, "gids": list(shopping_adgroup_ids)},
                        )
                    ] if shopping_adgroup_ids else []
                else:
                    target_camp_ids = [str(r[0]) for r in conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid"), {"cid": customer_id})]
                    target_kw_ids = [str(r[0]) for r in conn.execute(text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid"), {"cid": customer_id})] if collect_sa else []
                    target_ad_ids = [str(r[0]) for r in conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid"), {"cid": customer_id})]

        keyword_lookup = {}
        keyword_unique_lookup = {}
        try:
            text_freq = {}
            temp_rows = []
            group_rows = {}
            with engine.connect() as conn:
                kw_sql = "SELECT keyword_id, adgroup_id, keyword FROM dim_keyword WHERE customer_id = :cid"
                kw_params = {"cid": customer_id}
                if shopping_only and shopping_adgroup_ids:
                    kw_sql += " AND adgroup_id = ANY(:gids)"
                    kw_params["gids"] = list(shopping_adgroup_ids)
                for kid, gid, kw in conn.execute(text(kw_sql), kw_params):
                    if kid and gid and kw:
                        gid_s = str(gid)
                        kw_s = str(kw).strip()
                        kw_l = kw_s.lower()
                        kw_n = normalize_keyword_text(kw_s)
                        kid_s = str(kid)
                        keyword_lookup[(gid_s, kw_s)] = kid_s
                        keyword_lookup[(gid_s, kw_l)] = kid_s
                        keyword_lookup[(gid_s, kw_n)] = kid_s
                        group_rows.setdefault(gid_s, []).append((kw_n, kid_s))
                        text_freq[kw_n] = text_freq.get(kw_n, 0) + 1
                        temp_rows.append((kw_n, kid_s))
            for gid_s, rows in group_rows.items():
                keyword_lookup[(gid_s, '__rows__')] = rows
            unique_map = {}
            for kw_n, kid_s in temp_rows:
                if kw_n and text_freq.get(kw_n) == 1:
                    unique_map.setdefault(kw_n, []).append(kid_s)
            keyword_unique_lookup = unique_map
        except Exception:
            keyword_lookup = {}
            keyword_unique_lookup = {}

        live_keyword_resolver = None if fast_mode else make_live_keyword_resolver(customer_id)

        ad_to_campaign_map = build_ad_to_campaign_map(engine, customer_id)
        campaign_type_map = build_campaign_type_map(engine, customer_id)

        kst_now = datetime.utcnow() + timedelta(hours=9)
        use_realtime_fallback = False
        dfs: Dict[str, pd.DataFrame | None] = {}

        if target_date >= kst_now.date():
            use_realtime_fallback = True
            log(f"   ℹ️ [ {account_name} ] 당일 데이터는 실시간 stats 총합만 수집합니다.")
        else:
            log(f"   ⏳ [ {account_name} ] 리포트 생성 대기 중...")
            report_types = ["AD"]
            split_candidate_reports = []
            if split_enabled_for_date(target_date) and shopping_campaign_ids:
                split_candidate_reports = ["AD_CONVERSION", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
                report_types.extend(split_candidate_reports)
            dfs = fetch_multiple_stat_reports(customer_id, report_types, target_date)

            if dfs.get("AD") is None and all(dfs.get(tp) is None for tp in split_candidate_reports):
                log(f"   ⚠️ [ {account_name} ] AD / 전환 리포트가 모두 실패 → 실시간 stats 총합으로 대체합니다. (purchase/cart 미분리)")
                use_realtime_fallback = True

        if use_realtime_fallback:
            if collect_sa:
                c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily", scoped_replace=shopping_only)
                if shopping_only and target_kw_ids:
                    clear_fact_scope(engine, "fact_keyword_daily", customer_id, target_date, "keyword_id", target_kw_ids)
                    k_cnt = 0
                else:
                    k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily", scoped_replace=shopping_only) if not SKIP_KEYWORD_STATS else 0
                a_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily", scoped_replace=shopping_only) if not SKIP_AD_STATS else 0
                log(f"   ✅ [ {account_name} ] 실시간 총합 수집 완료: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")
            else:
                log(f"   ℹ️ [ {account_name} ] 당일/실시간 모드에서는 PC/M 전용 수집을 수행하지 않습니다.")
            device_ad_cnt = 0
            device_campaign_cnt = 0
            media_cnt, media_meta = collect_media_fact(
                engine, customer_id, target_date, None, ad_to_campaign_map, campaign_type_map, None,
                allowed_campaign_ids=set(target_camp_ids) if target_camp_ids else None,
                scoped_campaign_types=['쇼핑검색'] if shopping_only else None,
            )
            if media_cnt:
                log(f"   ✅ [ {account_name} ] 매체/지역/기기 요약 저장 완료: {media_cnt}건 | source={media_meta.get('status')}")
        else:
            split_report_ok = False
            camp_map, kw_map, ad_map = {}, {}, {}
            shop_query_rows: List[Dict[str, Any]] = []
            ad_report_df = dfs.get("AD")

            if not collect_sa:
                pass
            elif not split_enabled_for_date(target_date):
                log(f"   ℹ️ [ {account_name} ] 2026-03-11 이전 날짜는 purchase/cart/wishlist 분리 수집을 시도하지 않습니다.")
            elif not shopping_campaign_ids:
                log(f"   ℹ️ [ {account_name} ] 쇼핑검색 캠페인이 없어 purchase/cart/wishlist 분리 수집을 건너뜁니다.")
            else:
                source_maps = {}
                report_candidates = ["AD_CONVERSION", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
                for tp in report_candidates:
                    conv_df = dfs.get(tp)
                    if conv_df is None:
                        log(f"   ⚠️ [ {account_name} ] {tp} 리포트 실패 → 다음 전환 리포트로 진행합니다.")
                        continue
                    if conv_df.empty:
                        log(f"   ℹ️ [ {account_name} ] {tp} 리포트가 비어 있습니다. purchase/cart/wishlist 는 미확정(NULL)로 유지합니다.")
                        continue

                    report_allowed_campaign_ids = None

                    one_camp_map, one_kw_map, one_ad_map, one_summary = process_conversion_report(
                        conv_df,
                        allowed_campaign_ids=report_allowed_campaign_ids,
                        report_hint=tp,
                        keyword_lookup=keyword_lookup,
                        keyword_unique_lookup=keyword_unique_lookup,
                        live_keyword_resolver=live_keyword_resolver,
                        debug_account_name=account_name,
                        debug_target_date=str(target_date),
                    )
                    
                    if len(one_camp_map) == 0 and len(one_kw_map) == 0 and len(one_ad_map) == 0:
                        log(f"   ⚠️ [ {account_name} ] {tp} 데이터는 있으나 shopping purchase/cart/wishlist 파싱에 실패했습니다. debug_reports 원본을 확인하세요.")
                        continue

                    source_maps[tp] = (one_camp_map, one_kw_map, one_ad_map, one_summary)

                ad_conv_maps = source_maps.get("AD_CONVERSION", ({}, {}, {}, empty_split_summary()))
                shop_kw_maps = source_maps.get("SHOPPINGKEYWORD_CONVERSION_DETAIL", ({}, {}, {}, empty_split_summary()))

                ad_camp_map, ad_kw_map, ad_ad_map, ad_summary = ad_conv_maps
                shop_camp_map, shop_kw_map, shop_ad_map, shop_summary = shop_kw_maps

                shop_query_df = dfs.get("SHOPPINGKEYWORD_CONVERSION_DETAIL")
                if shop_query_df is not None and not shop_query_df.empty:
                    try:
                        shop_query_rows = parse_shopping_query_report(shop_query_df, target_date, customer_id)
                    except Exception as _e:
                        log(f"   ⚠️ [ {account_name} ] 쇼핑검색어 분리 저장 파싱 실패: {_e}")
                        shop_query_rows = []

                camp_map = ad_camp_map if ad_camp_map else shop_camp_map
                ad_map = ad_ad_map if ad_ad_map else shop_ad_map
                raw_kw_map = merge_split_maps(ad_kw_map, shop_kw_map)
                if shopping_only:
                    kw_map = {}
                else:
                    kw_map = filter_split_map_excluding_ids(raw_kw_map, shopping_keyword_ids)
                    removed_kw = max(0, len(raw_kw_map) - len(kw_map))
                    if removed_kw:
                        log(f"   ℹ️ [ {account_name} ] 쇼핑 키워드 split {removed_kw}건은 fact_keyword_daily 적재에서 제외합니다.")

                split_report_ok = bool(camp_map or kw_map or ad_map)

                final_split_summary = ad_summary if split_summary_has_values(ad_summary) else shop_summary
                if shopping_only and split_report_ok and split_summary_has_values(final_split_summary):
                    split_ok, split_reason = validate_shopping_split_summary(final_split_summary, ad_map)
                    if not split_ok:
                        log(f"   ⚠️ [ {account_name} ] shopping split 검증 실패 → 상세 split 저장을 건너뛰고 총합만 적재합니다. ({split_reason})")
                        camp_map, kw_map, ad_map = {}, {}, {}
                        shop_query_rows = []
                        split_report_ok = False

                if split_report_ok:
                    camp_ad_src = 'AD_CONVERSION' if ad_camp_map or ad_ad_map else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_camp_map or shop_ad_map else 'none')
                    kw_src = 'AD_CONVERSION+SHOPPINGKEYWORD_CONVERSION_DETAIL' if (ad_kw_map and shop_kw_map) else ('AD_CONVERSION' if ad_kw_map else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_kw_map else 'none'))
                    summary_src = 'AD_CONVERSION' if split_summary_has_values(ad_summary) else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if split_summary_has_values(shop_summary) else 'none')
                    query_src = 'SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_query_rows else 'none'
                    log(
                        f"   ✅ [ {account_name} ] shopping split 원천 사용: "
                        f"summary={summary_src}, campaign/ad={camp_ad_src}, keyword={kw_src}, query={query_src}"
                    )
                    if split_summary_has_values(final_split_summary):
                        log(f"   ℹ️ [ {account_name} ] detail split 파싱: {format_split_summary(final_split_summary)}")

            c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily", split_map=camp_map, scoped_replace=shopping_only) if collect_sa else 0
            if shopping_only and target_kw_ids:
                clear_fact_scope(engine, "fact_keyword_daily", customer_id, target_date, "keyword_id", target_kw_ids)
                k_cnt = 0
            else:
                k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily", split_map=kw_map, scoped_replace=shopping_only) if (collect_sa and not SKIP_KEYWORD_STATS) else 0

            device_ad_cnt = 0
            device_campaign_cnt = 0
            ad_stat = {}
            camp_device_stat = {}

            if not SKIP_AD_STATS:
                if ad_report_df is not None and not ad_report_df.empty:
                    if collect_sa:
                        if shopping_only:
                            a_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily", split_map=ad_map, scoped_replace=True)
                            ad_stat = {}
                        else:
                            ad_data_source = "report_split" if split_report_ok else "report_total_only"
                            ad_stat = parse_base_report(ad_report_df, "AD", ad_map, has_conv_report=split_report_ok)
                            a_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_ad_daily", "ad_id", ad_stat, data_source=ad_data_source) if ad_stat else 0
                    else:
                        a_cnt = 0
                        ad_stat = {}

                    if collect_device:
                        ad_device_stat, camp_device_stat, device_meta = parse_ad_device_report(ad_report_df, ad_to_campaign=ad_to_campaign_map)
                        if shopping_only:
                            ad_device_stat = filter_stat_result(ad_device_stat, set(target_ad_ids))
                            camp_device_stat = filter_stat_result(camp_device_stat, set(target_camp_ids))
                    else:
                        ad_device_stat, camp_device_stat, device_meta = {}, {}, {"status": "disabled", "reason": "collect_mode=sa_only"}
                    if collect_device and device_meta.get("status") == "ok":
                        device_ad_cnt = save_device_stats(
                            engine, customer_id, target_date, "fact_ad_device_daily", "ad_id", ad_device_stat,
                            data_source="report_device_total_only", source_report="AD"
                        )
                        device_campaign_cnt = save_device_stats(
                            engine, customer_id, target_date, "fact_campaign_device_daily", "campaign_id", camp_device_stat,
                            data_source="report_device_total_only", source_report="AD"
                        )
                        if ad_stat:
                            total_from_ad = {
                                "imp": sum(int(v.get("imp", 0) or 0) for v in ad_stat.values()),
                                "clk": sum(int(v.get("clk", 0) or 0) for v in ad_stat.values()),
                                "cost": sum(int(v.get("cost", 0) or 0) for v in ad_stat.values()),
                                "conv": sum(float(v.get("conv", 0.0) or 0.0) for v in ad_stat.values()),
                                "sales": sum(int(v.get("sales", 0) or 0) for v in ad_stat.values()),
                            }
                            total_from_device = summarize_stat_res(ad_device_stat)
                            diff_cost = total_from_ad["cost"] - total_from_device["cost"]
                            diff_sales = total_from_ad["sales"] - total_from_device["sales"]
                            diff_conv = round(total_from_ad["conv"] - total_from_device["conv"], 4)
                            if diff_cost or diff_sales or diff_conv:
                                log(
                                    f"   ⚠️ [ {account_name} ] PC/M 검증 차이 감지: cost={diff_cost}, sales={diff_sales}, conv={diff_conv} "
                                    f"(source_report=AD, device_rows={device_meta.get('ad_rows', 0)})"
                                )
                        miss = int(device_meta.get("missing_campaign_rows", 0) or 0)
                        miss_msg = f", 캠페인 매핑누락={miss}건" if miss else ""
                        log(
                            f"   ✅ [ {account_name} ] PC/M 분리 저장 완료: 캠페인({device_campaign_cnt}) | 소재({device_ad_cnt})"
                            f"{miss_msg} | parser={DEVICE_PARSER_VERSION}"
                        )
                    elif collect_device:
                        debug_keys = [
                            "header_idx", "ad_idx", "camp_idx", "device_idx", "imp_idx", "clk_idx", "cost_idx",
                            "conv_idx", "sales_idx", "rank_idx", "scan_rows", "reject_short", "reject_empty_ad",
                            "reject_empty_device", "reject_zero_metrics", "sample_headers", "preview_rows"
                        ]
                        extra_parts = []
                        for _k in debug_keys:
                            _v = device_meta.get(_k)
                            if _v in (None, "", [], {}):
                                continue
                            extra_parts.append(f"{_k}={_v}")
                        extra_msg = f" | {' | '.join(extra_parts)}" if extra_parts else ""
                        log(
                            f"   ℹ️ [ {account_name} ] AD 리포트에서 PC/M 컬럼을 확인하지 못해 기기 분리 저장은 건너뜁니다. "
                            f"status={device_meta.get('status')} | parser={DEVICE_PARSER_VERSION}{extra_msg}"
                        )
                else:
                    if collect_sa:
                        log(f"   ⚠️ [ {account_name} ] AD 리포트 없음 → 소재만 실시간 stats 총합으로 대체합니다.")
                        a_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily", split_map=ad_map, scoped_replace=shopping_only)
                    else:
                        log(f"   ℹ️ [ {account_name} ] AD 리포트가 없어 PC/M 전용 적재를 건너뜁니다.")
                        a_cnt = 0
            else:
                a_cnt = 0

            media_cnt, media_meta = collect_media_fact(
                engine, customer_id, target_date, ad_report_df, ad_to_campaign_map, campaign_type_map, camp_device_stat,
                allowed_campaign_ids=set(target_camp_ids) if target_camp_ids else None,
                scoped_campaign_types=['쇼핑검색'] if shopping_only else None,
            )
            detail_rows = int(media_meta.get('detail_rows', 0) or 0)
            summary_rows = int(media_meta.get('summary_rows', 0) or 0)
            distinct_media_count = int(media_meta.get('distinct_media_count', 0) or 0)
            media_preview = media_meta.get('distinct_media_preview') or []
            preview_msg = f" | media_preview={media_preview}" if media_preview else ""
            if media_cnt:
                log(
                    f"   ✅ [ {account_name} ] 매체/지역/기기 저장 완료: total_rows={media_cnt} | detail_rows={detail_rows} | "
                    f"summary_rows={summary_rows} | media_codes={distinct_media_count} | source={media_meta.get('status')}{preview_msg}"
                )
            else:
                log(
                    f"   ℹ️ [ {account_name} ] 매체/지역 자동 분해 원천이 없어 요약 행만 유지합니다. "
                    f"source={media_meta.get('status')} | detail_rows={detail_rows} | summary_rows={summary_rows}"
                )

            if collect_sa:
                replace_query_fact_range(engine, shop_query_rows, customer_id, target_date)
                if shop_query_rows:
                    log(f"   ✅ [ {account_name} ] 쇼핑검색어 분리 저장 완료: {len(shop_query_rows)}건")

            if c_cnt == 0 and k_cnt == 0 and a_cnt == 0 and device_ad_cnt == 0 and device_campaign_cnt == 0:
                log(f"❌ [ {account_name} ] 수집된 데이터가 0건입니다! (해당 날짜에 발생한 클릭/노출 성과가 없음)")
            else:
                if collect_mode == "device_only":
                    log(f"   ✅ [ {account_name} ] PC/M 전용 수집 완료: 캠페인({device_campaign_cnt}) | 소재({device_ad_cnt})")
                else:
                    mode_msg = "총합 + purchase/cart/wishlist 분리" if split_report_ok else "총합만 저장 / purchase.cart.wishlist 미분리"
                    if collect_device:
                        mode_msg += " + PC/M"
                    log(f"   ✅ [ {account_name} ] 리포트 수집 완료 ({mode_msg}): 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")

    except Exception as e:
        log(f"❌ [ {account_name} ] 계정 처리 중 오류 발생: {str(e)}")
    finally:
        if job_lock is not False:
            release_job_lock(job_lock, customer_id, target_date)

def main():
    engine = get_engine()
    ensure_tables(engine)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--account_name", type=str, default="", help="단일 업체명 또는 일부 문자열")
    parser.add_argument("--account_names", type=str, default="", help="쉼표(,)로 구분한 여러 업체명")
    parser.add_argument("--skip_dim", action="store_true")
    parser.add_argument("--fast", action="store_true", help="빠른 수집 모드: skip_dim 강제, debug 저장 및 live keyword API fallback 비활성화")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--collect_mode", type=str, default="sa_with_device", choices=["sa_only", "device_only", "sa_with_device"], help="sa_only=기존 SA만, device_only=PC/M만, sa_with_device=둘 다")
    parser.add_argument("--sa_scope", type=str, default="full", choices=["full", "ad_only", "전체", "소재만"], help="full=캠페인/키워드/소재 전체, ad_only=소재(AD)만")
    parser.add_argument("--shopping_only", action="store_true", help="쇼핑검색 캠페인만 수집/재적재")
    parser.add_argument("--include_gfa_accounts", action="store_true", help="이름 끝이 GFA 인 네이버 GFA 계정도 함께 대상으로 포함")
    args = parser.parse_args()

    global FAST_MODE
    FAST_MODE = bool(args.fast)
    if FAST_MODE:
        args.skip_dim = True

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else (datetime.utcnow() + timedelta(hours=9)).date() - timedelta(days=1)
    
    print("\n" + "="*50, flush=True)
    print(f"🚀🚀🚀 [ 현재 수집 진행 날짜: {target_date} ] 🚀🚀🚀", flush=True)
    print("="*50, flush=True)
    if FAST_MODE:
        print("⚡ 빠른 수집 모드: 구조 수집 스킵 / debug 저장 중지 / live keyword API fallback 비활성화", flush=True)
    if str(args.sa_scope).strip() == "소재만":
        args.sa_scope = "ad_only"
    elif str(args.sa_scope).strip() == "전체":
        args.sa_scope = "full"

    print(f"🧭 수집 모드: {args.collect_mode}", flush=True)
    print(f"🎯 수집 범위: {args.sa_scope}", flush=True)
    if args.shopping_only:
        print("🛍️ 쇼핑검색 전용 수집", flush=True)
    print("="*50 + "\n", flush=True)

    accounts_info = []
    if args.customer_id:
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        if load_naver_accounts is not None:
            try:
                accounts_info = load_naver_accounts(include_gfa=args.include_gfa_accounts)
                if accounts_info:
                    excluded_msg = "포함" if args.include_gfa_accounts else "제외"
                    log(f"🗂️ account_master 기준 계정 로드: {len(accounts_info)}개 (GFA {excluded_msg})")
            except Exception as e:
                log(f"⚠️ account_master 로드 실패, 레거시 accounts.xlsx 로 폴백합니다: {e}")

        if not accounts_info and os.path.exists("accounts.xlsx"):
            try:
                df_acc = pd.read_excel("accounts.xlsx")
            except Exception:
                try:
                    df_acc = pd.read_csv("accounts.xlsx")
                except Exception:
                    df_acc = None

            if df_acc is not None:
                id_col, name_col = None, None
                for c in df_acc.columns:
                    c_clean = str(c).replace(" ", "").lower()
                    if c_clean in ["커스텀id", "customerid", "customer_id", "id"]:
                        id_col = c
                    if c_clean in ["업체명", "accountname", "account_name", "name"]:
                        name_col = c

                if id_col and name_col:
                    seen_ids = set()
                    for _, row in df_acc.iterrows():
                        cid = str(row[id_col]).strip()
                        nm = str(row[name_col]).strip()
                        if not args.include_gfa_accounts and nm.lower().endswith(" gfa"):
                            continue
                        if cid and cid.lower() != 'nan' and cid not in seen_ids:
                            accounts_info.append({"id": cid, "name": nm})
                            seen_ids.add(cid)

        if not accounts_info:
            try:
                with engine.connect() as conn:
                    accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT customer_id, MAX(account_name) FROM accounts WHERE customer_id IS NOT NULL GROUP BY customer_id"))]
            except Exception:
                pass


    target_name_tokens = []
    if args.account_name and str(args.account_name).strip():
        target_name_tokens.append(str(args.account_name).strip())
    if args.account_names and str(args.account_names).strip():
        target_name_tokens.extend([x.strip() for x in str(args.account_names).split(",") if x.strip()])

    if target_name_tokens:
        exact_set = {x for x in target_name_tokens}
        filtered_exact = [acc for acc in accounts_info if str(acc.get("name", "")).strip() in exact_set]
        if filtered_exact:
            accounts_info = filtered_exact
        else:
            lowered = [x.lower() for x in target_name_tokens]
            accounts_info = [
                acc for acc in accounts_info
                if any(tok in str(acc.get("name", "")).lower() for tok in lowered)
            ]
        log(f"🎯 업체명 필터 적용: {', '.join(target_name_tokens)} -> {len(accounts_info)}개")

    unique_accounts = {}
    for acc in accounts_info:
        unique_accounts[acc["id"]] = acc
    accounts_info = list(unique_accounts.values())

    if not accounts_info: 
        log("⚠️ 수집할 계정이 없습니다.")
        return
        
    log(f"📋 최종 수집 대상 계정: {len(accounts_info)}개 / 동시 작업: {args.workers}개")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_account, engine, acc["id"], acc["name"], target_date, args.skip_dim, args.fast, args.collect_mode, args.shopping_only, args.sa_scope) for acc in accounts_info]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception: pass

if __name__ == "__main__":
    main()
