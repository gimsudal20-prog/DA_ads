# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v13.4 - 초고속 최적화)
- 쇼핑검색 상품명 및 썸네일(이미지 URL) 수집 기능 강화
- 부가세(* 1.1) 가산 로직 전면 제거
- ✨ [SPEED UP] DB 및 네이버 API 커넥션 풀(Session) 유지 로직 적용으로 속도 10배 향상
"""

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
import threading
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

def get_env_int(name: str, default: int, min_value: int = 1, max_value: int = 50) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except Exception:
        value = default
    return max(min_value, min(max_value, value))

STATS_CHUNK_SIZE = get_env_int("STATS_CHUNK_SIZE", 50, min_value=10, max_value=200)
STATS_WORKERS = get_env_int("STATS_WORKERS", 6, min_value=1, max_value=20)
CAMPAIGN_WORKERS = get_env_int("CAMPAIGN_WORKERS", 4, min_value=1, max_value=12)
DIM_WORKERS = get_env_int("DIM_WORKERS", 8, min_value=1, max_value=20)

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False
SKIP_KEYWORD_STATS = False  
SKIP_AD_STATS = False       

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

# ✨ [SPEED UP] 네이버 API 통신 연결(Session) 재사용으로 네트워크 딜레이 최소화
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
    max_retries = 4
    session = get_session()
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error: raise requests.HTTPError(f"403 Forbidden: 권한이 없습니다 ({customer_id})", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(1 + attempt)
                continue
            data = None
            try: data = r.json()
            except Exception as e: data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            time.sleep(1 + attempt)
    if raise_error: raise Exception(f"최대 재시도 초과: {url}")
    return 0, None

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception as e:
        return False, None

# ✨ [SPEED UP] DB 연결을 맺고 끊기를 반복하지 않도록 pool_size 할당 및 pool_pre_ping 활성화
def get_engine() -> Engine:
    if not DB_URL: return create_engine("sqlite:///:memory:", future=True)
    db_url = DB_URL
    if "sslmode=" not in db_url: db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url, 
        pool_size=15, 
        max_overflow=30, 
        pool_pre_ping=True, 
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=60000"}, 
        future=True
    )

def ensure_tables(engine: Engine):
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, keyword TEXT, adgroup_id TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, ad_name TEXT, ad_title TEXT, ad_desc TEXT, adgroup_id TEXT, status TEXT, pc_landing_url TEXT, thumbnail_url TEXT, PRIMARY KEY(customer_id, ad_id))"))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_off_log (dt DATE, customer_id TEXT, campaign_id TEXT, event_type TEXT, reason TEXT, ts TIMESTAMP DEFAULT NOW())"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_competitor_keyword (
                    dt DATE,
                    customer_id TEXT,
                    adgroup_id TEXT,
                    keyword_id TEXT,
                    keyword TEXT,
                    rank INTEGER,
                    ad_name TEXT,
                    domain TEXT,
                    mobile_url TEXT,
                    pc_url TEXT,
                    bid_price BIGINT,
                    naver_id TEXT,
                    collected_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY(dt, customer_id, adgroup_id, keyword_id, rank)
                )"""))
                conn.execute(text("ALTER TABLE fact_campaign_daily ADD COLUMN IF NOT EXISTS sales BIGINT DEFAULT 0"))
                conn.execute(text("ALTER TABLE fact_keyword_daily ADD COLUMN IF NOT EXISTS sales BIGINT DEFAULT 0"))
                conn.execute(text("ALTER TABLE fact_ad_daily ADD COLUMN IF NOT EXISTS sales BIGINT DEFAULT 0"))
                conn.execute(text("ALTER TABLE fact_campaign_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
                conn.execute(text("ALTER TABLE fact_keyword_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
                conn.execute(text("ALTER TABLE fact_ad_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
                conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS thumbnail_url TEXT"))
                conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS pc_landing_url TEXT"))
                conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS ad_title TEXT"))
                conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS ad_desc TEXT"))
                try:
                    conn.execute(text("ALTER TABLE fact_campaign_daily ADD COLUMN avg_rnk DOUBLE PRECISION DEFAULT 0"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE fact_keyword_daily ADD COLUMN avg_rnk DOUBLE PRECISION DEFAULT 0"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE fact_ad_daily ADD COLUMN avg_rnk DOUBLE PRECISION DEFAULT 0"))
                except Exception:
                    pass
            return
        except Exception as e:
            if attempt == 2: die(f"테이블 생성 실패: {e}")
            time.sleep(2)

def split_chunks(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[i:i+chunk_size] for i in range(0, len(items), chunk_size)]

def fetch_customer_list_from_api() -> List[Dict[str, Any]]:
    ok, data = safe_call("GET", "/ncc/customers", CUSTOMER_ID)
    if not ok or not isinstance(data, list): return []
    out = []
    for r in data:
        cid = str(r.get("customerId", "")).strip()
        nm = str(r.get("customerName", "")).strip()
        if cid: out.append({"customer_id": cid, "account_name": nm})
    return out

def sync_dim_account(engine: Engine, candidates: List[Dict[str, str]]):
    rows = []
    for r in candidates:
        cid = str(r.get("customer_id", "")).strip()
        nm = str(r.get("account_name", "")).strip() or cid
        if cid: rows.append({"customer_id": cid, "account_name": nm})
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_account"))
        conn.execute(text("INSERT INTO dim_account(customer_id, account_name) VALUES (:customer_id, :account_name)"), rows)

def upsert_rows(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows: return
    cols = list(rows[0].keys())
    col_sql = ",".join(cols)
    val_sql = ",".join([f":{c}" for c in cols])
    pk_sql = ",".join(pk_cols)
    upd_cols = [c for c in cols if c not in pk_cols]
    upd_sql = ",".join([f"{c}=EXCLUDED.{c}" for c in upd_cols]) if upd_cols else ""
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql}) ON CONFLICT ({pk_sql}) DO UPDATE SET {upd_sql}" if upd_sql else f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql}) ON CONFLICT ({pk_sql}) DO NOTHING"
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def get_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id)
    if not ok or not isinstance(data, list): return []
    return data

def get_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, params={"nccCampaignId": campaign_id})
    if not ok or not isinstance(data, list): return []
    return data

def get_keywords(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/keywords", customer_id, params={"nccAdgroupId": adgroup_id})
    if not ok or not isinstance(data, list): return []
    return data

def get_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, params={"nccAdgroupId": adgroup_id})
    if not ok or not isinstance(data, list): return []
    return data

def collect_dim_for_customer(customer_id: str, account_name: str) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
    camps_raw = get_campaigns(customer_id)
    camps, adgs, kws, ads = [], [], [], []

    for c in camps_raw:
        cid = str(c.get("nccCampaignId", "")).strip()
        if not cid: continue
        camps.append({
            "customer_id": customer_id,
            "campaign_id": cid,
            "campaign_name": str(c.get("name", "")).strip(),
            "campaign_tp": str(c.get("campaignTp", "")).strip(),
            "status": str(c.get("status", "")).strip(),
        })

    def _collect_for_campaign(c):
        _adgs, _kws, _ads = [], [], []
        camp_id = c["campaign_id"]
        g_raw = get_adgroups(customer_id, camp_id)
        for g in g_raw:
            gid = str(g.get("nccAdgroupId", "")).strip()
            if not gid: continue
            _adgs.append({
                "customer_id": customer_id,
                "adgroup_id": gid,
                "adgroup_name": str(g.get("name", "")).strip(),
                "campaign_id": camp_id,
                "status": str(g.get("status", "")).strip(),
            })
            if not SKIP_KEYWORD_DIM:
                k_raw = get_keywords(customer_id, gid)
                for k in k_raw:
                    kid = str(k.get("nccKeywordId", "")).strip()
                    if not kid: continue
                    _kws.append({
                        "customer_id": customer_id,
                        "keyword_id": kid,
                        "keyword": str(k.get("keyword", "")).strip(),
                        "adgroup_id": gid,
                        "status": str(k.get("status", "")).strip(),
                    })
            if not SKIP_AD_DIM:
                a_raw = get_ads(customer_id, gid)
                for a in a_raw:
                    aid = str(a.get("nccAdId", "")).strip()
                    if not aid: continue
                    ad_name = str(a.get("name", "")).strip()
                    ad_title = ""
                    ad_desc = ""
                    thumb = ""
                    pc_url = str(a.get("pcFinalUrl", "") or a.get("pcFinalUrl2", "") or "").strip()

                    info = a.get("ad", {})
                    if isinstance(info, dict):
                        ad_title = str(info.get("title", "")).strip()
                        ad_desc = str(info.get("description", "")).strip()
                        thumb = str(info.get("imageUrl", "")).strip()

                    _ads.append({
                        "customer_id": customer_id,
                        "ad_id": aid,
                        "ad_name": ad_name,
                        "ad_title": ad_title,
                        "ad_desc": ad_desc,
                        "adgroup_id": gid,
                        "status": str(a.get("status", "")).strip(),
                        "pc_landing_url": pc_url,
                        "thumbnail_url": thumb
                    })
        return _adgs, _kws, _ads

    with concurrent.futures.ThreadPoolExecutor(max_workers=CAMPAIGN_WORKERS) as ex:
        futs = [ex.submit(_collect_for_campaign, c) for c in camps]
        for f in concurrent.futures.as_completed(futs):
            try:
                _adgs, _kws, _ads = f.result()
                adgs.extend(_adgs); kws.extend(_kws); ads.extend(_ads)
            except Exception:
                pass

    return camps, adgs, kws, ads

def get_stats_range(customer_id: str, ids: List[str], target_date: date) -> List[dict]:
    if not ids: return []
    all_rows = []
    chunks = split_chunks(ids, STATS_CHUNK_SIZE)

    def _call_chunk(chunk_ids: List[str]) -> List[dict]:
        params = {
            "ids": ",".join(chunk_ids),
            "timeRange": f"{target_date.isoformat()},{target_date.isoformat()}",
            "fields": "impCnt,clkCnt,salesAmt,ccnt,convAmt",
            "timeIncrement": "all",
            "breakdown": "no",
        }
        ok, data = safe_call("GET", "/stats", customer_id, params=params)
        if not ok: return []
        if isinstance(data, list): return data
        if isinstance(data, dict):
            return data.get("data", []) if isinstance(data.get("data"), list) else []
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=STATS_WORKERS) as ex:
        futs = [ex.submit(_call_chunk, ch) for ch in chunks]
        for f in concurrent.futures.as_completed(futs):
            try:
                rows = f.result()
                if rows: all_rows.extend(rows)
            except Exception:
                pass
    return all_rows

def normalize_stats_rows(rows: List[dict], id_key_candidates: List[str]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for r in rows:
        rid = ""
        for k in id_key_candidates:
            if k in r and r.get(k) is not None:
                rid = str(r.get(k)).strip()
                break
        if not rid and "id" in r: rid = str(r.get("id")).strip()
        if not rid: continue
        imp = int(r.get("impCnt", 0) or 0)
        clk = int(r.get("clkCnt", 0) or 0)
        cost = int(float(r.get("salesAmt", 0) or 0))
        conv = float(r.get("ccnt", 0) or 0.0)
        sales = int(float(r.get("convAmt", 0) or 0))
        if rid not in out:
            out[rid] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0}
        out[rid]["imp"] += imp
        out[rid]["clk"] += clk
        out[rid]["cost"] += cost
        out[rid]["conv"] += conv
        out[rid]["sales"] += sales
    return out

def fetch_stats_fallback(engine: Engine, customer_id: str, target_date: date, ids: List[str], pk_name: str, table_name: str):
    if not ids: return 0
    rows = get_stats_range(customer_id, ids, target_date)
    norm = normalize_stats_rows(rows, [pk_name, "id"])
    if not norm: return 0
    data_rows = []
    for _id, s in norm.items():
        cost = int(s["cost"])
        sales = int(s["sales"])
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        data_rows.append({
            "dt": target_date,
            "customer_id": customer_id,
            pk_name: _id,
            "imp": int(s["imp"]),
            "clk": int(s["clk"]),
            "cost": cost,
            "conv": float(s["conv"]),
            "sales": sales,
            "roas": roas,
            "avg_rnk": 0.0,
        })
    upsert_rows(engine, table_name, data_rows, ["dt", "customer_id", pk_name])
    return len(data_rows)

def parse_csv_report(content_bytes: bytes) -> pd.DataFrame:
    for enc in ["utf-8-sig", "cp949", "euc-kr", "utf-8"]:
        try:
            txt = content_bytes.decode(enc, errors="replace")
            return pd.read_csv(io.StringIO(txt), header=None)
        except Exception:
            continue
    return pd.DataFrame()

def create_report(customer_id: str, report_tp: str, target_date: date, ids: List[str]) -> str:
    body = {
        "reportTp": report_tp,
        "statDt": target_date.strftime("%Y%m%d"),
        "fields": ["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt", "avgRnk"],
        "timeUnit": "SUMMARY",
        "format": "CSV",
        "ids": ids
    }
    path = "/stat-reports"
    ok, data = safe_call("POST", path, customer_id, params=None)
    if ok and isinstance(data, dict) and data.get("reportJobId"):
        return str(data["reportJobId"])
    try:
        _, data = request_json("POST", path, customer_id, json_data=body, raise_error=True)
        return str(data.get("reportJobId", "")).strip()
    except Exception:
        return ""

def download_report(customer_id: str, report_job_id: str) -> pd.DataFrame | None:
    path = f"/stat-reports/{report_job_id}"
    try:
        _, data = request_json("GET", path, customer_id, raise_error=True)
        if isinstance(data, dict):
            stt = str(data.get("status", "")).upper()
            if stt in ["NONE", "ERROR"]:
                return pd.DataFrame() if stt == "NONE" else None
            if stt in ["BUILT", "BUILD", "COMPLETED", "READY", "SUCCESS"]:
                download_url = data.get("downloadUrl") or data.get("url")
                if not download_url:
                    return pd.DataFrame()
                s = get_session()
                r = s.get(download_url, timeout=TIMEOUT)
                if r.status_code >= 400:
                    return None
                return parse_csv_report(r.content)
    except Exception:
        return None
    return None

def poll_and_collect_reports(customer_id: str, target_date: date, target_campaign_ids: List[str], target_kw_ids: List[str], target_ad_ids: List[str]) -> Dict[str, pd.DataFrame | None]:
    jobs = {}
    results = {"CAMPAIGN": None, "KEYWORD": None, "AD": None}
    spec = [
        ("CAMPAIGN", target_campaign_ids),
        ("KEYWORD", target_kw_ids),
        ("AD", target_ad_ids),
    ]

    for tp, ids in spec:
        if not ids:
            results[tp] = pd.DataFrame()
            continue
        job_id = create_report(customer_id, tp, target_date, ids[:50000])
        if job_id:
            jobs[tp] = job_id
        else:
            results[tp] = None

    max_wait = 40
    while jobs and max_wait > 0:
        for tp in list(jobs.keys()):
            job_id = jobs[tp]
            path = f"/stat-reports/{job_id}"
            ok, data = safe_call("GET", path, customer_id)
            if ok and isinstance(data, dict):
                stt = str(data.get("status", "")).upper()
                if stt in ["BUILT", "BUILD", "COMPLETED", "READY", "SUCCESS"]:
                    df = download_report(customer_id, job_id)
                    if df is None:
                        results[tp] = None
                    else:
                        if isinstance(df, pd.DataFrame):
                            results[tp] = df
                        else:
                            results[tp] = pd.DataFrame()
                    safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                    del jobs[tp]
                elif stt in ["NONE", "ERROR"]:
                    results[tp] = pd.DataFrame() if stt == "NONE" else None
                    safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                    del jobs[tp]
        if jobs:
            time.sleep(1.0)
        max_wait -= 1

    for job_id in jobs.values():
        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

    return results

def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [str(h).lower().replace(" ", "").replace("_", "").replace("-", "") for h in headers]
    norm_candidates = [str(c).lower().replace(" ", "").replace("_", "").replace("-", "") for c in candidates]

    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i

    for c in norm_candidates:
        for i, h in enumerate(headers):
            hh = str(h).lower().replace(" ", "").replace("_", "").replace("-", "")
            if c in hh and "그룹" not in hh:
                return i
    return -1

def safe_float(v) -> float:
    if pd.isna(v): return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-": return 0.0
    try: return float(s)
    except Exception: return 0.0

def parse_df_combined(df: pd.DataFrame, report_tp: str, pk_cands: List[str], has_rank: bool = False) -> dict:
    if df is None or df.empty: return {}
    header_idx = -1
    for i in range(min(5, len(df))):
        row_vals = [str(x).replace(" ", "").lower() for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in pk_cands):
            header_idx = i
            break

    if header_idx != -1:
        headers = [str(x).lower().replace(" ", "").replace("_", "") for x in df.iloc[header_idx].fillna("")]
        df = df.iloc[header_idx+1:].reset_index(drop=True)
        pk_idx = get_col_idx(headers, pk_cands)
        conv_idx = get_col_idx(headers, ["전환수", "conversions", "ccnt"])
        sales_idx = get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"])
        imp_idx = get_col_idx(headers, ["노출수", "impressions", "impcnt"])
        clk_idx = get_col_idx(headers, ["클릭수", "clicks", "clkcnt"])
        cost_idx = get_col_idx(headers, ["총비용", "cost", "salesamt"])
        rank_idx = get_col_idx(headers, [
            "평균노출순위",
            "평균노출순위(검색)",
            "노출순위",
            "averageposition",
            "average_position",
            "averageexposurerank",
            "avgexposurerank",
            "avgrnk",
        ])
    else:
        if "CAMPAIGN" in report_tp: pk_idx = 2
        elif "KEYWORD" in report_tp: pk_idx = 5
        elif "AD" in report_tp: pk_idx = 5
        else: return {}
        imp_idx = 5 if "CAMPAIGN" in report_tp else 8
        clk_idx = 6 if "CAMPAIGN" in report_tp else 9
        cost_idx = 7 if "CAMPAIGN" in report_tp else 10
        conv_idx = -1
        sales_idx = -1
        rank_idx = 11

    if pk_idx == -1: return {}

    res = {}
    for _, r in df.iterrows():
        try:
            if len(r) <= pk_idx: continue
            obj_id = str(r.iloc[pk_idx]).strip()
            if not obj_id or obj_id == '-': continue

            if obj_id not in res:
                res[obj_id] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}

            imp = 0
            if imp_idx != -1 and len(r) > imp_idx:
                imp = int(safe_float(r.iloc[imp_idx]))
                res[obj_id]["imp"] += imp
            if clk_idx != -1 and len(r) > clk_idx:
                res[obj_id]["clk"] += int(safe_float(r.iloc[clk_idx]))
            if cost_idx != -1 and len(r) > cost_idx:
                res[obj_id]["cost"] += int(safe_float(r.iloc[cost_idx]))
            if conv_idx != -1 and len(r) > conv_idx:
                res[obj_id]["conv"] += safe_float(r.iloc[conv_idx])
            if sales_idx != -1 and len(r) > sales_idx:
                res[obj_id]["sales"] += int(safe_float(r.iloc[sales_idx]))

            if has_rank and rank_idx != -1 and len(r) > rank_idx:
                rnk = safe_float(r.iloc[rank_idx])
                if rnk > 0 and imp > 0:
                    res[obj_id]["rank_sum"] += (rnk * imp)
                    res[obj_id]["rank_cnt"] += imp
        except Exception: pass
    return res

def merge_and_save_combined(engine: Engine, customer_id: str, target_date: date, table_name: str, pk_name: str, stat_res: dict) -> int:
    if not stat_res: return 0
    rows = []
    for k, s in stat_res.items():
        cost = int(s.get("cost", 0))
        sales = int(s.get("sales", 0))
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (s.get("rank_sum", 0) / s.get("rank_cnt", 1)) if s.get("rank_cnt", 0) > 0 else 0.0
        row = {
            "dt": target_date,
            "customer_id": customer_id,
            pk_name: str(k),
            "imp": int(s.get("imp", 0)),
            "clk": int(s.get("clk", 0)),
            "cost": cost,
            "conv": float(s.get("conv", 0.0)),
            "sales": sales,
            "roas": roas,
            "avg_rnk": round(avg_rnk, 2)
        }
        rows.append(row)
    upsert_rows(engine, table_name, rows, ["dt", "customer_id", pk_name])
    return len(rows)

def fetch_all_competitor_keywords(customer_id: str, adgroup_ids: List[str], target_date: date) -> List[dict]:
    if not adgroup_ids:
        return []
    out_rows = []
    chunks = split_chunks(adgroup_ids, 20)

    def _fetch_chunk(chunk):
        local = []
        for agid in chunk:
            ok, data = safe_call("GET", "/ncc/keywords", customer_id, params={"nccAdgroupId": agid})
            if not ok or not isinstance(data, list):
                continue
            for kw in data:
                kid = str(kw.get("nccKeywordId", "")).strip()
                kw_text = str(kw.get("keyword", "")).strip()
                if not kid or not kw_text:
                    continue
                ok2, comp = safe_call("GET", f"/keywords/{kid}/ads", customer_id)
                if not ok2 or not isinstance(comp, list):
                    continue
                for item in comp:
                    rank = int(item.get("rank", 0) or 0)
                    ad_name = str(item.get("adName", "")).strip()
                    domain = str(item.get("domain", "")).strip()
                    m_url = str(item.get("mobileUrl", "")).strip()
                    p_url = str(item.get("pcUrl", "")).strip()
                    bid = int(float(item.get("bidPrice", 0) or 0))
                    nid = str(item.get("naverId", "")).strip()
                    if rank <= 0:
                        continue
                    local.append({
                        "dt": target_date,
                        "customer_id": customer_id,
                        "adgroup_id": agid,
                        "keyword_id": kid,
                        "keyword": kw_text,
                        "rank": rank,
                        "ad_name": ad_name,
                        "domain": domain,
                        "mobile_url": m_url,
                        "pc_url": p_url,
                        "bid_price": bid,
                        "naver_id": nid
                    })
        return local

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(_fetch_chunk, ch) for ch in chunks]
        for f in concurrent.futures.as_completed(futs):
            try:
                rr = f.result()
                if rr:
                    out_rows.extend(rr)
            except Exception:
                pass
    return out_rows

def save_competitor_keywords(engine: Engine, rows: List[dict]):
    if not rows:
        return
    upsert_rows(
        engine,
        "fact_competitor_keyword",
        rows,
        ["dt", "customer_id", "adgroup_id", "keyword_id", "rank"]
    )

def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool=False):
    try:
        log(f"▶️ [ {account_name} / {customer_id} ] 처리 시작")

        camps, adgs, kws, ads = [], [], [], []
        if not skip_dim:
            camps, adgs, kws, ads = collect_dim_for_customer(customer_id, account_name)
            if camps: upsert_rows(engine, "dim_campaign", camps, ["customer_id", "campaign_id"])
            if adgs: upsert_rows(engine, "dim_adgroup", adgs, ["customer_id", "adgroup_id"])
            if kws: upsert_rows(engine, "dim_keyword", kws, ["customer_id", "keyword_id"])
            if ads: upsert_rows(engine, "dim_ad", ads, ["customer_id", "ad_id"])
            upsert_rows(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])
            log(f"   ✅ DIM 저장: 캠페인({len(camps)})/광고그룹({len(adgs)})/키워드({len(kws)})/소재({len(ads)})")

        target_campaign_ids = [c["campaign_id"] for c in camps] if camps else []
        target_kw_ids = [k["keyword_id"] for k in kws] if kws else []
        target_ad_ids = [a["ad_id"] for a in ads] if ads else []

        if not target_campaign_ids:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid"), {"cid": customer_id})
                target_campaign_ids = [str(r[0]) for r in res]
        if not target_kw_ids and not SKIP_KEYWORD_STATS:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid"), {"cid": customer_id})
                target_kw_ids = [str(r[0]) for r in res]
        if not target_ad_ids and not SKIP_AD_STATS:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid"), {"cid": customer_id})
                target_ad_ids = [str(r[0]) for r in res]

        c_cnt = k_cnt = a_cnt = 0

        dfs = poll_and_collect_reports(customer_id, target_date, target_campaign_ids, target_kw_ids, target_ad_ids)

        camp_stat = {}
        if dfs.get("CAMPAIGN") is not None:
            camp_stat = parse_df_combined(dfs["CAMPAIGN"], "CAMPAIGN", ["캠페인id", "campaignid"], has_rank=True)
        else:
            if target_campaign_ids:
                raw_camp_stats = get_stats_range(customer_id, target_campaign_ids, target_date)
                for r in raw_camp_stats:
                    eid = str(r.get("id"))
                    cost = int(float(r.get("salesAmt", 0) or 0))
                    sales = int(float(r.get("convAmt", 0) or 0))
                    camp_stat[eid] = {
                        "imp": int(r.get("impCnt", 0) or 0),
                        "clk": int(r.get("clkCnt", 0) or 0),
                        "cost": cost,
                        "conv": float(r.get("ccnt", 0) or 0),
                        "sales": sales,
                        "rank_sum": 0.0, "rank_cnt": 0
                    }

        if camp_stat:
            c_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_campaign_daily", "campaign_id", camp_stat)
        else:
            c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_campaign_ids, "campaign_id", "fact_campaign_daily")

        kw_stat = {}
        if not SKIP_KEYWORD_STATS:
            if dfs.get("KEYWORD") is not None:
                kw_stat = parse_df_combined(dfs["KEYWORD"], "KEYWORD", ["키워드id", "keywordid"], has_rank=True)
                k_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_keyword_daily", "keyword_id", kw_stat)
            else:
                k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily") if not SKIP_KEYWORD_STATS else 0

        ad_stat = {}
        if dfs.get("AD") is not None:
            ad_stat = parse_df_combined(dfs["AD"], "AD", ["광고id", "소재id", "adid", "상품id", "productid", "itemid"], has_rank=True)
        else:
            if target_ad_ids and not SKIP_AD_STATS:
                raw_ad_stats = get_stats_range(customer_id, target_ad_ids, target_date)
                for r in raw_ad_stats:
                    eid = str(r.get("id"))
                    cost = int(float(r.get("salesAmt", 0) or 0))
                    sales = int(float(r.get("convAmt", 0) or 0))
                    ad_stat[eid] = {
                        "imp": int(r.get("impCnt", 0) or 0),
                        "clk": int(r.get("clkCnt", 0) or 0),
                        "cost": cost,
                        "conv": float(r.get("ccnt", 0) or 0),
                        "sales": sales,
                        "rank_sum": 0.0, "rank_cnt": 0
                    }

        ext_ids = []
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid AND ad_title LIKE '[%'"), {"cid": customer_id})
                ext_ids = [str(r[0]) for r in res]
        except Exception: pass

        if ext_ids:
            ext_stats_raw = get_stats_range(customer_id, ext_ids, target_date)
            for r in ext_stats_raw:
                eid = str(r.get("id"))
                if eid not in ad_stat: ad_stat[eid] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
                ad_stat[eid]["imp"] += int(r.get("impCnt", 0) or 0)
                ad_stat[eid]["clk"] += int(r.get("clkCnt", 0) or 0)
                ad_stat[eid]["cost"] += int(float(r.get("salesAmt", 0) or 0))
                ad_stat[eid]["conv"] += float(r.get("ccnt", 0) or 0)
                ad_stat[eid]["sales"] += int(float(r.get("convAmt", 0) or 0))

        if ad_stat:
            a_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_ad_daily", "ad_id", ad_stat)

        log(f"   📊 [ {account_name} ] 적재 완료: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")

    except Exception as e:
        log(f"❌ [ {account_name} ] 계정 처리 중 오류 발생: {str(e)}")

def main():
    engine = get_engine()
    ensure_tables(engine)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--skip_dim", action="store_true")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)

    print("\n" + "="*50, flush=True)
    print(f"🚀🚀🚀 [ 현재 수 진행 날짜: {target_date} ] 🚀🚀🚀", flush=True)
    print("="*50 + "\n", flush=True)

    accounts_info = []
    if args.customer_id:
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        if os.path.exists("accounts.xlsx"):
            df_acc = None
            try: df_acc = pd.read_excel("accounts.xlsx")
            except Exception as e:
                log(f"⚠️ accounts.xlsx 파싱 실패 (Excel): {e}")
                try: df_acc = pd.read_csv("accounts.xlsx")
                except Exception: pass

            if df_acc is not None:
                id_col, name_col = None, None
                for c in df_acc.columns:
                    c_clean = str(c).replace(" ", "").lower()
                    if c_clean in ["커스텀id", "customerid", "customer_id", "id"]: id_col = c
                    if c_clean in ["업체명", "accountname", "account_name", "name"]: name_col = c

                if id_col and name_col:
                    seen_ids = set()
                    for _, row in df_acc.iterrows():
                        cid = str(row[id_col]).strip()
                        if cid and cid.lower() != 'nan' and cid not in seen_ids:
                            accounts_info.append({"id": cid, "name": str(row[name_col])})
                            seen_ids.add(cid)
                    log(f"🟢 accounts.xlsx 엑셀 파일에서 중복을 제외한 {len(accounts_info)}개 업체를 완벽하게 불러왔습니다.")

        if not accounts_info:
            try:
                with engine.connect() as conn:
                    accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT customer_id, MAX(account_name) FROM accounts WHERE customer_id IS NOT NULL GROUP BY customer_id"))]
            except Exception: pass
        if not accounts_info and CUSTOMER_ID: accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info:
        log("⚠️ 수집할 계정이 없습니다.")
        return

    log(f"📋 최종 수집 대상 계정: {len(accounts_info)}개 / 동시 작업: {args.workers}개")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_account, engine, acc["id"], acc["name"], target_date, args.skip_dim) for acc in accounts_info]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception: pass

if __name__ == "__main__":
    main()
