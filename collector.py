# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v4.23 - Ad Creative(소재내용) Fill Fix)

✅ 해결된 문제(추가):
1. [소재내용(dim_ad.ad_name) 공란]
   - /ncc/ads 응답은 광고 타입에 따라 소재 문구가 최상위가 아니라 `ad`(중첩 객체) 안에 들어오는 경우가 많습니다.
   - 기존 코드는 ad.get("name") / ad.get("title")만 읽어서, 대부분의 계정에서 소재내용이 비었습니다.
   - 수정: 광고 객체에서 headline/title/description/landingUrl 등을 폭넓게 추출해 `creative_text`를 만들고,
          dim_ad에 저장하도록 변경했습니다.
   - 대시보드에서 "소재내용"은 dim_ad.creative_text(또는 ad_name)로 조인해서 표시하면 됩니다.

2. [ROAS & Sales Fix 유지]
   - cost = salesAmt, sales = convAmt, roas = (sales/cost*100)
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
from urllib.parse import urlsplit  # (unused) but safe to keep
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path as _Path

# -------------------------
# util
# -------------------------
def die(msg: str):
    raise SystemExit(msg)

def _mask(s: str) -> str:
    if not s:
        return ""
    if len(s) <= 8:
        return s[:2] + "***"
    return s[:4] + "***" + s[-4:]

def _norm(s: str) -> str:
    return str(s or "").strip().lower()

def _pick(d: dict, keys: List[str], default=""):
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        v = str(v).strip()
        if v:
            return v
    return default

def _join_nonempty(parts: List[str], sep=" | ") -> str:
    out = []
    for p in parts:
        p = str(p or "").strip()
        if p:
            out.append(p)
    return sep.join(out)

# -------------------------
# .env 로딩
# -------------------------
def _load_env() -> str:
    candidates: List[_Path] = []
    try:
        script_dir = _Path(__file__).resolve().parent
        candidates += [script_dir / ".env", script_dir / "env.env", script_dir / "env.txt"]
    except Exception:
        pass
    cwd = _Path.cwd()
    candidates += [cwd / ".env", cwd / "env.env", cwd / "env.txt"]
    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=True)
            print(f"✅ env loaded: {p}")
            return str(p)
    load_dotenv(override=True)
    return ""

_ENV_FILE = _load_env()

# -------------------------
# ENV
# -------------------------
API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_ADS_LICENSE") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()

ENV_START_DT = os.getenv("START_DT", "").strip()
ENV_END_DT = os.getenv("END_DT", "").strip()
ENV_FORCE_DT = os.getenv("FORCE_DT", "").strip()

DIM_TTL_HOURS = int(os.getenv("DIM_TTL_HOURS", "24"))
FORCE_DIM_ENV = os.getenv("FORCE_DIM", "").strip() == "1"

SKIP_KEYWORD_DIM = os.getenv("SKIP_KEYWORD_DIM", "").strip() == "1"
SKIP_AD_DIM = os.getenv("SKIP_AD_DIM", "").strip() == "1"
SKIP_KEYWORD_STATS = os.getenv("SKIP_KEYWORD_STATS", "").strip() == "1"
SKIP_AD_STATS = os.getenv("SKIP_AD_STATS", "").strip() == "1"

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.05"))
CHUNK_INSERT = int(os.getenv("CHUNK_INSERT", "2000"))
IDS_CHUNK = int(os.getenv("IDS_CHUNK", "100"))

print(
    f"ENV_FILE={_ENV_FILE or '(default)'} | "
    f"API_KEY={_mask(API_KEY)} | API_SECRET={_mask(API_SECRET)} | "
    f"DB_URL={'set' if bool(DB_URL) else 'missing'}"
)
if not API_KEY or not API_SECRET:
    die("❌ API 키/시크릿이 비어있습니다.")

# -------------------------
# Signature
# -------------------------
def now_millis() -> str:
    return str(int(time.time() * 1000))

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

# -------------------------
# DB helpers
# -------------------------
def get_engine() -> Engine:
    if not DB_URL:
        die("DATABASE_URL is required")
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

def exec_sql(engine: Engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def ensure_tables(engine: Engine):
    # DIM tables
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT NOT NULL);""")
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS blocked_adgroups (customer_id TEXT, adgroup_id TEXT, code TEXT, last_seen TIMESTAMPTZ, PRIMARY KEY(customer_id, adgroup_id));""")
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS etl_state (customer_id TEXT PRIMARY KEY, last_dim_refresh TIMESTAMPTZ);""")
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id));""")
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE dim_campaign ADD COLUMN IF NOT EXISTS campaign_tp TEXT"))
    except Exception:
        pass

    exec_sql(engine, """CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id));""")
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id));""")

    # ✅ dim_ad 확장: 소재내용/랜딩 등을 저장
    exec_sql(engine, """CREATE TABLE IF NOT EXISTS dim_ad (
        customer_id TEXT,
        ad_id TEXT,
        adgroup_id TEXT,
        ad_name TEXT,
        status TEXT,
        ad_title TEXT,
        ad_desc TEXT,
        pc_landing_url TEXT,
        mobile_landing_url TEXT,
        creative_text TEXT,
        PRIMARY KEY(customer_id, ad_id)
    );""")
    # 기존 테이블에 컬럼 추가(있으면 무시)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS ad_title TEXT"))
            conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS ad_desc TEXT"))
            conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS pc_landing_url TEXT"))
            conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS mobile_landing_url TEXT"))
            conn.execute(text("ALTER TABLE dim_ad ADD COLUMN IF NOT EXISTS creative_text TEXT"))
    except Exception:
        pass

    # FACT tables - ROAS 컬럼 추가
    exec_sql(engine, """
    CREATE TABLE IF NOT EXISTS fact_campaign_daily (
        dt DATE, customer_id TEXT, campaign_id TEXT,
        imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0,
        roas DOUBLE PRECISION DEFAULT 0,
        PRIMARY KEY(dt, customer_id, campaign_id)
    );
    """)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE fact_campaign_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
    except Exception:
        pass

    exec_sql(engine, """
    CREATE TABLE IF NOT EXISTS fact_keyword_daily (
        dt DATE, customer_id TEXT, keyword_id TEXT,
        imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0,
        roas DOUBLE PRECISION DEFAULT 0,
        PRIMARY KEY(dt, customer_id, keyword_id)
    );
    """)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE fact_keyword_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
    except Exception:
        pass

    exec_sql(engine, """
    CREATE TABLE IF NOT EXISTS fact_ad_daily (
        dt DATE, customer_id TEXT, ad_id TEXT,
        imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0,
        roas DOUBLE PRECISION DEFAULT 0,
        PRIMARY KEY(dt, customer_id, ad_id)
    );
    """)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE fact_ad_daily ADD COLUMN IF NOT EXISTS roas DOUBLE PRECISION DEFAULT 0"))
    except Exception:
        pass

    exec_sql(engine, """
    CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
        dt DATE, customer_id TEXT, bizmoney_balance BIGINT, budget BIGINT,
        PRIMARY KEY(dt, customer_id)
    );
    """)

def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows:
        return
    cols = list(rows[0].keys())
    col_list = ", ".join([f'"{c}"' for c in cols])
    bind_list = ", ".join([f":{c}" for c in cols])
    pk_list = ", ".join([f'"{c}"' for c in pk_cols])
    update_cols = [c for c in cols if c not in pk_cols]
    set_list = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols]) if update_cols else ""
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({bind_list}) '
    sql += f"ON CONFLICT ({pk_list}) DO "
    sql += f"UPDATE SET {set_list}" if set_list else "NOTHING"
    with engine.begin() as conn:
        for i in range(0, len(rows), CHUNK_INSERT):
            conn.execute(text(sql), rows[i:i + CHUNK_INSERT])

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date, d2: date):
    if not rows:
        return
    exec_sql(engine, f'DELETE FROM "{table}" WHERE customer_id=:cid AND dt BETWEEN :d1 AND :d2',
             {"cid": str(customer_id), "d1": str(d1), "d2": str(d2)})
    cols = list(rows[0].keys())
    col_list = ", ".join([f'"{c}"' for c in cols])
    bind_list = ", ".join([f":{c}" for c in cols])
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({bind_list})'
    with engine.begin() as conn:
        for i in range(0, len(rows), CHUNK_INSERT):
            conn.execute(text(sql), rows[i:i + CHUNK_INSERT])

# -------------------------
# API Core
# -------------------------
def request_json(method: str, path: str, customer_id: str, params: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    with requests.Session() as session:
        req = requests.Request(method, BASE_URL + path, params=params)
        prepped = session.prepare_request(req)

        # ✅ Signature는 querystring 제외 → 순수 path만 사용
        headers = make_headers(method, path, customer_id)
        prepped.headers.update(headers)

        try:
            r = session.send(prepped, timeout=TIMEOUT)
            try:
                data = r.json()
            except Exception:
                data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} {data}", response=r)
            return r.status_code, data
        except Exception as e:
            if raise_error:
                raise e
            return 0, str(e)

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except requests.HTTPError as e:
        return False, str(e)

# -------------------------
# Entity list
# -------------------------
def list_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id, None)
    time.sleep(SLEEP_BETWEEN_CALLS)
    return data if ok and isinstance(data, list) else []

def list_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": campaign_id})
    time.sleep(SLEEP_BETWEEN_CALLS)
    return data if ok and isinstance(data, list) else []

def list_keywords(customer_id: str, adgroup_id: str) -> Tuple[bool, Any]:
    ok, data = safe_call("GET", "/ncc/keywords", customer_id, {"nccAdgroupId": adgroup_id})
    time.sleep(SLEEP_BETWEEN_CALLS)
    return ok, data

def list_ads(customer_id: str, adgroup_id: str) -> Tuple[bool, Any]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": adgroup_id})
    time.sleep(SLEEP_BETWEEN_CALLS)
    return ok, data

# -------------------------
# ✅ Ad(소재) 텍스트 추출
# -------------------------
def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    """
    /ncc/ads 응답에서 광고 타입별로 소재 문구/랜딩 정보를 최대한 뽑아냅니다.
    - 어떤 계정은 최상위에 title/name이 없고, ad_obj["ad"] 안에 headline/description/landingUrl 등이 들어옵니다.
    """
    ad_inner = ad_obj.get("ad") if isinstance(ad_obj.get("ad"), dict) else {}
    # 최상위/중첩 둘 다에서 후보 수집
    title = _pick(ad_obj, ["name", "title", "headline", "subject", "adName"], "") or _pick(ad_inner, ["headline", "title", "subject", "name"], "")
    desc = _pick(ad_obj, ["description", "desc", "adDescription"], "") or _pick(ad_inner, ["description", "desc", "adDescription"], "")

    # 랜딩 URL 후보: 계정/광고타입마다 키가 다를 수 있어 넓게 잡음
    pc_url = _pick(ad_obj, ["pcLandingUrl", "pcFinalUrl", "finalUrl", "landingUrl", "linkUrl"], "") or _pick(ad_inner, ["pcLandingUrl", "pcFinalUrl", "finalUrl", "landingUrl", "linkUrl"], "")
    m_url = _pick(ad_obj, ["mobileLandingUrl", "mobileFinalUrl", "mobileUrl", "mLandingUrl"], "") or _pick(ad_inner, ["mobileLandingUrl", "mobileFinalUrl", "mobileUrl", "mLandingUrl"], "")

    creative_text = _join_nonempty([title, desc, pc_url or m_url])

    return {
        "ad_title": title,
        "ad_desc": desc,
        "pc_landing_url": pc_url,
        "mobile_landing_url": m_url,
        "creative_text": creative_text,
    }

# -------------------------
# Stats (/stats)
# -------------------------
_STATS_FIELDS = ["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"]

def _fetch_stats_chunk_recursive(customer_id: str, ids: List[str], fields_json: str, time_range: str, depth=0) -> List[dict]:
    if not ids:
        return []

    params = {"ids": ids, "fields": fields_json, "timeRange": time_range}
    status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)

    if status == 200:
        time.sleep(SLEEP_BETWEEN_CALLS)
        sys.stdout.write(".")
        sys.stdout.flush()
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        return []

    if len(ids) == 1:
        sys.stdout.write("x")
        sys.stdout.flush()
        return []

    mid = len(ids) // 2
    left = ids[:mid]
    right = ids[mid:]
    if depth < 1:
        msg = str(data)[:200] if data else "Unknown"
        print(f"\n   ♻️ [오류 {status}] {msg} → {len(left)}/{len(right)}개로 쪼개서 재시도 중...", end="")

    return _fetch_stats_chunk_recursive(customer_id, left, fields_json, time_range, depth + 1) + \
        _fetch_stats_chunk_recursive(customer_id, right, fields_json, time_range, depth + 1)

def get_stats_range(customer_id: str, ids: List[str], d1: date, d2: date) -> List[dict]:
    out: List[dict] = []

    valid_ids = []
    for x in ids:
        s = str(x).strip()
        if not s or s.lower() == "nan":
            continue
        if s.startswith("nkw-") or s.startswith("nad-") or s.startswith("cmp-"):
            valid_ids.append(s)

    if not valid_ids:
        return out

    time_range = json.dumps({"since": str(d1), "until": str(d2)}, ensure_ascii=False, separators=(",", ":"))
    fields = json.dumps(_STATS_FIELDS, ensure_ascii=False, separators=(",", ":"))

    print(f"  [총 {len(valid_ids)}개 ID를 {IDS_CHUNK}개씩 처리 중]", end=" ")

    for i in range(0, len(valid_ids), IDS_CHUNK):
        chunk = valid_ids[i : i + IDS_CHUNK]
        results = _fetch_stats_chunk_recursive(customer_id, chunk, fields, time_range)
        out.extend(results)

    print(" 완료!")
    return out

# -------------------------
# FACT Helper: Parse & Calculate ROAS
# -------------------------
def _parse_and_calc_roas(r: dict, d_str: str, customer_id: str, id_key: str) -> dict:
    """API 응답(r)에서 공통 필드를 추출하고 ROAS를 계산하여 반환"""

    # ✅ 중요: Naver API "salesAmt" = Cost(비용), "convAmt" = Sales(매출)
    cost = int(float(r.get("salesAmt", 0) or 0))
    sales = int(float(r.get("convAmt", 0) or 0))

    # ✅ ROAS 계산
    roas = (sales / cost * 100) if cost > 0 else 0.0

    stat_dt = r.get("statDt") or r.get("date") or r.get("dt") or d_str

    return {
        "dt": str(stat_dt),
        "customer_id": str(customer_id),
        id_key: str(r.get("id")),
        "imp": int(r.get("impCnt", 0) or 0),
        "clk": int(r.get("clkCnt", 0) or 0),
        "cost": cost,
        "conv": float(r.get("ccnt", 0) or 0),
        "sales": sales,
        "roas": roas,
    }

# -------------------------
# Main Logic
# -------------------------
def refresh_fact_for_account_daily(engine: Engine, customer_id: str, account_name: str, target_date: date):
    d_str = str(target_date)
    print(f"\n--- 📅 날짜: {d_str} 수집 시작 ---")

    # 1. Campaign
    camp = pd.read_sql(text("SELECT campaign_id FROM dim_campaign WHERE customer_id=:cid"), engine, params={"cid": str(customer_id)})
    camp_ids = camp["campaign_id"].astype(str).tolist() if not camp.empty else []
    if camp_ids:
        print(f"  >> 캠페인 {len(camp_ids)}개 성과 수집...", end="")
        data = get_stats_range(customer_id, camp_ids, target_date, target_date)
        rows = [_parse_and_calc_roas(r, d_str, customer_id, "campaign_id") for r in data]
        if rows:
            replace_fact_range(engine, "fact_campaign_daily", rows, customer_id, target_date, target_date)

    # 2. Keyword
    if not SKIP_KEYWORD_STATS:
        kw = pd.read_sql(
            text(
                "SELECT k.keyword_id FROM dim_keyword k "
                "JOIN dim_adgroup g ON k.adgroup_id=g.adgroup_id "
                "JOIN dim_campaign c ON g.campaign_id=c.campaign_id "
                "WHERE c.customer_id=:cid AND (c.campaign_tp IN ('WEB_SITE','POWER_CONTENT') OR c.campaign_tp IS NULL)"
            ),
            engine,
            params={"cid": str(customer_id)},
        )
        kw_ids = kw["keyword_id"].astype(str).tolist() if not kw.empty else []
        if kw_ids:
            print(f"  >> 키워드 {len(kw_ids)}개 성과 수집...", end="")
            data = get_stats_range(customer_id, kw_ids, target_date, target_date)
            rows = [_parse_and_calc_roas(r, d_str, customer_id, "keyword_id") for r in data]
            if rows:
                replace_fact_range(engine, "fact_keyword_daily", rows, customer_id, target_date, target_date)

    # 3. Ad
    if not SKIP_AD_STATS:
        ad = pd.read_sql(
            text(
                "SELECT a.ad_id FROM dim_ad a "
                "JOIN dim_adgroup g ON a.adgroup_id=g.adgroup_id "
                "JOIN dim_campaign c ON g.campaign_id=c.campaign_id "
                "WHERE c.customer_id=:cid AND (c.campaign_tp IN ('WEB_SITE','POWER_CONTENT') OR c.campaign_tp IS NULL)"
            ),
            engine,
            params={"cid": str(customer_id)},
        )
        ad_ids = ad["ad_id"].astype(str).tolist() if not ad.empty else []
        if ad_ids:
            print(f"  >> 소재 {len(ad_ids)}개 성과 수집...", end="")
            data = get_stats_range(customer_id, ad_ids, target_date, target_date)
            rows = [_parse_and_calc_roas(r, d_str, customer_id, "ad_id") for r in data]
            if rows:
                replace_fact_range(engine, "fact_ad_daily", rows, customer_id, target_date, target_date)

def should_refresh_dim(engine: Engine, customer_id: str, force_dim: bool) -> bool:
    if force_dim:
        return True
    try:
        ck = pd.read_sql(text("SELECT 1 FROM dim_keyword WHERE customer_id=:cid LIMIT 1"), engine, params={"cid": str(customer_id)})
        ca = pd.read_sql(text("SELECT 1 FROM dim_ad WHERE customer_id=:cid LIMIT 1"), engine, params={"cid": str(customer_id)})
        if ck.empty or ca.empty:
            return True
    except Exception:
        pass
    df = pd.read_sql(text("SELECT last_dim_refresh FROM etl_state WHERE customer_id=:cid"), engine, params={"cid": str(customer_id)})
    if df.empty or df.iloc[0, 0] is None:
        return True
    last = df.iloc[0, 0]
    if isinstance(last, str):
        try:
            last = datetime.fromisoformat(last)
        except Exception:
            return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() > DIM_TTL_HOURS * 3600

def mark_dim_refreshed(engine: Engine, customer_id: str):
    exec_sql(engine, "INSERT INTO etl_state(customer_id, last_dim_refresh) VALUES(:cid, NOW()) ON CONFLICT (customer_id) DO UPDATE SET last_dim_refresh=NOW()", {"cid": str(customer_id)})

def load_blocked(engine: Engine, customer_id: str) -> set[str]:
    df = pd.read_sql(text("SELECT adgroup_id FROM blocked_adgroups WHERE customer_id=:cid"), engine, params={"cid": str(customer_id)})
    return set(df["adgroup_id"].astype(str).tolist()) if not df.empty else set()

def cache_blocked(engine: Engine, customer_id: str, adgroup_id: str, code: str):
    exec_sql(
        engine,
        "INSERT INTO blocked_adgroups(customer_id, adgroup_id, code, last_seen) VALUES(:cid, :gid, :code, NOW()) ON CONFLICT (customer_id, adgroup_id) DO UPDATE SET code=EXCLUDED.code, last_seen=NOW()",
        {"cid": str(customer_id), "gid": str(adgroup_id), "code": str(code)},
    )

def refresh_dim_for_account(engine: Engine, customer_id: str, account_name: str):
    print(f"=== {account_name} ({customer_id}) DIM refresh ===")
    campaigns = list_campaigns(customer_id)
    print(f" > 총 {len(campaigns)}개의 캠페인 발견됨.")
    camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []
    blocked = load_blocked(engine, customer_id)

    for i, c in enumerate(campaigns, 1):
        camp_id = str(c.get("nccCampaignId") or c.get("campaignId") or c.get("id") or "")
        c_name = c.get("name") or c.get("campaignName") or ""
        c_tp = c.get("campaignTp") or ""
        if not camp_id:
            continue

        print(f"  [{i}/{len(campaigns)}] 캠페인: {c_name} ({c_tp})")
        camp_rows.append({"customer_id": str(customer_id), "campaign_id": camp_id, "campaign_name": c_name, "campaign_tp": c_tp, "status": c.get("status") or ""})

        adgroups = list_adgroups(customer_id, camp_id)
        for g in adgroups:
            gid = str(g.get("nccAdgroupId") or g.get("adgroupId") or g.get("id") or "")
            if not gid:
                continue
            ag_rows.append({"customer_id": str(customer_id), "campaign_id": camp_id, "adgroup_id": gid, "adgroup_name": g.get("name") or g.get("adgroupName") or "", "status": g.get("status") or ""})
            if gid in blocked:
                continue

            if not SKIP_KEYWORD_DIM:
                ok, data = list_keywords(customer_id, gid)
                if ok and isinstance(data, list):
                    for kw in data:
                        kid = kw.get("nccKeywordId") or kw.get("keywordId") or kw.get("id")
                        if kid:
                            kw_rows.append({"customer_id": str(customer_id), "adgroup_id": gid, "keyword_id": str(kid), "keyword": kw.get("keyword") or "", "status": kw.get("status") or ""})
                else:
                    if "1018" in str(data) or "No permission" in str(data):
                        cache_blocked(engine, customer_id, gid, "1018")

            if not SKIP_AD_DIM:
                ok, data = list_ads(customer_id, gid)
                if ok and isinstance(data, list):
                    for ad in data:
                        aid = ad.get("nccAdId") or ad.get("adId") or ad.get("id")
                        if not aid:
                            continue

                        fields = extract_ad_creative_fields(ad)
                        # ad_name은 예전 호환용: title 우선
                        ad_name = fields["ad_title"] or (ad.get("name") or ad.get("title") or "")

                        ad_rows.append(
                            {
                                "customer_id": str(customer_id),
                                "adgroup_id": gid,
                                "ad_id": str(aid),
                                "ad_name": ad_name,
                                "status": ad.get("status") or "",
                                "ad_title": fields["ad_title"],
                                "ad_desc": fields["ad_desc"],
                                "pc_landing_url": fields["pc_landing_url"],
                                "mobile_landing_url": fields["mobile_landing_url"],
                                "creative_text": fields["creative_text"],
                            }
                        )
                else:
                    if "1018" in str(data) or "No permission" in str(data):
                        cache_blocked(engine, customer_id, gid, "1018")
        time.sleep(SLEEP_BETWEEN_CALLS)

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    if not SKIP_KEYWORD_DIM:
        upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
    if not SKIP_AD_DIM:
        upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    mark_dim_refreshed(engine, customer_id)
    print(f" -> DB 저장: 캠페인 {len(camp_rows)}, 그룹 {len(ag_rows)}, 키워드 {len(kw_rows)}, 소재 {len(ad_rows)}")

def fetch_and_save_bizmoney(engine: Engine, customer_id: str):
    ok, data = safe_call("GET", "/billing/bizmoney", customer_id)
    if ok and isinstance(data, dict):
        bizmoney = int(data.get("bizmoney", 0) or 0)
        budget = int(data.get("budget", 0) or 0)
        upsert_many(engine, "fact_bizmoney_daily", [{"dt": str(date.today()), "customer_id": str(customer_id), "bizmoney_balance": bizmoney, "budget": budget}], ["dt", "customer_id"])
        print(f"  $ 비즈머니: {bizmoney:,}원")

def load_accounts(engine: Engine, xlsx_path: str, filter_str: str | None) -> List[Tuple[str, str]]:
    if xlsx_path and os.path.exists(xlsx_path):
        df = pd.read_excel(xlsx_path)
        cols = {str(c).strip().lower().replace(" ", ""): c for c in df.columns}
        cid_col = next((cols[k] for k in cols if "customer" in k or "커스텀" in k), None)
        name_col = next((cols[k] for k in cols if "name" in k or "업체" in k or "계정" in k), None)
        if cid_col and name_col:
            df = df[[cid_col, name_col]].dropna()
            rows = [{"customer_id": str(int(r[cid_col])), "account_name": str(r[name_col]).strip()} for _, r in df.iterrows()]
            upsert_many(engine, "dim_account", rows, ["customer_id"])
            return [(r["customer_id"], r["account_name"]) for r in rows if not filter_str or filter_str in r["account_name"]]

    df = pd.read_sql(text("SELECT customer_id, account_name FROM dim_account"), engine)
    out = [(str(r["customer_id"]), str(r["account_name"])) for _, r in df.iterrows()]
    if filter_str:
        out = [x for x in out if filter_str in x[1]]
    return out

def resolve_dates(args) -> Tuple[date, date]:
    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").date()
        return d, d
    if args.start and args.end:
        return datetime.strptime(args.start, "%Y-%m-%d").date(), datetime.strptime(args.end, "%Y-%m-%d").date()
    if ENV_FORCE_DT:
        d = datetime.strptime(ENV_FORCE_DT, "%Y-%m-%d").date()
        return d, d
    if ENV_START_DT and ENV_END_DT:
        return datetime.strptime(ENV_START_DT, "%Y-%m-%d").date(), datetime.strptime(ENV_END_DT, "%Y-%m-%d").date()
    y = date.today() - timedelta(days=1)
    return y, y

def daterange(start_date: date, end_date: date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def main():
    engine = get_engine()
    ensure_tables(engine)
    ap = argparse.ArgumentParser()
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--date")
    ap.add_argument("--force-dim", action="store_true")
    ap.add_argument("--accounts-xlsx", default="")
    ap.add_argument("--account", default="")
    args = ap.parse_args()
    d1, d2 = resolve_dates(args)

    accounts = load_accounts(engine, args.accounts_xlsx, args.account.strip() or None)
    if not accounts:
        die("수집 대상 계정이 없습니다.")

    for cid, name in accounts:
        fetch_and_save_bizmoney(engine, cid)
        if should_refresh_dim(engine, cid, args.force_dim or FORCE_DIM_ENV):
            refresh_dim_for_account(engine, cid, name)
        else:
            print(f"=== {name} DIM refresh skip (TTL) ===")

        # ✅ 기간 루프: 기간 조회 시 하루씩 끊어서 수집
        for single_date in daterange(d1, d2):
            refresh_fact_for_account_daily(engine, cid, name, single_date)

    print("✅ 완료!")

if __name__ == "__main__":
    main()
