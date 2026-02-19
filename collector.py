# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v8.7 - 초고속 병렬 처리 & 스마트 재시도 적용)
- 개선 1: ThreadPoolExecutor를 통한 멀티스레딩(동시 수집) 적용으로 속도 비약적 향상
- 개선 2: 429(Too Many Requests) 에러 발생 시 2초 대기 후 재시도하는 로직 적용
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
import concurrent.futures  # ✅ 병렬 처리를 위해 추가
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------
# 1. 환경변수 및 설정
# -------------------------
def _load_env() -> str:
    load_dotenv(override=True)
    return ""

_ENV_FILE = _load_env()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
SLEEP_BETWEEN_CALLS = 0.05 
IDS_CHUNK = 50  # 속도 최적화 (50개씩 조회)

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False
SKIP_KEYWORD_STATS = False
SKIP_AD_STATS = False

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50)
print("=== [VERSION: v8.7_FAST_PARALLEL] ===")
print("=== 병렬 수집(다중 계정) + 스마트 재시도 엔진 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

# -------------------------
# 2. 서명 및 요청 (스마트 재시도 적용)
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

def request_json(method: str, path: str, customer_id: str, params: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    max_retries = 3  # ✅ 최대 3회 재시도
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = requests.request(method, url, headers=headers, params=params, timeout=TIMEOUT)
            
            # ✅ 네이버 API 차단(429)이나 서버 오류 발생 시 2초 대기 후 다시 시도
            if r.status_code == 429 or r.status_code >= 500:
                log(f"⚠️ API 오류 ({r.status_code}) - 계정 {customer_id}. 2초 대기 후 재시도 ({attempt+1}/{max_retries})...")
                time.sleep(2)
                continue

            data = None
            try:
                data = r.json()
            except Exception:
                data = r.text
                
            if raise_error and r.status_code >= 400:
                log(f"🔥 API Error {r.status_code}: {str(data)[:200]}")
                raise requests.HTTPError(f"{r.status_code}", response=r)
                
            return r.status_code, data
            
        except requests.exceptions.RequestException as e:
            log(f"⚠️ 네트워크 오류 - 계정 {customer_id}: {e}. 2초 후 재시도 ({attempt+1}/{max_retries})...")
            time.sleep(2)
            
    if raise_error:
        raise Exception(f"최대 재시도 횟수 초과: {url}")
    return 0, "Max retries exceeded"

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except requests.HTTPError:
        return False, None
    except Exception:
        return False, None

# -------------------------
# 3. DB 초기화 및 헬퍼
# -------------------------
def get_engine() -> Engine:
    if not DB_URL:
        return create_engine("sqlite:///:memory:", future=True)
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

def ensure_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_ad (
                customer_id TEXT, ad_id TEXT, adgroup_id TEXT,
                ad_name TEXT, status TEXT,
                ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT,
                PRIMARY KEY(customer_id, ad_id)
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_campaign_daily (
                dt DATE, customer_id TEXT, campaign_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, campaign_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_keyword_daily (
                dt DATE, customer_id TEXT, keyword_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, keyword_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_ad_daily (
                dt DATE, customer_id TEXT, ad_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, ad_id)
            )
        """))

# 초고속 Bulk Upsert
def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows: return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last')
    temp_table = f"tmp_{table}_{int(time.time()*1000)}" # 병렬 처리 충돌 방지를 위해 ms 단위 적용
    
    try:
        with engine.begin() as conn:
            df.head(0).to_sql(temp_table, conn, index=False, if_exists='replace')
            df.to_sql(temp_table, conn, index=False, if_exists='append', method='multi', chunksize=1000)
            
            cols = ", ".join([f'"{c}"' for c in df.columns])
            pk_clause = ", ".join([f'"{c}"' for c in pk_cols])
            set_clause = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in df.columns if c not in pk_cols])
            
            if set_clause:
                sql = f'INSERT INTO {table} ({cols}) SELECT * FROM {temp_table} ON CONFLICT ({pk_clause}) DO UPDATE SET {set_clause}'
            else:
                sql = f'INSERT INTO {table} ({cols}) SELECT * FROM {temp_table} ON CONFLICT ({pk_clause}) DO NOTHING'
                
            conn.execute(text(sql))
            conn.execute(text(f'DROP TABLE {temp_table}'))
    except Exception as e:
        log(f"⚠️ Upsert Error in {table}: {e}")

# 초고속 Fact 저장
def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    if not rows: return
    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last')
    
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            df.to_sql(table, conn, index=False, if_exists='append', method='multi', chunksize=1000)
    except Exception as e:
        log(f"⚠️ Fact Insert Error in {table}: {e}")

# -------------------------
# 4. 데이터 조회 (계층 구조)
# -------------------------
def list_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id)
    return data if ok and isinstance(data, list) else []

def list_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": campaign_id})
    return data if ok and isinstance(data, list) else []

def list_keywords(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/keywords", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []

def list_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []

def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad") if isinstance(ad_obj.get("ad"), dict) else {}
    def _pick(d, keys):
        for k in keys:
            if d.get(k): return str(d.get(k))
        return ""
    title = _pick(ad_obj, ["name", "title", "headline", "adName"]) or _pick(ad_inner, ["headline", "title", "name"])
    desc  = _pick(ad_obj, ["description", "desc", "adDescription"]) or _pick(ad_inner, ["description", "desc"])
    pc_url = _pick(ad_obj, ["pcLandingUrl", "pcFinalUrl", "landingUrl"]) or _pick(ad_inner, ["pcLandingUrl", "landingUrl"])
    m_url  = _pick(ad_obj, ["mobileLandingUrl", "mobileFinalUrl"]) or _pick(ad_inner, ["mobileLandingUrl"])
    creative_text = f"{title} | {desc}"
    if pc_url: creative_text += f" | {pc_url}"
    return {
        "ad_title": title, "ad_desc": desc,
        "pc_landing_url": pc_url, "mobile_landing_url": m_url,
        "creative_text": creative_text[:500]
    }

# -------------------------
# 5. 성과 조회
# -------------------------
def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out = []
    d_str = str(d1)
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        ids_str = ",".join(chunk)
        params = {"ids": ids_str, "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data:
            out.extend(data["data"])
    return out

def parse_stats(r: dict, d1: date, customer_id: str, id_key: str) -> dict:
    cost = int(float(r.get("salesAmt", 0) or 0))
    sales = int(float(r.get("convAmt", 0) or 0))
    roas = (sales / cost * 100) if cost > 0 else 0.0
    return {
        "dt": d1, "customer_id": str(customer_id), id_key: str(r.get("id")),
        "imp": int(r.get("impCnt", 0) or 0), "clk": int(r.get("clkCnt", 0) or 0),
        "cost": cost, "conv": float(r.get("ccnt", 0) or 0), "sales": sales, "roas": roas
    }

# -------------------------
# 6. 메인 처리기 (단일 계정)
# -------------------------
def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date):
    log(f"🚀 처리 시작: {account_name} ({customer_id}) / 날짜: {target_date}")
    
    camp_list = list_campaigns(customer_id)
    if not camp_list:
        log(f"   > [ {account_name} ] 캠페인이 없거나 수집 실패")
        return

    log(f"   > [ {account_name} ] 캠페인 {len(camp_list)}개 구조 수집 중...")
    
    camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []
    target_camp_ids, target_kw_ids, target_ad_ids = [], [], []

    for c in camp_list:
        cid = c.get("nccCampaignId")
        if not cid: continue
        target_camp_ids.append(cid)
        camp_rows.append({
            "customer_id": customer_id, "campaign_id": cid, 
            "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")
        })
        ags = list_adgroups(customer_id, cid)
        for g in ags:
            gid = g.get("nccAdgroupId")
            if not gid: continue
            ag_rows.append({
                "customer_id": customer_id, "adgroup_id": gid, "campaign_id": cid,
                "adgroup_name": g.get("name"), "status": g.get("status")
            })
            if not SKIP_KEYWORD_DIM:
                kws = list_keywords(customer_id, gid)
                for k in kws:
                    kid = k.get("nccKeywordId")
                    if kid:
                        target_kw_ids.append(kid)
                        kw_rows.append({
                            "customer_id": customer_id, "keyword_id": kid, "adgroup_id": gid,
                            "keyword": k.get("keyword"), "status": k.get("status")
                        })
            if not SKIP_AD_DIM:
                ads = list_ads(customer_id, gid)
                for a in ads:
                    aid = a.get("nccAdId")
                    if aid:
                        target_ad_ids.append(aid)
                        fields = extract_ad_creative_fields(a)
                        ad_rows.append({
                            "customer_id": customer_id, "ad_id": aid, "adgroup_id": gid,
                            "ad_name": a.get("name") or fields["ad_title"], "status": a.get("status"),
                            **fields
                        })

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    if kw_rows: upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
    if ad_rows: upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    
    log(f"   > [ {account_name} ] 성과 데이터 수집 중...")
    if target_camp_ids:
        raw = get_stats_range(customer_id, target_camp_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "campaign_id") for r in raw]
        replace_fact_range(engine, "fact_campaign_daily", rows, customer_id, target_date)
    if target_kw_ids and not SKIP_KEYWORD_STATS:
        raw = get_stats_range(customer_id, target_kw_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "keyword_id") for r in raw]
        replace_fact_range(engine, "fact_keyword_daily", rows, customer_id, target_date)
    if target_ad_ids and not SKIP_AD_STATS:
        raw = get_stats_range(customer_id, target_ad_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "ad_id") for r in raw]
        replace_fact_range(engine, "fact_ad_daily", rows, customer_id, target_date)

    log(f"✅ 완료: {account_name} ({customer_id})")

# -------------------------
# 7. 메인 실행 블록
# -------------------------
def main():
    engine = get_engine()
    ensure_tables(engine)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
        
    accounts_info = []
    if args.customer_id:
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT customer_id, account_name FROM dim_account"))
                accounts_info = [{"id": row[0], "name": row[1] or "Unknown"} for row in result]
        except Exception:
            pass
        
        if not accounts_info and CUSTOMER_ID:
            accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info:
        log("⚠️ 수집할 계정이 없습니다.")
        return

    log(f"📋 수집 대상 계정: {len(accounts_info)}개")

    # ✅ 병렬 처리 (스레드 풀) 적용
    max_workers = 4  # 4개 업체 동시 수집 (너무 높이면 차단 확률이 올라가므로 3~5개가 적당)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for acc in accounts_info:
            futures.append(
                executor.submit(process_account, engine, acc["id"], acc["name"], target_date)
            )
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"❌ 병렬 처리 중 계정 작업 실패: {e}")
                import traceback
                traceback.print_exc()

    log("🎉 모든 작업 완료")

if __name__ == "__main__":
    main()
