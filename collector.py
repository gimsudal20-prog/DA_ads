# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v8.6 - 업체명 표시 기능 추가)
- 개선 1: 로그에 Customer ID와 함께 '업체명'을 표시하여 가독성 향상
- 유지: v8.5의 초고속 수집/저장 엔진 (API 50개 조회, Bulk Upsert)
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
print("=== [VERSION: v8.6_NAME_DISPLAY] ===")
print("=== 업체명 표시 + 초고속 수집 엔진 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

# -------------------------
# 2. 서명 및 요청
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
    headers = make_headers(method, path, customer_id)
    try:
        r = requests.request(method, url, headers=headers, params=params, timeout=TIMEOUT)
        data = None
        try:
            data = r.json()
        except Exception:
            data = r.text
        if raise_error and r.status_code >= 400:
            log(f"🔥 API Error {r.status_code}: {str(data)[:200]}")
            raise requests.HTTPError(f"{r.status_code}", response=r)
        return r.status_code, data
    except Exception as e:
        if raise_error:
            raise e
        return 0, str(e)

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
    temp_table = f"tmp_{table}_{int(time.time())}"
    
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
            sys.stdout.write("■")
        else:
            sys.stdout.write("x")
        sys.stdout.flush()
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
# 6. 메인 로직
# -------------------------
def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date):
    # ✅ [수정] 로그에 업체명 표시
    log(f"🚀 처리 시작: {account_name} ({customer_id}) / 날짜: {target_date}")
    
    camp_list = list_campaigns(customer_id)
    log(f"   > 캠페인 {len(camp_list)}개 발견")
    
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

    log("   > 구조 데이터(DIM) DB 저장 중...")
    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    if kw_rows:
        log(f"     - 키워드 {len(kw_rows)}개 저장 중...")
        upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
    if ad_rows:
        log(f"     - 소재 {len(ad_rows)}개 저장 중...")
        upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    
    log(f"   > 성과 데이터(FACT) 수집 시작...")
    if target_camp_ids:
        print(f"     [캠페인 {len(target_camp_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_camp_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "campaign_id") for r in raw]
        replace_fact_range(engine, "fact_campaign_daily", rows, customer_id, target_date)
        print(" 완료")
    if target_kw_ids and not SKIP_KEYWORD_STATS:
        print(f"     [키워드 {len(target_kw_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_kw_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "keyword_id") for r in raw]
        replace_fact_range(engine, "fact_keyword_daily", rows, customer_id, target_date)
        print(" 완료")
    if target_ad_ids and not SKIP_AD_STATS:
        print(f"     [소재 {len(target_ad_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_ad_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "ad_id") for r in raw]
        replace_fact_range(engine, "fact_ad_daily", rows, customer_id, target_date)
        print(" 완료")

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
        # 단일 타겟 실행 시, 이름은 임의로 설정 (DB조회 안함)
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        try:
            with engine.connect() as conn:
                # ✅ [수정] 업체명(account_name)도 함께 조회
                result = conn.execute(text("SELECT customer_id, account_name FROM dim_account"))
                accounts_info = [{"id": row[0], "name": row[1] or "Unknown"} for row in result]
        except Exception:
            pass
        
        # DB에 계정 없으면 환경변수 사용
        if not accounts_info and CUSTOMER_ID:
            accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info:
        log("⚠️ 수집할 계정이 없습니다.")
        return

    log(f"📋 수집 대상 계정: {len(accounts_info)}개")

    for acc in accounts_info:
        try:
            # ✅ [수정] process_account에 이름 전달
            process_account(engine, acc["id"], acc["name"], target_date)
        except Exception as e:
            log(f"❌ 오류 발생 ({acc['name']} - {acc['id']}): {e}")
            import traceback
            traceback.print_exc()

    log("✅ 모든 작업 완료")

if __name__ == "__main__":
    main()
