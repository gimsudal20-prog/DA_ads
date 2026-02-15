# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Final: Safe Comma Strategy)
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
import urllib.parse
import ssl
import urllib.request
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------
# 1. 환경변수 로딩
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
IDS_CHUNK = 50 

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
else:
    log(f"🔑 API Key Loaded: Len={len(API_KEY)}, Prefix={API_KEY[:4]}...")
    log(f"🔑 Secret Loaded: Len={len(API_SECRET)}, Prefix={API_SECRET[:4]}..., Suffix=...{API_SECRET[-2:]}")

# -------------------------
# 2. 서명 및 API 요청 (쉼표 보존)
# -------------------------
def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    message = f"{timestamp}.{method}.{uri}"
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(hash.digest()).decode("utf-8")

def request_api(method: str, path: str, customer_id: str, params: dict = None) -> Any:
    
    # URL 조립 (핵심: 쉼표를 인코딩하지 않음)
    if params:
        # 1. 파라미터별로 인코딩하되, 쉼표(,)와 콜론(:)은 safe 문자로 지정하여 변환 막음
        # 네이버 API는 ids=1,2,3 처럼 쉼표가 살아있는 것을 원할 때가 많음
        query_parts = []
        # 알파벳 순서 정렬 (fields -> ids -> timeRange)
        for k in sorted(params.keys()):
            val = str(params[k])
            # safe=',:' -> 쉼표와 콜론은 %2C, %3A로 바꾸지 말고 그냥 둬라!
            encoded_val = urllib.parse.quote(val, safe=',:')
            query_parts.append(f"{k}={encoded_val}")
            
        raw_query = "&".join(query_parts)
        api_uri = f"{path}?{raw_query}"
    else:
        api_uri = path
    
    full_url = f"{BASE_URL}{api_uri}"
    
    # 2. 서명 생성 (쉼표가 살아있는 api_uri 그대로 서명)
    timestamp = str(int(time.time() * 1000))
    signature = generate_signature(timestamp, method, api_uri, API_SECRET)
    
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }

    # 3. 전송
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(full_url, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as res:
            if res.status == 200:
                return json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        if e.code == 403:
            log(f"⛔ 권한 오류 (403): {error_body}")
            # 디버깅: 서명한 주소 출력
            # log(f"   [Debug] Signed URI: {api_uri}")
        elif e.code == 429:
             time.sleep(1)
             return request_api(method, path, customer_id, params)
        else:
             log(f"⚠️ 요청 실패 ({e.code}): {error_body}")
        return None
    except Exception as e:
        log(f"⚠️ 네트워크 오류: {str(e)}")
        return None

# -------------------------
# 3. 데이터 조회 로직
# -------------------------
def get_engine() -> Engine:
    if not DB_URL:
        log("⚠️ DB_URL 없음: 메모리 DB 사용")
        return create_engine("sqlite:///:memory:", future=True)
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

def init_db(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_campaign_daily (
                dt DATE, customer_id TEXT, campaign_id TEXT,
                imp BIGINT DEFAULT 0, clk BIGINT DEFAULT 0, cost BIGINT DEFAULT 0, 
                conv DOUBLE PRECISION DEFAULT 0, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, campaign_id)
            )
        """))

def get_campaigns(customer_id: str) -> List[dict]:
    data = request_api("GET", "/ncc/campaigns", customer_id)
    return data if isinstance(data, list) else []

def get_stats(customer_id: str, ids: List[str], date_str: str) -> List[dict]:
    if not ids: return []
    
    # JSON 생성 (공백 제거)
    fields_json = json.dumps(["impCnt","clkCnt","salesAmt","ccnt","convAmt"], separators=(',', ':'))
    time_range_json = json.dumps({"since": date_str, "until": date_str}, separators=(',', ':'))
    
    results = []
    print("   > 상세 데이터 수집: ", end="")
    
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        ids_str = ",".join(chunk)
        
        # 딕셔너리로 준비 (request_api 내부에서 쉼표 보존 인코딩 처리함)
        params = {
            "ids": ids_str,
            "fields": fields_json,
            "timeRange": time_range_json
        }
        
        data = request_api("GET", "/stats", customer_id, params=params)
        
        if data and "data" in data:
            results.extend(data["data"])
            sys.stdout.write("■")
        else:
            sys.stdout.write("x")
        sys.stdout.flush()
            
    print(" 완료") 
    return results

def save_stats(engine: Engine, customer_id: str, target_date: date):
    dt_str = target_date.strftime("%Y-%m-%d")
    log(f"📅 데이터 수집 시작: {dt_str} (Customer: {customer_id})")
    
    # 1. 캠페인
    campaigns = get_campaigns(customer_id)
    if not campaigns:
        log("   > 캠페인 조회 실패 또는 없음")
        return

    camp_ids = [c["nccCampaignId"] for c in campaigns]
    log(f"   > 대상 캠페인: {len(camp_ids)}개")
    
    # 2. 성과
    stats = get_stats(customer_id, camp_ids, dt_str)
    
    rows = []
    for s in stats:
        cost = int(s.get("salesAmt", 0) or 0)
        sales = int(s.get("convAmt", 0) or 0)
        roas = (sales / cost * 100) if cost > 0 else 0.0
        
        rows.append({
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": s.get("id"),
            "imp": int(s.get("impCnt", 0) or 0),
            "clk": int(s.get("clkCnt", 0) or 0),
            "cost": cost,
            "conv": float(s.get("ccnt", 0) or 0),
            "sales": sales,
            "roas": roas
        })
    
    if rows:
        log(f"   > {len(rows)}개 데이터 저장 중...")
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM fact_campaign_daily WHERE dt = :dt AND customer_id = :cid"),
                {"dt": target_date, "cid": customer_id}
            )
            stmt = text("""
                INSERT INTO fact_campaign_daily (dt, customer_id, campaign_id, imp, clk, cost, conv, sales, roas)
                VALUES (:dt, :customer_id, :campaign_id, :imp, :clk, :cost, :conv, :sales, :roas)
            """)
            conn.execute(stmt, rows)
        log("   > 저장 완료!")
    else:
        log("   > (저장할 데이터 없음)")

def main():
    engine = get_engine()
    init_db(engine)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
    
    accounts = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT customer_id FROM dim_account"))
            accounts = [row[0] for row in result]
    except Exception:
        pass
    
    if not accounts and CUSTOMER_ID:
        accounts = [CUSTOMER_ID]
        
    if not accounts:
        log("⚠️ 수집할 계정이 없습니다. DB의 dim_account를 확인하세요.")
        return

    for cid in accounts:
        try:
            save_stats(engine, cid, target_date)
        except Exception as e:
            log(f"❌ 오류 발생 ({cid}): {e}")
            continue

    log("✅ 모든 작업 완료")

if __name__ == "__main__":
    main()
