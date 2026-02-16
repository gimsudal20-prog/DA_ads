# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Version: FINAL_PATH_ONLY_SIGNATURE)
"""

from __future__ import annotations

import os
import time
import json
import hmac
import base64
import hashlib
import sys
import argparse
import urllib.parse
import urllib.request
import ssl
from datetime import datetime, date, timedelta
from typing import Any, List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

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
IDS_CHUNK = 5 

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50)
print("=== [VERSION: FINAL_PATH_ONLY_SIGNATURE] ===")
print("=== 서명할 때 쿼리 스트링을 제외하고 경로만 서명합니다 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
else:
    log(f"🔑 API Key Loaded: Len={len(API_KEY)}, Prefix={API_KEY[:4]}...")
    log(f"🔑 Secret Loaded: Len={len(API_SECRET)}, Prefix={API_SECRET[:4]}..., Suffix=...{API_SECRET[-2:]}")

# -------------------------
# 2. 서명 및 요청 (핵심 수정)
# -------------------------
def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    message = f"{timestamp}.{method}.{uri}"
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(hash.digest()).decode("utf-8")

def request_stats_manual(customer_id: str, ids_str: str, date_str: str) -> Any:
    method = "GET"
    path = "/stats"
    timestamp = str(int(time.time() * 1000))
    
    # 1. 파라미터 값 준비 (JSON 공백 제거)
    fields_val = json.dumps(["impCnt","clkCnt","salesAmt","ccnt","convAmt"], separators=(',', ':'))
    time_val = json.dumps({"since": date_str, "until": date_str}, separators=(',', ':'))
    
    # 2. 전송용 URL 생성 (표준 인코딩)
    enc_ids = urllib.parse.quote(ids_str)
    enc_fields = urllib.parse.quote(fields_val)
    enc_time = urllib.parse.quote(time_val)
    
    # URL에는 파라미터를 붙임
    req_query = f"fields={enc_fields}&ids={enc_ids}&timeRange={enc_time}"
    full_url = f"{BASE_URL}{path}?{req_query}"
    
    # ---------------------------------------------------------
    # [핵심] 서명할 때는 파라미터를 뺍니다!
    # ---------------------------------------------------------
    # 기존: uri_to_sign = "/stats?fields=..."
    # 변경: uri_to_sign = "/stats"
    uri_to_sign = path 
    
    signature = generate_signature(timestamp, method, uri_to_sign, API_SECRET)
    
    headers = {
        # GET 요청에는 Content-Type이 필요 없는 경우가 많아 제거해봅니다 (혹시 몰라 주석처리)
        # "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }
    
    # SSL 설정
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(full_url, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as res:
            if res.status == 200:
                return json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 403:
            # 만약 "경로만 서명" 방식이 틀렸다면, 
            # 마지막 보루로 "전체 URL 서명"을 하되 헤더만 바꿔서 재시도
            log(f"🔥 Path Only 서명 실패 (403): {e.read().decode('utf-8')}")
            return request_stats_retry_full_sign(customer_id, ids_str, fields_val, time_val)
        elif e.code == 429:
             time.sleep(1)
             return request_stats_manual(customer_id, ids_str, date_str)
        else:
             log(f"⚠️ HTTP Error {e.code}: {e.read().decode('utf-8')}")
    except Exception as e:
        log(f"⚠️ 오류: {e}")
    return None

def request_stats_retry_full_sign(customer_id, ids_str, fields_val, time_val):
    # Fallback: 전체 URL 서명 (하지만 이번엔 쿼리 순서를 바꾸지 않고 그대로)
    method = "GET"
    path = "/stats"
    timestamp = str(int(time.time() * 1000))
    
    enc_ids = urllib.parse.quote(ids_str)
    enc_fields = urllib.parse.quote(fields_val)
    enc_time = urllib.parse.quote(time_val)
    
    # 전송용 & 서명용 동일하게 사용
    query_string = f"fields={enc_fields}&ids={enc_ids}&timeRange={enc_time}"
    uri_to_sign = f"{path}?{query_string}"
    
    signature = generate_signature(timestamp, method, uri_to_sign, API_SECRET)
    
    headers = {
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }
    
    full_url = f"{BASE_URL}{uri_to_sign}"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(full_url, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as res:
            if res.status == 200:
                return json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        log(f"🔥 [재시도 실패] HTTP Error {e.code}: {e.read().decode('utf-8')}")
    except Exception:
        pass
    return None

def request_campaigns(customer_id: str) -> List[dict]:
    method = "GET"
    uri = "/ncc/campaigns"
    timestamp = str(int(time.time() * 1000))
    signature = generate_signature(timestamp, method, uri, API_SECRET)
    
    headers = {
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }
    
    full_url = f"{BASE_URL}{uri}"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(full_url, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as res:
            if res.status == 200:
                return json.loads(res.read().decode('utf-8'))
    except Exception:
        pass
    return []

# -------------------------
# 3. 데이터 조회 로직
# -------------------------
def get_engine() -> Engine:
    if not DB_URL:
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

def get_stats(customer_id: str, ids: List[str], date_str: str) -> List[dict]:
    if not ids: return []
    
    results = []
    print("   > 상세 데이터 수집: ", end="")
    
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        ids_str = ",".join(chunk)
        
        data = request_stats_manual(customer_id, ids_str, date_str)
        
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
    
    campaigns = request_campaigns(customer_id)
    if not campaigns:
        log("   > 캠페인 조회 실패 또는 없음")
        return

    camp_ids = [c["nccCampaignId"] for c in campaigns]
    log(f"   > 대상 캠페인: {len(camp_ids)}개")
    
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
