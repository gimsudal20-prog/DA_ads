# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Final Version: Manual URL Construction)
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
from urllib.parse import urlencode, quote
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path as _Path

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
TIMEOUT = 60
IDS_CHUNK = 50  # 청크 사이즈를 조금 줄여서 안정성 확보

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

# 키 로딩 확인
if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
else:
    log(f"🔑 API Key Loaded: Len={len(API_KEY)}, Prefix={API_KEY[:4]}...")
    log(f"🔑 Secret Loaded: Len={len(API_SECRET)}, Prefix={API_SECRET[:4]}..., Suffix=...{API_SECRET[-2:]}")

# -------------------------
# 2. 서명(Signature) 및 API 요청 (수동 조립 방식)
# -------------------------
def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    message = f"{timestamp}.{method}.{uri}"
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(hash.digest()).decode("utf-8")

def get_headers(method: str, uri: str, customer_id: str) -> Dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    signature = generate_signature(timestamp, method, uri, API_SECRET)
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }

def request_api(method: str, path: str, customer_id: str, params: dict = None, retries=3) -> Any:
    """
    requests의 자동 인코딩을 쓰지 않고, urllib으로 직접 URL을 만듭니다.
    """
    # 1. 파라미터가 있다면 수동으로 쿼리스트링 생성
    if params:
        # safe chars: 네이버는 쉼표(,) 등을 인코딩하거나 안 하거나 민감하므로
        # 가장 표준적인 urlencode를 사용하되, 서명과 요청이 100% 일치하도록 함
        query_string = urlencode(params)
        api_uri = f"{path}?{query_string}"
    else:
        api_uri = path
    
    full_url = f"{BASE_URL}{api_uri}"
    
    # 2. 헤더 생성 (여기서 쓰인 api_uri와 실제 요청 URL이 정확히 같아야 함)
    headers = get_headers(method, api_uri, customer_id)
    
    with requests.Session() as session:
        for attempt in range(retries):
            try:
                # 3. params 인자를 쓰지 않고 full_url을 바로 요청
                response = session.request(method, full_url, headers=headers, timeout=TIMEOUT)
                
                if response.status_code == 200:
                    return response.json()
                
                if response.status_code == 429:
                    time.sleep(1 * (attempt + 1))
                    continue
                
                if response.status_code == 403:
                    log(f"⛔ 권한 오류 (403): {response.text}")
                    # 여기서 재시도하지 않고 예외 발생 (키 문제일 수 있으므로)
                    return None

                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    log(f"⚠️ 요청 실패: {str(e)}")
                    return None
                time.sleep(0.5)
    return None

# -------------------------
# 3. DB 및 로직
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
    
    # [중요] JSON 생성 시 공백 제거 (separators 사용)
    # 네이버 API는 공백(Space)이 URL 인코딩될 때 서명 불일치를 자주 일으킴
    fields_json = json.dumps(["impCnt","clkCnt","salesAmt","ccnt","convAmt"], separators=(',', ':'))
    time_range_json = json.dumps({"since": date_str, "until": date_str}, separators=(',', ':'))
    
    results = []
    print("   > 진행률: ", end="")
    
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        
        params = {
            "ids": ",".join(chunk),
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
            
    print("") 
    return results

def save_stats(engine: Engine, customer_id: str, target_date: date):
    dt_str = target_date.strftime("%Y-%m-%d")
    log(f"📅 데이터 수집 시작: {dt_str} (Customer: {customer_id})")
    
    campaigns = get_campaigns(customer_id)
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
    
    # 테스트를 위한 강제 계정 목록 (필요시 주석 해제)
    # accounts = ["2886931", "1346816", "1454161"] 
    
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
