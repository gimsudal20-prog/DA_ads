# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Version: DEBUG_FINAL_CHECK_v2)
"""

from __future__ import annotations

import os
import time
import json
import hmac
import base64
import hashlib
import sys
import argparse  # <--- 이 줄이 빠져서 오류가 났었습니다. 추가했습니다!
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
IDS_CHUNK = 5 # 안전하게 5개씩만 요청

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

# --- 버전 확인용 로그 (v2) ---
print("="*50)
print("=== [VERSION: DEBUG_FINAL_CHECK_v2_FIXED_IMPORT] ===")
print("=== 이 로그가 보이면 업데이트 성공입니다 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
else:
    log(f"🔑 API Key Loaded: Len={len(API_KEY)}, Prefix={API_KEY[:4]}...")
    log(f"🔑 Secret Loaded: Len={len(API_SECRET)}, Prefix={API_SECRET[:4]}..., Suffix=...{API_SECRET[-2:]}")

# -------------------------
# 2. 서명 및 요청 (100% 수동 조립)
# -------------------------
def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    # 서명 대상 URI를 로그로 남겨서 디버깅 가능하게 함
    # log(f"DEBUG: Signing Base -> {timestamp}.{method}.{uri}")
    message = f"{timestamp}.{method}.{uri}"
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(hash.digest()).decode("utf-8")

def request_stats_manual(customer_id: str, ids_str: str, date_str: str) -> Any:
    """
    [해결책]
    URL 파라미터를 라이브러리에 맡기지 않고, 문자열로 직접 조립합니다.
    그리고 '조립된 문자열 그대로' 서명하고 전송합니다.
    """
    method = "GET"
    path = "/stats"
    timestamp = str(int(time.time() * 1000))
    
    # 1. 파라미터 값 준비 (JSON 공백 제거 필수)
    fields_val = json.dumps(["impCnt","clkCnt","salesAmt","ccnt","convAmt"], separators=(',', ':'))
    time_val = json.dumps({"since": date_str, "until": date_str}, separators=(',', ':'))
    
    # 2. URL 인코딩 (urllib.parse.quote 사용)
    # 쉼표(,)도 %2C로 변환하는 것이 '표준'입니다.
    enc_ids = urllib.parse.quote(ids_str)
    enc_fields = urllib.parse.quote(fields_val)
    enc_time = urllib.parse.quote(time_val)
    
    # 3. 쿼리 스트링 직접 조립 (알파벳 순서: fields -> ids -> timeRange)
    query_string = f"fields={enc_fields}&ids={enc_ids}&timeRange={enc_time}"
    
    # 4. URI 생성
    uri_path = f"{path}?{query_string}"
    
    # 5. 서명 생성 (이 문자열 그대로!)
    signature = generate_signature(timestamp, method, uri_path, API_SECRET)
    
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }
    
    # 6. 전송 (requests 대신 urllib 사용으로 변조 방지)
    full_url = f"{BASE_URL}{uri_path}"
    
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
            # 403이면 한 번만 더 '쉼표 유지' 방식으로 재시도 (Fallback)
            return request_stats_retry_safe_comma(customer_id, ids_str, fields_val, time_val)
        elif e.code == 429:
             time.sleep(1)
             return request_stats_manual(customer_id, ids_str, date_str)
        else:
             pass
    except Exception:
        pass
    return None

def request_stats_retry_safe_comma(customer_id, ids_str, fields_val, time_val):
    # 백업 전략: 쉼표(,)를 인코딩하지 않고 보냄
    method = "GET"
    path = "/stats"
    timestamp = str(int(time.time() * 1000))
    
    enc_ids = urllib.parse.quote(ids_str, safe=',') # 쉼표 살림
    enc_fields = urllib.parse.quote(fields_val)
    enc_time = urllib.parse.quote(time_val)
    
    query_string = f"fields={enc_fields}&ids={enc_ids}&timeRange={enc_time}"
    uri_path = f"{path}?{query_string}"
    
    signature = generate_signature(timestamp, method, uri_path, API_SECRET)
    
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }
    
    full_url = f"{BASE_URL}{uri_path}"
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
    return None

# 캠페인 목록 조회
def request_campaigns(customer_id: str) -> List[dict]:
    method = "GET"
    uri = "/ncc/campaigns"
    timestamp = str(int(time.time() * 1000))
    signature = generate_signature(timestamp, method, uri, API_SECRET)
    
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
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

def get_stats(customer_id: str, ids: List[str], date_str: str) -> List[dict]:
    if not ids: return []
    
    results = []
    print("   > 상세 데이터 수집: ", end="")
    
    # URL 길이 이슈 방지를 위해 5개씩 끊어서 요청
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        ids_str = ",".join(chunk)
        
        # 여기서 request_stats_manual 호출
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
    
    # 1. 캠페인
    campaigns = request_campaigns(customer_id)
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
