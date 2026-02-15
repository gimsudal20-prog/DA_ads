# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Final Fix for GitHub Actions)
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
from urllib.parse import urlparse
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path as _Path

# -------------------------
# 1. 환경변수 로딩 및 검증
# -------------------------
def _load_env() -> str:
    """ .env 파일이 있으면 로드합니다 (로컬 개발용) """
    candidates: List[_Path] = []
    try:
        script_dir = _Path(__file__).resolve().parent
        candidates += [script_dir / ".env", script_dir / "env.env"]
    except Exception:
        pass
    
    cwd = _Path.cwd()
    candidates += [cwd / ".env", cwd / "env.env"]
    
    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=True)
            return str(p)
    
    load_dotenv(override=True)
    return ""

_ENV_FILE = _load_env()

# --- 중요: 키 값의 공백을 확실히 제거(.strip) ---
API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()

# 설정값
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip() # (옵션) 특정 고객 ID 강제 지정 시
BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
SLEEP_BETWEEN_CALLS = 0.1
CHUNK_INSERT = 2000
IDS_CHUNK = 100 # 한번에 조회할 ID 개수

# 로깅 헬퍼
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

# --- 디버깅: 키 로딩 상태 확인 (보안을 위해 일부만 출력) ---
if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
else:
    # 키가 제대로 들어왔는지 길이와 앞뒤 글자만 확인
    log(f"🔑 API Key Loaded: Len={len(API_KEY)}, Prefix={API_KEY[:4]}...")
    log(f"🔑 Secret Loaded: Len={len(API_SECRET)}, Prefix={API_SECRET[:4]}..., Suffix=...{API_SECRET[-2:]}")

# -------------------------
# 2. 서명(Signature) 생성 함수
# -------------------------
def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    """
    네이버 검색광고 API 서명 생성
    - method: GET, POST 등 (대문자)
    - uri: 도메인을 제외한 경로 + 쿼리스트링 (예: /stats?ids=123&fields=...)
    """
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

# -------------------------
# 3. API 요청 처리 (핵심 Fix)
# -------------------------
def request_api(method: str, path: str, customer_id: str, params: dict = None, retries=3) -> Any:
    """
    requests.prepare_request를 사용하여 '실제 전송되는 URL'로 서명을 생성합니다.
    Invalid Signature 오류를 방지하는 핵심 로직입니다.
    """
    url = BASE_URL + path
    
    with requests.Session() as session:
        req = requests.Request(method, url, params=params)
        prepped = session.prepare_request(req)
        
        # 중요: 쿼리 파라미터가 포함된 path_url을 서명에 사용
        # 예: /stats?ids=...&fields=...
        api_uri = prepped.path_url
        
        headers = get_headers(method, api_uri, customer_id)
        prepped.headers.update(headers)
        
        for attempt in range(retries):
            try:
                response = session.send(prepped, timeout=TIMEOUT)
                
                # 성공 시 데이터 반환
                if response.status_code == 200:
                    return response.json()
                
                # 429 Too Many Requests: 잠시 대기 후 재시도
                if response.status_code == 429:
                    time.sleep(1 * (attempt + 1))
                    continue
                
                # 403 Forbidden: 서명 오류 등 (재시도 의미 없음)
                if response.status_code == 403:
                    log(f"⛔ 권한 오류 (403): {response.text}")
                    # 여기서 재시도하지 않고 바로 예외 발생
                    raise requests.HTTPError(f"403 Forbidden: {response.text}", response=response)

                # 기타 오류
                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    log(f"⚠️ 요청 실패 ({method} {path}): {str(e)}")
                    return None
                time.sleep(0.5)
    return None

# -------------------------
# 4. DB 연결 및 테이블 생성
# -------------------------
def get_engine() -> Engine:
    if not DB_URL:
        # DB URL이 없으면 메모리 DB 사용 (테스트용)
        log("⚠️ DB_URL이 없어 sqlite 메모리 DB를 사용합니다.")
        return create_engine("sqlite:///:memory:", future=True)
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

def init_db(engine: Engine):
    """ 필요한 테이블이 없으면 생성 """
    with engine.begin() as conn:
        # 계정 정보
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_account (
                customer_id TEXT PRIMARY KEY, 
                account_name TEXT
            )
        """))
        # 캠페인 성과 (일별)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_campaign_daily (
                dt DATE, 
                customer_id TEXT, 
                campaign_id TEXT,
                imp BIGINT DEFAULT 0, 
                clk BIGINT DEFAULT 0, 
                cost BIGINT DEFAULT 0, 
                conv DOUBLE PRECISION DEFAULT 0, 
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, campaign_id)
            )
        """))
        # 필요한 다른 테이블들도 여기에 추가 가능...

# -------------------------
# 5. 데이터 수집 로직
# -------------------------
def get_campaigns(customer_id: str) -> List[dict]:
    data = request_api("GET", "/ncc/campaigns", customer_id)
    return data if isinstance(data, list) else []

def get_stats(customer_id: str, ids: List[str], date_str: str) -> List[dict]:
    """ 통계 데이터 조회 (재귀적으로 쪼개서 요청하지 않고 단순화함) """
    if not ids: 
        return []
    
    fields = '["impCnt","clkCnt","salesAmt","ccnt","convAmt"]'
    time_range = json.dumps({"since": date_str, "until": date_str})
    
    results = []
    
    # ID를 청크 단위로 잘라서 요청
    for i in range(0, len(ids), IDS_CHUNK):
        chunk = ids[i:i+IDS_CHUNK]
        chunk_ids = ",".join(chunk)
        
        params = {
            "ids": chunk_ids,
            "fields": fields,
            "timeRange": time_range
        }
        
        data = request_api("GET", "/stats", customer_id, params=params)
        if data and "data" in data:
            results.extend(data["data"])
            sys.stdout.write(".")
            sys.stdout.flush()
        else:
            sys.stdout.write("x")
            sys.stdout.flush()
            
    print("") # 줄바꿈
    return results

def save_stats(engine: Engine, customer_id: str, target_date: date):
    dt_str = target_date.strftime("%Y-%m-%d")
    log(f"📅 데이터 수집 시작: {dt_str} (Customer: {customer_id})")
    
    # 1. 캠페인 목록 조회
    campaigns = get_campaigns(customer_id)
    if not campaigns:
        log("   > 캠페인이 없거나 조회 실패")
        return

    camp_ids = [c["nccCampaignId"] for c in campaigns]
    log(f"   > 대상 캠페인: {len(camp_ids)}개")
    
    # 2. 성과 조회
    stats = get_stats(customer_id, camp_ids, dt_str)
    
    # 3. DB 저장용 데이터 변환
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
        # Upsert 로직 (PostgreSQL 기준)
        with engine.begin() as conn:
            # 기존 데이터 삭제 후 삽입 (간단한 방법)
            conn.execute(
                text("DELETE FROM fact_campaign_daily WHERE dt = :dt AND customer_id = :cid"),
                {"dt": target_date, "cid": customer_id}
            )
            
            stmt = text("""
                INSERT INTO fact_campaign_daily (dt, customer_id, campaign_id, imp, clk, cost, conv, sales, roas)
                VALUES (:dt, :customer_id, :campaign_id, :imp, :clk, :cost, :conv, :sales, :roas)
            """)
            conn.execute(stmt, rows)
        log("   > 저장 완료")
    else:
        log("   > 저장할 데이터가 없습니다.")

# -------------------------
# 6. 메인 실행부
# -------------------------
def main():
    engine = get_engine()
    init_db(engine)
    
    # 명령행 인자로 날짜 받기 (기본값: 어제)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD", default="")
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
    
    # 수집할 계정 목록 로드 (DB에서 가져오거나 환경변수 등)
    # 여기서는 예시로 DB의 dim_account를 조회
    accounts = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT customer_id FROM dim_account"))
            accounts = [row[0] for row in result]
    except Exception:
        pass
    
    # 만약 DB에 계정이 없으면 .env나 하드코딩된 ID 사용 (비상용)
    if not accounts and CUSTOMER_ID:
        accounts = [CUSTOMER_ID]
    
    if not accounts:
        log("⚠️ 수집할 광고주 계정(Customer ID)이 없습니다. dim_account 테이블을 확인하세요.")
        # 테스트를 위해 2886931 (휴비즈넷) 강제 추가 (필요시 주석 해제)
        # accounts = ["2886931"]
    
    for cid in accounts:
        try:
            save_stats(engine, cid, target_date)
        except Exception as e:
            log(f"❌ 오류 발생 ({cid}): {e}")
            # 에러가 나도 다음 계정 진행
            continue

    log("✅ 모든 작업 완료")

if __name__ == "__main__":
    main()
