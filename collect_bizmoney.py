# -*- coding: utf-8 -*-
"""
collect_bizmoney.py - 네이버 검색광고 비즈머니(잔액) 전용 수집기
- 수정사항: API 호출 에러 시 0원으로 저장하지 않고 건너뜀
- 수정사항: 에러 로그(상태코드, 메시지) 상세 출력
"""

import os
import sys
import time
import hmac
import base64
import hashlib
import requests
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. 환경변수 로드
load_dotenv()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()
BASE_URL = "https://api.searchad.naver.com"

if not API_KEY or not API_SECRET:
    print("❌ API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
    sys.exit(1)

# 2. API 서명 및 헤더 생성
def get_header(method, uri, customer_id):
    timestamp = str(int(time.time() * 1000))
    signature = hmac.new(
        API_SECRET.encode('utf-8'),
        f"{timestamp}.{method}.{uri}".encode('utf-8'),
        hashlib.sha256
    ).digest()
    
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": base64.b64encode(signature).decode('utf-8'),
    }

# 3. 비즈머니 조회 함수 (수정됨)
def get_bizmoney(customer_id):
    uri = "/billing/bizmoney"
    try:
        r = requests.get(BASE_URL + uri, headers=get_header("GET", uri, customer_id), timeout=10)
        
        if r.status_code == 200:
            return int(r.json().get("bizMoney", 0))
        else:
            # 에러 발생 시 로그 출력 후 None 반환 (0 반환 아님)
            print(f"⚠️ [API Error] {customer_id}: {r.status_code} - {r.text[:200]}")
            return None
            
    except Exception as e:
        print(f"⚠️ [System Error] {customer_id}: {e}")
        return None

# 4. 메인 로직
def main():
    if not DB_URL:
        print("❌ DATABASE_URL이 없습니다.")
        return

    engine = create_engine(DB_URL)
    
    # 테이블 생성 (없으면)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
                dt DATE, 
                customer_id TEXT, 
                bizmoney_balance BIGINT, 
                PRIMARY KEY(dt, customer_id)
            )
        """))

    # 수집 대상 계정 가져오기 (dim_account 테이블 활용)
    accounts = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT customer_id, account_name FROM dim_account")).fetchall()
            accounts = [{"id": str(r[0]), "name": r[1]} for r in rows]
    except Exception:
        pass

    # DB에 계정이 없으면 환경변수 단일 계정 사용
    if not accounts and CUSTOMER_ID:
        accounts = [{"id": CUSTOMER_ID, "name": "Target Account"}]

    print(f"📋 비즈머니 수집 대상: {len(accounts)}개 계정")
    
    today = date.today()
    success_count = 0
    
    for acc in accounts:
        cid = acc["id"]
        name = acc["name"] or "Unknown"
        
        balance = get_bizmoney(cid)
        
        # [중요] 에러(None)인 경우 저장하지 않고 건너뜀
        if balance is None:
            print(f"❌ {name}({cid}): 수집 실패 (로그 확인 필요)")
            continue

        # 정상 값인 경우에만 저장
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fact_bizmoney_daily (dt, customer_id, bizmoney_balance)
                VALUES (:dt, :cid, :bal)
                ON CONFLICT (dt, customer_id) 
                DO UPDATE SET bizmoney_balance = EXCLUDED.bizmoney_balance
            """), {"dt": today, "cid": cid, "bal": balance})
            
        print(f"✅ {name}({cid}): {balance:,}원 저장 완료")
        success_count += 1

    print(f"🚀 전체 완료: 성공 {success_count} / 전체 {len(accounts)}")

if __name__ == "__main__":
    main()
