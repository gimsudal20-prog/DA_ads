# -*- coding: utf-8 -*-
"""
collect_bizmoney.py - 네이버 검색광고 비즈머니(잔액) 수집기 (디버깅 모드)
"""

import os
import sys
import time
import hmac
import base64
import hashlib
import json
import requests
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()
BASE_URL = "https://api.searchad.naver.com"

if not API_KEY or not API_SECRET:
    print("❌ API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
    sys.exit(1)

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

def get_bizmoney(customer_id):
    uri = "/billing/bizmoney"
    try:
        r = requests.get(BASE_URL + uri, headers=get_header("GET", uri, customer_id), timeout=10)
        
        # ▼▼▼ [디버깅] 응답 내용 강제 출력 ▼▼▼
        if r.status_code == 200:
            data = r.json()
            balance = int(data.get("bizMoney", 0))
            
            # 0원이면 의심스러우니까 원본 데이터를 출력해봄
            if balance == 0:
                print(f"❓ {customer_id}: 0원 응답 받음 -> 원본: {json.dumps(data, ensure_ascii=False)}")
                
            return balance
        else:
            print(f"⚠️ [API Error] {customer_id}: {r.status_code} - {r.text[:200]}")
            return None
            
    except Exception as e:
        print(f"⚠️ [System Error] {customer_id}: {e}")
        return None

def main():
    if not DB_URL:
        print("❌ DATABASE_URL이 없습니다.")
        return

    engine = create_engine(DB_URL)
    
    # 테이블 생성
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
                dt DATE, customer_id TEXT, bizmoney_balance BIGINT, PRIMARY KEY(dt, customer_id)
            )
        """))

    # 계정 목록 조회
    accounts = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT customer_id, account_name FROM dim_account")).fetchall()
            accounts = [{"id": str(r[0]), "name": r[1]} for r in rows]
    except Exception:
        pass

    if not accounts and CUSTOMER_ID:
        accounts = [{"id": CUSTOMER_ID, "name": "Target Account"}]

    print(f"📋 수집 대상: {len(accounts)}개 계정")
    
    today = date.today()
    
    for acc in accounts:
        cid = acc["id"]
        name = acc["name"] or "Unknown"
        
        balance = get_bizmoney(cid)
        
        if balance is None:
            continue # 에러면 저장 안 함

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fact_bizmoney_daily (dt, customer_id, bizmoney_balance)
                VALUES (:dt, :cid, :bal)
                ON CONFLICT (dt, customer_id) DO UPDATE SET bizmoney_balance = EXCLUDED.bizmoney_balance
            """), {"dt": today, "cid": cid, "bal": balance})
            
        print(f"✅ {name}({cid}): {balance:,}원 저장")

if __name__ == "__main__":
    main()
