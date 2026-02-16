# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Version: DIAGNOSTIC_MODE_v4)
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

def _load_env() -> str:
    load_dotenv(override=True)
    return ""

_ENV_FILE = _load_env()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()
BASE_URL = "https://api.searchad.naver.com"
IDS_CHUNK = 1

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50)
print("=== [VERSION: DIAGNOSTIC_MODE_v4] ===")
print("=== 서명/파라미터 문제 격리 테스트를 수행합니다 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    message = f"{timestamp}.{method}.{uri}"
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(hash.digest()).decode("utf-8")

def send_request(method: str, uri: str, customer_id: str) -> Any:
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
            return res.status, json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        return e.code, body
    except Exception as e:
        return 999, str(e)

# --- [진단 로직] ---
def run_diagnostics(customer_id: str):
    log("🔬 [진단 1] 파라미터 없는 깡통 요청 (/stats)")
    code, body = send_request("GET", "/stats", customer_id)
    
    if code == 400:
        log("   ✅ 성공! (400 Bad Request가 떴다는 건 서명이 통과됐다는 뜻입니다)")
    elif code == 403:
        log(f"   ❌ 실패! (403 Forbidden - 서명 자체가 틀렸습니다)")
        log(f"      Response: {body}")
        # 여기서 실패하면 더 진행해도 의미 없음
        return False
    else:
        log(f"   ⚠️ 예상 밖의 응답: {code} / {body}")

    log("🔬 [진단 2] 단순 파라미터 1개 요청 (fields만)")
    # fields=["impCnt"] -> 인코딩 테스트
    fields_val = json.dumps(["impCnt"])
    enc_fields = urllib.parse.quote(fields_val)
    uri = f"/stats?fields={enc_fields}"
    
    code, body = send_request("GET", uri, customer_id)
    if code == 200 or code == 400: # 400이면 다른 필수 파라미터가 없어서 그런 거니 서명은 통과
        log("   ✅ 성공! (단순 파라미터 서명은 정상입니다)")
    else:
        log(f"   ❌ 실패! (복잡한 파라미터에서 서명이 깨집니다)")
        log(f"      URI: {uri}")
        log(f"      Response: {body}")
        return False
        
    log("🔬 [진단 3] 전체 파라미터 요청 (실제 데이터)")
    return True

# 캠페인 ID 가져오기 (이건 성공한다고 가정)
def get_first_campaign(customer_id: str):
    code, body = send_request("GET", "/ncc/campaigns", customer_id)
    if code == 200 and isinstance(body, list) and len(body) > 0:
        return body[0]["nccCampaignId"]
    return None

def main():
    # 1. 고객 ID 확인
    target_customer = CUSTOMER_ID
    if not target_customer:
        # DB 연결 시도 생략하고 환경변수 없으면 종료
        die("CUSTOMER_ID 환경변수가 없습니다.")

    log(f"🩺 진단 시작 (Customer: {target_customer})")
    
    # 2. 캠페인 목록 조회 테스트
    camp_id = get_first_campaign(target_customer)
    if not camp_id:
        die("캠페인 목록 조회 실패. API 키 권한을 다시 확인하세요.")
    
    log(f"   > 캠페인 조회 성공. 테스트용 ID: {camp_id}")
    
    # 3. Stats 진단
    if run_diagnostics(target_customer):
        log("🎉 진단 통과! 이제 로직을 합치면 됩니다.")
    else:
        log("💥 진단 실패. 위 로그를 분석해야 합니다.")

if __name__ == "__main__":
    main()
