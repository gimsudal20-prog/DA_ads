 # -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (Version: DIAGNOSTIC_MODE_v5)
"""

from __future__ import annotations

import os
import time
import json
import hmac
import base64
import hashlib
import sys
import urllib.parse
import urllib.request
import ssl
from datetime import datetime
from dotenv import load_dotenv

def _load_env() -> str:
    load_dotenv(override=True)
    return ""

_ENV_FILE = _load_env()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
BASE_URL = "https://api.searchad.naver.com"

# [수정] 로그에서 확인된 유효한 고객 ID를 강제로 넣었습니다.
TEST_CUSTOMER_ID = "1346816" 

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50)
print("=== [VERSION: DIAGNOSTIC_MODE_v5] ===")
print("=== ID 하드코딩: 환경변수 없이 진단을 수행합니다 ===")
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
    
    # SSL 인증서 무시 (Github Runner 환경 이슈 방지)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(full_url, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
            return res.status, json.loads(res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        return e.code, body
    except Exception as e:
        return 999, str(e)

def run_diagnostics():
    customer_id = TEST_CUSTOMER_ID
    log(f"🔍 진단 시작 (Target ID: {customer_id})")

    # ---------------------------------------------------------
    # TEST 1: 캠페인 목록 조회 (기본 권한 확인)
    # ---------------------------------------------------------
    log("\n[TEST 1] 캠페인 목록 조회 (/ncc/campaigns)")
    code, body = send_request("GET", "/ncc/campaigns", customer_id)
    if code == 200:
        log("   ✅ 성공! (API 키와 ID는 100% 정상입니다)")
    else:
        log(f"   ❌ 실패! (code={code})")
        log(f"      Response: {body}")
        return # 여기서 실패하면 뒤에는 볼 것도 없음

    # ---------------------------------------------------------
    # TEST 2: /stats (파라미터 없음)
    # ---------------------------------------------------------
    log("\n[TEST 2] 통계 API 깡통 요청 (/stats)")
    # 파라미터 없이 호출했을 때 400 Bad Request가 뜨면 서명은 통과한 것임.
    # 403 Forbidden이 뜨면 서명 자체가 틀린 것임.
    code, body = send_request("GET", "/stats", customer_id)
    
    if code == 400:
        log("   ✅ 성공! (400 Bad Request -> 서명 통과됨)")
    elif code == 403:
        log("   ❌ 실패! (403 Forbidden -> URL 서명 생성 방식이 틀림)")
        log(f"      Detail: {body}")
    else:
        log(f"   ⚠️ 의외의 결과: code={code} / {body}")

    # ---------------------------------------------------------
    # TEST 3: /stats (단순 파라미터)
    # ---------------------------------------------------------
    log("\n[TEST 3] 통계 API 단순 파라미터 (fields=['impCnt'])")
    # 특수문자 [], " 가 들어간 URL을 네이버가 어떻게 받아들이는지 확인
    fields_json = json.dumps(["impCnt"]) # ["impCnt"]
    enc_fields = urllib.parse.quote(fields_json) # %5B%22impCnt%22%5D
    
    uri = f"/stats?fields={enc_fields}"
    
    code, body = send_request("GET", uri, customer_id)
    
    if code == 200 or code == 400:
        log("   ✅ 성공! (특수문자 인코딩 서명 방식이 맞습니다)")
    elif code == 403:
        log("   ❌ 실패! (403 Forbidden -> 특수문자 서명 방식 불일치)")
        log(f"      URI: {uri}")
        log(f"      Detail: {body}")

if __name__ == "__main__":
    main()
