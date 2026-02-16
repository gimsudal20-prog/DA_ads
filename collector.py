# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v8.0 - Full Hierarchy)
- 기능: 캠페인 > 광고그룹 > 키워드/소재 구조 수집
- 특징: Path Only 서명 방식 적용 (403 방지), 소재 문구 파싱 포함
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
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip() # 환경변수 ID

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
SLEEP_BETWEEN_CALLS = 0.1 # API 보호를 위해 약간의 딜레이
IDS_CHUNK = 5 # URL 길이 제한 고려하여 안전하게 5개씩

# 수집 여부 스위치 (필요시 환경변수로 제어 가능)
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
print("=== [VERSION: v8.0_FULL_HIERARCHY] ===")
print("=== 캠페인/키워드/소재 데이터를 모두 수집합니다 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

# -------------------------
# 2. 서명 및 요청 (Path Only Signature)
# -------------------------
def now_millis() -> str:
    return str(int(time.time() * 1000))

def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    # 쿼리 스트링 제외, 경로만 서명 (403 해결의 핵심)
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
        # requests 라이브러리는 params를 자동으로 인코딩해서 붙여줌
        # 하지만 서명은 'path'만 가지고 했으므로 네이버 서버 통과 OK
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
    # DIM 테이블 (구조 정보)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
        
        # 소재 테이블 (상세 내용 포함)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_ad (
                customer_id TEXT, ad_id TEXT, adgroup_id TEXT,
                ad_name TEXT, status TEXT,
                ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT,
                PRIMARY KEY(customer_id, ad_id)
            )
        """))

        # FACT 테이블 (일별 성과)
        # 1. 캠페인
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_campaign_daily (
                dt DATE, customer_id TEXT, campaign_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, campaign_id)
            )
        """))
        # 2. 키워드
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_keyword_daily (
                dt DATE, customer_id TEXT, keyword_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, keyword_id)
            )
        """))
        # 3. 소재(Ad)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_ad_daily (
                dt DATE, customer_id TEXT, ad_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, ad_id)
            )
        """))

def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows: return
    
    # Pandas to SQL (replace 방식은 너무 느리므로 delete insert 방식 사용 권장하지만, 여기서는 간단히 처리)
    # 실제 프로덕션에서는 COPY 등을 쓰지만, 여기서는 Loop Insert or Pandas to_sql
    # 충돌 방지를 위해 간단한 로직: 기존 것 무시하고 덮어쓰기 or DELETE 후 INSERT
    # 여기서는 "DELETE 후 INSERT" 전략 사용 (PK 기준)
    
    # 간단하게 Pandas to_sql 사용 (append 모드, 에러 시 무시하는 로직 대신 replace 사용시 테이블이 날아가므로 주의)
    # 안전하게: 그냥 루프 돌면서 insert (속도 이슈 있으면 개선 필요)
    # 로컬 코드의 upsert_many 로직을 흉내내어 구현:
    
    # 여기서는 간단히: "해당 ID들에 대해 DELETE 후 INSERT"는 복잡하므로
    # INSERT OR REPLACE 구문이 DB마다 달라서, SQLAlchemy 기본 기능 활용
    
    # 가장 확실한 방법: 임시 테이블 없이 그냥 한땀한땀 넣되, 에러나면 pass (속도 느림)
    # 또는 Pandas의 to_sql을 쓰되 method='multi'
    
    df = pd.DataFrame(rows)
    # 데이터가 많으면 청크로 나눔
    try:
        # Postgres의 경우 ON CONFLICT 처리가 복잡하므로,
        # 단순하게: 이번 배치에 있는 PK들을 먼저 지우고 다시 넣음 (Bulk Delete-Insert)
        if not df.empty:
            keys = tuple(pk_cols)
            # 이 부분은 복잡도를 줄이기 위해 생략하고, 그냥 try-except insert로 진행
            # (대량 데이터 시 성능 이슈가 있을 수 있으나 현재 규모에선 OK)
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    # Upsert 쿼리 생성 (Postgres 전용)
                    cols = list(row.index)
                    vals = {c: row[c] for c in cols}
                    
                    # 쿼리 조립
                    col_str = ", ".join(cols)
                    val_str = ", ".join([f":{c}" for c in cols])
                    pk_str = ", ".join(pk_cols)
                    update_str = ", ".join([f"{c}=:{c}" for c in cols if c not in pk_cols])
                    
                    sql = f"""
                        INSERT INTO {table} ({col_str}) VALUES ({val_str})
                        ON CONFLICT ({pk_str}) DO UPDATE SET {update_str}
                    """
                    conn.execute(text(sql), vals)
    except Exception as e:
        log(f"⚠️ Upsert 오류 ({table}): {e}")

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    if not rows: return
    # 해당 날짜/고객 데이터 삭제 후 통째로 삽입
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"),
            {"cid": str(customer_id), "dt": d1}
        )
        if rows:
            conn.execute(
                text(f"INSERT INTO {table} ({', '.join(rows[0].keys())}) VALUES ({', '.join([':'+k for k in rows[0].keys()])})"),
                rows
            )

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
    # 소재 내용 추출 (복잡한 JSON 구조 처리)
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
        "ad_title": title,
        "ad_desc": desc,
        "pc_landing_url": pc_url,
        "mobile_landing_url": m_url,
        "creative_text": creative_text[:500] # 너무 길면 자름
    }

# -------------------------
# 5. 성과 조회 (Stats)
# -------------------------
def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out = []
    
    d_str = str(d1)
    # 필드 정의
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    
    # 5개씩 끊어서 요청 (IDS_CHUNK)
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
        "dt": d1,
        "customer_id": str(customer_id),
        id_key: str(r.get("id")),
        "imp": int(r.get("impCnt", 0) or 0),
        "clk": int(r.get("clkCnt", 0) or 0),
        "cost": cost,
        "conv": float(r.get("ccnt", 0) or 0),
        "sales": sales,
        "roas": roas
    }

# -------------------------
# 6. 메인 로직
# -------------------------
def process_account(engine: Engine, customer_id: str, target_date: date):
    log(f"🚀 처리 시작: {customer_id} ({target_date})")
    
    # 1. DIM Refresh (구조 수집)
    # 캠페인 -> 광고그룹 -> 키워드/소재 순으로 탐색
    camp_list = list_campaigns(customer_id)
    log(f"   > 캠페인 {len(camp_list)}개 발견")
    
    camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []
    
    # 성과 수집을 위한 ID 모음
    target_camp_ids = []
    target_kw_ids = []
    target_ad_ids = []

    for c in camp_list:
        cid = c.get("nccCampaignId")
        if not cid: continue
        target_camp_ids.append(cid)
        
        camp_rows.append({
            "customer_id": customer_id, "campaign_id": cid, 
            "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")
        })
        
        # 광고그룹 조회
        ags = list_adgroups(customer_id, cid)
        for g in ags:
            gid = g.get("nccAdgroupId")
            if not gid: continue
            
            ag_rows.append({
                "customer_id": customer_id, "adgroup_id": gid, "campaign_id": cid,
                "adgroup_name": g.get("name"), "status": g.get("status")
            })
            
            # 키워드 조회
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
            
            # 소재 조회
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
            
            time.sleep(SLEEP_BETWEEN_CALLS) # API 부하 조절

    # DIM 저장
    log("   > 구조 데이터(DIM) DB 저장 중...")
    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
    upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    
    # 2. FACT Refresh (성과 수집)
    log(f"   > 성과 데이터(FACT) 수집 시작... (날짜: {target_date})")
    
    # 캠페인 성과
    if target_camp_ids:
        print(f"     [캠페인 {len(target_camp_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_camp_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "campaign_id") for r in raw]
        replace_fact_range(engine, "fact_campaign_daily", rows, customer_id, target_date)
        print(" 저장 완료")

    # 키워드 성과
    if target_kw_ids and not SKIP_KEYWORD_STATS:
        print(f"     [키워드 {len(target_kw_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_kw_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "keyword_id") for r in raw]
        replace_fact_range(engine, "fact_keyword_daily", rows, customer_id, target_date)
        print(" 저장 완료")
        
    # 소재 성과
    if target_ad_ids and not SKIP_AD_STATS:
        print(f"     [소재 {len(target_ad_ids)}개] ", end="")
        raw = get_stats_range(customer_id, target_ad_ids, target_date)
        rows = [parse_stats(r, target_date, customer_id, "ad_id") for r in raw]
        replace_fact_range(engine, "fact_ad_daily", rows, customer_id, target_date)
        print(" 저장 완료")

def main():
    engine = get_engine()
    ensure_tables(engine)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
        
    # 계정 목록 로드 (DB 우선, 없으면 ENV)
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
        log("⚠️ 수집할 계정이 없습니다.")
        return

    for cid in accounts:
        try:
            process_account(engine, cid, target_date)
        except Exception as e:
            log(f"❌ 오류 발생 ({cid}): {e}")
            import traceback
            traceback.print_exc()

    log("✅ 모든 작업 완료")

if __name__ == "__main__":
    main()
