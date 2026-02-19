# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v9.0 - 대용량 통계 보고서 API 적용)
- 개선 1: /stat-reports API를 활용한 대용량 TSV 다운로드 방식 적용 (호출 횟수 극감)
- 개선 2: ThreadPoolExecutor를 통한 멀티스레딩(동시 수집) 적용
- 개선 3: 스마트 재시도 (429 에러 대응) 로직 포함
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
import io
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------
# 1. 환경변수 및 설정
# -------------------------
load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50)
print("=== [VERSION: v9.0_STAT_REPORTS] ===")
print("=== 대용량 리포트 API + 병렬 수집 엔진 ===")
print("="*50)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

# -------------------------
# 2. 서명 및 요청 (스마트 재시도)
# -------------------------
def now_millis() -> str:
    return str(int(time.time() * 1000))

def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
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

def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    max_retries = 3
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            
            if r.status_code == 429 or r.status_code >= 500:
                log(f"⚠️ API 한도/오류 ({r.status_code}) - {customer_id}. 2초 대기 후 재시도...")
                time.sleep(2)
                continue

            data = None
            try:
                data = r.json()
            except Exception:
                data = r.text
                
            if raise_error and r.status_code >= 400:
                log(f"🔥 API Error {r.status_code}: {str(data)[:200]}")
                raise requests.HTTPError(f"{r.status_code}", response=r)
                
            return r.status_code, data
            
        except requests.exceptions.RequestException as e:
            log(f"⚠️ 네트워크 오류 - {customer_id}: {e}. 2초 후 재시도...")
            time.sleep(2)
            
    if raise_error:
        raise Exception(f"최대 재시도 초과: {url}")
    return 0, None

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
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
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_ad (
                customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT,
                ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT,
                PRIMARY KEY(customer_id, ad_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_campaign_daily (
                dt DATE, customer_id TEXT, campaign_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, campaign_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_keyword_daily (
                dt DATE, customer_id TEXT, keyword_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, keyword_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_ad_daily (
                dt DATE, customer_id TEXT, ad_id TEXT,
                imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY(dt, customer_id, ad_id)
            )
        """))

def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows: return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last')
    temp_table = f"tmp_{table}_{int(time.time()*1000)}"
    try:
        with engine.begin() as conn:
            df.head(0).to_sql(temp_table, conn, index=False, if_exists='replace')
            df.to_sql(temp_table, conn, index=False, if_exists='append', method='multi', chunksize=1000)
            cols = ", ".join([f'"{c}"' for c in df.columns])
            pk_clause = ", ".join([f'"{c}"' for c in pk_cols])
            set_clause = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in df.columns if c not in pk_cols])
            
            if set_clause:
                sql = f'INSERT INTO {table} ({cols}) SELECT * FROM {temp_table} ON CONFLICT ({pk_clause}) DO UPDATE SET {set_clause}'
            else:
                sql = f'INSERT INTO {table} ({cols}) SELECT * FROM {temp_table} ON CONFLICT ({pk_clause}) DO NOTHING'
            conn.execute(text(sql))
            conn.execute(text(f'DROP TABLE {temp_table}'))
    except Exception as e:
        log(f"⚠️ Upsert Error in {table}: {e}")

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    if not rows: return
    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last')
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            df.to_sql(table, conn, index=False, if_exists='append', method='multi', chunksize=1000)
    except Exception as e:
        log(f"⚠️ Fact Insert Error in {table}: {e}")

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
    return {"ad_title": title, "ad_desc": desc, "pc_landing_url": pc_url, "mobile_landing_url": m_url, "creative_text": creative_text[:500]}

# -------------------------
# 5. 대용량 성과 리포트 조회 (Stat-Reports API)
# -------------------------
def fetch_stat_report(customer_id: str, report_tp: str, target_date: date) -> pd.DataFrame:
    dt_str = target_date.strftime("%Y%m%d")
    payload = {"reportTp": report_tp, "statDt": dt_str}
    
    # 1. 리포트 생성 요청
    status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
    if status != 200 or not data or "reportJobId" not in data:
        return pd.DataFrame()
        
    job_id = data["reportJobId"]
    download_url = None
    
    # 2. 리포트 생성 완료 대기 (최대 60초)
    for _ in range(30):
        time.sleep(2)
        s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
        if s_status == 200 and s_data:
            job_status = s_data.get("status")
            if job_status == "BUILT":
                download_url = s_data.get("downloadUrl")
                break
            elif job_status in ["ERROR", "NONE"]:
                return pd.DataFrame()
                
    if not download_url:
        log(f"⚠️ [ {customer_id} ] {report_tp} 리포트 생성 대기 시간 초과")
        return pd.DataFrame()
        
    # 3. TSV 다운로드 및 파싱
    try:
        r = requests.get(download_url, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), sep='\t')
        return df
    except Exception as e:
        log(f"⚠️ [ {customer_id} ] TSV 다운로드 실패: {e}")
        return pd.DataFrame()

def process_fact_from_tsv(engine: Engine, df: pd.DataFrame, table_name: str, id_col_name: str, customer_id: str, target_date: date):
    if df is None or df.empty:
        return
        
    def _find(kws):
        for c in df.columns:
            c_clean = c.replace(" ", "").lower()
            for kw in kws:
                if kw in c_clean: return c
        return None
        
    cid_col = _find(["캠페인아이디"]) if "campaign" in table_name else (_find(["키워드아이디"]) if "keyword" in table_name else _find(["소재아이디"]))
    if not cid_col: return
        
    imp_col = _find(["노출수"])
    clk_col = _find(["클릭수"])
    cost_col = _find(["총비용", "비용"])
    conv_col = _find(["총전환수", "전환수"])
    sales_col = _find(["전환매출액", "매출액"])
    
    rows = []
    for _, row in df.iterrows():
        target_id = str(row[cid_col])
        if not target_id or target_id == 'nan': continue
        
        imp = int(row[imp_col]) if imp_col and pd.notna(row[imp_col]) else 0
        clk = int(row[clk_col]) if clk_col and pd.notna(row[clk_col]) else 0
        cost_raw = float(row[cost_col]) if cost_col and pd.notna(row[cost_col]) else 0.0
        
        # VAT 제외 금액으로 변환 (기존 /stats API의 salesAmt와 기준 맞춤)
        cost_ex_vat = int(round(cost_raw / 1.1)) if cost_raw > 0 else 0
        
        conv = float(row[conv_col]) if conv_col and pd.notna(row[conv_col]) else 0.0
        sales = int(row[sales_col]) if sales_col and pd.notna(row[sales_col]) else 0
        roas = (sales / cost_ex_vat * 100) if cost_ex_vat > 0 else 0.0
        
        rows.append({
            "dt": target_date, "customer_id": str(customer_id), id_col_name: target_id,
            "imp": imp, "clk": clk, "cost": cost_ex_vat, "conv": conv, "sales": sales, "roas": roas
        })
        
    replace_fact_range(engine, table_name, rows, customer_id, target_date)

# -------------------------
# 6. 메인 처리기 (단일 계정)
# -------------------------
def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date):
    log(f"🚀 처리 시작: {account_name} ({customer_id}) / 날짜: {target_date}")
    
    # 1. 구조(Dimension) 데이터 수집 (여전히 캠페인 속성/이름을 위해 필요)
    camp_list = list_campaigns(customer_id)
    if not camp_list: return
    
    camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []

    for c in camp_list:
        cid = c.get("nccCampaignId")
        if not cid: continue
        camp_rows.append({
            "customer_id": customer_id, "campaign_id": cid, 
            "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")
        })
        ags = list_adgroups(customer_id, cid)
        for g in ags:
            gid = g.get("nccAdgroupId")
            if not gid: continue
            ag_rows.append({
                "customer_id": customer_id, "adgroup_id": gid, "campaign_id": cid,
                "adgroup_name": g.get("name"), "status": g.get("status")
            })
            if not SKIP_KEYWORD_DIM:
                kws = list_keywords(customer_id, gid)
                for k in kws:
                    kid = k.get("nccKeywordId")
                    if kid:
                        kw_rows.append({"customer_id": customer_id, "keyword_id": kid, "adgroup_id": gid, "keyword": k.get("keyword"), "status": k.get("status")})
            if not SKIP_AD_DIM:
                ads = list_ads(customer_id, gid)
                for a in ads:
                    aid = a.get("nccAdId")
                    if aid:
                        fields = extract_ad_creative_fields(a)
                        ad_rows.append({"customer_id": customer_id, "ad_id": aid, "adgroup_id": gid, "ad_name": a.get("name") or fields["ad_title"], "status": a.get("status"), **fields})

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    if kw_rows: upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
    if ad_rows: upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    
    # 2. 성과(Fact) 데이터 수집 - 대용량 TSV 다운로드
    log(f"   > [ {account_name} ] 대용량 리포트(TSV) 생성 및 저장 중...")
    
    camp_df = fetch_stat_report(customer_id, "CAMPAIGN", target_date)
    process_fact_from_tsv(engine, camp_df, "fact_campaign_daily", "campaign_id", customer_id, target_date)
    
    kw_df = fetch_stat_report(customer_id, "KEYWORD", target_date)
    process_fact_from_tsv(engine, kw_df, "fact_keyword_daily", "keyword_id", customer_id, target_date)
    
    ad_df = fetch_stat_report(customer_id, "AD", target_date)
    process_fact_from_tsv(engine, ad_df, "fact_ad_daily", "ad_id", customer_id, target_date)

    log(f"✅ 완료: {account_name} ({customer_id})")

# -------------------------
# 7. 메인 실행 블록
# -------------------------
def main():
    engine = get_engine()
    ensure_tables(engine)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    args = parser.parse_args()
    
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
        
    accounts_info = []
    if args.customer_id:
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT customer_id, account_name FROM dim_account"))
                accounts_info = [{"id": row[0], "name": row[1] or "Unknown"} for row in result]
        except Exception:
            pass
        
        if not accounts_info and CUSTOMER_ID:
            accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info:
        log("⚠️ 수집할 계정이 없습니다.")
        return

    log(f"📋 수집 대상 계정: {len(accounts_info)}개")

    # 병렬 처리 적용 (한 번에 4개 업체 동시 진행)
    max_workers = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for acc in accounts_info:
            futures.append(
                executor.submit(process_account, engine, acc["id"], acc["name"], target_date)
            )
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"❌ 병렬 처리 중 계정 작업 실패: {e}")
                import traceback
                traceback.print_exc()

    log("🎉 모든 작업 완료")

if __name__ == "__main__":
    main()
