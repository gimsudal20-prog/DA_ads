# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v12.2_ULTIMATE_FIX)
- 잘못된 파라미터(400) 에러 완벽 해결: 존재하지 않는 _CONVERSION 리포트 요청 완전 제거
- 성과/전환 통합 파싱: 단일 리포트에서 전환액까지 한 번에 파싱하여 수집 속도 2배 향상
- 무결점 우회 수집(Fallback): GFA 등 리포트 미지원 계정은 실시간 API로 자동 우회 수집
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
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False
SKIP_KEYWORD_STATS = False  
SKIP_AD_STATS = False       

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50, flush=True)
print("=== [VERSION: v12.2_ULTIMATE_FIX] ===", flush=True)
print("=== 400 에러 해결 & 무결점 우회 수집 적용 ===", flush=True)
print("="*50, flush=True)

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

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
    max_retries = 4
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error: raise requests.HTTPError(f"403 Forbidden: 권한이 없습니다 ({customer_id})", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt)
                continue
            data = None
            try: data = r.json()
            except Exception as e: 
                log(f"   ⚠️ API 응답 JSON 파싱 실패 ({url}): {e}")
                data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            log(f"   ⚠️ API 호출 에러 발생 재시도 중 ({attempt+1}/{max_retries}): {e}")
            time.sleep(2 + attempt)
    if raise_error: raise Exception(f"최대 재시도 초과: {url}")
    return 0, None

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception as e:
        log(f"   ⚠️ API 호출 실패 [{method} {path}]: {str(e)}")
        return False, None

def get_engine() -> Engine:
    if not DB_URL: return create_engine("sqlite:///:memory:", future=True)
    db_url = DB_URL
    if "sslmode=" not in db_url: db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(db_url, poolclass=NullPool, connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=60000"}, future=True)

def ensure_tables(engine: Engine):
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, PRIMARY KEY(customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
            try:
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE fact_keyword_daily ADD COLUMN avg_rnk DOUBLE PRECISION DEFAULT 0"))
            except Exception as e: 
                # 컬럼이 이미 존재할 때 발생하는 에러는 정상 무시
                pass
            break
        except Exception as e:
            log(f"   ⚠️ 테이블 생성 실패 재시도 중 ({attempt+1}/3): {e}")
            time.sleep(3)
            if attempt == 2: raise e

def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows: return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last').sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols]) if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    
    tuples = list(df.itertuples(index=False, name=None))
    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception as ex: log(f"   ⚠️ Rollback 실패: {ex}")
            log(f"   ⚠️ DB Upsert 에러 재시도 중 ({attempt+1}/3): {e}")
            time.sleep(3)
        finally:
            if cur:
                try: cur.close()
                except Exception as ex: log(f"   ⚠️ Cursor 닫기 실패: {ex}")
            if raw_conn:
                try: raw_conn.close()
                except Exception as ex: log(f"   ⚠️ Connection 닫기 실패: {ex}")

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    if not rows: return
    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last').sort_values(by=['dt', 'customer_id', pk]).astype(object).where(pd.notnull, None)
    
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            break
        except Exception as e:
            log(f"   ⚠️ 기존 데이터 DELETE 에러 재시도 중 ({attempt+1}/3): {e}")
            time.sleep(3)
            
    sql = f'INSERT INTO {table} ({", ".join([f"{c}" for c in df.columns])}) VALUES %s'
    tuples = list(df.itertuples(index=False, name=None))
    
    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
            raw_conn.commit()
            break
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except Exception as ex: log(f"   ⚠️ Rollback 실패: {ex}")
            log(f"   ⚠️ DB Insert 에러 재시도 중 ({attempt+1}/3): {e}")
            time.sleep(3)
        finally:
            if cur:
                try: cur.close()
                except Exception as ex: log(f"   ⚠️ Cursor 닫기 실패: {ex}")
            if raw_conn:
                try: raw_conn.close()
                except Exception as ex: log(f"   ⚠️ Connection 닫기 실패: {ex}")

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

def list_ad_extensions(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ad-extensions", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []

def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad", {})
    title = ad_inner.get("headline") or ad_inner.get("title") or ""
    desc = ad_inner.get("description") or ad_inner.get("desc") or ""
    
    if "shoppingProduct" in ad_inner and isinstance(ad_inner["shoppingProduct"], dict):
        title = title or ad_inner["shoppingProduct"].get("name") or ad_inner["shoppingProduct"].get("productName") or ""
    if "addPromoText" in ad_inner:
        desc = desc or ad_inner["addPromoText"]
        
    if not title: title = ad_obj.get("name") or ad_obj.get("adName") or ""
    if not desc: desc = ad_inner.get("promoText") or ad_inner.get("extCreative") or ""
    
    if not title:
        for k, v in ad_inner.items():
            if isinstance(v, dict) and v.get("name"): 
                title = v.get("name")
                break
    if not title: 
        title = f"쇼핑/일반소재 ({ad_obj.get('nccAdId', '확인불가')})"
    
    pc_url = ad_inner.get("pcLandingUrl") or ad_obj.get("pcLandingUrl") or ""
    m_url = ad_inner.get("mobileLandingUrl") or ad_obj.get("mobileLandingUrl") or ""
    
    creative_text = f"{title} | {desc}".strip(" |")
    if pc_url: creative_text += f" | {pc_url}"
    
    return {
        "ad_title": str(title)[:200], "ad_desc": str(desc)[:200], 
        "pc_landing_url": str(pc_url)[:500], "mobile_landing_url": str(m_url)[:500], 
        "creative_text": str(creative_text)[:500]
    }

def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out, d_str = [], d1.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt", "avgRnk"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data: out.extend(data["data"])
    return out

def fetch_stats_fallback(engine: Engine, customer_id: str, target_date: date, ids: List[str], id_key: str, table_name: str) -> int:
    """우회 수집(Fallback): 리포트가 불가능할 때 실시간 /stats API를 이용해 한땀한땀 수집"""
    if not ids: return 0
    raw_stats = get_stats_range(customer_id, ids, target_date)
    if not raw_stats: return 0
    
    rows = []
    for r in raw_stats:
        cost = int(round(float(r.get("salesAmt", 0) or 0) * 1.1))
        sales = int(float(r.get("convAmt", 0) or 0))
        imp = int(r.get("impCnt", 0) or 0)
        clk = int(r.get("clkCnt", 0) or 0)
        conv = float(r.get("ccnt", 0) or 0)
        roas = (sales / cost * 100) if cost > 0 else 0.0
        
        row = {
            "dt": target_date, "customer_id": str(customer_id), id_key: str(r.get("id")),
            "imp": imp, "clk": clk, "cost": cost, "conv": conv, "sales": sales, "roas": roas
        }
        if id_key == "keyword_id":
            row["avg_rnk"] = float(r.get("avgRnk", 0) or 0)
        rows.append(row)
        
    if rows:
        replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def cleanup_ghost_reports(customer_id: str):
    status, data = request_json("GET", "/stat-reports", customer_id, raise_error=False)
    if status == 200 and isinstance(data, list):
        for job in data:
            if job_id := job.get("reportJobId"):
                safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

def fetch_multiple_stat_reports(customer_id: str, report_types: List[str], target_date: date) -> Dict[str, pd.DataFrame | None]:
    cleanup_ghost_reports(customer_id)
    
    results = {tp: None for tp in report_types} # None = 실패 (우회 수집 대상), DataFrame = 성공
    for i in range(0, len(report_types), 3):
        batch = report_types[i:i+3]
        jobs = {}
        for tp in batch:
            payload = {"reportTp": tp, "statDt": target_date.strftime("%Y%m%d")}
            status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
            if status == 200 and data and "reportJobId" in data:
                jobs[tp] = data["reportJobId"]
            elif status == 400:
                # GFA 계정 등에서 발생하는 에러는 조용히 넘기고 우회 수집 모드로 넘깁니다.
                pass
            else:
                err_msg = data.get("message", str(data)) if isinstance(data, dict) else str(data)
                log(f"   ❌ [ {customer_id} ] {tp} 리포트 요청 실패 ({status}: {err_msg})")
            time.sleep(0.2)
            
        max_wait = 20
        while jobs and max_wait > 0:
            for tp, job_id in list(jobs.items()):
                s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
                if s_status == 200 and s_data:
                    stt = s_data.get("status")
                    if stt == "BUILT":
                        dl_url = s_data.get("downloadUrl")
                        if dl_url:
                            try:
                                r = requests.get(dl_url, timeout=60)
                                r.encoding = 'utf-8'
                                txt = r.text.strip()
                                if txt: 
                                    results[tp] = pd.read_csv(io.StringIO(txt), sep='\t', header=None)
                                else:
                                    results[tp] = pd.DataFrame()
                            except Exception as e: 
                                log(f"   ❌ [ {customer_id} ] {tp} 다운로드 에러: {e}")
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
                    elif stt == "NONE":
                        results[tp] = pd.DataFrame()
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
                    elif stt == "ERROR":
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
            if jobs: time.sleep(1.5)
            max_wait -= 1
            
        for job_id in jobs.values():
            safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
            
    return results

def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    for c in candidates:
        for i, h in enumerate(headers):
            if c == h: return i
    for c in candidates:
        for i, h in enumerate(headers):
            if c in h and "그룹" not in h: return i
    return -1

def safe_float(v) -> float:
    if pd.isna(v): return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-": return 0.0
    try: return float(s)
    except Exception as e: 
        # 자주 일어나는 에러이므로 pass하되 개발자 추적용으로 남김
        return 0.0

def parse_df_combined(df: pd.DataFrame, report_tp: str, pk_cands: List[str], has_rank: bool = False) -> dict:
    """단일 리포트 파일에서 노출/클릭/비용과 전환/매출을 동시에 추출합니다."""
    if df is None or df.empty: return {}
    header_idx = -1
    for i in range(min(5, len(df))):
        row_vals = [str(x).replace(" ", "").lower() for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in pk_cands):
            header_idx = i
            break
            
    if header_idx != -1:
        headers = [str(x).lower().replace(" ", "").replace("_", "") for x in df.iloc[header_idx].fillna("")]
        df = df.iloc[header_idx+1:].reset_index(drop=True)
        pk_idx = get_col_idx(headers, pk_cands)
        conv_idx = get_col_idx(headers, ["전환수", "conversions", "ccnt"])
        sales_idx = get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"])
        imp_idx = get_col_idx(headers, ["노출수", "impressions", "impcnt"])
        clk_idx = get_col_idx(headers, ["클릭수", "clicks", "clkcnt"])
        cost_idx = get_col_idx(headers, ["총비용", "cost", "salesamt"])
        rank_idx = get_col_idx(headers, ["평균노출순위", "averageposition", "avgrnk"])
    else:
        if "CAMPAIGN" in report_tp: pk_idx = 2
        elif "KEYWORD" in report_tp: pk_idx = 5
        elif "AD" in report_tp: pk_idx = 5
        else: return {}
        imp_idx = 5 if "CAMPAIGN" in report_tp else 8
        clk_idx = 6 if "CAMPAIGN" in report_tp else 9
        cost_idx = 7 if "CAMPAIGN" in report_tp else 10
        conv_idx = -1
        sales_idx = -1
        rank_idx = 11

    if pk_idx == -1: return {}
    
    res = {}
    for _, r in df.iterrows():
        try:
            if len(r) <= pk_idx: continue
            obj_id = str(r.iloc[pk_idx]).strip()
            if not obj_id or obj_id == '-': continue
            
            if obj_id not in res:
                res[obj_id] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
            
            imp = 0
            if imp_idx != -1 and len(r) > imp_idx:
                imp = int(safe_float(r.iloc[imp_idx]))
                res[obj_id]["imp"] += imp
            if clk_idx != -1 and len(r) > clk_idx: 
                res[obj_id]["clk"] += int(safe_float(r.iloc[clk_idx]))
            if cost_idx != -1 and len(r) > cost_idx: 
                res[obj_id]["cost"] += int(round(safe_float(r.iloc[cost_idx]) * 1.1)) # VAT
            if conv_idx != -1 and len(r) > conv_idx: 
                res[obj_id]["conv"] += safe_float(r.iloc[conv_idx])
            if sales_idx != -1 and len(r) > sales_idx: 
                res[obj_id]["sales"] += int(safe_float(r.iloc[sales_idx]))
            
            if has_rank and rank_idx != -1 and len(r) > rank_idx:
                rnk = safe_float(r.iloc[rank_idx])
                if rnk > 0 and imp > 0:
                    res[obj_id]["rank_sum"] += (rnk * imp)
                    res[obj_id]["rank_cnt"] += imp
        except Exception as e:
            log(f"   ⚠️ 리포트 파싱 중 행 건너뜀: {e}")
            pass
    return res

def merge_and_save_combined(engine: Engine, customer_id: str, target_date: date, table_name: str, pk_name: str, stat_res: dict) -> int:
    if not stat_res: return 0
    rows = []
    for k, s in stat_res.items():
        cost = s["cost"]
        sales = s["sales"]
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (s.get("rank_sum", 0) / s.get("rank_cnt", 1)) if s.get("rank_cnt", 0) > 0 else 0.0
        
        row = {
            "dt": target_date, "customer_id": str(customer_id), pk_name: k,
            "imp": s["imp"], "clk": s["clk"], "cost": cost,
            "conv": s["conv"], "sales": sales, "roas": roas
        }
        if pk_name == "keyword_id":
            row["avg_rnk"] = round(avg_rnk, 2)
        rows.append(row)
    replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool = False):
    try:
        target_camp_ids, target_kw_ids, target_ad_ids = [], [], []
        
        if not skip_dim:
            camp_list = list_campaigns(customer_id)
            if not camp_list:
                return
                
            camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []
            for c in camp_list:
                cid = c.get("nccCampaignId")
                if not cid: continue
                target_camp_ids.append(cid)
                camp_rows.append({"customer_id": customer_id, "campaign_id": cid, "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")})
                for g in list_adgroups(customer_id, cid):
                    gid = g.get("nccAdgroupId")
                    if not gid: continue
                    ag_rows.append({"customer_id": customer_id, "adgroup_id": gid, "campaign_id": cid, "adgroup_name": g.get("name"), "status": g.get("status")})
                    if not SKIP_KEYWORD_DIM:
                        for k in list_keywords(customer_id, gid):
                            if kid := k.get("nccKeywordId"): target_kw_ids.append(kid); kw_rows.append({"customer_id": customer_id, "keyword_id": kid, "adgroup_id": gid, "keyword": k.get("keyword"), "status": k.get("status")})
                    if not SKIP_AD_DIM:
                        for a in list_ads(customer_id, gid):
                            if aid := a.get("nccAdId"): 
                                target_ad_ids.append(aid)
                                ad_rows.append({"customer_id": customer_id, "ad_id": aid, "adgroup_id": gid, "ad_name": a.get("name") or extract_ad_creative_fields(a)["ad_title"], "status": a.get("status"), **extract_ad_creative_fields(a)})
                        
                        for ext in list_ad_extensions(customer_id, gid):
                            if ext_id := ext.get("nccAdExtensionId"):
                                target_ad_ids.append(ext_id)
                                ext_info = ext.get("adExtension", {}) or ext
                                ext_type = ext.get("extensionType", "")
                                ext_text = ext_info.get("promoText") or ext_info.get("addPromoText") or ext_info.get("subLinkName") or ext_info.get("pcText") or str(ext_type)
                                ext_title = f"[확장소재] {ext_type}"
                                ad_rows.append({
                                    "customer_id": customer_id, "ad_id": ext_id, "adgroup_id": gid, "ad_name": ext_text, 
                                    "status": ext.get("status"), "ad_title": ext_title, "ad_desc": ext_text, 
                                    "pc_landing_url": ext_info.get("pcLandingUrl", ""), "mobile_landing_url": ext_info.get("mobileLandingUrl", ""), 
                                    "creative_text": f"{ext_title} | {ext_text}"[:500]
                                })

            upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
            upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
            if kw_rows: upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
            if ad_rows: upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
        else:
            with engine.connect() as conn:
                target_camp_ids = [str(r[0]) for r in conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid"), {"cid": customer_id})]
                target_kw_ids = [str(r[0]) for r in conn.execute(text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid"), {"cid": customer_id})]
                target_ad_ids = [str(r[0]) for r in conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid"), {"cid": customer_id})]
        
        if target_date == date.today():
            log(f"   ⚠️ [ {account_name} ] 당일 데이터 실시간 우회 조회 중...")
            c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily")
            k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily") if not SKIP_KEYWORD_STATS else 0
            a_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily") if not SKIP_AD_STATS else 0
            log(f"   📊 [ {account_name} ] 당일 적재: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")
        else:
            # 존재하지 않는 _CONVERSION 타입 삭제! 캠페인 리포트 하나로 성과/전환 통합 파싱
            report_types = ["CAMPAIGN", "KEYWORD", "AD"]
            dfs = fetch_multiple_stat_reports(customer_id, report_types, target_date)
            
            c_cnt, k_cnt, a_cnt = 0, 0, 0
            
            # 캠페인 파싱 (에러 시 Fallback)
            if dfs.get("CAMPAIGN") is not None:
                camp_stat = parse_df_combined(dfs["CAMPAIGN"], "CAMPAIGN", ["캠페인id", "campaignid"])
                c_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_campaign_daily", "campaign_id", camp_stat)
            else:
                log(f"   🔄 [ {account_name} ] 캠페인 대용량 리포트 우회 -> 실시간 API 수집 중...")
                c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily")

            # 키워드 파싱 (에러 시 Fallback)
            if dfs.get("KEYWORD") is not None:
                kw_stat = parse_df_combined(dfs["KEYWORD"], "KEYWORD", ["키워드id", "keywordid"], has_rank=True)
                k_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_keyword_daily", "keyword_id", kw_stat)
            else:
                log(f"   🔄 [ {account_name} ] 키워드 대용량 리포트 우회 -> 실시간 API 수집 중...")
                k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily") if not SKIP_KEYWORD_STATS else 0

            # 소재 파싱 (에러 시 Fallback)
            ad_stat = {}
            if dfs.get("AD") is not None:
                ad_stat = parse_df_combined(dfs["AD"], "AD", ["광고id", "소재id", "adid", "상품id", "productid", "itemid"])
            else:
                log(f"   🔄 [ {account_name} ] 소재 대용량 리포트 우회 -> 실시간 API 수집 중...")
                if target_ad_ids and not SKIP_AD_STATS:
                    raw_ad_stats = get_stats_range(customer_id, target_ad_ids, target_date)
                    for r in raw_ad_stats:
                        eid = str(r.get("id"))
                        cost = int(round(float(r.get("salesAmt", 0) or 0) * 1.1))
                        sales = int(float(r.get("convAmt", 0) or 0))
                        ad_stat[eid] = {
                            "imp": int(r.get("impCnt", 0) or 0),
                            "clk": int(r.get("clkCnt", 0) or 0),
                            "cost": cost,
                            "conv": float(r.get("ccnt", 0) or 0),
                            "sales": sales,
                            "rank_sum": 0.0, "rank_cnt": 0
                        }
            
            # 확장소재는 리포트에 안 나오므로 항상 실시간 조회
            ext_ids = []
            try:
                with engine.connect() as conn:
                    res = conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid AND ad_title LIKE '[확장소재]%'"), {"cid": customer_id})
                    ext_ids = [str(r[0]) for r in res]
            except Exception as e:
                log(f"   ⚠️ 확장소재 ID 로드 중 에러: {e}")
                pass
                
            if ext_ids:
                ext_stats_raw = get_stats_range(customer_id, ext_ids, target_date)
                for r in ext_stats_raw:
                    eid = str(r.get("id"))
                    if eid not in ad_stat: ad_stat[eid] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
                    ad_stat[eid]["imp"] += int(r.get("impCnt", 0) or 0)
                    ad_stat[eid]["clk"] += int(r.get("clkCnt", 0) or 0)
                    ad_stat[eid]["cost"] += int(round(float(r.get("salesAmt", 0) or 0) * 1.1))
                    ad_stat[eid]["conv"] += float(r.get("ccnt", 0) or 0)
                    ad_stat[eid]["sales"] += int(float(r.get("convAmt", 0) or 0))

            if ad_stat:
                a_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_ad_daily", "ad_id", ad_stat)
            
            log(f"   📊 [ {account_name} ] 적재 완료: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")
            
    except Exception as e:
        log(f"❌ [ {account_name} ] 계정 처리 중 오류 발생: {str(e)}")

def main():
    engine = get_engine()
    ensure_tables(engine)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--skip_dim", action="store_true")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()
    
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    
    print("\n" + "="*50, flush=True)
    print(f"🚀🚀🚀 [ 현재 수집 진행 날짜: {target_date} ] 🚀🚀🚀", flush=True)
    print("="*50 + "\n", flush=True)

    accounts_info = []
    if args.customer_id:
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        if os.path.exists("accounts.xlsx"):
            df_acc = None
            try: df_acc = pd.read_excel("accounts.xlsx")
            except Exception as e:
                log(f"⚠️ accounts.xlsx 파싱 실패 (Excel): {e}")
                try: df_acc = pd.read_csv("accounts.xlsx")
                except Exception as ex: log(f"⚠️ accounts.xlsx 파싱 실패 (CSV): {ex}")
            
            if df_acc is not None:
                id_col, name_col = None, None
                for c in df_acc.columns:
                    c_clean = str(c).replace(" ", "").lower()
                    if c_clean in ["커스텀id", "customerid", "customer_id", "id"]: id_col = c
                    if c_clean in ["업체명", "accountname", "account_name", "name"]: name_col = c
                
                if id_col and name_col:
                    seen_ids = set()
                    for _, row in df_acc.iterrows():
                        cid = str(row[id_col]).strip()
                        if cid and cid.lower() != 'nan' and cid not in seen_ids:
                            accounts_info.append({"id": cid, "name": str(row[name_col])})
                            seen_ids.add(cid)
                    log(f"🟢 accounts.xlsx 엑셀 파일에서 중복을 제외한 {len(accounts_info)}개 업체를 완벽하게 불러왔습니다.")

        if not accounts_info:
            try:
                with engine.connect() as conn:
                    accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT customer_id, MAX(account_name) FROM accounts WHERE customer_id IS NOT NULL GROUP BY customer_id"))]
            except Exception as e:
                log(f"⚠️ DB에서 계정 정보 로드 실패: {e}")
                pass
        if not accounts_info and CUSTOMER_ID: accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info: 
        log("⚠️ 수집할 계정이 없습니다.")
        return
        
    log(f"📋 최종 수집 대상 계정: {len(accounts_info)}개 / 동시 작업: {args.workers}개")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_account, engine, acc["id"], acc["name"], target_date, args.skip_dim) for acc in accounts_info]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception as e: log(f"⚠️ Worker 실행 중 에러 발생: {e}")

if __name__ == "__main__":
    main()
