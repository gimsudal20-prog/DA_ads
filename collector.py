# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (쇼핑검색 및 확장소재 완벽 대응 버전)
- 쇼핑검색 상품 ID(nccProductId) 및 소재 ID 매핑 강화
- 노출용 제목, 이미지 URL, 확장소재 데이터 완벽 추출
- DB 및 네이버 API 커넥션 풀 유지로 속도 극대화
- 한글 엑셀 헤더(업체명, 커스텀 ID) 완벽 인식
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
import threading
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

if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def now_millis() -> str: return str(int(time.time() * 1000))

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
    session = get_session()
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error: raise requests.HTTPError(f"403 Forbidden: 권한이 없습니다 ({customer_id})", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(1 + attempt)
                continue
            data = None
            try: data = r.json()
            except Exception: data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            time.sleep(1 + attempt)
    if raise_error: raise Exception(f"최대 재시도 초과: {url}")
    return 0, None

def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception:
        return False, None

def get_engine() -> Engine:
    if not DB_URL: return create_engine("sqlite:///:memory:", future=True)
    db_url = DB_URL
    if "sslmode=" not in db_url: db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url, 
        pool_size=15, 
        max_overflow=30, 
        pool_pre_ping=True, 
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=60000"}, 
        future=True
    )

def ensure_tables(engine: Engine):
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, image_url TEXT, PRIMARY KEY(customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_off_log (dt DATE, customer_id TEXT, campaign_id TEXT, off_time TEXT, PRIMARY KEY(dt, customer_id, campaign_id))"""))
            break
        except Exception as e:
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
        except Exception:
            if raw_conn: raw_conn.rollback()
            time.sleep(3)
        finally:
            if cur: cur.close()
            if raw_conn: raw_conn.close()

def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    if not rows: return
    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=['dt', 'customer_id', pk], keep='last').sort_values(by=['dt', 'customer_id', pk]).astype(object).where(pd.notnull, None)
    
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            break
        except Exception:
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
        except Exception:
            if raw_conn: raw_conn.rollback()
            time.sleep(3)
        finally:
            if cur: cur.close()
            if raw_conn: raw_conn.close()

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
    if ok and isinstance(data, list) and data:
        return data
    ok_owner, data_owner = safe_call("GET", "/ncc/ads", customer_id, {"ownerId": adgroup_id})
    if ok_owner and isinstance(data_owner, list):
        return data_owner
    return data if ok and isinstance(data, list) else []

def list_ad_extensions(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ad-extensions", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []

def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad", {})
    
    # 1. 이미지 추출 (쇼핑검색, 일반소재 모두 대응)
    image_url = ""
    if "image" in ad_inner and isinstance(ad_inner["image"], dict):
        image_url = ad_inner["image"].get("imageUrl", "")
    if not image_url: 
        image_url = ad_inner.get("imageUrl") or ad_inner.get("mobileImageUrl") or ad_inner.get("pcImageUrl") or ""
    if not image_url and "shoppingProduct" in ad_inner:
        image_url = ad_inner["shoppingProduct"].get("imageUrl", "")
        
    # 2. 노출용 제목 / 소재 제목 추출
    title = ""
    desc = ""
    
    # 쇼핑검색의 노출용 제목 (프로모션 문구)
    if "valData" in ad_inner and isinstance(ad_inner["valData"], dict):
        title = ad_inner["valData"].get("productName") or ad_inner["valData"].get("title") or ""
    
    # 쇼핑검색 일반 상품명
    if not title and "shoppingProduct" in ad_inner and isinstance(ad_inner["shoppingProduct"], dict):
        title = ad_inner["shoppingProduct"].get("name") or ad_inner["shoppingProduct"].get("productName") or ""
        
    # 파워링크 일반 제목
    if not title:
        title = ad_inner.get("headline") or ad_inner.get("title") or ""
        
    # 3. 설명 및 프로모션 텍스트 추출
    desc = ad_inner.get("description") or ad_inner.get("desc") or ad_inner.get("addPromoText") or ad_inner.get("promoText") or ad_inner.get("extCreative") or ""
    
    if not title: title = ad_obj.get("name") or ad_obj.get("adName") or ""
    if not title:
        for k, v in ad_inner.items():
            if isinstance(v, dict) and v.get("name"): 
                title = v.get("name")
                break
    if not title: title = f"소재 ({ad_obj.get('nccAdId', '확인불가')})"
    
    pc_url = ad_inner.get("pcLandingUrl") or ad_obj.get("pcLandingUrl") or ""
    m_url = ad_inner.get("mobileLandingUrl") or ad_obj.get("mobileLandingUrl") or ""
    
    creative_text = f"{title} | {desc}".strip(" |")
    if pc_url: creative_text += f" | {pc_url}"
    
    return {
        "ad_title": str(title)[:200], "ad_desc": str(desc)[:200], 
        "pc_landing_url": str(pc_url)[:500], "mobile_landing_url": str(m_url)[:500], 
        "creative_text": str(creative_text)[:500], "image_url": str(image_url)[:1000]
    }

def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out = []
    d_str = d1.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt", "avgRnk"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data: out.extend(data["data"])
    return out

def fetch_stats_fallback(engine: Engine, customer_id: str, target_date: date, ids: List[str], id_key: str, table_name: str) -> int:
    if not ids: return 0
    raw_stats = get_stats_range(customer_id, ids, target_date)
    if not raw_stats: return 0
    rows = []
    for r in raw_stats:
        cost = int(float(r.get("salesAmt", 0) or 0))
        sales = int(float(r.get("convAmt", 0) or 0))
        imp = int(r.get("impCnt", 0) or 0)
        clk = int(r.get("clkCnt", 0) or 0)
        conv = float(r.get("ccnt", 0) or 0)
        roas = (sales / cost * 100) if cost > 0 else 0.0
        row = {
            "dt": target_date, "customer_id": str(customer_id), id_key: str(r.get("id")),
            "imp": imp, "clk": clk, "cost": cost, "conv": conv, "sales": sales, "roas": roas
        }
        if id_key in ["campaign_id", "keyword_id", "ad_id"]: row["avg_rnk"] = float(r.get("avgRnk", 0) or 0)
        rows.append(row)
    if rows: replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def cleanup_ghost_reports(customer_id: str):
    status, data = request_json("GET", "/stat-reports", customer_id, raise_error=False)
    if status == 200 and isinstance(data, list):
        for job in data:
            if job_id := job.get("reportJobId"): safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

def fetch_multiple_stat_reports(customer_id: str, report_types: List[str], target_date: date) -> Dict[str, pd.DataFrame | None]:
    cleanup_ghost_reports(customer_id)
    results = {tp: None for tp in report_types}
    session = get_session()
    for i in range(0, len(report_types), 3):
        batch = report_types[i:i+3]
        jobs = {}
        for tp in batch:
            payload = {"reportTp": tp, "statDt": target_date.strftime("%Y%m%d")}
            status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
            if status == 200 and data and "reportJobId" in data: jobs[tp] = data["reportJobId"]
            time.sleep(0.1)
        max_wait = 25
        while jobs and max_wait > 0:
            for tp, job_id in list(jobs.items()):
                s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
                if s_status == 200 and s_data:
                    stt = s_data.get("status")
                    if stt == "BUILT":
                        dl_url = s_data.get("downloadUrl")
                        if dl_url:
                            try:
                                r = session.get(dl_url, timeout=60); r.encoding = 'utf-8'
                                txt = r.text.strip()
                                results[tp] = pd.read_csv(io.StringIO(txt), sep='\t', header=None) if txt else pd.DataFrame()
                            except Exception: pass
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id); del jobs[tp]
                    elif stt in ["NONE", "ERROR"]:
                        results[tp] = pd.DataFrame() if stt == "NONE" else None
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id); del jobs[tp]
            if jobs: time.sleep(1.0)
            max_wait -= 1
        for job_id in jobs.values(): safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
    return results

def normalize_header(v: str) -> str: return str(v).lower().replace(" ", "").replace("_", "").replace("-", "")

def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [normalize_header(h) for h in headers]
    norm_candidates = [normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h: return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c in h and "그룹" not in h: return i
    return -1

def safe_float(v) -> float:
    if pd.isna(v): return 0.0
    s = str(v).replace(",", "").strip()
    return float(s) if s and s != "-" else 0.0

def parse_df_combined(df: pd.DataFrame, report_tp: str, pk_cands: List[str], has_rank: bool = False) -> dict:
    if df is None or df.empty: return {}
    header_idx = -1
    scan_limit = min(20, len(df))
    norm_pk_cands = [normalize_header(c) for c in pk_cands]
    for i in range(scan_limit):
        row_vals = [normalize_header(x) for x in df.iloc[i].fillna("")]
        if any(any(c == v or (c and c in v) for v in row_vals) for c in norm_pk_cands):
            header_idx = i; break
    if header_idx != -1:
        headers = [normalize_header(x) for x in df.iloc[header_idx].fillna("")]
        df = df.iloc[header_idx+1:].reset_index(drop=True)
        pk_idx = get_col_idx(headers, pk_cands)
        conv_idx = get_col_idx(headers, ["전환수", "conversions", "ccnt"])
        sales_idx = get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"])
        imp_idx = get_col_idx(headers, ["노출수", "impressions", "impcnt"])
        clk_idx = get_col_idx(headers, ["클릭수", "clicks", "clkcnt"])
        cost_idx = get_col_idx(headers, ["총비용", "cost", "salesamt"])
        rank_idx = get_col_idx(headers, ["평균노출순위", "averageposition", "avgrnk"])
    else:
        return {}
    if pk_idx == -1: return {}
    res = {}
    for _, r in df.iterrows():
        try:
            obj_id = str(r.iloc[pk_idx]).strip()
            if not obj_id or obj_id == '-' or normalize_header(obj_id) in ["id", "keywordid", "adid", "campaignid", "productid"]: continue
            if obj_id not in res: res[obj_id] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
            imp = int(safe_float(r.iloc[imp_idx])) if imp_idx != -1 else 0
            res[obj_id]["imp"] += imp
            if clk_idx != -1: res[obj_id]["clk"] += int(safe_float(r.iloc[clk_idx]))
            if cost_idx != -1: res[obj_id]["cost"] += int(safe_float(r.iloc[cost_idx]))
            if conv_idx != -1: res[obj_id]["conv"] += safe_float(r.iloc[conv_idx])
            if sales_idx != -1: res[obj_id]["sales"] += int(safe_float(r.iloc[sales_idx]))
            if has_rank and rank_idx != -1:
                rnk = safe_float(r.iloc[rank_idx])
                if rnk > 0 and imp > 0: res[obj_id]["rank_sum"] += (rnk * imp); res[obj_id]["rank_cnt"] += imp
        except Exception: pass
    return res

def merge_and_save_combined(engine: Engine, customer_id: str, target_date: date, table_name: str, pk_name: str, stat_res: dict) -> int:
    if not stat_res: return 0
    rows = []
    for k, s in stat_res.items():
        cost = s["cost"]; sales = s["sales"]; roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (s.get("rank_sum", 0) / s.get("rank_cnt", 1)) if s.get("rank_cnt", 0) > 0 else 0.0
        row = {"dt": target_date, "customer_id": str(customer_id), pk_name: k, "imp": s["imp"], "clk": s["clk"], "cost": cost, "conv": s["conv"], "sales": sales, "roas": roas}
        if pk_name in ["campaign_id", "keyword_id", "ad_id"]: row["avg_rnk"] = round(avg_rnk, 2)
        rows.append(row)
    replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool = False):
    try:
        target_camp_ids, target_kw_ids, target_ad_ids = [], [], []
        if not skip_dim:
            camp_list = list_campaigns(customer_id)
            if not camp_list: return
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
                        # 일반 소재 및 쇼핑검색 소재 수집
                        for a in list_ads(customer_id, gid):
                            aid = a.get("nccAdId")
                            pid = a.get("ad", {}).get("shoppingProduct", {}).get("nccProductId")
                            extracted = extract_ad_creative_fields(a)
                            
                            if aid: 
                                target_ad_ids.append(str(aid))
                                ad_rows.append({"customer_id": customer_id, "ad_id": str(aid), "adgroup_id": gid, "ad_name": a.get("name") or extracted["ad_title"], "status": a.get("status"), **extracted})
                            
                            # 쇼핑검색의 경우 nccProductId도 ID로 사용하여 듀얼 수집
                            if pid and str(pid) != str(aid): 
                                target_ad_ids.append(str(pid))
                                ad_rows.append({"customer_id": customer_id, "ad_id": str(pid), "adgroup_id": gid, "ad_name": a.get("name") or extracted["ad_title"], "status": a.get("status"), **extracted})
                        
                        # ✨ 확장소재 완벽 수집 (이미지 및 제목)
                        for ext in list_ad_extensions(customer_id, gid):
                            if ext_id := ext.get("nccAdExtensionId"):
                                target_ad_ids.append(str(ext_id))
                                ext_info = ext.get("adExtension", {}) or ext
                                ext_type = ext.get("extensionType", "")
                                
                                ext_text = ext_info.get("promoText") or ext_info.get("addPromoText") or ext_info.get("subLinkName") or str(ext_type)
                                ext_img = ext_info.get("pcImageUrl") or ext_info.get("mobileImageUrl") or ext_info.get("imageUrl") or ""
                                
                                ad_rows.append({
                                    "customer_id": customer_id, 
                                    "ad_id": str(ext_id), 
                                    "adgroup_id": gid, 
                                    "ad_name": ext_text, 
                                    "status": ext.get("status"), 
                                    "ad_title": f"[{ext_type}] {ext_text}", 
                                    "ad_desc": ext_text, 
                                    "pc_landing_url": ext_info.get("pcLandingUrl", ""), 
                                    "mobile_landing_url": ext_info.get("mobileLandingUrl", ""), 
                                    "creative_text": str(ext_text)[:500], 
                                    "image_url": str(ext_img)[:1000]
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
        
        target_ad_ids = list(dict.fromkeys([str(x) for x in target_ad_ids if str(x).strip()]))
        if target_date == date.today():
            c_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily")
            k_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily") if not SKIP_KEYWORD_STATS else 0
            a_cnt = fetch_stats_fallback(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily") if not SKIP_AD_STATS else 0
        else:
            report_types = ["CAMPAIGN", "KEYWORD", "AD"]
            dfs = fetch_multiple_stat_reports(customer_id, report_types, target_date)
            c_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_campaign_daily", "campaign_id", parse_df_combined(dfs.get("CAMPAIGN"), "CAMPAIGN", ["캠페인id", "campaignid"], has_rank=True)) if dfs.get("CAMPAIGN") is not None else 0
            k_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_keyword_daily", "keyword_id", parse_df_combined(dfs.get("KEYWORD"), "KEYWORD", ["키워드id", "keywordid", "상품id", "productid"], has_rank=True)) if dfs.get("KEYWORD") is not None else 0
            # 우선순위: 광고/소재ID 우선 탐색, 없으면 상품ID 탐색
            ad_pk_cands = ["소재id", "adid", "nccadid", "광고id", "상품id", "productid", "itemid", "nccproductid"]
            a_cnt = merge_and_save_combined(engine, customer_id, target_date, "fact_ad_daily", "ad_id", parse_df_combined(dfs.get("AD"), "AD", ad_pk_cands, has_rank=True)) if dfs.get("AD") is not None else 0
        log(f"   📊 [ {account_name} ] 적재 완료: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")
    except Exception as e: log(f"❌ [ {account_name} ] 오류: {str(e)}")

def main():
    engine = get_engine(); ensure_tables(engine)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=""); parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--skip_dim", action="store_true"); parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    
    accounts_info = []
    if args.customer_id: 
        accounts_info = [{"id": args.customer_id, "name": "Target Account"}]
    else:
        # ✨ 엑셀 컬럼 인식 로직 수정 (backfill.py와 완벽 동기화)
        if os.path.exists("accounts.xlsx"):
            try:
                df_acc = pd.read_excel("accounts.xlsx")
                id_col, name_col = None, None
                
                for c in df_acc.columns:
                    c_clean = str(c).replace(" ", "").lower()
                    if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id", "고객 id"]: id_col = c
                    if c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]: name_col = c
                
                if id_col and name_col:
                    seen = set()
                    for _, row in df_acc.iterrows():
                        cid = str(row[id_col]).strip()
                        if cid and cid.lower() != 'nan' and cid not in seen: 
                            accounts_info.append({"id": cid, "name": str(row[name_col])})
                            seen.add(cid)
                else:
                    log(f"❌ accounts.xlsx에 올바른 컬럼이 없습니다. 현재컬럼: {list(df_acc.columns)}")
            except Exception as e: 
                log(f"❌ accounts.xlsx 파싱 오류: {e}")
                
    if not accounts_info: 
        log("⚠️ 수집할 계정이 없습니다. 프로그램을 종료합니다.")
        return
        
    # ✨ 병렬 작업(기본 10개 스레드)으로 매우 빠르게 수집 진행
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_account, engine, acc["id"], acc["name"], target_date, args.skip_dim) for acc in accounts_info]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception: pass

if __name__ == "__main__":
    main()
