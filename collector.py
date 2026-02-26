# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v11.5_SHOPPING_MASTER)
- 쇼핑검색 상품 소재가 빈칸으로 파싱되어 UI에서 증발하는 버그 100% 차단 (강제 Fallback)
- 네이버 stat-reports가 지원하지 않는 확장소재(AD_EXTENSION) 성과를 /stats 우회 조회로 완벽 복구
- 3묶음 병렬 고속 다운로드 유지
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
print("=== [VERSION: v11.5_SHOPPING_MASTER] ===", flush=True)
print("=== 쇼핑검색 투명화 차단 & 확장소재 복구 ===", flush=True)
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
                if raise_error: raise requests.HTTPError(f"403 Forbidden: {customer_id}", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt)
                continue
            data = None
            try: data = r.json()
            except: data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            time.sleep(2 + attempt)
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
            except Exception: pass
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
        except Exception as e:
            if raw_conn:
                try: raw_conn.rollback()
                except: pass
            if attempt == 2: log(f"❌ Upsert Error in {table}: {e}")
            time.sleep(3)
        finally:
            if cur:
                try: cur.close()
                except: pass
            if raw_conn:
                try: raw_conn.close()
                except: pass

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
            if attempt == 2: log(f"❌ Delete Error: {e}")
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
                except: pass
            if attempt == 2: log(f"❌ Insert Error: {e}")
            time.sleep(3)
        finally:
            if cur:
                try: cur.close()
                except: pass
            if raw_conn:
                try: raw_conn.close()
                except: pass

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

# 🚨 [CRITICAL FIX] 소재 파싱 이중 안전장치 
def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad", {})
    title = ad_inner.get("headline") or ad_inner.get("title") or ""
    desc = ad_inner.get("description") or ad_inner.get("desc") or ""
    
    # 1차 파싱: 쇼핑검색 상품 및 추가홍보문구 시도
    if "shoppingProduct" in ad_inner and isinstance(ad_inner["shoppingProduct"], dict):
        title = title or ad_inner["shoppingProduct"].get("name") or ad_inner["shoppingProduct"].get("productName") or ""
    if "addPromoText" in ad_inner:
        desc = desc or ad_inner["addPromoText"]
        
    if not title: title = ad_obj.get("name") or ad_obj.get("adName") or ""
    if not desc: desc = ad_inner.get("promoText") or ad_inner.get("extCreative") or ""
    
    # 2차 파싱: 어떤 경우에도 title이 빈 칸이 되지 않도록 강력한 Fallback 적용 (빈 칸이면 대시보드에서 투명화되어 짤림)
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

def parse_stats(r: dict, d1: date, customer_id: str, id_key: str) -> dict:
    cost = int(round(float(r.get("salesAmt", 0) or 0) * 1.1)) # VAT
    sales = int(float(r.get("convAmt", 0) or 0))
    out = {
        "dt": d1, "customer_id": str(customer_id), id_key: str(r.get("id")),
        "imp": int(r.get("impCnt", 0) or 0), "clk": int(r.get("clkCnt", 0) or 0),
        "cost": cost, "conv": float(r.get("ccnt", 0) or 0), "sales": sales,
        "roas": (sales / cost * 100) if cost > 0 else 0.0
    }
    if id_key == "keyword_id":
        out["avg_rnk"] = float(r.get("avgRnk", 0) or 0)
    return out

def fetch_multiple_stat_reports(customer_id: str, report_types: List[str], target_date: date) -> Dict[str, pd.DataFrame]:
    results = {tp: pd.DataFrame() for tp in report_types}
    for i in range(0, len(report_types), 3):
        batch = report_types[i:i+3]
        jobs = {}
        for tp in batch:
            payload = {"reportTp": tp, "statDt": target_date.strftime("%Y%m%d")}
            status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
            if status == 200 and data and "reportJobId" in data:
                jobs[tp] = data["reportJobId"]
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
                                if txt: results[tp] = pd.read_csv(io.StringIO(txt), sep='\t', header=None)
                            except Exception: pass
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
                    elif stt in ["ERROR", "NONE"]:
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

def parse_df_to_dict(df: pd.DataFrame, report_tp: str, pk_cands: List[str], is_conv: bool, has_rank: bool = False) -> dict:
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
        conv_idx = 6 if "CAMPAIGN" in report_tp else 9
        sales_idx = 7 if "CAMPAIGN" in report_tp else 10
        rank_idx = 11

    if pk_idx == -1: return {}
    
    res = {}
    for _, r in df.iterrows():
        try:
            if len(r) <= pk_idx: continue
            obj_id = str(r[pk_idx]).strip()
            if not obj_id or obj_id == '-': continue
            
            if obj_id not in res:
                res[obj_id] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
            
            if is_conv:
                if conv_idx != -1 and len(r) > conv_idx: res[obj_id]["conv"] += float(r[conv_idx] if pd.notna(r[conv_idx]) else 0)
                if sales_idx != -1 and len(r) > sales_idx: res[obj_id]["sales"] += int(float(r[sales_idx] if pd.notna(r[sales_idx]) else 0))
            else:
                imp = 0
                if imp_idx != -1 and len(r) > imp_idx:
                    imp = int(float(r[imp_idx] if pd.notna(r[imp_idx]) else 0))
                    res[obj_id]["imp"] += imp
                if clk_idx != -1 and len(r) > clk_idx: res[obj_id]["clk"] += int(float(r[clk_idx] if pd.notna(r[clk_idx]) else 0))
                if cost_idx != -1 and len(r) > cost_idx: res[obj_id]["cost"] += int(round(float(r[cost_idx] if pd.notna(r[cost_idx]) else 0) * 1.1)) # VAT
                
                if has_rank and rank_idx != -1 and len(r) > rank_idx:
                    rnk = float(r[rank_idx] if pd.notna(r[rank_idx]) else 0)
                    if rnk > 0 and imp > 0:
                        res[obj_id]["rank_sum"] += (rnk * imp)
                        res[obj_id]["rank_cnt"] += imp
        except Exception:
            pass
    return res

def merge_and_save(engine: Engine, customer_id: str, target_date: date, table_name: str, pk_name: str, stat_res: dict, conv_res: dict) -> int:
    keys = set(stat_res.keys()) | set(conv_res.keys())
    if not keys: return 0
    rows = []
    for k in keys:
        s = stat_res.get(k, {"imp":0, "clk":0, "cost":0, "rank_sum":0.0, "rank_cnt":0})
        c = conv_res.get(k, {"conv":0.0, "sales":0})
        cost = s["cost"]
        sales = c["sales"]
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (s["rank_sum"] / s["rank_cnt"]) if s["rank_cnt"] > 0 else 0.0
        
        row = {
            "dt": target_date, "customer_id": str(customer_id), pk_name: k,
            "imp": s["imp"], "clk": s["clk"], "cost": cost,
            "conv": c["conv"], "sales": sales, "roas": roas
        }
        if pk_name == "keyword_id":
            row["avg_rnk"] = round(avg_rnk, 2)
        rows.append(row)
    replace_fact_range(engine, table_name, rows, customer_id, target_date)
    return len(rows)

def process_daily_reports_v2(engine: Engine, customer_id: str, target_date: date, account_name: str):
    report_types = ["CAMPAIGN", "CAMPAIGN_CONVERSION", "KEYWORD", "KEYWORD_CONVERSION", "AD", "AD_CONVERSION"]
    dfs = fetch_multiple_stat_reports(customer_id, report_types, target_date)
    
    camp_stat = parse_df_to_dict(dfs.get("CAMPAIGN"), "CAMPAIGN", ["캠페인id", "campaignid"], False)
    camp_conv = parse_df_to_dict(dfs.get("CAMPAIGN_CONVERSION"), "CAMPAIGN_CONVERSION", ["캠페인id", "campaignid"], True)
    
    kw_stat = parse_df_to_dict(dfs.get("KEYWORD"), "KEYWORD", ["키워드id", "keywordid"], False, has_rank=True)
    kw_conv = parse_df_to_dict(dfs.get("KEYWORD_CONVERSION"), "KEYWORD_CONVERSION", ["키워드id", "keywordid"], True)
    
    # 🚨 [CRITICAL FIX] 상품ID(쇼핑검색) 헤더 누락 방지 매핑
    ad_stat = parse_df_to_dict(dfs.get("AD"), "AD", ["광고id", "소재id", "adid", "상품id", "productid", "itemid"], False)
    ad_conv = parse_df_to_dict(dfs.get("AD_CONVERSION"), "AD_CONVERSION", ["광고id", "소재id", "adid", "상품id", "productid", "itemid"], True)
    
    # 🚨 [CRITICAL FIX] 확장소재(AD_EXTENSION) 우회 성과 조회 로직
    # 확장소재는 stat-reports 다운로드가 불가능하므로, DB에서 ID를 불러와 실시간 /stats API로 직접 때려서 과거 성과를 복구합니다.
    ext_ids = []
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid AND ad_title LIKE '[확장소재]%'"), {"cid": customer_id})
            ext_ids = [str(r[0]) for r in res]
    except Exception:
        pass
        
    if ext_ids:
        ext_stats_raw = get_stats_range(customer_id, ext_ids, target_date)
        for r in ext_stats_raw:
            eid = str(r.get("id"))
            if eid not in ad_stat: ad_stat[eid] = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0, "rank_sum": 0.0, "rank_cnt": 0}
            ad_stat[eid]["imp"] += int(r.get("impCnt", 0) or 0)
            ad_stat[eid]["clk"] += int(r.get("clkCnt", 0) or 0)
            ad_stat[eid]["cost"] += int(round(float(r.get("salesAmt", 0) or 0) * 1.1)) # VAT
            
            if eid not in ad_conv: ad_conv[eid] = {"conv": 0.0, "sales": 0}
            ad_conv[eid]["conv"] += float(r.get("ccnt", 0) or 0)
            ad_conv[eid]["sales"] += int(float(r.get("convAmt", 0) or 0))
    
    c_cnt = merge_and_save(engine, customer_id, target_date, "fact_campaign_daily", "campaign_id", camp_stat, camp_conv)
    k_cnt = merge_and_save(engine, customer_id, target_date, "fact_keyword_daily", "keyword_id", kw_stat, kw_conv)
    a_cnt = merge_and_save(engine, customer_id, target_date, "fact_ad_daily", "ad_id", ad_stat, ad_conv)
    
    if (c_cnt + k_cnt + a_cnt) > 0:
        log(f"   📊 [ {account_name} ] 리포트 적재 완료 (캠페인 {c_cnt} / 키워드 {k_cnt} / 소재(쇼핑/확장포함) {a_cnt})")

def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool = False):
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
    
    if target_date == date.today():
        if target_camp_ids: replace_fact_range(engine, "fact_campaign_daily", [parse_stats(r, target_date, customer_id, "campaign_id") for r in get_stats_range(customer_id, target_camp_ids, target_date)], customer_id, target_date)
        if target_kw_ids and not SKIP_KEYWORD_STATS: replace_fact_range(engine, "fact_keyword_daily", [parse_stats(r, target_date, customer_id, "keyword_id") for r in get_stats_range(customer_id, target_kw_ids, target_date)], customer_id, target_date)
        if target_ad_ids and not SKIP_AD_STATS: replace_fact_range(engine, "fact_ad_daily", [parse_stats(r, target_date, customer_id, "ad_id") for r in get_stats_range(customer_id, target_ad_ids, target_date)], customer_id, target_date)
    else:
        process_daily_reports_v2(engine, customer_id, target_date, account_name)
    log(f"✅ 완료: {account_name}")

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
            except:
                try: df_acc = pd.read_csv("accounts.xlsx")
                except Exception as e: log(f"⚠️ accounts.xlsx 파싱 실패: {e}")
            
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
            except: pass
        if not accounts_info and CUSTOMER_ID: accounts_info = [{"id": CUSTOMER_ID, "name": "Env Account"}]

    if not accounts_info: 
        log("⚠️ 수집할 계정이 없습니다.")
        return
        
    log(f"📋 최종 수집 대상 계정: {len(accounts_info)}개 / 동시 작업: {args.workers}개")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_account, engine, acc["id"], acc["name"], target_date, args.skip_dim) for acc in accounts_info]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except Exception as e:
                if "403" not in str(e): log(f"❌ 에러: {e}")

if __name__ == "__main__":
    main()
