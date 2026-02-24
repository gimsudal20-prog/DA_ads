# -*- coding: utf-8 -*-
"""
collector.py - 네이버 검색광고 수집기 (v9.31 - 광고비 부가세(VAT) 포함 패치)
- 원인: 네이버 대용량 리포트(TSV)로 내려받은 데이터의 '총비용'은 API 스펙상 [부가세 제외] 금액임
- 해결: TSV에서 읽은 비용(Index 11)에 1.1을 곱하여 부가세(VAT)를 포함시킨 뒤 적재하도록 수정 (네이버 화면과 100% 일치)
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
import uuid
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
SKIP_KEYWORD_STATS = False  
SKIP_AD_STATS = False       

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)

print("="*50, flush=True)
print("=== [VERSION: v9.31_VAT_INCLUDED] ===", flush=True)
print("=== 광고비 부가세(VAT) 10% 포함 완벽 일치 ===", flush=True)
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
    max_retries = 3
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error: raise requests.HTTPError(f"403 Forbidden: {customer_id}", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2)
                continue
            data = None
            try: data = r.json()
            except: data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e): raise e
            time.sleep(2)
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
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
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

def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    if not ids: return []
    out, d_str = [], d1.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(',', ':'))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data: out.extend(data["data"])
    return out

def parse_stats(r: dict, d1: date, customer_id: str, id_key: str) -> dict:
    cost = int(round(float(r.get("salesAmt", 0) or 0)))
    sales = int(float(r.get("convAmt", 0) or 0))
    return {
        "dt": d1, "customer_id": str(customer_id), id_key: str(r.get("id")),
        "imp": int(r.get("impCnt", 0) or 0), "clk": int(r.get("clkCnt", 0) or 0),
        "cost": cost, "conv": float(r.get("ccnt", 0) or 0), "sales": sales,
        "roas": (sales / cost * 100) if cost > 0 else 0.0
    }

def fetch_stat_report(customer_id: str, report_tp: str, target_date: date) -> pd.DataFrame:
    payload = {"reportTp": report_tp, "statDt": target_date.strftime("%Y%m%d")}
    status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
    
    if status != 200 or not data or "reportJobId" not in data: 
        return pd.DataFrame()
        
    job_id = data["reportJobId"]
    download_url = None
    
    try:
        for _ in range(30):
            time.sleep(2)
            s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
            if s_status == 200 and s_data:
                if s_data.get("status") == "BUILT":
                    download_url = s_data.get("downloadUrl")
                    break
                elif s_data.get("status") in ["ERROR", "NONE"]: 
                    return pd.DataFrame()
                    
        if not download_url: return pd.DataFrame()
        
        r = requests.get(download_url, headers=make_headers("GET", "/report-download", customer_id), timeout=60)
        r.raise_for_status()
        r.encoding = 'utf-8'
        return pd.read_csv(io.StringIO(r.text.strip()), sep='\t', header=None) if r.text.strip() else pd.DataFrame()
    except: 
        return pd.DataFrame()
    finally:
        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

def process_merged_reports(engine: Engine, ad_df: pd.DataFrame, conv_df: pd.DataFrame, customer_id: str, target_date: date, account_name: str):
    ad_records = {'camp': {}, 'kw': {}, 'ad': {}}
    
    # 1. 일반 리포트 (노출, 클릭, 광고비) 파싱
    if ad_df is not None and not ad_df.empty:
        if not str(ad_df.iloc[0, 0]).isdigit(): ad_df = ad_df.iloc[1:].reset_index(drop=True)
        if len(ad_df.columns) >= 12:
            ad_df[9] = pd.to_numeric(ad_df[9], errors='coerce').fillna(0)
            ad_df[10] = pd.to_numeric(ad_df[10], errors='coerce').fillna(0)
            ad_df[11] = pd.to_numeric(ad_df[11], errors='coerce').fillna(0)
            for group_col, cat in [(2, 'camp'), (4, 'kw'), (5, 'ad')]:
                valid = ad_df[ad_df[group_col].notna() & (ad_df[group_col].astype(str).str.strip() != '') & (ad_df[group_col].astype(str).str.strip() != '-')]
                if not valid.empty:
                    g = valid.groupby(group_col).agg({9:'sum', 10:'sum', 11:'sum'}).reset_index()
                    for _, r in g.iterrows():
                        # 🌟 V9.31 핵심: TSV의 순수 비용(r[11])에 1.1을 곱해 부가세를 포함시킴! (네이버 화면과 100% 일치)
                        cost_vat = int(round(float(r[11]) * 1.1))
                        ad_records[cat][str(r[group_col])] = {"imp": int(float(r[9])), "clk": int(float(r[10])), "cost": cost_vat}

    # 2. 전환 리포트 (전환수, 전환매출액) 파싱
    conv_records = {'camp': {}, 'kw': {}, 'ad': {}}
    if conv_df is not None and not conv_df.empty:
        if not str(conv_df.iloc[0, 0]).isdigit(): conv_df = conv_df.iloc[1:].reset_index(drop=True)
        if len(conv_df.columns) >= 13:
            conv_df[11] = pd.to_numeric(conv_df[11], errors='coerce').fillna(0)
            conv_df[12] = pd.to_numeric(conv_df[12], errors='coerce').fillna(0)
            for group_col, cat in [(2, 'camp'), (4, 'kw'), (5, 'ad')]:
                valid = conv_df[conv_df[group_col].notna() & (conv_df[group_col].astype(str).str.strip() != '') & (conv_df[group_col].astype(str).str.strip() != '-')]
                if not valid.empty:
                    g = valid.groupby(group_col).agg({11:'sum', 12:'sum'}).reset_index()
                    for _, r in g.iterrows():
                        conv_records[cat][str(r[group_col])] = {"conv": float(r[11]), "sales": int(float(r[12]))}

    # 3. 데이터 100% 융합 후 저장
    def _save(cat, table_name, id_col_name):
        keys = set(ad_records[cat].keys()) | set(conv_records[cat].keys())
        if not keys: return 0
        rows = []
        for k in keys:
            ad = ad_records[cat].get(k, {"imp":0, "clk":0, "cost":0})
            cv = conv_records[cat].get(k, {"conv":0.0, "sales":0})
            
            cost = ad["cost"]
            sales = cv["sales"]
            # 🌟 수정된 cost(VAT 포함)를 바탕으로 ROAS 재계산
            roas = (sales / cost * 100) if cost > 0 else 0.0
            
            rows.append({
                "dt": target_date, "customer_id": str(customer_id), id_col_name: k, 
                "imp": ad["imp"], "clk": ad["clk"], "cost": cost, 
                "conv": cv["conv"], "sales": sales, "roas": roas
            })
        replace_fact_range(engine, table_name, rows, customer_id, target_date)
        return len(rows)

    c_cnt = _save('camp', "fact_campaign_daily", "campaign_id")
    k_cnt = _save('kw', "fact_keyword_daily", "keyword_id")
    a_cnt = _save('ad', "fact_ad_daily", "ad_id")
    
    total = c_cnt + k_cnt + a_cnt
    if total > 0: log(f"   📊 [ {account_name} ] 데이터 적재 완료 (캠페인 {c_cnt} / 키워드 {k_cnt} / 소재 {a_cnt})")
    else: log(f"   ⚠️ [ {account_name} ] 리포트는 받았으나 광고비와 전환이 모두 0원입니다.")

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
                        if aid := a.get("nccAdId"): target_ad_ids.append(aid); ad_rows.append({"customer_id": customer_id, "ad_id": aid, "adgroup_id": gid, "ad_name": a.get("name") or extract_ad_creative_fields(a)["ad_title"], "status": a.get("status"), **extract_ad_creative_fields(a)})
        upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
        upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
        if kw_rows: upsert_many(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
        if ad_rows: upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    
    if target_date == date.today():
        if target_camp_ids: replace_fact_range(engine, "fact_campaign_daily", [parse_stats(r, target_date, customer_id, "campaign_id") for r in get_stats_range(customer_id, target_camp_ids, target_date)], customer_id, target_date)
        if target_kw_ids and not SKIP_KEYWORD_STATS: replace_fact_range(engine, "fact_keyword_daily", [parse_stats(r, target_date, customer_id, "keyword_id") for r in get_stats_range(customer_id, target_kw_ids, target_date)], customer_id, target_date)
        if target_ad_ids and not SKIP_AD_STATS: replace_fact_range(engine, "fact_ad_daily", [parse_stats(r, target_date, customer_id, "ad_id") for r in get_stats_range(customer_id, target_ad_ids, target_date)], customer_id, target_date)
    else:
        ad_df = fetch_stat_report(customer_id, "AD", target_date)
        conv_df = fetch_stat_report(customer_id, "AD_CONVERSION", target_date)
        process_merged_reports(engine, ad_df, conv_df, customer_id, target_date, account_name)
    log(f"✅ 완료: {account_name}")

def main():
    engine = get_engine()
    ensure_tables(engine)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--skip_dim", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
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
                    for _, row in df_acc.iterrows():
                        cid = str(row[id_col]).strip()
                        if cid and cid.lower() != 'nan': accounts_info.append({"id": cid, "name": str(row[name_col])})
                    log(f"🟢 accounts.xlsx 엑셀 파일에서 {len(accounts_info)}개 업체를 완벽하게 불러왔습니다.")

        if not accounts_info:
            try:
                with engine.connect() as conn:
                    accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT customer_id, account_name FROM accounts WHERE customer_id IS NOT NULL"))]
            except:
                try:
                    with engine.connect() as conn:
                        accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT id, name FROM accounts WHERE id IS NOT NULL"))]
                except:
                    try:
                        with engine.connect() as conn:
                            accounts_info = [{"id": str(row[0]).strip(), "name": str(row[1])} for row in conn.execute(text("SELECT customer_id, account_name FROM dim_account_meta WHERE customer_id IS NOT NULL"))]
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
