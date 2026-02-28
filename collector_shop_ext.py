# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 수집기 (쇼핑검색 확장소재 전용 테스트용)
"""

import os
import time
import json
import hmac
import base64
import hashlib
import argparse
import sys
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2.extras
from sqlalchemy.pool import NullPool

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def now_millis() -> str: return str(int(time.time() * 1000))

def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")

def request_json(method: str, path: str, customer_id: str, params: dict | None = None) -> tuple:
    url = BASE_URL + path
    ts = now_millis()
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sign_path_only(method.upper(), path, ts, API_SECRET),
    }
    for attempt in range(4):
        try:
            r = requests.request(method, url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 200: return r.json()
            if r.status_code in [429, 500, 502, 503, 504]:
                time.sleep(2 + attempt)
                continue
            return None
        except Exception:
            time.sleep(2 + attempt)
    return None

def get_engine():
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    return create_engine(db_url, poolclass=NullPool, future=True)

def upsert_many(engine, table: str, rows: list, pk_cols: list):
    if not rows: return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last')
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    
    conflict = f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols]) if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict}'
    
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn, cur = None, None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    except Exception as e:
        log(f"⚠️ {table} 저장 중 오류: {e}")
        if raw_conn: raw_conn.rollback()
    finally:
        if cur: cur.close()
        if raw_conn: raw_conn.close()

def process_account(engine, customer_id: str, target_date: date):
    log(f"--- [ {customer_id} ] 쇼핑검색 확장소재 전용 수집 시작 ({target_date}) ---")
    
    camps = request_json("GET", "/ncc/campaigns", customer_id)
    if not camps: return
    shop_camps = [c for c in camps if c.get("campaignTp") == "SHOPPING"]
    log(f"   ▶ 쇼핑검색 캠페인 {len(shop_camps)}개 발견")
    
    camp_rows, ag_rows, ad_rows = [], [], []
    target_ad_ids = []
    
    for c in shop_camps:
        cid = c.get("nccCampaignId")
        camp_rows.append({
            "customer_id": str(customer_id), "campaign_id": str(cid),
            "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")
        })
        
        # ✨ 1. 캠페인 레벨에 달린 확장소재 수집 (ownerId 사용)
        camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": cid}) or []
        if camp_exts:
            ag_rows.append({
                "customer_id": str(customer_id), "adgroup_id": f"CAMP_{cid}", "campaign_id": str(cid),
                "adgroup_name": "[캠페인 공통 소재]", "status": "ELIGIBLE"
            })
            for ext in camp_exts:
                ext_id = ext.get("nccAdExtensionId")
                if ext_id:
                    target_ad_ids.append(ext_id)
                    ext_info = ext.get("adExtension", {}) or ext
                    ext_type = ext.get("extensionType", "")
                    ext_text = ext_info.get("promoText") or ext_info.get("addPromoText") or ext_info.get("subLinkName") or ext_info.get("pcText") or str(ext_type)
                    ext_title = f"[확장소재] {ext_type}"
                    
                    ad_rows.append({
                        "customer_id": str(customer_id), "ad_id": str(ext_id), "adgroup_id": f"CAMP_{cid}",
                        "ad_name": ext_text, "status": ext.get("status"), "ad_title": ext_title, 
                        "ad_desc": ext_text, "pc_landing_url": ext_info.get("pcLandingUrl", ""), 
                        "mobile_landing_url": ext_info.get("mobileLandingUrl", ""),
                        "creative_text": f"{ext_title} | {ext_text}"[:500]
                    })
        
        # ✨ 2. 광고그룹 레벨에 달린 확장소재 수집 (ownerId 사용)
        groups = request_json("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": cid}) or []
        for g in groups:
            gid = g.get("nccAdgroupId")
            ag_rows.append({
                "customer_id": str(customer_id), "adgroup_id": str(gid), "campaign_id": str(cid),
                "adgroup_name": g.get("name"), "status": g.get("status")
            })
            
            extensions = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": gid}) or []
            for ext in extensions:
                ext_id = ext.get("nccAdExtensionId")
                if ext_id:
                    target_ad_ids.append(ext_id)
                    ext_info = ext.get("adExtension", {}) or ext
                    ext_type = ext.get("extensionType", "")
                    ext_text = ext_info.get("promoText") or ext_info.get("addPromoText") or ext_info.get("subLinkName") or ext_info.get("pcText") or str(ext_type)
                    ext_title = f"[확장소재] {ext_type}"
                    
                    ad_rows.append({
                        "customer_id": str(customer_id), "ad_id": str(ext_id), "adgroup_id": str(gid),
                        "ad_name": ext_text, "status": ext.get("status"), "ad_title": ext_title, 
                        "ad_desc": ext_text, "pc_landing_url": ext_info.get("pcLandingUrl", ""), 
                        "mobile_landing_url": ext_info.get("mobileLandingUrl", ""),
                        "creative_text": f"{ext_title} | {ext_text}"[:500]
                    })

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    log(f"   ▶ 캠페인({len(camp_rows)}), 광고그룹({len(ag_rows)}), 확장소재({len(ad_rows)}) 매핑 완료!")

    if target_ad_ids:
        log(f"   ▶ 확장소재 {len(target_ad_ids)}개 실시간 통계 조회 중...")
        d_str = target_date.strftime("%Y-%m-%d")
        fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(',', ':'))
        time_range = json.dumps({"since": d_str, "until": d_str}, separators=(',', ':'))
        
        raw_stats = []
        for i in range(0, len(target_ad_ids), 50):
            chunk = target_ad_ids[i:i+50]
            params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
            res = request_json("GET", "/stats", customer_id, params=params)
            if res and "data" in res: raw_stats.extend(res["data"])

        fact_rows = []
        for r in raw_stats:
            cost = int(round(float(r.get("salesAmt", 0) or 0) * 1.1))
            sales = int(float(r.get("convAmt", 0) or 0))
            fact_rows.append({
                "dt": target_date, "customer_id": str(customer_id), "ad_id": str(r.get("id")),
                "imp": int(r.get("impCnt", 0) or 0), "clk": int(r.get("clkCnt", 0) or 0), 
                "cost": cost, "conv": float(r.get("ccnt", 0) or 0), "sales": sales,
                "roas": (sales / cost * 100.0) if cost > 0 else 0.0
            })
            
        if fact_rows:
            df_fact = pd.DataFrame(fact_rows)
            try:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM fact_ad_daily WHERE customer_id=:cid AND dt=:dt AND ad_id IN :ids"), 
                                 {"cid": str(customer_id), "dt": target_date, "ids": tuple(target_ad_ids)})
            except Exception: pass
            
            tuples_f = list(df_fact.itertuples(index=False, name=None))
            col_names = '", "'.join(df_fact.columns)
            sql_f = f'INSERT INTO fact_ad_daily ("{col_names}") VALUES %s'
            
            raw_conn, cur = None, None
            try:
                raw_conn = engine.raw_connection()
                cur = raw_conn.cursor()
                psycopg2.extras.execute_values(cur, sql_f, tuples_f, page_size=2000)
                raw_conn.commit()
                log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
            except Exception as e: log(f"통계 저장 실패: {e}")
            finally:
                if cur: cur.close()
                if raw_conn: raw_conn.close()
        else:
            log("   ⚠️ 조회된 날짜에 노출/클릭이 발생한 확장소재가 없습니다.")

def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    args = parser.parse_args()
    
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    
    print("\n" + "="*50, flush=True)
    print(f"🛍️ 쇼핑검색 확장소재 전용 테스트 수집기 [날짜: {target_date}]", flush=True)
    print("="*50 + "\n", flush=True)

    accounts = []
    try:
        with engine.connect() as conn:
            accounts = [str(r[0]) for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta"))]
    except Exception: pass
    
    if not accounts:
        cid = os.getenv("CUSTOMER_ID")
        if cid: accounts = [cid]

    for acc in accounts:
        process_account(engine, acc, target_date)

if __name__ == "__main__":
    main()
