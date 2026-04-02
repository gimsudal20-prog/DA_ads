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

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None

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

def _flatten_ext_nodes(node):
    if isinstance(node, list):
        for item in node:
            yield from _flatten_ext_nodes(item)
    elif isinstance(node, dict):
        yield node
        for v in node.values():
            if isinstance(v, (dict, list)):
                yield from _flatten_ext_nodes(v)

def _first_non_empty_text(node, keys):
    for item in _flatten_ext_nodes(node):
        if not isinstance(item, dict):
            continue
        for k in keys:
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""

def _first_url(node, keys):
    for item in _flatten_ext_nodes(node):
        if not isinstance(item, dict):
            continue
        for k in keys:
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""

def _extract_ext_id(ext):
    if isinstance(ext, dict):
        for k in ("nccAdExtensionId", "id", "nccAdId"):
            v = ext.get(k)
            if v:
                return str(v)
        inner = ext.get("adExtension")
        if isinstance(inner, dict):
            for k in ("nccAdExtensionId", "id", "nccAdId"):
                v = inner.get(k)
                if v:
                    return str(v)
    return ""

def _normalize_ext_bucket(v: str) -> str:
    raw = str(v or "shopping").strip()
    mp = {
        "shopping": "shopping", "쇼핑검색": "shopping", "ssa": "shopping",
        "non_shopping": "non_shopping", "파워링크외": "non_shopping", "파워링크 외 검색광고": "non_shopping",
        "all": "all", "전체": "all",
    }
    return mp.get(raw, raw if raw in {"shopping","non_shopping","all"} else "shopping")

def parse_ext_name(ext: dict) -> str:
    ext_info = ext.get("adExtension", {}) or ext
    ext_type = ext.get("extensionType") or ext.get("type") or "쇼핑확장"
    
    cands = ["promoText", "addPromoText", "subLinkName", "pcText", "mobileText", "description", "title", "text"]
    text_val = ""
    for c in cands:
        if ext_info.get(c):
            text_val = str(ext_info[c]).strip()
            break
            
    if not text_val:
        vals = [str(v) for k, v in ext_info.items() if isinstance(v, str) and not v.startswith("http") and k not in ("extensionType", "status", "nccAdExtensionId", "ownerId", "customer_id", "type")]
        if vals:
            text_val = " / ".join(vals)
        else:
            text_val = str(ext_info)[:150]
            
    return f"[확장소재] {ext_type} | {text_val}"

def process_account(engine, customer_id: str, target_date: date, ext_bucket: str = "shopping"):
    bucket = _normalize_ext_bucket(ext_bucket)
    label = {"shopping":"쇼핑검색", "non_shopping":"파워링크외", "all":"전체"}.get(bucket, bucket)
    log(f"--- [ {customer_id} ] {label} 확장소재 수집 시작 ({target_date}) ---")
    
    camps = request_json("GET", "/ncc/campaigns", customer_id)
    if not camps:
        return
    shop_camps = [c for c in camps if str(c.get("campaignTp") or "").upper() == "SHOPPING"]
    non_shop_camps = [c for c in camps if str(c.get("campaignTp") or "").upper() != "SHOPPING"]
    if bucket == "shopping":
        target_camps = shop_camps
    elif bucket == "non_shopping":
        target_camps = non_shop_camps
    else:
        target_camps = list(camps)
    log(f"   ▶ 대상 캠페인 {len(target_camps)}개 | 쇼핑검색 {len(shop_camps)}개 | 파워링크외 {len(non_shop_camps)}개")
    
    camp_rows, ag_rows, ad_rows = [], [], []
    target_ad_ids = []
    
    for c in target_camps:
        cid = c.get("nccCampaignId")
        camp_rows.append({
            "customer_id": str(customer_id), "campaign_id": str(cid),
            "campaign_name": c.get("name"), "campaign_tp": c.get("campaignTp"), "status": c.get("status")
        })
        
        camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": cid}) or []
        if camp_exts:
            ag_rows.append({
                "customer_id": str(customer_id), "adgroup_id": f"CAMP_{cid}", "campaign_id": str(cid),
                "adgroup_name": "[캠페인 공통 소재]", "status": "ELIGIBLE"
            })
            for ext in camp_exts:
                ext_id = _extract_ext_id(ext)
                if ext_id:
                    target_ad_ids.append(ext_id)
                    display_name = parse_ext_name(ext)
                    pc_url = _first_url(ext, ["pcLandingUrl", "landingUrl", "linkUrl"])
                    mobile_url = _first_url(ext, ["mobileLandingUrl", "mobileLinkUrl", "landingUrl", "linkUrl"])
                    
                    ad_rows.append({
                        "customer_id": str(customer_id), "ad_id": str(ext_id), "adgroup_id": f"CAMP_{cid}",
                        "ad_name": display_name, "status": ext.get("status") if isinstance(ext, dict) else "", "ad_title": display_name, 
                        "ad_desc": display_name, "pc_landing_url": pc_url, 
                        "mobile_landing_url": mobile_url,
                        "creative_text": display_name[:500]
                    })
        
        groups = request_json("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": cid}) or []
        for g in groups:
            gid = g.get("nccAdgroupId")
            ag_rows.append({
                "customer_id": str(customer_id), "adgroup_id": str(gid), "campaign_id": str(cid),
                "adgroup_name": g.get("name"), "status": g.get("status")
            })
            
            extensions = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": gid}) or []
            for ext in extensions:
                ext_id = _extract_ext_id(ext)
                if ext_id:
                    target_ad_ids.append(ext_id)
                    display_name = parse_ext_name(ext)
                    pc_url = _first_url(ext, ["pcLandingUrl", "landingUrl", "linkUrl"])
                    mobile_url = _first_url(ext, ["mobileLandingUrl", "mobileLinkUrl", "landingUrl", "linkUrl"])
                    
                    ad_rows.append({
                        "customer_id": str(customer_id), "ad_id": str(ext_id), "adgroup_id": str(gid),
                        "ad_name": display_name, "status": ext.get("status") if isinstance(ext, dict) else "", "ad_title": display_name, 
                        "ad_desc": display_name, "pc_landing_url": pc_url, 
                        "mobile_landing_url": mobile_url,
                        "creative_text": display_name[:500]
                    })

    target_ad_ids = sorted({str(x) for x in target_ad_ids if str(x)})
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
            obj_id = str(r.get("id") or r.get("nccAdExtensionId") or r.get("nccAdId") or "").strip()
            if not obj_id:
                continue
            cost = int(float(r.get("salesAmt", 0) or 0))
            sales = int(float(r.get("convAmt", 0) or 0))
            fact_rows.append({
                "dt": target_date, "customer_id": str(customer_id), "ad_id": obj_id,
                "imp": int(r.get("impCnt", 0) or 0), "clk": int(r.get("clkCnt", 0) or 0), 
                "cost": cost, "conv": float(r.get("ccnt", 0) or 0), "sales": sales,
                "roas": (sales / cost * 100.0) if cost > 0 else 0.0
            })
            
        if fact_rows:
            upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
            log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
        else:
            log("   ⚠️ 조회된 날짜에 노출/클릭이 발생한 확장소재가 없습니다.")

def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--account_name", type=str, default="")
    parser.add_argument("--account_names", type=str, default="")
    parser.add_argument("--ext_bucket", type=str, default="shopping")
    args = parser.parse_args()
    
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    
    print("\n" + "="*50, flush=True)
    bucket = _normalize_ext_bucket(args.ext_bucket)
    bucket_label = {"shopping":"쇼핑검색", "non_shopping":"파워링크외", "all":"전체"}.get(bucket, bucket)
    print(f"🧩 확장소재 수집기 [날짜: {target_date} | 구분: {bucket_label}]", flush=True)
    print("="*50 + "\n", flush=True)

    accounts = []
    if load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=["sa"])
            accounts = [str(r["id"]).strip() for r in rows if str(r.get("id", "")).strip()]
        except Exception as e:
            log(f"⚠️ account_master 로드 실패, dim_account_meta 로 폴백합니다: {e}")

    if not accounts:
        try:
            with engine.connect() as conn:
                accounts = [str(r[0]) for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta WHERE COALESCE(naver_media_type, 'sa') <> 'gfa'"))]
        except Exception:
            pass

    if not accounts:
        cid = os.getenv("CUSTOMER_ID")
        if cid:
            accounts = [cid]

    target_name_tokens = []
    if getattr(args, "account_name", ""):
        target_name_tokens.append(str(args.account_name).strip())
    if getattr(args, "account_names", ""):
        target_name_tokens.extend([x.strip() for x in str(args.account_names).split(",") if x.strip()])

    if target_name_tokens and load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=["sa"])
            exact_set = {x for x in target_name_tokens}
            filtered = [r for r in rows if r["name"] in exact_set]
            if not filtered:
                lowered = [x.lower() for x in target_name_tokens]
                filtered = [r for r in rows if any(tok in r["name"].lower() for tok in lowered)]
            if filtered:
                accounts = [str(r["id"]).strip() for r in filtered]
                log(f"🎯 업체명 필터 적용: {', '.join(target_name_tokens)} -> {len(accounts)}개")
        except Exception:
            pass

    for acc in accounts:
        process_account(engine, acc, target_date)

if __name__ == "__main__":
    main()

