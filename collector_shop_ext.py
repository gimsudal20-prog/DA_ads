# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 확장소재 수집기

지원 버킷
- shopping: 쇼핑검색 캠페인 확장소재만
- non_shopping: 파워링크, 플레이스, 브랜드검색 등 쇼핑검색 외 전체
- all: 전체 확장소재
"""

import os
import time
import json
import hmac
import base64
import hashlib
import argparse
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


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


def request_json(method: str, path: str, customer_id: str, params: dict | None = None):
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
            if r.status_code == 200:
                return r.json()
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
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last")
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict = (
        f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    )
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
        if raw_conn:
            raw_conn.rollback()
    finally:
        if cur:
            cur.close()
        if raw_conn:
            raw_conn.close()


def clear_fact_scope(engine, customer_id: str, target_date: date, ad_ids: list[str]):
    ad_ids = sorted({str(x).strip() for x in (ad_ids or []) if str(x).strip()})
    if not ad_ids:
        return True
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM fact_ad_daily WHERE customer_id=:cid AND dt=:dt AND ad_id = ANY(:ids)"),
                    {"cid": str(customer_id), "dt": target_date, "ids": ad_ids},
                )
            return True
        except Exception as e:
            log(f"⚠️ fact_ad_daily 범위 삭제 실패({attempt+1}/3): {e}")
            time.sleep(2 + attempt)
    return False


def parse_ext_name(ext: dict) -> str:
    ext_info = ext.get("adExtension", {}) or ext
    ext_type = ext.get("extensionType") or ext.get("type") or "확장소재"
    cands = ["promoText", "addPromoText", "subLinkName", "pcText", "mobileText", "description", "title", "text"]
    text_val = ""
    for c in cands:
        if ext_info.get(c):
            text_val = str(ext_info[c]).strip()
            break
    if not text_val:
        vals = [
            str(v)
            for k, v in ext_info.items()
            if isinstance(v, str)
            and not v.startswith("http")
            and k not in ("extensionType", "status", "nccAdExtensionId", "ownerId", "customer_id", "type")
        ]
        text_val = " / ".join(vals) if vals else str(ext_info)[:150]
    return f"[확장소재] {ext_type} | {text_val}"


def campaign_bucket(campaign_tp: str | None) -> str:
    return "shopping" if str(campaign_tp or "").upper() == "SHOPPING" else "non_shopping"


def bucket_label(ext_bucket: str) -> str:
    return {"shopping": "쇼핑검색", "non_shopping": "파워링크외", "all": "전체"}.get(ext_bucket, ext_bucket)


def match_bucket(campaign_tp: str | None, ext_bucket: str) -> bool:
    bucket = campaign_bucket(campaign_tp)
    return ext_bucket == "all" or bucket == ext_bucket


def process_account(engine, customer_id: str, target_date: date, ext_bucket: str = "shopping"):
    log(f"--- [ {customer_id} ] {bucket_label(ext_bucket)} 확장소재 수집 시작 ({target_date}) ---")
    camps = request_json("GET", "/ncc/campaigns", customer_id)
    if not camps:
        return

    selected_camps = [c for c in camps if match_bucket(c.get("campaignTp"), ext_bucket)]
    shopping_cnt = sum(1 for c in selected_camps if campaign_bucket(c.get("campaignTp")) == "shopping")
    non_shopping_cnt = len(selected_camps) - shopping_cnt
    log(f"   ▶ 대상 캠페인 {len(selected_camps)}개 | 쇼핑검색 {shopping_cnt}개 | 파워링크외 {non_shopping_cnt}개")

    camp_rows, ag_rows, ad_rows = [], [], []
    target_ad_ids = []

    for c in selected_camps:
        cid = c.get("nccCampaignId")
        camp_rows.append(
            {
                "customer_id": str(customer_id),
                "campaign_id": str(cid),
                "campaign_name": c.get("name"),
                "campaign_tp": c.get("campaignTp"),
                "status": c.get("status"),
            }
        )

        camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": cid}) or []
        if camp_exts:
            ag_rows.append(
                {
                    "customer_id": str(customer_id),
                    "adgroup_id": f"CAMP_{cid}",
                    "campaign_id": str(cid),
                    "adgroup_name": "[캠페인 공통 소재]",
                    "status": "ELIGIBLE",
                }
            )
            for ext in camp_exts:
                ext_id = ext.get("nccAdExtensionId")
                if ext_id:
                    target_ad_ids.append(ext_id)
                    ext_info = ext.get("adExtension", {}) or ext
                    display_name = parse_ext_name(ext)
                    ad_rows.append(
                        {
                            "customer_id": str(customer_id),
                            "ad_id": str(ext_id),
                            "adgroup_id": f"CAMP_{cid}",
                            "ad_name": display_name,
                            "status": ext.get("status"),
                            "ad_title": display_name,
                            "ad_desc": display_name,
                            "pc_landing_url": ext_info.get("pcLandingUrl", ""),
                            "mobile_landing_url": ext_info.get("mobileLandingUrl", ""),
                            "creative_text": display_name[:500],
                        }
                    )

        groups = request_json("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": cid}) or []
        for g in groups:
            gid = g.get("nccAdgroupId")
            ag_rows.append(
                {
                    "customer_id": str(customer_id),
                    "adgroup_id": str(gid),
                    "campaign_id": str(cid),
                    "adgroup_name": g.get("name"),
                    "status": g.get("status"),
                }
            )

            extensions = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": gid}) or []
            for ext in extensions:
                ext_id = ext.get("nccAdExtensionId")
                if ext_id:
                    target_ad_ids.append(ext_id)
                    ext_info = ext.get("adExtension", {}) or ext
                    display_name = parse_ext_name(ext)
                    ad_rows.append(
                        {
                            "customer_id": str(customer_id),
                            "ad_id": str(ext_id),
                            "adgroup_id": str(gid),
                            "ad_name": display_name,
                            "status": ext.get("status"),
                            "ad_title": display_name,
                            "ad_desc": display_name,
                            "pc_landing_url": ext_info.get("pcLandingUrl", ""),
                            "mobile_landing_url": ext_info.get("mobileLandingUrl", ""),
                            "creative_text": display_name[:500],
                        }
                    )

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    log(f"   ▶ 캠페인({len(camp_rows)}), 광고그룹({len(ag_rows)}), 확장소재({len(ad_rows)}) 매핑 완료!")

    target_ad_ids = sorted({str(x).strip() for x in target_ad_ids if str(x).strip()})
    if not target_ad_ids:
        log("   ⚠️ 수집 대상 확장소재가 없습니다.")
        return

    log(f"   ▶ 확장소재 {len(target_ad_ids)}개 실시간 통계 조회 중...")
    d_str = target_date.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "cpc", "ccnt", "convAmt"], separators=(",", ":"))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))

    raw_stats = []
    for i in range(0, len(target_ad_ids), 50):
        chunk = target_ad_ids[i : i + 50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        res = request_json("GET", "/stats", customer_id, params=params)
        if res and "data" in res:
            raw_stats.extend(res["data"])

    fact_rows = []
    corrected_cost_rows = 0
    corrected_clk_rows = 0
    suspicious_equal_rows = 0
    for r in raw_stats:
        ad_id = str(r.get("id") or "").strip()
        if not ad_id:
            continue

        imp = int(float(r.get("impCnt", 0) or 0))
        clk = int(float(r.get("clkCnt", 0) or 0))
        cost = int(round(float(r.get("salesAmt", 0) or 0)))
        sales = int(round(float(r.get("convAmt", 0) or 0)))
        conv = float(r.get("ccnt", 0) or 0)
        cpc = float(r.get("cpc", 0) or 0)

        # 확장소재 /stats 응답은 매체별로 clk/cost 값이 불안정한 케이스가 있어
        # cpc를 이용해 클릭/비용을 상호 보정한다.
        if clk == 0 and cost > 0 and cpc > 0:
            est_clk = int(round(cost / cpc))
            if est_clk > 0:
                clk = est_clk
                corrected_clk_rows += 1

        if cost == 0 and clk > 0 and cpc > 0:
            est_cost = int(round(clk * cpc))
            if est_cost > 0:
                cost = est_cost
                corrected_cost_rows += 1

        # 클릭수가 광고비 컬럼으로 들어온 듯한 케이스 보정
        if clk > 0 and cost == clk and cpc > 1:
            est_cost = int(round(clk * cpc))
            if est_cost > cost:
                cost = est_cost
                corrected_cost_rows += 1
                suspicious_equal_rows += 1

        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            continue

        fact_rows.append(
            {
                "dt": target_date,
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "imp": imp,
                "clk": clk,
                "cost": cost,
                "conv": conv,
                "sales": sales,
                "roas": (sales / cost * 100.0) if cost > 0 else 0.0,
            }
        )

    if fact_rows:
        if clear_fact_scope(engine, customer_id, target_date, target_ad_ids):
            upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
            log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
            if corrected_clk_rows or corrected_cost_rows or suspicious_equal_rows:
                log(f"   ↪ 보정 적용 | 클릭 역산 {corrected_clk_rows}건 | 비용 역산 {corrected_cost_rows}건 | 클릭=비용 의심 보정 {suspicious_equal_rows}건")
        else:
            log("   ❌ fact_ad_daily 범위 삭제 실패로 적재를 중단했습니다. 중복 방지를 위해 확인 후 재실행하세요.")
    else:
        log("   ⚠️ 조회된 날짜에 노출/클릭이 발생한 확장소재가 없습니다.")


def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--account_name", type=str, default="")
    parser.add_argument("--account_names", type=str, default="")
    parser.add_argument("--ext_bucket", type=str, default="shopping", choices=["shopping", "non_shopping", "all"])
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)

    print("\n" + "=" * 50, flush=True)
    print(f"🧩 확장소재 수집기 [날짜: {target_date}]", flush=True)
    print("=" * 50 + "\n", flush=True)

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
                accounts = [
                    str(r[0])
                    for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta WHERE COALESCE(naver_media_type, 'sa') <> 'gfa'"))
                ]
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

    log(f"🧩 확장소재 수집 버킷: {bucket_label(args.ext_bucket)} ({args.ext_bucket})")
    for acc in accounts:
        process_account(engine, acc, target_date, args.ext_bucket)


if __name__ == "__main__":
    main()
