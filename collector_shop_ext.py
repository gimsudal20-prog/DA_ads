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


def _to_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_int(v, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(round(float(v)))
    except Exception:
        return default


def _preview_stat_row(r: dict) -> str:
    keep = [
        "id", "impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "ccnt", "convAmt",
        "cost", "amt", "spend", "chargeAmt", "viewCnt"
    ]
    slim = {k: r.get(k) for k in keep if k in r}
    return json.dumps(slim, ensure_ascii=False, separators=(",", ":"))


def _normalize_ext_metrics(r: dict, bucket: str):
    raw_imp = _to_int(r.get("impCnt", 0))
    raw_clk = _to_int(r.get("clkCnt", 0))
    raw_cost = _to_int(r.get("salesAmt", 0))
    raw_conv = _to_float(r.get("ccnt", 0))
    raw_sales = _to_int(r.get("convAmt", 0))
    ctr = _to_float(r.get("ctr", 0.0))
    cpc = _to_float(r.get("cpc", 0.0))

    imp = raw_imp
    clk = raw_clk
    cost = raw_cost
    conv = raw_conv
    sales = raw_sales

    debug_flags = []

    if clk <= 0 and cost > 0 and cpc > 0:
        est_clk = max(0, int(round(cost / cpc)))
        if est_clk > 0:
            clk = est_clk
            debug_flags.append("clk_from_cost_cpc")

    if clk <= 0 and imp > 0 and ctr > 0:
        est_clk = max(0, int(round(imp * ctr / 100.0)))
        if est_clk > 0:
            clk = est_clk
            debug_flags.append("clk_from_ctr")

    if cost <= 0 and clk > 0 and cpc > 0:
        est_cost = max(0, int(round(clk * cpc)))
        if est_cost > 0:
            cost = est_cost
            debug_flags.append("cost_from_clk_cpc")

    # 파워링크/플레이스 등에서는 salesAmt에 클릭수가 잘못 들어오는 케이스 보정
    if bucket == "non_shopping" and clk > 0 and cpc > 0:
        est_cost = max(0, int(round(clk * cpc)))
        if cost == clk or cost < max(1, int(round(est_cost * 0.35))):
            if est_cost > 0:
                cost = est_cost
                debug_flags.append("non_shopping_cost_recalc")

    # 쇼핑검색은 clkCnt가 0으로 오는 케이스가 있어 ctr/cpc 기반으로 최대한 복원
    if bucket == "shopping" and clk > 0 and cpc > 0:
        est_cost = max(0, int(round(clk * cpc)))
        if cost <= 0 or cost < max(1, int(round(est_cost * 0.35))):
            if est_cost > 0:
                cost = est_cost
                debug_flags.append("shopping_cost_recalc")

    return {
        "imp": imp,
        "clk": clk,
        "cost": cost,
        "conv": conv,
        "sales": sales,
        "cpc": cpc,
        "ctr": ctr,
        "debug_flags": debug_flags,
        "raw": {
            "imp": raw_imp,
            "clk": raw_clk,
            "cost": raw_cost,
            "conv": raw_conv,
            "sales": raw_sales,
        },
    }


def _iter_text_values(value):
    if isinstance(value, str):
        v = value.strip()
        if v and not v.startswith("http"):
            yield v
        return
    if isinstance(value, dict):
        skip_keys = {"extensionType", "status", "nccAdExtensionId", "ownerId", "customer_id", "type"}
        for k, v in value.items():
            if k in skip_keys:
                continue
            yield from _iter_text_values(v)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)


def _first_non_empty(value, keys):
    if isinstance(value, dict):
        for k in keys:
            v = value.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""
    if isinstance(value, list):
        for item in value:
            v = _first_non_empty(item, keys)
            if v:
                return v
    return ""


def _normalize_ext_info(ext: dict):
    ext_info = ext.get("adExtension")
    if isinstance(ext_info, (dict, list)):
        return ext_info
    return ext or {}


def parse_ext_name(ext: dict) -> str:
    ext_info = _normalize_ext_info(ext)
    ext_type = ext.get("extensionType") or ext.get("type") or "확장소재"
    cands = ["promoText", "addPromoText", "subLinkName", "pcText", "mobileText", "description", "title", "text", "name"]
    text_val = ""
    for c in cands:
        if ext_info.get(c):
            text_val = str(ext_info[c]).strip()
            break
    if not text_val:
        vals = []
        seen = set()
        for v in _iter_text_values(ext_info):
            if v not in seen:
                seen.add(v)
                vals.append(v)
            if len(vals) >= 5:
                break
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
    ad_bucket_map = {}

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
                    ad_bucket_map[str(ext_id)] = campaign_bucket(c.get("campaignTp"))
                    ext_info = _normalize_ext_info(ext)
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
                            "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                            "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
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
                    ad_bucket_map[str(ext_id)] = campaign_bucket(c.get("campaignTp"))
                    ext_info = _normalize_ext_info(ext)
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
                            "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                            "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
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
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "ccnt", "convAmt"], separators=(",", ":"))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))

    raw_stats = []
    for i in range(0, len(target_ad_ids), 50):
        chunk = target_ad_ids[i : i + 50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        res = request_json("GET", "/stats", customer_id, params=params)
        if res and "data" in res:
            raw_stats.extend(res["data"])

    if raw_stats:
        log(f"   ▶ /stats 응답 {len(raw_stats)}건 수신")

    fact_rows = []
    debug_counts = {
        "clk_from_cost_cpc": 0,
        "clk_from_ctr": 0,
        "cost_from_clk_cpc": 0,
        "non_shopping_cost_recalc": 0,
        "shopping_cost_recalc": 0,
    }
    suspicious_samples = []

    for r in raw_stats:
        ad_id = str(r.get("id") or "").strip()
        if not ad_id:
            continue

        bucket = ad_bucket_map.get(ad_id, "non_shopping")
        norm = _normalize_ext_metrics(r, bucket)

        for flag in norm["debug_flags"]:
            debug_counts[flag] = debug_counts.get(flag, 0) + 1

        if norm["debug_flags"] and len(suspicious_samples) < 8:
            suspicious_samples.append(
                f"bucket={bucket} ad_id={ad_id} flags={','.join(norm['debug_flags'])} raw={_preview_stat_row(r)} -> imp={norm['imp']}, clk={norm['clk']}, cost={norm['cost']}"
            )

        if norm["imp"] == 0 and norm["clk"] == 0 and norm["cost"] == 0 and norm["conv"] == 0 and norm["sales"] == 0:
            continue

        fact_rows.append(
            {
                "dt": target_date,
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "imp": norm["imp"],
                "clk": norm["clk"],
                "cost": norm["cost"],
                "conv": norm["conv"],
                "sales": norm["sales"],
                "roas": (norm["sales"] / norm["cost"] * 100.0) if norm["cost"] > 0 else 0.0,
            }
        )

    if fact_rows:
        if clear_fact_scope(engine, customer_id, target_date, target_ad_ids):
            upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
            log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
            total_fix = sum(debug_counts.values())
            if total_fix:
                log(
                    "   ↪ 보정 적용 | "
                    + ", ".join([
                        f"cost/cpc→클릭 {debug_counts.get('clk_from_cost_cpc', 0)}건",
                        f"ctr→클릭 {debug_counts.get('clk_from_ctr', 0)}건",
                        f"clk*cpc→비용 {debug_counts.get('cost_from_clk_cpc', 0)}건",
                        f"파워링크외 비용 재계산 {debug_counts.get('non_shopping_cost_recalc', 0)}건",
                        f"쇼핑 비용 재계산 {debug_counts.get('shopping_cost_recalc', 0)}건",
                    ])
                )
            if suspicious_samples:
                log("   ↪ 원본 응답 샘플(최대 8건)")
                for s in suspicious_samples:
                    log(f"      {s}")
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
