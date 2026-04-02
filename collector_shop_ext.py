# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 확장소재 수집기

지원 버킷
- shopping: 쇼핑검색(SSA) 캠페인 확장소재만
- non_shopping: 파워링크/플레이스/브랜드검색 등 쇼핑검색 외 검색광고 확장소재
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
            try:
                body = r.text[:500]
            except Exception:
                body = ""
            log(f"⚠️ API 오류 {r.status_code} | {path} | {body}")
            return None
        except Exception as e:
            log(f"⚠️ API 요청 예외 | {path} | {e}")
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
        raise
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


def _normalize_ext_info(ext: dict):
    ext_info = ext.get("adExtension")
    if isinstance(ext_info, (dict, list)):
        return ext_info
    return ext or {}


def _iter_dicts(value):
    if isinstance(value, dict):
        yield value
        for v in value.values():
            yield from _iter_dicts(v)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_dicts(item)


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
    for d in _iter_dicts(value):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def parse_ext_name(ext: dict) -> str:
    ext_info = _normalize_ext_info(ext)
    ext_type = ext.get("extensionType") or ext.get("type") or "확장소재"
    cands = ["promoText", "addPromoText", "subLinkName", "pcText", "mobileText", "description", "title", "text", "name"]
    text_val = _first_non_empty(ext_info, cands)
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
    return {"shopping": "쇼핑검색(SSA)", "non_shopping": "파워링크 외 검색광고", "all": "전체"}.get(ext_bucket, ext_bucket)


def match_bucket(campaign_tp: str | None, ext_bucket: str) -> bool:
    bucket = campaign_bucket(campaign_tp)
    return ext_bucket == "all" or bucket == ext_bucket


def _deep_candidates(row: dict, keys: list[str]):
    vals = []
    for d in _iter_dicts(row):
        for k in keys:
            if k in d and d.get(k) not in (None, ""):
                vals.append(d.get(k))
    return vals


def _first_metric(row: dict, keys: list[str], cast="int"):
    vals = _deep_candidates(row, keys)
    if not vals:
        return 0 if cast == "int" else 0.0
    if cast == "float":
        for v in vals:
            fv = _to_float(v, None)
            if fv is not None:
                return fv
        return 0.0
    for v in vals:
        iv = _to_int(v, None)
        if iv is not None:
            return iv
    return 0


def _raw_metrics_preview(r: dict) -> str:
    keep = [
        "id", "nccAdExtensionId", "impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "ccnt",
        "convAmt", "ror", "cpConv", "viewCnt", "cost", "spend", "chargeAmt", "clickCnt"
    ]
    slim = {}
    for d in _iter_dicts(r):
        for k in keep:
            if k in d and k not in slim:
                slim[k] = d.get(k)
    return json.dumps(slim, ensure_ascii=False, separators=(",", ":"))


def _normalize_stats_row(r: dict):
    ad_id = str(
        _first_metric(r, ["id", "nccAdExtensionId", "nccAdId"], cast="int") or
        _first_non_empty(r, ["id", "nccAdExtensionId", "nccAdId"]) or ""
    ).strip()
    if ad_id == "0":
        ad_id = str(_first_non_empty(r, ["id", "nccAdExtensionId", "nccAdId"]))

    imp = _first_metric(r, ["impCnt", "imp"])
    clk = _first_metric(r, ["clkCnt", "clickCnt", "clk", "clicks"])
    cost = _first_metric(r, ["salesAmt", "chargeAmt", "spend", "cost", "amt"])
    conv = _first_metric(r, ["ccnt", "convCnt", "conversionCnt", "conversions"], cast="float")
    sales = _first_metric(r, ["convAmt", "conversionValue", "sales", "convValue"])
    ctr = _first_metric(r, ["ctr"], cast="float")
    cpc = _first_metric(r, ["cpc"], cast="float")
    ror = _first_metric(r, ["ror", "roas"], cast="float")

    # 안전한 보정만 허용
    if clk <= 0 and imp > 0 and ctr > 0:
        clk = max(0, int(round(imp * ctr / 100.0)))
    if cost <= 0 and clk > 0 and cpc > 0:
        cost = max(0, int(round(clk * cpc)))
    if sales <= 0 and cost > 0 and ror > 0:
        sales = max(0, int(round(cost * ror / 100.0)))

    return {
        "ad_id": ad_id,
        "imp": imp,
        "clk": clk,
        "cost": cost,
        "conv": conv,
        "sales": sales,
        "ctr": ctr,
        "cpc": cpc,
        "ror": ror,
    }


def fetch_stats_for_ids(customer_id: str, ad_ids: list[str], target_date: date, bucket: str):
    d_str = target_date.strftime("%Y-%m-%d")
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "ccnt", "convAmt", "ror"], separators=(",", ":"))
    raw_stats = []

    for i in range(0, len(ad_ids), 50):
        chunk = ad_ids[i:i + 50]
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        res = request_json("GET", "/stats", customer_id, params=params)
        if isinstance(res, dict) and isinstance(res.get("data"), list):
            raw_stats.extend(res["data"])

    # 1차 응답이 비거나 일부 누락되면 개별 재조회
    found_ids = {str(_normalize_stats_row(r).get("ad_id") or "") for r in raw_stats}
    missing_ids = [x for x in ad_ids if str(x) not in found_ids]
    if missing_ids:
        log(f"   ↪ {bucket_label(bucket)} /stats 미응답 ID {len(missing_ids)}건 개별 재조회")
        for ad_id in missing_ids[:200]:
            params = {"ids": str(ad_id), "fields": fields, "timeRange": time_range}
            res = request_json("GET", "/stats", customer_id, params=params)
            if isinstance(res, dict) and isinstance(res.get("data"), list):
                raw_stats.extend(res["data"])

    return raw_stats


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
    ad_bucket_map = {}
    target_ad_ids = []

    for c in selected_camps:
        cid = c.get("nccCampaignId")
        bucket = campaign_bucket(c.get("campaignTp"))
        camp_rows.append({
            "customer_id": str(customer_id),
            "campaign_id": str(cid),
            "campaign_name": c.get("name"),
            "campaign_tp": c.get("campaignTp"),
            "status": c.get("status"),
        })

        camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": cid}) or []
        if camp_exts:
            ag_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": f"CAMP_{cid}",
                "campaign_id": str(cid),
                "adgroup_name": "[캠페인 공통 소재]",
                "status": "ELIGIBLE",
            })
            for ext in camp_exts:
                ext_id = str(ext.get("nccAdExtensionId") or "").strip()
                if not ext_id:
                    continue
                target_ad_ids.append(ext_id)
                ad_bucket_map[ext_id] = bucket
                ext_info = _normalize_ext_info(ext)
                display_name = parse_ext_name(ext)
                ad_rows.append({
                    "customer_id": str(customer_id),
                    "ad_id": ext_id,
                    "adgroup_id": f"CAMP_{cid}",
                    "ad_name": display_name,
                    "status": ext.get("status"),
                    "ad_title": display_name,
                    "ad_desc": display_name,
                    "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                    "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
                    "creative_text": display_name[:500],
                })

        groups = request_json("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": cid}) or []
        for g in groups:
            gid = g.get("nccAdgroupId")
            ag_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": str(gid),
                "campaign_id": str(cid),
                "adgroup_name": g.get("name"),
                "status": g.get("status"),
            })
            exts = request_json("GET", "/ncc/ad-extensions", customer_id, {"ownerId": gid}) or []
            for ext in exts:
                ext_id = str(ext.get("nccAdExtensionId") or "").strip()
                if not ext_id:
                    continue
                target_ad_ids.append(ext_id)
                ad_bucket_map[ext_id] = bucket
                ext_info = _normalize_ext_info(ext)
                display_name = parse_ext_name(ext)
                ad_rows.append({
                    "customer_id": str(customer_id),
                    "ad_id": ext_id,
                    "adgroup_id": str(gid),
                    "ad_name": display_name,
                    "status": ext.get("status"),
                    "ad_title": display_name,
                    "ad_desc": display_name,
                    "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                    "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
                    "creative_text": display_name[:500],
                })

    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    log(f"   ▶ 캠페인({len(camp_rows)}), 광고그룹({len(ag_rows)}), 확장소재({len(ad_rows)}) 매핑 완료!")

    target_ad_ids = sorted({x for x in target_ad_ids if x})
    if not target_ad_ids:
        log("   ⚠️ 수집 대상 확장소재가 없습니다.")
        return

    # 버킷별로 따로 /stats 요청해서 한쪽 응답이 다른 쪽을 먹는 문제를 방지
    ids_by_bucket = {
        "shopping": [x for x in target_ad_ids if ad_bucket_map.get(x) == "shopping"],
        "non_shopping": [x for x in target_ad_ids if ad_bucket_map.get(x) == "non_shopping"],
    }

    raw_stats = []
    for bucket, ids in ids_by_bucket.items():
        if not ids:
            continue
        log(f"   ▶ {bucket_label(bucket)} 확장소재 {len(ids)}개 /stats 조회 중...")
        bucket_stats = fetch_stats_for_ids(customer_id, ids, target_date, bucket)
        log(f"   ↪ {bucket_label(bucket)} /stats 응답 {len(bucket_stats)}건 수신")
        raw_stats.extend(bucket_stats)
        for sample in bucket_stats[:3]:
            log(f"   ↪ 샘플[{bucket}] {_raw_metrics_preview(sample)}")

    if not raw_stats:
        log("   ⚠️ /stats 응답이 없습니다.")
        return

    fact_rows = []
    bad_rows = []
    for r in raw_stats:
        norm = _normalize_stats_row(r)
        ad_id = str(norm.get("ad_id") or "").strip()
        if not ad_id:
            if len(bad_rows) < 10:
                bad_rows.append(f"ad_id_missing raw={_raw_metrics_preview(r)}")
            continue
        if norm["imp"] == 0 and norm["clk"] == 0 and norm["cost"] == 0 and norm["conv"] == 0 and norm["sales"] == 0:
            continue
        fact_rows.append({
            "dt": target_date,
            "customer_id": str(customer_id),
            "ad_id": ad_id,
            "imp": norm["imp"],
            "clk": norm["clk"],
            "cost": norm["cost"],
            "conv": norm["conv"],
            "sales": norm["sales"],
            "roas": (norm["sales"] / norm["cost"] * 100.0) if norm["cost"] > 0 else 0.0,
        })
        if len(bad_rows) < 10:
            bucket = ad_bucket_map.get(ad_id, "unknown")
            if bucket == "shopping" and norm["clk"] == 0 and norm["imp"] > 0:
                bad_rows.append(f"shopping_clk_zero ad_id={ad_id} raw={_raw_metrics_preview(r)}")
            if bucket == "non_shopping" and norm["cost"] == norm["clk"] and norm["clk"] > 0:
                bad_rows.append(f"non_shopping_cost_equals_clk ad_id={ad_id} raw={_raw_metrics_preview(r)}")
            if bucket == "non_shopping" and norm["cost"] > 0 and norm["conv"] == 0 and norm["sales"] == 0:
                bad_rows.append(f"non_shopping_conv_sales_zero ad_id={ad_id} raw={_raw_metrics_preview(r)}")

    if not fact_rows:
        log("   ⚠️ 조회된 날짜에 노출/클릭이 발생한 확장소재가 없습니다.")
        return

    if not clear_fact_scope(engine, customer_id, target_date, target_ad_ids):
        log("   ❌ fact_ad_daily 범위 삭제 실패로 적재를 중단했습니다. 중복 방지를 위해 확인 후 재실행하세요.")
        return

    upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
    log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")

    if bad_rows:
        log("   ↪ 확인 필요 샘플(최대 10건)")
        for row in bad_rows[:10]:
            log(f"      {row}")


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

    log(f"🧩 확장소재 수집 구분: {bucket_label(args.ext_bucket)} ({args.ext_bucket})")
    for acc in accounts:
        process_account(engine, acc, target_date, args.ext_bucket)


if __name__ == "__main__":
    main()
