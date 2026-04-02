# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 확장소재 수집기

수집 전략
- 성과는 /stats 직접 조회보다 stat-report 기반(ADEXTENSION / ADEXTENSION_CONVERSION)을 우선 사용
- 쇼핑검색/파워링크외 버킷은 구조 매핑 후 ad_id 기준으로 필터링
- 같은 날짜 재실행 시 dt+customer_id+ad_id 기준 업서트
"""

import os
import time
import io
import csv
import json
import re
import hmac
import base64
import hashlib
import argparse
import random
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
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
DEBUG_REPORT_DIR = os.getenv("DEBUG_REPORT_DIR", "debug_reports")


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


def make_headers(method: str, path: str, customer_id: str) -> dict:
    ts = now_millis()
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sign_path_only(method.upper(), path, ts, API_SECRET),
    }


def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=False):
    url = BASE_URL + path
    session = requests.Session()
    max_retries = 6
    for attempt in range(max_retries):
        try:
            r = session.request(method, url, headers=make_headers(method, path, customer_id), params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 + attempt + random.uniform(0.1, 0.8))
                continue
            data = None
            try:
                data = r.json()
            except Exception:
                data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                if raise_error:
                    raise
                return 0, str(e)
            time.sleep(2 + attempt)
    return 0, None


def get_engine():
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    return create_engine(db_url, poolclass=NullPool, future=True)


def save_debug_report(tp: str, customer_id: str, job_id: str, content: str):
    try:
        if not content:
            return
        os.makedirs(DEBUG_REPORT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(DEBUG_REPORT_DIR, f"{ts}_{customer_id}_{tp}_{job_id}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass


def parse_report_text_to_df(txt: str) -> pd.DataFrame:
    txt = (txt or "").replace("﻿", "").strip()
    if not txt:
        return pd.DataFrame()

    lines = [ln for ln in txt.splitlines() if ln is not None]
    if not lines:
        return pd.DataFrame()

    sample = [ln for ln in lines[:20] if str(ln).strip()]
    sep = "\t" if sum(ln.count("\t") for ln in sample) >= sum(ln.count(",") for ln in sample) else ","

    def _manual_parse(delim: str) -> pd.DataFrame:
        rows = []
        reader = csv.reader(io.StringIO(txt), delimiter=delim)
        max_cols = 0
        for row in reader:
            row = [str(c).strip() for c in row]
            rows.append(row)
            max_cols = max(max_cols, len(row))
        if not rows:
            return pd.DataFrame()
        norm_rows = [r + [""] * (max_cols - len(r)) for r in rows]
        return pd.DataFrame(norm_rows)

    tries = []
    if sep == "\t":
        tries = ["\t", ","]
    else:
        tries = [",", "\t"]

    for delim in tries:
        try:
            df = _manual_parse(delim)
            if not df.empty and max(df.shape) > 1:
                return df
        except Exception:
            pass

    for delim in tries:
        try:
            df = pd.read_csv(io.StringIO(txt), sep=delim, header=None, dtype=str, on_bad_lines="skip")
            if not df.empty:
                return df.fillna("")
        except Exception:
            pass

    return pd.DataFrame()


def resolve_download_url(dl_url: str) -> str:
    if not dl_url:
        return ""
    dl_url = str(dl_url).strip()
    if dl_url.startswith("http://") or dl_url.startswith("https://"):
        return dl_url
    if dl_url.startswith("/"):
        return BASE_URL + dl_url
    return f"{BASE_URL}/{dl_url.lstrip('/')}"


def download_report_dataframe(customer_id: str, tp: str, job_id: str, initial_url: str) -> pd.DataFrame:
    session = requests.Session()
    current_url = initial_url
    for retry in range(3):
        url = resolve_download_url(current_url)
        try:
            r = session.get(url, timeout=60, allow_redirects=True)
            if r.status_code == 200:
                r.encoding = "utf-8"
                save_debug_report(tp, customer_id, job_id, r.text)
                return parse_report_text_to_df(r.text)

            parsed = urlparse(url)
            if url.startswith(BASE_URL):
                r2 = session.get(url, headers=make_headers("GET", parsed.path or "/", customer_id), timeout=60, allow_redirects=True)
                if r2.status_code == 200:
                    r2.encoding = "utf-8"
                    save_debug_report(tp, customer_id, job_id, r2.text)
                    return parse_report_text_to_df(r2.text)

            s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id)
            if s_status == 200 and isinstance(s_data, dict) and s_data.get("downloadUrl"):
                current_url = s_data.get("downloadUrl")
            log(f"⚠️ [{tp}] 다운로드 실패 재시도 {retry+1}/3")
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ [{tp}] 다운로드 예외: {e} (재시도 {retry+1}/3)")
            time.sleep(2)
    return pd.DataFrame()


def fetch_stat_report(customer_id: str, report_tp: str, target_date: date) -> pd.DataFrame:
    payload = {"reportTp": report_tp, "statDt": target_date.strftime("%Y%m%d")}
    status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload)
    if status != 200 or not isinstance(data, dict) or not data.get("reportJobId"):
        log(f"⚠️ [{report_tp}] 리포트 요청 실패: HTTP {status} | {data}")
        return pd.DataFrame()

    job_id = data["reportJobId"]
    for _ in range(120):
        s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id)
        if s_status == 200 and isinstance(s_data, dict):
            st = s_data.get("status")
            if st == "BUILT":
                try:
                    return download_report_dataframe(customer_id, report_tp, job_id, s_data.get("downloadUrl", ""))
                finally:
                    request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
            if st == "NONE":
                request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
                log(f"⚠️ [{report_tp}] NONE 응답")
                return pd.DataFrame()
            if st == "ERROR":
                request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
                log(f"⚠️ [{report_tp}] ERROR 응답")
                return pd.DataFrame()
        time.sleep(1)
    request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
    log(f"⚠️ [{report_tp}] 타임아웃")
    return pd.DataFrame()


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
        if raw_conn:
            raw_conn.rollback()
        log(f"⚠️ {table} 저장 중 오류: {e}")
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
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM fact_ad_daily WHERE customer_id=:cid AND dt=:dt AND ad_id = ANY(:ids)"),
                {"cid": str(customer_id), "dt": target_date, "ids": ad_ids},
            )
        return True
    except Exception as e:
        log(f"⚠️ fact_ad_daily 범위 삭제 실패: {e}")
        return False


def normalize_header(v: str) -> str:
    return str(v).lower().replace(" ", "").replace("_", "").replace("-", "").replace('"', '').replace("'", "")


def get_col_idx(headers, candidates):
    norm_headers = [normalize_header(h) for h in headers]
    norm_candidates = [normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c in h and "그룹" not in h:
                return i
    return -1


def safe_float(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


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


REPORT_ID_ALIASES = [
    "확장소재id", "광고확장소재id", "nccadextensionid", "adextensionid", "adextension",
    "소재id", "광고id", "adid", "id", "nccadid"
]
REPORT_IMP_ALIASES = [
    "노출수", "노출", "impressions", "impression", "imp", "impcnt", "exposurecount"
]
REPORT_CLK_ALIASES = [
    "클릭수", "클릭", "clicks", "click", "clk", "clkcnt", "clickcount"
]
REPORT_COST_ALIASES = [
    "총비용", "비용", "광고비", "소진비용", "cost", "spend", "salesamt", "amount"
]
REPORT_CONV_ALIASES = [
    "전환수", "전환", "conversion", "conversions", "ccnt", "conv", "conversioncount"
]
REPORT_SALES_ALIASES = [
    "전환매출액", "전환매출", "매출", "매출액", "conversionvalue", "sales", "convamt", "salesamount"
]


def _row_non_empty_count(row_vals: list[str]) -> int:
    return sum(1 for x in row_vals if str(x).strip())


def _row_contains_any(row_vals: list[str], candidates: list[str]) -> bool:
    cand_norm = [normalize_header(c) for c in candidates]
    for rv in row_vals:
        for c in cand_norm:
            if c and c in rv:
                return True
    return False


def _find_header_row(df: pd.DataFrame, id_candidates: list[str]) -> int:
    max_rows = min(100, len(df))
    metric_aliases = REPORT_IMP_ALIASES + REPORT_CLK_ALIASES + REPORT_COST_ALIASES + REPORT_CONV_ALIASES + REPORT_SALES_ALIASES
    best_idx = -1
    best_score = -1
    for i in range(max_rows):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if _row_non_empty_count(row_vals) < 3:
            continue
        score = 0
        if _row_contains_any(row_vals, id_candidates):
            score += 4
        if _row_contains_any(row_vals, REPORT_IMP_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_CLK_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_COST_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_CONV_ALIASES):
            score += 1
        if _row_contains_any(row_vals, REPORT_SALES_ALIASES):
            score += 1
        if any(rv in {"date", "기준일", "일자", "statdt"} for rv in row_vals):
            score += 1
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 6:
            return i
    return best_idx if best_score >= 3 else -1


def _debug_preview_rows(df: pd.DataFrame, max_rows: int = 8):
    for i in range(min(max_rows, len(df))):
        row = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        row = [x for x in row if x]
        if row:
            log(f"   ↪ 헤더탐지 preview[{i}] {row[:12]}")


def _guess_id_col_from_rows(data_df: pd.DataFrame) -> int:
    max_scan = min(50, len(data_df))
    best_idx = -1
    best_hits = 0
    for col_idx in range(data_df.shape[1]):
        hits = 0
        for i in range(max_scan):
            try:
                val = str(data_df.iloc[i, col_idx]).strip().lower()
            except Exception:
                continue
            if val.startswith("ext-") or val.startswith("nad-"):
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_idx = col_idx
    return best_idx if best_hits >= 2 else -1




def _looks_like_fixed_adext_report(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    scan_rows = min(20, len(df))
    ext_hits = 0
    num_tail_hits = 0
    for i in range(scan_rows):
        vals = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        if len(vals) < 12:
            continue
        if len(vals) > 6 and str(vals[6]).strip().lower().startswith("ext-"):
            ext_hits += 1
        tail = vals[-2:]
        if len(tail) == 2 and all(re.fullmatch(r"-?\d+(?:\.\d+)?", str(x).replace(",", "")) for x in tail):
            num_tail_hits += 1
    return ext_hits >= 2 and num_tail_hits >= 2


def _guess_fixed_adext_columns(df: pd.DataFrame) -> dict:
    # 기본 포맷(로그 preview 기준)
    # [0]dt [1]customer_id [2]campaign_id [3]adgroup_id [4]keyword_id/- [5]ad_id(nad-*) [6]ext_id(ext-*) ... [9]device [10]imp [11]clk
    cols = {"ad_idx": 5, "id_idx": 6, "imp_idx": 10, "clk_idx": 11, "cost_idx": -1, "conv_idx": -1, "sales_idx": -1}
    if df is None or df.empty:
        return cols
    scan_rows = min(50, len(df))
    # ext-id / nad-id는 패턴으로 다시 탐지
    ext_best = (-1, -1)
    nad_best = (-1, -1)
    for col_idx in range(df.shape[1]):
        ext_hits = 0
        nad_hits = 0
        for i in range(scan_rows):
            try:
                v = str(df.iloc[i, col_idx]).strip().lower()
            except Exception:
                continue
            if v.startswith('ext-'):
                ext_hits += 1
            if v.startswith('nad-'):
                nad_hits += 1
        if ext_hits > ext_best[1]:
            ext_best = (col_idx, ext_hits)
        if nad_hits > nad_best[1]:
            nad_best = (col_idx, nad_hits)
    if ext_best[1] >= 2:
        cols['id_idx'] = ext_best[0]
    if nad_best[1] >= 2:
        cols['ad_idx'] = nad_best[0]

    # 노출/클릭은 뒤에서 가까운 숫자 2개를 우선 사용
    imp_guess = None
    clk_guess = None
    for i in range(scan_rows):
        vals = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        num_cols = []
        for idx, v in enumerate(vals):
            vv = str(v).replace(',', '')
            if re.fullmatch(r"-?\d+(?:\.\d+)?", vv):
                num_cols.append(idx)
        if len(num_cols) >= 2:
            imp_guess = num_cols[-2]
            clk_guess = num_cols[-1]
            break
    if imp_guess is not None and clk_guess is not None:
        cols['imp_idx'] = imp_guess
        cols['clk_idx'] = clk_guess
    return cols


def _parse_fixed_adext_report(df: pd.DataFrame, include_conv_cols: bool) -> dict:
    cols = _guess_fixed_adext_columns(df)
    log(
        f"   ↪ 고정포맷 fallback 적용: ad={cols['ad_idx']} ext={cols['id_idx']} imp={cols['imp_idx']} clk={cols['clk_idx']}"
    )
    out = {}
    for _, r in df.iterrows():
        vals = [str(x).strip() for x in r.fillna("").tolist()]
        if len(vals) <= max(cols['id_idx'], cols['imp_idx'], cols['clk_idx']):
            continue
        obj_id = vals[cols['id_idx']].strip()
        obj_id_l = obj_id.lower()
        if not obj_id or obj_id == '-' or not obj_id_l.startswith('ext-'):
            continue
        bucket = out.setdefault(obj_id, {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0})
        bucket['imp'] += int(safe_float(vals[cols['imp_idx']])) if cols['imp_idx'] != -1 else 0
        bucket['clk'] += int(safe_float(vals[cols['clk_idx']])) if cols['clk_idx'] != -1 else 0
        if cols['cost_idx'] != -1 and len(vals) > cols['cost_idx']:
            bucket['cost'] += int(safe_float(vals[cols['cost_idx']]))
        if include_conv_cols and cols['conv_idx'] != -1 and len(vals) > cols['conv_idx']:
            bucket['conv'] += safe_float(vals[cols['conv_idx']])
        if include_conv_cols and cols['sales_idx'] != -1 and len(vals) > cols['sales_idx']:
            bucket['sales'] += int(safe_float(vals[cols['sales_idx']]))
    return out

def _parse_metric_report(df: pd.DataFrame, id_candidates: list[str], include_conv_cols: bool) -> dict:
    if df is None or df.empty:
        return {}
    header_idx = _find_header_row(df, id_candidates)
    if header_idx == -1:
        if _looks_like_fixed_adext_report(df):
            log("⚠️ 리포트 헤더를 찾지 못했습니다. 쇼핑검색 ADEXTENSION 고정포맷 fallback으로 계속 진행합니다.")
            _debug_preview_rows(df)
            return _parse_fixed_adext_report(df, include_conv_cols)
        log("⚠️ 리포트 헤더를 찾지 못했습니다. preview를 남기고 이 리포트는 건너뜁니다.")
        _debug_preview_rows(df)
        return {}

    raw_headers = [str(x).strip() for x in df.iloc[header_idx].fillna("")]
    headers = [normalize_header(str(x)) for x in raw_headers]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)

    id_idx = get_col_idx(headers, REPORT_ID_ALIASES + id_candidates)
    imp_idx = get_col_idx(headers, REPORT_IMP_ALIASES)
    clk_idx = get_col_idx(headers, REPORT_CLK_ALIASES)
    cost_idx = get_col_idx(headers, REPORT_COST_ALIASES)
    conv_idx = get_col_idx(headers, REPORT_CONV_ALIASES)
    sales_idx = get_col_idx(headers, REPORT_SALES_ALIASES)

    if id_idx == -1:
        id_idx = _guess_id_col_from_rows(data_df)
        if id_idx != -1:
            log(f"   ↪ 확장소재 ID 컬럼 fallback 적용: col={id_idx} header='{raw_headers[id_idx] if id_idx < len(raw_headers) else ''}'")
    if id_idx == -1:
        log("⚠️ 리포트에 확장소재 ID 컬럼을 찾지 못했습니다. preview를 남깁니다.")
        _debug_preview_rows(df)
        return {}

    out = {}
    for _, r in data_df.iterrows():
        if len(r) <= id_idx:
            continue
        obj_id = str(r.iloc[id_idx]).strip()
        if not obj_id or obj_id == "-":
            continue
        obj_id_l = obj_id.lower()
        if obj_id_l in {"id", "adid", "adextensionid", "nccadextensionid", "확장소재id", "광고id", "소재id"}:
            continue
        if not (obj_id_l.startswith("ext-") or obj_id_l.startswith("nad-")):
            continue
        bucket = out.setdefault(obj_id, {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0})
        if imp_idx != -1 and len(r) > imp_idx:
            bucket["imp"] += int(safe_float(r.iloc[imp_idx]))
        if clk_idx != -1 and len(r) > clk_idx:
            bucket["clk"] += int(safe_float(r.iloc[clk_idx]))
        if cost_idx != -1 and len(r) > cost_idx:
            bucket["cost"] += int(safe_float(r.iloc[cost_idx]))
        if include_conv_cols and conv_idx != -1 and len(r) > conv_idx:
            bucket["conv"] += safe_float(r.iloc[conv_idx])
        if include_conv_cols and sales_idx != -1 and len(r) > sales_idx:
            bucket["sales"] += int(safe_float(r.iloc[sales_idx]))
    return out


def _merge_metric_maps(base_map: dict, conv_map: dict) -> dict:
    out = {}
    keys = set(base_map.keys()) | set(conv_map.keys())
    for k in keys:
        b = base_map.get(k, {})
        c = conv_map.get(k, {})
        out[k] = {
            "imp": int(b.get("imp", 0) or 0),
            "clk": int(b.get("clk", 0) or 0),
            "cost": int(b.get("cost", 0) or 0),
            "conv": float(c.get("conv", b.get("conv", 0.0)) or 0.0),
            "sales": int(c.get("sales", b.get("sales", 0)) or 0),
        }
    return out


def _report_sample(metric_map: dict, label: str):
    items = list(metric_map.items())[:5]
    for ad_id, m in items:
        log(f"   ↪ 샘플[{label}] ad_id={ad_id} imp={m.get('imp',0)} clk={m.get('clk',0)} cost={m.get('cost',0)} conv={m.get('conv',0)} sales={m.get('sales',0)}")


def process_account(engine, customer_id: str, target_date: date, ext_bucket: str = "shopping"):
    log(f"--- [ {customer_id} ] {bucket_label(ext_bucket)} 확장소재 수집 시작 ({target_date}) ---")
    status, camps = request_json("GET", "/ncc/campaigns", customer_id)
    if status != 200 or not isinstance(camps, list):
        log("⚠️ 캠페인 조회 실패")
        return

    selected_camps = [c for c in camps if match_bucket(c.get("campaignTp"), ext_bucket)]
    shopping_cnt = sum(1 for c in selected_camps if campaign_bucket(c.get("campaignTp")) == "shopping")
    non_shopping_cnt = len(selected_camps) - shopping_cnt
    log(f"   ▶ 대상 캠페인 {len(selected_camps)}개 | 쇼핑검색 {shopping_cnt}개 | 파워링크외 {non_shopping_cnt}개")

    camp_rows, ag_rows, ad_rows = [], [], []
    ad_bucket_map = {}
    target_ad_ids = []

    for c in selected_camps:
        cid = str(c.get("nccCampaignId"))
        bucket = campaign_bucket(c.get("campaignTp"))
        camp_rows.append({
            "customer_id": str(customer_id),
            "campaign_id": cid,
            "campaign_name": c.get("name"),
            "campaign_tp": c.get("campaignTp"),
            "status": c.get("status"),
        })

        for owner_id, agid, agname in [(cid, f"CAMP_{cid}", "[캠페인 공통 소재]")]:
            s, camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, params={"ownerId": owner_id})
            camp_exts = camp_exts if s == 200 and isinstance(camp_exts, list) else []
            if camp_exts:
                ag_rows.append({
                    "customer_id": str(customer_id),
                    "adgroup_id": agid,
                    "campaign_id": cid,
                    "adgroup_name": agname,
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
                        "adgroup_id": agid,
                        "ad_name": display_name,
                        "status": ext.get("status"),
                        "ad_title": display_name,
                        "ad_desc": display_name,
                        "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                        "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
                        "creative_text": display_name[:500],
                    })

        s_groups, groups = request_json("GET", "/ncc/adgroups", customer_id, params={"nccCampaignId": cid})
        groups = groups if s_groups == 200 and isinstance(groups, list) else []
        for g in groups:
            gid = str(g.get("nccAdgroupId"))
            ag_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": gid,
                "campaign_id": cid,
                "adgroup_name": g.get("name"),
                "status": g.get("status"),
            })
            s_exts, exts = request_json("GET", "/ncc/ad-extensions", customer_id, params={"ownerId": gid})
            exts = exts if s_exts == 200 and isinstance(exts, list) else []
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
                    "adgroup_id": gid,
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

    # 공식 대용량 리포트 기반 수집
    log(f"   ▶ ADEXTENSION / ADEXTENSION_CONVERSION 리포트 수집 중...")
    base_df = fetch_stat_report(customer_id, "ADEXTENSION", target_date)
    conv_df = fetch_stat_report(customer_id, "ADEXTENSION_CONVERSION", target_date)

    id_candidates = REPORT_ID_ALIASES
    base_map = _parse_metric_report(base_df, id_candidates, include_conv_cols=False)
    conv_map = _parse_metric_report(conv_df, id_candidates, include_conv_cols=True)
    metric_map = _merge_metric_maps(base_map, conv_map)

    # 대상 버킷으로만 필터
    metric_map = {ad_id: m for ad_id, m in metric_map.items() if ad_id in target_ad_ids}

    if base_map:
        _report_sample({k: v for k, v in base_map.items() if k in target_ad_ids}, "ADEXTENSION")
    if conv_map:
        _report_sample({k: v for k, v in conv_map.items() if k in target_ad_ids}, "ADEXTENSION_CONVERSION")

    if not metric_map:
        log("   ⚠️ 확장소재 성과 리포트에서 대상 ad_id 데이터를 찾지 못했습니다.")
        log(f"   ↪ ADEXTENSION rows={len(base_df) if base_df is not None else 0} | ADEXTENSION_CONVERSION rows={len(conv_df) if conv_df is not None else 0}")
        log("   ↪ debug_reports 폴더의 ADEXTENSION / ADEXTENSION_CONVERSION 원본을 확인하세요.")
        return

    missing = [ad_id for ad_id in target_ad_ids if ad_id not in metric_map]
    if missing:
        log(f"   ↪ 리포트 미포착 확장소재 {len(missing)}건")
        for ad_id in missing[:10]:
            log(f"      missing ad_id={ad_id} bucket={ad_bucket_map.get(ad_id)}")

    fact_rows = []
    shopping_zero = 0
    for ad_id, m in metric_map.items():
        imp = int(m.get("imp", 0) or 0)
        clk = int(m.get("clk", 0) or 0)
        cost = int(m.get("cost", 0) or 0)
        conv = float(m.get("conv", 0.0) or 0.0)
        sales = int(m.get("sales", 0) or 0)
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            continue
        if ad_bucket_map.get(ad_id) == "shopping" and imp > 0 and clk == 0:
            shopping_zero += 1
        fact_rows.append({
            "dt": target_date,
            "customer_id": str(customer_id),
            "ad_id": str(ad_id),
            "imp": imp,
            "clk": clk,
            "cost": cost,
            "conv": conv,
            "sales": sales,
            "roas": (sales / cost * 100.0) if cost > 0 else 0.0,
        })

    if not fact_rows:
        log("   ⚠️ 리포트상 성과가 있는 확장소재가 없습니다.")
        return

    if not clear_fact_scope(engine, customer_id, target_date, target_ad_ids):
        log("   ❌ fact_ad_daily 범위 삭제 실패로 적재를 중단합니다.")
        return

    upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
    log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
    if shopping_zero:
        log(f"   ↪ 쇼핑검색(SSA)에서 imp>0 이지만 clk=0 인 확장소재 {shopping_zero}건")


def normalize_ext_bucket(v: str) -> str:
    mapping = {
        "shopping": "shopping",
        "쇼핑검색": "shopping",
        "쇼핑검색(ssa)": "shopping",
        "non_shopping": "non_shopping",
        "파워링크외": "non_shopping",
        "파워링크 외": "non_shopping",
        "파워링크 외 검색광고": "non_shopping",
        "all": "all",
        "전체": "all",
    }
    key = str(v or "shopping").strip().lower()
    return mapping.get(key, str(v or "shopping").strip())


def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--account_name", type=str, default="")
    parser.add_argument("--account_names", type=str, default="")
    parser.add_argument("--ext_bucket", type=str, default="shopping")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    ext_bucket = normalize_ext_bucket(args.ext_bucket)

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
                accounts = [str(r[0]) for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta WHERE COALESCE(naver_media_type, 'sa') <> 'gfa'"))]
        except Exception:
            pass

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

    log(f"🧩 확장소재 수집 구분: {bucket_label(ext_bucket)} ({ext_bucket})")
    for acc in accounts:
        process_account(engine, acc, target_date, ext_bucket)


if __name__ == "__main__":
    main()
