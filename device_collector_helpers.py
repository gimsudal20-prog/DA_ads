# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg2.extras
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEVICE_HEADER_CANDIDATES = [
    "pc mobile type", "pc_mobile_type", "pc/mobile type", "pcmobiletype", "pc/mobile",
    "pcmobile", "pc_mobile", "pc mobile", "pc/m", "device", "device_name",
    "devicename", "기기", "디바이스", "노출기기", "노출 기기", "노출디바이스",
    "단말기", "pc모바일", "피시모바일", "기기구분", "디바이스구분", "플랫폼",
]


def _normalize_header(v: str) -> str:
    s = str(v or "").strip().lower()
    for ch in [" ", "_", "-", "\"", "'", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ":", ";", "\t", "\r", "\n"]:
        s = s.replace(ch, "")
    return s

def _get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [_normalize_header(h) for h in headers]
    norm_candidates = [_normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c and c in h:
                return i
    return -1


def _safe_float(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def normalize_device_name(v: Any) -> str:
    raw = str(v or "").strip()
    if not raw or raw == "-":
        return ""
    raw_upper = raw.upper().strip()
    norm = _normalize_header(raw)

    if raw_upper in {"P", "PC"} or norm in {"p", "pc", "desktop"} or "desktop" in norm:
        return "PC"
    if raw_upper in {"M", "MO", "MOBILE"} or norm in {"m", "mo", "mobile"}:
        return "MOBILE"
    if "모바일" in raw or "mobile" in norm or "phone" in norm or "app" in norm:
        return "MOBILE"
    if "pc" in norm:
        return "PC"
    return ""


def _ensure_column(engine: Engine, table: str, column: str, datatype: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {datatype}"))
    except Exception:
        pass


def ensure_device_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_campaign_device_daily (
                dt DATE,
                customer_id TEXT,
                campaign_id TEXT,
                device_name TEXT,
                imp BIGINT,
                clk BIGINT,
                cost BIGINT,
                conv DOUBLE PRECISION,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                avg_rnk DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                source_report TEXT,
                PRIMARY KEY(dt, customer_id, campaign_id, device_name)
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_ad_device_daily (
                dt DATE,
                customer_id TEXT,
                ad_id TEXT,
                device_name TEXT,
                imp BIGINT,
                clk BIGINT,
                cost BIGINT,
                conv DOUBLE PRECISION,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                avg_rnk DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                source_report TEXT,
                PRIMARY KEY(dt, customer_id, ad_id, device_name)
            )
            """
        ))

    for table in ["fact_campaign_device_daily", "fact_ad_device_daily"]:
        _ensure_column(engine, table, "roas", "DOUBLE PRECISION DEFAULT 0")
        _ensure_column(engine, table, "avg_rnk", "DOUBLE PRECISION DEFAULT 0")
        _ensure_column(engine, table, "data_source", "TEXT")
        _ensure_column(engine, table, "source_report", "TEXT")


def build_ad_to_campaign_map(engine: Engine, customer_id: str) -> Dict[str, str]:
    sql = """
    SELECT da.ad_id::text AS ad_id,
           COALESCE(dag.campaign_id::text, '') AS campaign_id
    FROM dim_ad da
    LEFT JOIN dim_adgroup dag
      ON da.customer_id::text = dag.customer_id::text
     AND da.adgroup_id::text = dag.adgroup_id::text
    WHERE da.customer_id::text = :cid
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id)}).fetchall()
        return {str(r[0]).strip(): str(r[1]).strip() for r in rows if str(r[0]).strip()}
    except Exception:
        return {}


def replace_device_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date, pk_name: str):
    pk_cols = ["dt", "customer_id", pk_name, "device_name"]
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt"), {"cid": str(customer_id), "dt": d1})
            break
        except Exception:
            time.sleep(2)
    if not rows:
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = (
        f"ON CONFLICT ({pk_str}) DO UPDATE SET " + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f"ON CONFLICT ({pk_str}) DO NOTHING"
    )
    sql = f"INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))

    for attempt in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            break
        except Exception:
            if raw_conn:
                try:
                    raw_conn.rollback()
                except Exception:
                    pass
            time.sleep(2)
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if raw_conn:
                try:
                    raw_conn.close()
                except Exception:
                    pass


def _looks_like_ad_id(v: Any) -> bool:
    s = str(v or "").strip().lower()
    if not s or s == "-":
        return False
    return s.startswith("nad-") or s.startswith("ad-") or s.startswith("nad")


def _detect_column_by_values(data_df: pd.DataFrame, checker, exclude: set[int] | None = None, min_hits: int = 2) -> int:
    exclude = exclude or set()
    if data_df is None or data_df.empty:
        return -1
    best_idx = -1
    best_hits = 0
    probe = data_df.head(300)
    for idx in range(probe.shape[1]):
        if idx in exclude:
            continue
        hits = 0
        seen = 0
        for v in probe.iloc[:, idx].tolist():
            s = str(v or "").strip()
            if not s or s == "-":
                continue
            seen += 1
            if checker(v):
                hits += 1
        if hits > best_hits and hits >= min_hits:
            best_idx = idx
            best_hits = hits
    return best_idx


def _find_header_idx(df: pd.DataFrame) -> int:
    pk_needles = [_normalize_header(x) for x in ["광고id", "소재id", "adid"]]
    metric_needles = [_normalize_header(x) for x in [
        "노출수", "impressions", "impcnt", "클릭수", "clicks", "clkcnt",
        "총비용", "cost", "salesamt", "전환수", "conversions", "ccnt"
    ]]
    for i in range(min(120, len(df))):
        row_vals = [_normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if not any(row_vals):
            continue
        has_pk = any(x in row_vals for x in pk_needles)
        has_metric = any(x in row_vals for x in metric_needles)
        if has_pk and has_metric:
            return i
        if has_pk and any("campaignid" == x or "광고그룹id" == x or "adgroupid" == x for x in row_vals):
            return i
        if has_pk and any("date" == x or "일자" == x for x in row_vals):
            return i
    return -1


def parse_ad_device_report(
    df: pd.DataFrame,
    ad_to_campaign: Dict[str, str] | None = None,
) -> Tuple[Dict[Tuple[str, str], dict], Dict[Tuple[str, str], dict], dict]:
    if df is None or df.empty:
        return {}, {}, {"status": "empty"}

    ad_to_campaign = ad_to_campaign or {}
    header_idx = _find_header_idx(df)

    if header_idx == -1:
        return {}, {}, {"status": "no_header"}

    raw_headers = [str(x) for x in df.iloc[header_idx].fillna("")]
    headers = [_normalize_header(x) for x in raw_headers]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)

    ad_idx = _get_col_idx(headers, ["광고id", "소재id", "adid"])
    camp_idx = _get_col_idx(headers, ["캠페인id", "campaignid"])
    device_idx = _get_col_idx(headers, DEVICE_HEADER_CANDIDATES)
    imp_idx = _get_col_idx(headers, ["노출수", "impressions", "impcnt"])
    clk_idx = _get_col_idx(headers, ["클릭수", "clicks", "clkcnt"])
    cost_idx = _get_col_idx(headers, ["총비용", "cost", "salesamt"])
    conv_idx = _get_col_idx(headers, ["전환수", "conversions", "ccnt"])
    sales_idx = _get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"])
    rank_idx = _get_col_idx(headers, ["평균노출순위", "averageposition", "avgrnk"])

    if ad_idx == -1:
        ad_idx = _detect_column_by_values(data_df, _looks_like_ad_id, min_hits=2)
    if device_idx == -1:
        exclude = {x for x in [ad_idx, camp_idx, imp_idx, clk_idx, cost_idx, conv_idx, sales_idx, rank_idx] if x != -1}
        device_idx = _detect_column_by_values(data_df, lambda v: bool(normalize_device_name(v)), exclude=exclude, min_hits=2)

    if ad_idx == -1 or device_idx == -1:
        sample_headers = [h for h in raw_headers if str(h).strip()][:12]
        return {}, {}, {
            "status": "missing_required_columns",
            "ad_idx": ad_idx,
            "device_idx": device_idx,
            "header_idx": header_idx,
            "sample_headers": sample_headers,
        }

    ad_stats: Dict[Tuple[str, str], dict] = {}
    for _, row in data_df.iterrows():
        if len(row) <= max(ad_idx, device_idx):
            continue
        ad_id = str(row.iloc[ad_idx]).strip()
        if not ad_id or ad_id == "-" or ad_id.lower() in {"adid", "광고id", "소재id"}:
            continue
        device_name = normalize_device_name(row.iloc[device_idx])
        if not device_name:
            continue

        imp = int(_safe_float(row.iloc[imp_idx])) if imp_idx != -1 and len(row) > imp_idx else 0
        clk = int(_safe_float(row.iloc[clk_idx])) if clk_idx != -1 and len(row) > clk_idx else 0
        cost = int(_safe_float(row.iloc[cost_idx])) if cost_idx != -1 and len(row) > cost_idx else 0
        conv = _safe_float(row.iloc[conv_idx]) if conv_idx != -1 and len(row) > conv_idx else 0.0
        sales = int(_safe_float(row.iloc[sales_idx])) if sales_idx != -1 and len(row) > sales_idx else 0
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            continue

        key = (ad_id, device_name)
        bucket = ad_stats.setdefault(key, {
            "campaign_id": "",
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "rank_sum": 0.0,
            "rank_cnt": 0,
        })
        campaign_id = str(row.iloc[camp_idx]).strip() if camp_idx != -1 and len(row) > camp_idx else ""
        bucket["campaign_id"] = bucket.get("campaign_id") or campaign_id or ad_to_campaign.get(ad_id, "")
        bucket["imp"] += imp
        bucket["clk"] += clk
        bucket["cost"] += cost
        bucket["conv"] += conv
        bucket["sales"] += sales

        if rank_idx != -1 and len(row) > rank_idx:
            rnk = _safe_float(row.iloc[rank_idx])
            if rnk > 0 and imp > 0:
                bucket["rank_sum"] += (rnk * imp)
                bucket["rank_cnt"] += imp

    if not ad_stats:
        return {}, {}, {"status": "no_rows"}

    campaign_stats: Dict[Tuple[str, str], dict] = {}
    missing_campaign_count = 0
    for (ad_id, device_name), bucket in ad_stats.items():
        campaign_id = str(bucket.get("campaign_id") or ad_to_campaign.get(ad_id, "")).strip()
        if not campaign_id:
            missing_campaign_count += 1
            continue
        key = (campaign_id, device_name)
        cb = campaign_stats.setdefault(key, {
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "rank_sum": 0.0,
            "rank_cnt": 0,
        })
        cb["imp"] += int(bucket.get("imp", 0) or 0)
        cb["clk"] += int(bucket.get("clk", 0) or 0)
        cb["cost"] += int(bucket.get("cost", 0) or 0)
        cb["conv"] += float(bucket.get("conv", 0.0) or 0.0)
        cb["sales"] += int(bucket.get("sales", 0) or 0)
        cb["rank_sum"] += float(bucket.get("rank_sum", 0.0) or 0.0)
        cb["rank_cnt"] += int(bucket.get("rank_cnt", 0) or 0)

    return ad_stats, campaign_stats, {
        "status": "ok",
        "ad_rows": len(ad_stats),
        "campaign_rows": len(campaign_stats),
        "missing_campaign_rows": missing_campaign_count,
    }


def save_device_stats(
    engine: Engine,
    customer_id: str,
    target_date: date,
    table_name: str,
    pk_name: str,
    stat_res: Dict[Tuple[str, str], dict],
    data_source: str = "report_device_total_only",
    source_report: str = "AD",
) -> int:
    if not stat_res:
        return 0

    rows = []
    for (entity_id, device_name), s in stat_res.items():
        if not entity_id or not device_name:
            continue
        cost = int(s.get("cost", 0) or 0)
        sales = int(s.get("sales", 0) or 0)
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (float(s.get("rank_sum", 0.0) or 0.0) / int(s.get("rank_cnt", 1) or 1)) if int(s.get("rank_cnt", 0) or 0) > 0 else 0.0
        rows.append({
            "dt": target_date,
            "customer_id": str(customer_id),
            pk_name: str(entity_id),
            "device_name": str(device_name),
            "imp": int(s.get("imp", 0) or 0),
            "clk": int(s.get("clk", 0) or 0),
            "cost": cost,
            "conv": float(s.get("conv", 0.0) or 0.0),
            "sales": sales,
            "roas": roas,
            "avg_rnk": round(avg_rnk, 2),
            "data_source": data_source,
            "source_report": source_report,
        })

    replace_device_fact_range(engine, table_name, rows, customer_id, target_date, pk_name)
    return len(rows)


def summarize_stat_res(stat_res: Dict[Tuple[str, str], dict]) -> dict:
    out = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0}
    for s in (stat_res or {}).values():
        out["imp"] += int(s.get("imp", 0) or 0)
        out["clk"] += int(s.get("clk", 0) or 0)
        out["cost"] += int(s.get("cost", 0) or 0)
        out["conv"] += float(s.get("conv", 0.0) or 0.0)
        out["sales"] += int(s.get("sales", 0) or 0)
    return out
