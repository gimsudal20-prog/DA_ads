# -*- coding: utf-8 -*-
"""data.py - DB access + cached queries + shared formatting helpers.

This file intentionally contains:
- DB engine + SQL helpers
- Cached query functions (campaign/keyword/ad/budget)
- Shared format helpers (used by UI/pages)

(승훈 요청) 파일 분리: CSS -> styles.py, UI -> ui.py, Pages/router -> pages.py.
"""

from __future__ import annotations

import os
import re
import io
import time
import math
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()

# Streamlit cache hashing (Engine)
_HASH_FUNCS = {Engine: lambda e: e.url.render_as_string(hide_password=True)}

# -----------------------------
# Campaign type label
# -----------------------------
_CAMPAIGN_TP_LABEL = {
    "web_site": "파워링크",
    "website": "파워링크",
    "power_link": "파워링크",
    "shopping": "쇼핑검색",
    "shopping_search": "쇼핑검색",
    "power_content": "파워콘텐츠",
    "power_contents": "파워콘텐츠",
    "powercontent": "파워콘텐츠",
    "place": "플레이스",
    "place_search": "플레이스",
    "brand_search": "브랜드검색",
    "brandsearch": "브랜드검색",
}
_LABEL_TO_TP_KEYS: Dict[str, List[str]] = {}
for k, v in _CAMPAIGN_TP_LABEL.items():
    _LABEL_TO_TP_KEYS.setdefault(v, []).append(k)

# -----------------------------
# Accounts / Meta
# -----------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        try:
            db_url = str(st.secrets.get("DATABASE_URL", "")).strip()
        except Exception:
            db_url = ""

    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. (.env env var or Streamlit secrets)")

    if "sslmode=" not in db_url:
        joiner = "&" if "?" in db_url else "?"
        db_url = db_url + f"{joiner}sslmode=require"

    return db_url


@st.cache_resource(show_spinner=False)
def get_engine():
    url = get_database_url()
    connect_args = {
        "sslmode": "require",
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }

    use_nullpool = False

    if use_nullpool:
        return create_engine(url, connect_args=connect_args, poolclass=NullPool, future=True)

    return create_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=300,
        pool_use_lifo=True,
        future=True,
    )

def _reset_engine_cache() -> None:
    try:
        get_engine.clear()  # type: ignore[attr-defined]
    except Exception:
        pass

def sql_read(engine, sql: str, params: Optional[dict] = None, retries: int = 2) -> pd.DataFrame:
    last_err: Exception | None = None
    _engine = engine

    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except Exception as e:
            last_err = e
            try:
                _engine.dispose()
            except Exception:
                pass

            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine

            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err

def sql_exec(engine, sql: str, params: Optional[dict] = None, retries: int = 1) -> None:
    last_err = None
    for i in range(retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), params or {})
            return
        except Exception as e:
            last_err = e
            try:
                engine.dispose()
            except Exception:
                pass
            if i < retries:
                time.sleep(0.25 * (2 ** i))
                continue
            raise last_err

def db_ping(engine, retries: int = 2) -> None:
    last_err: Exception | None = None
    _engine = engine
    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            try:
                _engine.dispose()
            except Exception:
                pass
            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine
            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err

def _get_table_names_cached(engine, schema: str = "public") -> set:
    cache = st.session_state.setdefault("_table_names_cache", {})
    if schema in cache:
        return cache[schema]
    try:
        insp = inspect(engine)
        names = set(insp.get_table_names(schema=schema))
    except Exception:
        names = set()
    cache[schema] = names
    return names

def table_exists(engine, table: str, schema: str = "public") -> bool:
    return table in _get_table_names_cached(engine, schema=schema)

def get_table_columns(engine, table: str, schema: str = "public") -> set:
    cache = st.session_state.setdefault("_table_cols_cache", {})
    key = f"{schema}.{table}"
    if key in cache:
        return cache[key]
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        out = {str(c.get("name", "")).lower() for c in cols}
    except Exception:
        out = set()
    cache[key] = out
    return out

def _sql_in_str_list(values: List[int]) -> str:
    safe = []
    for v in values:
        try:
            safe.append(f"'{int(v)}'")
        except Exception:
            continue
    return ",".join(safe) if safe else "''"

def _sql_in_text_list(values: List[str]) -> str:
    safe: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        s = s.replace("'", "''")
        safe.append(f"'{s}'")
    return ",".join(safe) if safe else "''"

# -----------------------------
# Download helpers (cached)
# -----------------------------
@st.cache_data(hash_funcs=_HASH_FUNCS, show_spinner=False)
def _fact_has_sales(engine, fact_table: str) -> bool:
    return "sales" in get_table_columns(engine, fact_table)

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=180, show_spinner=False)
def query_budget_bundle(
    _engine,
    cids: Tuple[int, ...],
    yesterday: date,
    avg_d1: date,
    avg_d2: date,
    month_d1: date,
    month_d2: date,
    avg_days: int,
) -> pd.DataFrame:
    if not (
        table_exists(_engine, "dim_account_meta")
        and table_exists(_engine, "fact_campaign_daily")
        and table_exists(_engine, "fact_bizmoney_daily")
    ):
        return pd.DataFrame()

    where_cid = ""
    if cids:
        where_cid = f"WHERE m.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    WITH meta AS (
      SELECT customer_id::text AS customer_id, account_name, manager, COALESCE(monthly_budget,0) AS monthly_budget
      FROM dim_account_meta m
      {where_cid}
    ),
    biz AS (
      SELECT DISTINCT ON (customer_id::text)
        customer_id::text AS customer_id,
        bizmoney_balance,
        dt AS last_update
      FROM fact_bizmoney_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
      ORDER BY customer_id::text, dt DESC
    ),
    camp AS (
      SELECT
        customer_id::text AS customer_id,
        SUM(cost) FILTER (WHERE dt = :y) AS y_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :a1 AND :a2) AS avg_sum_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :m1 AND :m2) AS month_cost
      FROM fact_campaign_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
        AND dt BETWEEN :min_dt AND :max_dt
      GROUP BY customer_id::text
    )
    SELECT
      meta.customer_id,
      meta.account_name,
      meta.manager,
      meta.monthly_budget,
      COALESCE(biz.bizmoney_balance,0) AS bizmoney_balance,
      biz.last_update,
      COALESCE(camp.y_cost,0) AS y_cost,
      COALESCE(camp.avg_sum_cost,0) AS avg_sum_cost,
      COALESCE(camp.month_cost,0) AS current_month_cost
    FROM meta
    LEFT JOIN biz ON meta.customer_id = biz.customer_id
    LEFT JOIN camp ON meta.customer_id = camp.customer_id
    ORDER BY meta.account_name
    """

    min_dt = min(yesterday, avg_d1, month_d1)
    max_dt = max(yesterday, avg_d2, month_d2)

    df = sql_read(
        _engine,
        sql,
        {
            "y": str(yesterday),
            "a1": str(avg_d1),
            "a2": str(avg_d2),
            "m1": str(month_d1),
            "m2": str(month_d2),
            "min_dt": str(min_dt),
            "max_dt": str(max_dt),
        },
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["monthly_budget", "bizmoney_balance", "y_cost", "avg_sum_cost", "current_month_cost"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    df["avg_cost"] = df["avg_sum_cost"].astype(float) / float(max(avg_days, 1))
    return df

def _safe_int(x, default: int = 0) -> int:
    try:
        if pd.isna(x) or x == "":
            return default
        return int(float(x))
    except Exception:
        return default

def format_currency(val) -> str:
    return f"{_safe_int(val):,}원"

def format_number_commas(val) -> str:
    return f"{_safe_int(val):,}"

def format_roas(val) -> str:
    try:
        if pd.isna(val):
            return "-"
        return f"{float(val):.0f}%"
    except Exception:
        return "-"

def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")

    def _fmt(x):
        if pd.isna(x):
            return ""
        if float(x) == 0.0:
            return "0%"
        return f"{float(x):.1f}%"

    out[col] = s.map(_fmt)
    return out


# -----------------------------
# Campaign summary rows (Naver-like)
# -----------------------------

def finalize_display_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()

    def _num(s):
        try:
            return pd.to_numeric(s, errors="coerce")
        except Exception:
            return pd.Series([None] * len(out))

    if "노출" in out.columns and "클릭" in out.columns and "CTR(%)" not in out.columns:
        imp = _num(out["노출"])
        clk = _num(out["클릭"])
        out["CTR(%)"] = _safe_div(clk, imp) * 100.0

    if "광고비" in out.columns and "클릭" in out.columns and "CPC(원)" not in out.columns:
        cost = _num(out["광고비"])
        clk = _num(out["클릭"])
        out["CPC(원)"] = _safe_div(cost, clk)

    if "광고비" in out.columns and "전환" in out.columns and "CPA(원)" not in out.columns:
        cost = _num(out["광고비"])
        conv = _num(out["전환"])
        out["CPA(원)"] = _safe_div(cost, conv)

    if "매출" in out.columns and "광고비" in out.columns and "ROAS(%)" not in out.columns and "ROAS" not in out.columns:
        sales = _num(out["매출"])
        cost = _num(out["광고비"])
        out["ROAS(%)"] = _safe_div(sales, cost) * 100.0

    for col in ["노출", "클릭", "전환"]:
        if col in out.columns:
            if pd.api.types.is_numeric_dtype(out[col]):
                out[col] = out[col].fillna(0).round(0)
            out[col] = out[col].apply(format_number_commas)

    for col in ["광고비", "매출", "CPC(원)", "CPA(원)"]:
        if col in out.columns:
            if pd.api.types.is_numeric_dtype(out[col]) is False:
                out[col] = pd.to_numeric(out[col], errors="coerce")
            out[col] = out[col].apply(format_currency)

    if "CTR(%)" in out.columns:
        out = finalize_ctr_col(out, "CTR(%)")

    if "ROAS(%)" in out.columns:
        out["ROAS(%)"] = pd.to_numeric(out["ROAS(%)"], errors="coerce").apply(format_roas)
    if "ROAS" in out.columns:
        out["ROAS"] = pd.to_numeric(out["ROAS"], errors="coerce").apply(format_roas)

    return out

def build_campaign_summary_rows_from_numeric(
    df_numeric: pd.DataFrame,
    campaign_type_col: str = "campaign_type",
    campaign_name_col: str = "campaign_name",
) -> pd.DataFrame:
    if df_numeric is None or df_numeric.empty:
        return pd.DataFrame()

    x = df_numeric.copy()
    if campaign_type_col not in x.columns:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c not in x.columns:
            x[c] = 0
        x[c] = pd.to_numeric(x[c], errors="coerce").fillna(0)

    x[campaign_type_col] = x[campaign_type_col].fillna("").astype(str).str.strip()
    x = x[x[campaign_type_col] != ""].copy()
    if x.empty:
        return pd.DataFrame()

    if "campaign_id" in x.columns and "customer_id" in x.columns:
        x["_camp_key"] = x["customer_id"].astype(str) + ":" + x["campaign_id"].astype(str)
    else:
        x["_camp_key"] = x.get(campaign_name_col, "").astype(str)

    def _make_row(label_type: str, g: pd.DataFrame) -> dict:
        n = int(g["_camp_key"].nunique())
        imp = float(g["imp"].sum())
        clk = float(g["clk"].sum())
        cost = float(g["cost"].sum())
        conv = float(g["conv"].sum())
        sales = float(g["sales"].sum()) if "sales" in g.columns else 0.0

        ctr = (clk / imp * 100.0) if imp > 0 else 0.0
        cpc = (cost / clk) if clk > 0 else 0.0
        cpa = (cost / conv) if conv > 0 else 0.0
        roas = (sales / cost * 100.0) if cost > 0 else 0.0

        return {
            "업체명": "",
            "담당자": "",
            "광고유형": label_type,
            "캠페인": f"캠페인 {n}개 결과",
            "노출": int(imp),
            "클릭": int(clk),
            "CTR(%)": float(ctr),
            "CPC": format_currency(cpc),
            "광고비": format_currency(cost),
            "전환": int(conv),
            "CPA": format_currency(cpa),
            "전환매출": format_currency(sales),
            "ROAS(%)": format_roas(roas),
        }

    rows = []
    rows.append(_make_row("종합", x))

    for tp, g in x.groupby(campaign_type_col, dropna=False):
        tp = str(tp).strip() or "기타"
        rows.append(_make_row(tp, g))

    out = pd.DataFrame(rows)
    out["CTR(%)"] = pd.to_numeric(out["CTR(%)"], errors="coerce").fillna(0).astype(float)
    out = finalize_ctr_col(out, "CTR(%)")

    cols = ["업체명", "담당자", "광고유형", "캠페인", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    return out[cols].copy()

def parse_currency(val_str) -> int:
    if pd.isna(val_str):
        return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0

def campaign_tp_to_label(tp: str) -> str:
    t = (tp or "").strip()
    if not t:
        return ""
    key = t.lower()
    return _CAMPAIGN_TP_LABEL.get(key, t)

def label_to_tp_keys(labels: Tuple[str, ...]) -> List[str]:
    keys: List[str] = []
    for lab in labels:
        keys.extend(_LABEL_TO_TP_KEYS.get(str(lab), []))
    out = []
    seen = set()
    for x in keys:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df["campaign_tp"] = df.get("campaign_tp", "").fillna("")
    df["campaign_type_label"] = df["campaign_tp"].astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []
    raw = dim_campaign.get("campaign_tp", pd.Series([], dtype=str))
    present = set()
    for x in raw.dropna().astype(str).tolist():
        lab = campaign_tp_to_label(x)
        lab = str(lab).strip()
        if lab and lab not in ("미분류", "종합", "기타"):
            present.add(lab)
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    opts = [x for x in order if x in present]
    extra = sorted([x for x in present if x not in set(order)])
    return opts + extra

def normalize_accounts_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    def find_col(cands: List[str]) -> Optional[str]:
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                cc = cand.lower().replace(" ", "").replace("_", "")
                if lc == cc:
                    return c
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                if cand in lc:
                    return c
        return None

    cid_col = find_col(["customer_id", "customerid", "커스텀id", "커스텀 id", "커스텀ID"])
    name_col = find_col(["account_name", "accountname", "업체명", "업체"])
    mgr_col = find_col(["manager", "담당자", "담당"])

    if not cid_col or not name_col:
        raise ValueError(f"accounts.xlsx is missing columns. Available: {list(df.columns)}")

    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(df[cid_col], errors="coerce").astype("Int64")
    out["account_name"] = df[name_col].astype(str).str.strip()
    out["manager"] = df[mgr_col].astype(str).str.strip() if mgr_col else ""
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    out["manager"] = out["manager"].fillna("").astype(str)
    out = out.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)
    return out

def ensure_meta_table(engine) -> None:
    sql_exec(
        engine,
        """CREATE TABLE IF NOT EXISTS dim_account_meta (
          customer_id BIGINT PRIMARY KEY,
          account_name TEXT NOT NULL,
          manager TEXT DEFAULT '',
          monthly_budget BIGINT DEFAULT 0,
          updated_at TIMESTAMPTZ DEFAULT now()
        );""",
    )

def seed_from_accounts_xlsx(engine, df: Optional[pd.DataFrame] = None) -> Dict[str, int]:
    ensure_meta_table(engine)

    if df is None:
        if not os.path.exists(ACCOUNTS_XLSX):
            return {"meta": 0}
        df = pd.read_excel(ACCOUNTS_XLSX)

    acc = normalize_accounts_columns(df)

    upsert_meta = """
    INSERT INTO dim_account_meta (customer_id, account_name, manager, updated_at)
    VALUES (:customer_id, :account_name, :manager, now())
    ON CONFLICT (customer_id) DO UPDATE SET
      account_name = EXCLUDED.account_name,
      manager = EXCLUDED.manager,
      updated_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_meta), acc.to_dict(orient="records"))

    return {"meta": int(len(acc))}


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=600, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_account_meta"):
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df = sql_read(
        _engine,
        """
        SELECT customer_id, account_name, manager, monthly_budget, updated_at
        FROM dim_account_meta
        ORDER BY account_name
        """,
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    df["manager"] = df.get("manager", "").fillna("").astype(str).str.strip()
    df["account_name"] = df.get("account_name", "").fillna("").astype(str).str.strip()
    return df

def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
    if not table_exists(engine, "dim_account_meta"):
        return
    sql_exec(
        engine,
        """
        UPDATE dim_account_meta
        SET monthly_budget = :b, updated_at = now()
        WHERE customer_id = :cid
        """,
        {"b": int(monthly_budget), "cid": int(customer_id)},
    )

# -----------------------------
# Data freshness (single query)
# -----------------------------
@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=600, show_spinner=False)
def query_latest_dates(_engine) -> Dict[str, str]:
    tables = ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily"]
    out: Dict[str, str] = {}
    for t in tables:
        try:
            df = sql_read(_engine, f"SELECT MAX(dt) AS mx FROM {t}")
            mx = df.iloc[0, 0] if (df is not None and not df.empty) else None
            out[str(t)] = str(mx)[:10] if mx is not None else "-"
        except Exception:
            continue
    return out

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=60, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    parts = []
    params = {}
    def _add(label: str, table: str):
        if table_exists(_engine, table):
            parts.append(f"SELECT '{label}' AS k, MAX(dt) AS dt FROM {table}")
    _add("campaign", "fact_campaign_daily")
    _add("keyword", "fact_keyword_daily")
    _add("ad", "fact_ad_daily")
    _add("bizmoney", "fact_bizmoney_daily")

    if not parts:
        return {"campaign": None, "keyword": None, "ad": None, "bizmoney": None}

    sql = " UNION ALL ".join(parts)
    df = sql_read(_engine, sql, params)
    out = {"campaign": None, "keyword": None, "ad": None, "bizmoney": None}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        k = str(r.get("k"))
        out[k] = r.get("dt")
    return out

def query_campaign_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          c.campaign_name,
          c.campaign_tp
        FROM top t
        JOIN c_f c
          ON t.customer_id = c.customer_id
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM top t
        LEFT JOIN dim_campaign c
          ON t.customer_id = c.customer_id::text
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")

    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"

    return df.reset_index(drop=True)


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          c.campaign_name,
          c.campaign_tp
        FROM picked p
        JOIN c_f c
          ON p.customer_id = c.customer_id
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM picked p
        LEFT JOIN dim_campaign c
          ON p.customer_id = c.customer_id::text
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"

    return df.reset_index(drop=True)

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_daily_slice(_engine, d1: date, d2: date) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily") or not table_exists(_engine, "dim_account_meta"):
        return pd.DataFrame(columns=["dt","customer_id","account_name","manager","campaign_id","campaign_name","campaign_tp","campaign_type","imp","clk","cost","conv","sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    join_campaign = ""
    select_campaign = "''::text AS campaign_name, ''::text AS campaign_tp"
    group_campaign = ""
    if table_exists(_engine, "dim_campaign"):
        join_campaign = "LEFT JOIN dim_campaign c ON c.customer_id::text = f.customer_id::text AND c.campaign_id = f.campaign_id"
        select_campaign = "COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name, COALESCE(NULLIF(c.campaign_tp,''),'') AS campaign_tp"
        group_campaign = ", c.campaign_name, c.campaign_tp"

    sql = f"""
    SELECT
      f.dt::date AS dt,
      f.customer_id::text AS customer_id,
      COALESCE(NULLIF(m.account_name,''),'') AS account_name,
      COALESCE(NULLIF(m.manager,''),'') AS manager,
      f.campaign_id,
      {select_campaign},
      SUM(f.imp)  AS imp,
      SUM(f.clk)  AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      {sales_expr} AS sales
    FROM fact_campaign_daily f
    JOIN dim_account_meta m
      ON m.customer_id = f.customer_id::text
    {join_campaign}
    WHERE f.dt BETWEEN :d1 AND :d2
    GROUP BY f.dt::date, f.customer_id::text, m.account_name, m.manager, f.campaign_id{group_campaign}
    ORDER BY f.dt::date
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt","customer_id","account_name","manager","campaign_id","campaign_name","campaign_tp","campaign_type","imp","clk","cost","conv","sales"])

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["imp","clk","cost","conv","sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["campaign_tp"] = df.get("campaign_tp", "").astype(str)
    df["campaign_type"] = df["campaign_tp"].map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    return df.reset_index(drop=True)


# -----------------------------
# Timeseries Queries (for charts)
# -----------------------------

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        JOIN c_f c
          ON f.customer_id::text = c.customer_id
         AND f.campaign_id = c.campaign_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_one_timeseries(_engine, d1: date, d2: date, customer_id: int, campaign_id: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    sql = f"""
    SELECT
      f.dt::date AS dt,
      SUM(f.imp)  AS imp,
      SUM(f.clk)  AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      {sales_expr} AS sales
    FROM fact_campaign_daily f
    WHERE f.dt BETWEEN :d1 AND :d2
      AND f.customer_id::text = :cid
      AND f.campaign_id = :camp_id
    GROUP BY f.dt::date
    ORDER BY f.dt::date
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "cid": str(customer_id), "camp_id": int(campaign_id)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_ad_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_ad"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        a_f AS (
          SELECT a.customer_id::text AS customer_id, a.ad_id::text AS ad_id
          FROM dim_ad a
          JOIN g_f g ON a.customer_id::text = g.customer_id AND a.adgroup_id::text = g.adgroup_id
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        JOIN a_f a ON f.customer_id::text = a.customer_id AND f.ad_id::text = a.ad_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_keyword_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_keyword"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        k_f AS (
          SELECT k.customer_id::text AS customer_id, k.keyword_id::text AS keyword_id
          FROM dim_keyword k
          JOIN g_f g ON k.customer_id::text = g.customer_id AND k.adgroup_id::text = g.adgroup_id
        )
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        JOIN k_f k ON fk.customer_id::text = k.customer_id AND fk.keyword_id::text = k.keyword_id
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


# -----------------------------
# Altair Charts (rounded / smooth)
# -----------------------------

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_ad_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()

    fad_cols = get_table_columns(_engine, "fact_ad_daily")
    has_sales = "sales" in fad_cols
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    has_ag = "adgroup_id" in fad_cols
    has_cp = "campaign_id" in fad_cols
    ag_expr = "MIN(f.adgroup_id::text)" if has_ag else "NULL::text"
    cp_expr = "MIN(f.campaign_id::text)" if has_cp else "NULL::text"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    tp_in = _sql_in_text_list([str(x).lower() for x in tp_keys])

    dim_ad_exists = table_exists(_engine, "dim_ad")
    dim_ag_exists = table_exists(_engine, "dim_adgroup")
    dim_cp_exists = table_exists(_engine, "dim_campaign")

    ad_cols = get_table_columns(_engine, "dim_ad") if dim_ad_exists else set()
    ad_text_expr = "COALESCE(NULLIF(TRIM(a.creative_text),''), NULLIF(TRIM(a.ad_name),''), '')" if "creative_text" in ad_cols else "COALESCE(NULLIF(TRIM(a.ad_name),''), '')"

    cp_cols = get_table_columns(_engine, "dim_campaign") if dim_cp_exists else set()
    cp_tp_col = "campaign_tp" if "campaign_tp" in cp_cols else ("campaign_type" if "campaign_type" in cp_cols else None)
    cp_name_col = "campaign_name" if "campaign_name" in cp_cols else ("name" if "name" in cp_cols else None)
    
    ag_cols = get_table_columns(_engine, "dim_adgroup") if dim_ag_exists else set()
    ag_name_col = "adgroup_name" if "adgroup_name" in ag_cols else ("name" if "name" in ag_cols else None)

    dim_ad_adgroup_expr = "a.adgroup_id::text" if (dim_ad_exists and "adgroup_id" in ad_cols) else "NULL::text"
    dim_ad_campaign_expr = "a.campaign_id::text" if (dim_ad_exists and "campaign_id" in ad_cols) else "NULL::text"
    dim_ag_campaign_expr = "g.campaign_id::text" if (dim_ag_exists and "campaign_id" in ag_cols) else "NULL::text"

    adgroup_join_key = f"COALESCE(NULLIF(p.f_adgroup_id, ''), NULLIF({dim_ad_adgroup_expr}, ''))"
    campaign_join_key = f"COALESCE(NULLIF(p.f_campaign_id, ''), NULLIF({dim_ad_campaign_expr}, ''), NULLIF({dim_ag_campaign_expr}, ''))"

    join_ad = "LEFT JOIN dim_ad a ON p.customer_id = a.customer_id::text AND p.ad_id = a.ad_id::text" if dim_ad_exists else "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS ad_id, NULL::text AS adgroup_id, NULL::text AS campaign_id, NULL::text AS ad_name, NULL::text AS creative_text) a ON 1=0"
    join_ag = f"LEFT JOIN dim_adgroup g ON p.customer_id = g.customer_id::text AND g.adgroup_id::text = {adgroup_join_key}" if dim_ag_exists else "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS adgroup_id, NULL::text AS campaign_id, NULL::text AS adgroup_name) g ON 1=0"
    join_cp = f"LEFT JOIN dim_campaign c ON p.customer_id = c.customer_id::text AND c.campaign_id::text = {campaign_join_key}" if dim_cp_exists else f"LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS campaign_id, NULL::text AS campaign_name, NULL::text AS campaign_tp) c ON 1=0"

    type_filter_clause = ""
    if tp_keys and dim_cp_exists and cp_tp_col:
        type_filter_clause = f"AND LOWER(COALESCE(NULLIF(TRIM(c.{cp_tp_col}), ''), '')) IN ({tp_in})"

    campaign_name_expr = f"COALESCE(NULLIF(TRIM(c.{cp_name_col}),''),'')" if cp_name_col else "''"
    campaign_tp_expr = f"COALESCE(NULLIF(TRIM(c.{cp_tp_col}),''),'')" if cp_tp_col else "''"
    adgroup_name_expr = f"COALESCE(NULLIF(TRIM(g.{ag_name_col}),''),'')" if ag_name_col else "''"

    sql = f"""
    WITH base AS (
      SELECT
        f.customer_id::text AS customer_id,
        f.ad_id::text AS ad_id,
        {ag_expr} AS f_adgroup_id,
        {cp_expr} AS f_campaign_id,
        SUM(COALESCE(f.imp,0))  AS imp,
        SUM(COALESCE(f.clk,0))  AS clk,
        SUM(COALESCE(f.cost,0)) AS cost,
        SUM(COALESCE(f.conv,0)) AS conv,
        {sales_expr} AS sales
      FROM fact_ad_daily f
      WHERE f.dt BETWEEN :d1 AND :d2
        {where_cid}
      GROUP BY f.customer_id::text, f.ad_id::text
    ),
    top AS (
      SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
    )
    SELECT
      p.customer_id,
      p.ad_id,
      p.imp, p.clk, p.cost, p.conv, p.sales,
      {ad_text_expr} AS ad_name,
      {adgroup_name_expr} AS adgroup_name,
      {campaign_name_expr} AS campaign_name,
      {campaign_tp_expr} AS campaign_tp
    FROM top p
    {join_ad}
    {join_ag}
    {join_cp}
    WHERE 1=1
      {type_filter_clause}
    ORDER BY p.cost DESC NULLS LAST
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"

    return df.reset_index(drop=True)


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_ad_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()

    fad_cols = get_table_columns(_engine, "fact_ad_daily")
    has_sales = "sales" in fad_cols
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    has_ag = "adgroup_id" in fad_cols
    has_cp = "campaign_id" in fad_cols
    ag_expr = "MIN(f.adgroup_id::text)" if has_ag else "NULL::text"
    cp_expr = "MIN(f.campaign_id::text)" if has_cp else "NULL::text"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    tp_in = _sql_in_text_list([str(x).lower() for x in tp_keys])

    dim_ad_exists = table_exists(_engine, "dim_ad")
    dim_ag_exists = table_exists(_engine, "dim_adgroup")
    dim_cp_exists = table_exists(_engine, "dim_campaign")

    ad_cols = get_table_columns(_engine, "dim_ad") if dim_ad_exists else set()
    ad_text_expr = "COALESCE(NULLIF(TRIM(a.creative_text),''), NULLIF(TRIM(a.ad_name),''), '')" if "creative_text" in ad_cols else "COALESCE(NULLIF(TRIM(a.ad_name),''), '')"

    cp_cols = get_table_columns(_engine, "dim_campaign") if dim_cp_exists else set()
    cp_tp_col = "campaign_tp" if "campaign_tp" in cp_cols else ("campaign_type" if "campaign_type" in cp_cols else None)
    cp_name_col = "campaign_name" if "campaign_name" in cp_cols else ("name" if "name" in cp_cols else None)
    
    ag_cols = get_table_columns(_engine, "dim_adgroup") if dim_ag_exists else set()
    ag_name_col = "adgroup_name" if "adgroup_name" in ag_cols else ("name" if "name" in ag_cols else None)

    dim_ad_adgroup_expr = "a.adgroup_id::text" if (dim_ad_exists and "adgroup_id" in ad_cols) else "NULL::text"
    dim_ad_campaign_expr = "a.campaign_id::text" if (dim_ad_exists and "campaign_id" in ad_cols) else "NULL::text"
    dim_ag_campaign_expr = "g.campaign_id::text" if (dim_ag_exists and "campaign_id" in ag_cols) else "NULL::text"

    adgroup_join_key = f"COALESCE(NULLIF(p.f_adgroup_id, ''), NULLIF({dim_ad_adgroup_expr}, ''))"
    campaign_join_key = f"COALESCE(NULLIF(p.f_campaign_id, ''), NULLIF({dim_ad_campaign_expr}, ''), NULLIF({dim_ag_campaign_expr}, ''))"

    join_ad = "LEFT JOIN dim_ad a ON p.customer_id = a.customer_id::text AND p.ad_id = a.ad_id::text" if dim_ad_exists else "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS ad_id, NULL::text AS adgroup_id, NULL::text AS campaign_id, NULL::text AS ad_name, NULL::text AS creative_text) a ON 1=0"
    join_ag = f"LEFT JOIN dim_adgroup g ON p.customer_id = g.customer_id::text AND g.adgroup_id::text = {adgroup_join_key}" if dim_ag_exists else "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS adgroup_id, NULL::text AS campaign_id, NULL::text AS adgroup_name) g ON 1=0"
    join_cp = f"LEFT JOIN dim_campaign c ON p.customer_id = c.customer_id::text AND c.campaign_id::text = {campaign_join_key}" if dim_cp_exists else f"LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS campaign_id, NULL::text AS campaign_name, NULL::text AS campaign_tp) c ON 1=0"

    type_filter_clause = ""
    if tp_keys and dim_cp_exists and cp_tp_col:
        type_filter_clause = f"AND LOWER(COALESCE(NULLIF(TRIM(c.{cp_tp_col}), ''), '')) IN ({tp_in})"

    campaign_name_expr = f"COALESCE(NULLIF(TRIM(c.{cp_name_col}),''),'')" if cp_name_col else "''"
    campaign_tp_expr = f"COALESCE(NULLIF(TRIM(c.{cp_tp_col}),''),'')" if cp_tp_col else "''"
    adgroup_name_expr = f"COALESCE(NULLIF(TRIM(g.{ag_name_col}),''),'')" if ag_name_col else "''"

    sql = f"""
    WITH base AS (
      SELECT
        f.customer_id::text AS customer_id,
        f.ad_id::text AS ad_id,
        {ag_expr} AS f_adgroup_id,
        {cp_expr} AS f_campaign_id,
        SUM(COALESCE(f.imp,0))  AS imp,
        SUM(COALESCE(f.clk,0))  AS clk,
        SUM(COALESCE(f.cost,0)) AS cost,
        SUM(COALESCE(f.conv,0)) AS conv,
        {sales_expr} AS sales
      FROM fact_ad_daily f
      WHERE f.dt BETWEEN :d1 AND :d2
        {where_cid}
      GROUP BY f.customer_id::text, f.ad_id::text
    ),
    cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
    clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
    conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
    picked AS (
      SELECT * FROM cost_top
      UNION
      SELECT * FROM clk_top
      UNION
      SELECT * FROM conv_top
    )
    SELECT
      p.customer_id,
      p.ad_id,
      p.imp, p.clk, p.cost, p.conv, p.sales,
      {ad_text_expr} AS ad_name,
      {adgroup_name_expr} AS adgroup_name,
      {campaign_name_expr} AS campaign_name,
      {campaign_tp_expr} AS campaign_tp
    FROM picked p
    {join_ad}
    {join_ag}
    {join_cp}
    WHERE 1=1
      {type_filter_clause}
    ORDER BY p.cost DESC NULLS LAST
    """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"

    return df.reset_index(drop=True)

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_keyword_bundle(
    _engine,
    d1: date,
    d2: date,
    customer_ids: List[str],
    type_sel: Tuple[str, ...],
    topn_cost: int = 300,
) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    has_adgroup = "adgroup_id" in fk_cols
    has_campaign = "campaign_id" in fk_cols

    sales_sum = "SUM(COALESCE(fk.sales, 0)) AS sales" if "sales" in fk_cols else "0::numeric AS sales"

    kw_text_col = None
    for cand in ("keyword", "keyword_name", "kw", "query", "keyword_text"):
        if cand in fk_cols:
            kw_text_col = cand
            break
    kw_text_select = (
        f"MIN(NULLIF(TRIM(fk.{kw_text_col}), '')) AS keyword_text" if kw_text_col else "NULL::text AS keyword_text"
    )

    cid_clause = ""
    if customer_ids:
        cid_clause = f"AND fk.customer_id::text IN ({_sql_in_str_list(customer_ids)})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    tp_in = _sql_in_text_list([str(x).lower() for x in tp_keys])

    has_dim_keyword = table_exists(_engine, "dim_keyword")
    has_dim_adgroup = table_exists(_engine, "dim_adgroup")
    has_dim_campaign = table_exists(_engine, "dim_campaign")

    dim_kw_cols = get_table_columns(_engine, "dim_keyword") if has_dim_keyword else set()
    dim_ag_cols = get_table_columns(_engine, "dim_adgroup") if has_dim_adgroup else set()
    dim_cp_cols = get_table_columns(_engine, "dim_campaign") if has_dim_campaign else set()

    kw_name_col = None
    if has_dim_keyword:
        for cand in ("keyword_name", "keyword", "rel_keyword", "rel_keyword_name", "name"):
            if cand in dim_kw_cols:
                kw_name_col = cand
                break

    ag_name_col = "adgroup_name" if "adgroup_name" in dim_ag_cols else ("name" if "name" in dim_ag_cols else None)
    cp_name_col = "campaign_name" if "campaign_name" in dim_cp_cols else ("name" if "name" in dim_cp_cols else None)
    cp_tp_col = "campaign_tp" if "campaign_tp" in dim_cp_cols else ("campaign_type" if "campaign_type" in dim_cp_cols else None)

    if has_dim_campaign and cp_tp_col:
        case_lines = []
        for k, v in _CAMPAIGN_TP_LABEL.items():
            case_lines.append(f"WHEN LOWER(COALESCE(NULLIF(TRIM(c.{cp_tp_col}), ''), '')) = '{k}' THEN '{v}'")
        case_expr = "CASE " + " ".join(case_lines) + " ELSE '기타' END"
        campaign_tp_expr = f"COALESCE(NULLIF(TRIM(c.{cp_tp_col}), ''), '')"
    else:
        case_expr = "'기타'"
        campaign_tp_expr = "''"

    dim_kw_adgroup_expr = "k.adgroup_id::text" if (has_dim_keyword and "adgroup_id" in dim_kw_cols) else "NULL::text"
    dim_kw_campaign_expr = "k.campaign_id::text" if (has_dim_keyword and "campaign_id" in dim_kw_cols) else "NULL::text"

    adgroup_join_key = f"COALESCE(NULLIF(b.adgroup_id, ''), NULLIF({dim_kw_adgroup_expr}, ''))"
    campaign_join_key = f"COALESCE(NULLIF(b.campaign_id, ''), NULLIF({dim_kw_campaign_expr}, ''), g.campaign_id::text)"

    if has_dim_keyword and kw_name_col:
        keyword_expr = f"COALESCE(NULLIF(TRIM(k.{kw_name_col}), ''), NULLIF(TRIM(b.keyword_text), ''), b.keyword_id)"
    else:
        keyword_expr = f"COALESCE(NULLIF(TRIM(b.keyword_text), ''), b.keyword_id)"

    adgroup_name_expr = (
        f"COALESCE(NULLIF(TRIM(g.{ag_name_col}), ''), '')" if (has_dim_adgroup and ag_name_col) else "''"
    )
    campaign_name_expr = (
        f"COALESCE(NULLIF(TRIM(c.{cp_name_col}), ''), '')" if (has_dim_campaign and cp_name_col) else "''"
    )

    join_dim_keyword = ""
    if has_dim_keyword:
        join_dim_keyword = """
        LEFT JOIN dim_keyword k
          ON b.customer_id = k.customer_id::text
         AND b.keyword_id  = k.keyword_id::text
        """

    join_dim_adgroup = ""
    if has_dim_adgroup:
        join_dim_adgroup = f"""
        LEFT JOIN dim_adgroup g
          ON b.customer_id = g.customer_id::text
         AND g.adgroup_id::text = {adgroup_join_key}
        """
    else:
        join_dim_adgroup = "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS adgroup_id, NULL::text AS campaign_id, NULL::text AS adgroup_name) g ON 1=0"

    join_dim_campaign = ""
    if has_dim_campaign and cp_name_col and cp_tp_col:
        join_dim_campaign = f"""
        LEFT JOIN dim_campaign c
          ON b.customer_id = c.customer_id::text
         AND c.campaign_id::text = {campaign_join_key}
        """
    else:
        join_dim_campaign = "LEFT JOIN (SELECT NULL::text AS customer_id, NULL::text AS campaign_id, NULL::text AS campaign_name, NULL::text AS campaign_tp) c ON 1=0"

    type_filter_clause = ""
    if tp_keys and has_dim_campaign and cp_tp_col:
        type_filter_clause = f"AND LOWER(COALESCE(NULLIF(TRIM(scope.campaign_tp), ''), '')) IN ({tp_in})"

    sql = f"""
    WITH base AS (
      SELECT
        fk.customer_id::text AS customer_id,
        fk.keyword_id::text  AS keyword_id,
        {("MIN(fk.adgroup_id::text) AS adgroup_id," if has_adgroup else "NULL::text AS adgroup_id,")}
        {("MIN(fk.campaign_id::text) AS campaign_id," if has_campaign else "NULL::text AS campaign_id,")}
        {kw_text_select},
        SUM(COALESCE(fk.imp, 0))  AS imp,
        SUM(COALESCE(fk.clk, 0))  AS clk,
        SUM(COALESCE(fk.cost, 0)) AS cost,
        SUM(COALESCE(fk.conv, 0)) AS conv,
        {sales_sum}
      FROM fact_keyword_daily fk
      WHERE fk.dt BETWEEN :d1 AND :d2
        {cid_clause}
      GROUP BY fk.customer_id::text, fk.keyword_id::text
    ),
    top_cost0 AS (
      SELECT customer_id, keyword_id
      FROM base
      WHERE cost IS NOT NULL
      ORDER BY cost DESC
      LIMIT {int(topn_cost)}
    ),
    top_clk0 AS (
      SELECT customer_id, keyword_id
      FROM base
      WHERE clk IS NOT NULL
      ORDER BY clk DESC
      LIMIT {int(topn_cost)}
    ),
    top_conv0 AS (
      SELECT customer_id, keyword_id
      FROM base
      WHERE conv IS NOT NULL
      ORDER BY conv DESC
      LIMIT {int(topn_cost)}
    ),
    picked_ids AS (
      SELECT customer_id, keyword_id FROM top_cost0
      UNION
      SELECT customer_id, keyword_id FROM top_clk0
      UNION
      SELECT customer_id, keyword_id FROM top_conv0
    ),
    picked AS (
      SELECT
        i.customer_id,
        i.keyword_id,
        ROW_NUMBER() OVER (ORDER BY b.cost DESC NULLS LAST) AS rn_cost,
        ROW_NUMBER() OVER (ORDER BY b.clk  DESC NULLS LAST) AS rn_clk,
        ROW_NUMBER() OVER (ORDER BY b.conv DESC NULLS LAST) AS rn_conv
      FROM picked_ids i
      JOIN base b
        ON i.customer_id = b.customer_id
       AND i.keyword_id  = b.keyword_id
    ),
    scope AS (
      SELECT
        b.customer_id,
        b.keyword_id,
        {keyword_expr} AS keyword,
        {adgroup_name_expr} AS adgroup_name,
        {campaign_name_expr} AS campaign_name,
        {campaign_tp_expr} AS campaign_tp,
        {case_expr} AS campaign_type_label
      FROM base b
      {join_dim_keyword}
      {join_dim_adgroup}
      {join_dim_campaign}
    )
    SELECT
      b.customer_id,
      b.keyword_id,
      scope.keyword,
      scope.adgroup_name,
      scope.campaign_name,
      scope.campaign_tp,
      scope.campaign_type_label,
      b.imp, b.clk, b.cost, b.conv, b.sales,
      p.rn_cost, p.rn_clk, p.rn_conv
    FROM picked p
    JOIN base b
      ON p.customer_id = b.customer_id
     AND p.keyword_id  = b.keyword_id
    LEFT JOIN scope
      ON b.customer_id = scope.customer_id
     AND b.keyword_id  = scope.keyword_id
    WHERE 1=1
      {type_filter_clause}
    ORDER BY b.cost DESC NULLS LAST
    """

    df = sql_read(_engine, sql, params={"d1": d1, "d2": d2})
    if df is None:
        return pd.DataFrame()
    return df

def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    out["ctr"] = (out["clk"] / out["imp"].replace(0, np.nan)) * 100
    out["cpc"] = out["cost"] / out["clk"].replace(0, np.nan)
    out["cpa"] = out["cost"] / out["conv"].replace(0, np.nan)
    out["roas"] = (out["sales"] / out["cost"].replace(0, np.nan)) * 100

    return out

# -----------------------------
# Period comparison (DoD / WoW / MoM)
# -----------------------------

def _last_day_of_month(y: int, m: int) -> int:
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    return (nxt - timedelta(days=1)).day

def _shift_month(d: date, months: int) -> date:
    base = (d.year * 12) + (d.month - 1) + int(months)
    y = base // 12
    m = (base % 12) + 1
    day = min(int(d.day), _last_day_of_month(int(y), int(m)))
    return date(int(y), int(m), int(day))

def _period_compare_range(d1: date, d2: date, mode: str) -> Tuple[date, date]:
    mode = str(mode or "").strip()
    if mode == "전일대비":
        return d1 - timedelta(days=1), d2 - timedelta(days=1)
    if mode == "전주대비":
        return d1 - timedelta(days=7), d2 - timedelta(days=7)
    return _shift_month(d1, -1), _shift_month(d2, -1)

period_compare_range = _period_compare_range

def _safe_div(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return float(a) / float(b)
    except Exception:
        return 0.0

def _pct_change(curr: float, prev: float) -> Optional[float]:
    if prev == 0:
        return 0.0 if curr == 0 else None
    return (float(curr) - float(prev)) / float(prev) * 100.0

pct_change = _pct_change

def _pct_to_str(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        return f"{float(p):+.1f}%"
    except Exception:
        return "—"

def _pct_to_arrow(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        p = float(p)
        if p > 0:
            return f"▲ {abs(p):.1f}%"
        if p < 0:
            return f"▼ {abs(p):.1f}%"
        return f"• {abs(p):.1f}%"
    except Exception:
        return "—"

pct_to_arrow = _pct_to_arrow

def _fmt_point(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        return f"{float(p):+.1f}p"
    except Exception:
        return "—"

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> Dict[str, float]:
    entity = str(entity or "").lower().strip()
    try:
        if entity == "campaign":
            ts = query_campaign_timeseries(_engine, d1, d2, cids, type_sel)
        elif entity == "keyword":
            ts = query_keyword_timeseries(_engine, d1, d2, cids, type_sel)
        else:
            ts = query_ad_timeseries(_engine, d1, d2, cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is None or ts.empty:
        return {"imp": 0.0, "clk": 0.0, "cost": 0.0, "conv": 0.0, "sales": 0.0, "ctr": 0.0, "cpc": 0.0, "cpa": 0.0, "roas": 0.0}

    def _sum(col: str) -> float:
        if col not in ts.columns:
            return 0.0
        return float(pd.to_numeric(ts[col], errors="coerce").fillna(0).sum())

    imp = _sum("imp")
    clk = _sum("clk")
    cost = _sum("cost")
    conv = _sum("conv")
    sales = _sum("sales")
    ctr = _safe_div(clk, imp) * 100.0
    cpc = _safe_div(cost, clk)
    cpa = _safe_div(cost, conv)
    roas = _safe_div(sales, cost) * 100.0

    return {"imp": imp, "clk": clk, "cost": cost, "conv": conv, "sales": sales, "ctr": ctr, "cpc": cpc, "cpa": cpa, "roas": roas}
