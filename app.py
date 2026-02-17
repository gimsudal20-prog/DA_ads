
# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.2.0: 모바일 필터 고정 노출 + 키워드/소재 초고속 SQL TopN)
- 모바일에서 필터가 "안 보이는" 문제: 사이드바/익스팬더 의존 제거 → 본문 상단에 항상 노출 + 폼(적용 버튼) 방식
- 키워드/소재 속도 개선:
  * fact_*_daily 전체를 pandas로 다 들고오지 않고, DB에서 바로 집계(SUM) + TopN + 디멘션 JOIN 1번에 끝
  * 리스트 파라미터(IN/ANY/expanding)로 인한 ProgrammingError 회피: customer_id/type 필터는 안전한 "리터럴 IN (...)"로 구성
- 웹사이트 모드 UI: Streamlit 기본 크롬(햄버거/툴바/푸터 등) 숨김 CSS 유지
"""

import os
import re
import io
from datetime import date, timedelta
from typing import List, Optional, Dict, Tuple, Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import altair as alt
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# BUILD TAG (배포 확인용)
# -----------------------------
BUILD_TAG = "v7.2.2 (2026-02-17) - Fix TEXT vs INT IN-clause (customer_id) + restore budget/keyword queries"
# -----------------------------
# CONFIG / THRESHOLDS
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# -----------------------------
# PAGE CONFIG + GLOBAL CSS
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

GLOBAL_UI_CSS = """
<style>
  /* 웹사이트 모드: Streamlit 기본 크롬 숨김(환경에 따라 소유자에게는 일부 노출될 수 있음) */
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }
  div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
  div[data-testid="stDecoration"] { display: none; }
  div[data-testid="stStatusWidget"] { visibility: hidden; height: 0px; }

  h1,h2,h3 { letter-spacing: -0.2px; }
  div[data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; background: rgba(2, 132, 199, 0.06); }

  .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; margin-right:6px; }
  .b-red { background: rgba(239,68,68,0.12); color: rgb(185,28,28); }
  .b-yellow { background: rgba(234,179,8,0.16); color: rgb(161,98,7); }
  .b-green { background: rgba(34,197,94,0.12); color: rgb(21,128,61); }
  .b-gray { background: rgba(148,163,184,0.18); color: rgb(51,65,85); }

  /* 데이터프레임 index 숨김 (Streamlit 버전에 따라 적용 범위 다를 수 있음) */
  thead tr th:first-child { display:none }
  tbody th { display:none }

  /* 모바일에서 필터 영역이 위로 너무 붙지 않게 */
  .filter-wrap { padding: 8px 10px; border-radius: 14px; background: rgba(148,163,184,0.10); }
</style>
"""
st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)

# -----------------------------
# Download helpers
# -----------------------------
def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str = "data", key_prefix: str = "") -> None:
    if df is None or df.empty:
        return
    st.markdown(
        """
        <style>
        div[data-testid="stDownloadButton"] button {
            padding: 0.15rem 0.55rem !important;
            font-size: 0.80rem !important;
            line-height: 1.2 !important;
            min-height: 28px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns([1, 8])
    with c1:
        st.download_button(
            "XLSX",
            data=df_to_xlsx_bytes(df, sheet_name=sheet_name),
            file_name=f"{filename_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            use_container_width=True,
        )
    with c2:
        st.caption("다운로드")

# -----------------------------
# DB helpers
# -----------------------------
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
    return create_engine(get_database_url(), pool_pre_ping=True, future=True)

def sql_read(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def sql_exec(engine, sql: str, params: Optional[dict] = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def table_exists(engine, table: str, schema: str = "public") -> bool:
    try:
        insp = inspect(engine)
        return table in set(insp.get_table_names(schema=schema))
    except Exception:
        return False

def get_table_columns(engine, table: str, schema: str = "public") -> set:
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        return set([str(c.get("name", "")).lower() for c in cols])
    except Exception:
        return set()

# -----------------------------
# DB maintenance helpers (optional)
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_table_max_dt(_engine, table: str) -> Optional[str]:
    """Return MAX(dt) for a fact table (YYYY-MM-DD)."""
    if not table_exists(_engine, table):
        return None
    try:
        df = sql_read(_engine, f"SELECT MAX(dt) AS max_dt FROM {table}")
        if df is None or df.empty:
            return None
        v = df.loc[0, "max_dt"]
        return str(v) if pd.notna(v) else None
    except Exception:
        return None

def render_data_freshness(_engine) -> None:
    """Small status badges for data freshness."""
    items = [
        ("fact_campaign_daily", "캠페인"),
        ("fact_keyword_daily", "키워드"),
        ("fact_ad_daily", "소재"),
        ("fact_bizmoney_daily", "비즈머니"),
    ]
    pills = []
    for t, label in items:
        md = get_table_max_dt(_engine, t)
        pills.append(f"<span class='pill'>{label}: {md or '-'} </span>")
    st.markdown("<div style='display:flex; gap:6px; flex-wrap:wrap; margin:2px 0 10px 0;'>" + "".join(pills) + "</div>", unsafe_allow_html=True)

def create_perf_indexes(_engine) -> Dict[str, Any]:
    """Create commonly-needed indexes (IF NOT EXISTS)."""
    ddls = [
        # fact tables
        ("idx_fcd_dt_customer", "CREATE INDEX IF NOT EXISTS idx_fcd_dt_customer ON public.fact_campaign_daily (dt, customer_id)"),
        ("idx_fcd_customer_campaign_dt", "CREATE INDEX IF NOT EXISTS idx_fcd_customer_campaign_dt ON public.fact_campaign_daily (customer_id, campaign_id, dt)"),
        ("idx_fkd_dt_customer", "CREATE INDEX IF NOT EXISTS idx_fkd_dt_customer ON public.fact_keyword_daily (dt, customer_id)"),
        ("idx_fkd_customer_keyword_dt", "CREATE INDEX IF NOT EXISTS idx_fkd_customer_keyword_dt ON public.fact_keyword_daily (customer_id, keyword_id, dt)"),
        ("idx_fad_dt_customer", "CREATE INDEX IF NOT EXISTS idx_fad_dt_customer ON public.fact_ad_daily (dt, customer_id)"),
        ("idx_fad_customer_ad_dt", "CREATE INDEX IF NOT EXISTS idx_fad_customer_ad_dt ON public.fact_ad_daily (customer_id, ad_id, dt)"),
        ("idx_fbm_customer_dt", "CREATE INDEX IF NOT EXISTS idx_fbm_customer_dt ON public.fact_bizmoney_daily (customer_id, dt)"),
        # dim tables (join keys)
        ("idx_dim_campaign_pk", "CREATE INDEX IF NOT EXISTS idx_dim_campaign_pk ON public.dim_campaign (customer_id, campaign_id)"),
        ("idx_dim_adgroup_pk", "CREATE INDEX IF NOT EXISTS idx_dim_adgroup_pk ON public.dim_adgroup (customer_id, adgroup_id)"),
        ("idx_dim_keyword_pk", "CREATE INDEX IF NOT EXISTS idx_dim_keyword_pk ON public.dim_keyword (customer_id, keyword_id)"),
        ("idx_dim_ad_pk", "CREATE INDEX IF NOT EXISTS idx_dim_ad_pk ON public.dim_ad (customer_id, ad_id)"),
    ]

    ok, fail = [], []
    for name, ddl in ddls:
        try:
            sql_exec(_engine, ddl)
            ok.append(name)
        except Exception as e:
            fail.append({"name": name, "error": str(e)})

    return {"ok": ok, "fail": fail}

def analyze_perf_tables(_engine) -> Dict[str, Any]:
    """Try ANALYZE (may require privileges)."""
    targets = ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily", "dim_campaign", "dim_adgroup", "dim_keyword", "dim_ad"]
    ok, fail = [], []
    for t in targets:
        if not table_exists(_engine, t):
            continue
        try:
            sql_exec(_engine, f"ANALYZE {t}")
            ok.append(t)
        except Exception as e:
            fail.append({"table": t, "error": str(e)})
    return {"ok": ok, "fail": fail}

# -----------------------------
# Utilities
# -----------------------------
def format_currency(val) -> str:
    if pd.isna(val) or val == "":
        return "0원"
    try:
        return f"{int(float(val)):,}원"
    except Exception:
        return "0원"

def format_number_commas(val) -> str:
    if pd.isna(val) or val == "":
        return "0"
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return "0"

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
    out[col] = s.apply(_fmt)
    return out

def parse_currency(val_str) -> int:
    if pd.isna(val_str):
        return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0

def add_rates(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    if "sales" not in g.columns:
        g["sales"] = 0
    g["revenue"] = pd.to_numeric(g["sales"], errors="coerce").fillna(0)
    g["roas"] = (g["revenue"] / g["cost"].replace({0: pd.NA})) * 100
    return g

def render_live_clock(tz: str = "Asia/Seoul"):
    components.html(
        f"""
        <div style="display:flex; justify-content:flex-end; align-items:center; width:100%;
                    font-size:12px; color:rgba(49,51,63,0.7); margin-top:-6px; margin-bottom:8px;">
          <span id="live-clock"></span>
        </div>
        <script>
          const tz = "{tz}";
          function tick() {{
            const now = new Date();
            const fmt = new Intl.DateTimeFormat('ko-KR', {{
              timeZone: tz,
              year: 'numeric', month: '2-digit', day: '2-digit',
              hour: '2-digit', minute: '2-digit', second: '2-digit',
              hour12: false
            }});
            document.getElementById('live-clock').textContent = "현재 시각: " + fmt.format(now);
          }}
          tick();
          setInterval(tick, 1000);
        </script>
        """,
        height=32,
    )

# -----------------------------
# Campaign type mapping (SQL CASE)
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

def campaign_tp_to_label(tp: str) -> str:
    t = (tp or "").strip()
    if not t:
        return ""
    key = t.lower()
    return _CAMPAIGN_TP_LABEL.get(key, t)

def campaign_type_case_sql(col: str = "cp.campaign_tp") -> str:
    # Postgres에서 쓰는 CASE 식
    # col: 문자열 컬럼
    return f"""
    CASE lower(trim(coalesce({col}, '')))
      WHEN 'web_site' THEN '파워링크'
      WHEN 'website' THEN '파워링크'
      WHEN 'power_link' THEN '파워링크'
      WHEN 'shopping' THEN '쇼핑검색'
      WHEN 'shopping_search' THEN '쇼핑검색'
      WHEN 'power_content' THEN '파워콘텐츠'
      WHEN 'power_contents' THEN '파워콘텐츠'
      WHEN 'powercontent' THEN '파워콘텐츠'
      WHEN 'place' THEN '플레이스'
      WHEN 'place_search' THEN '플레이스'
      WHEN 'brand_search' THEN '브랜드검색'
      WHEN 'brandsearch' THEN '브랜드검색'
      ELSE '기타'
    END
    """

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []
    raw = dim_campaign.get("campaign_type_label", pd.Series([], dtype=str))
    present = set(
        [
            x.strip()
            for x in raw.dropna().astype(str).tolist()
            if x and str(x).strip() and str(x).strip() not in ("미분류", "종합", "기타")
        ]
    )
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    opts = [x for x in order if x in present]
    extra = sorted([x for x in present if x not in set(order)])
    return opts + extra

# -----------------------------
# Accounts seed / meta
# -----------------------------
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

def seed_from_accounts_xlsx(engine) -> Dict[str, int]:
    if not os.path.exists(ACCOUNTS_XLSX):
        return {"meta": 0, "dim": 0}
    df = pd.read_excel(ACCOUNTS_XLSX)
    acc = normalize_accounts_columns(df)

    sql_exec(
        engine,
        """CREATE TABLE IF NOT EXISTS dim_account_meta (
      customer_id BIGINT PRIMARY KEY,
      account_name TEXT NOT NULL,
      manager TEXT DEFAULT '',
      monthly_budget BIGINT DEFAULT 0
    );""",
    )
    sql_exec(engine, "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();")
    sql_exec(engine, """CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT NOT NULL);""")

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

    dim_rows = acc[["customer_id", "account_name"]].copy()
    dim_rows["customer_id"] = dim_rows["customer_id"].astype(str)
    upsert_dim = """
    INSERT INTO dim_account (customer_id, account_name)
    VALUES (:customer_id, :account_name)
    ON CONFLICT (customer_id) DO UPDATE SET account_name = EXCLUDED.account_name;
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_dim), dim_rows.to_dict(orient="records"))

    return {"meta": int(len(acc)), "dim": int(len(dim_rows))}

@st.cache_data(ttl=3600, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    df = sql_read(
        _engine,
        """
        SELECT customer_id, account_name, manager, monthly_budget, updated_at
        FROM dim_account_meta
        ORDER BY account_name
        """,
    )
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()
    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df.empty:
        return df
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df

def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
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
# SQL helpers: safe literal IN (...)
# -----------------------------
def _sql_in_int(values: Tuple[int, ...]) -> str:
    """Return an SQL IN (...) list for integer-like IDs.

    NOTE: Many of our tables store IDs as TEXT in Postgres.
    To avoid errors like `operator does not exist: text = integer`,
    we emit quoted literals (type 'unknown'), e.g. ('420332','360788').
    Postgres can compare these both to TEXT columns (no cast) and to INT columns (implicit cast).
    """
    if not values:
        return ""
    safe = []
    for v in values:
        try:
            safe.append(str(int(v)))
        except Exception:
            continue
    if not safe:
        return ""
    vals = ",".join(f"'{v}'" for v in safe)
    return f"({vals})"


def _sql_in_text(values: Tuple[str, ...]) -> str:
    if not values:
        return ""
    safe = []
    for v in values:
        s = str(v).replace("'", "''")
        safe.append(f"'{s}'")
    return "(" + ",".join(safe) + ")"

# -----------------------------
# Fast SQL: Budget
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_latest_bizmoney(_engine, customer_ids: Tuple[int, ...] = ()) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"):
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
    cid_clause = f" WHERE customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    sql = f"""
    SELECT DISTINCT ON (customer_id) customer_id, bizmoney_balance, dt as last_update
    FROM fact_bizmoney_daily
    {cid_clause}
    ORDER BY customer_id, dt DESC
    """
    df = sql_read(_engine, sql)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def get_cost_by_customer_on_date(_engine, target_dt: date, customer_ids: Tuple[int, ...] = ()) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "y_cost"])
    cid_clause = f" AND customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    sql = f"""
    SELECT customer_id, SUM(cost) AS y_cost
    FROM fact_campaign_daily
    WHERE dt = :d
    {cid_clause}
    GROUP BY customer_id
    """
    df = sql_read(_engine, sql, {"d": str(target_dt)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["y_cost"] = pd.to_numeric(df["y_cost"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600, show_spinner=False)
def get_recent_avg_cost(_engine, d1: date, d2: date, customer_ids: Tuple[int, ...] = ()) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])
    if d2 < d1:
        d1 = d2
    cid_clause = f" AND customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    sql = f"""
    SELECT customer_id, SUM(cost) AS sum_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    {cid_clause}
    GROUP BY customer_id
    """
    tmp = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if tmp.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"]).copy()
    tmp["customer_id"] = tmp["customer_id"].astype("int64")
    days = max((d2 - d1).days + 1, 1)
    tmp["avg_cost"] = pd.to_numeric(tmp["sum_cost"], errors="coerce").fillna(0).astype(float) / float(days)
    return tmp[["customer_id", "avg_cost"]]

@st.cache_data(ttl=600, show_spinner=False)
def get_monthly_cost(_engine, target_date: date, customer_ids: Tuple[int, ...] = ()) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    start_dt = target_date.replace(day=1)
    if target_date.month == 12:
        end_dt = date(target_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)

    cid_clause = f" AND customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    sql = f"""
    SELECT customer_id, SUM(cost) as current_month_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    {cid_clause}
    GROUP BY customer_id
    """
    df = sql_read(_engine, sql, {"d1": str(start_dt), "d2": str(end_dt)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["current_month_cost"] = pd.to_numeric(df["current_month_cost"], errors="coerce").fillna(0).astype("int64")
    return df

# -----------------------------
# Fast SQL: Campaign / Keyword / Ad (TopN)
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_agg(_engine, d1: date, d2: date, customer_ids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()

    cid_clause = f" AND f.customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    ctype = campaign_type_case_sql("c.campaign_tp")
    type_clause = f" AND {ctype} IN {_sql_in_text(type_sel)}" if type_sel else ""

    sql = f"""
    WITH agg AS (
      SELECT f.customer_id, f.campaign_id,
             SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost,
             SUM(f.conv) AS conv, SUM(COALESCE(f.sales,0)) AS sales
      FROM fact_campaign_daily f
      WHERE f.dt BETWEEN :d1 AND :d2
      {cid_clause}
      GROUP BY f.customer_id, f.campaign_id
    )
    SELECT a.customer_id, a.campaign_id, a.imp, a.clk, a.cost, a.conv, a.sales,
           COALESCE(c.campaign_name,'') AS campaign_name,
           {ctype} AS campaign_type_label
    FROM agg a
    LEFT JOIN dim_campaign c
      ON a.customer_id = c.customer_id AND a.campaign_id = c.campaign_id
    WHERE {ctype} <> '기타'
    {type_clause}
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df.empty:
        return df
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_daily_sum(_engine, d1: date, d2: date, customer_ids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()
    if not table_exists(_engine, "dim_campaign"):
        # 타입필터가 없으면 그냥 합계만
        cid_clause = f" AND customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
        sql = f"""
        SELECT dt, SUM(imp) AS imp, SUM(clk) AS clk, SUM(cost) AS cost, SUM(conv) AS conv, SUM(COALESCE(sales,0)) AS sales
        FROM fact_campaign_daily
        WHERE dt BETWEEN :d1 AND :d2
        {cid_clause}
        GROUP BY dt
        ORDER BY dt
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
        if not df.empty:
            df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        return df

    cid_clause = f" AND f.customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    ctype = campaign_type_case_sql("c.campaign_tp")
    type_clause = f" AND {ctype} IN {_sql_in_text(type_sel)}" if type_sel else ""

    sql = f"""
    SELECT f.dt,
           SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost,
           SUM(f.conv) AS conv, SUM(COALESCE(f.sales,0)) AS sales
    FROM fact_campaign_daily f
    LEFT JOIN dim_campaign c
      ON f.customer_id = c.customer_id AND f.campaign_id = c.campaign_id
    WHERE f.dt BETWEEN :d1 AND :d2
      {cid_clause}
      AND {ctype} <> '기타'
      {type_clause}
    GROUP BY f.dt
    ORDER BY f.dt
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if not df.empty:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df

@st.cache_data(ttl=600, show_spinner=False)
@st.cache_data(ttl=600, show_spinner=False)
def query_keyword_topn(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """Keyword Top-N aggregated in DB, then (optionally) filtered in Python.

    Speed tricks:
    - Aggregate first (fact), limit early (top CTE), then join dims only for those rows.
    - Avoid SQLAlchemy 'expanding' bind issues by using a safe literal IN clause for customer_ids.
    """
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()

    cid_clause = ""
    if customer_ids:
        cid_clause = f"AND customer_id IN {_sql_in_int(tuple(customer_ids))}"

    # If type filter is used, oversample a bit so filtering doesn't empty the list.
    probe_lim = int(max(50, top_n)) * (5 if (type_sel and len(type_sel) > 0) else 1)

    sql = f"""
    WITH agg AS (
        SELECT
            customer_id,
            keyword_id,
            SUM(imp) AS imp,
            SUM(clk) AS clk,
            SUM(cost) AS cost,
            SUM(conv) AS conv,
            SUM(COALESCE(sales, 0)) AS sales
        FROM fact_keyword_daily
        WHERE dt BETWEEN :d1 AND :d2
          {cid_clause}
        GROUP BY customer_id, keyword_id
    ),
    top AS (
        SELECT * FROM agg
        ORDER BY cost DESC
        LIMIT :probe_lim
    ),
    kw AS (
        SELECT
            k.customer_id,
            k.keyword_id,
            MAX(k.keyword) AS keyword,
            MAX(k.adgroup_id) AS adgroup_id
        FROM dim_keyword k
        JOIN top t
          ON t.customer_id = k.customer_id
         AND t.keyword_id  = k.keyword_id
        GROUP BY k.customer_id, k.keyword_id
    ),
    ag AS (
        SELECT
            g.customer_id,
            g.adgroup_id,
            MAX(g.adgroup_name) AS adgroup_name,
            MAX(g.campaign_id)  AS campaign_id
        FROM dim_adgroup g
        JOIN kw
          ON kw.customer_id = g.customer_id
         AND kw.adgroup_id  = g.adgroup_id
        GROUP BY g.customer_id, g.adgroup_id
    ),
    cp AS (
        SELECT
            c.customer_id,
            c.campaign_id,
            MAX(c.campaign_name) AS campaign_name,
            MAX(c.campaign_tp)   AS campaign_tp
        FROM dim_campaign c
        JOIN ag
          ON ag.customer_id = c.customer_id
         AND ag.campaign_id = c.campaign_id
        GROUP BY c.customer_id, c.campaign_id
    )
    SELECT
        t.customer_id,
        t.keyword_id,
        t.imp, t.clk, t.cost, t.conv, t.sales,
        COALESCE(kw.keyword, '')       AS keyword,
        COALESCE(ag.adgroup_name, '')  AS adgroup_name,
        COALESCE(cp.campaign_name, '') AS campaign_name,
        COALESCE(cp.campaign_tp, '')   AS campaign_tp
    FROM top t
    LEFT JOIN kw ON kw.customer_id = t.customer_id AND kw.keyword_id = t.keyword_id
    LEFT JOIN ag ON ag.customer_id = kw.customer_id AND ag.adgroup_id = kw.adgroup_id
    LEFT JOIN cp ON cp.customer_id = ag.customer_id AND cp.campaign_id = ag.campaign_id
    ORDER BY t.cost DESC
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "probe_lim": probe_lim})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["campaign_type"] = df.get("campaign_tp", "").apply(campaign_tp_to_label)
    # remove '기타' by default (요청 기준)
    df = df[df["campaign_type"] != "기타"]

    if type_sel:
        df = df[df["campaign_type"].isin(type_sel)]

    df = df.sort_values("cost", ascending=False).head(int(top_n))
    return df.reset_index(drop=True)

@st.cache_data(ttl=600, show_spinner=False)
def query_ad_topn(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """Ad (creative) Top-N aggregated in DB, then (optionally) filtered in Python."""
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()

    cid_clause = ""
    if customer_ids:
        cid_clause = f"AND customer_id IN {_sql_in_int(tuple(customer_ids))}"

    probe_lim = int(max(50, top_n)) * (5 if (type_sel and len(type_sel) > 0) else 1)

    sql = f"""
    WITH agg AS (
        SELECT
            customer_id,
            ad_id,
            SUM(imp) AS imp,
            SUM(clk) AS clk,
            SUM(cost) AS cost,
            SUM(conv) AS conv,
            SUM(COALESCE(sales, 0)) AS sales
        FROM fact_ad_daily
        WHERE dt BETWEEN :d1 AND :d2
          {cid_clause}
        GROUP BY customer_id, ad_id
    ),
    top AS (
        SELECT * FROM agg
        ORDER BY cost DESC
        LIMIT :probe_lim
    ),
    ad AS (
        SELECT
            a.customer_id,
            a.ad_id,
            MAX(a.ad_name)   AS ad_name,
            MAX(a.adgroup_id) AS adgroup_id
        FROM dim_ad a
        JOIN top t
          ON t.customer_id = a.customer_id
         AND t.ad_id       = a.ad_id
        GROUP BY a.customer_id, a.ad_id
    ),
    ag AS (
        SELECT
            g.customer_id,
            g.adgroup_id,
            MAX(g.adgroup_name) AS adgroup_name,
            MAX(g.campaign_id)  AS campaign_id
        FROM dim_adgroup g
        JOIN ad
          ON ad.customer_id = g.customer_id
         AND ad.adgroup_id  = g.adgroup_id
        GROUP BY g.customer_id, g.adgroup_id
    ),
    cp AS (
        SELECT
            c.customer_id,
            c.campaign_id,
            MAX(c.campaign_name) AS campaign_name,
            MAX(c.campaign_tp)   AS campaign_tp
        FROM dim_campaign c
        JOIN ag
          ON ag.customer_id = c.customer_id
         AND ag.campaign_id = c.campaign_id
        GROUP BY c.customer_id, c.campaign_id
    )
    SELECT
        t.customer_id,
        t.ad_id,
        t.imp, t.clk, t.cost, t.conv, t.sales,
        COALESCE(ad.ad_name, '')       AS ad_name,
        COALESCE(ag.adgroup_name, '')  AS adgroup_name,
        COALESCE(cp.campaign_name, '') AS campaign_name,
        COALESCE(cp.campaign_tp, '')   AS campaign_tp
    FROM top t
    LEFT JOIN ad ON ad.customer_id = t.customer_id AND ad.ad_id = t.ad_id
    LEFT JOIN ag ON ag.customer_id = ad.customer_id AND ag.adgroup_id = ad.adgroup_id
    LEFT JOIN cp ON cp.customer_id = ag.customer_id AND cp.campaign_id = ag.campaign_id
    ORDER BY t.cost DESC
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "probe_lim": probe_lim})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["campaign_type"] = df.get("campaign_tp", "").apply(campaign_tp_to_label)
    df = df[df["campaign_type"] != "기타"]
    if type_sel:
        df = df[df["campaign_type"].isin(type_sel)]

    df = df.sort_values("cost", ascending=False).head(int(top_n))
    return df.reset_index(drop=True)

def build_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    """
    공통 필터 패널 (모바일/데스크톱 모두 메인 영역에 노출)

    반환 keys (호환 유지):
    - start, end  : date
    - d1, d2      : date (alias)
    - manager, account, q
    - type_sel    : Tuple[str, ...]   # 캠페인 유형(라벨) 필터 (예: 파워링크/쇼핑검색)
    - top_n_keyword / top_n_ad / top_n_campaign
    - selected_customer_ids : List[int]  (필터 결과 customer_id)
    """
    today = date.today()
    default_end = today - timedelta(days=1)  # 기본: 어제
    default_start = default_end

    defaults = {
        "q": "",
        "manager": [],
        "account": [],
        "type_sel": tuple(type_opts) if type_opts else tuple(),
        "period_mode": "어제",
        "d1": default_start,
        "d2": default_end,
        "top_n_keyword": 100,
        "top_n_ad": 100,
        "top_n_campaign": 100,
    }

    if "filters_applied" not in st.session_state:
        st.session_state["filters_applied"] = defaults.copy()

    # ---- UI (메인 영역)
    with st.expander("필터", expanded=True):
        c1, c2, c3 = st.columns([2, 2, 2])

        with c1:
            q = st.text_input("업체명 검색", value=st.session_state["filters_applied"].get("q", ""))
            manager_opts = sorted(
                [x for x in meta.get("manager", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip() != ""]
            )
            manager_sel = st.multiselect(
                "담당자",
                manager_opts,
                default=st.session_state["filters_applied"].get("manager", []),
            )

        with c2:
            account_opts_all = sorted(
                [x for x in meta.get("account_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip() != ""]
            )
            account_sel = st.multiselect(
                "업체",
                account_opts_all,
                default=st.session_state["filters_applied"].get("account", []),
            )

            type_sel = tuple(
                st.multiselect(
                    "캠페인 유형",
                    type_opts or [],
                    default=list(st.session_state["filters_applied"].get("type_sel", tuple(type_opts) if type_opts else tuple())),
                )
            )

        with c3:
            period_mode = st.selectbox(
                "기간",
                ["어제", "최근 3일", "최근 7일", "직접 선택"],
                index=["어제", "최근 3일", "최근 7일", "직접 선택"].index(st.session_state["filters_applied"].get("period_mode", "어제")),
            )

            if period_mode == "최근 3일":
                d2 = default_end
                d1 = d2 - timedelta(days=2)
            elif period_mode == "최근 7일":
                d2 = default_end
                d1 = d2 - timedelta(days=6)
            elif period_mode == "직접 선택":
                d1d2 = st.date_input(
                    "기간 선택",
                    value=(st.session_state["filters_applied"].get("d1", default_start), st.session_state["filters_applied"].get("d2", default_end)),
                )
                if isinstance(d1d2, (list, tuple)) and len(d1d2) == 2:
                    d1, d2 = d1d2[0], d1d2[1]
                else:
                    d1, d2 = default_start, default_end
            else:
                d1, d2 = default_start, default_end

            top_n_keyword = st.slider("키워드 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_keyword", 100)), step=10)
            top_n_ad = st.slider("소재 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_ad", 100)), step=10)
            top_n_campaign = st.slider("캠페인 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_campaign", 100)), step=10)

        apply_btn = st.button("적용", use_container_width=True)

    if apply_btn:
        st.session_state["filters_applied"] = {
            "q": q,
            "manager": manager_sel,
            "account": account_sel,
            "type_sel": type_sel,
            "period_mode": period_mode,
            "d1": d1,
            "d2": d2,
            "top_n_keyword": top_n_keyword,
            "top_n_ad": top_n_ad,
            "top_n_campaign": top_n_campaign,
        }

    f = dict(st.session_state.get("filters_applied", defaults))

    # ---- aliases for backward compatibility
    f["start"] = f.get("d1", default_start)
    f["end"] = f.get("d2", default_end)

    # ---- compute selected_customer_ids from meta filters (for SQL IN)
    df = meta.copy()
    if f.get("manager"):
        df = df[df["manager"].isin(f["manager"])]
    if f.get("account"):
        df = df[df["account_name"].isin(f["account"])]
    if f.get("q"):
        q_ = str(f["q"]).strip()
        if q_:
            df = df[df["account_name"].astype(str).str.contains(q_, case=False, na=False)]

    # 빈 리스트는 "전체" 의미로 사용 (기존 로직 호환)
    cids = df["customer_id"].dropna().astype(int).tolist() if len(df) < len(meta) else []
    f["selected_customer_ids"] = cids

    return f


def main():
    st.title("네이버 검색광고 통합 대시보드")
    st.caption(f"빌드: {BUILD_TAG}")

    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

    # quick status (data freshness)
    render_data_freshness(engine)

    # seed (best effort)
    try:
        seed_from_accounts_xlsx(engine)
    except Exception:
        pass

    meta = get_meta(engine)
    if meta is None or meta.empty:
        st.error("dim_account_meta가 비어있습니다. settings에서 accounts.xlsx 동기화를 먼저 해주세요.")
        return

    dim_campaign = load_dim_campaign(engine)
    type_opts = get_campaign_type_options(dim_campaign)

    f = build_filters(meta, type_opts)

    # menu: mobile friendly selectbox
    page = st.selectbox("메뉴", ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정/연결"], index=0)

    st.divider()

    if page == "전체 예산/잔액 관리":
        page_budget(meta, engine, f)
    elif page == "성과(캠페인)":
        page_perf_campaign(meta, engine, f)
    elif page == "성과(키워드)":
        page_perf_keyword(meta, engine, f)
    elif page == "성과(소재)":
        page_perf_ad(meta, engine, f)
    else:
        page_settings(engine)

if __name__ == "__main__":
    main()
