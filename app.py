
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
from typing import List, Optional, Dict, Tuple

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
BUILD_TAG = "v7.2.0 (2026-02-17)"

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
    if not values:
        return ""
    vals = ",".join(str(int(v)) for v in values)
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
def query_keyword_topn(_engine, d1: date, d2: date, customer_ids: Tuple[int, ...], type_sel: Tuple[str, ...], topn: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()

    has_kw = table_exists(_engine, "dim_keyword")
    has_ag = table_exists(_engine, "dim_adgroup")
    has_cp = table_exists(_engine, "dim_campaign")

    cid_clause = f" AND f.customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    # 타입필터는 dim_campaign 없으면 적용 불가
    type_clause = ""
    ctype = campaign_type_case_sql("cp.campaign_tp")

    if has_cp and type_sel:
        type_clause = f" AND {ctype} IN {_sql_in_text(type_sel)}"

    kw_cte = ""
    ag_cte = ""
    cp_cte = ""
    join_kw = ""
    join_ag = ""
    join_cp = ""
    select_dim = "''::text AS keyword, ''::text AS adgroup_name, ''::text AS campaign_name, '기타'::text AS campaign_type_label"

    if has_kw:
        kw_cte = """
        , kw AS (
          SELECT DISTINCT ON (customer_id, keyword_id)
                 customer_id, keyword_id, keyword, adgroup_id
          FROM dim_keyword
        )
        """
        join_kw = "LEFT JOIN kw ON a.customer_id = kw.customer_id AND a.keyword_id = kw.keyword_id"

    if has_ag:
        ag_cte = """
        , ag AS (
          SELECT DISTINCT ON (customer_id, adgroup_id)
                 customer_id, adgroup_id, adgroup_name, campaign_id
          FROM dim_adgroup
        )
        """
        if has_kw:
            join_ag = "LEFT JOIN ag ON kw.customer_id = ag.customer_id AND kw.adgroup_id = ag.adgroup_id"
        else:
            # dim_keyword 없으면 adgroup 연결 불가
            join_ag = ""

    if has_cp:
        cp_cte = """
        , cp AS (
          SELECT DISTINCT ON (customer_id, campaign_id)
                 customer_id, campaign_id, campaign_name, campaign_tp
          FROM dim_campaign
        )
        """
        if has_ag and has_kw:
            join_cp = "LEFT JOIN cp ON ag.customer_id = cp.customer_id AND ag.campaign_id = cp.campaign_id"
        else:
            join_cp = ""

    if has_kw and has_ag and has_cp:
        select_dim = f"""
        COALESCE(kw.keyword,'') AS keyword,
        COALESCE(ag.adgroup_name,'') AS adgroup_name,
        COALESCE(cp.campaign_name,'') AS campaign_name,
        {ctype} AS campaign_type_label
        """

    # 타입 제외(기타) — dim이 있을 때만 의미 있음
    etc_clause = ""
    if has_kw and has_ag and has_cp:
        etc_clause = f" AND {ctype} <> '기타' "

    sql = f"""
    WITH agg AS (
      SELECT f.customer_id, f.keyword_id,
             SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost,
             SUM(f.conv) AS conv, SUM(COALESCE(f.sales,0)) AS sales
      FROM fact_keyword_daily f
      WHERE f.dt BETWEEN :d1 AND :d2
      {cid_clause}
      GROUP BY f.customer_id, f.keyword_id
    )
    {kw_cte}
    {ag_cte}
    {cp_cte}
    SELECT a.customer_id, a.keyword_id, a.imp, a.clk, a.cost, a.conv, a.sales,
           {select_dim}
    FROM agg a
    {join_kw}
    {join_ag}
    {join_cp}
    WHERE 1=1
    {etc_clause}
    {type_clause}
    ORDER BY a.cost DESC
    LIMIT :lim
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(topn)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_ad_topn(_engine, d1: date, d2: date, customer_ids: Tuple[int, ...], type_sel: Tuple[str, ...], topn: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()

    has_ad = table_exists(_engine, "dim_ad")
    has_ag = table_exists(_engine, "dim_adgroup")
    has_cp = table_exists(_engine, "dim_campaign")

    cid_clause = f" AND f.customer_id IN {_sql_in_int(customer_ids)}" if customer_ids else ""
    ctype = campaign_type_case_sql("cp.campaign_tp")
    type_clause = f" AND {ctype} IN {_sql_in_text(type_sel)}" if (has_cp and type_sel) else ""

    ad_cte = ""
    ag_cte = ""
    cp_cte = ""
    join_ad = ""
    join_ag = ""
    join_cp = ""

    cols = get_table_columns(_engine, "dim_ad") if has_ad else set()
    if has_ad:
        if "creative_text" in cols:
            ad_cte = """
            , ad AS (
              SELECT DISTINCT ON (customer_id, ad_id)
                     customer_id, ad_id,
                     COALESCE(NULLIF(creative_text,''), NULLIF(ad_name,''), '') AS ad_name,
                     adgroup_id
              FROM dim_ad
            )
            """
        else:
            ad_cte = """
            , ad AS (
              SELECT DISTINCT ON (customer_id, ad_id)
                     customer_id, ad_id,
                     COALESCE(ad_name,'') AS ad_name,
                     adgroup_id
              FROM dim_ad
            )
            """
        join_ad = "LEFT JOIN ad ON a.customer_id = ad.customer_id AND a.ad_id = ad.ad_id"

    if has_ag:
        ag_cte = """
        , ag AS (
          SELECT DISTINCT ON (customer_id, adgroup_id)
                 customer_id, adgroup_id, adgroup_name, campaign_id
          FROM dim_adgroup
        )
        """
        if has_ad:
            join_ag = "LEFT JOIN ag ON ad.customer_id = ag.customer_id AND ad.adgroup_id = ag.adgroup_id"

    if has_cp:
        cp_cte = """
        , cp AS (
          SELECT DISTINCT ON (customer_id, campaign_id)
                 customer_id, campaign_id, campaign_name, campaign_tp
          FROM dim_campaign
        )
        """
        if has_ad and has_ag:
            join_cp = "LEFT JOIN cp ON ag.customer_id = cp.customer_id AND ag.campaign_id = cp.campaign_id"

    select_dim = "''::text AS ad_name, ''::text AS adgroup_name, ''::text AS campaign_name, '기타'::text AS campaign_type_label"
    etc_clause = ""
    if has_ad and has_ag and has_cp:
        select_dim = f"""
        COALESCE(ad.ad_name,'') AS ad_name,
        COALESCE(ag.adgroup_name,'') AS adgroup_name,
        COALESCE(cp.campaign_name,'') AS campaign_name,
        {ctype} AS campaign_type_label
        """
        etc_clause = f" AND {ctype} <> '기타' "

    sql = f"""
    WITH agg AS (
      SELECT f.customer_id, f.ad_id,
             SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost,
             SUM(f.conv) AS conv, SUM(COALESCE(f.sales,0)) AS sales
      FROM fact_ad_daily f
      WHERE f.dt BETWEEN :d1 AND :d2
      {cid_clause}
      GROUP BY f.customer_id, f.ad_id
    )
    {ad_cte}
    {ag_cte}
    {cp_cte}
    SELECT a.customer_id, a.ad_id, a.imp, a.clk, a.cost, a.conv, a.sales,
           {select_dim}
    FROM agg a
    {join_ad}
    {join_ag}
    {join_cp}
    WHERE 1=1
    {etc_clause}
    {type_clause}
    ORDER BY a.cost DESC
    LIMIT :lim
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(topn)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

# -----------------------------
# Filters (mobile-friendly, always visible)
# -----------------------------
def build_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    # session defaults
    if "filters" not in st.session_state:
        today = date.today()
        y = today - timedelta(days=1)
        st.session_state["filters"] = {
            "q": "",
            "manager_sel": [],
            "company_sel": [],
            "period": "어제",
            "start": y,
            "end": y,
            "type_sel": [],
        }

    cur = st.session_state["filters"]
    managers = sorted([m for m in meta.get("manager", pd.Series([], dtype=str)).fillna("").unique().tolist() if str(m).strip()])
    companies = meta.get("account_name", pd.Series([], dtype=str)).fillna("").astype(str).tolist()

    st.markdown("### 필터")
    st.markdown('<div class="filter-wrap">', unsafe_allow_html=True)
    with st.form("filters_form", clear_on_submit=False):
        c1, c2 = st.columns([1.2, 1])
        with c1:
            q = st.text_input("업체명 검색", value=cur.get("q", ""), placeholder="예: 실리콘플러스")
            company_sel = st.multiselect("업체(다중 선택)", options=companies, default=cur.get("company_sel", []))
        with c2:
            manager_sel = st.multiselect("담당자(다중 선택)", options=managers, default=cur.get("manager_sel", []))
            type_sel = st.multiselect("광고유형", options=type_opts, default=cur.get("type_sel", []))

        c3, c4, c5 = st.columns([1.2, 1, 1])
        with c3:
            period = st.selectbox("기간", ["오늘", "어제", "최근 7일(오늘 제외)", "최근 30일(오늘 제외)", "직접 선택"], index=["오늘","어제","최근 7일(오늘 제외)","최근 30일(오늘 제외)","직접 선택"].index(cur.get("period","어제")))
        with c4:
            start = cur.get("start")
            end = cur.get("end")
            today = date.today()
            if period == "오늘":
                start = today
                end = today
            elif period == "어제":
                end = today - timedelta(days=1)
                start = end
            elif period.startswith("최근 7일"):
                end = today - timedelta(days=1)
                start = end - timedelta(days=6)
            elif period.startswith("최근 30일"):
                end = today - timedelta(days=1)
                start = end - timedelta(days=29)
            else:
                start = st.date_input("시작일", value=start if isinstance(start, date) else (today - timedelta(days=7)))
        with c5:
            if period == "직접 선택":
                end = st.date_input("종료일", value=end if isinstance(end, date) else (today - timedelta(days=1)))
            else:
                st.caption(f"선택: {start} ~ {end}")

        submit = st.form_submit_button("✅ 필터 적용", use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    if submit:
        if period == "직접 선택" and end < start:
            st.warning("종료일은 시작일 이후여야 합니다. (적용되지 않음)")
        else:
            st.session_state["filters"] = {
                "q": q,
                "manager_sel": manager_sel,
                "company_sel": company_sel,
                "period": period,
                "start": start,
                "end": end,
                "type_sel": type_sel,
            }
            st.rerun()

    # applied filters
    f = st.session_state["filters"].copy()

    # customer_id resolution priority: company_sel > manager_sel > q > all
    df = meta.copy()
    if f["q"]:
        df = df[df["account_name"].astype(str).str.contains(f["q"], case=False, na=False)]
    if f["manager_sel"]:
        df = df[df["manager"].isin(f["manager_sel"])]
    if f["company_sel"]:
        df = meta[meta["account_name"].isin(f["company_sel"])].copy()

    f["selected_customer_ids"] = df["customer_id"].astype(int).tolist() if not df.empty else []
    return f

# -----------------------------
# Pages
# -----------------------------
def page_budget(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()

    sel_ids = tuple(int(x) for x in f.get("selected_customer_ids", []) if int(x) > 0)

    df = meta.copy()
    if sel_ids:
        df = df[df["customer_id"].isin(list(sel_ids))].copy()

    # Bizmoney + yesterday cost
    yesterday = date.today() - timedelta(days=1)

    try:
        biz = get_latest_bizmoney(engine, sel_ids)
    except Exception as e:
        biz = pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
        st.warning(f"비즈머니 조회 실패: {e}")

    try:
        y_cost_df = get_cost_by_customer_on_date(engine, yesterday, sel_ids)
    except Exception as e:
        y_cost_df = pd.DataFrame(columns=["customer_id", "y_cost"])
        st.warning(f"전일 소진액 조회 실패: {e}")

    # Recent avg cost
    avg_df = pd.DataFrame(columns=["customer_id", "avg_cost"])
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1)
        d1 = d2 - timedelta(days=TOPUP_AVG_DAYS - 1)
        try:
            avg_df = get_recent_avg_cost(engine, d1, d2, sel_ids)
        except Exception as e:
            st.warning(f"최근 평균소진 조회 실패(표시는 계속): {e}")

    # Build view
    base = df[["customer_id", "account_name", "manager"]].copy()
    view = base.merge(biz, on="customer_id", how="left").merge(y_cost_df, on="customer_id", how="left").merge(avg_df, on="customer_id", how="left")

    view["bizmoney_balance"] = pd.to_numeric(view.get("bizmoney_balance", 0), errors="coerce").fillna(0).astype("int64")
    view["y_cost"] = pd.to_numeric(view.get("y_cost", 0), errors="coerce").fillna(0).astype("int64")
    view["avg_cost"] = pd.to_numeric(view.get("avg_cost", 0.0), errors="coerce").fillna(0.0).astype(float)

    if "last_update" in view.columns:
        view["last_update"] = pd.to_datetime(view["last_update"], errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
    else:
        view["last_update"] = "-"

    view["days_cover"] = pd.NA
    mask_avg = view["avg_cost"] > 0
    view.loc[mask_avg, "days_cover"] = view.loc[mask_avg, "bizmoney_balance"].astype(float) / view.loc[mask_avg, "avg_cost"].astype(float)

    view["threshold"] = (view["avg_cost"] * float(TOPUP_DAYS_COVER)).fillna(0.0).astype(float)
    view["threshold"] = view["threshold"].apply(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    view["상태"] = "🟢 여유"
    view.loc[view["bizmoney_balance"].astype(float) < view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    def _fmt_days(d):
        if pd.isna(d) or d is None:
            return "-"
        try:
            dd = float(d)
        except Exception:
            return "-"
        if dd > 99:
            return "99+일"
        return f"{dd:.1f}일"

    view["bizmoney_fmt"] = view["bizmoney_balance"].apply(format_currency)
    view["y_cost_fmt"] = view["y_cost"].apply(format_currency)
    view["avg_cost_fmt"] = view["avg_cost"].apply(format_currency)
    view["days_cover_fmt"] = view["days_cover"].apply(_fmt_days)

    # Summary cards
    total_balance = int(view["bizmoney_balance"].sum()) if not view.empty else 0
    count_low_balance = int((view["상태"].astype(str).str.contains("충전필요")).sum()) if not view.empty else 0

    # Monthly budget (only for selected customers to reduce SQL)
    try:
        month_cost_df = get_monthly_cost(engine, f["end"], sel_ids)
    except Exception as e:
        month_cost_df = pd.DataFrame(columns=["customer_id", "current_month_cost"])
        st.warning(f"월 사용액 조회 실패: {e}")

    budget_view = df[["customer_id", "account_name", "manager", "monthly_budget"]].merge(month_cost_df, on="customer_id", how="left")
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)
    total_month_cost = int(budget_view["current_month_cost_val"].sum()) if not budget_view.empty else 0
    budget_view["usage_rate"] = 0.0
    m = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m, "usage_rate"] = budget_view.loc[m, "current_month_cost_val"] / budget_view.loc[m, "monthly_budget_val"]
    count_over_budget = int((budget_view["usage_rate"] >= 1.0).sum()) if not budget_view.empty else 0

    st.markdown("### 🔍 전체 계정 요약 (Command Center)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 비즈머니 잔액", format_currency(total_balance))
    c2.metric(f"{f['end'].month}월 총 사용액", format_currency(total_month_cost))
    c3.metric("충전 필요 계정", f"{count_low_balance}건", delta_color="inverse")
    c4.metric("예산 초과 계정", f"{count_over_budget}건", delta_color="inverse")

    st.divider()

    st.markdown("### 💳 비즈머니 잔액 현황")
    need_topup = count_low_balance
    ok_topup = int(len(view) - need_topup) if not view.empty else 0
    st.markdown(
        f'<span class="badge b-red">충전필요 {need_topup}건</span>'
        f'<span class="badge b-green">여유 {ok_topup}건</span>',
        unsafe_allow_html=True,
    )
    show_only_topup = st.checkbox("충전필요만 보기", value=st.session_state.get("show_only_topup", False), key="show_only_topup")

    view["_rank"] = view["상태"].apply(lambda s: 0 if "충전필요" in str(s) else 1)
    view = view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        view = view[view["상태"].str.contains("충전필요", na=False)].copy()

    st.dataframe(
        view[["account_name","manager","bizmoney_fmt","avg_cost_fmt","days_cover_fmt","y_cost_fmt","상태","last_update"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "account_name": "업체명",
            "manager": "담당자",
            "bizmoney_fmt": st.column_config.TextColumn("비즈머니 잔액"),
            "avg_cost_fmt": st.column_config.TextColumn(f"최근{TOPUP_AVG_DAYS}일 평균소진"),
            "days_cover_fmt": st.column_config.TextColumn("D-소진"),
            "y_cost_fmt": st.column_config.TextColumn("전일 소진액"),
            "상태": "상태",
            "last_update": "확인일자",
        },
    )

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({f['end'].strftime('%Y년 %m월')} 기준)")
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

    budget_view["monthly_budget_edit"] = budget_view["monthly_budget_val"].apply(format_number_commas)
    budget_view["current_month_cost_disp"] = budget_view["current_month_cost_val"].apply(format_number_commas)

    def get_status(rate, budget):
        if budget == 0:
            return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0:
            return ("🔴 초과", "초과", 0)
        if rate >= 0.9:
            return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)

    tmp = budget_view.apply(lambda r: get_status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
    budget_view["status_icon"] = tmp[0]
    budget_view["status_text"] = tmp[1]
    budget_view["_rank"] = tmp[2].astype(int)

    cnt_over = int((budget_view["status_text"] == "초과").sum())
    cnt_warn = int((budget_view["status_text"] == "주의").sum())
    cnt_unset = int((budget_view["status_text"] == "미설정").sum())
    st.markdown(
        f'<span class="badge b-red">초과 {cnt_over}건</span>'
        f'<span class="badge b-yellow">주의 {cnt_warn}건</span>'
        f'<span class="badge b-gray">미설정 {cnt_unset}건</span>',
        unsafe_allow_html=True,
    )

    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

    c1, c2 = st.columns([3, 1])
    with c1:
        edited = st.data_editor(
            budget_view[["customer_id","account_name","manager","monthly_budget_edit","current_month_cost_disp","usage_pct","status_icon"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "customer_id": st.column_config.NumberColumn("CID", disabled=True),
                "account_name": st.column_config.TextColumn("업체명", disabled=True),
                "manager": st.column_config.TextColumn("담당자", disabled=True),
                "monthly_budget_edit": st.column_config.TextColumn("월 예산 (원)", help="예: 500,000", max_chars=20),
                "current_month_cost_disp": st.column_config.TextColumn(f"{f['end'].month}월 사용액", disabled=True),
                "usage_pct": st.column_config.NumberColumn("집행률(%)", format="%.1f", disabled=True),
                "status_icon": st.column_config.TextColumn("상태", disabled=True),
            },
            key="budget_editor_v7_2_0",
        )
    with c2:
        st.markdown(
            """
            <div style="padding:12px 14px; border-radius:12px; background-color:rgba(2,132,199,0.06); line-height:1.85; font-size:14px;">
              <b>상태 가이드</b><br><br>
              🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
              🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
              🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
              ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if st.button("💾 예산 저장 및 업데이트", type="primary", use_container_width=True):
            orig_budget = budget_view.set_index("customer_id")["monthly_budget_val"].to_dict()
            changed = 0
            for _, r in edited.iterrows():
                cid = int(r.get("customer_id", 0))
                if cid == 0:
                    continue
                new_val = parse_currency(r.get("monthly_budget_edit", "0"))
                if new_val != int(orig_budget.get(cid, 0)):
                    update_monthly_budget(engine, cid, new_val)
                    changed += 1

            if changed:
                st.success(f"{changed}건 수정 완료.")
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("변경 없음.")

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🚀 성과 대시보드 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = tuple(int(x) for x in f.get("selected_customer_ids", []) if int(x) > 0)
    type_sel = tuple(f.get("type_sel", []))

    # summary by campaign
    camp = query_campaign_agg(engine, f["start"], f["end"], sel_ids, type_sel)
    if camp is None or camp.empty:
        st.warning("데이터 없음")
        return

    camp = add_rates(camp)
    camp = camp.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")

    # overall KPIs
    curr_imp = float(camp["imp"].sum())
    curr_clk = float(camp["clk"].sum())
    curr_cost = float(camp["cost"].sum())
    curr_conv = float(camp["conv"].sum())
    curr_sales = float(pd.to_numeric(camp.get("sales", 0), errors="coerce").fillna(0).sum())
    curr_ctr = (curr_clk / curr_imp * 100.0) if curr_imp else 0.0
    curr_cpa = (curr_cost / curr_conv) if curr_conv else 0.0
    curr_roas = (curr_sales / curr_cost * 100.0) if curr_cost else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("총 광고비", format_currency(curr_cost))
    c2.metric("총 전환", f"{int(curr_conv):,}")
    c3.metric("전체 CTR", f"{curr_ctr:.1f}%")
    c4.metric("전체 CPA", format_currency(curr_cpa) if curr_conv else "-")
    c5.metric("전체 ROAS", f"{curr_roas:.0f}%" if curr_cost else "-")

    st.divider()

    # Daily chart (SQL)
    daily = query_campaign_daily_sum(engine, f["start"], f["end"], sel_ids, type_sel)
    if daily is None or daily.empty:
        st.info("일별 추세 데이터 없음")
        return

    daily["dt_label"] = daily["dt"].dt.strftime("%m-%d")
    daily["roas"] = daily.apply(lambda r: (r["sales"] / r["cost"] * 100) if r["cost"] > 0 else 0, axis=1)
    daily["cpa"] = daily.apply(lambda r: (r["cost"] / r["conv"]) if r["conv"] > 0 else 0, axis=1)

    st.subheader("📈 일별 추세 분석")
    metric_opt = st.radio("비교할 지표 선택 (우측 Y축)", ["ROAS", "클릭수", "노출수", "전환수", "CPA"], horizontal=True, index=0)

    if metric_opt == "ROAS":
        y_col, y_title, line_color = "roas", "ROAS (%)", "#ef4444"
        daily["tooltip_val"] = daily["roas"].apply(lambda x: f"{x:.0f}%")
    elif metric_opt == "클릭수":
        y_col, y_title, line_color = "clk", "클릭수 (회)", "#10b981"
        daily["tooltip_val"] = daily["clk"].apply(lambda x: f"{int(x):,}")
    elif metric_opt == "노출수":
        y_col, y_title, line_color = "imp", "노출수 (회)", "#f59e0b"
        daily["tooltip_val"] = daily["imp"].apply(lambda x: f"{int(x):,}")
    elif metric_opt == "전환수":
        y_col, y_title, line_color = "conv", "전환수 (건)", "#8b5cf6"
        daily["tooltip_val"] = daily["conv"].apply(lambda x: f"{int(x):,}")
    else:
        y_col, y_title, line_color = "cpa", "CPA (원)", "#ec4899"
        daily["tooltip_val"] = daily["cpa"].apply(lambda x: f"{int(x):,}")

    base = alt.Chart(daily).encode(
        x=alt.X("dt_label:N", title="날짜", sort=alt.SortField(field="dt", order="ascending"), axis=alt.Axis(labelAngle=0))
    )
    tooltip_common = [
        alt.Tooltip("dt:T", title="날짜", format="%Y-%m-%d"),
        alt.Tooltip("cost:Q", title="광고비", format=","),
        alt.Tooltip("tooltip_val:N", title=metric_opt),
    ]
    bar = base.mark_bar(color="#3b82f6", opacity=0.8, width=20).encode(
        y=alt.Y("cost:Q", title="광고비 (원)", axis=alt.Axis(format=",d")), tooltip=tooltip_common
    )
    line = base.mark_line(color=line_color, strokeWidth=3).encode(
        y=alt.Y(f"{y_col}:Q", title=y_title, scale=alt.Scale(zero=False))
    )
    point = base.mark_circle(color=line_color, size=60).encode(
        y=alt.Y(f"{y_col}:Q", axis=None), tooltip=tooltip_common
    )
    chart = alt.layer(bar, line, point).resolve_scale(y="independent")
    st.altair_chart(chart, use_container_width=True)

    st.divider()
    st.subheader("📋 캠페인 상세 리스트 (Top N)")
    with st.form("camp_topn_form"):
        top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, int(st.session_state.get("camp_topn", 300)), 50)
        go = st.form_submit_button("조회", use_container_width=True)
    if go:
        st.session_state["camp_topn"] = int(top_n)
    top_n = int(st.session_state.get("camp_topn", 300))

    show = camp.sort_values("cost", ascending=False).head(top_n).copy()
    show["cost"] = show["cost"].apply(format_currency)
    show["sales"] = pd.to_numeric(show.get("sales", 0), errors="coerce").fillna(0).apply(format_currency)
    show["cpc"] = show["cpc"].apply(format_currency)
    show["cpa"] = show["cpa"].apply(format_currency)
    show["roas_disp"] = show["roas"].apply(format_roas)

    show = show.rename(columns={
        "account_name":"업체명","manager":"담당자","campaign_type_label":"광고유형","campaign_name":"캠페인",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA",
        "sales":"전환매출","roas_disp":"ROAS(%)"
    })
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","광고유형","캠페인","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")
    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_캠페인_{f['start']}_{f['end']}", "campaign", "camp")

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🔑 성과 대시보드 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = tuple(int(x) for x in f.get("selected_customer_ids", []) if int(x) > 0)
    type_sel = tuple(f.get("type_sel", []))

    with st.form("kw_topn_form"):
        top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, int(st.session_state.get("kw_topn", 300)), 50)
        go = st.form_submit_button("조회", use_container_width=True)
    if go:
        st.session_state["kw_topn"] = int(top_n)
    top_n = int(st.session_state.get("kw_topn", 300))

    df = query_keyword_topn(engine, f["start"], f["end"], sel_ids, type_sel, top_n)
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")

    show = df.copy()
    show["cost"] = show["cost"].apply(format_currency)
    show["sales"] = pd.to_numeric(show.get("sales", 0), errors="coerce").fillna(0).apply(format_currency)
    show["cpc"] = show["cpc"].apply(format_currency)
    show["cpa"] = show["cpa"].apply(format_currency)
    show["roas_disp"] = show["roas"].apply(format_roas)

    show = show.rename(columns={
        "account_name":"업체명","manager":"담당자","campaign_name":"캠페인","adgroup_name":"광고그룹","keyword":"키워드",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA",
        "sales":"전환매출","roas_disp":"ROAS(%)","campaign_type_label":"광고유형"
    })
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","광고유형","캠페인","광고그룹","키워드","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")
    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_키워드_{f['start']}_{f['end']}", "keyword", "kw")

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🧩 성과 대시보드 (소재/광고)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = tuple(int(x) for x in f.get("selected_customer_ids", []) if int(x) > 0)
    type_sel = tuple(f.get("type_sel", []))

    with st.form("ad_topn_form"):
        top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, int(st.session_state.get("ad_topn", 300)), 50)
        go = st.form_submit_button("조회", use_container_width=True)
    if go:
        st.session_state["ad_topn"] = int(top_n)
    top_n = int(st.session_state.get("ad_topn", 300))

    df = query_ad_topn(engine, f["start"], f["end"], sel_ids, type_sel, top_n)
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")

    show = df.copy()
    show["cost"] = show["cost"].apply(format_currency)
    show["sales"] = pd.to_numeric(show.get("sales", 0), errors="coerce").fillna(0).apply(format_currency)
    show["cpc"] = show["cpc"].apply(format_currency)
    show["cpa"] = show["cpa"].apply(format_currency)
    show["roas_disp"] = show["roas"].apply(format_roas)

    show = show.rename(columns={
        "account_name":"업체명","manager":"담당자","ad_id":"소재ID","ad_name":"소재내용",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA",
        "sales":"전환매출","roas_disp":"ROAS(%)","campaign_name":"캠페인","adgroup_name":"광고그룹","campaign_type_label":"광고유형"
    })
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","광고유형","캠페인","광고그룹","소재ID","소재내용","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")

    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={"소재내용": st.column_config.TextColumn("소재내용", width="medium")},
    )
    render_download_compact(view_df, f"성과_소재_{f['start']}_{f['end']}", "ad", "ad")

def page_settings(engine):
    st.markdown("## 설정 / 연결")
    try:
        sql_read(engine, "SELECT 1 AS ok")
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return
    if st.button("🔁 accounts.xlsx → DB 동기화"):
        res = seed_from_accounts_xlsx(engine)
        st.success(f"완료: meta {res['meta']}건")
        st.cache_data.clear()
        st.rerun()

# -----------------------------
# Main
# -----------------------------
def main():
    st.title("네이버 검색광고 통합 대시보드")
    st.caption(f"빌드: {BUILD_TAG}")

    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

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
