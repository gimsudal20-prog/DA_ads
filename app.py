# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.3.0)

✅ 이번 버전 핵심
1) (중요) customer_id 타입 불일치(text vs int)로 발생하던
   - bizmoney/yesterday/avg/monthly 비용 조회 실패
   - 키워드/소재/캠페인 TopN 쿼리 실패
   를 전부 해결:
   - SQL에서 customer_id IN 필터는 **항상 문자열 리스트**를 바인딩(expanding bindparam)해서 비교

2) 모바일에서 필터가 안 보이던 문제 해결:
   - 사이드바(햄버거) 의존 X
   - 필터를 메인 화면 상단 Expander로 이동
   - 메뉴도 상단 가로 라디오로 제공

3) 속도 개선(특히 키워드/소재):
   - fact_* 테이블 전체 로드 후 pandas groupby 대신
   - DB에서 기간+계정(+유형) 조건으로 **집계 + TopN**을 바로 수행

4) Streamlit "웹사이트 모드"(메뉴/헤더/푸터 숨김) CSS는 유지하되,
   - 모바일에서 사이드바 토글이 필요 없도록 구조 변경

※ 이 파일을 그대로 app.py로 덮어쓰면 됩니다.
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
from sqlalchemy import create_engine, text, inspect, bindparam
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# BUILD TAG
# -----------------------------
BUILD_TAG = "v7.3.0 (2026-02-17)"

# -----------------------------
# Streamlit Page
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

# -----------------------------
# CONFIG
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# -----------------------------
# Global UI CSS (Website mode)
# -----------------------------
GLOBAL_UI_CSS = """
<style>
  /* Streamlit 기본 크롬 숨김 (소유자 뷰에서 일부는 완전히 숨김이 안 될 수 있음) */
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }
  div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
  div[data-testid="stDecoration"] { display: none; }
  div[data-testid="stStatusWidget"] { visibility: hidden; height: 0px; }

  /* 인덱스 컬럼 숨김(테이블/에디터 공통) */
  thead tr th:first-child { display:none !important; }
  tbody th { display:none !important; }

  h1,h2,h3 { letter-spacing: -0.2px; }

  .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; margin-right:6px; }
  .b-red { background: rgba(239,68,68,0.12); color: rgb(185,28,28); }
  .b-yellow { background: rgba(234,179,8,0.16); color: rgb(161,98,7); }
  .b-green { background: rgba(34,197,94,0.12); color: rgb(21,128,61); }
  .b-gray { background: rgba(148,163,184,0.18); color: rgb(51,65,85); }

  div[data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; background: rgba(2, 132, 199, 0.06); }
</style>
"""

st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)


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
# Formatters
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


def sql_read(engine, sql: str, params: Optional[dict] = None, expanding_keys: Optional[set] = None) -> pd.DataFrame:
    """pandas.read_sql + SQLAlchemy expanding bindparam 안전 래퍼"""
    stmt = text(sql)
    if expanding_keys:
        for k in expanding_keys:
            stmt = stmt.bindparams(bindparam(k, expanding=True))

    with engine.connect() as conn:
        return pd.read_sql(stmt, conn, params=params or {})


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


def normalize_cids(cids: Optional[List[int]]) -> Tuple[str, ...]:
    """DB fact 테이블의 customer_id가 text여도 안전하게 필터링하기 위해 문자열로 통일"""
    if not cids:
        return tuple()
    return tuple([str(int(x)) for x in cids if str(x).strip()])


# -----------------------------
# Campaign Type mapping
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


def labels_to_tps(labels: List[str]) -> Tuple[str, ...]:
    if not labels:
        return tuple()
    lab_set = set([str(x).strip() for x in labels if str(x).strip()])
    tps = sorted([tp for tp, lab in _CAMPAIGN_TP_LABEL.items() if lab in lab_set])
    return tuple(tps)


# -----------------------------
# accounts.xlsx -> meta seed
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
        df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type_label"])

    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type_label"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df


def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []
    present = set(
        [
            x.strip()
            for x in dim_campaign.get("campaign_type_label", pd.Series([], dtype=str)).dropna().astype(str).tolist()
            if x and x.strip() and x.strip() not in ("기타", "미분류", "종합")
        ]
    )
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    opts = [x for x in order if x in present]
    extra = sorted([x for x in present if x not in set(order)])
    return opts + extra


# -----------------------------
# Budget queries (fast + type safe)
# -----------------------------

@st.cache_data(ttl=300, show_spinner=False)
def get_latest_bizmoney(_engine, customer_ids: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"):
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])

    params = {}
    where = ""
    expanding = None
    if customer_ids:
        where = "WHERE customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding = {"customer_ids"}

    sql = f"""
    SELECT DISTINCT ON (customer_id)
      customer_id,
      bizmoney_balance,
      dt as last_update
    FROM fact_bizmoney_daily
    {where}
    ORDER BY customer_id, dt DESC
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def get_yesterday_cost(_engine, d: date, customer_ids: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "y_cost"])

    params = {"d": str(d)}
    where = "WHERE dt = :d"
    expanding = None
    if customer_ids:
        where += " AND customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding = {"customer_ids"}

    sql = f"""
    SELECT customer_id, SUM(cost) AS y_cost
    FROM fact_campaign_daily
    {where}
    GROUP BY customer_id
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "y_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["y_cost"] = pd.to_numeric(df["y_cost"], errors="coerce").fillna(0).astype("int64")
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_recent_avg_cost(_engine, d1: date, d2: date, customer_ids: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    if d2 < d1:
        d1 = d2

    params = {"d1": str(d1), "d2": str(d2)}
    where = "WHERE dt BETWEEN :d1 AND :d2"
    expanding = None
    if customer_ids:
        where += " AND customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding = {"customer_ids"}

    sql = f"""
    SELECT customer_id, SUM(cost) AS sum_cost
    FROM fact_campaign_daily
    {where}
    GROUP BY customer_id
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    days = max((d2 - d1).days + 1, 1)
    df["avg_cost"] = pd.to_numeric(df["sum_cost"], errors="coerce").fillna(0).astype(float) / float(days)
    return df[["customer_id", "avg_cost"]]


@st.cache_data(ttl=600, show_spinner=False)
def get_monthly_cost(_engine, target_date: date, customer_ids: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    start_dt = target_date.replace(day=1)
    if target_date.month == 12:
        end_dt = date(target_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)

    params = {"d1": str(start_dt), "d2": str(end_dt)}
    where = "WHERE dt BETWEEN :d1 AND :d2"
    expanding = None
    if customer_ids:
        where += " AND customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding = {"customer_ids"}

    sql = f"""
    SELECT customer_id, SUM(cost) AS current_month_cost
    FROM fact_campaign_daily
    {where}
    GROUP BY customer_id
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["current_month_cost"] = pd.to_numeric(df["current_month_cost"], errors="coerce").fillna(0).astype("int64")
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
# Performance queries (TopN in DB)
# -----------------------------

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_daily(_engine, d1: date, d2: date, customer_ids: Tuple[str, ...], type_tps: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    # JOIN dim_campaign for type filter (optional)
    join = ""
    where_tp = ""
    expanding = set()
    params = {"d1": str(d1), "d2": str(d2)}

    where = "WHERE f.dt BETWEEN :d1 AND :d2"

    if customer_ids:
        where += " AND f.customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding.add("customer_ids")

    if type_tps:
        join = "LEFT JOIN dim_campaign c ON f.customer_id = c.customer_id AND f.campaign_id = c.campaign_id"
        where_tp = " AND LOWER(COALESCE(c.campaign_tp,'')) IN :type_tps"
        params["type_tps"] = list(type_tps)
        expanding.add("type_tps")

    sql = f"""
    SELECT
      f.dt,
      SUM(f.imp) AS imp,
      SUM(f.clk) AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      SUM(COALESCE(f.sales,0)) AS sales
    FROM fact_campaign_daily f
    {join}
    {where}{where_tp}
    GROUP BY f.dt
    ORDER BY f.dt
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding if expanding else None)
    if df.empty:
        return df

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_topn(_engine, d1: date, d2: date, customer_ids: Tuple[str, ...], type_tps: Tuple[str, ...], topn: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily") or not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()

    expanding = set()
    params = {"d1": str(d1), "d2": str(d2), "lim": int(topn)}

    where = "WHERE f.dt BETWEEN :d1 AND :d2"
    if customer_ids:
        where += " AND f.customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding.add("customer_ids")

    if type_tps:
        where += " AND LOWER(COALESCE(c.campaign_tp,'')) IN :type_tps"
        params["type_tps"] = list(type_tps)
        expanding.add("type_tps")

    sql = f"""
    SELECT
      f.customer_id,
      f.campaign_id,
      MAX(c.campaign_name) AS campaign_name,
      MAX(c.campaign_tp) AS campaign_tp,
      SUM(f.imp) AS imp,
      SUM(f.clk) AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      SUM(COALESCE(f.sales,0)) AS sales
    FROM fact_campaign_daily f
    LEFT JOIN dim_campaign c
      ON f.customer_id = c.customer_id AND f.campaign_id = c.campaign_id
    {where}
    GROUP BY f.customer_id, f.campaign_id
    ORDER BY cost DESC
    LIMIT :lim
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding if expanding else None)
    if df.empty:
        return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df["campaign_type_label"] = df.get("campaign_tp", "").apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    return df


@st.cache_data(ttl=600, show_spinner=False)
def query_keyword_topn(_engine, d1: date, d2: date, customer_ids: Tuple[str, ...], type_tps: Tuple[str, ...], topn: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily") or not (
        table_exists(_engine, "dim_keyword") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")
    ):
        return pd.DataFrame()

    expanding = set()
    params = {"d1": str(d1), "d2": str(d2), "lim": int(topn)}

    where = "WHERE f.dt BETWEEN :d1 AND :d2"
    if customer_ids:
        where += " AND f.customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding.add("customer_ids")

    if type_tps:
        where += " AND LOWER(COALESCE(c.campaign_tp,'')) IN :type_tps"
        params["type_tps"] = list(type_tps)
        expanding.add("type_tps")

    sql = f"""
    SELECT
      f.customer_id,
      f.keyword_id,
      MAX(k.keyword) AS keyword,
      MAX(g.adgroup_name) AS adgroup_name,
      MAX(c.campaign_name) AS campaign_name,
      MAX(c.campaign_tp) AS campaign_tp,
      SUM(f.imp) AS imp,
      SUM(f.clk) AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      SUM(COALESCE(f.sales,0)) AS sales
    FROM fact_keyword_daily f
    LEFT JOIN dim_keyword k
      ON f.customer_id = k.customer_id AND f.keyword_id = k.keyword_id
    LEFT JOIN dim_adgroup g
      ON k.customer_id = g.customer_id AND k.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    {where}
    GROUP BY f.customer_id, f.keyword_id
    ORDER BY cost DESC
    LIMIT :lim
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding if expanding else None)
    if df.empty:
        return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df["campaign_type_label"] = df.get("campaign_tp", "").apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    return df


@st.cache_data(ttl=600, show_spinner=False)
def query_ad_topn(_engine, d1: date, d2: date, customer_ids: Tuple[str, ...], type_tps: Tuple[str, ...], topn: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily") or not (
        table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")
    ):
        return pd.DataFrame()

    expanding = set()
    params = {"d1": str(d1), "d2": str(d2), "lim": int(topn)}

    where = "WHERE f.dt BETWEEN :d1 AND :d2"
    if customer_ids:
        where += " AND f.customer_id IN :customer_ids"
        params["customer_ids"] = list(customer_ids)
        expanding.add("customer_ids")

    if type_tps:
        where += " AND LOWER(COALESCE(c.campaign_tp,'')) IN :type_tps"
        params["type_tps"] = list(type_tps)
        expanding.add("type_tps")

    cols = get_table_columns(_engine, "dim_ad")
    if "creative_text" in cols:
        ad_name_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')"
    else:
        ad_name_expr = "COALESCE(NULLIF(a.ad_name,''), '')"

    sql = f"""
    SELECT
      f.customer_id,
      f.ad_id,
      MAX({ad_name_expr}) AS ad_name,
      MAX(g.adgroup_name) AS adgroup_name,
      MAX(c.campaign_name) AS campaign_name,
      MAX(c.campaign_tp) AS campaign_tp,
      SUM(f.imp) AS imp,
      SUM(f.clk) AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      SUM(COALESCE(f.sales,0)) AS sales
    FROM fact_ad_daily f
    LEFT JOIN dim_ad a
      ON f.customer_id = a.customer_id AND f.ad_id = a.ad_id
    LEFT JOIN dim_adgroup g
      ON a.customer_id = g.customer_id AND a.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    {where}
    GROUP BY f.customer_id, f.ad_id
    ORDER BY cost DESC
    LIMIT :lim
    """

    df = sql_read(_engine, sql, params=params, expanding_keys=expanding if expanding else None)
    if df.empty:
        return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df["campaign_type_label"] = df.get("campaign_tp", "").apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    return df


def add_rates(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    g["revenue"] = g.get("sales", 0)
    g["roas"] = (g["revenue"] / g["cost"].replace({0: pd.NA})) * 100
    return g


# -----------------------------
# UI: Filters (main area)
# -----------------------------

def build_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    today = date.today()

    # defaults (yesterday)
    default_end = today - timedelta(days=1)
    default_start = default_end

    with st.expander("필터", expanded=False):
        c1, c2, c3 = st.columns([2.2, 1.3, 1.5])
        with c1:
            q = st.text_input("업체명 검색", value=st.session_state.get("f_q", ""), placeholder="예: 실리콘플러스")
            st.session_state["f_q"] = q

        with c2:
            managers = sorted([m for m in meta.get("manager", pd.Series([], dtype=str)).fillna("").unique().tolist() if str(m).strip()])
            manager_sel = st.multiselect("담당자", options=managers, default=st.session_state.get("f_mgr", []))
            st.session_state["f_mgr"] = manager_sel

        with c3:
            # company multi-select
            tmp = meta.copy()
            if q:
                tmp = tmp[tmp["account_name"].str.contains(q, case=False, na=False)]
            if manager_sel:
                tmp = tmp[tmp["manager"].isin(manager_sel)]
            company_labels = tmp["account_name"].tolist()
            company_sel = st.multiselect("업체", options=company_labels, default=st.session_state.get("f_company", []))
            st.session_state["f_company"] = company_sel

        st.markdown("---")
        c4, c5 = st.columns([1.4, 2.6])
        with c4:
            period = st.selectbox(
                "기간",
                ["오늘", "어제", "최근 7일(오늘 제외)", "최근 30일(오늘 제외)", "직접 선택"],
                index=int(st.session_state.get("f_period_idx", 1)),
            )
            st.session_state["f_period_idx"] = ["오늘", "어제", "최근 7일(오늘 제외)", "최근 30일(오늘 제외)", "직접 선택"].index(period)

        with c5:
            if period == "오늘":
                start, end = today, today
            elif period == "어제":
                start, end = default_end, default_end
            elif period.startswith("최근 7일"):
                end = today - timedelta(days=1)
                start = end - timedelta(days=6)
            elif period.startswith("최근 30일"):
                end = today - timedelta(days=1)
                start = end - timedelta(days=29)
            else:
                start = st.date_input("시작일", value=st.session_state.get("f_start", default_start))
                end = st.date_input("종료일", value=st.session_state.get("f_end", default_end))
                if isinstance(start, date):
                    st.session_state["f_start"] = start
                if isinstance(end, date):
                    st.session_state["f_end"] = end

            if end < start:
                st.warning("종료일은 시작일 이후여야 합니다. (자동으로 어제로 고정)")
                start, end = default_start, default_end

            st.caption(f"선택 기간: {start} ~ {end}")

        type_sel = st.multiselect(
            "광고유형(선택)",
            options=type_opts,
            default=st.session_state.get("f_types", []),
        )
        st.session_state["f_types"] = type_sel

    # selected customer IDs
    sel_ids: List[int] = []
    if company_sel:
        sel_ids = meta[meta["account_name"].isin(company_sel)]["customer_id"].astype(int).tolist()
    elif manager_sel:
        sel_ids = meta[meta["manager"].isin(manager_sel)]["customer_id"].astype(int).tolist()

    return {
        "q": q,
        "manager_sel": manager_sel,
        "company_sel": company_sel,
        "selected_customer_ids": sel_ids,
        "start": start,
        "end": end,
        "type_sel": type_sel,
    }


# -----------------------------
# Pages
# -----------------------------

def page_budget(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()

    df = meta.copy()

    # apply filters
    if f.get("selected_customer_ids"):
        df = df[df["customer_id"].isin(f["selected_customer_ids"])]
    else:
        if f.get("manager_sel"):
            df = df[df["manager"].isin(f["manager_sel"])]
        if f.get("q"):
            df = df[df["account_name"].str.contains(f["q"], case=False, na=False)]

    cids = normalize_cids(df["customer_id"].astype(int).tolist())

    # ---- Bizmoney / Yesterday / Avg
    yesterday = date.today() - timedelta(days=1)

    try:
        biz = get_latest_bizmoney(engine, cids)
        biz_view = df[["customer_id", "account_name", "manager"]].merge(biz, on="customer_id", how="left")
        biz_view["bizmoney_balance"] = biz_view["bizmoney_balance"].fillna(0)
        if "last_update" in biz_view.columns:
            biz_view["last_update"] = pd.to_datetime(biz_view["last_update"], errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
        else:
            biz_view["last_update"] = "-"
    except Exception as e:
        st.warning(f"비즈머니 조회 실패: {e}")
        biz_view = df[["customer_id", "account_name", "manager"]].copy()
        biz_view["bizmoney_balance"] = 0
        biz_view["last_update"] = "-"

    try:
        y_cost_df = get_yesterday_cost(engine, yesterday, cids)
        biz_view = biz_view.merge(y_cost_df, on="customer_id", how="left")
        biz_view["y_cost"] = biz_view["y_cost"].fillna(0)
    except Exception as e:
        st.warning(f"전일 소진액 조회 실패: {e}")
        biz_view["y_cost"] = 0

    # recent avg based on end date
    avg_df = pd.DataFrame(columns=["customer_id", "avg_cost"])
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1)
        d1 = d2 - timedelta(days=TOPUP_AVG_DAYS - 1)
        try:
            avg_df = get_recent_avg_cost(engine, d1, d2, cids)
        except Exception as e:
            st.warning(f"최근 평균소진 조회 실패(표시는 계속): {e}")

    biz_view = biz_view.merge(avg_df, on="customer_id", how="left")
    biz_view["avg_cost"] = biz_view["avg_cost"].fillna(0.0)

    # 계산
    biz_view["days_cover"] = pd.NA
    mask_avg = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[mask_avg, "days_cover"] = biz_view.loc[mask_avg, "bizmoney_balance"].astype(float) / biz_view.loc[mask_avg, "avg_cost"].astype(float)

    biz_view["threshold"] = biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)
    biz_view["threshold"] = biz_view["threshold"].fillna(0).astype(float)
    biz_view["threshold"] = biz_view["threshold"].apply(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].apply(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].apply(format_currency)
    biz_view["전일 소진액"] = biz_view["y_cost"].apply(format_currency)

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

    biz_view["D-소진"] = biz_view["days_cover"].apply(_fmt_days)

    # ---- Monthly budget view
    try:
        month_cost_df = get_monthly_cost(engine, f["end"], cids)
    except Exception as e:
        st.warning(f"월 사용액 조회 실패: {e}")
        month_cost_df = pd.DataFrame(columns=["customer_id", "current_month_cost"])

    budget_view = df[["customer_id", "account_name", "manager", "monthly_budget"]].merge(month_cost_df, on="customer_id", how="left")
    budget_view["monthly_budget"] = budget_view["monthly_budget"].fillna(0).astype(int)
    budget_view["current_month_cost"] = budget_view["current_month_cost"].fillna(0).astype(int)

    budget_view["집행률"] = 0.0
    mask = budget_view["monthly_budget"] > 0
    budget_view.loc[mask, "집행률"] = budget_view.loc[mask, "current_month_cost"] / budget_view.loc[mask, "monthly_budget"] * 100.0

    # Summary metrics
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(budget_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int((biz_view["상태"].astype(str).str.contains("충전필요")).sum())
    count_over_budget = int((budget_view["집행률"] >= 100.0).sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 비즈머니 잔액", format_currency(total_balance))
    c2.metric(f"{f['end'].month}월 총 사용액", format_currency(total_month_cost))
    c3.metric("충전 필요 계정", f"{count_low_balance}건", delta_color="inverse")
    c4.metric("예산 초과 계정", f"{count_over_budget}건", delta_color="inverse")

    st.divider()

    st.markdown("### 💳 비즈머니 잔액 현황")
    need_topup = count_low_balance
    ok_topup = int(len(biz_view) - need_topup)
    st.markdown(
        f'<span class="badge b-red">충전필요 {need_topup}건</span>'
        f'<span class="badge b-green">여유 {ok_topup}건</span>',
        unsafe_allow_html=True,
    )

    show_only_topup = st.checkbox("충전필요만 보기", value=st.session_state.get("show_only_topup", False), key="show_only_topup")

    biz_view["_rank"] = biz_view["상태"].apply(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    show_biz = biz_view[[
        "account_name",
        "manager",
        "비즈머니 잔액",
        f"최근{TOPUP_AVG_DAYS}일 평균소진",
        "D-소진",
        "전일 소진액",
        "상태",
        "last_update",
    ]].rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "last_update": "확인일자",
    })

    st.dataframe(show_biz, use_container_width=True)

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({f['end'].strftime('%Y년 %m월')} 기준)")

    def status_row(rate_pct: float, budget: int) -> Tuple[str, int]:
        if budget <= 0:
            return "⚪ 미설정", 3
        if rate_pct >= 100:
            return "🔴 초과", 0
        if rate_pct >= 90:
            return "🟡 주의", 1
        return "🟢 적정", 2

    budget_view["status_icon"], budget_view["_rank"] = zip(*budget_view.apply(lambda r: status_row(float(r["집행률"]), int(r["monthly_budget"])), axis=1))

    cnt_over = int((budget_view["status_icon"].astype(str).str.contains("초과")).sum())
    cnt_warn = int((budget_view["status_icon"].astype(str).str.contains("주의")).sum())
    cnt_unset = int((budget_view["status_icon"].astype(str).str.contains("미설정")).sum())
    st.markdown(
        f'<span class="badge b-red">초과 {cnt_over}건</span>'
        f'<span class="badge b-yellow">주의 {cnt_warn}건</span>'
        f'<span class="badge b-gray">미설정 {cnt_unset}건</span>',
        unsafe_allow_html=True,
    )

    show_budget = budget_view.sort_values(["_rank", "집행률", "account_name"], ascending=[True, False, True]).copy()

    show_budget_disp = show_budget[["account_name", "manager", "monthly_budget", "current_month_cost", "집행률", "status_icon"]].copy()
    show_budget_disp["monthly_budget"] = show_budget_disp["monthly_budget"].apply(format_number_commas)
    show_budget_disp["current_month_cost"] = show_budget_disp["current_month_cost"].apply(format_number_commas)
    show_budget_disp["집행률"] = show_budget_disp["집행률"].apply(lambda x: f"{float(x):.1f}%")
    show_budget_disp = show_budget_disp.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "monthly_budget": "월 예산(원)",
        "current_month_cost": f"{f['end'].month}월 사용액",
        "status_icon": "상태",
    })

    st.dataframe(show_budget_disp, use_container_width=True)

    # --- Simple budget editor (stable on mobile)
    with st.expander("예산 수정", expanded=False):
        options = show_budget[["customer_id", "account_name"]].copy().sort_values("account_name")
        if options.empty:
            st.info("수정할 계정이 없습니다.")
        else:
            label_map = {f"{r.account_name} ({int(r.customer_id)})": int(r.customer_id) for r in options.itertuples(index=False)}
            sel_label = st.selectbox("계정 선택", options=list(label_map.keys()))
            cid = label_map[sel_label]
            cur_budget = int(meta.loc[meta["customer_id"] == cid, "monthly_budget"].fillna(0).iloc[0]) if (meta["customer_id"] == cid).any() else 0

            new_budget_str = st.text_input("새 월 예산(원)", value=format_number_commas(cur_budget), help="예: 500,000")
            new_budget = parse_currency(new_budget_str)

            cA, cB = st.columns([1, 3])
            with cA:
                if st.button("💾 저장", type="primary"):
                    update_monthly_budget(engine, cid, new_budget)
                    st.success("저장 완료")
                    st.cache_data.clear()
                    st.rerun()
            with cB:
                st.caption("※ 모바일에서 안정적으로 쓰려고 테이블 직접 편집 대신 선택+입력 방식으로 바꿨어요.")


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🚀 성과 대시보드 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = f.get("selected_customer_ids", [])
    cids = normalize_cids(sel_ids) if sel_ids else tuple()
    type_tps = labels_to_tps(f.get("type_sel", []))

    # daily chart data
    daily = query_campaign_daily(engine, f["start"], f["end"], cids, type_tps)
    if daily.empty:
        st.warning("데이터 없음")
        return

    # metrics
    curr_imp = float(daily["imp"].sum())
    curr_clk = float(daily["clk"].sum())
    curr_cost = float(daily["cost"].sum())
    curr_conv = float(daily["conv"].sum())
    curr_sales = float(daily["sales"].sum())

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

    st.subheader("📈 일별 추세")
    daily = daily.copy()
    daily["dt_label"] = daily["dt"].dt.strftime("%m-%d")
    daily["roas"] = daily.apply(lambda r: (r["sales"] / r["cost"] * 100) if r["cost"] > 0 else 0, axis=1)

    base = alt.Chart(daily).encode(
        x=alt.X("dt_label:N", title="날짜", sort=alt.SortField(field="dt", order="ascending"), axis=alt.Axis(labelAngle=0)),
    )

    bar = base.mark_bar(opacity=0.8, width=18).encode(
        y=alt.Y("cost:Q", title="광고비(원)", axis=alt.Axis(format=",d")),
        tooltip=[
            alt.Tooltip("dt:T", title="날짜", format="%Y-%m-%d"),
            alt.Tooltip("cost:Q", title="광고비", format=","),
            alt.Tooltip("clk:Q", title="클릭", format=","),
            alt.Tooltip("conv:Q", title="전환", format=","),
        ],
    )

    line = base.mark_line(strokeWidth=3).encode(
        y=alt.Y("roas:Q", title="ROAS(%)", scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("roas:Q", title="ROAS(%)", format=".0f")],
    )

    st.altair_chart(alt.layer(bar, line).resolve_scale(y="independent"), use_container_width=True)

    st.divider()

    st.subheader("📋 캠페인 TopN")
    top_n = st.slider("표시 개수(광고비 기준 Top N)", 20, 500, int(st.session_state.get("camp_topn", 100)), 10)
    st.session_state["camp_topn"] = top_n

    topdf = query_campaign_topn(engine, f["start"], f["end"], cids, type_tps, top_n)
    if topdf.empty:
        st.warning("데이터 없음")
        return

    topdf = add_rates(topdf)
    topdf = topdf.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    show = topdf.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_type_label": "광고유형",
        "campaign_name": "캠페인",
        "imp": "노출",
        "clk": "클릭",
        "conv": "전환",
    })

    cols = ["업체명", "담당자", "광고유형", "캠페인", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = show[cols].copy()
    for c in ["노출", "클릭", "전환"]:
        view_df[c] = pd.to_numeric(view_df[c], errors="coerce").fillna(0).astype(int)
    view_df = finalize_ctr_col(view_df, "CTR(%)")

    st.dataframe(view_df, use_container_width=True)


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🔑 성과 대시보드 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = f.get("selected_customer_ids", [])
    cids = normalize_cids(sel_ids) if sel_ids else tuple()
    type_tps = labels_to_tps(f.get("type_sel", []))

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, int(st.session_state.get("kw_topn", 300)), 50)
    st.session_state["kw_topn"] = top_n

    df = query_keyword_topn(engine, f["start"], f["end"], cids, type_tps, top_n)
    if df.empty:
        st.warning("데이터 없음")
        return

    df = add_rates(df)
    df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    show = df.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "keyword": "키워드",
        "imp": "노출",
        "clk": "클릭",
        "conv": "전환",
    })

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "키워드", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = show[cols].copy()
    for c in ["노출", "클릭", "전환"]:
        view_df[c] = pd.to_numeric(view_df[c], errors="coerce").fillna(0).astype(int)
    view_df = finalize_ctr_col(view_df, "CTR(%)")

    st.dataframe(view_df, use_container_width=True)


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 🧩 성과 대시보드 (소재/광고)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    sel_ids = f.get("selected_customer_ids", [])
    cids = normalize_cids(sel_ids) if sel_ids else tuple()
    type_tps = labels_to_tps(f.get("type_sel", []))

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, int(st.session_state.get("ad_topn", 300)), 50)
    st.session_state["ad_topn"] = top_n

    df = query_ad_topn(engine, f["start"], f["end"], cids, type_tps, top_n)
    if df.empty:
        st.warning("데이터 없음")
        return

    df = add_rates(df)
    df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    show = df.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "ad_id": "소재ID",
        "ad_name": "소재내용",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "imp": "노출",
        "clk": "클릭",
        "conv": "전환",
    })

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = show[cols].copy()
    for c in ["노출", "클릭", "전환"]:
        view_df[c] = pd.to_numeric(view_df[c], errors="coerce").fillna(0).astype(int)
    view_df = finalize_ctr_col(view_df, "CTR(%)")

    st.dataframe(view_df, use_container_width=True)


def page_settings(engine):
    st.markdown("## 설정 / 연결")
    try:
        sql_read(engine, "SELECT 1 AS ok")
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button("🔁 accounts.xlsx → DB 동기화"):
            res = seed_from_accounts_xlsx(engine)
            st.success(f"완료: meta {res['meta']}건")
            st.cache_data.clear()
            st.rerun()
    with c2:
        st.caption("accounts.xlsx를 수정했다면 동기화 버튼을 눌러주세요.")


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

    # seed silently
    try:
        seed_from_accounts_xlsx(engine)
    except Exception:
        pass

    meta = get_meta(engine)
    dim_campaign = load_dim_campaign(engine)
    type_opts = get_campaign_type_options(dim_campaign)

    # Menu (mobile friendly)
    pages = ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정/연결"]
    page = st.radio("메뉴", pages, horizontal=True, label_visibility="collapsed", index=int(st.session_state.get("page_idx", 0)))
    st.session_state["page_idx"] = pages.index(page)

    # Filters (main content)
    f = build_filters(meta, type_opts)

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
