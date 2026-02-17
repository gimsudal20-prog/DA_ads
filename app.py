# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.1.1: 키워드/소재 속도 개선 + ProgrammingError 회피 + 웹사이트 모드)

핵심 변경점
1) ✅ NameError( load_dim_campaign 미정의 ) 방지: 모든 함수/페이지를 main() 위에 명확히 정의
2) ✅ 키워드/소재 탭 속도 개선
   - fact_* 테이블을 "원본 그대로 로드"하지 않고, DB에서 먼저 집계(SUM) + cost 기준 Top N만 가져오기
   - dim 테이블도 Top N 범위에 필요한 값만 Join (IN 리스트는 **파라미터 바인딩이 아닌 SQL 리터럴**로 생성 → ProgrammingError 회피)
3) ✅ ProgrammingError 회피
   - SQLAlchemy expanding/ANY/IN 파라미터 리스트 전달을 최대한 제거
   - 날짜/limit 등만 바인딩하고, ID 리스트는 안전한 리터럴 IN (...)로 생성
4) ✅ 웹사이트 모드(크롬 숨김) CSS 유지
"""

import os
import re
import io
from datetime import date, timedelta
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import altair as alt
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

# =============================
# Page config / build tag
# =============================
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")
BUILD_TAG = "v7.1.3 (2026-02-17) - cid 타입 자동판별 + 키워드/예산 ProgrammingError/속도 개선"

# =============================
# Global UI CSS (Website mode)
# =============================
GLOBAL_UI_CSS = """
<style>
  /* 웹사이트 모드: Streamlit 기본 크롬 숨김 (환경/권한에 따라 일부는 소유자에게만 보일 수 있음) */
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }
  div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
  div[data-testid="stDecoration"] { display: none; }
  div[data-testid="stStatusWidget"] { visibility: hidden; height: 0px; }

  section[data-testid="stSidebar"] { padding-top: 8px; }

  /* index column 숨김 */
  thead tr th:first-child { display:none }
  tbody th { display:none }

  /* metric 카드 느낌 */
  div[data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; background: rgba(2, 132, 199, 0.06); }

  /* badge */
  .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; margin-right:6px; }
  .b-red { background: rgba(239,68,68,0.12); color: rgb(185,28,28); }
  .b-yellow { background: rgba(234,179,8,0.16); color: rgb(161,98,7); }
  .b-green { background: rgba(34,197,94,0.12); color: rgb(21,128,61); }
  .b-gray { background: rgba(148,163,184,0.18); color: rgb(51,65,85); }

  /* download button compact */
  div[data-testid="stDownloadButton"] button {
    padding: 0.15rem 0.55rem !important;
    font-size: 0.80rem !important;
    line-height: 1.2 !important;
    min-height: 28px !important;
  }
</style>
"""
st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)

# =============================
# Config / thresholds
# =============================
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# =============================
# Download helpers
# =============================
def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str = "data", key_prefix: str = "") -> None:
    if df is None or df.empty:
        return
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

# =============================
# Live clock
# =============================
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

# =============================
# DB helpers
# =============================
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

# =============================
# Safe IN literal builders (avoid list param binding)
# =============================
def _to_int_list(values: List[Any]) -> List[int]:
    out: List[int] = []
    for v in values or []:
        try:
            out.append(int(v))
        except Exception:
            continue
    return out

def sql_in_ints(values: List[Any]) -> str:
    ints = _to_int_list(values)
    if not ints:
        return "(NULL)"
    return "(" + ",".join(str(x) for x in sorted(set(ints))) + ")"

def sql_in_strs(values: List[Any]) -> str:
    # Quote-safe for SQL literals: ' -> ''
    vals = []
    for v in values or []:
        if v is None:
            continue
        s = str(v)
        s = s.replace("'", "''")
        vals.append(s)
    if not vals:
        return "(NULL)"
    uniq = sorted(set(vals))
    return "(" + ",".join("'" + x + "'" for x in uniq) + ")"


@st.cache_data(ttl=3600, show_spinner=False)
def get_column_type(_engine, table: str, column: str, schema: str = "public") -> str:
    """Return lowercased column type string (best-effort)."""
    try:
        insp = inspect(_engine)
        cols = insp.get_columns(table, schema=schema)
        for c in cols:
            if str(c.get("name", "")).lower() == str(column).lower():
                return str(c.get("type", "")).lower()
    except Exception:
        return ""
    return ""

def customer_id_filter_sql(_engine, table: str, customer_ids, col_expr: str = "customer_id") -> str:
    """
    customer_id 컬럼 타입(TEXT/BIGINT 등) 환경차이를 자동으로 흡수해서
    IN 필터가 ProgrammingError로 터지지 않게 만드는 안전 필터.
    - INT 계열이면: col_expr IN (1,2,3)
    - TEXT 계열이면: col_expr IN ('1','2','3')
    - 타입을 못 찾으면: col_expr::text IN ('1','2','3') (안전 fallback)
    """
    cids = _to_int_list(customer_ids)
    if not cids:
        return ""
    t = get_column_type(_engine, table, "customer_id")
    if t and "int" in t:
        return f" AND {col_expr} IN {sql_in_ints(cids)} "
    if t and ("char" in t or "text" in t or "uuid" in t):
        return f" AND {col_expr} IN {sql_in_strs([str(x) for x in cids])} "
    # unknown type → safest
    return f" AND {col_expr}::text IN {sql_in_strs([str(x) for x in cids])} "

# =============================
# Utilities / formatters
# =============================
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
    if not s:
        return 0
    return int(s)

# =============================
# Campaign type mapping
# =============================
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

# =============================
# Accounts meta seed (optional)
# =============================
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
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
        df["manager"] = df.get("manager", "").fillna("").astype(str)
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

# =============================
# Core loaders
# =============================
@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type_label"])

    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type_label"])
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df

@st.cache_data(ttl=300, show_spinner=False)
def get_latest_bizmoney(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"):
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
    sql = """
    SELECT DISTINCT ON (customer_id) customer_id, bizmoney_balance, dt as last_update
    FROM fact_bizmoney_daily
    ORDER BY customer_id, dt DESC
    """
    df = sql_read(_engine, sql)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600, show_spinner=False)
def get_monthly_cost(_engine, target_date: date) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    start_dt = target_date.replace(day=1)
    if target_date.month == 12:
        end_dt = date(target_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)

    sql = """
    SELECT customer_id, SUM(cost) as current_month_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    GROUP BY customer_id
    """
    df = sql_read(_engine, sql, {"d1": str(start_dt), "d2": str(end_dt)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["current_month_cost"] = pd.to_numeric(df["current_month_cost"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600, show_spinner=False)
def get_recent_avg_cost(_engine, d1: date, d2: date) -> pd.DataFrame:
    """기간 내 customer_id별 sum(cost)을 가져와 평균/일로 변환 (ID 리스트 파라미터 없음)"""
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])
    if d2 < d1:
        d1 = d2

    sql = """
    SELECT customer_id, SUM(cost) AS sum_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
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

# =============================
# Aggregation loaders for speed (Top N)
# =============================
def _customer_filter_sql(customer_ids: Optional[List[int]]) -> str:
    cids = _to_int_list(customer_ids)
    if not cids:
        return ""
    # customer_id 컬럼 타입(TEXT/BIGINT) 환경 차이로 인한 ProgrammingError 방지
    cid_sql = sql_in_strs([str(x) for x in cids])
    return f" AND customer_id::text IN {cid_sql} "

@st.cache_data(ttl=600, show_spinner=False)
def load_campaign_agg(_engine, d1: date, d2: date, customer_ids: Optional[List[int]], type_sel: List[str]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    # aggregate
    cid_filter = customer_id_filter_sql(_engine, "fact_campaign_daily", customer_ids, col_expr="customer_id")
    sql = f"""
    SELECT customer_id, campaign_id,
           SUM(imp) AS imp,
           SUM(clk) AS clk,
           SUM(cost) AS cost,
           SUM(conv) AS conv,
           SUM(COALESCE(sales,0)) AS sales
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    {cid_filter}
    GROUP BY customer_id, campaign_id
    """
    fact = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if fact.empty:
        return fact

    fact["customer_id"] = pd.to_numeric(fact["customer_id"], errors="coerce").fillna(0).astype("int64")

    # type filter (join on dim_campaign in pandas; type_sel empty => keep all but '기타' drop later)
    dim_campaign = load_dim_campaign(_engine)
    if not dim_campaign.empty:
        dc = dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]].copy()
        fact = fact.merge(dc, on=["customer_id", "campaign_id"], how="left")
        fact["campaign_type_label"] = fact["campaign_type_label"].fillna("").astype(str).str.strip()
        fact.loc[fact["campaign_type_label"] == "", "campaign_type_label"] = "기타"
        fact = fact[fact["campaign_type_label"] != "기타"]
        if type_sel:
            fact = fact[fact["campaign_type_label"].isin(type_sel)]
    return fact

@st.cache_data(ttl=600, show_spinner=False)
def load_keyword_agg_topn(_engine, d1: date, d2: date, customer_ids: Optional[List[int]], type_sel: List[str], top_n: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()

    top_n = int(max(min(top_n, 2000), 50))
    cid_filter = customer_id_filter_sql(_engine, "fact_keyword_daily", customer_ids, col_expr="customer_id")
    sql = f"""
    SELECT customer_id, keyword_id,
           SUM(imp) AS imp,
           SUM(clk) AS clk,
           SUM(cost) AS cost,
           SUM(conv) AS conv,
           SUM(COALESCE(sales,0)) AS sales
    FROM fact_keyword_daily
    WHERE dt BETWEEN :d1 AND :d2
    {cid_filter}
    GROUP BY customer_id, keyword_id
    ORDER BY SUM(cost) DESC
    LIMIT :lim
    """
    fact = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if fact.empty:
        return fact
    fact["customer_id"] = pd.to_numeric(fact["customer_id"], errors="coerce").fillna(0).astype("int64")

    # type filter (fast map based on adgroup->campaign->tp). Avoid list params.
    # We build a minimal join using customer_ids & keyword_ids IN literals (not params).
    kwids = fact["keyword_id"].dropna().astype(str).unique().tolist()
    cids = fact["customer_id"].dropna().astype(int).unique().tolist()
    dimj = load_keyword_dim_join(_engine, cids, kwids)  # safe (literals)
    if not dimj.empty:
        fact = fact.merge(dimj[["customer_id", "keyword_id", "campaign_type_label"]], on=["customer_id", "keyword_id"], how="left")
        fact["campaign_type_label"] = fact["campaign_type_label"].fillna("").astype(str).str.strip()
        fact.loc[fact["campaign_type_label"] == "", "campaign_type_label"] = "기타"
        fact = fact[fact["campaign_type_label"] != "기타"]
        if type_sel:
            fact = fact[fact["campaign_type_label"].isin(type_sel)]
    else:
        # dim join 실패 시 그래도 보여주되, type filter는 건너뜀
        pass

    return fact

@st.cache_data(ttl=600, show_spinner=False)
def load_ad_agg_topn(_engine, d1: date, d2: date, customer_ids: Optional[List[int]], type_sel: List[str], top_n: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()

    top_n = int(max(min(top_n, 2000), 50))
    cid_filter = customer_id_filter_sql(_engine, "fact_ad_daily", customer_ids, col_expr="customer_id")
    sql = f"""
    SELECT customer_id, ad_id,
           SUM(imp) AS imp,
           SUM(clk) AS clk,
           SUM(cost) AS cost,
           SUM(conv) AS conv,
           SUM(COALESCE(sales,0)) AS sales
    FROM fact_ad_daily
    WHERE dt BETWEEN :d1 AND :d2
    {cid_filter}
    GROUP BY customer_id, ad_id
    ORDER BY SUM(cost) DESC
    LIMIT :lim
    """
    fact = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if fact.empty:
        return fact
    fact["customer_id"] = pd.to_numeric(fact["customer_id"], errors="coerce").fillna(0).astype("int64")

    adids = fact["ad_id"].dropna().astype(str).unique().tolist()
    cids = fact["customer_id"].dropna().astype(int).unique().tolist()
    dimj = load_ad_dim_join(_engine, cids, adids)
    if not dimj.empty:
        fact = fact.merge(dimj[["customer_id", "ad_id", "campaign_type_label"]], on=["customer_id", "ad_id"], how="left")
        fact["campaign_type_label"] = fact["campaign_type_label"].fillna("").astype(str).str.strip()
        fact.loc[fact["campaign_type_label"] == "", "campaign_type_label"] = "기타"
        fact = fact[fact["campaign_type_label"] != "기타"]
        if type_sel:
            fact = fact[fact["campaign_type_label"].isin(type_sel)]
    return fact

# =============================
# Dim joins (TopN 범위만) - ProgrammingError 회피 버전
# =============================
@st.cache_data(ttl=3600, show_spinner=False)
def load_keyword_dim_join(_engine, customer_ids: List[int], keyword_ids: List[str]) -> pd.DataFrame:
    """
    dim_keyword + dim_adgroup + dim_campaign 를 TopN 키워드만 기준으로 join.
    - customer_ids/keyword_ids 를 SQL 파라미터로 넘기지 않고, IN (...) 리터럴로 생성.
    """
    if not (table_exists(_engine, "dim_keyword") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame(columns=["customer_id","keyword_id","keyword","adgroup_id","adgroup_name","campaign_id","campaign_name","campaign_type_label"])

    cids = _to_int_list(customer_ids)
    # keyword_id는 텍스트/숫자 섞일 수 있어 str로 통일
    kwids = [str(x) for x in (keyword_ids or []) if x is not None]

    # 너무 커지면(keyword_ids가 과도하게 많으면) customer_ids만으로 제한 (쿼리 길이 폭주 방지)
    kw_filter = ""
    if 0 < len(kwids) <= 2000:
        kw_filter = f" AND k.keyword_id IN {sql_in_strs(kwids)} "

    cid_filter = ""
    if cids:
        cid_filter = customer_id_filter_sql(_engine, "dim_keyword", cids, col_expr="k.customer_id")

    sql = f"""
    SELECT
      k.customer_id,
      k.keyword_id,
      k.keyword,
      k.adgroup_id,
      g.adgroup_name,
      g.campaign_id,
      c.campaign_name,
      COALESCE(NULLIF(TRIM(c.campaign_tp), ''), '') AS campaign_tp
    FROM dim_keyword k
    LEFT JOIN dim_adgroup g
      ON k.customer_id = g.customer_id AND k.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    WHERE 1=1
      {cid_filter}
      {kw_filter}
    """
    df = sql_read(_engine, sql)
    if df.empty:
        return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df.drop(columns=["campaign_tp"], errors="ignore")

@st.cache_data(ttl=3600, show_spinner=False)
def load_ad_dim_join(_engine, customer_ids: List[int], ad_ids: List[str]) -> pd.DataFrame:
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame(columns=["customer_id","ad_id","ad_name","adgroup_id","campaign_id","campaign_name","campaign_type_label"])

    cids = _to_int_list(customer_ids)
    adids = [str(x) for x in (ad_ids or []) if x is not None]

    ad_filter = ""
    if 0 < len(adids) <= 2000:
        ad_filter = f" AND a.ad_id IN {sql_in_strs(adids)} "

    cid_filter = ""
    if cids:
        cid_filter = customer_id_filter_sql(_engine, "dim_ad", cids, col_expr="a.customer_id")

    cols = get_table_columns(_engine, "dim_ad")
    if "creative_text" in cols:
        ad_name_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')"
    else:
        ad_name_expr = "COALESCE(NULLIF(a.ad_name,''), '')"

    sql = f"""
    SELECT
      a.customer_id,
      a.ad_id,
      {ad_name_expr} AS ad_name,
      a.adgroup_id,
      g.campaign_id,
      c.campaign_name,
      COALESCE(NULLIF(TRIM(c.campaign_tp), ''), '') AS campaign_tp
    FROM dim_ad a
    LEFT JOIN dim_adgroup g
      ON a.customer_id = g.customer_id AND a.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    WHERE 1=1
      {cid_filter}
      {ad_filter}
    """
    df = sql_read(_engine, sql)
    if df.empty:
        return df
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df.drop(columns=["campaign_tp"], errors="ignore")

# =============================
# KPI helpers
# =============================
def add_rates(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    g["revenue"] = g.get("sales", 0)
    g["roas"] = (g["revenue"] / g["cost"].replace({0: pd.NA})) * 100
    return g

def calculate_delta(curr: float, prev: float, is_percent: bool = False, inverse: bool = False):
    if prev == 0:
        return None, "off"
    diff = curr - prev
    if is_percent:
        val_str = f"{diff:+.1f}%p"
    else:
        val_str = f"{diff:+,.0f}"
    color = "inverse" if inverse else "normal"
    return val_str, color

# =============================
# Sidebar filters
# =============================
def sidebar_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict[str, Any]:
    st.sidebar.title("필터")

    with st.sidebar.expander("업체/담당자", expanded=True):
        q = st.text_input("업체명 검색", placeholder="예: 실리콘플러스")
        managers = sorted([m for m in meta.get("manager", pd.Series([], dtype=str)).fillna("").unique().tolist() if str(m).strip()])
        manager_sel = st.multiselect("담당자", options=managers, default=[])

        tmp = meta.copy()
        if q:
            tmp = tmp[tmp["account_name"].str.contains(q, case=False, na=False)]
        if manager_sel:
            tmp = tmp[tmp["manager"].isin(manager_sel)]

        opt = tmp[["account_name", "customer_id"]].copy() if not tmp.empty else pd.DataFrame(columns=["account_name","customer_id"])
        opt["label"] = opt["account_name"]
        labels = opt["label"].tolist()
        company_sel_labels = st.multiselect("업체", options=labels, default=[])
        sel_ids = opt[opt["label"].isin(company_sel_labels)]["customer_id"].astype(int).tolist() if company_sel_labels else []

    with st.sidebar.expander("기간", expanded=True):
        period = st.selectbox("기간", ["오늘", "어제", "최근 7일(오늘 제외)", "최근 30일(오늘 제외)", "직접 선택"], index=1)
        today = date.today()

        if period == "오늘":
            start, end = today, today
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
            c1, c2 = st.columns(2)
            start = c1.date_input("시작일", value=today - timedelta(days=7))
            end = c2.date_input("종료일", value=today - timedelta(days=1))
            if end < start:
                st.warning("종료일은 시작일 이후여야 합니다.")
        st.caption(f"선택 기간: {start} ~ {end}")

    with st.sidebar.expander("광고유형", expanded=True):
        type_sel = st.multiselect("검색광고 종류", options=type_opts, default=[])
        st.caption("※ '기타' 유형은 자동으로 제외됩니다.")

    return {"q": q, "manager_sel": manager_sel, "selected_customer_ids": sel_ids, "start": start, "end": end, "type_sel": type_sel}

def resolve_selected_ids(meta: pd.DataFrame, f: Dict[str, Any]) -> List[int]:
    sel_ids = f.get("selected_customer_ids", [])
    if (not sel_ids) and f.get("manager_sel"):
        sel_ids = meta[meta["manager"].isin(f["manager_sel"])]["customer_id"].astype(int).tolist()
    return [int(x) for x in sel_ids] if sel_ids else []

# =============================
# Pages
# =============================
def page_budget(meta: pd.DataFrame, engine, f: Dict[str, Any]):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()

    df = meta.copy()
    sel_ids = resolve_selected_ids(meta, f)
    if sel_ids:
        df = df[df["customer_id"].isin(sel_ids)]
    else:
        if f.get("manager_sel"):
            df = df[df["manager"].isin(f["manager_sel"])]
        if f.get("q"):
            df = df[df["account_name"].str.contains(f["q"], case=False, na=False)]
        if f.get("selected_customer_ids"):
            df = df[df["customer_id"].isin(f["selected_customer_ids"])]

    biz = get_latest_bizmoney(engine)

    # yesterday cost (small query)
    yesterday = date.today() - timedelta(days=1)
    y_cost = pd.DataFrame(columns=["customer_id", "y_cost"])
    if table_exists(engine, "fact_campaign_daily") and not df.empty:
        cid_filter = customer_id_filter_sql(engine, "fact_campaign_daily", df["customer_id"].astype(int).tolist(), col_expr="customer_id")
        sql = f"""
        SELECT customer_id, SUM(cost) AS y_cost
        FROM fact_campaign_daily
      WHERE dt = :d {cid_filter}
        GROUP BY customer_id
        """
        y_cost = sql_read(engine, sql, {"d": str(yesterday)})
        if not y_cost.empty:
            y_cost["customer_id"] = pd.to_numeric(y_cost["customer_id"], errors="coerce").fillna(0).astype("int64")
            y_cost["y_cost"] = pd.to_numeric(y_cost["y_cost"], errors="coerce").fillna(0).astype("int64")

    # base view
    if not biz.empty:
        biz_view = df[["customer_id", "account_name", "manager"]].merge(biz, on="customer_id", how="left")
        biz_view["bizmoney_balance"] = biz_view["bizmoney_balance"].fillna(0)
        if "last_update" in biz_view.columns:
            biz_view["last_update"] = pd.to_datetime(biz_view["last_update"], errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
    else:
        biz_view = df[["customer_id", "account_name", "manager"]].copy()
        biz_view["bizmoney_balance"] = 0
        biz_view["last_update"] = "-"

    if not y_cost.empty:
        biz_view = biz_view.merge(y_cost, on="customer_id", how="left")
        biz_view["y_cost"] = biz_view["y_cost"].fillna(0)
    else:
        biz_view["y_cost"] = 0

    # recent avg cost (no id-list params)
    avg_df = pd.DataFrame(columns=["customer_id", "avg_cost"])
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1)
        d1 = d2 - timedelta(days=TOPUP_AVG_DAYS - 1)
        try:
            avg_df = get_recent_avg_cost(engine, d1, d2)
        except Exception as e:
            st.warning(f"최근 평균소진 계산 실패(표시는 계속): {e}")

    if not avg_df.empty:
        biz_view = biz_view.merge(avg_df, on="customer_id", how="left")
        biz_view["avg_cost"] = biz_view["avg_cost"].fillna(0.0)
    else:
        biz_view["avg_cost"] = 0.0

    # vector calc
    biz_view["days_cover"] = pd.NA
    mask_avg = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[mask_avg, "days_cover"] = (
        biz_view.loc[mask_avg, "bizmoney_balance"].astype(float) / biz_view.loc[mask_avg, "avg_cost"].astype(float)
    )

    biz_view["threshold"] = biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)
    biz_view["threshold"] = biz_view["threshold"].fillna(0).astype(float)
    biz_view["threshold"] = biz_view["threshold"].apply(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    biz_view["bizmoney_fmt"] = biz_view["bizmoney_balance"].apply(format_currency)
    biz_view["y_cost_fmt"] = biz_view["y_cost"].apply(format_currency)
    biz_view["avg_cost_fmt"] = biz_view["avg_cost"].apply(format_currency)

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
    biz_view["days_cover_fmt"] = biz_view["days_cover"].apply(_fmt_days)

    month_cost_df = get_monthly_cost(engine, f["end"])
    budget_view = df[["customer_id", "account_name", "manager", "monthly_budget"]].merge(month_cost_df, on="customer_id", how="left")
    budget_view["monthly_budget_val"] = budget_view["monthly_budget"].fillna(0).astype(int)
    budget_view["current_month_cost_val"] = budget_view["current_month_cost"].fillna(0).astype(int)

    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(budget_view["current_month_cost_val"], errors="coerce").fillna(0).sum())
    count_low_balance = int((biz_view["상태"].astype(str).str.contains("충전필요")).sum())

    budget_view["usage_rate"] = 0.0
    mask = budget_view["monthly_budget_val"] > 0
    budget_view.loc[mask, "usage_rate"] = budget_view.loc[mask, "current_month_cost_val"] / budget_view.loc[mask, "monthly_budget_val"]
    count_over_budget = int((budget_view["usage_rate"] >= 1.0).sum())

    st.markdown("### 🔍 전체 계정 요약 (Command Center)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 비즈머니 잔액", format_currency(total_balance))
    c2.metric(f"{f['end'].month}월 총 사용액", format_currency(total_month_cost))
    c3.metric("충전 필요 계정", f"{count_low_balance}건", delta_color="inverse")
    c4.metric("예산 초과 계정", f"{count_over_budget}건", delta_color="inverse", delta="100% 이상" if count_over_budget > 0 else None)

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

    st.dataframe(
        biz_view[["account_name","manager","bizmoney_fmt","avg_cost_fmt","days_cover_fmt","y_cost_fmt","상태","last_update"]],
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
            key="budget_editor_v7_1_1",
        )

    with c2:
        st.markdown(
            """
            <div style="padding:12px 14px;border-radius:12px;background-color:rgba(2,132,199,0.06);line-height:1.85;font-size:14px;">
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

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict[str, Any], dim_campaign: pd.DataFrame):
    st.markdown("## 🚀 성과 대시보드 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")
    type_sel = f.get("type_sel", [])
    sel_ids = resolve_selected_ids(meta, f)

    fact = load_campaign_agg(engine, f["start"], f["end"], sel_ids if sel_ids else None, type_sel)
    if fact.empty:
        st.warning("데이터 없음")
        return

    # prev period
    duration = (f["end"] - f["start"]).days + 1
    prev_end = f["start"] - timedelta(days=1)
    prev_start = prev_end - timedelta(days=duration - 1)
    fact_prev = load_campaign_agg(engine, prev_start, prev_end, sel_ids if sel_ids else None, type_sel)

    # KPIs
    curr_imp = float(fact.get("imp", pd.Series([0])).sum())
    curr_clk = float(fact.get("clk", pd.Series([0])).sum())
    curr_cost = float(fact.get("cost", pd.Series([0])).sum())
    curr_conv = float(fact.get("conv", pd.Series([0])).sum())
    curr_sales = float(pd.to_numeric(fact.get("sales", pd.Series([0])), errors="coerce").fillna(0).sum())

    curr_ctr = (curr_clk / curr_imp * 100.0) if curr_imp else 0.0
    curr_cpa = (curr_cost / curr_conv) if curr_conv else 0.0
    curr_roas = (curr_sales / curr_cost * 100.0) if curr_cost else 0.0

    prev_imp = float(fact_prev.get("imp", pd.Series([0])).sum()) if not fact_prev.empty else 0.0
    prev_clk = float(fact_prev.get("clk", pd.Series([0])).sum()) if not fact_prev.empty else 0.0
    prev_cost = float(fact_prev.get("cost", pd.Series([0])).sum()) if not fact_prev.empty else 0.0
    prev_conv = float(fact_prev.get("conv", pd.Series([0])).sum()) if not fact_prev.empty else 0.0
    prev_sales = float(pd.to_numeric(fact_prev.get("sales", pd.Series([0])), errors="coerce").fillna(0).sum()) if not fact_prev.empty else 0.0

    prev_ctr = (prev_clk / prev_imp * 100.0) if prev_imp else 0.0
    prev_cpa = (prev_cost / prev_conv) if prev_conv else 0.0
    prev_roas = (prev_sales / prev_cost * 100.0) if prev_cost else 0.0

    d_cost, _ = calculate_delta(curr_cost, prev_cost, inverse=False)
    d_conv, _ = calculate_delta(curr_conv, prev_conv)
    d_ctr, _ = calculate_delta(curr_ctr, prev_ctr, is_percent=True)
    d_cpa, c_cpa = calculate_delta(curr_cpa, prev_cpa, inverse=True)
    d_roas, _ = calculate_delta(curr_roas, prev_roas, is_percent=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("총 광고비", format_currency(curr_cost), delta=d_cost, delta_color="normal")
    c2.metric("총 전환", f"{int(curr_conv):,}", delta=d_conv)
    c3.metric("전체 CTR", f"{curr_ctr:.1f}%", delta=d_ctr)
    c4.metric("전체 CPA", format_currency(curr_cpa) if curr_conv else "-", delta=d_cpa, delta_color=c_cpa)
    c5.metric("전체 ROAS", f"{curr_roas:.0f}%" if curr_cost else "-", delta=d_roas)

    st.caption(f"※ 비교 기간: {prev_start} ~ {prev_end}")
    st.divider()

    # Detail table
    dc = dim_campaign[["customer_id","campaign_id","campaign_name","campaign_type_label"]].copy() if not dim_campaign.empty else pd.DataFrame()
    if not dc.empty:
        dc["customer_id"] = pd.to_numeric(dc["customer_id"], errors="coerce").fillna(0).astype("int64")

    g = fact.copy()
    g = add_rates(g)
    g = g.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")
    if not dc.empty:
        g = g.merge(dc, on=["customer_id","campaign_id"], how="left")

    show = g.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={"account_name":"업체명","manager":"담당자","campaign_type_label":"광고유형","campaign_name":"캠페인","imp":"노출","clk":"클릭","conv":"전환"})
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)
    view_df = finalize_ctr_col(show[["업체명","담당자","광고유형","캠페인","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]].copy(), "CTR(%)")
    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_캠페인_{f['start']}_{f['end']}", "campaign", "camp")

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict[str, Any], dim_campaign: pd.DataFrame):
    st.markdown("## 🔑 성과 대시보드 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    type_sel = f.get("type_sel", [])
    sel_ids = resolve_selected_ids(meta, f)

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, 300, 50, key="kw_topn_v7_1_1")
    fact = load_keyword_agg_topn(engine, f["start"], f["end"], sel_ids if sel_ids else None, type_sel, int(top_n))
    if fact.empty:
        st.warning("데이터 없음")
        return

    # dim join for display
    kwids = fact["keyword_id"].dropna().astype(str).unique().tolist()
    cids = fact["customer_id"].dropna().astype(int).unique().tolist()
    dimj = load_keyword_dim_join(engine, cids, kwids)

    g = fact.copy()
    g = add_rates(g)
    g = g.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")
    if not dimj.empty:
        g = g.merge(dimj, on=["customer_id","keyword_id"], how="left")
    g["keyword"] = g.get("keyword", "").fillna("").astype(str)
    g["adgroup_name"] = g.get("adgroup_name", "").fillna("").astype(str)
    g["campaign_name"] = g.get("campaign_name", "").fillna("").astype(str)

    show = g.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={"account_name":"업체명","manager":"담당자","campaign_name":"캠페인","adgroup_name":"광고그룹","keyword":"키워드","imp":"노출","clk":"클릭","conv":"전환"})
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","캠페인","광고그룹","키워드","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")

    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_키워드_{f['start']}_{f['end']}", "keyword", "kw")

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict[str, Any], dim_campaign: pd.DataFrame):
    st.markdown("## 🧩 성과 대시보드 (소재/광고)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    type_sel = f.get("type_sel", [])
    sel_ids = resolve_selected_ids(meta, f)

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, 300, 50, key="ad_topn_v7_1_1")
    fact = load_ad_agg_topn(engine, f["start"], f["end"], sel_ids if sel_ids else None, type_sel, int(top_n))
    if fact.empty:
        st.warning("데이터 없음")
        return

    adids = fact["ad_id"].dropna().astype(str).unique().tolist()
    cids = fact["customer_id"].dropna().astype(int).unique().tolist()
    dimj = load_ad_dim_join(engine, cids, adids)

    g = fact.copy()
    g = add_rates(g)
    g = g.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")
    if not dimj.empty:
        g = g.merge(dimj, on=["customer_id","ad_id"], how="left")
    g["ad_name"] = g.get("ad_name", "").fillna("").astype(str)

    show = g.copy()
    show["광고비"] = show["cost"].apply(format_currency)
    show["전환매출"] = show["sales"].apply(format_currency)
    show["CPC"] = show["cpc"].apply(format_currency)
    show["CPA"] = show["cpa"].apply(format_currency)
    show["ROAS(%)"] = show["roas"].apply(format_roas)
    show["CTR(%)"] = show["ctr"]

    show = show.rename(columns={"account_name":"업체명","manager":"담당자","ad_id":"소재ID","ad_name":"소재내용","imp":"노출","clk":"클릭","conv":"전환"})
    for c in ["노출","클릭","전환"]:
        show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","소재ID","소재내용","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","전환매출","ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")

    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={"소재내용": st.column_config.TextColumn("소재내용", width="medium")},
    )
    render_download_compact(view_df, f"성과_소재_{f['start']}_{f['end']}", "ad", "ad")

# =============================
# Settings / Speed: indexes
# =============================
INDEX_SQL = [
    # fact tables
    "CREATE INDEX IF NOT EXISTS idx_fact_campaign_daily_dt_cid ON fact_campaign_daily (dt, customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_campaign_daily_cid_camp ON fact_campaign_daily (customer_id, campaign_id, dt);",
    "CREATE INDEX IF NOT EXISTS idx_fact_keyword_daily_dt_cid ON fact_keyword_daily (dt, customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_keyword_daily_cid_kw_dt ON fact_keyword_daily (customer_id, keyword_id, dt);",
    "CREATE INDEX IF NOT EXISTS idx_fact_ad_daily_dt_cid ON fact_ad_daily (dt, customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_ad_daily_cid_ad_dt ON fact_ad_daily (customer_id, ad_id, dt);",
    # dim tables
    "CREATE INDEX IF NOT EXISTS idx_dim_campaign_cid_camp ON dim_campaign (customer_id, campaign_id);",
    "CREATE INDEX IF NOT EXISTS idx_dim_adgroup_cid_ag ON dim_adgroup (customer_id, adgroup_id);",
    "CREATE INDEX IF NOT EXISTS idx_dim_keyword_cid_kw ON dim_keyword (customer_id, keyword_id);",
    "CREATE INDEX IF NOT EXISTS idx_dim_ad_cid_ad ON dim_ad (customer_id, ad_id);",
]

def page_settings(engine):
    st.markdown("## 설정 / 연결 / 속도")
    st.caption(f"빌드: {BUILD_TAG}")

    # DB status
    try:
        sql_read(engine, "SELECT 1 AS ok")
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🧹 캐시 초기화 (권장: 코드 배포 직후)", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("캐시 초기화 완료. 새로고침/재실행됩니다.")
            st.rerun()

    with c2:
        if st.button("🔁 accounts.xlsx → DB 동기화", use_container_width=True):
            res = seed_from_accounts_xlsx(engine)
            st.success(f"완료: meta {res['meta']}건")
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.markdown("### 🚀 속도 최적화(인덱스)")
    st.caption("※ DB 권한이 있으면 한 번만 실행해두면, 키워드/소재 탭이 체감으로 빨라집니다.")
    if st.button("⚡ 인덱스 생성/보강 실행", type="primary"):
        ok = 0
        fail = 0
        for s in INDEX_SQL:
            try:
                sql_exec(engine, s)
                ok += 1
            except Exception:
                fail += 1
        st.success(f"인덱스 실행 완료: 성공 {ok} / 실패 {fail}")
        st.caption("실패가 있어도 일부는 이미 존재하거나 권한 이슈일 수 있어요.")
        st.cache_data.clear()

# =============================
# Main
# =============================
def main():
    st.title("네이버 검색광고 통합 대시보드")
    st.caption(f"빌드: {BUILD_TAG}")

    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

    # seed (only if table missing/empty) - 가벼운 시도
    try:
        if os.path.exists(ACCOUNTS_XLSX):
            if not table_exists(engine, "dim_account_meta"):
                seed_from_accounts_xlsx(engine)
            else:
                # meta가 비어있으면 1회 시드
                cnt = sql_read(engine, "SELECT COUNT(*) AS cnt FROM dim_account_meta")
                if not cnt.empty and int(cnt.loc[0, "cnt"]) == 0:
                    seed_from_accounts_xlsx(engine)
    except Exception:
        pass

    meta = get_meta(engine)
    if meta.empty:
        st.warning("dim_account_meta 가 비어있습니다. 설정 탭에서 accounts.xlsx 동기화를 실행하세요.")
        # 그래도 실행은 가능하지만 필터/표시가 빈 상태일 수 있음.

    dim_campaign = load_dim_campaign(engine)
    type_opts = get_campaign_type_options(dim_campaign)
    f = sidebar_filters(meta if not meta.empty else pd.DataFrame(columns=["account_name","customer_id","manager"]), type_opts)

    page = st.sidebar.radio("메뉴", ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정/연결"])

    if page == "전체 예산/잔액 관리":
        page_budget(meta, engine, f)
    elif page == "성과(캠페인)":
        page_perf_campaign(meta, engine, f, dim_campaign)
    elif page == "성과(키워드)":
        page_perf_keyword(meta, engine, f, dim_campaign)
    elif page == "성과(소재)":
        page_perf_ad(meta, engine, f, dim_campaign)
    else:
        page_settings(engine)

if __name__ == "__main__":
    main()
