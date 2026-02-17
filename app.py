# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.1.0: 키워드 속도 개선 + ProgrammingError 회피)

✅ 이번 수정 (v7.1.0)
1) [키워드 탭 속도 개선]
   - DB에서 keyword_id 단위로 바로 집계 + dim 조인까지 한 번에 처리
   - 광고비 기준 Top N만 조회(기본 300) → 화면 로딩 체감 개선

2) [Streamlit Cloud SQLAlchemy ProgrammingError 방지]
   - customer_ids/keyword_ids 같은 리스트 바인딩(IN/ANY/expanding) 사용 제거
   - 대신 customer_id는 OR 조건으로 안전하게 생성하여 전달

3) [웹사이트 모드 UI 유지]
   - Streamlit 기본 툴바/메뉴 숨김 CSS 유지
"""

import os
import re
import io
from datetime import date, timedelta
from typing import List, Optional, Dict

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import altair as alt
from sqlalchemy import create_engine, text, inspect, bindparam
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Download helpers
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
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


st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

# -----------------------------
# BUILD TAG (배포 확인용)
# -----------------------------
# Streamlit Cloud에서 코드가 실제로 교체/배포됐는지 한눈에 확인하려고 넣어둠.
BUILD_TAG = "v7.1.0 (2026-02-17)"

# -----------------------------
# CONFIG / THRESHOLDS
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

# -----------------------------
# GLOBAL_UI_CSS
# -----------------------------
GLOBAL_UI_CSS = """
<style>
  /* 웹사이트 모드: Streamlit 기본 크롬 숨김(환경에 따라 일부는 소유자에게만 보일 수 있음) */
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }
  div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
  div[data-testid="stDecoration"] { display: none; }
  div[data-testid="stStatusWidget"] { visibility: hidden; height: 0px; }

  h2, h3 { letter-spacing: -0.2px; }
  div[data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; background: rgba(2, 132, 199, 0.06); }
  .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; margin-right:6px; }
  .b-red { background: rgba(239,68,68,0.12); color: rgb(185,28,28); }
  .b-yellow { background: rgba(234,179,8,0.16); color: rgb(161,98,7); }
  .b-green { background: rgba(34,197,94,0.12); color: rgb(21,128,61); }
  .b-gray { background: rgba(148,163,184,0.18); color: rgb(51,65,85); }

  section[data-testid="stSidebar"] { padding-top: 8px; }
  thead tr th:first-child { display:none }
  tbody th { display:none }

  /* 테이블 숫자 정렬(대략) */
  .num { font-variant-numeric: tabular-nums; }
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


APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# --------------------
# DB helpers (Optimized with Caching)
# --------------------
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
    """
    SQL reader with optional expanding list params (IN :param) to avoid ProgrammingError.
    - expanding_keys: set of param names that should be expanded.
    """
    params = params or {}
    stmt = text(sql)
    if expanding_keys:
        for k in expanding_keys:
            if k in params:
                stmt = stmt.bindparams(bindparam(k, expanding=True))
    with engine.connect() as conn:
        return pd.read_sql(stmt, conn, params=params)


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


# --------------------
# Utilities (Formatters)
# --------------------
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


# --------------------
# Campaign Type
# --------------------
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


def apply_type_filter_to_fact(fact: pd.DataFrame, dim_campaign: pd.DataFrame, type_sel: List[str]) -> pd.DataFrame:
    if fact is None or fact.empty:
        return fact
    if dim_campaign is None or dim_campaign.empty:
        return pd.DataFrame(columns=fact.columns)

    dc = dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]].copy()
    dc["customer_id"] = pd.to_numeric(dc["customer_id"], errors="coerce").astype("Int64")
    dc = dc.dropna(subset=["customer_id"]).copy()
    dc["customer_id"] = dc["customer_id"].astype("int64")

    tmp = fact.copy()
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"]).copy()
    tmp["customer_id"] = tmp["customer_id"].astype("int64")

    tmp = tmp.merge(dc, on=["customer_id", "campaign_id"], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("").astype(str).str.strip()
    tmp.loc[tmp["campaign_type_label"] == "", "campaign_type_label"] = "기타"

    tmp = tmp[tmp["campaign_type_label"] != "기타"]

    if not type_sel:
        return tmp.drop(columns=["campaign_type_label"], errors="ignore")

    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"], errors="ignore")


# --------------------
# [속도 개선] 타입 필터용 맵 캐싱 (키워드/소재용)
# --------------------
@st.cache_data(ttl=3600, show_spinner=False)
def get_kw_type_map(_engine) -> pd.DataFrame:
    if not (table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_keyword")):
        return pd.DataFrame(columns=["customer_id", "keyword_id", "campaign_type_label"])

    sql = """
    SELECT
      k.customer_id,
      k.keyword_id,
      COALESCE(NULLIF(TRIM(c.campaign_tp), ''), '') AS campaign_tp
    FROM dim_keyword k
    LEFT JOIN dim_adgroup g
      ON k.customer_id = g.customer_id AND k.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    """
    df = sql_read(_engine, sql)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "keyword_id", "campaign_type_label"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df[["customer_id", "keyword_id", "campaign_type_label"]].drop_duplicates()


@st.cache_data(ttl=3600, show_spinner=False)
def get_ad_type_map(_engine) -> pd.DataFrame:
    if not (table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_ad")):
        return pd.DataFrame(columns=["customer_id", "ad_id", "campaign_type_label"])

    sql = """
    SELECT
      a.customer_id,
      a.ad_id,
      COALESCE(NULLIF(TRIM(c.campaign_tp), ''), '') AS campaign_tp
    FROM dim_ad a
    LEFT JOIN dim_adgroup g
      ON a.customer_id = g.customer_id AND a.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    """
    df = sql_read(_engine, sql)
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "ad_id", "campaign_type_label"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df["campaign_type_label"] = df["campaign_tp"].apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df[["customer_id", "ad_id", "campaign_type_label"]].drop_duplicates()


def apply_type_filter_to_kw_ad_fact_fast(engine, fact: pd.DataFrame, type_sel: List[str], level: str) -> pd.DataFrame:
    if fact is None or fact.empty:
        return fact

    tmp = fact.copy()
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"]).copy()
    tmp["customer_id"] = tmp["customer_id"].astype("int64")

    if level == "keyword":
        m = get_kw_type_map(engine)
        if m.empty:
            return fact
        tmp = tmp.merge(m, on=["customer_id", "keyword_id"], how="left")
    else:
        m = get_ad_type_map(engine)
        if m.empty:
            return fact
        tmp = tmp.merge(m, on=["customer_id", "ad_id"], how="left")

    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("").astype(str).str.strip()
    tmp.loc[tmp["campaign_type_label"] == "", "campaign_type_label"] = "기타"

    tmp = tmp[tmp["campaign_type_label"] != "기타"]

    if not type_sel:
        return tmp.drop(columns=["campaign_type_label"], errors="ignore")

    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"], errors="ignore")


# --------------------
# DB Sync & Meta
# --------------------
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


@st.cache_data(ttl=300, show_spinner=False)
def get_latest_bizmoney(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"):
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
    sql = """
    SELECT DISTINCT ON (customer_id) customer_id, bizmoney_balance, dt as last_update
    FROM fact_bizmoney_daily ORDER BY customer_id, dt DESC
    """
    df = sql_read(_engine, sql)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
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
        df["current_month_cost"] = df["current_month_cost"].fillna(0).astype("int64")
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_recent_avg_cost(_engine, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """
    ✅ 초안/배포 환경 차이로 IN/ANY 파라미터가 깨지면서 ProgrammingError가 나는 케이스가 있어서,
    **SQL에서는 리스트 바인딩/집계를 아예 하지 않고** 기간 범위의 raw row를 가져온 뒤 pandas로 평균소진을 계산합니다.

    - customer_ids는 pandas에서만 필터링
    - cost가 text여도 pd.to_numeric(errors='coerce')로 안전 처리
    """
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    if d2 < d1:
        d1 = d2

    sql = """
    SELECT customer_id, cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    """

    tmp = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if tmp.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"]).copy()
    tmp["customer_id"] = tmp["customer_id"].astype("int64")

    if customer_ids:
        allow = set(int(x) for x in customer_ids)
        tmp = tmp[tmp["customer_id"].isin(allow)].copy()

    tmp["cost"] = pd.to_numeric(tmp.get("cost", 0), errors="coerce").fillna(0.0)

    days = max((d2 - d1).days + 1, 1)
    g = tmp.groupby("customer_id", as_index=False)["cost"].sum().rename(columns={"cost": "sum_cost"})
    g["avg_cost"] = g["sum_cost"].astype(float) / float(days)
    return g[["customer_id", "avg_cost"]]


# --------------------
# Sidebar
# --------------------
def sidebar_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    st.sidebar.title("필터")

    with st.sidebar.expander("업체/담당자", expanded=True):
        q = st.text_input("업체명 검색", placeholder="예: 실리콘플러스")
        managers = sorted([m for m in meta["manager"].fillna("").unique().tolist() if str(m).strip()])
        manager_sel = st.multiselect("담당자", options=managers, default=[])

        tmp = meta.copy()
        if q:
            tmp = tmp[tmp["account_name"].str.contains(q, case=False, na=False)]
        if manager_sel:
            tmp = tmp[tmp["manager"].isin(manager_sel)]

        opt = tmp[["account_name", "customer_id"]].copy()
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


def resolve_selected_ids(meta: pd.DataFrame, f: Dict) -> List[int]:
    sel_ids = f["selected_customer_ids"]
    if (not sel_ids) and f["manager_sel"]:
        sel_ids = meta[meta["manager"].isin(f["manager_sel"])]["customer_id"].astype(int).tolist()
    return sel_ids


# --------------------
# Loaders (speed: select only needed cols)
# --------------------
FACT_COLS = {
    "fact_campaign_daily": ["dt", "customer_id", "campaign_id", "imp", "clk", "cost", "conv", "sales"],
    "fact_keyword_daily": ["dt", "customer_id", "keyword_id", "imp", "clk", "cost", "conv", "sales"],
    "fact_ad_daily": ["dt", "customer_id", "ad_id", "imp", "clk", "cost", "conv", "sales"],
}


@st.cache_data(ttl=600, show_spinner=False)
def load_fact(_engine, table: str, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    if not table_exists(_engine, table):
        return pd.DataFrame()

    cols = FACT_COLS.get(table, None)
    sel = ", ".join(cols) if cols else "*"

    df = sql_read(_engine, f"SELECT {sel} FROM {table} WHERE dt BETWEEN :d1 AND :d2", {"d1": str(d1), "d2": str(d2)})

    if df.empty:
        return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    if "sales" not in df.columns:
        df["sales"] = 0

    if customer_ids:
        df = df[df["customer_id"].isin([int(x) for x in customer_ids])].copy()

    return df



@st.cache_data(ttl=3600, show_spinner=False)
def get_cols_cached(_engine, table: str, schema: str = "public") -> List[str]:
    # Inspector is a bit heavy; cache table columns for fast schema checks.
    cols = get_table_columns(_engine, table, schema=schema)
    return sorted(list(cols))


@st.cache_data(ttl=600, show_spinner=False)
# --------------------
# Keyword tab (Ultra-safe / Fast)
# --------------------
def _or_clause(field: str, values: Optional[List[int]], prefix: str) -> tuple[str, Dict[str, int]]:
    """Build a safe OR-clause without list-binding (to avoid ProgrammingError on some DB drivers)."""
    if not values:
        return "", {}
    vals: List[int] = []
    for v in values:
        try:
            vals.append(int(v))
        except Exception:
            continue
    if not vals:
        return "", {}

    parts: List[str] = []
    params: Dict[str, int] = {}
    for i, v in enumerate(vals):
        k = f"{prefix}{i}"
        parts.append(f"{field} = :{k}")
        params[k] = int(v)

    return "(" + " OR ".join(parts) + ")", params


@st.cache_data(ttl=600, show_spinner=False)
def load_keyword_top_join(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Optional[List[int]] = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """✅ 키워드 탭 전용 초고속/안전 로더

    - DB에서 keyword_id 단위로 바로 집계 + dim 조인까지 한 번에 수행
    - customer_ids / keyword_ids 같은 '리스트 바인딩(IN/ANY/expanding)'을 쓰지 않음
      → Streamlit Cloud 환경에서 자주 보이던 SQLAlchemy ProgrammingError 회피
    - cost 기준 상위 limit만 가져와서 응답 속도 개선
    """
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame(
            columns=[
                "customer_id",
                "keyword_id",
                "imp",
                "clk",
                "cost",
                "conv",
                "sales",
                "keyword",
                "adgroup_name",
                "campaign_name",
                "campaign_tp",
            ]
        )

    cols = set(get_cols_cached(_engine, "fact_keyword_daily"))
    sales_expr = "SUM(COALESCE(f.sales,0)) AS sales" if "sales" in cols else "0::bigint AS sales"

    where = "f.dt BETWEEN :d1 AND :d2"
    params: Dict[str, object] = {"d1": str(d1), "d2": str(d2), "limit": int(limit)}

    clause, p = _or_clause("f.customer_id", customer_ids, "cid")
    if clause:
        where += f" AND {clause}"
        params.update(p)

    sql = f"""
    WITH agg AS (
      SELECT
        f.customer_id,
        f.keyword_id,
        SUM(f.imp)  AS imp,
        SUM(f.clk)  AS clk,
        SUM(f.cost) AS cost,
        SUM(f.conv) AS conv,
        {sales_expr}
      FROM fact_keyword_daily f
      WHERE {where}
      GROUP BY f.customer_id, f.keyword_id
      ORDER BY SUM(f.cost) DESC
      LIMIT :limit
    )
    SELECT
      a.customer_id,
      a.keyword_id,
      a.imp,
      a.clk,
      a.cost,
      a.conv,
      a.sales,
      COALESCE(k.keyword,'') AS keyword,
      COALESCE(g.adgroup_name,'') AS adgroup_name,
      COALESCE(c.campaign_name,'') AS campaign_name,
      COALESCE(NULLIF(TRIM(c.campaign_tp), ''), '') AS campaign_tp
    FROM agg a
    LEFT JOIN dim_keyword k
      ON a.customer_id = k.customer_id AND a.keyword_id = k.keyword_id
    LEFT JOIN dim_adgroup g
      ON k.customer_id = g.customer_id AND k.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id
    """

    df = sql_read(_engine, sql, params=params)
    if df.empty:
        return df

    # 타입 정리
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df["imp"] = pd.to_numeric(df.get("imp", 0), errors="coerce").fillna(0).astype("int64")
    df["clk"] = pd.to_numeric(df.get("clk", 0), errors="coerce").fillna(0).astype("int64")
    df["cost"] = pd.to_numeric(df.get("cost", 0), errors="coerce").fillna(0).astype("int64")
    df["conv"] = pd.to_numeric(df.get("conv", 0), errors="coerce").fillna(0).astype("float")
    df["sales"] = pd.to_numeric(df.get("sales", 0), errors="coerce").fillna(0).astype("float")

    df["keyword"] = df.get("keyword", "").fillna("").astype(str)
    df["adgroup_name"] = df.get("adgroup_name", "").fillna("").astype(str)
    df["campaign_name"] = df.get("campaign_name", "").fillna("").astype(str)
    df["campaign_tp"] = df.get("campaign_tp", "").fillna("").astype(str)

    return df


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict, dim_campaign: pd.DataFrame):
    st.markdown("## 🔑 성과 대시보드 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    type_sel = f.get("type_sel", [])
    sel_ids = resolve_selected_ids(meta, f)

    # 키워드 탭은 데이터가 가장 많음 → 필터가 없으면 느릴 수 있음
    if (not sel_ids) and (not f.get("manager_sel")) and (not f.get("q")) and (len(meta) >= 30):
        st.info("키워드 탭은 데이터가 많아서 느릴 수 있어요. '업체' 또는 '담당자'를 먼저 선택하면 속도가 크게 개선됩니다.")

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, 300, 50, key="kw_topn")

    # 유형 필터를 나중에 적용하므로 미리 조금 더 많이 가져옴(그래도 제한)
    pre_limit = int(min(max(int(top_n) * 4, 600), 20000))

    fact = load_keyword_top_join(
        engine,
        f["start"],
        f["end"],
        customer_ids=sel_ids if sel_ids else None,
        limit=pre_limit,
    )

    if fact is None or fact.empty:
        st.warning("데이터 없음")
        return

    # 캠페인 타입 라벨 + '기타' 제거
    fact["campaign_type_label"] = fact.get("campaign_tp", "").apply(campaign_tp_to_label)
    fact.loc[fact["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    fact = fact[fact["campaign_type_label"] != "기타"].copy()

    # 선택 유형 필터
    if type_sel:
        fact = fact[fact["campaign_type_label"].isin(type_sel)].copy()

    if fact.empty:
        st.warning("선택한 조건에 해당하는 키워드 데이터가 없습니다.")
        return

    # 최종 Top N
    fact = fact.sort_values("cost", ascending=False).head(int(top_n)).copy()

    # 담당자/업체명 합치기
    fact = fact.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    g = add_rates(fact)

    show = g.copy()
    show["cost"] = show["cost"].apply(format_currency)
    show["sales"] = pd.to_numeric(show.get("sales", 0), errors="coerce").fillna(0).apply(format_currency)
    show["cpc"] = show["cpc"].apply(format_currency)
    show["cpa"] = show["cpa"].apply(format_currency)
    show["roas_disp"] = show["roas"].apply(format_roas)

    show = show.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_type_label": "광고유형",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "keyword": "키워드",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "sales": "전환매출",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "roas_disp": "ROAS(%)",
        }
    )

    for c in ["노출", "클릭"]:
        if c in show.columns:
            show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)
    if "전환" in show.columns:
        show["전환"] = pd.to_numeric(show["전환"], errors="coerce").fillna(0).astype(float)

    cols = ["업체명", "담당자", "광고유형", "캠페인", "광고그룹", "키워드",
            "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")

    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_키워드_{f['start']}_{f['end']}", "keyword", "kw")

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict, dim_campaign: pd.DataFrame):
    st.markdown("## 성과 대시보드 (소재/광고)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    type_sel = f.get("type_sel", [])
    sel_ids = resolve_selected_ids(meta, f)

    fact = load_fact(engine, "fact_ad_daily", f["start"], f["end"], customer_ids=sel_ids if sel_ids else None)
    fact = apply_type_filter_to_kw_ad_fact_fast(engine, fact, type_sel, level="ad")

    if fact.empty:
        st.warning("데이터 없음")
        return

    dim = load_dim_ad(engine)

    g = fact.groupby(["customer_id", "ad_id"], as_index=False)[["imp", "clk", "cost", "conv", "sales"]].sum()
    g = add_rates(g)
    g = g.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")
    if not dim.empty:
        g = g.merge(dim, on=["customer_id", "ad_id"], how="left")
    g["ad_name"] = g.get("ad_name", pd.Series([""] * len(g))).fillna("")

    top_n = st.slider("표시 개수(광고비 기준 Top N)", 50, 2000, 300, 50, key="ad_topn")
    g2 = g.sort_values("cost", ascending=False).head(int(top_n)).copy()

    show = g2.copy()
    show["cost"] = show["cost"].apply(format_currency)
    if "sales" in show.columns:
        show["sales"] = show["sales"].apply(format_currency)
    show["cpc"] = show["cpc"].apply(format_currency)
    show["cpa"] = show["cpa"].apply(format_currency)
    show["roas_disp"] = show["roas"].apply(format_roas)

    show = show.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "ad_id": "소재ID",
            "ad_name": "소재내용",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "sales": "전환매출",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "roas_disp": "ROAS(%)",
        }
    )

    cols = ["업체명", "담당자", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = finalize_ctr_col(show[cols].copy(), "CTR(%)")

    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "소재내용": st.column_config.TextColumn("소재내용", width="medium"),
        },
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
        st.rerun()


def main():
    st.title("네이버 검색광고 통합 대시보드")
    st.caption(f"빌드: {BUILD_TAG} · 파일: {__file__}")
    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

    try:
        seed_from_accounts_xlsx(engine)
    except Exception:
        pass

    meta = get_meta(engine)
    dim_campaign = load_dim_campaign(engine)

    type_opts = get_campaign_type_options(dim_campaign)
    f = sidebar_filters(meta, type_opts)

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
