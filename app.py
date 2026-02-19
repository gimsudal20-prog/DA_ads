# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (정리본)

✅ 반영사항
- iOS Safari 프론트 오류(TypeError: ... e[s].sticky) 회피:
  * Streamlit 내부 DOM(data-testid 등)을 건드리는 CSS/JS 제거
  * st.data_editor 사용 안 함 → 폼 기반 예산 업데이트로 변경
- customer_id 타입(TEXT/BIGINT 혼재) 안전:
  * 모든 JOIN/필터에서 customer_id::text 로 통일
  * IN 필터는 문자열 리터럴('420332') 형태로 구성

필수 환경변수
- DATABASE_URL (PostgreSQL/Supabase)

옵션 환경변수
- TOPUP_STATIC_THRESHOLD (기본 50000)
- TOPUP_AVG_DAYS (기본 3)
- TOPUP_DAYS_COVER (기본 2)
- ACCOUNTS_XLSX (기본: app.py와 같은 폴더의 accounts.xlsx)

필수 테이블(조회용)
- dim_account_meta (Settings 탭에서 seed 가능)
- dim_campaign, dim_adgroup, dim_keyword, dim_ad
- fact_campaign_daily, fact_keyword_daily, fact_ad_daily, fact_bizmoney_daily
"""

from __future__ import annotations

import io
import os
import re
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

import plotly.graph_objects as go

load_dotenv()

# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

BUILD_TAG = "v7.9 (정리본)"
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# -----------------------------
# Safe CSS (no fragile DOM hooks)
# -----------------------------
st.markdown(
    """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");
html, body, .stApp { font-family: Pretendard, system-ui, -apple-system, "Apple SD Gothic Neo", "Malgun Gothic", sans-serif; }
.block-container { max-width: 1400px; padding-top: calc(env(safe-area-inset-top) + 3.8rem) !important; }
#MainMenu { visibility:hidden; } footer { visibility:hidden; }
.card { border:1px solid rgba(180,196,217,.55); border-radius:16px; padding:14px 16px; background:#fff; box-shadow:0 6px 18px rgba(2,8,23,.05); }
.badge { display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px; font-size:12px; font-weight:800;
         border:1px solid rgba(180,196,217,.55); background:rgba(235,238,242,.6); margin-right:6px; }
.badge.blue{ background: rgba(37,99,235,.10); border-color: rgba(37,99,235,.20); color:#1D4ED8; }
.badge.red { background: rgba(239,68,68,.10); border-color: rgba(239,68,68,.20); color:#DC2626; }
.badge.green{ background: rgba(16,185,129,.10); border-color: rgba(16,185,129,.20); color:#047857; }
.smallmuted{ color:rgba(2,8,23,.55); font-size:12px; }
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# DB helpers
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    url = None
    for k in ["DATABASE_URL", "SUPABASE_DATABASE_URL", "SUPABASE_DB_URL", "POSTGRES_URL", "POSTGRES_URL_NON_POOLING", "DB_URL"]:
        v = os.getenv(k)
        if v and str(v).strip():
            url = str(v).strip()
            break
    if not url:
        raise RuntimeError("환경변수 DATABASE_URL이 없습니다.")

    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if "sslmode=" not in url and url.startswith("postgresql"):
        url = url + ("&" if "?" in url else "?") + "sslmode=require"
    return create_engine(url, pool_pre_ping=True)

def sql_read(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def sql_exec(engine, sql: str, params: Optional[dict] = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def table_exists(engine, table: str, schema: str = "public") -> bool:
    try:
        return table in set(inspect(engine).get_table_names(schema=schema))
    except Exception:
        return False

def get_table_columns(engine, table: str, schema: str = "public") -> set:
    cache = st.session_state.setdefault("_table_cols_cache", {})
    key = f"{schema}.{table}"
    if key in cache:
        return cache[key]
    try:
        cols = inspect(engine).get_columns(table, schema=schema)
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
            pass
    return ",".join(safe) if safe else "''"

# -----------------------------
# Download (cached)
# -----------------------------
@st.cache_data(show_spinner=False)
def _df_json_to_csv_bytes(df_json: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    return df.to_csv(index=False).encode("utf-8-sig")

@st.cache_data(show_spinner=False)
def _df_json_to_xlsx_bytes(df_json: str, sheet_name: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=str(sheet_name)[:31])
    return out.getvalue()

def render_download(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    if df is None or df.empty:
        return
    j = df.to_json(orient="split")
    c1, c2, c3 = st.columns([1, 1, 6])
    with c1:
        st.download_button("CSV", _df_json_to_csv_bytes(j), f"{filename_base}.csv", "text/csv", key=f"{key_prefix}_csv", use_container_width=True)
    with c2:
        st.download_button("XLSX", _df_json_to_xlsx_bytes(j, sheet_name), f"{filename_base}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"{key_prefix}_xlsx", use_container_width=True)
    with c3:
        st.caption("다운로드")

# -----------------------------
# Formatters
# -----------------------------
def _safe_int(x, default=0) -> int:
    try:
        if pd.isna(x) or x == "":
            return default
        return int(float(x))
    except Exception:
        return default

def format_won(x) -> str:
    return f"{_safe_int(x):,}원"

def format_num(x) -> str:
    return f"{_safe_int(x):,}"

def format_roas(x) -> str:
    try:
        if pd.isna(x): return "-"
        return f"{float(x):.0f}%"
    except Exception:
        return "-"

def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df
    s = pd.to_numeric(df[col], errors="coerce")
    df = df.copy()
    df[col] = s.map(lambda v: "" if pd.isna(v) else ("0%" if float(v) == 0 else f"{float(v):.1f}%"))
    return df

def parse_currency(v) -> int:
    s = re.sub(r"[^\d]", "", str(v or ""))
    return int(s) if s else 0

# -----------------------------
# Campaign type mapping
# -----------------------------
_TP_LABEL = {
    "web_site": "파워링크", "website": "파워링크", "power_link": "파워링크", "powerlink": "파워링크",
    "shopping": "쇼핑검색", "shopping_search": "쇼핑검색",
    "power_content": "파워콘텐츠", "power_contents": "파워콘텐츠", "powercontent": "파워콘텐츠",
    "place": "플레이스", "place_search": "플레이스",
    "brand_search": "브랜드검색", "brandsearch": "브랜드검색",
}

_LABEL_TO_KEYS: Dict[str, List[str]] = {}
for k, v in _TP_LABEL.items():
    _LABEL_TO_KEYS.setdefault(v, []).append(k)

def tp_to_label(tp: str) -> str:
    key = str(tp or "").strip().lower()
    return _TP_LABEL.get(key, str(tp or "").strip())

def labels_to_tp_keys(labels: Tuple[str, ...]) -> List[str]:
    keys: List[str] = []
    for lab in labels:
        keys += _LABEL_TO_KEYS.get(str(lab), [])
    out, seen = [], set()
    for k in keys:
        if k not in seen:
            out.append(k); seen.add(k)
    return out

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(engine) -> pd.DataFrame:
    if not table_exists(engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df = sql_read(engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    df["campaign_tp"] = df.get("campaign_tp", "").fillna("")
    df["campaign_type"] = df["campaign_tp"].astype(str).map(tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    return df

def campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []
    present = sorted({tp_to_label(x) for x in dim_campaign["campaign_tp"].dropna().astype(str).tolist() if tp_to_label(x) not in ("", "기타")})
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    out = [x for x in order if x in present] + [x for x in present if x not in set(order)]
    return out

# -----------------------------
# Meta (accounts)
# -----------------------------
def ensure_meta_table(engine) -> None:
    sql_exec(
        engine,
        """
        CREATE TABLE IF NOT EXISTS dim_account_meta (
          customer_id BIGINT PRIMARY KEY,
          account_name TEXT NOT NULL,
          manager TEXT DEFAULT '',
          monthly_budget BIGINT DEFAULT 0,
          updated_at TIMESTAMPTZ DEFAULT now()
        );
        """,
    )

def normalize_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    def pick(cands):
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                if lc == cand.lower().replace(" ", "").replace("_", ""):
                    return c
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                if cand.lower().replace(" ", "").replace("_", "") in lc:
                    return c
        return None
    cid = pick(["customer_id", "customerid", "커스텀id", "커스텀ID"])
    name = pick(["account_name", "accountname", "업체명", "업체"])
    mgr = pick(["manager", "담당자", "담당"])
    if not cid or not name:
        raise ValueError(f"accounts.xlsx 컬럼을 확인해줘. 현재: {list(df.columns)}")
    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(df[cid], errors="coerce").astype("Int64")
    out["account_name"] = df[name].astype(str).str.strip()
    out["manager"] = df[mgr].astype(str).str.strip() if mgr else ""
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    out = out.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)
    return out

def seed_meta(engine, accounts_df: Optional[pd.DataFrame] = None) -> int:
    ensure_meta_table(engine)
    if accounts_df is None:
        if not os.path.exists(ACCOUNTS_XLSX):
            raise FileNotFoundError(f"accounts.xlsx 없음: {ACCOUNTS_XLSX}")
        accounts_df = pd.read_excel(ACCOUNTS_XLSX)
    acc = normalize_accounts(accounts_df)
    sqlq = """
    INSERT INTO dim_account_meta (customer_id, account_name, manager, updated_at)
    VALUES (:customer_id, :account_name, :manager, now())
    ON CONFLICT (customer_id) DO UPDATE SET
      account_name = EXCLUDED.account_name,
      manager = EXCLUDED.manager,
      updated_at = now();
    """
    with engine.begin() as conn:
        conn.execute(text(sqlq), acc.to_dict(orient="records"))
    return int(len(acc))

@st.cache_data(ttl=600, show_spinner=False)
def get_meta(engine) -> pd.DataFrame:
    if not table_exists(engine, "dim_account_meta"):
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])
    df = sql_read(engine, "SELECT customer_id, account_name, manager, monthly_budget, updated_at FROM dim_account_meta ORDER BY account_name")
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    df["manager"] = df.get("manager", "").fillna("").astype(str)
    df["account_name"] = df.get("account_name", "").fillna("").astype(str)
    return df

def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
    sql_exec(engine, "UPDATE dim_account_meta SET monthly_budget=:b, updated_at=now() WHERE customer_id=:cid",
             {"b": int(monthly_budget), "cid": int(customer_id)})

@st.cache_data(ttl=600, show_spinner=False)
def latest_dates(engine) -> Dict[str, str]:
    parts = []
    for t in ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily"]:
        if table_exists(engine, t):
            parts.append(f"SELECT '{t}' AS t, MAX(dt) AS mx FROM {t}")
    if not parts:
        return {}
    df = sql_read(engine, " UNION ALL ".join(parts))
    out = {}
    for _, r in df.iterrows():
        out[str(r["t"])] = (str(r["mx"])[:10] if r["mx"] is not None else "-")
    return out

# -----------------------------
# Charts
# -----------------------------
def line_chart(df: pd.DataFrame, x: str, y: str, title: str, yfmt: str = ",.0f", height: int = 260):
    if df is None or df.empty:
        return None
    d = df.copy()
    d[x] = pd.to_datetime(d[x], errors="coerce")
    d = d.dropna(subset=[x]).sort_values(x)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d[x], y=pd.to_numeric(d[y], errors="coerce").fillna(0), mode="lines+markers",
                             line=dict(width=3, shape="spline"), marker=dict(size=5)))
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=46, b=10), title=dict(text=title, x=0),
                      xaxis=dict(showgrid=False), yaxis=dict(tickformat=yfmt, gridcolor="rgba(0,0,0,0.06)", zeroline=False),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig

def show_chart(fig):
    if fig is None: return
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    d = df.copy()
    d["ctr"] = (d["clk"] / d["imp"].replace({0: pd.NA})) * 100
    d["cpc"] = d["cost"] / d["clk"].replace({0: pd.NA})
    d["cpa"] = d["cost"] / d["conv"].replace({0: pd.NA})
    d["roas"] = (d["sales"] / d["cost"].replace({0: pd.NA})) * 100
    return d

# -----------------------------
# Filters
# -----------------------------
def build_filters(meta: pd.DataFrame, dim_campaign: pd.DataFrame) -> Dict:
    with st.expander("필터", expanded=True):
        c1, c2, c3, c4 = st.columns([1.1, 1.2, 1.0, 1.0], gap="large")
        with c1:
            kw = st.text_input("업체명 검색", value=st.session_state.get("flt_kw", ""), placeholder="예: 실리콘플러스", key="flt_kw")
            mgr_opts = sorted([x for x in meta["manager"].dropna().unique().tolist() if x and x != "nan"])
            mgr_sel = st.multiselect("담당자", mgr_opts, default=st.session_state.get("tmp_mgr_sel", []), key="tmp_mgr_sel")
        with c2:
            dfm = meta.copy()
            if mgr_sel:
                dfm = dfm[dfm["manager"].isin(mgr_sel)]
            if kw:
                dfm = dfm[dfm["account_name"].str.contains(kw.strip(), case=False, na=False)]
            acc_opts = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if x and x != "nan"])
            if "tmp_acc_sel" not in st.session_state:
                st.session_state["tmp_acc_sel"] = acc_opts[:]
            acc_sel = st.multiselect("업체", acc_opts, default=st.session_state.get("tmp_acc_sel", acc_opts[:]), key="tmp_acc_sel")
        with c3:
            type_opts = campaign_type_options(dim_campaign)
            if "tmp_type_sel" not in st.session_state:
                st.session_state["tmp_type_sel"] = type_opts[:]
            type_sel = st.multiselect("광고유형", type_opts, default=st.session_state.get("tmp_type_sel", type_opts[:]), key="tmp_type_sel")

            period_opt = ["오늘", "어제", "최근 7일", "최근 30일", "직접 선택"]
            period = st.selectbox("기간", period_opt, index=int(st.session_state.get("flt_period_idx", 1)))
            today = date.today()
            if period == "오늘":
                start, end = today, today
            elif period == "어제":
                start, end = today - timedelta(days=1), today - timedelta(days=1)
            elif period == "최근 7일":
                start, end = today - timedelta(days=6), today
            elif period == "최근 30일":
                start, end = today - timedelta(days=29), today
            else:
                dr = st.date_input("직접 선택", value=(today - timedelta(days=6), today), key="flt_daterange")
                start, end = (dr if isinstance(dr, tuple) else (today - timedelta(days=6), today))
        with c4:
            k_top = st.slider("키워드 TOP N", 10, 500, int(st.session_state.get("k_top", 300)), 10, key="k_top")
            a_top = st.slider("소재 TOP N", 10, 500, int(st.session_state.get("a_top", 200)), 10, key="a_top")
            c_top = st.slider("캠페인 TOP N", 10, 500, int(st.session_state.get("c_top", 200)), 10, key="c_top")
            apply = st.button("✅ 적용", use_container_width=True)

    if apply:
        st.session_state["ready"] = True
        st.session_state["flt_period_idx"] = period_opt.index(period)

    ready = bool(st.session_state.get("ready", False))

    if not acc_sel:
        cids = meta["customer_id"].dropna().astype(int).tolist()
    else:
        cids = meta[meta["account_name"].isin(acc_sel)]["customer_id"].dropna().astype(int).tolist()

    return {
        "ready": ready,
        "start": start,
        "end": end,
        "type_sel": tuple(type_sel or ()),
        "cids": tuple(cids),
        "top_keyword": int(k_top),
        "top_ad": int(a_top),
        "top_campaign": int(c_top),
    }

# -----------------------------
# Budget query (single query)
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def query_budget_bundle(engine, cids: Tuple[int, ...], end_dt: date) -> pd.DataFrame:
    if not (table_exists(engine, "dim_account_meta") and table_exists(engine, "fact_campaign_daily") and table_exists(engine, "fact_bizmoney_daily")):
        return pd.DataFrame()

    yesterday = end_dt - timedelta(days=1)
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    month_d2 = (date(end_dt.year + (1 if end_dt.month == 12 else 0), 1 if end_dt.month == 12 else end_dt.month + 1, 1) - timedelta(days=1))

    where = ""
    if cids:
        where = f"WHERE m.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sqlq = f"""
    WITH meta AS (
      SELECT customer_id::text AS customer_id, account_name, manager, COALESCE(monthly_budget,0) AS monthly_budget
      FROM dim_account_meta m
      {where}
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

    df = sql_read(engine, sqlq, {
        "y": str(yesterday), "a1": str(avg_d1), "a2": str(avg_d2),
        "m1": str(month_d1), "m2": str(month_d2),
        "min_dt": str(min_dt), "max_dt": str(max_dt)
    })
    if df is None or df.empty:
        return pd.DataFrame()

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["monthly_budget","bizmoney_balance","y_cost","avg_sum_cost","current_month_cost"]:
        df[c] = pd.to_numeric(df.get(c,0), errors="coerce").fillna(0.0)
    df["avg_cost"] = df["avg_sum_cost"].astype(float) / float(max(TOPUP_AVG_DAYS, 1))
    return df

# -----------------------------
# Perf queries
# -----------------------------
def _fact_has_sales(engine, fact_table: str) -> bool:
    return "sales" in get_table_columns(engine, fact_table)

@st.cache_data(ttl=300, show_spinner=False)
def ts_campaign(engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    sales_expr = "SUM(COALESCE(f.sales,0))" if _fact_has_sales(engine, "fact_campaign_daily") else "0::numeric"
    where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    tp_keys = labels_to_tp_keys(type_sel) if type_sel else []
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sqlq = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        )
        SELECT f.dt::date AS dt,
               SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales
        FROM fact_campaign_daily f
        JOIN c_f c ON f.customer_id::text=c.customer_id AND f.campaign_id=c.campaign_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.dt::date ORDER BY f.dt::date
        """
    else:
        sqlq = f"""
        SELECT f.dt::date AS dt,
               SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales
        FROM fact_campaign_daily f
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.dt::date ORDER BY f.dt::date
        """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    for c in ["imp","clk","cost","conv","sales"]:
        df[c] = pd.to_numeric(df.get(c,0), errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def ts_keyword(engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(engine, "fact_keyword_daily"):
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    fk_cols = get_table_columns(engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"
    where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    tp_keys = labels_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(engine,"dim_campaign") and table_exists(engine,"dim_adgroup") and table_exists(engine,"dim_keyword"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sqlq = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text=c.customer_id AND g.campaign_id::text=c.campaign_id
        ),
        k_f AS (
          SELECT k.customer_id::text AS customer_id, k.keyword_id::text AS keyword_id
          FROM dim_keyword k
          JOIN g_f g ON k.customer_id::text=g.customer_id AND k.adgroup_id::text=g.adgroup_id
        )
        SELECT fk.dt::date AS dt,
               SUM(fk.imp) imp, SUM(fk.clk) clk, SUM(fk.cost) cost, SUM(fk.conv) conv, {sales_expr} sales
        FROM fact_keyword_daily fk
        JOIN k_f k ON fk.customer_id::text=k.customer_id AND fk.keyword_id::text=k.keyword_id
        WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY fk.dt::date ORDER BY fk.dt::date
        """
    else:
        sqlq = f"""
        SELECT fk.dt::date AS dt,
               SUM(fk.imp) imp, SUM(fk.clk) clk, SUM(fk.cost) cost, SUM(fk.conv) conv, {sales_expr} sales
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY fk.dt::date ORDER BY fk.dt::date
        """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    for c in ["imp","clk","cost","conv","sales"]:
        df[c] = pd.to_numeric(df.get(c,0), errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def ts_ad(engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    if not table_exists(engine, "fact_ad_daily"):
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    sales_expr = "SUM(COALESCE(f.sales,0))" if _fact_has_sales(engine, "fact_ad_daily") else "0::numeric"
    where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    sqlq = f"""
    SELECT f.dt::date AS dt,
           SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales
    FROM fact_ad_daily f
    WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
    GROUP BY f.dt::date ORDER BY f.dt::date
    """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt","imp","clk","cost","conv","sales"])
    for c in ["imp","clk","cost","conv","sales"]:
        df[c] = pd.to_numeric(df.get(c,0), errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def bundle_campaign(engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], topn_cost: int) -> pd.DataFrame:
    if not table_exists(engine, "fact_campaign_daily"):
        return pd.DataFrame()
    sales_expr = "SUM(COALESCE(f.sales,0))" if _fact_has_sales(engine, "fact_campaign_daily") else "0::numeric"
    where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    tp_keys = labels_to_tp_keys(type_sel) if type_sel else []
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sqlq = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id, campaign_name, campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT f.customer_id::text customer_id, f.campaign_id,
                 SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales
          FROM fact_campaign_daily f
          JOIN c_f c ON f.customer_id::text=c.customer_id AND f.campaign_id=c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        )
        SELECT b.*, c.campaign_name, c.campaign_tp
        FROM base b JOIN c_f c ON b.customer_id=c.customer_id AND b.campaign_id=c.campaign_id
        ORDER BY b.cost DESC NULLS LAST
        LIMIT :lim
        """
    else:
        sqlq = f"""
        SELECT f.customer_id::text customer_id, f.campaign_id,
               SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales,
               COALESCE(NULLIF(c.campaign_name,''),'') campaign_name,
               COALESCE(NULLIF(c.campaign_tp,''),'') campaign_tp
        FROM fact_campaign_daily f
        LEFT JOIN dim_campaign c ON f.customer_id::text=c.customer_id::text AND f.campaign_id=c.campaign_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id::text, f.campaign_id, c.campaign_name, c.campaign_tp
        ORDER BY cost DESC NULLS LAST
        LIMIT :lim
        """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2), "lim": int(topn_cost)})
    return df if df is not None else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def bundle_ad(engine, d1: date, d2: date, cids: Tuple[int, ...], topn_cost: int) -> pd.DataFrame:
    if not table_exists(engine, "fact_ad_daily") or not table_exists(engine,"dim_ad"):
        return pd.DataFrame()
    sales_expr = "SUM(COALESCE(f.sales,0))" if _fact_has_sales(engine, "fact_ad_daily") else "0::numeric"
    where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    cols = get_table_columns(engine, "dim_ad")
    ad_txt = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"
    sqlq = f"""
    SELECT
      f.customer_id::text customer_id, f.ad_id,
      SUM(f.imp) imp, SUM(f.clk) clk, SUM(f.cost) cost, SUM(f.conv) conv, {sales_expr} sales,
      {ad_txt} AS ad_name,
      COALESCE(NULLIF(g.adgroup_name,''),'') adgroup_name,
      COALESCE(NULLIF(c.campaign_name,''),'') campaign_name,
      COALESCE(NULLIF(c.campaign_tp,''),'') campaign_tp
    FROM fact_ad_daily f
    LEFT JOIN dim_ad a ON f.customer_id::text=a.customer_id::text AND f.ad_id=a.ad_id
    LEFT JOIN dim_adgroup g ON a.customer_id::text=g.customer_id::text AND a.adgroup_id=g.adgroup_id
    LEFT JOIN dim_campaign c ON g.customer_id::text=c.customer_id::text AND g.campaign_id=c.campaign_id
    WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
    GROUP BY f.customer_id::text, f.ad_id, ad_name, g.adgroup_name, c.campaign_name, c.campaign_tp
    ORDER BY cost DESC NULLS LAST
    LIMIT :lim
    """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2), "lim": int(topn_cost)})
    return df if df is not None else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def bundle_keyword(engine, d1: date, d2: date, cids: Tuple[int, ...], topn_cost: int) -> pd.DataFrame:
    if not (table_exists(engine,"fact_keyword_daily") and table_exists(engine,"dim_keyword") and table_exists(engine,"dim_adgroup") and table_exists(engine,"dim_campaign")):
        return pd.DataFrame()
    fk_cols = get_table_columns(engine, "fact_keyword_daily")
    sales_sum = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"
    kw_cols = get_table_columns(engine, "dim_keyword")
    kw_expr = "k.keyword" if "keyword" in kw_cols else ("k.keyword_name" if "keyword_name" in kw_cols else "''::text")

    where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    sqlq = f"""
    WITH scope AS (
      SELECT
        k.customer_id::text customer_id,
        k.keyword_id::text  keyword_id,
        COALESCE(NULLIF(TRIM({kw_expr}),''),'') keyword,
        k.adgroup_id::text adgroup_id,
        COALESCE(NULLIF(TRIM(g.adgroup_name),''),'') adgroup_name,
        g.campaign_id::text campaign_id,
        COALESCE(NULLIF(TRIM(c.campaign_name),''),'') campaign_name,
        COALESCE(NULLIF(TRIM(c.campaign_tp),''),'') campaign_tp
      FROM dim_keyword k
      LEFT JOIN dim_adgroup g ON k.customer_id::text=g.customer_id::text AND k.adgroup_id::text=g.adgroup_id::text
      LEFT JOIN dim_campaign c ON g.customer_id::text=c.customer_id::text AND g.campaign_id::text=c.campaign_id::text
    ),
    base AS (
      SELECT
        fk.customer_id::text customer_id, fk.keyword_id::text keyword_id,
        SUM(fk.imp) imp, SUM(fk.clk) clk, SUM(fk.cost) cost, SUM(fk.conv) conv, {sales_sum} sales
      FROM fact_keyword_daily fk
      JOIN scope s ON fk.customer_id::text=s.customer_id AND fk.keyword_id::text=s.keyword_id
      WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid}
      GROUP BY fk.customer_id::text, fk.keyword_id::text
    )
    SELECT b.*, s.keyword, s.adgroup_name, s.campaign_name, s.campaign_tp
    FROM base b
    LEFT JOIN scope s ON b.customer_id=s.customer_id AND b.keyword_id=s.keyword_id
    ORDER BY b.cost DESC NULLS LAST
    LIMIT :lim
    """
    df = sql_read(engine, sqlq, {"d1": str(d1), "d2": str(d2), "lim": int(topn_cost)})
    return df if df is not None else pd.DataFrame()

# -----------------------------
# Pages
# -----------------------------
def header(latest: Dict[str,str]):
    st.markdown(
        f"""
<div class="card">
  <div style="display:flex; justify-content:space-between; gap:16px; flex-wrap:wrap;">
    <div>
      <div class="smallmuted" style="letter-spacing:.16em; font-weight:900;">NAVER SEARCH ADS · DASHBOARD</div>
      <div style="font-size:32px; font-weight:900; margin-top:6px;">네이버 검색광고 통합 대시보드</div>
      <div class="smallmuted" style="margin-top:6px;">빌드: <b>{BUILD_TAG}</b></div>
    </div>
    <div style="text-align:right;">
      <div class="smallmuted" style="letter-spacing:.14em; font-weight:900;">DATA FRESHNESS</div>
      <div style="margin-top:10px;">
        <span class="badge blue">캠페인 {latest.get("fact_campaign_daily","-")}</span>
        <span class="badge blue">키워드 {latest.get("fact_keyword_daily","-")}</span>
        <span class="badge blue">소재 {latest.get("fact_ad_daily","-")}</span>
        <span class="badge blue">비즈머니 {latest.get("fact_bizmoney_daily","-")}</span>
      </div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

def page_budget(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 💰 예산/잔액")
    bundle = query_budget_bundle(engine, f["cids"], f["end"])
    if bundle is None or bundle.empty:
        st.warning("예산/잔액 데이터를 불러올 수 없습니다. (fact_bizmoney_daily/fact_campaign_daily 확인)")
        return

    v = bundle.copy()
    v["last_update"] = pd.to_datetime(v.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
    v["days_cover"] = pd.NA
    mask = v["avg_cost"].astype(float) > 0
    v.loc[mask, "days_cover"] = v.loc[mask, "bizmoney_balance"].astype(float) / v.loc[mask, "avg_cost"].astype(float)

    v["threshold"] = (v["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    v["threshold"] = v["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    v["상태"] = "🟢 여유"
    v.loc[v["bizmoney_balance"].astype(float) < v["threshold"].astype(float), "상태"] = "🔴 충전필요"

    total_balance = int(pd.to_numeric(v["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(v["current_month_cost"], errors="coerce").fillna(0).sum())
    need_cnt = int(v["상태"].astype(str).str.contains("충전필요").sum())

    c1, c2, c3 = st.columns(3)
    c1.metric("총 비즈머니 잔액", format_won(total_balance))
    c2.metric(f"{f['end'].month}월 총 사용액", format_won(total_month_cost))
    c3.metric("충전 필요 계정", f"{need_cnt}건")
    st.markdown(
        f"<span class='badge red'>충전필요 {need_cnt}건</span><span class='badge green'>여유 {len(v)-need_cnt}건</span>",
        unsafe_allow_html=True,
    )

    only_topup = st.checkbox("충전필요만 보기", value=False)
    v["_rank"] = v["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
    v = v.sort_values(["_rank","bizmoney_balance","account_name"]).drop(columns=["_rank"])
    if only_topup:
        v = v[v["상태"].str.contains("충전필요", na=False)].copy()

    def _days(x):
        if pd.isna(x): return "-"
        try:
            xx = float(x)
        except Exception:
            return "-"
        return "99+일" if xx > 99 else f"{xx:.1f}일"

    view = pd.DataFrame({
        "업체명": v["account_name"],
        "담당자": v["manager"],
        "비즈머니 잔액": v["bizmoney_balance"].map(format_won),
        f"최근{TOPUP_AVG_DAYS}일 평균소진": v["avg_cost"].map(format_won),
        "D-소진": v["days_cover"].map(_days),
        "전일 소진액": v["y_cost"].map(format_won),
        "상태": v["상태"],
        "확인일자": v["last_update"],
    })
    st.dataframe(view, use_container_width=True, hide_index=True)
    render_download(view, f"예산_잔액_{f['start']}_{f['end']}", "budget", "budget")

    st.divider()
    st.markdown(f"### 📅 월 예산 ({f['end'].strftime('%Y-%m')})")

    bv = bundle.copy()
    bv["monthly_budget"] = pd.to_numeric(bv.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    bv["current_month_cost"] = pd.to_numeric(bv.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)
    bv["usage_rate"] = 0.0
    m2 = bv["monthly_budget"] > 0
    bv.loc[m2, "usage_rate"] = bv.loc[m2, "current_month_cost"] / bv.loc[m2, "monthly_budget"]
    bv["usage_pct"] = (bv["usage_rate"] * 100).fillna(0.0)

    def _st(rate: float, budget: int):
        if budget == 0: return ("⚪ 미설정", 3)
        if rate >= 1.0: return ("🔴 초과", 0)
        if rate >= 0.9: return ("🟡 주의", 1)
        return ("🟢 적정", 2)

    tmp = bv.apply(lambda r: _st(float(r["usage_rate"]), int(r["monthly_budget"])), axis=1, result_type="expand")
    bv["상태"] = tmp[0]; bv["_rank"] = tmp[1].astype(int)
    bv = bv.sort_values(["_rank","usage_rate","account_name"], ascending=[True, False, True]).reset_index(drop=True)

    bview = pd.DataFrame({
        "업체명": bv["account_name"],
        "담당자": bv["manager"],
        "월 예산(원)": bv["monthly_budget"].map(format_num),
        f"{f['end'].month}월 사용액": bv["current_month_cost"].map(format_num),
        "집행률(%)": bv["usage_pct"].map(lambda x: round(float(x),1)),
        "상태": bv["상태"],
    })
    st.dataframe(bview, use_container_width=True, hide_index=True)
    render_download(bview, f"월예산_{f['start']}_{f['end']}", "monthly_budget", "mb")

    st.markdown("#### ✍️ 월 예산 수정 (선택 → 입력 → 저장)")
    labels = (bv["account_name"].astype(str) + " (" + bv["customer_id"].astype(str) + ")").tolist()
    label_to_cid = dict(zip(labels, bv["customer_id"].astype(int).tolist()))
    if labels:
        with st.form("budget_update_form"):
            sel = st.selectbox("업체", labels, index=0)
            cid = int(label_to_cid.get(sel, 0))
            cur_budget = int(bv.loc[bv["customer_id"] == cid, "monthly_budget"].iloc[0])
            new_budget = st.text_input("새 월 예산(원)", value=format_num(cur_budget))
            ok = st.form_submit_button("💾 저장", use_container_width=True)
        if ok:
            nb = parse_currency(new_budget)
            update_monthly_budget(engine, cid, nb)
            st.success("저장 완료 (캐시 갱신)")
            st.cache_data.clear()
            st.rerun()

def _merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    x = df.copy()
    x["customer_id"] = pd.to_numeric(x["customer_id"], errors="coerce").fillna(0).astype("int64")
    return x.merge(meta[["customer_id","account_name","manager"]], on="customer_id", how="left")

def page_campaign(meta: pd.DataFrame, engine, f: Dict):
    if not f["ready"]:
        st.info("필터에서 **✅ 적용**을 누르면 조회가 시작돼.")
        return
    st.markdown("## 🚀 캠페인 성과")
    ts = ts_campaign(engine, f["start"], f["end"], f["cids"], f["type_sel"])
    if ts is not None and not ts.empty:
        cost = float(ts["cost"].sum()); clk = float(ts["clk"].sum()); conv = float(ts["conv"].sum())
        sales = float(ts.get("sales",0).sum()) if "sales" in ts.columns else 0.0
        roas = (sales / cost * 100) if cost > 0 else 0.0
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("총 광고비", format_won(cost))
        c2.metric("총 클릭", format_num(clk))
        c3.metric("총 전환", format_num(conv))
        c4.metric("총 ROAS", f"{roas:.0f}%")
        show_chart(line_chart(ts, "dt", "cost", "광고비(원)"))
    bundle = bundle_campaign(engine, f["start"], f["end"], f["cids"], f["type_sel"], f["top_campaign"])
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return
    df = _merge_meta(bundle, meta)
    df["campaign_type"] = df.get("campaign_tp","").astype(str).map(tp_to_label)
    df = add_rates(df)

    top5 = df.sort_values("cost", ascending=False).head(5).copy()
    top5_view = pd.DataFrame({
        "업체명": top5["account_name"],
        "광고유형": top5["campaign_type"],
        "캠페인": top5["campaign_name"],
        "광고비": top5["cost"].map(format_won),
        "전환": pd.to_numeric(top5["conv"], errors="coerce").fillna(0).astype(int),
    })
    with st.expander("📌 광고비 TOP5", expanded=True):
        st.dataframe(top5_view, use_container_width=True, hide_index=True)

    view = df.copy()
    view = view.rename(columns={
        "account_name":"업체명","manager":"담당자","campaign_type":"광고유형","campaign_name":"캠페인",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","sales":"매출","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA","roas":"ROAS(%)"
    })
    view["광고비"] = view["광고비"].map(format_won)
    view["매출"] = pd.to_numeric(view.get("매출",0), errors="coerce").fillna(0).map(format_won)
    view["CPC"] = pd.to_numeric(view.get("CPC",0), errors="coerce").fillna(0).map(format_won)
    view["CPA"] = pd.to_numeric(view.get("CPA",0), errors="coerce").fillna(0).map(format_won)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")
    view["노출"] = pd.to_numeric(view["노출"], errors="coerce").fillna(0).astype(int)
    view["클릭"] = pd.to_numeric(view["클릭"], errors="coerce").fillna(0).astype(int)
    view["전환"] = pd.to_numeric(view["전환"], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","광고유형","캠페인","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","매출","ROAS(%)"]
    out = view[cols].copy()
    st.dataframe(out, use_container_width=True, hide_index=True)
    render_download(out, f"캠페인_TOP{f['top_campaign']}_{f['start']}_{f['end']}", "campaign", "camp")

def page_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f["ready"]:
        st.info("필터에서 **✅ 적용**을 누르면 조회가 시작돼.")
        return
    st.markdown("## 🔎 키워드 성과")
    ts = ts_keyword(engine, f["start"], f["end"], f["cids"], f["type_sel"])
    if ts is not None and not ts.empty:
        cost = float(ts["cost"].sum()); clk = float(ts["clk"].sum()); conv = float(ts["conv"].sum())
        sales = float(ts.get("sales",0).sum()) if "sales" in ts.columns else 0.0
        roas = (sales / cost * 100) if cost > 0 else 0.0
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("총 광고비", format_won(cost))
        c2.metric("총 클릭", format_num(clk))
        c3.metric("총 전환", format_num(conv))
        c4.metric("총 ROAS", f"{roas:.0f}%")
        show_chart(line_chart(ts, "dt", "cost", "광고비(원)"))

    bundle = bundle_keyword(engine, f["start"], f["end"], f["cids"], f["top_keyword"])
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return
    df = _merge_meta(bundle, meta)
    df = add_rates(df)

    top10 = df.sort_values("cost", ascending=False).head(10).copy()
    top10_view = pd.DataFrame({
        "업체명": top10["account_name"],
        "키워드": top10.get("keyword",""),
        "광고비": top10["cost"].map(format_won),
        "전환": pd.to_numeric(top10["conv"], errors="coerce").fillna(0).astype(int),
    })
    with st.expander("📌 광고비 TOP10", expanded=True):
        st.dataframe(top10_view, use_container_width=True, hide_index=True)

    view = df.copy()
    view = view.rename(columns={
        "account_name":"업체명","manager":"담당자","campaign_name":"캠페인","adgroup_name":"광고그룹","keyword":"키워드",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","sales":"매출","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA","roas":"ROAS(%)"
    })
    view["광고비"] = view["광고비"].map(format_won)
    view["매출"] = pd.to_numeric(view.get("매출",0), errors="coerce").fillna(0).map(format_won)
    view["CPC"] = pd.to_numeric(view.get("CPC",0), errors="coerce").fillna(0).map(format_won)
    view["CPA"] = pd.to_numeric(view.get("CPA",0), errors="coerce").fillna(0).map(format_won)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")
    view["노출"] = pd.to_numeric(view["노출"], errors="coerce").fillna(0).astype(int)
    view["클릭"] = pd.to_numeric(view["클릭"], errors="coerce").fillna(0).astype(int)
    view["전환"] = pd.to_numeric(view["전환"], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","캠페인","광고그룹","키워드","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","매출","ROAS(%)"]
    out = view[cols].copy()
    st.dataframe(out, use_container_width=True, hide_index=True)
    render_download(out, f"키워드_TOP{f['top_keyword']}_{f['start']}_{f['end']}", "keyword", "kw")

def page_ad(meta: pd.DataFrame, engine, f: Dict):
    if not f["ready"]:
        st.info("필터에서 **✅ 적용**을 누르면 조회가 시작돼.")
        return
    st.markdown("## 🧩 소재 성과")
    ts = ts_ad(engine, f["start"], f["end"], f["cids"], f["type_sel"])
    if ts is not None and not ts.empty:
        cost = float(ts["cost"].sum()); clk = float(ts["clk"].sum()); conv = float(ts["conv"].sum())
        sales = float(ts.get("sales",0).sum()) if "sales" in ts.columns else 0.0
        roas = (sales / cost * 100) if cost > 0 else 0.0
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("총 광고비", format_won(cost))
        c2.metric("총 클릭", format_num(clk))
        c3.metric("총 전환", format_num(conv))
        c4.metric("총 ROAS", f"{roas:.0f}%")
        show_chart(line_chart(ts, "dt", "cost", "광고비(원)"))

    bundle = bundle_ad(engine, f["start"], f["end"], f["cids"], f["top_ad"])
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return
    df = _merge_meta(bundle, meta)
    df["campaign_type"] = df.get("campaign_tp","").astype(str).map(tp_to_label)
    df = add_rates(df)

    top5 = df.sort_values("cost", ascending=False).head(5).copy()
    top5_view = pd.DataFrame({
        "업체명": top5["account_name"],
        "캠페인": top5.get("campaign_name",""),
        "소재내용": top5.get("ad_name",""),
        "광고비": top5["cost"].map(format_won),
        "전환": pd.to_numeric(top5["conv"], errors="coerce").fillna(0).astype(int),
    })
    with st.expander("📌 광고비 TOP5", expanded=True):
        st.dataframe(top5_view, use_container_width=True, hide_index=True)

    view = df.copy()
    view = view.rename(columns={
        "account_name":"업체명","manager":"담당자","campaign_name":"캠페인","adgroup_name":"광고그룹","ad_id":"소재ID","ad_name":"소재내용",
        "imp":"노출","clk":"클릭","cost":"광고비","conv":"전환","sales":"매출","ctr":"CTR(%)","cpc":"CPC","cpa":"CPA","roas":"ROAS(%)"
    })
    view["광고비"] = view["광고비"].map(format_won)
    view["매출"] = pd.to_numeric(view.get("매출",0), errors="coerce").fillna(0).map(format_won)
    view["CPC"] = pd.to_numeric(view.get("CPC",0), errors="coerce").fillna(0).map(format_won)
    view["CPA"] = pd.to_numeric(view.get("CPA",0), errors="coerce").fillna(0).map(format_won)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")
    view["노출"] = pd.to_numeric(view["노출"], errors="coerce").fillna(0).astype(int)
    view["클릭"] = pd.to_numeric(view["클릭"], errors="coerce").fillna(0).astype(int)
    view["전환"] = pd.to_numeric(view["전환"], errors="coerce").fillna(0).astype(int)

    cols = ["업체명","담당자","캠페인","광고그룹","소재ID","소재내용","노출","클릭","CTR(%)","CPC","광고비","전환","CPA","매출","ROAS(%)"]
    out = view[cols].copy()
    st.dataframe(out, use_container_width=True, hide_index=True)
    render_download(out, f"소재_TOP{f['top_ad']}_{f['start']}_{f['end']}", "ad", "ad")

def page_settings(engine):
    st.markdown("## ⚙️ 설정")
    ensure_meta_table(engine)
    st.caption("accounts.xlsx → dim_account_meta 시드/업데이트")

    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"])
    df_up = None
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.success(f"업로드 OK: {len(df_up):,} rows")
            st.dataframe(df_up.head(20), use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"엑셀 읽기 실패: {e}")

    if st.button("📥 accounts → dim_account_meta 반영", use_container_width=True):
        try:
            n = seed_meta(engine, accounts_df=df_up)
            st.success(f"완료: {n}개 반영")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"실패: {e}")

    if st.button("🧹 캐시 초기화", use_container_width=True):
        st.cache_data.clear()
        st.session_state.pop("_table_cols_cache", None)
        st.success("캐시 초기화 완료")
        st.rerun()

    st.divider()
    meta = get_meta(engine)
    if meta is None or meta.empty:
        st.warning("dim_account_meta가 비어있습니다. Settings에서 시드하세요.")
    else:
        view = meta.copy()
        view["monthly_budget"] = pd.to_numeric(view.get("monthly_budget",0), errors="coerce").fillna(0).astype(int).map(format_num)
        view = view.rename(columns={"account_name":"업체명","manager":"담당자","monthly_budget":"월예산(원)"})
        st.dataframe(view[["customer_id","업체명","담당자","월예산(원)"]], use_container_width=True, hide_index=True)

# -----------------------------
# Main
# -----------------------------
def main():
    try:
        engine = get_engine()
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        st.stop()

    meta = get_meta(engine)
    dim_campaign = load_dim_campaign(engine)
    header(latest_dates(engine))

    if meta is None or meta.empty:
        st.warning("dim_account_meta가 비어있어. ⚙️ 설정 탭에서 accounts.xlsx로 시드해줘.")
    f = build_filters(meta if meta is not None else pd.DataFrame(), dim_campaign)

    tabs = st.tabs(["💰 예산/잔액", "🚀 캠페인", "🔎 키워드", "🧩 소재", "⚙️ 설정"])
    with tabs[0]:
        page_budget(meta, engine, f)
    with tabs[1]:
        page_campaign(meta, engine, f)
    with tabs[2]:
        page_keyword(meta, engine, f)
    with tabs[3]:
        page_ad(meta, engine, f)
    with tabs[4]:
        page_settings(engine)

if __name__ == "__main__":
    main()
