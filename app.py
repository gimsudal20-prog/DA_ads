# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.0: 캐싱 적용 + 속도 최적화)
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
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Page Configuration
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

# -----------------------------
# CSS & Styling
# -----------------------------
GLOBAL_UI_CSS = """
<style>
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
</style>
"""
st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)

# -----------------------------
# Config / Constants
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

# -----------------------------
# Database Connection (Cached Resource)
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
    """DB 연결 객체 생성 (전역 캐싱)"""
    return create_engine(get_database_url(), pool_pre_ping=True, future=True)

# -----------------------------
# Data Loaders (Cached Data)
# -----------------------------
# 중요: _engine 처럼 밑줄을 붙이면 Streamlit이 해싱(변경감지)에서 제외합니다.
# 속도 향상의 핵심입니다.

def sql_read(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    """Helper: 단순 쿼리 실행 (캐싱 안함, 내부용)"""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def sql_exec(engine, sql: str, params: Optional[dict] = None) -> None:
    """Helper: INSERT/UPDATE 실행"""
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

# 🔥 핵심: 데이터 조회 함수에 캐싱(@st.cache_data) 적용
# ttl=600 : 10분간 데이터 보관 (새로고침해도 DB 안감)

@st.cache_data(ttl=600, show_spinner=False)
def load_fact(_engine, table: str, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """
    FACT 테이블 조회 (날짜 범위)
    - 최적화: SELECT * 대신 필요한 컬럼만 명시
    """
    if not table_exists(_engine, table):
        return pd.DataFrame()

    # 필요한 컬럼만 가져오기 (전송량 감소)
    # 공통 컬럼: dt, customer_id, imp, clk, cost, conv, sales
    # 테이블별 ID: campaign_id / keyword_id / ad_id
    
    id_col = "campaign_id"
    if "keyword" in table: id_col = "keyword_id"
    elif "ad" in table: id_col = "ad_id"

    # sales 컬럼 존재 여부 확인 (안전장치)
    cols_check = get_table_columns(_engine, table)
    has_sales = "sales" in cols_check
    sales_part = ", sales" if has_sales else ", 0 as sales"

    sql = f"""
        SELECT dt, customer_id, {id_col}, imp, clk, cost, conv {sales_part}
        FROM {table}
        WHERE dt BETWEEN :d1 AND :d2
    """
    
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

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

@st.cache_data(ttl=3600) # 1시간 캐시
def get_meta(_engine) -> pd.DataFrame:
    """계정 메타 정보 조회"""
    if not table_exists(_engine, "dim_account_meta"):
         return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget"])
         
    df = sql_read(
        _engine,
        """
        SELECT customer_id, account_name, manager, monthly_budget, updated_at
        FROM dim_account_meta
        ORDER BY account_name
        """
    )
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=300) # 5분 캐시 (비즈머니는 자주 바뀔 수 있음)
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

@st.cache_data(ttl=600)
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

@st.cache_data(ttl=600)
def get_recent_avg_cost(_engine, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])
    
    # 내부적으로 cached load_fact 호출 대신 직접 쿼리하여 최적화
    sql = """
        SELECT customer_id, SUM(cost) as total_cost
        FROM fact_campaign_daily
        WHERE dt BETWEEN :d1 AND :d2
        GROUP BY customer_id
    """
    tmp = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    
    if tmp.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])
        
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"])
    tmp["customer_id"] = tmp["customer_id"].astype("int64")

    if customer_ids:
        tmp = tmp[tmp["customer_id"].isin(customer_ids)]

    days_diff = max((d2 - d1).days + 1, 1)
    tmp["avg_cost"] = tmp["total_cost"].astype(float) / days_diff
    return tmp[["customer_id", "avg_cost"]]

@st.cache_data(ttl=3600)
def get_dim_campaign(_engine) -> pd.DataFrame:
    """캠페인 DIM 정보 조회 (1시간 캐시)"""
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()
    return sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")

@st.cache_data(ttl=3600)
def get_dim_ad(_engine) -> pd.DataFrame:
    """소재 DIM 정보 조회 (1시간 캐시)"""
    if not table_exists(_engine, "dim_ad"):
        return pd.DataFrame()
    
    cols = get_table_columns(_engine, "dim_ad")
    if "creative_text" in cols:
        return sql_read(_engine, "SELECT customer_id, ad_id, COALESCE(NULLIF(creative_text,''), NULLIF(ad_name,''), '') AS ad_name, adgroup_id FROM dim_ad")
    else:
        return sql_read(_engine, "SELECT customer_id, ad_id, ad_name, adgroup_id FROM dim_ad")

# -----------------------------
# Helpers: Excel Download & Clock
# -----------------------------
def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str = "data", key_prefix: str = "") -> None:
    if df is None or df.empty: return
    st.markdown("""
        <style>
        div[data-testid="stDownloadButton"] button {
            padding: 0.15rem 0.55rem !important;
            font-size: 0.80rem !important;
            line-height: 1.2 !important;
            min-height: 28px !important;
        }
        </style>
        """, unsafe_allow_html=True)
    c1, c2 = st.columns([1, 8])
    with c1:
        st.download_button(
            "XLSX", data=df_to_xlsx_bytes(df, sheet_name=sheet_name),
            file_name=f"{filename_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx", use_container_width=True
        )
    with c2: st.caption("다운로드")

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
              timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
              hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
            }});
            document.getElementById('live-clock').textContent = "현재 시각: " + fmt.format(now);
          }}
          tick(); setInterval(tick, 1000);
        </script>
        """, height=32
    )

# -----------------------------
# Formatters & Calculations
# -----------------------------
def format_currency(val) -> str:
    try: return f"{int(float(val)):,}원"
    except: return "0원"

def format_roas(val) -> str:
    try: return "-" if pd.isna(val) else f"{float(val):.0f}%"
    except: return "-"

def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns: return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")
    out[col] = s.apply(lambda x: "" if pd.isna(x) else ("0%" if float(x)==0 else f"{float(x):.1f}%"))
    return out

def add_rates(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    if "sales" not in g.columns: g["sales"] = 0
    g["roas"] = (g["sales"] / g["cost"].replace({0: pd.NA})) * 100
    return g

def calculate_delta(curr: float, prev: float, is_percent: bool = False, inverse: bool = False):
    if prev == 0: return None, "off"
    diff = curr - prev
    val_str = f"{diff:+.1f}%p" if is_percent else f"{diff:+,.0f}"
    color = "inverse" if inverse else "normal"
    return val_str, color

def campaign_tp_to_label(tp: str) -> str:
    _map = {"web_site": "파워링크", "website": "파워링크", "shopping": "쇼핑검색", "power_content": "파워콘텐츠", "place": "플레이스", "brand_search": "브랜드검색"}
    key = str(tp or "").strip().lower()
    for k, v in _map.items():
        if k in key: return v
    return tp or ""

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty: return []
    raw = dim_campaign.get("campaign_type_label", pd.Series([], dtype=str))
    present = set([x.strip() for x in raw.dropna().astype(str).tolist() if x and "기타" not in x])
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    return [x for x in order if x in present] + sorted([x for x in present if x not in order])

# -----------------------------
# Filters
# -----------------------------
def apply_type_filter_to_fact(fact: pd.DataFrame, dim_campaign: pd.DataFrame, type_sel: List[str]) -> pd.DataFrame:
    if fact is None or fact.empty or not type_sel: return fact
    if dim_campaign is None or dim_campaign.empty: return pd.DataFrame(columns=fact.columns)
    
    dc = dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]].copy()
    # Merge optimization: Ensure types match
    for d in [dc, fact]:
        d["customer_id"] = pd.to_numeric(d["customer_id"], errors="coerce").astype("Int64").fillna(0).astype(int)
    
    tmp = fact.merge(dc, on=["customer_id", "campaign_id"], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])

def apply_type_filter_to_kw_ad_fact(engine, fact: pd.DataFrame, dim_campaign: pd.DataFrame, type_sel: List[str], level: str) -> pd.DataFrame:
    if fact is None or fact.empty or not type_sel: return fact
    if dim_campaign is None or dim_campaign.empty: return pd.DataFrame(columns=fact.columns)

    # Note: dim_adgroup join required. Not cached here to avoid complexity, but could be.
    dim_grp = sql_read(engine, "SELECT customer_id, adgroup_id, campaign_id FROM dim_adgroup")
    if dim_grp.empty: return fact
    
    dc = dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]]
    dim_grp = dim_grp.merge(dc, on=["customer_id", "campaign_id"], how="left")
    dim_grp["campaign_type_label"] = dim_grp["campaign_type_label"].fillna("기타")

    target_dim = None
    if level == "keyword":
        target_dim = sql_read(engine, "SELECT customer_id, keyword_id, adgroup_id FROM dim_keyword")
        join_key = "keyword_id"
    else:
        target_dim = sql_read(engine, "SELECT customer_id, ad_id, adgroup_id FROM dim_ad")
        join_key = "ad_id"

    if target_dim.empty: return fact
    
    # Merge Chain
    target_dim = target_dim.merge(dim_grp[["customer_id", "adgroup_id", "campaign_type_label"]], on=["customer_id", "adgroup_id"], how="left")
    
    # Fact Merge
    tmp = fact.merge(target_dim[["customer_id", join_key, "campaign_type_label"]], on=["customer_id", join_key], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])


# -----------------------------
# Sidebar & Logic
# -----------------------------
def sidebar_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    st.sidebar.title("필터")
    with st.sidebar.expander("업체/담당자", expanded=True):
        q = st.text_input("업체명 검색", placeholder="예: 실리콘플러스")
        managers = sorted([m for m in meta["manager"].fillna("").unique().tolist() if str(m).strip()])
        manager_sel = st.multiselect("담당자", options=managers, default=[])
        
        tmp = meta.copy()
        if q: tmp = tmp[tmp["account_name"].str.contains(q, case=False, na=False)]
        if manager_sel: tmp = tmp[tmp["manager"].isin(manager_sel)]
        
        opt = tmp[["account_name", "customer_id"]].copy()
        opt["label"] = opt["account_name"]
        company_sel_labels = st.multiselect("업체", options=opt["label"].tolist(), default=[])
        sel_ids = opt[opt["label"].isin(company_sel_labels)]["customer_id"].astype(int).tolist() if company_sel_labels else []

    with st.sidebar.expander("기간", expanded=True):
        period = st.selectbox("기간", ["오늘", "어제", "최근 7일(오늘 제외)", "최근 30일(오늘 제외)", "직접 선택"], index=2)
        today = date.today()
        if period == "오늘": start, end = today, today
        elif period == "어제": start = end = today - timedelta(days=1)
        elif "7일" in period: end = today - timedelta(days=1); start = end - timedelta(days=6)
        elif "30일" in period: end = today - timedelta(days=1); start = end - timedelta(days=29)
        else:
            c1, c2 = st.columns(2)
            start = c1.date_input("시작일", value=today - timedelta(days=7))
            end = c2.date_input("종료일", value=today - timedelta(days=1))
        st.caption(f"{start} ~ {end}")

    with st.sidebar.expander("광고유형", expanded=True):
        type_sel = st.multiselect("검색광고 종류", options=type_opts, default=[])
    
    return {"q": q, "manager_sel": manager_sel, "selected_customer_ids": sel_ids, "start": start, "end": end, "type_sel": type_sel}

def resolve_selected_ids(meta: pd.DataFrame, f: Dict) -> List[int]:
    sel_ids = f["selected_customer_ids"]
    if (not sel_ids) and f["manager_sel"]:
        sel_ids = meta[meta["manager"].isin(f["manager_sel"])]["customer_id"].astype(int).tolist()
    return sel_ids

# -----------------------------
# Pages
# -----------------------------
def page_budget(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()
    
    df = meta.copy()
    if f["manager_sel"]: df = df[df["manager"].isin(f["manager_sel"])]
    if f["q"]: df = df[df["account_name"].str.contains(f["q"], case=False, na=False)]
    if f["selected_customer_ids"]: df = df[df["customer_id"].isin(f["selected_customer_ids"])]

    biz = get_latest_bizmoney(engine)
    
    # Yesterday Cost
    yesterday = date.today() - timedelta(days=1)
    df_yst = load_fact(engine, "fact_campaign_daily", yesterday, yesterday)
    if not df_yst.empty:
        df_yst = df_yst.groupby("customer_id", as_index=False)["cost"].sum().rename(columns={"cost": "y_cost"})
    
    # Biz View Construction
    biz_view = df[["customer_id", "account_name", "manager"]]
    if not biz.empty: biz_view = biz_view.merge(biz, on="customer_id", how="left")
    else: biz_view["bizmoney_balance"] = 0; biz_view["last_update"] = "-"
    
    if not df_yst.empty: biz_view = biz_view.merge(df_yst, on="customer_id", how="left")
    else: biz_view["y_cost"] = 0
    
    biz_view["bizmoney_balance"] = biz_view["bizmoney_balance"].fillna(0)
    biz_view["y_cost"] = biz_view["y_cost"].fillna(0)

    # Avg Cost
    avg_df = pd.DataFrame()
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1)
        d1 = d2 - timedelta(days=TOPUP_AVG_DAYS - 1)
        avg_df = get_recent_avg_cost(engine, d1, d2, customer_ids=df["customer_id"].tolist())
    
    if not avg_df.empty: biz_view = biz_view.merge(avg_df, on="customer_id", how="left")
    else: biz_view["avg_cost"] = 0.0
    
    # Calc Logic
    biz_view["avg_cost"] = biz_view["avg_cost"].fillna(0)
    biz_view["days_cover"] = biz_view.apply(lambda r: (r["bizmoney_balance"]/r["avg_cost"]) if r["avg_cost"]>0 else None, axis=1)
    
    biz_view["상태"] = biz_view.apply(lambda r: "🔴 충전필요" if r["bizmoney_balance"] < max(r["avg_cost"]*TOPUP_DAYS_COVER, TOPUP_STATIC_THRESHOLD) else "🟢 여유", axis=1)
    
    # Formatting
    biz_view["bizmoney_fmt"] = biz_view["bizmoney_balance"].apply(format_currency)
    biz_view["y_cost_fmt"] = biz_view["y_cost"].apply(format_currency)
    biz_view["avg_cost_fmt"] = biz_view["avg_cost"].apply(format_currency)
    biz_view["days_cover_fmt"] = biz_view["days_cover"].apply(lambda d: "-" if pd.isna(d) else ("99+일" if d>99 else f"{d:.1f}일"))

    # Budget View
    month_cost_df = get_monthly_cost(engine, f["end"])
    budget_view = df[["customer_id", "account_name", "manager", "monthly_budget"]].merge(month_cost_df, on="customer_id", how="left")
    budget_view["monthly_budget_val"] = budget_view["monthly_budget"].fillna(0).astype(int)
    budget_view["current_month_cost_val"] = budget_view["current_month_cost"].fillna(0).astype(int)
    budget_view["usage_rate"] = budget_view.apply(lambda r: (r["current_month_cost_val"]/r["monthly_budget_val"]) if r["monthly_budget_val"]>0 else 0, axis=1)

    # KPIs
    st.markdown("### 🔍 전체 계정 요약 (Command Center)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 비즈머니 잔액", format_currency(biz_view["bizmoney_balance"].sum()))
    c2.metric(f"{f['end'].month}월 총 사용액", format_currency(budget_view["current_month_cost_val"].sum()))
    cnt_low = int(biz_view["상태"].str.contains("충전필요").sum())
    c3.metric("충전 필요 계정", f"{cnt_low}건", delta_color="inverse")
    cnt_over = int((budget_view["usage_rate"] >= 1.0).sum())
    c4.metric("예산 초과 계정", f"{cnt_over}건", delta_color="inverse")
    st.divider()

    # Table 1: Bizmoney
    st.markdown("### 💳 비즈머니 잔액 현황")
    show_only_topup = st.checkbox("충전필요만 보기", key="show_only_topup")
    if show_only_topup: biz_view = biz_view[biz_view["상태"].str.contains("충전필요")]
    
    def _style_biz(row): return ["background-color: rgba(239,68,68,0.08); font-weight: 700;"] * len(row) if "충전필요" in str(row.get("상태", "")) else [""] * len(row)
    
    st.dataframe(
        biz_view[["account_name", "manager", "bizmoney_fmt", "avg_cost_fmt", "days_cover_fmt", "y_cost_fmt", "상태", "last_update"]].style.apply(_style_biz, axis=1),
        use_container_width=True, hide_index=True,
        column_config={"account_name":"업체명", "bizmoney_fmt":"비즈머니", "avg_cost_fmt":f"최근{TOPUP_AVG_DAYS}일 평균", "days_cover_fmt":"D-소진", "y_cost_fmt":"전일소진"}
    )
    st.divider()

    # Table 2: Budget
    st.markdown(f"### 📅 월 예산 관리 ({f['end'].month}월)")
    # Logic for status
    def get_status(rate, budget):
        if budget == 0: return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0: return ("🔴 초과", "초과", 0)
        if rate >= 0.9: return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)
    
    tmp = budget_view.apply(lambda r: get_status(r["usage_rate"], r["monthly_budget_val"]), axis=1, result_type="expand")
    budget_view["status_icon"], budget_view["status_text"], budget_view["_rank"] = tmp[0], tmp[1], tmp[2]
    budget_view["usage_pct"] = (budget_view["usage_rate"]*100).fillna(0)
    budget_view = budget_view.sort_values(["_rank", "usage_rate"], ascending=[True, False])

    c1, c2 = st.columns([3, 1])
    with c1:
        edited = st.data_editor(
            budget_view[["customer_id", "account_name", "manager", "monthly_budget_val", "current_month_cost_val", "usage_pct", "status_icon"]],
            use_container_width=True, hide_index=True,
            column_config={
                "customer_id": st.column_config.NumberColumn("CID", disabled=True),
                "account_name": "업체명",
                "monthly_budget_val": st.column_config.NumberColumn("월 예산", format="%,d", min_value=0, step=10000),
                "current_month_cost_val": st.column_config.NumberColumn(f"{f['end'].month}월 사용액", disabled=True, format="%,d"),
                "usage_pct": st.column_config.NumberColumn("집행률", format="%.1f%%", disabled=True),
                "status_icon": st.column_config.TextColumn("상태", disabled=True)
            }, key="budget_editor"
        )
    with c2:
        st.info("💡 우측 '월 예산'을 더블클릭하여 수정 후 저장하세요.")
        if st.button("💾 예산 저장", type="primary", use_container_width=True):
            with engine.begin() as conn:
                for _, r in edited.iterrows():
                    cid, val = int(r["customer_id"]), int(r["monthly_budget_val"])
                    conn.execute(text("UPDATE dim_account_meta SET monthly_budget=:b, updated_at=now() WHERE customer_id=:c"), {"b":val, "c":cid})
            get_meta.clear() # Clear cache
            st.success("저장 완료"); st.rerun()


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict, dim_campaign: pd.DataFrame):
    st.markdown("## 🚀 성과 대시보드 (캠페인)")
    sel_ids = resolve_selected_ids(meta, f)
    
    fact = load_fact(engine, "fact_campaign_daily", f["start"], f["end"], sel_ids)
    fact = apply_type_filter_to_fact(fact, dim_campaign, f["type_sel"])
    
    if fact.empty: st.warning("데이터 없음"); return

    # Prev period
    duration = (f["end"] - f["start"]).days + 1
    prev_end = f["start"] - timedelta(days=1)
    prev_start = prev_end - timedelta(days=duration - 1)
    fact_prev = load_fact(engine, "fact_campaign_daily", prev_start, prev_end, sel_ids)
    fact_prev = apply_type_filter_to_fact(fact_prev, dim_campaign, f["type_sel"])

    # Metrics
    c_imp, c_clk, c_cost, c_conv, c_sales = fact["imp"].sum(), fact["clk"].sum(), fact["cost"].sum(), fact["conv"].sum(), fact["sales"].sum()
    p_imp, p_clk, p_cost, p_conv, p_sales = (fact_prev["imp"].sum(), fact_prev["clk"].sum(), fact_prev["cost"].sum(), fact_prev["conv"].sum(), fact_prev["sales"].sum()) if not fact_prev.empty else (0,0,0,0,0)

    tab1, tab2 = st.tabs(["📊 차트 & 요약", "📋 상세 리스트"])
    
    with tab1:
        c1, c2, c3, c4, c5 = st.columns(5)
        d_cost, _ = calculate_delta(c_cost, p_cost)
        c1.metric("총 광고비", format_currency(c_cost), delta=d_cost)
        d_conv, _ = calculate_delta(c_conv, p_conv)
        c2.metric("총 전환", f"{int(c_conv):,}", delta=d_conv)
        c_ctr = (c_clk/c_imp*100) if c_imp else 0; p_ctr = (p_clk/p_imp*100) if p_imp else 0
        d_ctr, _ = calculate_delta(c_ctr, p_ctr, True)
        c3.metric("CTR", f"{c_ctr:.2f}%", delta=d_ctr)
        c_roas = (c_sales/c_cost*100) if c_cost else 0; p_roas = (p_sales/p_cost*100) if p_cost else 0
        d_roas, _ = calculate_delta(c_roas, p_roas, True)
        c5.metric("ROAS", f"{c_roas:.0f}%", delta=d_roas)
        st.divider()

        # Chart
        st.subheader("📈 일별 추세")
        daily = fact.groupby("dt", as_index=False)[["imp", "clk", "cost", "conv", "sales"]].sum()
        daily["dt"] = pd.to_datetime(daily["dt"])
        daily["roas"] = daily.apply(lambda r: (r["sales"]/r["cost"]*100) if r["cost"] else 0, axis=1)
        
        base = alt.Chart(daily).encode(x=alt.X("dt:T", title="날짜"))
        bar = base.mark_bar(opacity=0.5).encode(y=alt.Y("cost:Q", title="광고비"), tooltip=["dt", "cost"])
        line = base.mark_line(color="red").encode(y=alt.Y("roas:Q", title="ROAS(%)"), tooltip=["dt", "roas"])
        st.altair_chart((bar + line).resolve_scale(y='independent'), use_container_width=True)

    with tab2:
        # Detailed Table
        g = fact.groupby(["customer_id", "campaign_id"], as_index=False)[["imp", "clk", "cost", "conv", "sales"]].sum()
        g = add_rates(g)
        g = g.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")
        g = g.merge(dim_campaign, on=["customer_id", "campaign_id"], how="left")
        g["campaign_name"] = g["campaign_name"].fillna("미확인")
        
        show = g.sort_values("cost", ascending=False)
        show["cost_fmt"] = show["cost"].apply(format_currency)
        show["roas_fmt"] = show["roas"].apply(format_roas)
        show["ctr_fmt"] = show["ctr"].apply(lambda x: f"{x:.2f}%")
        
        st.dataframe(
            show[["account_name", "campaign_name", "cost_fmt", "roas_fmt", "ctr_fmt", "conv", "imp", "clk"]],
            use_container_width=True, hide_index=True,
            column_config={"cost_fmt": "광고비", "roas_fmt": "ROAS", "ctr_fmt": "CTR"}
        )
        render_download_compact(show, f"report_campaign_{f['start']}")

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict, dim_campaign: pd.DataFrame):
    st.markdown("## 🔑 성과 대시보드 (키워드)")
    sel_ids = resolve_selected_ids(meta, f)
    fact = load_fact(engine, "fact_keyword_daily", f["start"], f["end"], sel_ids)
    fact = apply_type_filter_to_kw_ad_fact(engine, fact, dim_campaign, f["type_sel"], "keyword")
    
    if fact.empty: st.warning("데이터 없음"); return

    g = fact.groupby(["customer_id", "keyword_id"], as_index=False)[["imp", "clk", "cost", "conv", "sales"]].sum()
    g = add_rates(g)
    
    # Get Keyword Name (Ad-hoc join)
    kw_ids = tuple(g["keyword_id"].unique())
    if kw_ids:
        # Use sql_read here as it's specific ID lookup
        q = f"SELECT keyword_id, keyword FROM dim_keyword WHERE keyword_id IN {kw_ids}".replace(",)", ")")
        if len(kw_ids)==1: q = q.replace("IN", "=").replace(str(kw_ids), f"'{kw_ids[0]}'")
        dim_kw = sql_read(engine, q)
        g = g.merge(dim_kw, on="keyword_id", how="left")
    
    g = g.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
    
    st.subheader("🏆 키워드 Top 20 (광고비 기준)")
    top = g.sort_values("cost", ascending=False).head(20)
    
    top["cost"] = top["cost"].apply(format_currency)
    top["roas"] = top["roas"].apply(format_roas)
    top["ctr"] = top["ctr"].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(
        top[["account_name", "keyword", "cost", "roas", "conv", "ctr", "clk"]],
        use_container_width=True, hide_index=True
    )

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict, dim_campaign: pd.DataFrame):
    st.markdown("## 🖼️ 성과 대시보드 (소재)")
    sel_ids = resolve_selected_ids(meta, f)
    fact = load_fact(engine, "fact_ad_daily", f["start"], f["end"], sel_ids)
    fact = apply_type_filter_to_kw_ad_fact(engine, fact, dim_campaign, f["type_sel"], "ad")
    
    if fact.empty: st.warning("데이터 없음"); return

    g = fact.groupby(["customer_id", "ad_id"], as_index=False)[["imp", "clk", "cost", "conv", "sales"]].sum()
    g = add_rates(g)
    
    # Get Ad Name (using cached dim_ad is better if full load)
    # But for optimization, let's load full dim_ad since user might need search
    dim_ad = get_dim_ad(engine)
    if not dim_ad.empty:
        dim_ad["customer_id"] = pd.to_numeric(dim_ad["customer_id"], errors="coerce").fillna(0).astype("int64")
        g = g.merge(dim_ad, on=["customer_id", "ad_id"], how="left")
    
    g = g.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
    
    st.subheader("🏆 소재 Top 20 (광고비 기준)")
    top = g.sort_values("cost", ascending=False).head(20)
    top["cost"] = top["cost"].apply(format_currency)
    top["roas"] = top["roas"].apply(format_roas)
    
    st.dataframe(
        top[["account_name", "ad_name", "cost", "roas", "conv", "clk"]],
        use_container_width=True, hide_index=True,
        column_config={"ad_name": st.column_config.TextColumn("소재내용", width="medium")}
    )

def page_settings(engine):
    st.markdown("## 설정 / 연결")
    if st.button("캐시 비우기 (새로고침)"):
        st.cache_data.clear()
        st.success("완료!")
        st.rerun()

# -----------------------------
# Main Entry
# -----------------------------
def main():
    st.title("네이버 검색광고 통합 대시보드")
    try:
        engine = get_engine()
    except Exception as e:
        st.error(f"DB 연결 실패: {e}"); return

    # Load Meta (Cached)
    meta = get_meta(engine)
    dim_campaign = get_dim_campaign(engine)
    
    if not dim_campaign.empty:
        dim_campaign["customer_id"] = pd.to_numeric(dim_campaign["customer_id"], errors="coerce").fillna(0).astype("int64")
        dim_campaign["campaign_type_label"] = dim_campaign["campaign_tp"].apply(campaign_tp_to_label)
        dim_campaign.loc[dim_campaign["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"

    type_opts = get_campaign_type_options(dim_campaign)
    f = sidebar_filters(meta, type_opts)

    page = st.sidebar.radio("메뉴", ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정"])

    if page == "전체 예산/잔액 관리": page_budget(meta, engine, f)
    elif page == "성과(캠페인)": page_perf_campaign(meta, engine, f, dim_campaign)
    elif page == "성과(키워드)": page_perf_keyword(meta, engine, f, dim_campaign)
    elif page == "성과(소재)": page_perf_ad(meta, engine, f, dim_campaign)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
