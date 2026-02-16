# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.5: SQL 그룹핑 최적화 + 한글 UI 복구)
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
# Database Connection
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

# -----------------------------
# Data Loaders (Core Optimization)
# -----------------------------
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

# [최적화 1] 일별 데이터 로딩 (차트용, 캠페인용) - 캐시 10분
@st.cache_data(ttl=600, show_spinner=False)
def load_fact(_engine, table: str, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    if not table_exists(_engine, table): return pd.DataFrame()
    
    # ID 컬럼 자동 식별
    if "keyword" in table: id_col = "keyword_id"
    elif "ad" in table: id_col = "ad_id"
    else: id_col = "campaign_id"
    
    # sales 컬럼 확인
    cols = get_table_columns(_engine, table)
    sales_expr = "sales" if "sales" in cols else "0 as sales"

    sql = f"""
        SELECT dt, customer_id, {id_col}, imp, clk, cost, conv, {sales_expr}
        FROM {table}
        WHERE dt BETWEEN :d1 AND :d2
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df.empty: return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    if customer_ids:
        df = df[df["customer_id"].isin([int(x) for x in customer_ids])].copy()
    return df

# [최적화 2] ★핵심★ 합계 데이터 로딩 (키워드/소재용) - DB에서 미리 합쳐서 가져옴
@st.cache_data(ttl=600, show_spinner=False)
def load_fact_aggregated(_engine, table: str, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    if not table_exists(_engine, table): return pd.DataFrame()
    
    if "keyword" in table: id_col = "keyword_id"
    elif "ad" in table: id_col = "ad_id"
    else: id_col = "campaign_id"
    
    cols = get_table_columns(_engine, table)
    sales_expr = "SUM(sales) as sales" if "sales" in cols else "0 as sales"

    # GROUP BY를 DB에서 실행하여 전송량 90% 감소
    sql = f"""
        SELECT customer_id, {id_col}, 
               SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, SUM(conv) as conv, {sales_expr}
        FROM {table}
        WHERE dt BETWEEN :d1 AND :d2
        GROUP BY customer_id, {id_col}
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df.empty: return df

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    if customer_ids:
        df = df[df["customer_id"].isin([int(x) for x in customer_ids])].copy()
    return df

@st.cache_data(ttl=3600)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_account_meta"): return pd.DataFrame(columns=["customer_id", "account_name", "manager"])
    df = sql_read(_engine, "SELECT customer_id, account_name, manager, monthly_budget FROM dim_account_meta ORDER BY account_name")
    if not df.empty: df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=300)
def get_latest_bizmoney(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"): return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
    sql = "SELECT DISTINCT ON (customer_id) customer_id, bizmoney_balance, dt as last_update FROM fact_bizmoney_daily ORDER BY customer_id, dt DESC"
    df = sql_read(_engine, sql)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=600)
def get_recent_avg_cost(_engine, d1: date, d2: date) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame(columns=["customer_id", "avg_cost"])
    sql = "SELECT customer_id, SUM(cost) as total_cost FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 GROUP BY customer_id"
    tmp = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if tmp.empty: return pd.DataFrame(columns=["customer_id", "avg_cost"])
    
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").fillna(0).astype("int64")
    days = max((d2 - d1).days + 1, 1)
    tmp["avg_cost"] = tmp["total_cost"].astype(float) / days
    return tmp[["customer_id", "avg_cost"]]

@st.cache_data(ttl=600)
def get_monthly_cost(_engine, target_date: date) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame(columns=["customer_id", "current_month_cost"])
    start_dt = target_date.replace(day=1)
    if target_date.month == 12: end_dt = date(target_date.year+1, 1, 1) - timedelta(days=1)
    else: end_dt = date(target_date.year, target_date.month+1, 1) - timedelta(days=1)
    
    sql = "SELECT customer_id, SUM(cost) as current_month_cost FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 GROUP BY customer_id"
    df = sql_read(_engine, sql, {"d1": str(start_dt), "d2": str(end_dt)})
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["current_month_cost"] = df["current_month_cost"].fillna(0).astype("int64")
    return df

# -----------------------------
# Helpers
# -----------------------------
def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str = "data", key_prefix: str = "") -> None:
    if df is None or df.empty: return
    c1, c2 = st.columns([1, 8])
    with c1:
        st.download_button("XLSX", data=df_to_xlsx_bytes(df, sheet_name), file_name=f"{filename_base}.xlsx", key=f"{key_prefix}_xlsx")
    with c2: st.caption("다운로드")

def render_live_clock(tz: str = "Asia/Seoul"):
    components.html(f"""<div style='text-align:right; font-size:12px; color:#666;'><span id='clock'></span></div><script>setInterval(()=>document.getElementById('clock').innerText=new Date().toLocaleString('ko-KR',{{timeZone:'{tz}'}}),1000)</script>""", height=30)

def format_currency(val):
    try: return f"{int(float(val)):,}원"
    except: return "0원"

def format_roas(val):
    try: return "-" if pd.isna(val) else f"{float(val):.0f}%"
    except: return "-"

def add_rates(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    if "sales" not in g.columns: g["sales"] = 0
    g["roas"] = (g["sales"] / g["cost"].replace({0: pd.NA})) * 100
    return g

def calculate_delta(curr, prev, is_percent=False, inverse=False):
    if prev == 0: return None, "off"
    diff = curr - prev
    val = f"{diff:+.1f}%p" if is_percent else f"{diff:+,.0f}"
    color = "inverse" if inverse else "normal"
    return val, color

def campaign_tp_to_label(tp):
    map_ = {"web": "파워링크", "shop": "쇼핑검색", "place": "플레이스", "brand": "브랜드", "content": "파워콘텐츠"}
    tp = str(tp).lower()
    for k, v in map_.items():
        if k in tp: return v
    return tp

def get_campaign_type_options(dim):
    if dim.empty: return []
    return sorted(list(set(dim["campaign_type_label"].dropna().unique())))

# -----------------------------
# Filters (Type)
# -----------------------------
def apply_type_filter(df, dim_campaign, type_sel):
    if df.empty or not type_sel: return df
    if dim_campaign.empty: return df
    
    # Merge for filtering
    tmp = df.merge(dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]], on=["customer_id", "campaign_id"], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])

def apply_type_filter_kw_ad(_engine, df, dim_campaign, type_sel, level="keyword"):
    if df.empty or not type_sel: return df
    
    # AdGroup Load (Not cached to keep simple, or minimal cache)
    dim_grp = sql_read(_engine, "SELECT customer_id, adgroup_id, campaign_id FROM dim_adgroup")
    if dim_grp.empty: return df
    
    # Join Chain: Item -> AdGroup -> Campaign -> Type
    # 1. Join AdGroup to Campaign to get Type
    dim_grp = dim_grp.merge(dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]], on=["customer_id", "campaign_id"], how="left")
    
    # 2. Get Item -> AdGroup mapping
    id_col = "keyword_id" if level == "keyword" else "ad_id"
    table = "dim_keyword" if level == "keyword" else "dim_ad"
    if not table_exists(_engine, table): return df
    
    dim_item = sql_read(_engine, f"SELECT customer_id, {id_col}, adgroup_id FROM {table}")
    dim_item = dim_item.merge(dim_grp[["customer_id", "adgroup_id", "campaign_type_label"]], on=["customer_id", "adgroup_id"], how="left")
    
    # 3. Filter Fact
    tmp = df.merge(dim_item[["customer_id", id_col, "campaign_type_label"]], on=["customer_id", id_col], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])


# -----------------------------
# Sidebar
# -----------------------------
def sidebar_filters(meta):
    st.sidebar.title("필터")
    with st.sidebar.expander("업체/담당자", expanded=True):
        q = st.text_input("업체명 검색")
        managers = sorted([x for x in meta["manager"].dropna().unique() if x])
        mgr_sel = st.multiselect("담당자", managers)
        
        filtered_meta = meta.copy()
        if q: filtered_meta = filtered_meta[filtered_meta["account_name"].str.contains(q, case=False)]
        if mgr_sel: filtered_meta = filtered_meta[filtered_meta["manager"].isin(mgr_sel)]
        
        cust_opts = filtered_meta["account_name"].tolist()
        cust_sel = st.multiselect("업체", cust_opts)
        
        sel_ids = []
        if cust_sel: sel_ids = filtered_meta[filtered_meta["account_name"].isin(cust_sel)]["customer_id"].tolist()
        elif mgr_sel: sel_ids = filtered_meta["customer_id"].tolist()
        
    with st.sidebar.expander("기간", expanded=True):
        # [최적화] 기본값을 '어제'로 변경 (Index 1) -> 첫 로딩 속도 향상
        p = st.selectbox("기간", ["오늘", "어제", "최근 7일", "최근 30일", "직접 선택"], index=1)
        today = date.today()
        if p=="오늘": s=e=today
        elif p=="어제": s=e=today-timedelta(days=1)
        elif "7일" in p: e=today-timedelta(days=1); s=e-timedelta(days=6)
        elif "30일" in p: e=today-timedelta(days=1); s=e-timedelta(days=29)
        else:
            c1,c2=st.columns(2)
            s=c1.date_input("시작"); e=c2.date_input("종료")
    
    return {"start": s, "end": e, "ids": sel_ids}

# -----------------------------
# Pages
# -----------------------------
def page_budget(meta, engine, f):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()
    
    # Filter Logic
    target_ids = f["ids"] if f["ids"] else meta["customer_id"].tolist()
    view = meta[meta["customer_id"].isin(target_ids)].copy()
    
    # Data Load
    biz = get_latest_bizmoney(engine)
    yst_cost = load_fact(engine, "fact_campaign_daily", date.today()-timedelta(days=1), date.today()-timedelta(days=1))
    if not yst_cost.empty:
        yst_cost = yst_cost.groupby("customer_id", as_index=False)["cost"].sum().rename(columns={"cost":"y_cost"})
    
    avg_cost = pd.DataFrame()
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1); d1 = d2 - timedelta(days=TOPUP_AVG_DAYS-1)
        avg_cost = get_recent_avg_cost(engine, d1, d2)
    
    # Merge
    view = view.merge(biz, on="customer_id", how="left").fillna({"bizmoney_balance":0})
    if not yst_cost.empty: view = view.merge(yst_cost, on="customer_id", how="left").fillna({"y_cost":0})
    else: view["y_cost"] = 0
    if not avg_cost.empty: view = view.merge(avg_cost, on="customer_id", how="left").fillna({"avg_cost":0})
    else: view["avg_cost"] = 0
    
    # Logic
    view["days"] = view.apply(lambda x: x["bizmoney_balance"]/x["avg_cost"] if x["avg_cost"]>0 else 999, axis=1)
    view["status"] = view.apply(lambda x: "🔴 충전필요" if x["bizmoney_balance"] < max(x["avg_cost"]*TOPUP_DAYS_COVER, TOPUP_STATIC_THRESHOLD) else "🟢 여유", axis=1)
    
    # Summary
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("총 비즈머니", format_currency(view["bizmoney_balance"].sum()))
    month_cost = get_monthly_cost(engine, f["end"])
    view = view.merge(month_cost, on="customer_id", how="left").fillna({"current_month_cost":0})
    c2.metric(f"{f['end'].month}월 사용액", format_currency(view["current_month_cost"].sum()))
    c3.metric("충전필요", f'{sum(view["status"].str.contains("충전"))}건')
    
    # Table
    st.markdown("### 💳 잔액 현황")
    if st.checkbox("충전필요만 보기"): view = view[view["status"].str.contains("충전")]
    
    show = view[["account_name", "manager", "bizmoney_balance", "avg_cost", "days", "y_cost", "status"]].copy()
    show["days"] = show["days"].apply(lambda x: f"{x:.1f}일" if x<100 else "99+일")
    for c in ["bizmoney_balance", "avg_cost", "y_cost"]: show[c] = show[c].apply(format_currency)
    
    def style_row(row): return ["background-color: #fee2e2"]*len(row) if "충전" in str(row["status"]) else [""]*len(row)
    st.dataframe(show.style.apply(style_row, axis=1), use_container_width=True, hide_index=True,
                 column_config={"account_name":"업체명", "bizmoney_balance":"비즈머니", "avg_cost":"평균소진", "days":"소진가능", "y_cost":"전일소진"})


def page_perf_campaign(meta, engine, f, dim_camp, type_sel):
    st.markdown("## 🚀 성과 (캠페인)")
    ids = f["ids"]
    
    # [최적화] 캠페인은 데이터가 적으므로 load_fact(일별) 사용 -> 차트 그리기 위해
    df = load_fact(engine, "fact_campaign_daily", f["start"], f["end"], ids)
    df = apply_type_filter(df, dim_camp, type_sel)
    
    if df.empty: st.warning("데이터 없음"); return
    
    # Prev
    d_len = (f["end"]-f["start"]).days + 1
    df_p = load_fact(engine, "fact_campaign_daily", f["start"]-timedelta(days=d_len), f["end"]-timedelta(days=d_len), ids)
    df_p = apply_type_filter(df_p, dim_camp, type_sel)
    
    # Metrics
    curr = df[["cost","sales","conv","imp","clk"]].sum()
    prev = df_p[["cost","sales","conv","imp","clk"]].sum() if not df_p.empty else curr*0
    
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("광고비", format_currency(curr["cost"]), calculate_delta(curr["cost"], prev["cost"])[0])
    c2.metric("매출", format_currency(curr["sales"]), calculate_delta(curr["sales"], prev["sales"])[0])
    c3.metric("ROAS", format_roas(curr["sales"]/curr["cost"]*100 if curr["cost"] else 0))
    c4.metric("전환수", f"{int(curr['conv']):,}")
    c5.metric("클릭수", f"{int(curr['clk']):,}")
    
    st.divider()
    
    t1, t2 = st.tabs(["차트", "상세 테이블"])
    with t1:
        daily = df.groupby("dt", as_index=False)[["cost","sales"]].sum()
        daily["roas"] = daily["sales"]/daily["cost"]*100
        base = alt.Chart(daily).encode(x="dt:T")
        c = base.mark_bar().encode(y="cost") + base.mark_line(color='red').encode(y="roas")
        st.altair_chart(c.resolve_scale(y='independent'), use_container_width=True)
        
    with t2:
        g = df.groupby(["customer_id","campaign_id"], as_index=False)[["imp","clk","cost","conv","sales"]].sum()
        g = add_rates(g)
        g = g.merge(meta[["customer_id","account_name"]], on="customer_id").merge(dim_camp, on=["customer_id","campaign_id"], how="left")
        
        # [한글 복구] 컬럼명 한글로 변경
        show = g[["account_name", "campaign_name", "imp", "clk", "ctr", "cpc", "cost", "conv", "cpa", "sales", "roas"]].copy()
        show = show.rename(columns={
            "account_name": "업체명", "campaign_name": "캠페인명",
            "imp": "노출수", "clk": "클릭수", "ctr": "클릭률(%)", "cpc": "평균클릭비용",
            "cost": "광고비", "conv": "전환수", "cpa": "전환당비용", "sales": "매출액", "roas": "ROAS(%)"
        })
        
        # 포맷팅
        for c in ["광고비", "평균클릭비용", "전환당비용", "매출액"]: show[c] = show[c].apply(format_currency)
        for c in ["클릭률(%)", "ROAS(%)"]: show[c] = show[c].apply(lambda x: f"{x:.2f}%")
        for c in ["노출수", "클릭수", "전환수"]: show[c] = show[c].apply(lambda x: f"{int(x):,}")
        
        st.dataframe(show, use_container_width=True, hide_index=True)
        render_download_compact(show, "campaign_report")


def page_perf_keyword(meta, engine, f, dim_camp, type_sel):
    st.markdown("## 🔑 성과 (키워드)")
    ids = f["ids"]
    
    # [최적화 적용] load_fact_aggregated 사용 (일별 데이터 X, 합계만 O -> 속도 10배 향상)
    df = load_fact_aggregated(engine, "fact_keyword_daily", f["start"], f["end"], ids)
    df = apply_type_filter_kw_ad(engine, df, dim_camp, type_sel, "keyword")
    
    if df.empty: st.warning("데이터 없음"); return
    
    # 키워드명 가져오기 (필요한 ID만 조회)
    kw_ids = tuple(df["keyword_id"].unique())
    if kw_ids:
        # Tuple handling for SQL IN clause
        if len(kw_ids) == 1: clause = f"= '{kw_ids[0]}'"
        else: clause = f"IN {kw_ids}"
        dim_kw = sql_read(engine, f"SELECT keyword_id, keyword FROM dim_keyword WHERE keyword_id {clause}")
        df = df.merge(dim_kw, on="keyword_id", how="left")
    
    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name"]], on="customer_id")
    
    # [한글 복구] Top 20 리스트
    st.subheader("🏆 키워드 Top 20 (광고비 기준)")
    top = df.sort_values("cost", ascending=False).head(20)
    
    show = top[["account_name", "keyword", "imp", "clk", "ctr", "cost", "conv", "roas"]].copy()
    show = show.rename(columns={
        "account_name": "업체명", "keyword": "키워드",
        "imp": "노출수", "clk": "클릭수", "ctr": "클릭률",
        "cost": "광고비", "conv": "전환수", "roas": "ROAS"
    })
    
    show["광고비"] = show["광고비"].apply(format_currency)
    show["ROAS"] = show["ROAS"].apply(lambda x: f"{x:.0f}%")
    show["클릭률"] = show["클릭률"].apply(lambda x: f"{x:.2f}%")
    show["노출수"] = show["노출수"].apply(lambda x: f"{int(x):,}")
    show["클릭수"] = show["클릭수"].apply(lambda x: f"{int(x):,}")
    
    st.dataframe(show, use_container_width=True, hide_index=True)


def page_perf_ad(meta, engine, f, dim_camp, type_sel):
    st.markdown("## 🖼️ 성과 (소재)")
    ids = f["ids"]
    
    # [최적화 적용] load_fact_aggregated 사용
    df = load_fact_aggregated(engine, "fact_ad_daily", f["start"], f["end"], ids)
    df = apply_type_filter_kw_ad(engine, df, dim_camp, type_sel, "ad")
    
    if df.empty: st.warning("데이터 없음"); return
    
    # 소재명 가져오기
    ad_ids = tuple(df["ad_id"].unique())
    if ad_ids:
        if len(ad_ids) == 1: clause = f"= '{ad_ids[0]}'"
        else: clause = f"IN {ad_ids}"
        # 소재 테이블에 컬럼 있는지 확인
        cols = get_table_columns(engine, "dim_ad")
        name_col = "creative_text" if "creative_text" in cols else "ad_name"
        dim_ad = sql_read(engine, f"SELECT ad_id, {name_col} as ad_name FROM dim_ad WHERE ad_id {clause}")
        df = df.merge(dim_ad, on="ad_id", how="left")

    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name"]], on="customer_id")
    
    st.subheader("🏆 소재 Top 20 (광고비 기준)")
    top = df.sort_values("cost", ascending=False).head(20)
    
    show = top[["account_name", "ad_name", "cost", "roas", "conv", "clk"]].copy()
    show = show.rename(columns={"account_name":"업체명", "ad_name":"소재내용", "cost":"광고비", "roas":"ROAS", "conv":"전환수", "clk":"클릭수"})
    
    show["광고비"] = show["광고비"].apply(format_currency)
    show["ROAS"] = show["ROAS"].apply(lambda x: f"{x:.0f}%")
    
    st.dataframe(show, use_container_width=True, hide_index=True, column_config={"소재내용": st.column_config.TextColumn("소재내용", width="large")})


def page_settings(engine):
    st.markdown("## ⚙️ 설정")
    if st.button("캐시 비우기 (새로고침)"):
        st.cache_data.clear()
        st.success("완료"); st.rerun()

# -----------------------------
# Main
# -----------------------------
def main():
    try: engine = get_engine()
    except Exception as e: st.error(f"DB Error: {e}"); return
    
    meta = get_meta(engine)
    
    dim_camp = pd.DataFrame()
    if table_exists(engine, "dim_campaign"):
        dim_camp = sql_read(engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
        dim_camp["campaign_type_label"] = dim_camp["campaign_tp"].apply(campaign_tp_to_label)

    # Sidebar
    type_opts = get_campaign_type_options(dim_camp)
    
    with st.sidebar.expander("광고유형", expanded=True):
        type_sel = st.multiselect("유형", type_opts)

    f = sidebar_filters(meta)
    
    page = st.sidebar.radio("메뉴", ["예산/잔액", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정"])
    
    if page=="예산/잔액": page_budget(meta, engine, f)
    elif page=="성과(캠페인)": page_perf_campaign(meta, engine, f, dim_camp, type_sel)
    elif page=="성과(키워드)": page_perf_keyword(meta, engine, f, dim_camp, type_sel)
    elif page=="성과(소재)": page_perf_ad(meta, engine, f, dim_camp, type_sel)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
