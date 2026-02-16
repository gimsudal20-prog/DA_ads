# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.8: 포맷 오류 수정 + 내부 필터 + SQL 그룹핑 최적화)
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
  /* 드롭다운 등 위젯 간격 조정 */
  div[data-testid="stMultiSelect"] { margin-bottom: 10px; }
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
        raise RuntimeError("DATABASE_URL is not set.")
    if "sslmode=" not in db_url:
        joiner = "&" if "?" in db_url else "?"
        db_url = db_url + f"{joiner}sslmode=require"
    return db_url

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(get_database_url(), pool_pre_ping=True, future=True)

# -----------------------------
# Helpers: DB & Data
# -----------------------------
def sql_read(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def sql_exec(engine, sql: str, params: Optional[dict] = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def table_exists(engine, table: str) -> bool:
    try:
        insp = inspect(engine)
        return table in set(insp.get_table_names(schema="public"))
    except: return False

def get_table_columns(engine, table: str) -> set:
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema="public")
        return set([c["name"].lower() for c in cols])
    except: return set()

# -----------------------------
# Optimized Data Loaders (SQL Grouping)
# -----------------------------

# [최적화 1] 차트용 일별 데이터 (가벼움)
@st.cache_data(ttl=600, show_spinner=False)
def get_trend_data(_engine, table: str, d1: date, d2: date, ids: Optional[List[int]]) -> pd.DataFrame:
    if not table_exists(_engine, table): return pd.DataFrame()
    cols = get_table_columns(_engine, table)
    sales_col = "sales" if "sales" in cols else "0"
    
    # DB에서 날짜별로 그룹핑해서 가져옴 (전송량 최소화)
    sql = f"""
        SELECT dt, SUM(cost) as cost, SUM({sales_col}) as sales
        FROM {table}
        WHERE dt BETWEEN :d1 AND :d2
    """
    if ids:
        sql += f" AND customer_id IN ({','.join(map(str, ids))})"
    sql += " GROUP BY dt ORDER BY dt"
    
    return sql_read(_engine, sql, {"d1":str(d1), "d2":str(d2)})

# [최적화 2] 테이블용 합계 데이터 (가장 중요: 키워드/소재 탭 속도 개선의 핵심)
@st.cache_data(ttl=600, show_spinner=False)
def get_aggregated_stats(_engine, table: str, d1: date, d2: date, ids: Optional[List[int]], id_col: str) -> pd.DataFrame:
    if not table_exists(_engine, table): return pd.DataFrame()
    cols = get_table_columns(_engine, table)
    sales_col = "sales" if "sales" in cols else "0"
    
    # DB에서 ID별로 합쳐서 가져옴 (파이썬 연산 제거)
    sql = f"""
        SELECT customer_id, {id_col}, 
               SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
               SUM(conv) as conv, SUM({sales_col}) as sales
        FROM {table}
        WHERE dt BETWEEN :d1 AND :d2
    """
    if ids:
        sql += f" AND customer_id IN ({','.join(map(str, ids))})"
    sql += f" GROUP BY customer_id, {id_col}"
    
    return sql_read(_engine, sql, {"d1":str(d1), "d2":str(d2)})

@st.cache_data(ttl=3600)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_account_meta"): 
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget"])
    df = sql_read(_engine, "SELECT customer_id, account_name, manager, monthly_budget FROM dim_account_meta ORDER BY account_name")
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    return df

@st.cache_data(ttl=300)
def get_latest_bizmoney(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"): 
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])
    sql = "SELECT DISTINCT ON (customer_id) customer_id, bizmoney_balance, dt as last_update FROM fact_bizmoney_daily ORDER BY customer_id, dt DESC"
    df = sql_read(_engine, sql)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0).astype("int64")
    return df

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

# -----------------------------
# Common Helpers
# -----------------------------
def render_live_clock(tz: str = "Asia/Seoul"):
    components.html(f"""<div style='text-align:right; font-size:12px; color:#666;'><span id='clock'></span></div><script>setInterval(()=>document.getElementById('clock').innerText=new Date().toLocaleString('ko-KR',{{timeZone:'{tz}'}}),1000)</script>""", height=30)

def df_to_xlsx_bytes(df, sheet="data"):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name=sheet[:31])
    return out.getvalue()

def render_download_compact(df, fname):
    if df is None or df.empty: return
    c1, c2 = st.columns([1,8])
    with c1: st.download_button("XLSX", df_to_xlsx_bytes(df), f"{fname}.xlsx", key=f"dl_{fname}")
    with c2: st.caption("다운로드")

def format_currency(val):
    try: return f"{int(float(val)):,}원"
    except: return "0원"

def format_roas(val):
    try: return "-" if pd.isna(val) else f"{float(val):.0f}%"
    except: return "-"

def add_rates(g):
    g = g.copy()
    g["ctr"] = (g["clk"] / g["imp"].replace({0: pd.NA})) * 100
    g["cpc"] = g["cost"] / g["clk"].replace({0: pd.NA})
    g["cpa"] = g["cost"] / g["conv"].replace({0: pd.NA})
    if "sales" not in g.columns: g["sales"] = 0
    g["roas"] = (g["sales"] / g["cost"].replace({0: pd.NA})) * 100
    return g

def calculate_delta(curr, prev):
    if prev == 0: return None, "off"
    diff = curr - prev
    return f"{diff:+,.0f}", "normal"

def campaign_tp_to_label(tp):
    map_ = {"web": "파워링크", "shop": "쇼핑검색", "place": "플레이스", "brand": "브랜드", "content": "파워콘텐츠"}
    tp = str(tp).lower()
    for k, v in map_.items():
        if k in tp: return v
    return tp

def get_campaign_type_options(dim):
    if dim.empty: return []
    return sorted(list(set(dim["campaign_type_label"].dropna().unique())))

def apply_type_filter(df, dim_campaign, type_sel):
    if df.empty or not type_sel: return df
    tmp = df.merge(dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]], on=["customer_id", "campaign_id"], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])

def apply_type_filter_kw_ad(_engine, df, dim_campaign, type_sel, level="keyword"):
    if df.empty or not type_sel: return df
    dim_grp = sql_read(_engine, "SELECT customer_id, adgroup_id, campaign_id FROM dim_adgroup")
    if dim_grp.empty: return df
    
    dim_grp = dim_grp.merge(dim_campaign[["customer_id", "campaign_id", "campaign_type_label"]], on=["customer_id", "campaign_id"], how="left")
    
    id_col = "keyword_id" if level == "keyword" else "ad_id"
    table = "dim_keyword" if level == "keyword" else "dim_ad"
    if not table_exists(_engine, table): return df
    
    dim_item = sql_read(_engine, f"SELECT customer_id, {id_col}, adgroup_id FROM {table}")
    dim_item = dim_item.merge(dim_grp[["customer_id", "adgroup_id", "campaign_type_label"]], on=["customer_id", "adgroup_id"], how="left")
    
    tmp = df.merge(dim_item[["customer_id", id_col, "campaign_type_label"]], on=["customer_id", id_col], how="left")
    tmp["campaign_type_label"] = tmp["campaign_type_label"].fillna("기타")
    return tmp[tmp["campaign_type_label"].isin(type_sel)].drop(columns=["campaign_type_label"])

# -----------------------------
# Sidebar
# -----------------------------
def sidebar_filters(meta, type_opts):
    st.sidebar.title("필터")
    with st.sidebar.expander("업체/담당자", expanded=True):
        q = st.text_input("업체명 검색")
        managers = sorted([x for x in meta["manager"].dropna().unique() if x])
        # 한글 Placeholder
        mgr_sel = st.multiselect("담당자", managers, placeholder="담당자 선택")
        
        filtered_meta = meta.copy()
        if q: filtered_meta = filtered_meta[filtered_meta["account_name"].str.contains(q, case=False)]
        if mgr_sel: filtered_meta = filtered_meta[filtered_meta["manager"].isin(mgr_sel)]
        
        cust_opts = sorted(filtered_meta["account_name"].tolist())
        # 한글 Placeholder
        cust_sel = st.multiselect("업체", cust_opts, placeholder="업체 선택 (복수 가능)")
        
        sel_ids = []
        if cust_sel: sel_ids = filtered_meta[filtered_meta["account_name"].isin(cust_sel)]["customer_id"].tolist()
        elif mgr_sel: sel_ids = filtered_meta["customer_id"].tolist()
        
    with st.sidebar.expander("기간", expanded=True):
        # 기본값: 어제 (Index 1) -> 속도 향상
        p = st.selectbox("기간", ["오늘", "어제", "최근 7일", "최근 30일", "직접 선택"], index=1)
        today = date.today()
        if p=="오늘": s=e=today
        elif p=="어제": s=e=today-timedelta(days=1)
        elif "7일" in p: e=today-timedelta(days=1); s=e-timedelta(days=6)
        elif "30일" in p: e=today-timedelta(days=1); s=e-timedelta(days=29)
        else:
            c1,c2=st.columns(2)
            s=c1.date_input("시작"); e=c2.date_input("종료")
    
    # 사이드바에는 글로벌 광고유형 필터 제거 (요청하신 대로 대시보드 내부로 이동)
    return {"start": s, "end": e, "ids": sel_ids}

# -----------------------------
# Pages
# -----------------------------
def page_budget(meta, engine, f):
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_live_clock()
    
    target_ids = f["ids"] if f["ids"] else meta["customer_id"].tolist()
    view = meta[meta["customer_id"].isin(target_ids)].copy()
    
    biz = get_latest_bizmoney(engine)
    yst_cost = get_trend_data(engine, "fact_campaign_daily", date.today()-timedelta(days=1), date.today()-timedelta(days=1), None)
    if not yst_cost.empty:
        # yst_cost는 날짜별 sum이므로 customer_id별 sum을 구하려면 aggregated stats 필요
        # 예산 페이지용 간이 쿼리
        yst_df = sql_read(engine, "SELECT customer_id, SUM(cost) as y_cost FROM fact_campaign_daily WHERE dt = :d GROUP BY customer_id", {"d":str(date.today()-timedelta(days=1))})
    else: yst_df = pd.DataFrame()

    avg_cost = pd.DataFrame()
    if TOPUP_AVG_DAYS > 0:
        d2 = f["end"] - timedelta(days=1); d1 = d2 - timedelta(days=TOPUP_AVG_DAYS-1)
        avg_cost = get_recent_avg_cost(engine, d1, d2)
    
    view = view.merge(biz, on="customer_id", how="left").fillna({"bizmoney_balance":0})
    if not yst_df.empty: 
        yst_df["customer_id"] = pd.to_numeric(yst_df["customer_id"]).astype("int64")
        view = view.merge(yst_df, on="customer_id", how="left").fillna({"y_cost":0})
    else: view["y_cost"] = 0
    if not avg_cost.empty: view = view.merge(avg_cost, on="customer_id", how="left").fillna({"avg_cost":0})
    else: view["avg_cost"] = 0
    
    view["days"] = view.apply(lambda x: x["bizmoney_balance"]/x["avg_cost"] if x["avg_cost"]>0 else 999, axis=1)
    view["status"] = view.apply(lambda x: "🔴 충전필요" if x["bizmoney_balance"] < max(x["avg_cost"]*TOPUP_DAYS_COVER, TOPUP_STATIC_THRESHOLD) else "🟢 여유", axis=1)
    
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("총 비즈머니", format_currency(view["bizmoney_balance"].sum()))
    month_cost = get_monthly_cost(engine, f["end"])
    view = view.merge(month_cost, on="customer_id", how="left").fillna({"current_month_cost":0})
    c2.metric(f"{f['end'].month}월 사용액", format_currency(view["current_month_cost"].sum()))
    c3.metric("충전필요", f'{sum(view["status"].str.contains("충전"))}건')
    
    st.markdown("### 💳 잔액 현황")
    if st.checkbox("충전필요만 보기"): view = view[view["status"].str.contains("충전")]
    
    show = view[["account_name", "manager", "bizmoney_balance", "avg_cost", "days", "y_cost", "status"]].copy()
    show["days"] = show["days"].apply(lambda x: f"{x:.1f}일" if x<100 else "99+일")
    for c in ["bizmoney_balance", "avg_cost", "y_cost"]: show[c] = show[c].apply(format_currency)
    
    def style_row(row): return ["background-color: #fee2e2"]*len(row) if "충전" in str(row["status"]) else [""]*len(row)
    st.dataframe(show.style.apply(style_row, axis=1), use_container_width=True, hide_index=True,
                 column_config={"account_name":"업체명", "bizmoney_balance":"비즈머니", "avg_cost":"평균소진", "days":"소진가능", "y_cost":"전일소진"})

    st.divider()
    st.markdown(f"### 📅 월 예산 관리 ({f['end'].month}월)")
    
    # [수정 1: 포맷 에러 해결] 정수형으로 강제 변환 및 포맷 문자열 수정
    view["monthly_budget"] = view["monthly_budget"].fillna(0).astype(int)
    view["current_month_cost"] = view["current_month_cost"].fillna(0).astype(int)
    view["usage_pct"] = (view["current_month_cost"] / view["monthly_budget"].replace(0, 1) * 100)
    
    # 상태 아이콘
    def get_status(rate, budget):
        if budget == 0: return "⚪ 미설정"
        if rate >= 100: return "🔴 초과"
        if rate >= 90: return "🟡 주의"
        return "🟢 적정"
    view["status_icon"] = view.apply(lambda r: get_status(r["usage_pct"], r["monthly_budget"]), axis=1)

    c1, c2 = st.columns([3, 1])
    with c1:
        # [수정 1] format="%.0f" 또는 "%d"를 사용하여 에러 방지
        edited = st.data_editor(
            view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost", "usage_pct", "status_icon"]],
            use_container_width=True, hide_index=True,
            column_config={
                "customer_id": st.column_config.NumberColumn("CID", disabled=True),
                "account_name": st.column_config.TextColumn("업체명", disabled=True),
                "monthly_budget": st.column_config.NumberColumn("월 예산", format="%d", min_value=0, step=10000), # 콤마 제거된 정수 포맷
                "current_month_cost": st.column_config.NumberColumn(f"{f['end'].month}월 사용액", format="%d", disabled=True),
                "usage_pct": st.column_config.NumberColumn("집행률", format="%.1f%%", disabled=True),
                "status_icon": st.column_config.TextColumn("상태", disabled=True)
            }
        )
    with c2:
        if st.button("💾 예산 저장", type="primary", use_container_width=True):
            with engine.begin() as conn:
                for _, r in edited.iterrows():
                    cid, val = int(r["customer_id"]), int(r["monthly_budget"])
                    conn.execute(text("UPDATE dim_account_meta SET monthly_budget=:b, updated_at=now() WHERE customer_id=:c"), {"b":val, "c":cid})
            st.cache_data.clear()
            st.success("저장 완료")
            st.rerun()


def page_perf_campaign(meta, engine, f, dim_camp, type_opts):
    st.markdown("## 🚀 성과 (캠페인)")
    ids = f["ids"]
    
    # [수정 2: 대시보드 내부 필터]
    c1, c2 = st.columns([1, 3])
    with c1:
        type_sel = st.multiselect("광고유형 필터", type_opts, placeholder="유형 선택 (전체 보기)")
    
    # [최적화] 차트용 데이터는 일별 로딩
    trend_df = get_trend_data(engine, "fact_campaign_daily", f["start"], f["end"], ids)
    
    # [최적화] 테이블용 데이터는 합계 로딩 (매우 빠름)
    agg_df = get_aggregated_stats(engine, "fact_campaign_daily", f["start"], f["end"], ids, "campaign_id")
    agg_df = apply_type_filter(agg_df, dim_camp, type_sel)
    
    if agg_df.empty: st.warning("데이터 없음"); return
    
    # Metrics
    sums = agg_df[["cost","sales","conv","imp","clk"]].sum()
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("광고비", format_currency(sums["cost"]))
    c2.metric("매출", format_currency(sums["sales"]))
    c3.metric("ROAS", format_roas(sums["sales"]/sums["cost"]*100 if sums["cost"] else 0))
    c4.metric("전환수", f"{int(sums['conv']):,}")
    c5.metric("클릭수", f"{int(sums['clk']):,}")
    
    st.divider()
    
    t1, t2 = st.tabs(["차트", "상세 테이블"])
    with t1:
        if not trend_df.empty:
            trend_df["roas"] = trend_df.apply(lambda r: r["sales"]/r["cost"]*100 if r["cost"]>0 else 0, axis=1)
            base = alt.Chart(trend_df).encode(x="dt:T")
            c = base.mark_bar().encode(y="cost") + base.mark_line(color='red').encode(y="roas")
            st.altair_chart(c.resolve_scale(y='independent'), use_container_width=True)
        else: st.info("차트 데이터 없음")
        
    with t2:
        df = add_rates(agg_df)
        df = df.merge(meta[["customer_id","account_name"]], on="customer_id").merge(dim_camp, on=["customer_id","campaign_id"], how="left")
        
        show = df[["account_name", "campaign_name", "imp", "clk", "ctr", "cpc", "cost", "conv", "cpa", "sales", "roas"]].copy()
        show.rename(columns={"account_name":"업체명", "campaign_name":"캠페인", "imp":"노출", "clk":"클릭", "ctr":"CTR", "cpc":"CPC", "cost":"광고비", "conv":"전환", "cpa":"CPA", "sales":"매출", "roas":"ROAS"}, inplace=True)
        
        # 포맷팅 (String 변환)
        show["광고비"] = show["광고비"].apply(format_currency)
        show["매출"] = show["매출"].apply(format_currency)
        show["CPC"] = show["CPC"].apply(format_currency)
        show["CPA"] = show["CPA"].apply(format_currency)
        show["ROAS"] = show["ROAS"].apply(lambda x: f"{x:.0f}%")
        show["CTR"] = show["CTR"].apply(lambda x: f"{x:.2f}%")
        for c in ["노출", "클릭", "전환"]: show[c] = show[c].apply(lambda x: f"{int(x):,}")
        
        st.dataframe(show, use_container_width=True, hide_index=True)
        render_download_compact(show, "campaign_report")


def page_perf_keyword(meta, engine, f, dim_camp, type_opts):
    st.markdown("## 🔑 성과 (키워드)")
    ids = f["ids"]
    
    # [수정 2: 대시보드 내부 필터]
    c1, c2 = st.columns([1, 3])
    with c1:
        type_sel = st.multiselect("광고유형 필터", type_opts, placeholder="유형 선택")

    # [최적화 3] load_aggregated_stats 사용 -> 속도 개선의 핵심
    df = get_aggregated_stats(engine, "fact_keyword_daily", f["start"], f["end"], ids, "keyword_id")
    df = apply_type_filter_kw_ad(engine, df, dim_camp, type_sel, "keyword")
    
    if df.empty: st.warning("데이터 없음"); return
    
    # 이름 매핑 (필요한 ID만 조회)
    kw_ids = tuple(df["keyword_id"].unique())
    if kw_ids:
        if len(kw_ids)==1: clause = f"= '{kw_ids[0]}'"
        else: clause = f"IN {kw_ids}"
        dim_kw = sql_read(engine, f"SELECT keyword_id, keyword FROM dim_keyword WHERE keyword_id {clause}")
        df = df.merge(dim_kw, on="keyword_id", how="left")
    
    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name"]], on="customer_id")
    
    st.subheader("🏆 키워드 Top 20 (광고비 기준)")
    top = df.sort_values("cost", ascending=False).head(20)
    
    show = top[["account_name", "keyword", "imp", "clk", "ctr", "cost", "conv", "roas"]].copy()
    show.rename(columns={"account_name":"업체명", "keyword":"키워드", "imp":"노출", "clk":"클릭", "ctr":"CTR", "cost":"광고비", "conv":"전환", "roas":"ROAS"}, inplace=True)
    
    show["광고비"] = show["광고비"].apply(format_currency)
    show["ROAS"] = show["ROAS"].apply(lambda x: f"{x:.0f}%")
    show["CTR"] = show["CTR"].apply(lambda x: f"{x:.2f}%")
    show["노출"] = show["노출"].apply(lambda x: f"{int(x):,}")
    show["클릭"] = show["클릭"].apply(lambda x: f"{int(x):,}")
    
    st.dataframe(show, use_container_width=True, hide_index=True)


def page_perf_ad(meta, engine, f, dim_camp, type_opts):
    st.markdown("## 🖼️ 성과 (소재)")
    ids = f["ids"]
    
    # [수정 2: 대시보드 내부 필터]
    c1, c2 = st.columns([1, 3])
    with c1:
        type_sel = st.multiselect("광고유형 필터", type_opts, placeholder="유형 선택")

    # [최적화 3] load_aggregated_stats 사용
    df = get_aggregated_stats(engine, "fact_ad_daily", f["start"], f["end"], ids, "ad_id")
    df = apply_type_filter_kw_ad(engine, df, dim_camp, type_sel, "ad")
    
    if df.empty: st.warning("데이터 없음"); return
    
    ad_ids = tuple(df["ad_id"].unique())
    if ad_ids:
        if len(ad_ids)==1: clause = f"= '{ad_ids[0]}'"
        else: clause = f"IN {ad_ids}"
        cols = get_table_columns(engine, "dim_ad")
        name_col = "creative_text" if "creative_text" in cols else "ad_name"
        dim_ad = sql_read(engine, f"SELECT ad_id, {name_col} as ad_name FROM dim_ad WHERE ad_id {clause}")
        df = df.merge(dim_ad, on="ad_id", how="left")

    df = add_rates(df)
    df = df.merge(meta[["customer_id","account_name"]], on="customer_id")
    
    st.subheader("🏆 소재 Top 20 (광고비 기준)")
    top = df.sort_values("cost", ascending=False).head(20)
    
    show = top[["account_name", "ad_name", "cost", "roas", "conv", "clk"]].copy()
    show.rename(columns={"account_name":"업체명", "ad_name":"소재내용", "cost":"광고비", "roas":"ROAS", "conv":"전환", "clk":"클릭"}, inplace=True)
    
    show["광고비"] = show["광고비"].apply(format_currency)
    show["ROAS"] = show["ROAS"].apply(lambda x: f"{x:.0f}%")
    
    st.dataframe(show, use_container_width=True, hide_index=True, column_config={"소재내용": st.column_config.TextColumn("소재내용", width="large")})


def page_settings(engine):
    st.markdown("## ⚙️ 설정")
    if st.button("캐시 비우기 (데이터 새로고침)"):
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

    type_opts = get_campaign_type_options(dim_camp)
    f = sidebar_filters(meta, type_opts)
    
    page = st.sidebar.radio("메뉴", ["예산/잔액", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정"])
    
    if page=="예산/잔액": page_budget(meta, engine, f)
    elif page=="성과(캠페인)": page_perf_campaign(meta, engine, f, dim_camp, type_opts)
    elif page=="성과(키워드)": page_perf_keyword(meta, engine, f, dim_camp, type_opts)
    elif page=="성과(소재)": page_perf_ad(meta, engine, f, dim_camp, type_opts)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
