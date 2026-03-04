# -*- coding: utf-8 -*-
"""data.py - Database connection, caching, and common queries."""

import os
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from datetime import date

# ==========================================
# 1. Database Connection
# ==========================================
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url: return create_engine("sqlite:///:memory:", future=True)
    if "sslmode=" not in db_url: db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(db_url, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=1800, future=True)

def db_ping(engine) -> bool:
    try:
        with engine.connect() as conn: conn.execute(text("SELECT 1"))
        return True
    except Exception: return False

def table_exists(engine, table_name: str) -> bool:
    if "_table_names_cache" not in st.session_state:
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
                st.session_state["_table_names_cache"] = [r[0] for r in res]
        except Exception: return False
    return table_name in st.session_state["_table_names_cache"]

@st.cache_data(ttl=3600, show_spinner=False)
def get_table_columns(_engine, table_name: str) -> list:
    try:
        with _engine.connect() as conn:
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND table_schema='public'"))
            return [r[0] for r in res]
    except Exception: return []

@st.cache_data(ttl=600, show_spinner=False)
def sql_read(_engine, query: str, params: dict = None) -> pd.DataFrame:
    try:
        with _engine.connect() as conn: return pd.read_sql(text(query), conn, params=params)
    except Exception as e:
        st.error(f"데이터베이스 조회 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

def sql_exec(_engine, query: str, params: dict = None) -> None:
    try:
        with _engine.begin() as conn: conn.execute(text(query), params or {})
    except Exception as e:
        st.error(f"DB 실행(삭제/수정) 중 오류 발생: {e}")
        raise e

def _sql_in_str_list(lst: list) -> str:
    if not lst: return "''"
    return ",".join(f"'{str(x)}'" for x in lst)

# ==========================================
# 2. Metadata & Dimensions & Seeding
# ==========================================
def seed_from_accounts_xlsx(engine, df=None, file_buffer=None):
    try:
        if df is None and file_buffer is not None: df = pd.read_excel(file_buffer)
        if df is not None:
            df.to_sql("dim_customer", engine, if_exists="replace", index=False)
            if "_table_names_cache" in st.session_state: del st.session_state["_table_names_cache"]
            get_meta.clear()
            return {"meta": len(df)}
        return {"meta": 0}
    except Exception as e:
        st.error(f"데이터 적재 중 오류 발생: {e}")
        return {"meta": 0}

@st.cache_data(ttl=3600, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_customer"): return pd.DataFrame()
    return sql_read(_engine, "SELECT * FROM dim_customer")

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"): return pd.DataFrame()
    return sql_read(_engine, "SELECT * FROM dim_campaign")

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> list:
    if dim_campaign is None or dim_campaign.empty: return ["파워링크", "쇼핑검색"]
    col_name = "campaign_tp" if "campaign_tp" in dim_campaign.columns else ("campaign_type_label" if "campaign_type_label" in dim_campaign.columns else "campaign_type")
    if col_name not in dim_campaign.columns: return ["파워링크", "쇼핑검색"]
    opts = [str(x) for x in dim_campaign[col_name].dropna().unique() if str(x).strip()]
    return opts if opts else ["파워링크", "쇼핑검색"]

@st.cache_data(ttl=600, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    dates = {}
    for tbl in ["fact_campaign_daily", "fact_adgroup_daily", "fact_keyword_daily", "fact_ad_daily"]:
        if table_exists(_engine, tbl):
            df = sql_read(_engine, f"SELECT MAX(dt) as dt FROM {tbl}")
            if not df.empty and pd.notna(df.iloc[0]['dt']): dates[tbl] = df.iloc[0]['dt']
    return dates

# ==========================================
# 3. Helper Functions (Math & Formatting)
# ==========================================
def pct_change(cur: float, base: float) -> float:
    if not base or base == 0: return 100.0 if cur and cur > 0 else 0.0
    return ((cur - base) / base) * 100.0

def pct_to_arrow(val) -> str:
    if val is None or pd.isna(val): return "-"
    if val > 0: return f"▲ {val:.1f}%"
    if val < 0: return f"▼ {abs(val):.1f}%"
    return "-"

def format_currency(val) -> str:
    try: return f"{int(float(val)):,}원"
    except (ValueError, TypeError): return "0원"

def format_number_commas(val) -> str:
    try: return f"{int(float(val)):,}"
    except (ValueError, TypeError): return "0"

# ==========================================
# 4. Data Aggregation Queries
# ==========================================
# ✨ [FIX 1] 누락되었던 예산 및 잔액 관련 함수들 전격 복구
@st.cache_data(ttl=600, show_spinner=False)
def query_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, avg_days: int) -> pd.DataFrame:
    meta = get_meta(_engine)
    if meta.empty: return pd.DataFrame()
    
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql_avg = f"SELECT customer_id, SUM(cost)/{avg_days}.0 as avg_cost FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
    df_avg = sql_read(_engine, sql_avg, {"d1": str(avg_d1), "d2": str(avg_d2)})
    
    sql_m = f"SELECT customer_id, SUM(cost) as current_month_cost, SUM(sales) as current_month_sales FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
    df_m = sql_read(_engine, sql_m, {"d1": str(month_d1), "d2": str(month_d2)})
    
    if table_exists(_engine, "fact_bizmoney_daily"):
        df_b = sql_read(_engine, f"SELECT customer_id, MAX(bizmoney_balance) as bizmoney_balance FROM fact_bizmoney_daily WHERE dt = :d1 {where_cid} GROUP BY customer_id", {"d1": str(yesterday)})
    else:
        df_b = pd.DataFrame(columns=["customer_id", "bizmoney_balance"])
        
    df = meta.copy()
    if cids: df = df[df["customer_id"].isin(cids)]
    if not df_avg.empty: df = df.merge(df_avg, on="customer_id", how="left")
    if not df_m.empty: df = df.merge(df_m, on="customer_id", how="left")
    if not df_b.empty: df = df.merge(df_b, on="customer_id", how="left")
    
    for c in ["avg_cost", "current_month_cost", "current_month_sales", "bizmoney_balance", "monthly_budget"]:
        if c not in df.columns: df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        
    if "manager" not in df.columns: df["manager"] = "담당자 미지정"
    if "account_name" not in df.columns: df["account_name"] = df["customer_id"].astype(str)
    return df

def update_monthly_budget(_engine, cid: int, val: int):
    try: sql_exec(_engine, "UPDATE dim_customer SET monthly_budget = :val WHERE customer_id = :cid", {"val": val, "cid": cid})
    except Exception: pass

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_off_log(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_off_log"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    return sql_read(_engine, f"SELECT * FROM fact_campaign_off_log WHERE dt BETWEEN :d1 AND :d2 {where_cid}", {"d1": str(d1), "d2": str(d2)})


@st.cache_data(ttl=600, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: tuple, type_sel: tuple) -> dict:
    if not table_exists(_engine, f"fact_{entity}_daily"): return {}
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    df = sql_read(_engine, f"SELECT SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, SUM(conv) as conv, SUM(sales) as sales FROM fact_{entity}_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid}", {"d1": str(d1), "d2": str(d2)})
    if df.empty: return {}
    row = df.iloc[0].fillna(0).to_dict()
    row['ctr'] = (row['clk'] / row['imp'] * 100) if row.get('imp', 0) > 0 else 0
    row['cpc'] = (row['cost'] / row['clk']) if row.get('clk', 0) > 0 else 0
    row['roas'] = (row['sales'] / row['cost'] * 100) if row.get('cost', 0) > 0 else 0
    return row

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int=0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame()
    where_cid = f"AND f.customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    # ✨ [FIX 2] campaign_tp 로 DB 스키마 완벽 일치 반영
    sql = f"""
        SELECT 
            f.customer_id, f.campaign_id, 
            MAX(d.campaign_name) as campaign_name, MAX(d.campaign_tp) as campaign_type,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_campaign_daily f
        LEFT JOIN dim_campaign d ON f.campaign_id = d.campaign_id AND f.customer_id = d.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, f.campaign_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type' in df.columns: df = df[df['campaign_type'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_keyword_bundle(_engine, d1: date, d2: date, cids: list, type_sel: tuple, topn_cost: int=0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"): return pd.DataFrame()
    where_cid = f"AND f.customer_id IN ({_sql_in_str_list(cids)})" if cids else ""
    # ✨ [FIX 3] fact_keyword_daily에 없는 f.campaign_id 우회 조인 및 campaign_tp 수정
    sql = f"""
        SELECT 
            f.customer_id, a.campaign_id, f.adgroup_id, f.keyword_id,
            MAX(c.campaign_name) as campaign_name, MAX(c.campaign_tp) as campaign_type_label,
            MAX(a.adgroup_name) as adgroup_name, MAX(k.keyword) as keyword,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_keyword_daily f
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id
        LEFT JOIN dim_campaign c ON a.campaign_id = c.campaign_id
        LEFT JOIN dim_keyword k ON f.keyword_id = k.keyword_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, a.campaign_id, f.adgroup_id, f.keyword_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type_label' in df.columns: df = df[df['campaign_type_label'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_ad_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int=0, top_k: int=50) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"): return pd.DataFrame()
    where_cid = f"AND f.customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    # ✨ [FIX 3] fact_ad_daily에 없는 f.campaign_id 우회 조인 및 campaign_tp 수정
    sql = f"""
        SELECT 
            f.customer_id, a.campaign_id, f.adgroup_id, f.ad_id,
            MAX(c.campaign_name) as campaign_name, MAX(c.campaign_tp) as campaign_type_label,
            MAX(a.adgroup_name) as adgroup_name, MAX(ad.ad_name) as ad_name, MAX(ad.landing_url) as landing_url,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_ad_daily f
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id
        LEFT JOIN dim_campaign c ON a.campaign_id = c.campaign_id
        LEFT JOIN dim_ad ad ON f.ad_id = ad.ad_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, a.campaign_id, f.adgroup_id, f.ad_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type_label' in df.columns: df = df[df['campaign_type_label'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    df = sql_read(_engine, f"SELECT dt, SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, SUM(conv) as conv, SUM(sales) as sales FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY dt ORDER BY dt", {"d1": str(d1), "d2": str(d2)})
    if not df.empty: df["dt"] = pd.to_datetime(df["dt"])
    return df
