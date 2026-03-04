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
    if not db_url: 
        return create_engine("sqlite:///:memory:", future=True)
        
    if "sslmode=" not in db_url: 
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
        
    return create_engine(
        db_url, 
        pool_size=10, 
        max_overflow=20, 
        pool_pre_ping=True,  
        pool_recycle=1800,   
        future=True
    )

def db_ping(engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

def table_exists(engine, table_name: str) -> bool:
    if "_table_names_cache" not in st.session_state:
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
                st.session_state["_table_names_cache"] = [r[0] for r in res]
        except Exception:
            return False
    return table_name in st.session_state["_table_names_cache"]

@st.cache_data(ttl=3600, show_spinner=False)
def get_table_columns(_engine, table_name: str) -> list:
    try:
        with _engine.connect() as conn:
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND table_schema='public'"))
            return [r[0] for r in res]
    except Exception:
        return []

@st.cache_data(ttl=600, show_spinner=False)
def sql_read(_engine, query: str, params: dict = None) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)
    except Exception as e:
        st.error(f"데이터베이스 조회 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

def sql_exec(_engine, query: str, params: dict = None) -> None:
    try:
        with _engine.begin() as conn:
            conn.execute(text(query), params or {})
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
        if df is None and file_buffer is not None:
            df = pd.read_excel(file_buffer)
            
        if df is not None:
            df.to_sql("dim_customer", engine, if_exists="replace", index=False)
            
            if "_table_names_cache" in st.session_state:
                del st.session_state["_table_names_cache"]
            get_meta.clear()
            return {"meta": len(df)}
        return {"meta": 0}
    except Exception as e:
        st.error(f"데이터 적재 중 오류 발생: {e}")
        return {"meta": 0}

@st.cache_data(ttl=3600, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_customer"): 
        return pd.DataFrame()
    return sql_read(_engine, "SELECT * FROM dim_customer")

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"): 
        return pd.DataFrame()
    return sql_read(_engine, "SELECT * FROM dim_campaign")

# ✨ [FIX] 올바른 컬럼명(campaign_type)을 우선적으로 찾도록 수정
def get_campaign_type_options(dim_campaign: pd.DataFrame) -> list:
    if dim_campaign is None or dim_campaign.empty:
        return ["파워링크", "쇼핑검색", "파워컨텐츠", "브랜드검색", "플레이스"]
    
    col_name = "campaign_type"
    if "campaign_type_label" in dim_campaign.columns:
        col_name = "campaign_type_label"
    elif "campaign_type" not in dim_campaign.columns:
        return ["파워링크", "쇼핑검색"]
        
    opts = [str(x) for x in dim_campaign[col_name].dropna().unique() if str(x).strip()]
    return opts if opts else ["파워링크", "쇼핑검색"]

@st.cache_data(ttl=600, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    dates = {}
    tables = ["fact_campaign_daily", "fact_adgroup_daily", "fact_keyword_daily", "fact_ad_daily"]
    for tbl in tables:
        if table_exists(_engine, tbl):
            df = sql_read(_engine, f"SELECT MAX(dt) as dt FROM {tbl}")
            if not df.empty and pd.notna(df.iloc[0]['dt']):
                dates[tbl] = df.iloc[0]['dt']
    return dates

# ==========================================
# 3. Helper Functions (Math & Formatting)
# ==========================================
def pct_change(cur: float, base: float) -> float:
    if not base or base == 0: 
        return 100.0 if cur and cur > 0 else 0.0
    return ((cur - base) / base) * 100.0

def pct_to_arrow(val) -> str:
    if val is None or pd.isna(val): return "-"
    if val > 0: return f"▲ {val:.1f}%"
    if val < 0: return f"▼ {abs(val):.1f}%"
    return "-"

def format_currency(val) -> str:
    try: 
        return f"{int(float(val)):,}원"
    except (ValueError, TypeError): 
        return "0원"

def format_number_commas(val) -> str:
    try: 
        return f"{int(float(val)):,}"
    except (ValueError, TypeError): 
        return "0"

# ==========================================
# 4. Data Aggregation Queries
# ==========================================
@st.cache_data(ttl=600, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: tuple, type_sel: tuple) -> dict:
    if not table_exists(_engine, f"fact_{entity}_daily"): return {}
    
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql = f"""
        SELECT 
            SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
            SUM(conv) as conv, SUM(sales) as sales 
        FROM fact_{entity}_daily 
        WHERE dt BETWEEN :d1 AND :d2 {where_cid}
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
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
    
    # ✨ [FIX] d.campaign_type_label -> d.campaign_type 로 컬럼명 오류 수정
    sql = f"""
        SELECT 
            f.customer_id, f.campaign_id, 
            MAX(d.campaign_name) as campaign_name, MAX(d.campaign_type) as campaign_type,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_campaign_daily f
        LEFT JOIN dim_campaign d ON f.campaign_id = d.campaign_id AND f.customer_id = d.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, f.campaign_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type' in df.columns:
        df = df[df['campaign_type'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_keyword_bundle(_engine, d1: date, d2: date, cids: list, type_sel: tuple, topn_cost: int=0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"): return pd.DataFrame()
    where_cid = f"AND f.customer_id IN ({_sql_in_str_list(cids)})" if cids else ""
    
    # ✨ [FIX] c.campaign_type_label -> c.campaign_type 로 수정
    sql = f"""
        SELECT 
            f.customer_id, f.campaign_id, f.adgroup_id, f.keyword_id,
            MAX(c.campaign_name) as campaign_name, MAX(c.campaign_type) as campaign_type,
            MAX(a.adgroup_name) as adgroup_name, MAX(k.keyword) as keyword,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_keyword_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id
        LEFT JOIN dim_keyword k ON f.keyword_id = k.keyword_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, f.campaign_id, f.adgroup_id, f.keyword_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type' in df.columns:
        df = df[df['campaign_type'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_ad_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int=0, top_k: int=50) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"): return pd.DataFrame()
    where_cid = f"AND f.customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    # ✨ [FIX] c.campaign_type_label -> c.campaign_type 로 수정
    sql = f"""
        SELECT 
            f.customer_id, f.campaign_id, f.adgroup_id, f.ad_id,
            MAX(c.campaign_name) as campaign_name, MAX(c.campaign_type) as campaign_type,
            MAX(g.adgroup_name) as adgroup_name, MAX(a.ad_name) as ad_name, MAX(a.landing_url) as landing_url,
            SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, 
            SUM(f.conv) as conv, SUM(f.sales) as sales 
        FROM fact_ad_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id
        LEFT JOIN dim_adgroup g ON f.adgroup_id = g.adgroup_id
        LEFT JOIN dim_ad a ON f.ad_id = a.ad_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, f.campaign_id, f.adgroup_id, f.ad_id
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if type_sel and not df.empty and 'campaign_type' in df.columns:
        df = df[df['campaign_type'].isin(type_sel)]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql = f"""
        SELECT 
            dt, SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
            SUM(conv) as conv, SUM(sales) as sales 
        FROM fact_campaign_daily 
        WHERE dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY dt ORDER BY dt
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if not df.empty:
        df["dt"] = pd.to_datetime(df["dt"])
    return df
