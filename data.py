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
        st.error(f"데이터베이스 조회 중 오류가 발했습니다: {e}")
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
            rename_map = {"업체명": "account_name", "커스텀 ID": "customer_id", "커스텀ID": "customer_id", "커스텀 id": "customer_id", "담당자": "manager"}
            df = df.rename(columns=rename_map)
            
            if table_exists(engine, "dim_customer"):
                try:
                    old_df = sql_read(engine, "SELECT * FROM dim_customer")
                    cid_col = next((c for c in old_df.columns if c in ["customer_id", "커스텀 ID", "커스텀 id", "커스텀ID"]), None)
                    if cid_col and "monthly_budget" in old_df.columns:
                        budget_map = dict(zip(old_df[cid_col], old_df["monthly_budget"]))
                        df["monthly_budget"] = df["customer_id"].map(budget_map).fillna(0)
                except Exception: pass
            
            if "monthly_budget" not in df.columns:
                df["monthly_budget"] = 0
                
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
    df = sql_read(_engine, "SELECT * FROM dim_customer")
    if not df.empty:
        rename_map = {"업체명": "account_name", "커스텀 ID": "customer_id", "커스텀ID": "customer_id", "커스텀 id": "customer_id", "담당자": "manager"}
        df = df.rename(columns=rename_map)
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"): return pd.DataFrame()
    return sql_read(_engine, "SELECT * FROM dim_campaign")

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> list:
    if dim_campaign is None or dim_campaign.empty: return ["파워링크", "쇼핑검색"]
    col_name = "campaign_tp" if "campaign_tp" in dim_campaign.columns else ("campaign_type_label" if "campaign_type_label" in dim_campaign.columns else "campaign_type")
    if col_name not in dim_campaign.columns: return ["파워링크", "쇼핑검색"]
    
    mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
    raw_opts = [str(x) for x in dim_campaign[col_name].dropna().unique() if str(x).strip()]
    opts = list(set([mapping.get(x.upper(), x) for x in raw_opts]))
    return sorted(opts) if opts else ["파워링크", "쇼핑검색"]

def _map_campaign_types(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if not df.empty and col_name in df.columns:
        mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        df[col_name] = df[col_name].apply(lambda x: mapping.get(str(x).upper(), x) if pd.notna(x) else x)
    return df

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
# 4. Data Aggregation Queries (🚀 인덱스 풀가동 최적화 버전)
# ==========================================
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
        latest_dt_df = sql_read(_engine, "SELECT MAX(dt) as latest_dt FROM fact_bizmoney_daily")
        latest_dt = None if latest_dt_df.empty else latest_dt_df.iloc[0].get("latest_dt")
        bizmoney_dt = latest_dt if pd.notna(latest_dt) else yesterday
        df_b = sql_read(
            _engine,
            f"SELECT customer_id, MAX(bizmoney_balance) as bizmoney_balance FROM fact_bizmoney_daily WHERE dt = :d1 {where_cid} GROUP BY customer_id",
            {"d1": str(bizmoney_dt)},
        )
    else:
        df_b = pd.DataFrame(columns=["customer_id", "bizmoney_balance"])
        
    df = meta.copy()
    if cids: df = df[df["customer_id"].isin(cids)]
    
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype(int)
    if not df_avg.empty: df_avg["customer_id"] = pd.to_numeric(df_avg["customer_id"], errors="coerce").fillna(0).astype(int)
    if not df_m.empty: df_m["customer_id"] = pd.to_numeric(df_m["customer_id"], errors="coerce").fillna(0).astype(int)
    if not df_b.empty: df_b["customer_id"] = pd.to_numeric(df_b["customer_id"], errors="coerce").fillna(0).astype(int)
    
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
    try:
        cols = get_table_columns(_engine, "dim_customer")
        if "customer_id" not in cols:
            df = sql_read(_engine, "SELECT * FROM dim_customer")
            rename_map = {"업체명": "account_name", "커스텀 ID": "customer_id", "커스텀ID": "customer_id", "커스텀 id": "customer_id", "담당자": "manager"}
            df = df.rename(columns=rename_map)
            if "monthly_budget" not in df.columns: df["monthly_budget"] = 0
            df.to_sql("dim_customer", _engine, if_exists="replace", index=False)
        else:
            if "monthly_budget" not in cols:
                sql_exec(_engine, "ALTER TABLE dim_customer ADD COLUMN monthly_budget BIGINT DEFAULT 0")
        sql_exec(_engine, "UPDATE dim_customer SET monthly_budget = :val WHERE customer_id = :cid", {"val": val, "cid": cid})
    except Exception as e:
        st.error(f"예산 저장 중 오류 발생: {e}")

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_off_log(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_off_log"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    return sql_read(_engine, f"SELECT * FROM fact_campaign_off_log WHERE dt BETWEEN :d1 AND :d2 {where_cid}", {"d1": str(d1), "d2": str(d2)})

@st.cache_data(ttl=600, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: tuple, type_sel: tuple) -> dict:
    if not table_exists(_engine, f"fact_{entity}_daily"): return {}
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""

    type_join_sql = ""
    type_where_sql = ""
    if type_sel and table_exists(_engine, "dim_campaign"):
        fact_cols = get_table_columns(_engine, f"fact_{entity}_daily")
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if "campaign_id" in fact_cols and cp_col in dim_cols:
            rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENT", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
            db_types = [rev_map.get(t, t) for t in type_sel]
            type_list_str = ",".join([f"'{x}'" for x in db_types])
            type_join_sql = "JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
            type_where_sql = f"AND c.{cp_col} IN ({type_list_str})"

    sql = f"""
        SELECT
            SUM(f.imp) as imp,
            SUM(f.clk) as clk,
            SUM(f.cost) as cost,
            SUM(f.conv) as conv,
            SUM(f.sales) as sales
        FROM fact_{entity}_daily f
        {type_join_sql}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
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
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    cols = get_table_columns(_engine, "dim_campaign")
    cp_col = "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")
    
    type_filter_sql = ""
    if type_sel:
        rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENT", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
        db_types = [rev_map.get(t, t) for t in type_sel]
        type_list_str = ",".join([f"'{x}'" for x in db_types])
        type_filter_sql = f"AND c.{cp_col} IN ({type_list_str})"

    rank_col = None
    camp_fact_cols = get_table_columns(_engine, "fact_campaign_daily")
    for candidate in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"]:
        if candidate in camp_fact_cols:
            rank_col = candidate
            break

    rank_agg_sql = ""
    rank_select_sql = ""
    if rank_col:
        rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank"
        rank_select_sql = ", agg.avg_rank"

    sql = f"""
        WITH agg AS (
            SELECT customer_id, campaign_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
                   SUM(conv) as conv, SUM(sales) as sales{rank_agg_sql}
            FROM fact_campaign_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, campaign_id
        )
        SELECT 
            agg.customer_id, agg.campaign_id, 
            c.campaign_name, c.{cp_col} as campaign_type,
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales{rank_select_sql} 
        FROM agg
        JOIN dim_campaign c ON agg.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    
    if topn_cost > 0: sql += f" ORDER BY agg.cost DESC LIMIT {topn_cost}"
    
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    df = _map_campaign_types(df, 'campaign_type')
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_keyword_bundle(_engine, d1: date, d2: date, cids: list, type_sel: tuple, topn_cost: int=0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(cids)})" if cids else ""
    
    cols = get_table_columns(_engine, "dim_campaign")
    cp_col = "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")
    
    type_filter_sql = ""
    if type_sel:
        rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENT", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
        db_types = [rev_map.get(t, t) for t in type_sel]
        type_list_str = ",".join([f"'{x}'" for x in db_types])
        type_filter_sql = f"AND c.{cp_col} IN ({type_list_str})"

    rank_col = None
    kw_fact_cols = get_table_columns(_engine, "fact_keyword_daily")
    for candidate in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"]:
        if candidate in kw_fact_cols:
            rank_col = candidate
            break

    rank_agg_sql = ""
    rank_select_sql = ""
    if rank_col:
        rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank"
        rank_select_sql = ", agg.avg_rank"

    # 🔥 [핵심] JOIN에 agg.customer_id를 모두 추가하여 인덱스 풀가동 (100배 빨라짐)
    sql = f"""
        WITH agg AS (
            SELECT customer_id, keyword_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
                   SUM(conv) as conv, SUM(sales) as sales{rank_agg_sql}
            FROM fact_keyword_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, keyword_id
        )
        SELECT 
            agg.customer_id, a.campaign_id, k.adgroup_id, agg.keyword_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, k.keyword,
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales{rank_select_sql} 
        FROM agg
        JOIN dim_keyword k ON agg.keyword_id = k.keyword_id AND agg.customer_id = k.customer_id
        JOIN dim_adgroup a ON k.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    
    if topn_cost > 0: sql += f" ORDER BY agg.cost DESC LIMIT {topn_cost}"
        
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    df = _map_campaign_types(df, 'campaign_type_label')
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_ad_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int=0, top_k: int=50) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    cols = get_table_columns(_engine, "dim_campaign")
    cp_col = "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")
    
    ad_cols = get_table_columns(_engine, "dim_ad")
    url_col = "pc_landing_url" if "pc_landing_url" in ad_cols else "landing_url"
    ad_title_col = "ad_title" if "ad_title" in ad_cols else "ad_name"
    image_select_sql = "ad.image_url" if "image_url" in ad_cols else "''"
    
    type_filter_sql = ""
    if type_sel:
        rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENT", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
        db_types = [rev_map.get(t, t) for t in type_sel]
        type_list_str = ",".join([f"'{x}'" for x in db_types])
        type_filter_sql = f"AND c.{cp_col} IN ({type_list_str})"

    rank_col = None
    ad_fact_cols = get_table_columns(_engine, "fact_ad_daily")
    for candidate in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"]:
        if candidate in ad_fact_cols:
            rank_col = candidate
            break

    rank_agg_sql = ""
    rank_select_sql = ""
    if rank_col:
        rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank"
        rank_select_sql = ", agg.avg_rank"

    # 🔥 [핵심] 소재 데이터도 완벽한 인덱스 매칭 및 INNER JOIN 적용
    sql = f"""
        WITH agg AS (
            SELECT customer_id, ad_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, 
                   SUM(conv) as conv, SUM(sales) as sales{rank_agg_sql}
            FROM fact_ad_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, ad_id
        )
        SELECT 
            agg.customer_id, a.campaign_id, ad.adgroup_id, agg.ad_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, ad.ad_name, ad.{ad_title_col} as ad_title, {image_select_sql} as image_url, ad.{url_col} as landing_url,
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales{rank_select_sql} 
        FROM agg
        JOIN dim_ad ad ON agg.ad_id = ad.ad_id AND agg.customer_id = ad.customer_id
        JOIN dim_adgroup a ON ad.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    
    if topn_cost > 0: sql += f" ORDER BY agg.cost DESC LIMIT {topn_cost}"
        
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    df = _map_campaign_types(df, 'campaign_type_label')
    return df

@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"): return pd.DataFrame()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    df = sql_read(_engine, f"SELECT dt, SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost, SUM(conv) as conv, SUM(sales) as sales FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY dt ORDER BY dt", {"d1": str(d1), "d2": str(d2)})
    if not df.empty: df["dt"] = pd.to_datetime(df["dt"])
    return df
