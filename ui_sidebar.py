import streamlit as st
from datetime import date, timedelta
from state import FilterState
from database import table_exists, sql_read
import pandas as pd

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty: return []
    if not manager_sel and not account_sel: return []
    df = meta.copy()
    if manager_sel and "manager" in df.columns:
        df = df[df["manager"].astype(str).str.strip().isin([str(x).strip() for x in manager_sel])]
    if account_sel and "account_name" in df.columns:
        df = df[df["account_name"].astype(str).str.strip().isin([str(x).strip() for x in account_sel])]
    return sorted(pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64").drop_duplicates().tolist())

def render_sidebar(meta: pd.DataFrame, engine):
    """모든 페이지에서 공통으로 렌더링될 필터 및 메뉴"""
    sv = FilterState.get()
    
    with st.sidebar:
        st.markdown("### 🔍 검색조건")
        
        period_mode = st.selectbox("기간", ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"], 
                                   index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")))
        
        today = date.today()
        if period_mode == "직접 선택":
            d1 = st.date_input("시작일", sv.get("d1", today))
            d2 = st.date_input("종료일", sv.get("d2", today))
        else:
            d2 = today if period_mode in ("오늘", "이번 달") else today - timedelta(days=1)
            if period_mode in ("오늘", "어제"): d1 = d2
            elif period_mode == "최근 7일": d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d1 = date(today.year, today.month, 1)
            else:
                d2 = date(today.year, today.month, 1) - timedelta(days=1)
                d1 = date(d2.year, d2.month, 1)
            st.text_input("시작일", str(d1), disabled=True)
            st.text_input("종료일", str(d2), disabled=True)
            
        managers = sorted(meta["manager"].dropna().unique().tolist()) if not meta.empty and "manager" in meta.columns else []
        manager_sel = st.multiselect("담당자", managers, default=sv.get("manager", []))
        
        accounts_by_mgr = sorted(meta["account_name"].dropna().unique().tolist()) if not meta.empty else []
        if manager_sel and not meta.empty:
            accounts_by_mgr = sorted(meta[meta["manager"].isin(manager_sel)]["account_name"].dropna().unique().tolist())
            
        account_sel = st.multiselect("계정", accounts_by_mgr, default=[a for a in sv.get("account", []) if a in accounts_by_mgr])
        
        cids = resolve_customer_ids(meta, manager_sel, account_sel)
        
        FilterState.update(period_mode=period_mode, d1=d1, d2=d2, manager=manager_sel, account=account_sel, customer_ids=cids, selected_customer_ids=cids)