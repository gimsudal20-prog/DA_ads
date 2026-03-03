# -*- coding: utf-8 -*-
"""page_helpers.py - Shared UI helpers, filters, and rendering logic for pages."""

from __future__ import annotations

import os
import numpy as np
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from typing import Dict, List

from data import *
from ui import *

from data import pct_change, pct_to_arrow

BUILD_TAG = os.getenv("APP_BUILD", "")
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty: return []
    df = meta.copy()
    if manager_sel and "manager" in df.columns:
        sel = [str(x).strip() for x in manager_sel if str(x).strip()]
        if sel: df = df[df["manager"].astype(str).str.strip().isin(sel)]
    if account_sel and "account_name" in df.columns:
        sel = [str(x).strip() for x in account_sel if str(x).strip()]
        if sel: df = df[df["account_name"].astype(str).str.strip().isin(sel)]
    if "customer_id" not in df.columns: return []
    s = pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64")
    return sorted(s.drop_duplicates().tolist())

def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    try: return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception: return col.multiselect(label, options, default=default, key=key)

def get_dynamic_cmp_options(d1: date, d2: date) -> List[str]:
    delta = (d2 - d1).days + 1
    if delta == 1: return ["비교 안함", "전일대비"]
    elif delta == 7: return ["비교 안함", "전주대비"]
    elif 28 <= delta <= 31: return ["비교 안함", "전월대비"]
    else: return ["비교 안함", "이전 같은 기간 대비"]

def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    today = date.today()
    default_end = today - timedelta(days=1)
    default_start = default_end

    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "", "manager": [], "account": [], "type_sel": [],
            "period_mode": "어제", "d1": default_start, "d2": default_end,
            "top_n_keyword": 300, "top_n_ad": 200, "top_n_campaign": 200, "prefetch_warm": True,
        }
    sv = st.session_state["filters_v8"]

    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    with st.expander("🔍 조회 기간 및 필터 설정 (여기를 열어주세요)", expanded=True):
        st.caption("💡 여기서 선택한 날짜와 계정 기준으로 대시보드의 모든 데이터가 즉시 변경됩니다.")
        
        r1 = st.columns([1.5, 1.5, 1.5, 3], gap="medium")
        period_mode = r1[0].selectbox("📅 기간 선택", ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"], index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")), key="f_period_mode")
        
        if period_mode == "직접 선택":
            d1 = r1[1].date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = r1[2].date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달": d2 = date(today.year, today.month, 1) - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            r1[1].text_input("시작일", str(d1), disabled=True, key="f_d1_ro")
            r1[2].text_input("종료일", str(d2), disabled=True, key="f_d2_ro")

        q = r1[3].text_input("텍스트 검색", sv.get("q", ""), key="f_q", placeholder="찾고 싶은 키워드나 캠페인 이름을 입력하세요")

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        r2 = st.columns([1.5, 2, 1.5], gap="medium")
        manager_sel = ui_multiselect(r2[0], "담당자 필터", managers, default=sv.get("manager", []), key="f_manager", placeholder="모든 담당자")

        accounts_by_mgr = accounts
        if manager_sel:
            try:
                dfm = meta.copy()
                if "manager" in dfm.columns and "account_name" in dfm.columns:
                    dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                    accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
            except Exception: pass

        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
        account_sel = ui_multiselect(r2[1], "광고주(계정) 필터", accounts_by_mgr, default=prev_acc, key="f_account", placeholder="전체 계정 합산보기")
        type_sel = ui_multiselect(r2[2], "광고 유형 필터", type_opts, default=sv.get("type_sel", []), key="f_type_sel", placeholder="모든 광고 보기")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv
    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "ready": True,
    }

def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or meta is None or meta.empty: return df
    out = df.copy()
    out["customer_id"] = pd.to_numeric(out["customer_id"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    meta_copy = meta.copy()
    meta_copy["customer_id"] = pd.to_numeric(meta_copy["customer_id"], errors="coerce").astype("int64")
    return out.merge(meta_copy[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

def append_comparison_data(df_cur: pd.DataFrame, df_prev: pd.DataFrame, join_keys: list) -> pd.DataFrame:
    if df_prev is None or df_prev.empty or df_cur is None or df_cur.empty:
        return df_cur
        
    df_cur_copy = df_cur.copy()
    valid_join_keys = [k for k in join_keys if k in df_cur_copy.columns and k in df_prev.columns]
    if not valid_join_keys: return df_cur_copy
    
    for k in valid_join_keys:
        df_cur_copy[k] = df_cur_copy[k].astype(str)
        df_prev[k] = df_prev[k].astype(str)
        
    val_cols = [c for c in ['cost', 'sales', 'conv', 'clk', 'imp'] if c in df_prev.columns]
    base_tmp = df_prev[valid_join_keys + val_cols].copy()
    
    for c in val_cols:
        base_tmp[c] = pd.to_numeric(base_tmp[c], errors='coerce').fillna(0)
        
    base_tmp = base_tmp.groupby(valid_join_keys, as_index=False).sum()
    base_tmp.rename(columns={'cost':'p_cost', 'sales':'p_sales', 'conv':'p_conv', 'clk':'p_clk', 'imp':'p_imp'}, inplace=True)
    
    out = df_cur_copy.merge(base_tmp, on=valid_join_keys, how='left')
    for c in ['p_cost', 'p_sales', 'p_conv', 'p_clk', 'p_imp']:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors='coerce').fillna(0)
        else: out[c] = 0
        
    cur_cost = pd.to_numeric(out.get("광고비", 0), errors='coerce').fillna(0)
    cur_sales = pd.to_numeric(out.get("전환매출", 0), errors='coerce').fillna(0)
    cur_conv = pd.to_numeric(out.get("전환", 0), errors='coerce').fillna(0)
    cur_roas = pd.to_numeric(out.get("ROAS(%)", 0), errors='coerce').fillna(0)
    
    out["광고비 증감(%)"] = np.where(out["p_cost"] > 0, (cur_cost - out["p_cost"]) / out["p_cost"] * 100, np.where(cur_cost > 0, 100.0, 0.0))
    p_roas = np.where(out["p_cost"] > 0, (out["p_sales"] / out["p_cost"]) * 100, 0.0)
    out["p_roas"] = p_roas  
    
    out["ROAS 증감(%)"] = cur_roas - p_roas
    out["전환 증감"] = cur_conv - out["p_conv"]
    
    def fmt_pct(x):
        if pd.isna(x) or x == 0: return "-"
        return f"▲ {x:.2f}%" if x > 0 else (f"▼ {abs(x):.2f}%" if x < 0 else "-")
    def fmt_diff(x):
        if pd.isna(x) or x == 0: return "-"
        return f"▲ {int(x)}" if x > 0 else (f"▼ {abs(int(x))}" if x < 0 else "-")
        
    out["광고비 증감(%)"] = out["광고비 증감(%)"].apply(fmt_pct)
    out["ROAS 증감(%)"] = out["ROAS 증감(%)"].apply(fmt_pct)
    out["전환 증감"] = out["전환 증감"].apply(fmt_diff)
    
    return out

def render_side_by_side_metrics(row: pd.Series, prev_label: str, cur_label: str, deltas: dict = None):
    if deltas is None: deltas = {}
    c1, c2 = st.columns(2)
    
    def _badge(val_str, invert=False):
        if not val_str or val_str == "-": return ""
        is_up = "▲" in val_str
        if invert:
            color = "#B91C1C" if is_up else "#047857"
            bg = "#FEE2E2" if is_up else "#D1FAE5"
        else:
            color = "#047857" if is_up else "#B91C1C"
            bg = "#D1FAE5" if is_up else "#FEE2E2"
        return f"<span style='color:{color}; background:{bg}; padding:2px 6px; border-radius:4px; font-size:11.5px; font-weight:700; margin-left:8px; vertical-align:middle;'>{val_str}</span>"
    
    def _card(title, imp, clk, cost, conv, sales, roas, is_cur=False, d=None):
        if d is None: d = {}
        bg = "#F8FAFC" if not is_cur else "#EFF6FF"
        border = "#E2E8F0" if not is_cur else "#BFDBFE"
        color_title = "#475569" if not is_cur else "#1E40AF"
        
        f_cost = format_currency(cost)
        f_sales = format_currency(sales)
        f_roas = f"{roas:,.2f}%"
        f_imp = format_number_commas(imp)
        f_clk = format_number_commas(clk)
        f_conv = f"{conv:,.1f}"
        
        b_cost = _badge(d.get('cost'), invert=True) if is_cur else ""
        b_sales = _badge(d.get('sales')) if is_cur else ""
        b_roas = _badge(d.get('roas')) if is_cur else ""
        b_imp = _badge(d.get('imp')) if is_cur else ""
        b_clk = _badge(d.get('clk')) if is_cur else ""
        b_conv = _badge(d.get('conv')) if is_cur else ""
        
        html = f"""
        <div style='background:{bg}; padding:20px; border-radius:12px; border:1px solid {border}; box-shadow: 0 1px 2px rgba(0,0,0,0.05);'>
            <h4 style='text-align:center; margin-top:0; margin-bottom:16px; color:{color_title}; font-size:16px; font-weight:700;'>{title}</h4>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>광고비</span>
                <span><span style='font-weight:700; color:#0F172A;'>{f_cost}</span>{b_cost}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>전환매출</span>
                <span><span style='font-weight:700; color:#0F172A;'>{f_sales}</span>{b_sales}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:12px; padding-bottom:12px; border-bottom:1px dashed #CBD5E1;'>
                <span style='color:#64748B; font-weight:600;'>ROAS</span>
                <span><span style='font-weight:800; color:#EF4444; font-size:15px;'>{f_roas}</span>{b_roas}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>노출수</span>
                <span><span style='color:#334155; font-size:13px; font-weight:600;'>{f_imp}</span>{b_imp}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>클릭수</span>
                <span><span style='color:#334155; font-size:13px; font-weight:600;'>{f_clk}</span>{b_clk}</span>
            </div>
            <div style='display:flex; justify-content:space-between;'>
                <span style='color:#64748B; font-size:13px;'>전환수</span>
                <span><span style='color:#334155; font-size:13px; font-weight:600;'>{f_conv}</span>{b_conv}</span>
            </div>
        </div>
        """
        return html
        
    with c1:
        st.markdown(_card(prev_label, row.get('p_imp',0), row.get('p_clk',0), row.get('p_cost',0), row.get('p_conv',0), row.get('p_sales',0), row.get('p_roas',0)), unsafe_allow_html=True)
    with c2:
        st.markdown(_card(cur_label, row.get('노출',0), row.get('클릭',0), row.get('광고비',0), row.get('전환',0), row.get('전환매출',0), row.get('ROAS(%)',0), True, deltas), unsafe_allow_html=True)

def render_comparison_section(df: pd.DataFrame, cmp_mode: str, b1: date, b2: date, d1: date, d2: date, section_title: str = "선택 항목 상세 비교"):
    st.markdown(f"### 🔍 {section_title} (Side-by-Side)")
    agg_cur = df[['노출', '클릭', '광고비', '전환', '전환매출']].sum()
    agg_prev = df[['p_imp', 'p_clk', 'p_cost', 'p_conv', 'p_sales']].sum() if 'p_cost' in df.columns else None
    
    combined_row = pd.Series({
        '노출': agg_cur.get('노출', 0),
        '클릭': agg_cur.get('클릭', 0),
        '광고비': agg_cur.get('광고비', 0),
        '전환': agg_cur.get('전환', 0),
        '전환매출': agg_cur.get('전환매출', 0),
        'ROAS(%)': (agg_cur.get('전환매출', 0) / agg_cur.get('광고비', 0) * 100) if agg_cur.get('광고비', 0) > 0 else 0,
        'p_imp': agg_prev.get('p_imp', 0) if agg_prev is not None else 0,
        'p_clk': agg_prev.get('p_clk', 0) if agg_prev is not None else 0,
        'p_cost': agg_prev.get('p_cost', 0) if agg_prev is not None else 0,
        'p_conv': agg_prev.get('p_conv', 0) if agg_prev is not None else 0,
        'p_sales': agg_prev.get('p_sales', 0) if agg_prev is not None else 0,
        'p_roas': (agg_prev.get('p_sales', 0) / agg_prev.get('p_cost', 0) * 100) if agg_prev is not None and agg_prev.get('p_cost', 0) > 0 else 0,
    })
    
    deltas = {}
    if agg_prev is not None:
        deltas['cost'] = pct_to_arrow(pct_change(combined_row['광고비'], combined_row['p_cost']))
        deltas['sales'] = pct_to_arrow(pct_change(combined_row['전환매출'], combined_row['p_sales']))
        deltas['imp'] = pct_to_arrow(pct_change(combined_row['노출'], combined_row['p_imp']))
        deltas['clk'] = pct_to_arrow(pct_change(combined_row['클릭'], combined_row['p_clk']))
        
        roas_diff = combined_row['ROAS(%)'] - combined_row['p_roas']
        deltas['roas'] = f"▲ {abs(roas_diff):.2f}%" if roas_diff > 0 else (f"▼ {abs(roas_diff):.2f}%" if roas_diff < 0 else "-")
        
        conv_diff = combined_row['전환'] - combined_row['p_conv']
        deltas['conv'] = f"▲ {abs(conv_diff):.1f}" if conv_diff > 0 else (f"▼ {abs(conv_diff):.1f}" if conv_diff < 0 else "-")
    
    prev_label = f"비교 기간 ({cmp_mode})<br><span style='font-size:13px; font-weight:normal;'>{b1} ~ {b2}</span>"
    cur_label = f"조회 기간 (현재)<br><span style='font-size:13px; font-weight:normal;'>{d1} ~ {d2}</span>"
    
    render_side_by_side_metrics(combined_row, prev_label, cur_label, deltas)
    st.divider()

def _render_ab_test_sbs(df_grp: pd.DataFrame, d1: date, d2: date):
    st.markdown("<div class='nv-sec-title'>📊 소재 A/B 비교 (선택한 그룹 내 상위 2개)</div>", unsafe_allow_html=True)
    st.caption(f"조회 기간: {d1} ~ {d2}")
    
    valid_ads = df_grp.sort_values(by=['노출', '광고비'], ascending=[False, False])
    if len(valid_ads) < 2:
        st.info("해당 그룹에 비교 가능한 소재가 2개 이상 없습니다.")
        st.divider()
        return
        
    ad1, ad2 = valid_ads.iloc[0], valid_ads.iloc[1]
    c1, c2 = st.columns(2)
    
    def _card(row, label):
        return f"""
        <div style='background:#F8FAFC; padding:20px; border-radius:12px; border:2px solid #E2E8F0;'>
            <div style='text-align:center; font-size:13px; font-weight:800; color:#475569; margin-bottom:8px;'>{label}</div>
            <h4 style='text-align:center; margin-top:0; margin-bottom:16px; color:#1E40AF; font-size:15px; font-weight:700;'>{row['소재내용']}</h4>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>광고비</span>
                <span style='font-weight:700; color:#0F172A;'>{format_currency(row.get('광고비',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>전환매출</span>
                <span style='font-weight:700; color:#0F172A;'>{format_currency(row.get('전환매출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:12px; padding-bottom:12px; border-bottom:1px dashed #CBD5E1;'>
                <span style='color:#64748B; font-weight:600;'>ROAS</span>
                <span style='font-weight:800; color:#EF4444; font-size:15px;'>{row.get('ROAS(%)',0):.2f}%</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>노출수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{format_number_commas(row.get('노출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>클릭수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{format_number_commas(row.get('클릭',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between;'>
                <span style='color:#64748B; font-size:13px;'>전환수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{row.get('전환',0):.1f}</span>
            </div>
        </div>
        """
    
    with c1: st.markdown(_card(ad1, "💡 소재 A"), unsafe_allow_html=True)
    with c2: st.markdown(_card(ad2, "💡 소재 B"), unsafe_allow_html=True)
    st.divider()

# ✨ [NEW] 항목별 상세 대조표를 생성해주는 공통 UI 위젯 함수
def render_item_comparison_search(entity_label: str, df_cur: pd.DataFrame, df_base: pd.DataFrame, name_col: str, d1: date, d2: date, b1: date, b2: date):
    import streamlit as st
    import pandas as pd
    
    st.markdown(f"<div style='font-size:16px; font-weight:700; margin-top:24px; margin-bottom:12px;'>🔍 특정 {entity_label} 상세 성과 비교</div>", unsafe_allow_html=True)
    
    items_cur = set(df_cur[name_col].dropna().astype(str).unique()) if not df_cur.empty and name_col in df_cur.columns else set()
    items_base = set(df_base[name_col].dropna().astype(str).unique()) if not df_base.empty and name_col in df_base.columns else set()
    
    all_items = sorted([x for x in list(items_cur | items_base) if str(x).strip() != ''])
    
    if not all_items:
        st.info("검색 가능한 데이터가 없습니다.")
        return
        
    selected = st.selectbox(f"분석할 {entity_label}을(를) 검색 및 선택하세요.", ["- 선택 안함 -"] + all_items, key=f"search_{entity_label}_{name_col}")
    
    if selected != "- 선택 안함 -":
        c_df = df_cur[df_cur[name_col] == selected] if not df_cur.empty else pd.DataFrame()
        b_df = df_base[df_base[name_col] == selected] if not df_base.empty else pd.DataFrame()
        
        def _get(df, c_kr, c_en): 
            if not df.empty:
                if c_kr in df.columns: return float(pd.to_numeric(df[c_kr], errors='coerce').fillna(0).sum())
                if c_en in df.columns: return float(pd.to_numeric(df[c_en], errors='coerce').fillna(0).sum())
            return 0.0
        
        c_cost = _get(c_df, "광고비", "cost")
        c_sales = _get(c_df, "전환매출", "sales")
        c_clk = _get(c_df, "클릭", "clk")
        c_imp = _get(c_df, "노출", "imp")
        c_conv = _get(c_df, "전환", "conv")
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        
        b_cost = _get(b_df, "광고비", "cost")
        b_sales = _get(b_df, "전환매출", "sales")
        b_clk = _get(b_df, "클릭", "clk")
        b_imp = _get(b_df, "노출", "imp")
        b_conv = _get(b_df, "전환", "conv")
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        
        def fmt_krw(v): return f"{int(v):,}원"
        def fmt_num(v): return f"{int(v):,}"
        def fmt_pct(v): return f"{v:.1f}%"
        
        def calc_delta(c, b, reverse=False):
            if b == 0: return "<span style='color:#888;'>비교불가</span>"
            pct = (c - b) / b * 100
            if pct == 0: return "<span style='color:#888;'>변동없음</span>"
            is_good = (pct < 0) if reverse else (pct > 0)
            color = "#FC503D" if not is_good else "#32D74B"
            sign = "▲" if pct > 0 else "▼"
            return f"<span style='color:{color}; font-weight:700;'>{sign} {abs(pct):.1f}%</span>"
            
        html = f"""
        <div style='background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 12px; padding: 20px; margin-top: 12px; margin-bottom: 24px;'>
            <div style='font-size: 15px; font-weight: 800; color: #111; margin-bottom: 16px; border-bottom: 1px solid #ddd; padding-bottom: 8px;'>
                🎯 [{selected}] 성과 대조표
            </div>
            <div style='display: flex; gap: 20px; justify-content: space-between; flex-wrap: wrap;'>
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #375FFF; margin-bottom: 12px;'>🔵 현재 기간 ({d1} ~ {d2})</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='font-weight:600; float:right;'>{fmt_krw(c_cost)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='font-weight:600; float:right;'>{fmt_krw(c_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='font-weight:600; color:#375FFF; float:right;'>{fmt_pct(c_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_conv)}</span>
                    </div>
                </div>
                
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #777; margin-bottom: 12px;'>⚪ 비교 기간 ({b1} ~ {b2})</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='font-weight:600; float:right;'>{fmt_krw(b_cost)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='font-weight:600; float:right;'>{fmt_krw(b_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='font-weight:600; float:right;'>{fmt_pct(b_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_conv)}</span>
                    </div>
                </div>
                
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #111; margin-bottom: 12px;'>📊 증감 (Delta)</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='float:right;'>{calc_delta(c_cost, b_cost, reverse=True)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='float:right;'>{calc_delta(c_sales, b_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='float:right;'>{calc_delta(c_roas, b_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='float:right;'>{calc_delta(c_imp, b_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='float:right;'>{calc_delta(c_clk, b_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='float:right;'>{calc_delta(c_conv, b_conv)}</span>
                    </div>
                </div>
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
