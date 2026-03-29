# -*- coding: utf-8 -*-
"""view_keyword.py - Keyword & Adgroup performance page view (Rank Delta Toggle & Integer Format Fixed)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import query_keyword_bundle, query_ad_bundle
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta, render_item_comparison_search

FMT_DICT = {
    "노출": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
    "클릭": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
    "CTR(%)": "{:,.2f}%", 
    "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
    "CPC(원)": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
    "전환": "{:,.0f}", "전환 증감": "{:+.1f}%", "전환 차이": "{:+,.0f}",
    "CPA(원)": "{:,.0f}원",
    "전환매출": "{:,.0f}원", "전환매출 증감": "{:+.1f}%", "전환매출 차이": "{:+,.0f}원",
    "ROAS(%)": "{:,.1f}%", "ROAS 증감": "{:+.1f}%",
    "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
}

def _style_delta_numeric(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #1A73E8; font-weight: 700;' if v > 0 else 'color: #EA4335; font-weight: 700;'

def _style_delta_numeric_neg(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #EA4335; font-weight: 700;' if v > 0 else 'color: #1A73E8; font-weight: 700;'

def _apply_delta_styles(styler, df: pd.DataFrame):
    pos_cols = [c for c in ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '전환 증감', '전환 차이', '전환매출 증감', '전환매출 차이', 'ROAS 증감'] if c in df.columns]
    neg_cols = [c for c in ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이', '순위 변화'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.map(_style_delta_numeric_neg, subset=neg_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_cols)
    return styler

def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0: return "미수집"
    return f"{num:.0f}위"

def _filter_shopping_general_ads(df: pd.DataFrame, allow_unknown_type: bool = False) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame() if df is None else df
    work = df.copy()
    campaign_type = work.get("캠페인유형", pd.Series("", index=work.index)).astype(str).str.strip()
    def _is_general_ad(row):
        ctype = str(row.get("캠페인유형", "")).strip()
        if "쇼핑" in ctype or "SHOPPING" in ctype:
            ad_name = str(row.get("키워드", "")).strip()
            if ad_name.startswith("http") or ad_name.endswith(".jpg") or ad_name.endswith(".png"): return False
            if len(ad_name) > 30 and ("할인" in ad_name or "혜택" in ad_name or "리뷰" in ad_name): return False
            return True
        return allow_unknown_type
    if "키워드" in work.columns:
        mask = work.apply(_is_general_ad, axis=1)
        return work[mask].copy()
    return work

def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view

def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
    if 'avg_rank' in base_df.columns: agg_dict['avg_rank'] = 'mean'
        
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales', 'avg_rank': 'b_avg_rank'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else: merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = merged[c].fillna(0)
    if 'b_avg_rank' not in merged.columns: merged['b_avg_rank'] = np.nan

    def _vec_pct_diff(c, b):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        return pct, diff

    c_imp, b_imp = merged.get('노출', 0), merged.get('b_imp', 0)
    c_clk, b_clk = merged.get('클릭', 0), merged.get('b_clk', 0)
    c_cost, b_cost = merged.get('광고비', 0), merged.get('b_cost', 0)
    c_cpc = np.where(c_clk > 0, c_cost / c_clk, 0)
    b_cpc = np.where(b_clk > 0, b_cost / b_clk, 0)
    c_conv, b_conv = merged.get('전환', 0), merged.get('b_conv', 0)
    c_sales, b_sales = merged.get('전환매출', 0), merged.get('b_sales', 0)

    merged['노출 증감'], merged['노출 차이'] = _vec_pct_diff(c_imp, b_imp)
    merged['클릭 증감'], merged['클릭 차이'] = _vec_pct_diff(c_clk, b_clk)
    merged['광고비 증감'], merged['광고비 차이'] = _vec_pct_diff(c_cost, b_cost)
    merged['CPC 증감'], merged['CPC 차이'] = _vec_pct_diff(c_cpc, b_cpc)
    merged['전환 증감'], merged['전환 차이'] = _vec_pct_diff(c_conv, b_conv)
    merged['전환매출 증감'], merged['전환매출 차이'] = _vec_pct_diff(c_sales, b_sales)

    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    b_roas = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)
    merged['ROAS 증감'] = c_roas - b_roas

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns: merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)
        
    return merged


@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_keyword_view(kw_bundle, ad_bundle, meta):
    if (kw_bundle is None or kw_bundle.empty) and (ad_bundle is None or ad_bundle.empty): return pd.DataFrame()
    df_kw = _perf_common_merge_meta(kw_bundle, meta) if not kw_bundle.empty else pd.DataFrame()
    df_ad = _perf_common_merge_meta(ad_bundle, meta) if not ad_bundle.empty else pd.DataFrame()
    view_kw, view_ad = pd.DataFrame(), pd.DataFrame()
    
    if not df_kw.empty: view_kw = df_kw.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"})
    if not df_ad.empty:
        view_ad = df_ad.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"})
        view_ad = _filter_shopping_general_ads(view_ad, allow_unknown_type=True)
        
    if view_kw.empty and view_ad.empty: return pd.DataFrame()
    elif view_kw.empty: view = view_ad.copy()
    elif view_ad.empty: view = view_kw.copy()
    else: view = pd.concat([view_kw, view_ad], ignore_index=True)
    
    view = _add_perf_metrics(view)
    if "avg_rank" in view.columns: view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    # 파워링크의 빈칸(캐시 찌꺼기) 키워드 핀셋 삭제
    if "키워드" in view.columns and "캠페인유형" in view.columns:
        is_powerlink = view["캠페인유형"] == "파워링크"
        is_empty_kw = view["키워드"].isna() | view["키워드"].astype(str).str.strip().isin(["", "nan", "None", "NaN"])
        view = view[~(is_powerlink & is_empty_kw)]

    return view


FAST_KW_CONFIG = {
    "노출": st.column_config.NumberColumn("노출", format="%,d"),
    "클릭": st.column_config.NumberColumn("클릭", format="%,d"),
    "CTR(%)": st.column_config.NumberColumn("CTR(%)", format="%.2f %%"),
    "CPC(원)": st.column_config.NumberColumn("CPC(원)", format="%,d원"),
    "광고비": st.column_config.NumberColumn("광고비", format="%,d원"),
    "전환": st.column_config.NumberColumn("전환", format="%,.1f"),
    "CPA(원)": st.column_config.NumberColumn("CPA(원)", format="%,d원"),
    "전환매출": st.column_config.NumberColumn("전환매출", format="%,d원"),
    "ROAS(%)": st.column_config.NumberColumn("ROAS(%)", format="%.2f %%"),
}

def _render_sticky_table(df, first_col: str, height: int = 550, col_config: dict = None):
    try:
        rows = len(df.index)
        if rows <= 0: calc_height = 100
        elif rows == 1: calc_height = 80
        else: calc_height = min(height, max(100, 40 + (rows * 36)))
    except: calc_height = height
    cfg = col_config.copy() if col_config else {}
    cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    st.dataframe(df, use_container_width=True, height=calc_height, hide_index=True, column_config=cfg)


@st.fragment
def render_keyword_main(view, top_n):
    if view.empty:
        st.info("해당 기간의 키워드/소재 성과 데이터가 없습니다.")
        return

    col1, col2 = st.columns(2)
    camps = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp = col1.selectbox("캠페인 필터", camps, key="kw_camp_filter_main")
    
    filtered_for_grp = view.copy()
    if sel_camp != "전체": filtered_for_grp = filtered_for_grp[filtered_for_grp["캠페인"] == sel_camp]
    
    grps = ["전체"] + sorted([str(x) for x in filtered_for_grp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_for_grp.columns else ["전체"]
    sel_grp = col2.selectbox("광고그룹 필터", grps, key="kw_grp_filter_main")

    col3, col4 = st.columns([3, 1])
    search_kw = col3.text_input("🔍 키워드 검색", key="kw_search_main")
    exact_match_main = col3.checkbox("☑️ 완전 일치", key="kw_exact_main")
    
    col4.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
    agg_kw = col4.checkbox("🎯 동일 키워드 합산 (PC/MO 통합)", key="kw_agg_main", help="캠페인/광고그룹이 달라도 이름이 같은 키워드의 성과를 하나로 합산합니다.")
    
    disp = filtered_for_grp.copy()
    if sel_grp != "전체": disp = disp[disp["광고그룹"] == sel_grp]
    if search_kw:
        if exact_match_main:
            disp = disp[disp["키워드"].astype(str).str.lower() == search_kw.strip().lower()]
        else:
            disp = disp[disp["키워드"].astype(str).str.contains(search_kw, case=False, na=False)]

    base_cols = ["키워드", "캠페인", "광고그룹", "업체명", "담당자", "캠페인유형"]
    if "평균순위" in disp.columns: base_cols.append("평균순위")
    metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]

    # 키워드 합산(Agg) 처리
    if agg_kw:
        agg_dict = {"노출":"sum", "클릭":"sum", "광고비":"sum", "전환":"sum", "전환매출":"sum"}
        grp_cols = ["업체명", "키워드"]
        if "customer_id" in disp.columns: grp_cols.insert(0, "customer_id")
        
        disp = disp.groupby(grp_cols, as_index=False).agg(agg_dict)
        disp = _add_perf_metrics(disp)
        base_cols = ["키워드", "업체명"] # 캠페인, 광고그룹, 순위 등 제외

    final_cols = [c for c in base_cols + metrics_cols if c in disp.columns]
    disp = disp[final_cols].sort_values("광고비", ascending=False).head(top_n)
    
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>키워드/소재 종합 성과 데이터</div>", unsafe_allow_html=True)
    _render_sticky_table(disp, "키워드", height=550, col_config=FAST_KW_CONFIG)


@st.fragment
def render_keyword_cmp(view, engine, cids, type_sel, top_n, start_dt, end_dt):
    st.markdown("<div style='display:flex; justify-content:flex-start; margin-bottom:8px;'>", unsafe_allow_html=True)
    show_deltas = st.toggle("📊 증감율 보기", value=False, key="kw_abs_toggle")
    st.markdown("</div>", unsafe_allow_html=True)

    opts = get_dynamic_cmp_options(start_dt, end_dt)
    cmp_opts = [o for o in opts if o != "비교 안함"]
    cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="kw_cmp_mode")

    if view.empty:
        st.info("현재 기간의 키워드/소재 데이터가 없습니다.")
        return

    col_camp_cmp, col_grp_cmp = st.columns(2)
    camps_cmp = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp_cmp = col_camp_cmp.selectbox("캠페인 필터", camps_cmp, key="kw_camp_filter_cmp")

    filtered_cmp = view.copy()
    if sel_camp_cmp != "전체": filtered_cmp = filtered_cmp[filtered_cmp["캠페인"] == sel_camp_cmp]

    grps_cmp = ["전체"] + sorted([str(x) for x in filtered_cmp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_cmp.columns else ["전체"]
    sel_grp_cmp = col_grp_cmp.selectbox("광고그룹 필터", grps_cmp, key="kw_grp_filter_cmp")

    col_search_cmp, col_agg_cmp = st.columns([3, 1])
    search_kw_cmp = col_search_cmp.text_input("🔍 키워드 검색", key="kw_search_cmp")
    exact_match_cmp = col_search_cmp.checkbox("☑️ 완전 일치", key="kw_exact_cmp")
    
    col_agg_cmp.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
    agg_kw_cmp = col_agg_cmp.checkbox("🎯 동일 키워드 합산", key="kw_agg_cmp", help="PC/MO 등 그룹으로 나뉜 동일 키워드 성과를 하나로 합산합니다.")

    disp = filtered_cmp.copy()
    if sel_grp_cmp != "전체": disp = disp[disp["광고그룹"] == sel_grp_cmp]
    if search_kw_cmp:
        if exact_match_cmp:
            disp = disp[disp["키워드"].astype(str).str.lower() == search_kw_cmp.strip().lower()]
        else:
            disp = disp[disp["키워드"].astype(str).str.contains(search_kw_cmp, case=False, na=False)]

    b1, b2 = period_compare_range(start_dt, end_dt, cmp_mode)
    base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=50000)
    base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=50000, top_k=50)

    base_kw = base_kw_bundle.rename(columns={"keyword": "키워드"}) if not base_kw_bundle.empty else pd.DataFrame()
    base_ad = base_ad_bundle.rename(columns={"ad_name": "키워드"}) if not base_ad_bundle.empty else pd.DataFrame()
    base_bundle = pd.concat([base_kw, base_ad], ignore_index=True)

    if agg_kw_cmp:
        # 1. 대상 기간 합산
        agg_dict = {"노출":"sum", "클릭":"sum", "광고비":"sum", "전환":"sum", "전환매출":"sum"}
        grp_cols = ["업체명", "키워드"]
        if "customer_id" in disp.columns: grp_cols.insert(0, "customer_id")
        disp = disp.groupby(grp_cols, as_index=False).agg(agg_dict)
        disp = _add_perf_metrics(disp)

        # 2. 비교 기간(base) 합산
        if not base_bundle.empty:
            base_grp_cols = ["키워드"]
            if "customer_id" in base_bundle.columns: base_grp_cols.insert(0, "customer_id")
            base_agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
            base_bundle = base_bundle.groupby(base_grp_cols, as_index=False).agg(base_agg_dict)
        
        valid_keys = [k for k in ["customer_id", "키워드"] if k in disp.columns and k in base_bundle.columns]
        base_cols_cmp = ["키워드", "업체명"]
    else:
        valid_keys = [k for k in ["customer_id", "adgroup_id", "키워드"] if k in disp.columns and k in base_bundle.columns]
        base_cols_cmp = ["키워드", "캠페인", "광고그룹", "업체명", "담당자", "캠페인유형"]
        if "평균순위" in disp.columns: base_cols_cmp.append("평균순위")

    if not base_bundle.empty:
        disp_cmp = _apply_comparison_metrics(disp, base_bundle, valid_keys)
    else: 
        disp_cmp = _apply_comparison_metrics(disp, pd.DataFrame(), [])

    metrics_cols_cmp = []
    metrics_cols_cmp.extend(["노출", "노출 증감", "노출 차이"] if show_deltas else ["노출"])
    metrics_cols_cmp.extend(["클릭", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭"])
    metrics_cols_cmp.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
    metrics_cols_cmp.extend(["CPC(원)", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC(원)"])
    metrics_cols_cmp.extend(["전환", "전환 증감", "전환 차이"] if show_deltas else ["전환"])
    metrics_cols_cmp.extend(["CPA(원)"])
    metrics_cols_cmp.extend(["전환매출", "전환매출 증감", "전환매출 차이"] if show_deltas else ["전환매출"])
    metrics_cols_cmp.extend(["ROAS(%)", "ROAS 증감"] if show_deltas else ["ROAS(%)"])

    if not agg_kw_cmp and show_deltas and ("avg_rank" in disp_cmp.columns or "평균순위" in disp_cmp.columns):
        metrics_cols_cmp.append("순위 변화")

    render_item_comparison_search("키워드/소재", disp_cmp, base_bundle, "키워드", start_dt, end_dt, b1, b2)

    final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in disp_cmp.columns]
    disp_final = disp_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()

    styled_cmp = disp_final.style.format(FMT_DICT)
    styled_cmp = _apply_delta_styles(styled_cmp, disp_final)

    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>키워드 기간 비교 표</div>", unsafe_allow_html=True)
    
    st.dataframe(styled_cmp, use_container_width=True, height=550, hide_index=True, column_config={
        "키워드": st.column_config.TextColumn("키워드", pinned=True, width="medium")
    })

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("<div class='nv-sec-title'>키워드/소재(쇼핑) 상세 분석</div>", unsafe_allow_html=True)
    st.caption("파워링크는 키워드 단위, 쇼핑검색은 일반 상품소재 단위 성과를 보여줍니다.")
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_keyword", 300))
    kw_bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)
    ad_bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=50000, top_k=50)
    view = compute_keyword_view(kw_bundle, ad_bundle, meta)
    selected_tab = st.pills("분석 탭 선택", ["종합 성과", "기간 비교"], default="종합 성과")
    if selected_tab == "종합 성과": render_keyword_main(view, top_n)
    elif selected_tab == "기간 비교": render_keyword_cmp(view, engine, cids, type_sel, top_n, f["start"], f["end"])
