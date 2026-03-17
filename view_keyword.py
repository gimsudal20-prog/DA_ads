# -*- coding: utf-8 -*-
"""view_keyword.py - Keyword & Adgroup performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import query_keyword_bundle, query_ad_bundle
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas


def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"


def _filter_shopping_general_ads(df: pd.DataFrame, allow_unknown_type: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    work = df.copy()
    campaign_type = work.get("캠페인유형", pd.Series("", index=work.index)).astype(str).str.strip()
    
    def _is_general_ad(row):
        ctype = str(row.get("캠페인유형", "")).strip()
        if "쇼핑" in ctype or "SHOPPING" in ctype:
            ad_name = str(row.get("키워드/상품명", "")).strip()
            
            if ad_name.startswith("http") or ad_name.endswith((".jpg", ".png", ".jpeg", ".gif")):
                return False
                
            ext_keywords = ["추가홍보문구", "홍보문구", "확장소재", "서브링크", "가격링크", "파워링크이미지", "추가제목", "플레이스정보"]
            if any(ext in ad_name for ext in ext_keywords):
                return False
                
            return True
        return allow_unknown_type

    if "키워드/상품명" in work.columns:
        mask = work.apply(_is_general_ad, axis=1)
        return work[mask].copy()
        
    return work


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view


def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    
    for k in merge_keys:
        if k in view_df.columns:
            view_df[k] = view_df[k].astype(str)
        if k in base_df.columns:
            base_df[k] = base_df[k].astype(str)
            
    for c in ['imp', 'clk', 'cost', 'conv', 'sales']:
        if c in base_df.columns:
            base_df[c] = pd.to_numeric(base_df[c], errors='coerce').fillna(0)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
    if 'avg_rank' in base_df.columns:
        agg_dict['avg_rank'] = 'mean'
        base_df['avg_rank'] = pd.to_numeric(base_df['avg_rank'], errors='coerce')
        
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg({k:v for k,v in agg_dict.items() if k in base_df.columns}).reset_index()
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales', 'avg_rank': 'b_avg_rank'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = pd.to_numeric(merged[c], errors='coerce').fillna(0)
        
    if 'b_avg_rank' not in merged.columns: merged['b_avg_rank'] = np.nan

    merged['이전 노출'] = merged['b_imp']
    merged['노출 증감'] = merged['노출'] - merged['이전 노출']
    merged['노출 증감(%)'] = np.where(merged['이전 노출'] > 0, (merged['노출 증감'] / merged['이전 노출']) * 100, np.where(merged['노출'] > 0, 100.0, 0.0))

    merged['이전 클릭'] = merged['b_clk']
    merged['클릭 증감'] = merged['클릭'] - merged['이전 클릭']
    merged['클릭 증감(%)'] = np.where(merged['이전 클릭'] > 0, (merged['클릭 증감'] / merged['이전 클릭']) * 100, np.where(merged['클릭'] > 0, 100.0, 0.0))

    merged['이전 광고비'] = merged['b_cost']
    merged['광고비 증감'] = merged['광고비'] - merged['이전 광고비']
    merged['광고비 증감(%)'] = np.where(merged['이전 광고비'] > 0, (merged['광고비 증감'] / merged['이전 광고비']) * 100, np.where(merged['광고비'] > 0, 100.0, 0.0))

    merged['이전 CPC(원)'] = np.where(merged['이전 클릭'] > 0, merged['이전 광고비'] / merged['이전 클릭'], 0.0)
    merged['CPC 증감'] = merged['CPC(원)'] - merged['이전 CPC(원)']
    merged['CPC 증감(%)'] = np.where(merged['이전 CPC(원)'] > 0, (merged['CPC 증감'] / merged['이전 CPC(원)']) * 100, np.where(merged['CPC(원)'] > 0, 100.0, 0.0))

    merged['이전 전환'] = merged['b_conv']
    merged['전환 증감'] = merged['전환'] - merged['이전 전환']
    
    merged['이전 전환매출'] = merged['b_sales']
    merged['이전 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 전환매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['ROAS 증감(%)'] = merged['ROAS(%)'] - merged['이전 ROAS(%)']

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns:
            merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['이전 평균순위'] = merged['b_avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)
        
    return merged


@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_keyword_view(kw_bundle, ad_bundle, meta):
    if (kw_bundle is None or kw_bundle.empty) and (ad_bundle is None or ad_bundle.empty):
        return pd.DataFrame()
        
    df_kw = _perf_common_merge_meta(kw_bundle, meta) if not kw_bundle.empty else pd.DataFrame()
    df_ad = _perf_common_merge_meta(ad_bundle, meta) if not ad_bundle.empty else pd.DataFrame()
    
    view_kw = pd.DataFrame()
    view_ad = pd.DataFrame()
    
    if not df_kw.empty:
        view_kw = df_kw.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형",
            "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드/상품명",
            "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
        })
    
    if not df_ad.empty:
        if "ad_title" in df_ad.columns:
            df_ad["final_ad_name"] = df_ad["ad_title"].fillna("").astype(str).str.strip()
            mask_empty = df_ad["final_ad_name"].isin(["", "nan", "None"])
            df_ad.loc[mask_empty, "final_ad_name"] = df_ad.loc[mask_empty, "ad_name"].astype(str)
        else:
            df_ad["final_ad_name"] = df_ad["ad_name"].astype(str)

        view_ad = df_ad.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형",
            "campaign_name": "캠페인", "adgroup_name": "광고그룹", "final_ad_name": "키워드/상품명",
            "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
        })
        view_ad = _filter_shopping_general_ads(view_ad, allow_unknown_type=True)
        
    if view_kw.empty and view_ad.empty:
        return pd.DataFrame()
    elif view_kw.empty:
        view = view_ad.copy()
    elif view_ad.empty:
        view = view_kw.copy()
    else:
        view = pd.concat([view_kw, view_ad], ignore_index=True)
        
    view = _add_perf_metrics(view)
    if "avg_rank" in view.columns:
        view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)
        
    return view


@st.fragment
def render_keyword_main(view, top_n, fmt):
    if view.empty:
        st.info("해당 기간의 키워드/소재 성과 데이터가 없습니다.")
        return

    col_camp, col_grp = st.columns(2)
    camps = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp = col_camp.selectbox("캠페인 필터", camps, key="kw_camp_filter_main")

    filtered_for_grp = view.copy()
    if sel_camp != "전체":
        filtered_for_grp = filtered_for_grp[filtered_for_grp["캠페인"] == sel_camp]

    grps = ["전체"] + sorted([str(x) for x in filtered_for_grp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_for_grp.columns else ["전체"]
    sel_grp = col_grp.selectbox("광고그룹 필터", grps, key="kw_grp_filter_main")

    disp = filtered_for_grp.copy()
    if sel_grp != "전체":
        disp = disp[disp["광고그룹"] == sel_grp]

    base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드/상품명"]
    if "평균순위" in disp.columns:
        base_cols.append("평균순위")
    metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
    final_cols = [c for c in base_cols + metrics_cols if c in disp.columns]

    disp = disp[final_cols].sort_values("광고비", ascending=False).head(top_n)

    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>키워드/소재 종합 성과 데이터</div>", unsafe_allow_html=True)
    st.dataframe(
        disp.style.format(fmt), 
        use_container_width=True, 
        height=550, 
        hide_index=True 
    )


@st.fragment
def render_keyword_cmp(view, engine, cids, type_sel, top_n, fmt_cmp, start_dt, end_dt):
    opts = get_dynamic_cmp_options(start_dt, end_dt)
    cmp_opts = [o for o in opts if o != "비교 안함"]
    cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="kw_cmp_mode")

    b1, b2 = period_compare_range(start_dt, end_dt, cmp_mode)
    
    # ✨ 동그라미 로딩(spinner) 추가 (비교 기간)
    with st.spinner("🔄 비교 기간의 데이터를 불러오는 중입니다..."):
        base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=50000)
        base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=50000, top_k=50)

    if view.empty:
        st.info("현재 기간의 키워드/소재 데이터가 없습니다.")
        return

    base_kw = base_kw_bundle.rename(columns={"keyword": "키워드/상품명"}) if not base_kw_bundle.empty else pd.DataFrame()
    
    if not base_ad_bundle.empty:
        if "ad_title" in base_ad_bundle.columns:
            base_ad_bundle["final_ad_name"] = base_ad_bundle["ad_title"].fillna("").astype(str).str.strip()
            mask_empty = base_ad_bundle["final_ad_name"].isin(["", "nan", "None"])
            base_ad_bundle.loc[mask_empty, "final_ad_name"] = base_ad_bundle.loc[mask_empty, "ad_name"].astype(str)
        else:
            base_ad_bundle["final_ad_name"] = base_ad_bundle["ad_name"].astype(str)
            
        base_ad = base_ad_bundle.rename(columns={"final_ad_name": "키워드/상품명"})
    else:
        base_ad = pd.DataFrame()
        
    base_bundle = pd.concat([base_kw, base_ad], ignore_index=True)

    view_cmp = view.copy()
    if not base_bundle.empty:
        valid_keys = [k for k in ["customer_id", "adgroup_id", "키워드/상품명"] if k in view_cmp.columns and k in base_bundle.columns]
        if valid_keys:
            view_cmp = _apply_comparison_metrics(view_cmp, base_bundle, valid_keys)
        else:
            view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [])
    else:
        view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [])

    metrics_cols_cmp = [
        "노출", "이전 노출", "노출 증감", "노출 증감(%)",
        "클릭", "이전 클릭", "클릭 증감", "클릭 증감(%)",
        "광고비", "이전 광고비", "광고비 증감", "광고비 증감(%)",
        "CPC(원)", "이전 CPC(원)", "CPC 증감", "CPC 증감(%)",
        "전환", "이전 전환", "전환 증감", 
        "CPA(원)", "전환매출", "이전 ROAS(%)", "ROAS(%)", "ROAS 증감(%)"
    ]

    base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드/상품명"]
    if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
        base_cols_cmp.extend(["평균순위", "이전 평균순위", "순위 변화"])

    render_item_comparison_search("키워드/소재", view_cmp, base_bundle, "키워드/상품명", start_dt, end_dt, b1, b2)

    col_camp_cmp, col_grp_cmp = st.columns(2)
    camps_cmp = ["전체"] + sorted([str(x) for x in view_cmp["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view_cmp.columns else ["전체"]
    sel_camp_cmp = col_camp_cmp.selectbox("캠페인 필터", camps_cmp, key="kw_camp_filter_cmp")

    filtered_cmp = view_cmp.copy()
    if sel_camp_cmp != "전체":
        filtered_cmp = filtered_cmp[filtered_cmp["캠페인"] == sel_camp_cmp]

    grps_cmp = ["전체"] + sorted([str(x) for x in filtered_cmp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_cmp.columns else ["전체"]
    sel_grp_cmp = col_grp_cmp.selectbox("광고그룹 필터", grps_cmp, key="kw_grp_filter_cmp")

    disp = filtered_cmp.copy()
    if sel_grp_cmp != "전체":
        disp = disp[disp["광고그룹"] == sel_grp_cmp]

    final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in disp.columns]
    disp = disp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()

    styled_cmp = disp.style.format(fmt_cmp)
    delta_cols = [c for c in ["노출 증감(%)", "노출 증감", "클릭 증감(%)", "클릭 증감", "광고비 증감(%)", "광고비 증감", "CPC 증감(%)", "CPC 증감", "순위 변화", "전환 증감", "ROAS 증감(%)"] if c in disp.columns]
    if delta_cols:
        try: styled_cmp = styled_cmp.map(style_table_deltas, subset=delta_cols)
        except AttributeError: styled_cmp = styled_cmp.applymap(style_table_deltas, subset=delta_cols)

    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>키워드/소재 기간 비교 표</div>", unsafe_allow_html=True)
    st.dataframe(
        styled_cmp, 
        use_container_width=True, 
        height=550, 
        hide_index=True
    )


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return
    st.markdown("<div class='nv-sec-title'>키워드/소재(쇼핑) 상세 분석</div>", unsafe_allow_html=True)
    st.caption("파워링크는 키워드 단위, 쇼핑검색은 일반 상품소재 단위 성과를 보여줍니다.")

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_keyword", 300))

    # ✨ 동그라미 로딩(spinner) 추가
    with st.spinner("🔄 키워드 및 소재 데이터를 집계하고 있습니다... 잠시만 기다려주세요."):
        kw_bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)
        ad_bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=50000, top_k=50)
        view = compute_keyword_view(kw_bundle, ad_bundle, meta)

    tab_main, tab_cmp = st.tabs(["종합 성과", "기간 비교"])
    
    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"
    }

    with tab_main:
        render_keyword_main(view, top_n, fmt)

    with tab_cmp:
        fmt_cmp = fmt.copy()
        fmt_cmp.update({
            "이전 노출": "{:,.0f}", "노출 증감": "{:+,.0f}", "노출 증감(%)": "{:+.2f}%",
            "이전 클릭": "{:,.0f}", "클릭 증감": "{:+,.0f}", "클릭 증감(%)": "{:+.2f}%",
            "이전 광고비": "{:,.0f}", "광고비 증감": "{:+,.0f}", "광고비 증감(%)": "{:+.2f}%",
            "이전 CPC(원)": "{:,.0f}", "CPC 증감": "{:+,.0f}", "CPC 증감(%)": "{:+.2f}%",
            "이전 전환": "{:,.1f}", "전환 증감": "{:+.1f}",
            "이전 ROAS(%)": "{:,.2f}%", "ROAS 증감(%)": "{:+.2f}%",
            "순위 변화": "{:+.1f}"
        })
        render_keyword_cmp(view, engine, cids, type_sel, top_n, fmt_cmp, f["start"], f["end"])
