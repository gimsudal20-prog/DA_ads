# -*- coding: utf-8 -*-
"""view_ad.py - Ad performance & A/B Testing page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta, _render_ab_test_sbs, render_item_comparison_search, style_table_deltas

# ✨ 최고속도 최적화: 가장 무거운 연산(병합, 특수문자 제거, 지표 계산)을 캐싱
@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_ad_view(bundle, meta):
    if bundle is None or bundle.empty:
        return pd.DataFrame()
        
    df = _perf_common_merge_meta(bundle, meta)
    
    view = df.rename(columns={
        "account_name": "업체명", "manager": "담당자", 
        "campaign_type": "캠페인유형", "campaign_type_label": "캠페인유형",
        "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "소재내용", 
        "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
    }).copy()
    
    if "캠페인유형" not in view.columns and "campaign_type" in view.columns:
        view["캠페인유형"] = view["campaign_type"]

    if "소재내용" in view.columns:
        view["_clean_ad"] = view["소재내용"].astype(str).str.replace("|", "").str.strip()
        view = view[view["_clean_ad"] != ""]
        view = view.drop(columns=["_clean_ad"])

    if view.empty:
        return pd.DataFrame()

    for c in ["노출", "클릭", "광고비", "전환", "전환매출"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
        else: view[c] = 0

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CVR(%)"] = np.where(view["클릭"] > 0, (view["전환"] / view["클릭"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    
    return view

# ✨ UI 속도 개선 1: 소재 탭 내부(파워링크, 쇼핑검색)를 파편화하여 필터 조작 시 부분 렌더링
@st.fragment
def _render_ad_tab(df_tab: pd.DataFrame, ad_type_name: str, top_n: int, fmt: dict, start_dt, end_dt):
    if df_tab.empty:
        st.info(f"해당 기간의 {ad_type_name} 데이터가 없습니다.")
        return

    c1, c2 = st.columns([1, 1])
    with c1:
        camps = ["전체"] + sorted([str(x) for x in df_tab["캠페인"].unique() if str(x).strip()])
        sel_camp = st.selectbox("캠페인 필터", camps, key=f"ad_tab_f1_{ad_type_name}")
    with c2:
        if sel_camp != "전체":
            filtered_grp = df_tab[df_tab["캠페인"] == sel_camp]
            grps = ["전체"] + sorted([str(x) for x in filtered_grp["광고그룹"].unique() if str(x).strip()])
            sel_grp = st.selectbox("광고그룹 필터", grps, key=f"ad_tab_f2_{ad_type_name}")
        else:
            sel_grp = "전체"
            st.selectbox("광고그룹 필터", ["전체"], disabled=True, key=f"ad_tab_f2_dis_{ad_type_name}")

    if sel_camp != "전체":
        df_tab = df_tab[df_tab["캠페인"] == sel_camp]
        if sel_grp != "전체":
            df_tab = df_tab[df_tab["광고그룹"] == sel_grp]
            _render_ab_test_sbs(df_tab, start_dt, end_dt)

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
    disp = df_tab[[c for c in cols if c in df_tab.columns]].copy()
    disp = disp.sort_values("광고비", ascending=False).head(top_n)

    st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>{ad_type_name} 성과 표</div>", unsafe_allow_html=True)
    render_big_table(disp.style.format(fmt), f"ad_bt_{ad_type_name}", 500)

# ✨ UI 속도 개선 2: 랜딩페이지 효율 탭 파편화
@st.fragment
def render_landing_tab(view):
    if "landing_url" in view.columns:
        df_lp = view[view["landing_url"].astype(str) != ""].copy()
        if df_lp.empty: st.info("수집된 URL 데이터가 없습니다.")
        else:
            lp_grp = df_lp.groupby("landing_url", as_index=False)[["노출", "클릭", "광고비", "전환", "전환매출"]].sum()
            lp_grp["CTR(%)"] = np.where(lp_grp["노출"] > 0, (lp_grp["클릭"]/lp_grp["노출"])*100, 0)
            lp_grp["CVR(%)"] = np.where(lp_grp["클릭"] > 0, (lp_grp["전환"]/lp_grp["클릭"])*100, 0)
            lp_grp["ROAS(%)"] = np.where(lp_grp["광고비"] > 0, (lp_grp["전환매출"]/lp_grp["광고비"])*100, 0)
            lp_grp = lp_grp.rename(columns={"landing_url": "랜딩페이지 URL"}).sort_values("광고비", ascending=False)
            
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px;'>랜딩페이지(URL) 효율</div>", unsafe_allow_html=True)
            st.dataframe(
                lp_grp.style.background_gradient(cmap="Blues", subset=["CVR(%)", "ROAS(%)"]).format({'노출': '{:,.0f}', '클릭': '{:,.0f}', '광고비': '{:,.0f}', '전환': '{:,.1f}', '전환매출': '{:,.0f}', 'CTR(%)': '{:,.2f}%', 'CVR(%)': '{:,.2f}%', 'ROAS(%)': '{:,.2f}%'}), 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "랜딩페이지 URL": st.column_config.LinkColumn("랜딩페이지 URL (클릭 시 이동)", display_text="링크 열기")
                }
            )
    else: st.info("랜딩페이지 URL 컬럼이 없습니다.")

# ✨ UI 속도 개선 3: 소재 기간 비교 탭 파편화
@st.fragment
def render_ad_cmp_tab(view, engine, cids, type_sel, top_n, fmt, start_dt, end_dt):
    cmp_sub_mode = st.radio("비교 대상", ["파워링크", "쇼핑검색"], horizontal=True, key="ad_cmp_sub")
    opts = get_dynamic_cmp_options(start_dt, end_dt)
    cmp_mode = st.radio("비교 기준", [o for o in opts if o != "비교 안함"], horizontal=True, key="ad_cmp_base")
        
    b1, b2 = period_compare_range(start_dt, end_dt, cmp_mode)
    base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=10000, top_k=50)

    df_target = view[view["캠페인유형"] == "파워링크"].copy() if "파워링크" in cmp_sub_mode else view[view["캠페인유형"] == "쇼핑검색"].copy()
    if "쇼핑검색" in cmp_sub_mode:
        df_target = df_target[df_target['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]
        df_target = df_target[~df_target['소재내용'].astype(str).str.contains('TALK', na=False, case=False)]

    if df_target.empty: st.info("비교할 데이터가 없습니다.")
    else:
        if not base_ad_bundle.empty:
            valid_keys = [k for k in ['customer_id', 'ad_id'] if k in df_target.columns and k in base_ad_bundle.columns]
            if valid_keys: df_target = append_comparison_data(df_target, base_ad_bundle, valid_keys)
        
        c1, c2 = st.columns(2)
        with c1:
            sel_c = st.selectbox("캠페인 필터", ["전체"] + sorted(df_target["캠페인"].unique().tolist()), key="ad_cmp_f1")
        if sel_c != "전체":
            df_target = df_target[df_target["캠페인"] == sel_c]

        base_for_search = base_ad_bundle.rename(columns={"ad_name": "소재내용"}) if not base_ad_bundle.empty else pd.DataFrame()
        render_item_comparison_search("소재", df_target, base_for_search, "소재내용", start_dt, end_dt, b1, b2)

        cols_cmp = ["업체명", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "전환", "전환매출", "ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
        disp_c = df_target[[c for c in cols_cmp if c in df_target.columns]].sort_values("광고비", ascending=False).head(top_n)
        
        styled_cmp = disp_c.style.format(fmt)
        delta_cols = [c for c in ["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"] if c in disp_c.columns]
        if delta_cols:
            try: styled_cmp = styled_cmp.map(style_table_deltas, subset=delta_cols)
            except AttributeError: styled_cmp = styled_cmp.applymap(style_table_deltas, subset=delta_cols)
            
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>소재 기간 비교 데이터</div>", unsafe_allow_html=True)
        render_big_table(styled_cmp, "ad_cmp_grid_final", 500)


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("<div class='nv-sec-title'>광고 소재 및 랜딩페이지 분석</div>", unsafe_allow_html=True)
    
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_ad", 200))
    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=10000, top_k=50)
    
    # 캐시된 함수를 통해 로딩 속도 획기적 개선
    view = compute_ad_view(bundle, meta)
    if view.empty:
        st.info("해당 기간에 분석할 유효한 광고 소재(카피) 데이터가 없습니다.")
        return

    tab_pl, tab_shop, tab_landing, tab_cmp = st.tabs(["파워링크", "쇼핑검색", "랜딩페이지 효율", "기간 비교"])
    
    fmt = {"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}", "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"}

    with tab_pl:
        df_pl = view[view["캠페인유형"] == "파워링크"].copy()
        _render_ad_tab(df_pl, "파워링크", top_n, fmt, f["start"], f["end"])
        
    with tab_shop:
        df_shop = view[view["캠페인유형"] == "쇼핑검색"].copy()
        if not df_shop.empty:
            df_shop = df_shop[df_shop['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]
            df_shop = df_shop[~df_shop['소재내용'].astype(str).str.contains('TALK', na=False, case=False)]
        _render_ad_tab(df_shop, "쇼핑검색 확장소재", top_n, fmt, f["start"], f["end"])

    with tab_landing:
        render_landing_tab(view)

    with tab_cmp:
        render_ad_cmp_tab(view, engine, cids, type_sel, top_n, fmt, f["start"], f["end"])
