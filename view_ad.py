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
from page_helpers import _perf_common_merge_meta, _render_ab_test_sbs

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🧩 성과 (광고 소재 분석)")
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_ad", 200))
    
    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=10000, top_k=50)
    if bundle is None or bundle.empty: return

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
        st.info("해당 기간에 분석할 유효한 광고 소재(카피) 데이터가 없습니다.")
        return

    for c in ["노출", "클릭", "광고비", "전환", "전환매출"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
        else: view[c] = 0

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CVR(%)"] = np.where(view["클릭"] > 0, (view["전환"] / view["클릭"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)

    # ✨ [핵심 조치] 비교 기능을 전용 탭으로 분리
    tab_pl, tab_shop, tab_landing, tab_cmp = st.tabs(["🎯 파워링크 (일반 소재)", "🛍️ 쇼핑검색 (확장소재 전용)", "🔗 랜딩페이지(URL) 효율 분석", "⚖️ 기간 비교 분석"])
    
    fmt = {"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}", "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"}

    def _render_ad_tab(df_tab: pd.DataFrame, title_prefix: str, ad_type_name: str):
        if df_tab.empty:
            st.info(f"해당 기간의 {ad_type_name} 데이터가 없습니다.")
            return

        c1, c2 = st.columns([1, 1])
        with c1:
            camps = ["전체"] + sorted([str(x) for x in df_tab["캠페인"].unique() if str(x).strip()])
            sel_camp = st.selectbox("🎯 소속 캠페인 필터", camps, key=f"ad_camp_filter_{ad_type_name}")
            
        with c2:
            if sel_camp != "전체":
                filtered_grp = df_tab[df_tab["캠페인"] == sel_camp]
                grps = ["전체"] + sorted([str(x) for x in filtered_grp["광고그룹"].unique() if str(x).strip()])
                sel_grp = st.selectbox("📂 소속 광고그룹 필터", grps, key=f"ad_grp_filter_{ad_type_name}")
            else:
                sel_grp = "전체"
                st.selectbox("📂 소속 광고그룹 필터", ["전체"], disabled=True, key=f"ad_grp_filter_{ad_type_name}")

        st.divider()

        if sel_camp != "전체":
            df_tab = df_tab[df_tab["캠페인"] == sel_camp]
            if sel_grp != "전체":
                df_tab = df_tab[df_tab["광고그룹"] == sel_grp]
                _render_ab_test_sbs(df_tab, f["start"], f["end"])

        cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
            
        disp = df_tab[[c for c in cols if c in df_tab.columns]].copy()
        disp = disp.sort_values("광고비", ascending=False).head(top_n)

        styled_disp = disp.style.format(fmt)
        st.markdown(f"#### 📊 {ad_type_name} 상세 성과 표")
        render_big_table(styled_disp, f"ad_big_table_{ad_type_name}", 500)

    with tab_pl:
        df_pl = view[view["캠페인유형"] == "파워링크"] if "캠페인유형" in view.columns else view
        _render_ad_tab(df_pl, "파워링크", "파워링크 소재")
        
    with tab_shop:
        df_shop = view[view["캠페인유형"] == "쇼핑검색"] if "캠페인유형" in view.columns else pd.DataFrame()
        if not df_shop.empty:
            df_shop = df_shop[df_shop['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]
        if not df_shop.empty:
            _render_ad_tab(df_shop, "쇼핑검색", "쇼핑검색 확장소재")
        else:
            st.info("해당 기간에 분석할 쇼핑검색 확장소재가 없습니다.")

    with tab_landing:
        st.markdown("### 🔗 랜딩페이지(URL)별 구매 전환율(CVR) 비교")
        st.caption("어떤 상세페이지나 기획전 URL로 고객을 보냈을 때 구매 전환율(CVR)과 ROAS가 가장 높은지 분석합니다.")
        
        if "landing_url" in view.columns:
            df_lp = view[view["landing_url"].astype(str) != ""].copy()
            if df_lp.empty:
                st.info("수집된 랜딩페이지 URL 데이터가 없습니다. (수집기를 다시 한 번 돌려 DB를 업데이트해주세요.)")
            else:
                lp_grp = df_lp.groupby("landing_url", as_index=False)[["노출", "클릭", "광고비", "전환", "전환매출"]].sum()
                
                lp_grp["CTR(%)"] = np.where(lp_grp["노출"] > 0, (lp_grp["클릭"] / lp_grp["노출"]) * 100, 0)
                lp_grp["CVR(%)"] = np.where(lp_grp["클릭"] > 0, (lp_grp["전환"] / lp_grp["클릭"]) * 100, 0)
                lp_grp["ROAS(%)"] = np.where(lp_grp["광고비"] > 0, (lp_grp["전환매출"] / lp_grp["광고비"]) * 100, 0)
                
                lp_grp = lp_grp.rename(columns={"landing_url": "랜딩페이지 URL"})
                lp_grp = lp_grp.sort_values("광고비", ascending=False).reset_index(drop=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                styled_df = lp_grp.style.background_gradient(cmap="Greens", subset=["CVR(%)", "ROAS(%)"]).format({
                    '노출': '{:,.0f}', '클릭': '{:,.0f}', '광고비': '{:,.0f}', 
                    '전환': '{:,.1f}', '전환매출': '{:,.0f}', 
                    'CTR(%)': '{:,.2f}%', 'CVR(%)': '{:,.2f}%', 'ROAS(%)': '{:,.2f}%'
                })
                
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.info("DB 구조에 랜딩페이지 URL 정보가 아직 포함되어 있지 않습니다. (수집기를 최신 버전으로 돌려주세요)")

    # ✨ [핵심 조치] 소재 기간 비교 전용 탭 구축
    with tab_cmp:
        st.markdown("### ⚖️ 기간 비교 분석")
        cmp_view_mode = st.radio("비교 대상 선택", ["🎯 파워링크 일반 소재", "🛍️ 쇼핑검색 확장소재"], horizontal=True)
        
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("📊 기간 비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="ad_cmp_mode")
        
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=10000, top_k=50)

        df_target = view[view["캠페인유형"] == "파워링크"].copy() if "파워링크" in cmp_view_mode else view[view["캠페인유형"] == "쇼핑검색"].copy()
        if "쇼핑검색" in cmp_view_mode:
            df_target = df_target[df_target['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]

        if df_target.empty:
            st.info("비교할 데이터가 없습니다.")
        else:
            if not base_ad_bundle.empty:
                valid_keys = [k for k in ['customer_id', 'ad_id'] if k in df_target.columns and k in base_ad_bundle.columns]
                if valid_keys:
                    df_target = append_comparison_data(df_target, base_ad_bundle, valid_keys)
                    
            c1, c2 = st.columns([1, 1])
            with c1:
                camps = ["전체"] + sorted([str(x) for x in df_target["캠페인"].unique() if str(x).strip()])
                sel_camp = st.selectbox("🎯 소속 캠페인 필터", camps, key=f"ad_cmp_camp_filter")
                
            with c2:
                if sel_camp != "전체":
                    filtered_grp = df_target[df_target["캠페인"] == sel_camp]
                    grps = ["전체"] + sorted([str(x) for x in filtered_grp["광고그룹"].unique() if str(x).strip()])
                    sel_grp = st.selectbox("📂 소속 광고그룹 필터", grps, key=f"ad_cmp_grp_filter")
                else:
                    sel_grp = "전체"
                    st.selectbox("📂 소속 광고그룹 필터", ["전체"], disabled=True, key=f"ad_cmp_grp_filter")

            st.divider()

            if sel_camp != "전체":
                df_target = df_target[df_target["캠페인"] == sel_camp]
                if sel_grp != "전체":
                    df_target = df_target[df_target["광고그룹"] == sel_grp]
                    _render_ab_test_sbs(df_target, f["start"], f["end"])

                if not df_target.empty:
                    render_comparison_section(df_target, cmp_mode, b1, b2, f["start"], f["end"], "선택 항목 상세 비교")

            cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
            
            disp = df_target[[c for c in cols if c in df_target.columns]].copy()
            disp = disp.sort_values("광고비", ascending=False).head(top_n)

            styled_disp = disp.style.format(fmt)

            st.markdown("#### 📊 소재 기간 비교 표")
            render_big_table(styled_disp, f"ad_cmp_grid", 500)
