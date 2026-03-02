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

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8-sig')

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🧩 광고 소재 및 랜딩페이지 분석")
    st.caption("어떤 카피와 랜딩페이지가 고객의 마음을 움직였는지 A/B 테스트 결과를 확인하세요.")
    
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

    tab_pl, tab_shop, tab_landing, tab_cmp = st.tabs(["🎯 파워링크 (일반 소재)", "🛍️ 쇼핑검색 (확장소재 전용)", "🔗 랜딩페이지(URL) 효율 분석", "⚖️ 기간 비교 분석"])
    
    fmt = {"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}", "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"}

    def _render_ad_tab(df_tab: pd.DataFrame, ad_type_name: str):
        if df_tab.empty:
            st.info(f"해당 기간의 {ad_type_name} 데이터가 없습니다.")
            return

        with st.container():
            st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:16px;'>", unsafe_allow_html=True)
            c1, c2 = st.columns([1, 1])
            with c1:
                camps = ["전체"] + sorted([str(x) for x in df_tab["캠페인"].unique() if str(x).strip()])
                sel_camp = st.selectbox("🎯 소속 캠페인 필터", camps, key=f"ad_tab_f1_{ad_type_name}")
            with c2:
                if sel_camp != "전체":
                    filtered_grp = df_tab[df_tab["캠페인"] == sel_camp]
                    grps = ["전체"] + sorted([str(x) for x in filtered_grp["광고그룹"].unique() if str(x).strip()])
                    sel_grp = st.selectbox("📂 소속 광고그룹 필터", grps, key=f"ad_tab_f2_{ad_type_name}")
                else:
                    sel_grp = "전체"
                    st.selectbox("📂 소속 광고그룹 필터", ["전체"], disabled=True, key=f"ad_tab_f2_dis_{ad_type_name}")
            st.markdown("</div>", unsafe_allow_html=True)

        if sel_camp != "전체":
            df_tab = df_tab[df_tab["캠페인"] == sel_camp]
            if sel_grp != "전체":
                df_tab = df_tab[df_tab["광고그룹"] == sel_grp]
                _render_ab_test_sbs(df_tab, f["start"], f["end"])

        cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
        disp = df_tab[[c for c in cols if c in df_tab.columns]].copy()
        disp = disp.sort_values("광고비", ascending=False).head(top_n)

        # ✨ 다운로드 버튼
        col1, col2 = st.columns([8, 2])
        with col1: st.markdown(f"#### 📊 {ad_type_name} 상세 성과 표")
        with col2: st.download_button(label="📥 CSV 다운로드", data=convert_df_to_csv(disp), file_name=f'ad_performance_{ad_type_name}.csv', mime='text/csv', key=f'dl_{ad_type_name}', use_container_width=True)
        
        render_big_table(disp.style.format(fmt), f"ad_bt_{ad_type_name}", 500)

    with tab_pl:
        df_pl = view[view["캠페인유형"] == "파워링크"].copy()
        _render_ad_tab(df_pl, "파워링크")
        
    with tab_shop:
        df_shop = view[view["캠페인유형"] == "쇼핑검색"].copy()
        if not df_shop.empty:
            df_shop = df_shop[df_shop['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]
            df_shop = df_shop[~df_shop['소재내용'].astype(str).str.contains('TALK', na=False, case=False)]
        _render_ad_tab(df_shop, "쇼핑검색 확장소재")

    with tab_landing:
        if "landing_url" in view.columns:
            df_lp = view[view["landing_url"].astype(str) != ""].copy()
            if df_lp.empty: st.info("수집된 URL 데이터가 없습니다.")
            else:
                lp_grp = df_lp.groupby("landing_url", as_index=False)[["노출", "클릭", "광고비", "전환", "전환매출"]].sum()
                lp_grp["CTR(%)"] = np.where(lp_grp["노출"] > 0, (lp_grp["클릭"]/lp_grp["노출"])*100, 0)
                lp_grp["CVR(%)"] = np.where(lp_grp["클릭"] > 0, (lp_grp["전환"]/lp_grp["클릭"])*100, 0)
                lp_grp["ROAS(%)"] = np.where(lp_grp["광고비"] > 0, (lp_grp["전환매출"]/lp_grp["광고비"])*100, 0)
                lp_grp = lp_grp.rename(columns={"landing_url": "랜딩페이지 URL"}).sort_values("광고비", ascending=False)
                
                # ✨ 다운로드 버튼
                col1, col2 = st.columns([8, 2])
                with col1: st.markdown("### 🔗 랜딩페이지(URL)별 효율 분석")
                with col2: st.download_button(label="📥 CSV 다운로드", data=convert_df_to_csv(lp_grp), file_name='landing_page_performance.csv', mime='text/csv', use_container_width=True)
                
                # ✨ [NEW] URL을 클릭 가능하게 만들어 즉시 확인 가능하도록 UX 개선!
                st.dataframe(
                    lp_grp.style.background_gradient(cmap="Greens", subset=["CVR(%)", "ROAS(%)"]).format({'노출': '{:,.0f}', '클릭': '{:,.0f}', '광고비': '{:,.0f}', '전환': '{:,.1f}', '전환매출': '{:,.0f}', 'CTR(%)': '{:,.2f}%', 'CVR(%)': '{:,.2f}%', 'ROAS(%)': '{:,.2f}%'}), 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "랜딩페이지 URL": st.column_config.LinkColumn("랜딩페이지 URL (클릭 시 이동)", display_text="🔗 링크 열기")
                    }
                )
        else: st.info("랜딩페이지 URL 컬럼이 없습니다.")

    with tab_cmp:
        with st.container():
            st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:16px;'>", unsafe_allow_html=True)
            cmp_sub_mode = st.radio("비교 대상", ["🎯 파워링크 일반", "🛍️ 쇼핑검색 확장"], horizontal=True, key="ad_cmp_sub")
            opts = get_dynamic_cmp_options(f["start"], f["end"])
            cmp_mode = st.radio("비교 기준", [o for o in opts if o != "비교 안함"], horizontal=True, key="ad_cmp_base")
            st.markdown("</div>", unsafe_allow_html=True)
            
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
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
                render_comparison_section(df_target, cmp_mode, b1, b2, f["start"], f["end"], "상세 비교 결과")

            cols_cmp = ["업체명", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "전환", "전환매출", "ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
            disp_c = df_target[[c for c in cols_cmp if c in df_target.columns]].sort_values("광고비", ascending=False).head(top_n)
            
            col1, col2 = st.columns([8, 2])
            with col1: st.markdown("### ⚖️ 소재 기간 비교 분석")
            with col2: st.download_button(label="📥 CSV 다운로드", data=convert_df_to_csv(disp_c), file_name='compare_ad_performance.csv', mime='text/csv', use_container_width=True)

            render_big_table(disp_c.style.format(fmt), "ad_cmp_grid_final", 500)
