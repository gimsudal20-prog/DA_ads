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

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0).round(2)
    view["CVR(%)"] = np.where(view["클릭"] > 0, (view["전환"] / view["클릭"]) * 100, 0.0).round(2)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0).round(0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0).round(0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0).round(0)

    tab_pl, tab_shop = st.tabs(["🎯 파워링크 (일반 소재)", "🛍️ 쇼핑검색 (확장소재 전용)"])

    def _render_ad_tab(df_tab: pd.DataFrame, title_prefix: str, ad_type_name: str):
        if df_tab.empty:
            st.info(f"해당 기간의 {ad_type_name} 데이터가 없습니다.")
            return

        opts_ad = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_mode_ad = st.radio(f"📊 소재 단위 기간 비교", opts_ad, horizontal=True, key=f"ad_cmp_mode_{ad_type_name}")
        
        b1, b2 = None, None
        if cmp_mode_ad != "비교 안함":
            b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode_ad)
            base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=10000, top_k=50)
            if not base_ad_bundle.empty:
                valid_keys = [k for k in ['customer_id', 'ad_id'] if k in df_tab.columns and k in base_ad_bundle.columns]
                if valid_keys:
                    df_tab = append_comparison_data(df_tab, base_ad_bundle, valid_keys)
                
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

            if cmp_mode_ad != "비교 안함" and not df_tab.empty:
                render_comparison_section(df_tab, cmp_mode_ad, b1, b2, f["start"], f["end"], f"선택 {ad_type_name} 상세 비교")

        cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
        if cmp_mode_ad != "비교 안함":
            cols.extend(["광고비 증감(%)", "ROAS 증감(%p)", "전환 증감"])
            
        disp = df_tab[[c for c in cols if c in df_tab.columns]].copy()
        disp = disp.sort_values("광고비", ascending=False).head(top_n)

        for c in ["노출", "클릭", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]:
            if c in disp.columns: disp[c] = disp[c].astype(int)
        if "CTR(%)" in disp.columns: disp["CTR(%)"] = disp["CTR(%)"].astype(float).round(2)

        st.markdown(f"#### 📊 {ad_type_name} 상세 성과 표")
        render_big_table(disp, f"ad_big_table_{ad_type_name}", 500)

    with tab_pl:
        df_pl = view[view["캠페인유형"] == "파워링크"] if "캠페인유형" in view.columns else view
        _render_ad_tab(df_pl, "파워링크", "파워링크 소재")
        
    with tab_shop:
        df_shop = view[view["캠페인유형"] == "쇼핑검색"] if "캠페인유형" in view.columns else pd.DataFrame()
        
        if not df_shop.empty:
            df_shop = df_shop[df_shop['소재내용'].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)]

        st.info("💡 **쇼핑검색 확장소재 전용 분석:** 오직 쇼핑검색 추가로 등록한 '확장소재(추가홍보문구 등)' 데이터만 표시됩니다. (일반 상품 소재는 '키워드' 탭으로 이동되었습니다.)")
        
        if not df_shop.empty:
            ext_count = len(df_shop)
            st.success(f"🎉 성공! 수집된 쇼핑검색 확장소재(추가홍보문구 등)가 **{ext_count}건** 발견되어 분석되었습니다.")
            _render_ad_tab(df_shop, "쇼핑검색", "쇼핑검색 확장소재")
        else:
            st.warning("해당 기간에 사용된 쇼핑검색 확장소재가 없습니다.")
