# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict

from data import query_campaign_bundle
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, append_comparison_data, render_comparison_section, _perf_common_merge_meta

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🚀 성과 (캠페인 단위)")

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
    if bundle is None or bundle.empty:
        st.info("선택된 기간/조건에 해당하는 캠페인 데이터가 없습니다.")
        return

    df = _perf_common_merge_meta(bundle, meta)
    
    view = df.rename(columns={
        "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
        "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", 
        "cost": "광고비", "conv": "전환", "sales": "전환매출"
    }).copy()
    
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
        else: view[c] = 0

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)

    # ✨ [핵심 조치] 비교 기능을 전용 탭으로 분리
    tab_main, tab_cmp = st.tabs(["📊 캠페인 종합 성과", "⚖️ 기간 비교 분석"])

    fmt = {"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}", "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"}

    with tab_main:
        c1, c2 = st.columns([1, 2])
        with c1:
            if not view.empty and "캠페인" in view.columns:
                camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].unique() if str(x).strip()])
                sel_camp_main = st.selectbox("🎯 개별 캠페인 검색/필터", camps_main, key="camp_name_filter_main")
            else:
                sel_camp_main = "전체"

        disp_main = view.copy()
        if sel_camp_main != "전체":
            disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
        metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
        
        final_cols = [c for c in base_cols + metrics_cols if c in disp_main.columns]
        disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n)

        styled_disp_main = disp_main.style.format(fmt)
        st.markdown("#### 📊 캠페인 종합 성과 표")
        render_big_table(styled_disp_main, "camp_grid_main", 550)

    with tab_cmp:
        st.markdown("### ⚖️ 기간 비교 분석 (캠페인)")
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("📊 기간 비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")
        
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)
        
        view_cmp = view.copy()
        if not base_bundle.empty:
            valid_keys = [k for k in ['customer_id', 'campaign_id'] if k in view_cmp.columns and k in base_bundle.columns]
            if valid_keys:
                view_cmp = append_comparison_data(view_cmp, base_bundle, valid_keys)

        c1, c2 = st.columns([1, 2])
        with c1:
            if not view_cmp.empty and "캠페인" in view_cmp.columns:
                camps_cmp = ["전체"] + sorted([str(x) for x in view_cmp["캠페인"].unique() if str(x).strip()])
                sel_camp_cmp = st.selectbox("🎯 개별 캠페인 검색/필터", camps_cmp, key="camp_name_filter_cmp")
            else:
                sel_camp_cmp = "전체"

        if sel_camp_cmp != "전체":
            view_cmp = view_cmp[view_cmp["캠페인"] == sel_camp_cmp]
            if not view_cmp.empty:
                render_comparison_section(view_cmp, cmp_mode, b1, b2, f["start"], f["end"], "선택 캠페인 상세 비교")

        metrics_cols_cmp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
        final_cols_cmp = [c for c in base_cols + metrics_cols_cmp if c in view_cmp.columns]
        disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n)

        styled_disp_cmp = disp_cmp.style.format(fmt)
        st.markdown("#### 📊 캠페인 기간 비교 표")
        render_big_table(styled_disp_cmp, "camp_grid_cmp", 550)
