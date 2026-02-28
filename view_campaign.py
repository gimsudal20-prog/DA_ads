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

    opts = get_dynamic_cmp_options(f["start"], f["end"])
    is_cmp = st.toggle(f"📊 기간 비교 켜기 ({opts[1]})", value=False, key="camp_cmp_toggle")
    cmp_mode = opts[1] if is_cmp else "비교 안함"
    
    b1, b2 = None, None
    if cmp_mode != "비교 안함":
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)
        if not base_bundle.empty:
            valid_keys = [k for k in ['customer_id', 'campaign_id'] if k in view.columns and k in base_bundle.columns]
            if valid_keys:
                view = append_comparison_data(view, base_bundle, valid_keys)

    c1, c2 = st.columns([1, 2])
    with c1:
        if not view.empty and "캠페인" in view.columns:
            camps = ["전체"] + sorted([str(x) for x in view["캠페인"].unique() if str(x).strip()])
            sel_camp = st.selectbox("🎯 개별 캠페인 검색/필터", camps, key="camp_name_filter")
        else:
            sel_camp = "전체"

    if sel_camp != "전체":
        view = view[view["캠페인"] == sel_camp]
        if cmp_mode != "비교 안함" and not view.empty:
            render_comparison_section(view, cmp_mode, b1, b2, f["start"], f["end"], "선택 캠페인 상세 비교")

    base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
    metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
    if cmp_mode != "비교 안함":
        metrics_cols.extend(["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"])
        
    final_cols = [c for c in base_cols + metrics_cols if c in view.columns]
    disp = view[final_cols].sort_values("광고비", ascending=False).head(top_n)

    # ✨ [NEW] 데이터를 억지로 자르지 않고, Styler 포맷터를 통해 예쁘게 표출합니다!
    fmt = {}
    if "노출" in disp.columns: fmt["노출"] = "{:,.0f}"
    if "클릭" in disp.columns: fmt["클릭"] = "{:,.0f}"
    if "광고비" in disp.columns: fmt["광고비"] = "{:,.0f}"
    if "CPC(원)" in disp.columns: fmt["CPC(원)"] = "{:,.0f}"
    if "CPA(원)" in disp.columns: fmt["CPA(원)"] = "{:,.0f}"
    if "전환매출" in disp.columns: fmt["전환매출"] = "{:,.0f}"
    if "전환" in disp.columns: fmt["전환"] = "{:,.1f}"
    if "CTR(%)" in disp.columns: fmt["CTR(%)"] = "{:,.2f}%"
    if "ROAS(%)" in disp.columns: fmt["ROAS(%)"] = "{:,.2f}%"

    styled_disp = disp.style.format(fmt)

    st.markdown("#### 📊 캠페인 종합 성과 표")
    render_big_table(styled_disp, "camp_grid", 550)
