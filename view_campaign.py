# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import *
from ui import *
from page_helpers import *
# ✨ [추가] 언더스코어(_)로 시작하는 함수 명시적 불러오기
from page_helpers import _perf_common_merge_meta

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🚀 성과 (캠페인 기준)")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_mode = st.radio("📊 캠페인 단위 기간 비교", opts, horizontal=True, key="camp_cmp_mode")
        st.caption("선택한 이전 기간의 성과와 직접 비교하여 증감율을 제공합니다.")
    
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_campaign", 200))
    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 5000), top_k=10)
    if bundle is None or bundle.empty: return

    bundle = _perf_common_merge_meta(bundle, meta)
    view = bundle.rename(columns={
        "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형", "campaign_type_label": "캠페인유형",
        "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비",
        "conv": "전환", "sales": "전환매출"
    }).copy()

    if "캠페인유형" not in view.columns and "campaign_type" in view.columns:
        view = view.rename(columns={"campaign_type": "캠페인유형"})

    for c in ["노출", "클릭", "광고비", "전환", "전환매출"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
        else: view[c] = 0

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0).round(2)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0).round(0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0).round(0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0).round(0)

    b1, b2 = None, None
    if cmp_mode != "비교 안함":
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=10000, top_k=10)
        if not base_bundle.empty:
            view = append_comparison_data(view, base_bundle, ['customer_id', 'campaign_id'])

    c1, c2 = st.columns([1, 3])
    with c1:
        camps = ["전체"] + sorted([str(x) for x in view["캠페인"].unique() if str(x).strip()])
        sel_camp = st.selectbox("🎯 개별 캠페인 검색/필터", camps, key="camp_name_filter", help="타이핑하여 캠페인명을 검색할 수 있습니다.")

    if sel_camp != "전체": 
        view = view[view["캠페인"] == sel_camp]
        if cmp_mode != "비교 안함" and not view.empty:
            render_comparison_section(view, cmp_mode, b1, b2, f["start"], f["end"], "선택 캠페인 상세 비교")

    base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
    metrics_cols = ["노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
    
    if cmp_mode != "비교 안함":
        metrics_cols.extend(["광고비 증감(%)", "ROAS 증감(%p)", "전환 증감"])

    cols = base_cols + metrics_cols
    disp = view[[c for c in cols if c in view.columns]].copy()
    disp = disp.sort_values("광고비", ascending=False).head(top_n)

    for c in ["노출", "클릭", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]:
        if c in disp.columns: disp[c] = disp[c].astype(int)
    if "CTR(%)" in disp.columns: disp["CTR(%)"] = disp["CTR(%)"].astype(float).round(2)

    render_big_table(disp, key="camp_main_grid", height=560)
