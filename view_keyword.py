# -*- coding: utf-8 -*-
"""view_keyword.py - Keyword & Adgroup performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False): return
    st.markdown("## 🔎 성과 (그룹 / 키워드 단위)")
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_keyword", 300))
    
    bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=10000)

    # ✨ [핵심 조치] 비교 기능을 전용 탭으로 분리
    tab_pl, tab_shop, tab_cmp, tab_neg = st.tabs(["🎯 파워링크", "🛒 쇼핑검색", "⚖️ 기간 비교 분석", "💸 저효율 키워드 발굴기"])
    
    df_pl_raw = bundle[bundle["campaign_type_label"] == "파워링크"] if bundle is not None and not bundle.empty and "campaign_type_label" in bundle.columns else pd.DataFrame()
    fmt = {"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}", "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"}

    with tab_pl:
        is_group_view = st.toggle("📂 광고그룹 단위로 요약해서 보기", value=False, key="kw_view_toggle")
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        if not is_group_view:
            if not df_pl_raw.empty:
                view = _perf_common_merge_meta(df_pl_raw.sort_values("cost", ascending=False).head(top_n), meta)
                view = view.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()
                for c in ["광고비", "전환매출", "노출", "클릭", "전환"]: view[c] = pd.to_numeric(view.get(c,0), errors="coerce").fillna(0)
                view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
                view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
                view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
                view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)

                base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드"]
                if "avg_rank" in view.columns:
                    view["평균순위"] = view["avg_rank"].apply(lambda x: f"{float(x):.1f}위" if float(x) > 0 else "미수집")
                    base_cols.append("평균순위")
                    
                metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]

                c1, c2 = st.columns([1, 3])
                with c1:
                    view["_filter_label"] = view["캠페인"].astype(str) + " > " + view["광고그룹"].astype(str) + " > " + view["키워드"].astype(str)
                    kws = ["전체"] + sorted([str(x) for x in view["_filter_label"].unique() if str(x).strip()])
                    sel_kw = st.selectbox("🎯 개별 키워드 검색/필터", kws, key="kw_name_filter_pl_main")

                if sel_kw != "전체": view = view[view["_filter_label"] == sel_kw]

                disp = view[[c for c in base_cols + metrics_cols if c in view.columns]].copy()
                styled_disp = disp.style.format(fmt)
                
                if "평균순위" in view.columns:
                    all_kws = sorted([str(x) for x in view["키워드"].unique() if str(x).strip()])
                    selected_kws = st.multiselect("모니터링 핵심 키워드 선택", all_kws, default=all_kws[:4] if len(all_kws) >= 4 else all_kws, key="star_kws_main")
                    if selected_kws:
                        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
                        cols = st.columns(4)
                        target_df = view[view["키워드"].isin(selected_kws)]
                        for idx, kw in enumerate(selected_kws):
                            row_df = target_df[target_df["키워드"] == kw]
                            if not row_df.empty:
                                row = row_df.iloc[0]
                                avg_rank = getattr(row, 'avg_rank', 0)
                                rank_str = "순위 미수집" if pd.isna(avg_rank) or avg_rank == 0 else f"평균 {float(avg_rank):.1f}위"
                                roas = getattr(row, 'ROAS(%)', 0)
                                with cols[idx % 4]:
                                    ui_metric_or_stmetric(title=kw, value=rank_str, desc=f"ROAS {roas:.2f}%", key=f"kw_star_main_{idx}")
                st.divider()
                st.markdown("#### 📊 검색어별 상세 성과 표")
                render_big_table(styled_disp, "pl_grid_main", 500)
            else:
                st.info("해당 기간의 파워링크 키워드 데이터가 없습니다.")

        else:
            if not df_pl_raw.empty:
                grp_cols = [c for c in ['customer_id', 'campaign_type_label', 'campaign_name', 'adgroup_id', 'adgroup_name'] if c in df_pl_raw.columns]
                val_cols = [c for c in ['imp', 'clk', 'cost', 'conv', 'sales'] if c in df_pl_raw.columns]
                grp_cur = df_pl_raw.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp_cur = _perf_common_merge_meta(grp_cur, meta)
                
                view_grp = grp_cur.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()
                for c in ["광고비", "전환매출", "노출", "클릭", "전환"]: view_grp[c] = pd.to_numeric(view_grp[c], errors="coerce").fillna(0)
                view_grp["CTR(%)"] = np.where(view_grp.get("노출", 0) > 0, (view_grp.get("클릭", 0) / view_grp.get("노출", 0)) * 100, 0.0)
                view_grp["CPC(원)"] = np.where(view_grp.get("클릭", 0) > 0, view_grp.get("광고비", 0) / view_grp.get("클릭", 0), 0.0)
                view_grp["CPA(원)"] = np.where(view_grp.get("전환", 0) > 0, view_grp.get("광고비", 0) / view_grp.get("전환", 0), 0.0)
                view_grp["ROAS(%)"] = np.where(view_grp.get("광고비", 0) > 0, (view_grp.get("전환매출", 0) / view_grp.get("광고비", 0)) * 100, 0.0)
                        
                c1, c2 = st.columns([1, 3])
                with c1:
                    if not view_grp.empty and "캠페인" in view_grp.columns and "광고그룹" in view_grp.columns:
                        view_grp["_filter_label"] = view_grp["캠페인"].astype(str) + " > " + view_grp["광고그룹"].astype(str)
                        grps = ["전체"] + sorted([str(x) for x in view_grp["_filter_label"].unique() if str(x).strip()])
                    else:
                        grps = ["전체"]
                    sel_grp = st.selectbox("🎯 개별 광고그룹 검색/필터", grps, key="grp_name_filter_main")

                if sel_grp != "전체": view_grp = view_grp[view_grp["_filter_label"] == sel_grp]
                        
                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                metrics_cols_grp = ["노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                
                final_cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in view_grp.columns]
                disp_grp = view_grp[final_cols_grp].sort_values(by="광고비" if "광고비" in view_grp.columns else final_cols_grp[0], ascending=False).head(top_n)
                
                styled_disp_grp = disp_grp.style.format(fmt)
                st.markdown("#### 📊 광고그룹별 종합 성과 표")
                render_big_table(styled_disp_grp, "pl_grp_grid_main", 500)
            else:
                st.info("파워링크 그룹 데이터가 없습니다.")
            
    with tab_shop:
        st.markdown("### 🛒 쇼핑검색 (상품/일반소재)")
        shop_ad_bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=10000, top_k=50)
        
        if shop_ad_bundle is not None and not shop_ad_bundle.empty:
            shop_ad_df = _perf_common_merge_meta(shop_ad_bundle, meta)
            view_shop = shop_ad_df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "상품/소재명", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()

            if "캠페인유형" not in view_shop.columns and "campaign_type" in view_shop.columns: view_shop["캠페인유형"] = view_shop["campaign_type"]

            is_shopping = view_shop["캠페인유형"] == "쇼핑검색"
            is_not_ext = ~view_shop["상품/소재명"].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)
            view_shop = view_shop[is_shopping & is_not_ext].copy()

            if not view_shop.empty:
                for c in ["노출", "클릭", "광고비", "전환", "전환매출"]: view_shop[c] = pd.to_numeric(view_shop.get(c, 0), errors="coerce").fillna(0)
                view_shop["CTR(%)"] = np.where(view_shop["노출"] > 0, (view_shop["클릭"] / view_shop["노출"]) * 100, 0.0)
                view_shop["CPC(원)"] = np.where(view_shop["클릭"] > 0, view_shop["광고비"] / view_shop["클릭"], 0.0)
                view_shop["CPA(원)"] = np.where(view_shop["전환"] > 0, view_shop["광고비"] / view_shop["전환"], 0.0)
                view_shop["ROAS(%)"] = np.where(view_shop["광고비"] > 0, (view_shop["전환매출"] / view_shop["광고비"]) * 100, 0.0)

                metrics_cols_shop = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]

                c1, c2 = st.columns([1, 3])
                with c1:
                    if "캠페인" in view_shop.columns and "광고그룹" in view_shop.columns and "상품/소재명" in view_shop.columns:
                        view_shop["_filter_label"] = view_shop["캠페인"].astype(str) + " > " + view_shop["광고그룹"].astype(str) + " > " + view_shop["상품/소재명"].astype(str)
                        items = ["전체"] + sorted([str(x) for x in view_shop["_filter_label"].unique() if str(x).strip()])
                    else:
                        items = ["전체"]
                    sel_item = st.selectbox("🎯 개별 상품/소재 검색/필터", items, key="shop_item_filter_main")

                if sel_item != "전체": view_shop = view_shop[view_shop["_filter_label"] == sel_item]

                base_cols_shop = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "상품/소재명"]
                final_cols_shop = [c for c in base_cols_shop + metrics_cols_shop if c in view_shop.columns]
                
                disp_shop = view_shop[final_cols_shop].sort_values("광고비", ascending=False).head(top_n)
                styled_disp_shop = disp_shop.style.format(fmt)

                st.markdown("#### 📊 상품/소재별 상세 성과 표")
                render_big_table(styled_disp_shop, "shop_general_grid_main", 500)
            else:
                st.info("해당 기간의 쇼핑검색 일반소재(상품) 데이터가 없습니다.")
        else:
            st.info("해당 기간의 쇼핑검색 데이터가 없습니다.")

    # ✨ [핵심 조치] 비교 분석 탭 통합 구축
    with tab_cmp:
        st.markdown("### ⚖️ 기간 비교 분석")
        cmp_view_mode = st.radio("비교 대상 선택", ["🔑 파워링크 - 키워드 단위", "📂 파워링크 - 광고그룹 단위", "🛒 쇼핑검색 - 상품 단위"], horizontal=True)
        
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("📊 기간 비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="kw_cmp_mode")
        
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

        if cmp_view_mode == "🔑 파워링크 - 키워드 단위":
            if df_pl_raw.empty:
                st.info("비교할 파워링크 데이터가 없습니다.")
            else:
                base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=20000)
                view = _perf_common_merge_meta(df_pl_raw.sort_values("cost", ascending=False).head(top_n), meta)
                view = view.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()
                for c in ["광고비", "전환매출", "노출", "클릭", "전환"]: view[c] = pd.to_numeric(view.get(c,0), errors="coerce").fillna(0)
                view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
                view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
                view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
                view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
                
                base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드"]
                metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                
                if not base_kw_bundle.empty:
                    valid_keys = [k for k in ['customer_id', 'keyword_id'] if k in view.columns and k in base_kw_bundle.columns]
                    if valid_keys:
                        view = append_comparison_data(view, base_kw_bundle, valid_keys)
                        metrics_cols.extend(["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"])

                c1, c2 = st.columns([1, 3])
                with c1:
                    view["_filter_label"] = view["캠페인"].astype(str) + " > " + view["광고그룹"].astype(str) + " > " + view["키워드"].astype(str)
                    kws = ["전체"] + sorted([str(x) for x in view["_filter_label"].unique() if str(x).strip()])
                    sel_kw = st.selectbox("🎯 개별 검색/필터", kws, key="kw_cmp_filter_pl_kw")

                if sel_kw != "전체":
                    view = view[view["_filter_label"] == sel_kw]
                    if not view.empty: render_comparison_section(view, cmp_mode, b1, b2, f["start"], f["end"], "선택 항목 상세 비교")

                disp = view[[c for c in base_cols + metrics_cols if c in view.columns]].copy()
                st.markdown("#### 📊 파워링크 키워드 기간 비교 표")
                render_big_table(disp.style.format(fmt), "pl_cmp_kw", 500)

        elif cmp_view_mode == "📂 파워링크 - 광고그룹 단위":
            if df_pl_raw.empty:
                st.info("비교할 파워링크 그룹 데이터가 없습니다.")
            else:
                grp_cols = [c for c in ['customer_id', 'campaign_type_label', 'campaign_name', 'adgroup_id', 'adgroup_name'] if c in df_pl_raw.columns]
                val_cols = [c for c in ['imp', 'clk', 'cost', 'conv', 'sales'] if c in df_pl_raw.columns]
                grp_cur = df_pl_raw.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp_cur = _perf_common_merge_meta(grp_cur, meta)
                view = grp_cur.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()
                for c in ["광고비", "전환매출", "노출", "클릭", "전환"]: view[c] = pd.to_numeric(view.get(c,0), errors="coerce").fillna(0)
                view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
                view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
                view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
                view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
                
                base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=20000)
                base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                
                if not base_kw_bundle.empty:
                    valid_keys = [k for k in ['customer_id', 'adgroup_id'] if k in view.columns and k in base_kw_bundle.columns]
                    if valid_keys:
                        view = append_comparison_data(view, base_kw_bundle, valid_keys)
                        metrics_cols.extend(["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"])

                c1, c2 = st.columns([1, 3])
                with c1:
                    view["_filter_label"] = view["캠페인"].astype(str) + " > " + view["광고그룹"].astype(str)
                    kws = ["전체"] + sorted([str(x) for x in view["_filter_label"].unique() if str(x).strip()])
                    sel_kw = st.selectbox("🎯 개별 검색/필터", kws, key="kw_cmp_filter_pl_grp")

                if sel_kw != "전체":
                    view = view[view["_filter_label"] == sel_kw]
                    if not view.empty: render_comparison_section(view, cmp_mode, b1, b2, f["start"], f["end"], "선택 항목 상세 비교")

                disp = view[[c for c in base_cols + metrics_cols if c in view.columns]].sort_values("광고비", ascending=False).head(top_n).copy()
                st.markdown("#### 📊 파워링크 그룹 기간 비교 표")
                render_big_table(disp.style.format(fmt), "pl_cmp_grp", 500)
                
        elif cmp_view_mode == "🛒 쇼핑검색 - 상품 단위":
            shop_ad_bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=10000, top_k=50)
            if shop_ad_bundle is None or shop_ad_bundle.empty:
                st.info("비교할 데이터가 없습니다.")
            else:
                base_shop_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000, top_k=50)
                shop_ad_df = _perf_common_merge_meta(shop_ad_bundle, meta)
                view = shop_ad_df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "상품/소재명", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}).copy()
                if "캠페인유형" not in view.columns and "campaign_type" in view.columns: view["캠페인유형"] = view["campaign_type"]
                is_shopping = view["캠페인유형"] == "쇼핑검색"
                is_not_ext = ~view["상품/소재명"].astype(str).str.contains(r'\[확장소재\]', na=False, regex=True)
                view = view[is_shopping & is_not_ext].copy()
                
                if view.empty:
                    st.info("비교할 일반 상품 데이터가 없습니다.")
                else:
                    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]: view[c] = pd.to_numeric(view.get(c,0), errors="coerce").fillna(0)
                    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
                    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
                    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
                    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
                    
                    base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "상품/소재명"]
                    metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                    
                    if base_shop_bundle is not None and not base_shop_bundle.empty:
                        valid_keys = [k for k in ['customer_id', 'ad_id'] if k in view.columns and k in base_shop_bundle.columns]
                        if valid_keys:
                            view = append_comparison_data(view, base_shop_bundle, valid_keys)
                            metrics_cols.extend(["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"])
                            
                    c1, c2 = st.columns([1, 3])
                    with c1:
                        if "캠페인" in view.columns and "광고그룹" in view.columns and "상품/소재명" in view.columns:
                            view["_filter_label"] = view["캠페인"].astype(str) + " > " + view["광고그룹"].astype(str) + " > " + view["상품/소재명"].astype(str)
                            items = ["전체"] + sorted([str(x) for x in view["_filter_label"].unique() if str(x).strip()])
                        else:
                            items = ["전체"]
                        sel_kw = st.selectbox("🎯 개별 검색/필터", items, key="kw_cmp_filter_shop")

                    if sel_kw != "전체":
                        view = view[view["_filter_label"] == sel_kw]
                        if not view.empty: render_comparison_section(view, cmp_mode, b1, b2, f["start"], f["end"], "선택 항목 상세 비교")

                    disp = view[[c for c in base_cols + metrics_cols if c in view.columns]].sort_values("광고비", ascending=False).head(top_n).copy()
                    st.markdown("#### 📊 쇼핑검색 상품 기간 비교 표")
                    render_big_table(disp.style.format(fmt), "shop_cmp_grid", 500)

    with tab_neg:
        st.markdown("### 💸 저효율 등록 키워드 발굴기")
        st.caption("내가 등록하여 입찰 중인 키워드 중에서 클릭(비용)은 지속적으로 발생하지만 전환이 전혀 없는 키워드 목록입니다. **네이버 광고 시스템에서 입찰가를 낮추거나 OFF 상태로 변경할 것**을 강력히 권장합니다.")
        
        if df_pl_raw.empty:
            st.info("데이터가 부족하여 저효율 키워드를 분석할 수 없습니다.")
        else:
            leak_view = df_pl_raw.rename(columns={
                "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", 
                "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환"
            }).copy()
            
            for c in ["노출", "클릭", "광고비", "전환"]:
                leak_view[c] = pd.to_numeric(leak_view[c], errors="coerce").fillna(0)
            
            leak_df = leak_view[leak_view["전환"] == 0].copy()
            
            c1, c2 = st.columns([1, 2])
            with c1:
                min_leak_cost = st.slider("최소 누수 비용 (원)", 5000, 100000, 20000, 5000, help="이 금액 이상 소진되었으나 전환이 0건인 키워드를 찾습니다.")
            
            target_leak = leak_df[leak_df["광고비"] >= min_leak_cost].sort_values("광고비", ascending=False)
            
            if target_leak.empty:
                st.success(f"🎉 현재 기준(비용 {format_currency(min_leak_cost)} 이상, 전환 0)에 해당하는 비용 누수 키워드가 없습니다!")
            else:
                target_leak["CTR(%)"] = np.where(target_leak["노출"] > 0, (target_leak["클릭"] / target_leak["노출"]) * 100, 0.0)
                st.warning(f"🚨 총 **{len(target_leak)}개**의 등록 키워드에서 심각한 비용 누수가 발견되었습니다! 네이버에서 입찰가를 조절하세요.")
                
                disp_leak = target_leak[["캠페인", "광고그룹", "키워드", "노출", "클릭", "광고비", "CTR(%)"]].copy()
                
                fmt_leak = {}
                if "노출" in disp_leak.columns: fmt_leak["노출"] = "{:,.0f}"
                if "클릭" in disp_leak.columns: fmt_leak["클릭"] = "{:,.0f}"
                if "광고비" in disp_leak.columns: fmt_leak["광고비"] = "{:,.0f}"
                if "CTR(%)" in disp_leak.columns: fmt_leak["CTR(%)"] = "{:,.2f}%"
                styled_disp_leak = disp_leak.style.format(fmt_leak)
                
                render_big_table(styled_disp_leak, key="leak_keyword_grid", height=400)
