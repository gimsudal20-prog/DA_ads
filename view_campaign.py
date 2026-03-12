# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict

from data import query_campaign_bundle, query_keyword_bundle, query_campaign_off_log, load_dim_campaign
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, append_comparison_data, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas

def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"

def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view

def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in out.columns:
            out[k] = out[k].astype(str)
    return out

def _keyword_rank_by_keys(kw_bundle: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if kw_bundle is None or kw_bundle.empty or "avg_rank" not in kw_bundle.columns:
        return pd.DataFrame(columns=keys + ["avg_rank"])
    tmp = kw_bundle.copy()
    tmp["imp"] = pd.to_numeric(tmp.get("imp", 0), errors="coerce").fillna(0.0)
    tmp["avg_rank"] = pd.to_numeric(tmp.get("avg_rank", np.nan), errors="coerce")
    tmp["_rank_imp"] = tmp["avg_rank"].fillna(0.0) * tmp["imp"]
    grp = tmp.groupby(keys, as_index=False)[["_rank_imp", "imp"]].sum()
    grp["avg_rank"] = np.where(grp["imp"] > 0, grp["_rank_imp"] / grp["imp"], np.nan)
    return grp[keys + ["avg_rank"]]

def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    
    for k in merge_keys:
        if k in view_df.columns:
            view_df[k] = view_df[k].astype(str).str.replace(r'\.0$', '', regex=True)
        if k in base_df.columns:
            base_df[k] = base_df[k].astype(str).str.replace(r'\.0$', '', regex=True)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
    if 'avg_rank' in base_df.columns:
        agg_dict['avg_rank'] = 'mean'
        
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales', 'avg_rank': 'b_avg_rank'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = merged[c].fillna(0)
        
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

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return
    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
    kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)

    view = pd.DataFrame()
    if bundle is not None and not bundle.empty:
        df = _perf_common_merge_meta(bundle, meta)
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
            "campaign_name": "캠페인", "imp": "노출", "clk": "클릭",
            "cost": "광고비", "conv": "전환", "sales": "전환매출"
        }).copy()
        view = _add_perf_metrics(view)

        if not kw_bundle_cur.empty:
            rank_map_camp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id"])
            if not rank_map_camp.empty:
                key_cols = ["customer_id", "campaign_id"]
                view = _normalize_merge_keys(view, key_cols)
                rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
                if "avg_rank" in view.columns:
                    view = view.drop(columns=["avg_rank"])
                view = view.merge(rank_map_camp, on=key_cols, how="left")
                
        if "avg_rank" in view.columns:
            view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    tab_main, tab_group, tab_cmp, tab_history = st.tabs(["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"])
    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"
    }

    with tab_main:
        if view.empty:
            st.info("해당 기간의 캠페인 성과 데이터가 없습니다.")
        else:
            camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
            sel_camp_main = st.selectbox("캠페인 검색", camps_main, key="camp_name_filter_main")

            disp_main = view.copy()
            if sel_camp_main != "전체":
                disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

            base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
            if "평균순위" in disp_main.columns:
                base_cols.append("평균순위")
            metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
            final_cols = [c for c in base_cols + metrics_cols if c in disp_main.columns]
            disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n)

            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>캠페인 종합 성과 데이터</div>", unsafe_allow_html=True)
            render_big_table(disp_main.style.format(fmt), "camp_grid_main", 550)

    with tab_group:
        if kw_bundle_cur is None or kw_bundle_cur.empty:
            st.info("광고그룹 성과 데이터가 없습니다.")
        else:
            grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in kw_bundle_cur.columns]
            val_cols = [c for c in ["imp", "clk", "cost", "conv", "sales"] if c in kw_bundle_cur.columns]
            if not grp_cols or not val_cols:
                st.info("광고그룹 성과 데이터가 없습니다.")
            else:
                grp = kw_bundle_cur.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp = _perf_common_merge_meta(grp, meta)
                grouped = grp.rename(columns={
                    "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인",
                    "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
                }).copy()

                rank_map_grp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id", "adgroup_id"])
                if not rank_map_grp.empty:
                    key_cols_grp = ["customer_id", "campaign_id", "adgroup_id"]
                    grouped = _normalize_merge_keys(grouped, key_cols_grp)
                    rank_map_grp = _normalize_merge_keys(rank_map_grp, key_cols_grp)
                    if "avg_rank" in grouped.columns:
                        grouped = grouped.drop(columns=["avg_rank"])
                    grouped = grouped.merge(rank_map_grp, on=key_cols_grp, how="left")
                    grouped["평균순위"] = grouped["avg_rank"].apply(_format_avg_rank)

                camps = ["전체"] + sorted([str(x) for x in grouped["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in grouped.columns else ["전체"]
                sel_camp = st.selectbox("캠페인 필터", camps, key="camp_group_filter")
                if sel_camp != "전체" and "캠페인" in grouped.columns:
                    grouped = grouped[grouped["캠페인"] == sel_camp]

                grouped = _add_perf_metrics(grouped)
                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                if "평균순위" in grouped.columns:
                    base_cols_grp.append("평균순위")
                metrics_cols_grp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in grouped.columns]
                disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n)

                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>광고그룹별 성과 데이터</div>", unsafe_allow_html=True)
                render_big_table(disp_grp.style.format(fmt), "camp_group_grid", 550)

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

        cmp_view_mode = st.radio("비교 대상 선택", ["캠페인 단위", "광고그룹 단위"], horizontal=True, key="camp_view_mode_cmp")
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")

        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=50000)

        if cmp_view_mode == "캠페인 단위":
            if view.empty:
                st.info("현재 기간의 캠페인 데이터가 없습니다.")
            else:
                base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)

                if not base_bundle.empty and not base_kw_bundle.empty:
                    base_rank_map = _keyword_rank_by_keys(base_kw_bundle, ["customer_id", "campaign_id"])
                    if not base_rank_map.empty:
                        key_cols_cmp = ["customer_id", "campaign_id"]
                        base_bundle = _normalize_merge_keys(base_bundle, key_cols_cmp)
                        base_rank_map = _normalize_merge_keys(base_rank_map, key_cols_cmp)
                        if "avg_rank" in base_bundle.columns:
                            base_bundle = base_bundle.drop(columns=["avg_rank"])
                        base_bundle = base_bundle.merge(base_rank_map, on=key_cols_cmp, how="left")

                view_cmp = view.copy()
                if not base_bundle.empty:
                    valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
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

                base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인"]
                if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
                    base_cols_cmp.extend(["평균순위", "이전 평균순위", "순위 변화"])

                base_for_search = base_bundle.rename(columns={"campaign_name": "캠페인"}) if not base_bundle.empty else pd.DataFrame()
                render_item_comparison_search("캠페인", view_cmp, base_for_search, "캠페인", f["start"], f["end"], b1, b2)

                final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
                disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()

                styled_cmp = disp_cmp.style.format(fmt_cmp)
                delta_cols = [c for c in ["노출 증감(%)", "노출 증감", "클릭 증감(%)", "클릭 증감", "광고비 증감(%)", "광고비 증감", "CPC 증감(%)", "CPC 증감", "순위 변화", "전환 증감", "ROAS 증감(%)"] if c in disp_cmp.columns]
                if delta_cols:
                    try: styled_cmp = styled_cmp.map(style_table_deltas, subset=delta_cols)
                    except AttributeError: styled_cmp = styled_cmp.applymap(style_table_deltas, subset=delta_cols)

                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>캠페인 기간 비교 표</div>", unsafe_allow_html=True)
                render_big_table(styled_cmp, "camp_cmp_grid", 550)

        elif cmp_view_mode == "광고그룹 단위":
            if kw_bundle_cur is None or kw_bundle_cur.empty:
                st.info("비교할 광고그룹 데이터가 없습니다.")
            else:
                grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in kw_bundle_cur.columns]
                val_cols = [c for c in ["imp", "clk", "cost", "conv", "sales"] if c in kw_bundle_cur.columns]
                
                agg_dict_cur = {col: 'sum' for col in val_cols}
                grp_cur = kw_bundle_cur.groupby(grp_cols, as_index=False).agg(agg_dict_cur)
                grp_cur = _perf_common_merge_meta(grp_cur, meta)
                
                rank_map_grp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id", "adgroup_id"])
                if not rank_map_grp.empty:
                    key_cols_grp = ["customer_id", "campaign_id", "adgroup_id"]
                    grp_cur = _normalize_merge_keys(grp_cur, key_cols_grp)
                    rank_map_grp = _normalize_merge_keys(rank_map_grp, key_cols_grp)
                    if "avg_rank" in grp_cur.columns:
                        grp_cur = grp_cur.drop(columns=["avg_rank"])
                    grp_cur = grp_cur.merge(rank_map_grp, on=key_cols_grp, how="left")

                view_grp = grp_cur.rename(columns={
                    "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인",
                    "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
                }).copy()
                
                for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
                    view_grp[c] = pd.to_numeric(view_grp.get(c,0), errors="coerce").fillna(0)
                view_grp["CTR(%)"] = np.where(view_grp["노출"] > 0, (view_grp["클릭"] / view_grp["노출"]) * 100, 0.0)
                view_grp["CPC(원)"] = np.where(view_grp["클릭"] > 0, view_grp["광고비"] / view_grp["클릭"], 0.0)
                view_grp["CPA(원)"] = np.where(view_grp["전환"] > 0, view_grp["광고비"] / view_grp["전환"], 0.0)
                view_grp["ROAS(%)"] = np.where(view_grp["광고비"] > 0, (view_grp["전환매출"] / view_grp["광고비"]) * 100, 0.0)

                if not base_kw_bundle.empty:
                    b_grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id"] if c in base_kw_bundle.columns]
                    b_val_cols = [c for c in ["imp", "clk", "cost", "conv", "sales"] if c in base_kw_bundle.columns]
                    b_agg_dict = {col: 'sum' for col in b_val_cols}
                    base_grp_df = base_kw_bundle.groupby(b_grp_cols, as_index=False).agg(b_agg_dict)
                    
                    b_rank_map = _keyword_rank_by_keys(base_kw_bundle, ["customer_id", "campaign_id", "adgroup_id"])
                    if not b_rank_map.empty:
                        base_grp_df = _normalize_merge_keys(base_grp_df, b_grp_cols)
                        b_rank_map = _normalize_merge_keys(b_rank_map, b_grp_cols)
                        if "avg_rank" in base_grp_df.columns:
                            base_grp_df = base_grp_df.drop(columns=["avg_rank"])
                        base_grp_df = base_grp_df.merge(b_rank_map, on=b_grp_cols, how="left")
                else:
                    base_grp_df = pd.DataFrame()

                valid_keys = [k for k in ['customer_id', 'adgroup_id'] if k in view_grp.columns and k in base_grp_df.columns]
                if valid_keys:
                    view_grp = _apply_comparison_metrics(view_grp, base_grp_df, valid_keys)
                else:
                    view_grp = _apply_comparison_metrics(view_grp, pd.DataFrame(), [])
                    
                metrics_cols_grp = [
                    "노출", "이전 노출", "노출 증감", "노출 증감(%)",
                    "클릭", "이전 클릭", "클릭 증감", "클릭 증감(%)",
                    "광고비", "이전 광고비", "광고비 증감", "광고비 증감(%)",
                    "CPC(원)", "이전 CPC(원)", "CPC 증감", "CPC 증감(%)",
                    "전환", "이전 전환", "전환 증감", 
                    "CPA(원)", "전환매출", "이전 ROAS(%)", "ROAS(%)", "ROAS 증감(%)"
                ]

                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                if "avg_rank" in view_grp.columns or "평균순위" in view_grp.columns:
                    base_cols_grp.extend(["평균순위", "이전 평균순위", "순위 변화"])

                base_for_search = base_kw_bundle.rename(columns={"adgroup_name": "광고그룹"}) if not base_kw_bundle.empty else pd.DataFrame()
                render_item_comparison_search("광고그룹", view_grp, base_for_search, "광고그룹", f["start"], f["end"], b1, b2)

                final_cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in view_grp.columns]
                disp_grp = view_grp[final_cols_grp].sort_values("광고비", ascending=False).head(top_n).copy()

                styled_cmp_grp = disp_grp.style.format(fmt_cmp)
                delta_cols = [c for c in ["노출 증감(%)", "노출 증감", "클릭 증감(%)", "클릭 증감", "광고비 증감(%)", "광고비 증감", "CPC 증감(%)", "CPC 증감", "순위 변화", "전환 증감", "ROAS 증감(%)"] if c in disp_grp.columns]
                if delta_cols:
                    try: styled_cmp_grp = styled_cmp_grp.map(style_table_deltas, subset=delta_cols)
                    except AttributeError: styled_cmp_grp = styled_cmp_grp.applymap(style_table_deltas, subset=delta_cols)

                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>광고그룹 기간 비교 표</div>", unsafe_allow_html=True)
                render_big_table(styled_cmp_grp, "camp_grp_cmp_grid", 550)

    with tab_history:
        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty:
            st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        else:
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty and "campaign_id" in dim_camp.columns and "campaign_name" in dim_camp.columns:
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                off_log["campaign_id"] = off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
                # NaN 값을 안전하게 처리
                off_log["campaign_name"] = off_log["campaign_name"].fillna(off_log["campaign_id"])
            else:
                if "campaign_name" not in off_log.columns:
                    off_log["campaign_name"] = off_log["campaign_id"]
                
            if not meta.empty and "customer_id" in meta.columns and "account_name" in meta.columns:
                meta_copy = meta.copy()
                meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
                off_log["customer_id"] = off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
                # NaN 값을 안전하게 처리 (데이터 실종 방지)
                off_log["account_name"] = off_log["account_name"].fillna(off_log["customer_id"])
            else:
                if "account_name" not in off_log.columns:
                    off_log["account_name"] = off_log["customer_id"]
            
            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            
            st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
            accounts_list = ["전체"] + sorted([str(x) for x in off_log["account_name"].unique() if str(x).strip()])
            
            # 사이드바에서 선택된 광고주가 있으면 자동으로 기본 선택되도록 설정
            default_idx = 0
            if cids and not meta.empty:
                selected_acc_names = meta[meta['customer_id'].astype(str).isin([str(x) for x in cids])]['account_name'].dropna().unique()
                if len(selected_acc_names) > 0 and selected_acc_names[0] in accounts_list:
                    default_idx = accounts_list.index(selected_acc_names[0])
                    
            sel_acc = st.selectbox("🔍 업체 검색 (광고주 선택)", accounts_list, index=default_idx, key="history_acc_filter")
            
            filtered_log = off_log if sel_acc == "전체" else off_log[off_log["account_name"] == sel_acc]
            
            if filtered_log.empty:
                st.info(f"[{sel_acc}] 업체의 꺼짐 기록이 없습니다.")
            else:
                pivot_df = filtered_log.pivot_table(
                    index=["account_name", "campaign_name"], 
                    columns="dt_str", 
                    values="off_time", 
                    aggfunc='first'
                ).reset_index()
                
                pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명"}).fillna("-")
                
                st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>일자별 꺼짐 기록 ({sel_acc})</div>", unsafe_allow_html=True)
                st.dataframe(pivot_df, use_container_width=True, hide_index=True)
