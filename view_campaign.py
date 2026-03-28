# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view (Rank Delta Toggle & Integer Format Fixed)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from typing import Dict
from datetime import date

from data import (
    query_campaign_bundle,
    query_keyword_bundle,
    query_ad_bundle,
    query_campaign_off_log,
    load_dim_campaign,
    sql_read,
    table_exists,
    get_table_columns,
    _sql_in_str_list,
)
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta

FMT_DICT = {
    "노출": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
    "클릭": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
    "CTR(%)": "{:,.2f}%", 
    "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
    "CPC(원)": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
    "구매완료수": "{:,.0f}", "구매 증감": "{:+.1f}%", "구매 차이": "{:+,.0f}",
    "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.1f}%", "구매 매출 차이": "{:+,.0f}원",
    "구매 ROAS(%)": "{:,.1f}%", "구매 ROAS 증감": "{:+.1f}%",
    "총 전환수": "{:,.0f}", "총 전환 증감": "{:+.1f}%", "총 전환 차이": "{:+,.0f}",
    "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.1f}%", "총 매출 차이": "{:+,.0f}원",
    "통합 ROAS(%)": "{:,.1f}%", "통합 ROAS 증감": "{:+.1f}%",
    "장바구니수": "{:,.0f}", "장바구니 증감": "{:+.1f}%", "장바구니 차이": "{:+,.0f}",
    "위시리스트수": "{:,.0f}", "위시리스트 증감": "{:+.1f}%", "위시리스트 차이": "{:+,.0f}",
    "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
}

def _style_delta_numeric(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #1A73E8; font-weight: 700;' if v > 0 else 'color: #EA4335; font-weight: 700;'

def _style_delta_numeric_neg(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #EA4335; font-weight: 700;' if v > 0 else 'color: #1A73E8; font-weight: 700;'

def _apply_delta_styles(styler, df: pd.DataFrame):
    pos_cols = [c for c in ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '장바구니 증감', '장바구니 차이', '위시리스트 증감', '위시리스트 차이', '구매 증감', '구매 차이', '구매 매출 증감', '구매 매출 차이', '구매 ROAS 증감', '총 전환 증감', '총 전환 차이', '총 매출 증감', '총 매출 차이', '통합 ROAS 증감'] if c in df.columns]
    neg_cols = [c for c in ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이', '순위 변화'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.map(_style_delta_numeric_neg, subset=neg_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_cols)
    return styler

def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0: return "미수집"
    return f"{num:.0f}위"

def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "구매완료 매출", "장바구니 매출액", "위시리스트 매출액", "노출", "클릭", "구매완료수", "장바구니수", "위시리스트수", "tot_conv", "tot_sales"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    if "tot_conv" in view.columns:
        view["총 전환수"] = view["tot_conv"]
        view["총 전환매출"] = view["tot_sales"]
    else:
        view["총 전환수"] = view.get("구매완료수", 0) + view.get("장바구니수", 0) + view.get("위시리스트수", 0)
        view["총 전환매출"] = view.get("구매완료 매출", 0) + view.get("장바구니 매출액", 0) + view.get("위시리스트 매출액", 0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["구매 ROAS(%)"] = np.where(view["광고비"] > 0, (view["구매완료 매출"] / view["광고비"]) * 100, 0.0)
    view["통합 ROAS(%)"] = np.where(view["광고비"] > 0, (view["총 전환매출"] / view["광고비"]) * 100, 0.0)
    return view

def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df

    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)

    val_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_conv', 'tot_sales']
    for c in val_cols:
        if c in base_df.columns: base_df[c] = pd.to_numeric(base_df[c], errors='coerce').fillna(0)

    agg_dict = {c: 'sum' for c in val_cols if c in base_df.columns}
    if 'avg_rank' in base_df.columns:
        agg_dict['avg_rank'] = 'mean'
        base_df['avg_rank'] = pd.to_numeric(base_df['avg_rank'], errors='coerce')

    if not base_df.empty and merge_keys:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={c: f"b_{c}" for c in agg_dict.keys()})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()

    for c in val_cols:
        bc = f"b_{c}"
        if bc not in merged.columns: merged[bc] = 0
        merged[bc] = pd.to_numeric(merged[bc], errors='coerce').fillna(0)

    if 'b_avg_rank' not in merged.columns: merged['b_avg_rank'] = np.nan

    def _vec_pct_diff(c, b):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        return pct, diff

    c_imp, b_imp = merged.get('노출', 0), merged.get('b_imp', 0)
    c_clk, b_clk = merged.get('클릭', 0), merged.get('b_clk', 0)
    c_cost, b_cost = merged.get('광고비', 0), merged.get('b_cost', 0)
    c_cpc = np.where(c_clk > 0, c_cost / c_clk, 0)
    b_cpc = np.where(b_clk > 0, b_cost / b_clk, 0)

    merged['노출 증감'], merged['노출 차이'] = _vec_pct_diff(c_imp, b_imp)
    merged['클릭 증감'], merged['클릭 차이'] = _vec_pct_diff(c_clk, b_clk)
    merged['광고비 증감'], merged['광고비 차이'] = _vec_pct_diff(c_cost, b_cost)
    merged['CPC 증감'], merged['CPC 차이'] = _vec_pct_diff(c_cpc, b_cpc)

    c_cart, b_cart = merged.get('장바구니수', 0), merged.get('b_cart_conv', 0)
    c_wish, b_wish = merged.get('위시리스트수', 0), merged.get('b_wishlist_conv', 0)
    c_conv, b_conv = merged.get('구매완료수', 0), merged.get('b_conv', 0)
    c_sales, b_sales = merged.get('구매완료 매출', 0), merged.get('b_sales', 0)
    c_tconv, b_tconv = merged.get('총 전환수', 0), merged.get('b_tot_conv', merged.get('b_conv', 0) + merged.get('b_cart_conv', 0) + merged.get('b_wishlist_conv', 0))
    c_tsales, b_tsales = merged.get('총 전환매출', 0), merged.get('b_tot_sales', merged.get('b_sales', 0) + merged.get('b_cart_sales', 0) + merged.get('b_wishlist_sales', 0))

    merged['장바구니 증감'], merged['장바구니 차이'] = _vec_pct_diff(c_cart, b_cart)
    merged['위시리스트 증감'], merged['위시리스트 차이'] = _vec_pct_diff(c_wish, b_wish)
    merged['구매 증감'], merged['구매 차이'] = _vec_pct_diff(c_conv, b_conv)
    merged['구매 매출 증감'], merged['구매 매출 차이'] = _vec_pct_diff(c_sales, b_sales)
    merged['총 전환 증감'], merged['총 전환 차이'] = _vec_pct_diff(c_tconv, b_tconv)
    merged['총 매출 증감'], merged['총 매출 차이'] = _vec_pct_diff(c_tsales, b_tsales)

    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    b_roas = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)
    merged['구매 ROAS 증감'] = c_roas - b_roas

    c_troas = np.where(c_cost > 0, (c_tsales / c_cost) * 100, 0)
    b_troas = np.where(b_cost > 0, (b_tsales / b_cost) * 100, 0)
    merged['통합 ROAS 증감'] = c_troas - b_troas

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns: merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)

    return merged

def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in out.columns: out[k] = out[k].astype(str)
    return out

def _keyword_rank_by_keys(detail_bundle: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if detail_bundle is None or detail_bundle.empty or "avg_rank" not in detail_bundle.columns:
        return pd.DataFrame(columns=keys + ["avg_rank"])
    tmp = detail_bundle.copy()
    tmp["imp"] = pd.to_numeric(tmp.get("imp", 0), errors="coerce").fillna(0.0)
    tmp["avg_rank"] = pd.to_numeric(tmp.get("avg_rank", np.nan), errors="coerce")
    tmp["_rank_imp"] = tmp["avg_rank"].fillna(0.0) * tmp["imp"]
    grp = tmp.groupby(keys, as_index=False)[["_rank_imp", "imp"]].sum()
    grp["avg_rank"] = np.where(grp["imp"] > 0, grp["_rank_imp"] / grp["imp"], np.nan)
    return grp[keys + ["avg_rank"]]

def _compact_df_height(df: pd.DataFrame, min_height: int = 72, max_height: int = 260) -> int:
    try:
        rows = len(df.index)
        if rows <= 0: return min_height
        if rows == 1: return 74
        if rows == 2: return 108
        return max(min_height, min(40 + rows * 34, max_height))
    except: return min_height

def _query_device_breakdown(engine, d1, d2, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if table_exists(engine, "fact_campaign_device_daily"):
        fact_cols = get_table_columns(engine, "fact_campaign_device_daily")
        if "device_name" in fact_cols:
            where_cid = f"AND f.customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
            join_sql = ""
            type_filter = ""
            if type_sel and table_exists(engine, "dim_campaign"):
                dim_cols = get_table_columns(engine, "dim_campaign")
                cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
                rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENTS", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
                db_types = [rev_map.get(t, t) for t in type_sel]
                join_sql = f" LEFT JOIN dim_campaign dc ON f.customer_id::text = dc.customer_id::text AND f.campaign_id::text = dc.campaign_id::text "
                type_filter = f"AND dc.{cp_col} IN ({_sql_in_str_list(list(db_types))})"
            sql = f"""
                SELECT COALESCE(NULLIF(TRIM(f.device_name), ''), '미분류') AS device_name,
                       SUM(f.cost) AS cost
                FROM fact_campaign_device_daily f
                {join_sql}
                WHERE f.dt BETWEEN :d1 AND :d2
                  {where_cid}
                  {type_filter}
                GROUP BY COALESCE(NULLIF(TRIM(f.device_name), ''), '미분류')
                HAVING SUM(f.cost) > 0
                ORDER BY SUM(f.cost) DESC
            """
            try:
                df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
                if df is not None and not df.empty:
                    return df
            except Exception:
                pass

    if not table_exists(engine, "fact_media_daily"): return pd.DataFrame()
    cols = get_table_columns(engine, "fact_media_daily")
    if "device_name" not in cols: return pd.DataFrame()

    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    type_filter = f"AND campaign_type IN ({_sql_in_str_list(list(type_sel))})" if type_sel and "campaign_type" in cols else ""
    sql = f"SELECT COALESCE(NULLIF(TRIM(device_name), ''), '미분류') AS device_name, SUM(cost) AS cost FROM fact_media_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} {type_filter} GROUP BY COALESCE(NULLIF(TRIM(device_name), ''), '미분류') HAVING SUM(cost) > 0 ORDER BY SUM(cost) DESC"
    try: return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
    except: return pd.DataFrame()

FAST_COL_CONFIG = {
    "노출": st.column_config.NumberColumn("노출", format="%d"),
    "클릭": st.column_config.NumberColumn("클릭", format="%d"),
    "광고비": st.column_config.NumberColumn("광고비", format="%d 원"),
    "CPC(원)": st.column_config.NumberColumn("CPC(원)", format="%d 원"),
    "CTR(%)": st.column_config.NumberColumn("CTR(%)", format="%.1f %%"),
    "구매완료수": st.column_config.NumberColumn("구매완료수", format="%d"),
    "구매완료 매출": st.column_config.NumberColumn("구매완료 매출", format="%d 원"),
    "구매 ROAS(%)": st.column_config.NumberColumn("구매 ROAS(%)", format="%.1f %%"),
    "총 전환수": st.column_config.NumberColumn("총 전환수", format="%d"),
    "총 전환매출": st.column_config.NumberColumn("총 전환매출", format="%d 원"),
    "통합 ROAS(%)": st.column_config.NumberColumn("통합 ROAS(%)", format="%.1f %%"),
}

@st.fragment
def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return

    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))
    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)

    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터가 포함되어 있어 '통합 전환' 기준으로 성과가 표시됩니다.")
    funnel_toggle = False

    with st.spinner("🔄 최신 필터 조건에 맞추어 데이터를 실시간으로 집계하고 있습니다..."):
        bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
        if bundle is None or bundle.empty: return

        kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=0)
        ad_bundle_cur = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=0, top_k=50)

        kw_tmp = kw_bundle_cur.rename(columns={"keyword": "item_name"}) if not kw_bundle_cur.empty else pd.DataFrame()
        if not ad_bundle_cur.empty:
            ad_tmp = ad_bundle_cur.copy()
            if "ad_title" in ad_tmp.columns:
                ad_tmp["final_ad_name"] = ad_tmp["ad_title"].fillna("").astype(str).str.strip()
                mask_empty = ad_tmp["final_ad_name"].isin(["", "nan", "None"])
                ad_tmp.loc[mask_empty, "final_ad_name"] = ad_tmp.loc[mask_empty, "ad_name"].astype(str)
            else: ad_tmp["final_ad_name"] = ad_tmp["ad_name"].astype(str)
            ad_tmp = ad_tmp.rename(columns={"final_ad_name": "item_name"})
        else: ad_tmp = pd.DataFrame()

        valid_detail = [df for df in [kw_tmp, ad_tmp] if not df.empty]
        detail_bundle = pd.concat(valid_detail, ignore_index=True) if valid_detail else pd.DataFrame()
        
        df = _perf_common_merge_meta(bundle, meta)
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
            "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비",
            "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
            "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액",
            "conv": "구매완료수", "sales": "구매완료 매출"
        }).copy()
        view = _add_perf_metrics(view)

        if not detail_bundle.empty:
            rank_map_camp = _keyword_rank_by_keys(detail_bundle, ["customer_id", "campaign_id"])
            if not rank_map_camp.empty:
                key_cols = ["customer_id", "campaign_id"]
                view = _normalize_merge_keys(view, key_cols)
                rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
                view = view.merge(rank_map_camp, on=key_cols, how="left")
        if "avg_rank" in view.columns: view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    selected_tab = st.pills("분석 탭 선택", ["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"], default="종합 성과")

    if selected_tab == "종합 성과":
        camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
        sel_camp_main = st.selectbox("캠페인 검색", camps_main, key="camp_name_filter_main")
        disp_main = view.copy()
        if sel_camp_main != "전체": disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
        if "평균순위" in disp_main.columns: base_cols.append("평균순위")

        if has_pre_patch_cur:
            all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
            roas_col, sales_col = "통합 ROAS(%)", "총 전환매출"
        else:
            if not funnel_toggle:
                all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "구매완료수", "구매완료 매출", "구매 ROAS(%)"]
                roas_col, sales_col = "구매 ROAS(%)", "구매완료 매출"
            else:
                all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "구매완료수", "구매완료 매출", "구매 ROAS(%)", "장바구니수", "장바구니 매출액", "장바구니 ROAS(%)", "위시리스트수", "위시리스트 매출액", "위시리스트 ROAS(%)", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
                roas_col, sales_col = "구매 ROAS(%)", "구매완료 매출"

        col_type, col_device = st.columns([1.5, 1])
        with col_type:
            type_grp = disp_main.groupby("캠페인유형").agg({"광고비": "sum", sales_col: "sum"}).reset_index()
            total_cost = type_grp["광고비"].sum()
            type_grp["지출 비중(%)"] = np.where(total_cost > 0, (type_grp["광고비"] / total_cost) * 100, 0.0)
            type_grp[roas_col] = np.where(type_grp["광고비"] > 0, (type_grp[sales_col] / type_grp["광고비"]) * 100, 0.0)
            type_grp = type_grp.sort_values("광고비", ascending=False)
            st.dataframe(
                type_grp, width="stretch", height=_compact_df_height(type_grp, min_height=74, max_height=220), hide_index=True,
                column_config={"캠페인유형": st.column_config.TextColumn("캠페인 유형"), "광고비": st.column_config.NumberColumn("총 광고비", format="%d 원"), sales_col: st.column_config.NumberColumn(sales_col, format="%d 원"), "지출 비중(%)": st.column_config.ProgressColumn("지출 비중", format="%.1f%%", min_value=0, max_value=100), roas_col: st.column_config.NumberColumn(f"평균 {roas_col}", format="%.1f %%")}
            )
        with col_device:
            device_df = _query_device_breakdown(engine, f["start"], f["end"], cids, type_sel)
            if not device_df.empty:
                st.markdown("<div style='font-size:13px; color:#555; text-align:center; margin-bottom:5px;'>기기별 광고비 지출 비중</div>", unsafe_allow_html=True)
                fig = px.pie(device_df, values="cost", names="device_name", hole=0.55)
                fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=180, showlegend=True)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            else: st.info("기기별 다차원 데이터가 없어 지출 비중을 표시할 수 없습니다.")

        final_cols = [c for c in base_cols + all_metrics_cols if c in disp_main.columns]
        disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n).reset_index(drop=True)
        event = st.dataframe(disp_main, width="stretch", hide_index=True, selection_mode="single-row", on_select="rerun", column_config=FAST_COL_CONFIG)

        selected_rows = event.selection.rows
        if selected_rows:
            selected_idx = selected_rows[0]
            selected_campaign = disp_main.iloc[selected_idx]["캠페인"]
            kw_detail = detail_bundle[detail_bundle["campaign_name"] == selected_campaign].copy()
            st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
            with st.container(border=True):
                st.markdown(f"<h5 style='color: #335CFF; margin-bottom: 8px;'>[{selected_campaign}] 하위 그룹/상세 성과</h5>", unsafe_allow_html=True)
                if not kw_detail.empty:
                    for c in ["cart_sales", "cart_conv", "wishlist_sales", "wishlist_conv"]:
                        if c not in kw_detail.columns: kw_detail[c] = 0
                    kw_view = kw_detail.rename(columns={"adgroup_name": "광고그룹", "item_name": "키워드/상품명", "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액", "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액", "conv": "구매완료수", "sales": "구매완료 매출"})
                    kw_view['광고그룹'] = kw_view['광고그룹'].fillna('미분류').replace('', '미분류')
                    kw_view['키워드/상품명'] = kw_view['키워드/상품명'].fillna('미분류').replace('', '미분류')
                    grp_kw = kw_view.groupby(['광고그룹', '키워드/상품명'], as_index=False)[['노출', '클릭', '광고비', '장바구니수', '장바구니 매출액', '위시리스트수', '위시리스트 매출액', '구매완료수', '구매완료 매출']].sum()
                    grp_kw = _add_perf_metrics(grp_kw)
                    scatter_df = grp_kw[grp_kw['광고비'] > 0].sort_values('광고비', ascending=False).head(30).copy()
                    if not scatter_df.empty:
                        scatter_df['짧은이름'] = scatter_df['키워드/상품명'].apply(lambda x: str(x)[:12] + "...")
                        scatter_df['클릭_size'] = scatter_df['클릭'].apply(lambda x: max(x, 1))
                        fig_scatter = px.scatter(scatter_df, x='광고비', y=roas_col, color='광고그룹', size='클릭_size', text='짧은이름', hover_data={'키워드/상품명': True, '광고비': ':,.0f', roas_col: ':.1f', '클릭': ':,.0f'})
                        fig_scatter.update_traces(textposition='top center', textfont_size=11, marker=dict(line=dict(width=1, color='white')))
                        fig_scatter.add_hline(y=100, line_dash="dash", line_color="#EF4444")
                        fig_scatter.update_layout(margin=dict(t=20, l=10, r=20, b=10), height=450, xaxis_title="광고 소진액 (원)", yaxis_title=f"{roas_col}", legend_title="광고그룹")
                        st.plotly_chart(fig_scatter, use_container_width=True, config={'displayModeBar': False})

                    sub_cols = ["광고그룹", "키워드/상품명", "노출", "클릭", "CTR(%)", "광고비", "구매완료수", "구매완료 매출", "구매 ROAS(%)"]
                    if has_pre_patch_cur: sub_cols = ["광고그룹", "키워드/상품명", "노출", "클릭", "CTR(%)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
                    kw_disp = grp_kw[[c for c in sub_cols if c in grp_kw.columns]].sort_values("광고비", ascending=False).head(100)
                    st.dataframe(kw_disp, width="stretch", hide_index=True, column_config=FAST_COL_CONFIG)
                else: st.info("해당 캠페인에 등록된 하위 키워드/소재 데이터가 없습니다.")

    elif selected_tab == "그룹 성과":
        if detail_bundle is None or detail_bundle.empty:
            st.info("광고그룹 성과 데이터가 없습니다.")
        else:
            grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in detail_bundle.columns]
            val_cols = [c for c in ["imp", "clk", "cost", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales", "conv", "sales"] if c in detail_bundle.columns]
            if not grp_cols or not val_cols: st.info("광고그룹 성과 데이터가 없습니다.")
            else:
                grp = detail_bundle.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp = _perf_common_merge_meta(grp, meta)
                grouped = grp.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액", "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액", "conv": "구매완료수", "sales": "구매완료 매출"}).copy()
                grouped = _add_perf_metrics(grouped)
                
                camps = ["전체"] + sorted([str(x) for x in grouped["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in grouped.columns else ["전체"]
                sel_camp = st.selectbox("캠페인 필터", camps, key="camp_group_filter")
                if sel_camp != "전체": grouped = grouped[grouped["캠페인"] == sel_camp]

                all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "구매완료수", "구매완료 매출", "구매 ROAS(%)"] if not has_pre_patch_cur else ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                cols_grp = [c for c in base_cols_grp + all_metrics_cols if c in grouped.columns]
                disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n)
                st.dataframe(disp_grp, width="stretch", hide_index=True, column_config=FAST_COL_CONFIG)

    elif selected_tab == "기간 비교":
        st.markdown("<div style='display:flex; justify-content:flex-end; margin-bottom:8px;'>", unsafe_allow_html=True)
        # ⚡ 토글 명칭 변경 (왼쪽 정렬)
        show_deltas = st.toggle("📊 증감율 보기", value=False, key="camp_abs_toggle")
        st.markdown("</div>", unsafe_allow_html=True)

        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

        with st.spinner("🔄 이전 기간의 데이터를 불러오는 중입니다..."):
            base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)

        view_cmp = view.copy()
        valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
        if not base_bundle.empty and valid_keys: view_cmp = _apply_comparison_metrics(view_cmp, base_bundle, valid_keys)
        else: view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns])

        has_pre_patch_base = (b1 < patch_date) if b1 else False
        show_mode = "integrated_only" if (has_pre_patch_base or has_pre_patch_cur) else "purchase_default"
        if show_mode == "integrated_only": st.warning("⚠️ 비교 기간에 3월 11일 이전(네이버 퍼널 분리 패치 전) 데이터가 포함되어 '통합 전환' 기준으로 표시합니다.")

        # ⚡ 토글 ON/OFF 에 따른 컬럼 표출 및 "순위 변화" 조건 완벽 적용
        metrics_cols_cmp = []
        metrics_cols_cmp.extend(["노출", "노출 증감", "노출 차이"] if show_deltas else ["노출"])
        metrics_cols_cmp.extend(["클릭", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭"])
        metrics_cols_cmp.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
        metrics_cols_cmp.extend(["CPC(원)", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC(원)"])

        if show_mode == "integrated_only":
            metrics_cols_cmp.extend(["총 전환수", "총 전환 증감", "총 전환 차이"] if show_deltas else ["총 전환수"])
            metrics_cols_cmp.extend(["총 전환매출", "총 매출 증감", "총 매출 차이"] if show_deltas else ["총 전환매출"])
            metrics_cols_cmp.extend(["통합 ROAS(%)", "통합 ROAS 증감"] if show_deltas else ["통합 ROAS(%)"])
        else:
            metrics_cols_cmp.extend(["구매완료수", "구매 증감", "구매 차이"] if show_deltas else ["구매완료수"])
            metrics_cols_cmp.extend(["구매완료 매출", "구매 매출 증감", "구매 매출 차이"] if show_deltas else ["구매완료 매출"])
            metrics_cols_cmp.extend(["구매 ROAS(%)", "구매 ROAS 증감"] if show_deltas else ["구매 ROAS(%)"])

        base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인"]
        if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
            base_cols_cmp.append("평균순위")
            if show_deltas: # ⚡ 토글을 켰을 때만 순위 변화 등장
                metrics_cols_cmp.append("순위 변화")

        final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
        disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()

        styled_cmp = disp_cmp.style.format(FMT_DICT)
        styled_cmp = _apply_delta_styles(styled_cmp, disp_cmp)

        st.dataframe(styled_cmp, width="stretch", height=560, hide_index=True, column_config={
            "캠페인": st.column_config.TextColumn("캠페인", pinned=True)
        })

    elif selected_tab == "꺼짐 기록":
        st.info("이 지면에서는 상세 퍼널보다 안정적인 광고 운영 여부가 중요합니다.")
        try:
            days_diff = (pd.to_datetime(f["end"]) - pd.to_datetime(f["start"])).days + 1
            if days_diff < 3: st.warning("단기 데이터(3일 미만) 기반 예산 증액 주의: 일시적인 효율 상승일 수 있습니다.")
        except: pass

        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty: st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        else:
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty:
                dim_camp["campaign_id"], off_log["campaign_id"] = dim_camp["campaign_id"].astype(str), off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
            else: off_log["campaign_name"] = off_log["campaign_id"]

            if not meta.empty:
                meta_copy = meta.copy()
                meta_copy["customer_id"], off_log["customer_id"] = meta_copy["customer_id"].astype(str), off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            else: off_log["account_name"] = off_log["customer_id"]

            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            pivot_df = off_log.pivot_table(index=["account_name", "campaign_name"], columns="dt_str", values="off_time", aggfunc='first').reset_index()
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"}).fillna("-")

            if not view.empty and "통합 ROAS(%)" in view.columns:
                roas_df = view[["업체명", "캠페인", "통합 ROAS(%)"]].drop_duplicates()
                pivot_df = pivot_df.merge(roas_df, on=["업체명", "캠페인"], how="left")
                cols = pivot_df.columns.tolist()
                cols.insert(2, cols.pop(cols.index('통합 ROAS(%)')))
                pivot_df = pivot_df[cols]

            st.dataframe(pivot_df, width="stretch", hide_index=True)
