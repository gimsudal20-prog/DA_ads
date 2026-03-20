# -*- coding: utf-8 -*-
"""view_keyword.py - Keyword & Adgroup performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import query_keyword_bundle, query_ad_bundle
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas


def _inject_keyword_css():
    st.markdown("""
    <style>
    .kw-toolbar {
        background: var(--nv-surface);
        border: 1px solid var(--nv-line);
        border-radius: 12px;
        padding: 14px 16px 10px 16px;
        margin-bottom: 16px;
    }
    .kw-toolbar-title {
        font-size: 13px;
        font-weight: 700;
        color: var(--nv-text);
        margin-bottom: 10px;
    }
    .kw-section-caption {
        font-size: 12px;
        color: var(--nv-muted);
        margin-top: -6px;
        margin-bottom: 12px;
    }
    </style>
    """, unsafe_allow_html=True)


def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"


def _filter_shopping_general_ads(df: pd.DataFrame, allow_unknown_type: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()

    def _is_general_ad(row):
        ctype = str(row.get("캠페인유형", "")).strip().upper()
        if "파워링크" in ctype or "WEB_SITE" in ctype:
            return False
        if "쇼핑" in ctype or "SHOPPING" in ctype:
            ad_name = str(row.get("키워드/상품명", "")).strip()
            if ad_name.startswith("http") or ad_name.endswith((".jpg", ".png", ".jpeg", ".gif")):
                return False
            ext_keywords = ["추가홍보문구", "홍보문구", "확장소재", "서브링크", "가격링크", "파워링크이미지", "추가제목", "플레이스정보"]
            if any(ext in ad_name for ext in ext_keywords):
                return False
            return True
        return allow_unknown_type

    if "키워드/상품명" in work.columns:
        mask = work.apply(_is_general_ad, axis=1)
        return work[mask].copy()
    return work


def _filter_keyword_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    ctype = work.get("캠페인유형", pd.Series([""] * len(work), index=work.index)).astype(str).str.upper()
    if mode == "파워링크":
        return work[ctype.str.contains("파워링크|WEB_SITE", na=False)].copy()
    if mode == "쇼핑검색":
        return work[ctype.str.contains("쇼핑|SHOPPING", na=False)].copy()
    return work


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "구매완료 매출", "장바구니 매출액", "위시리스트 매출액", "노출", "클릭", "구매완료수", "장바구니수", "위시리스트수", "tot_conv", "tot_sales"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    if "tot_conv" in view.columns:
        view["총 전환수"] = view["tot_conv"]
        view["총 전환매출"] = view["tot_sales"]
    else:
        view["총 전환수"] = view.get("구매완료수", 0) + view.get("장바구니수", 0) + view.get("위시리스트수", 0)
        view["총 전환매출"] = view.get("구매완료 매출", 0) + view.get("장바구니 매출액", 0) + view.get("위시리스트 매출액", 0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["구매 ROAS(%)"] = np.where(view["광고비"] > 0, (view["구매완료 매출"] / view["광고비"]) * 100, 0.0)
    view["장바구니 ROAS(%)"] = np.where(view["광고비"] > 0, (view["장바구니 매출액"] / view["광고비"]) * 100, 0.0)
    view["통합 ROAS(%)"] = np.where(view["광고비"] > 0, (view["총 전환매출"] / view["광고비"]) * 100, 0.0)

    if "위시리스트 매출액" in view.columns:
        view["위시리스트 ROAS(%)"] = np.where(view["광고비"] > 0, (view["위시리스트 매출액"] / view["광고비"]) * 100, 0.0)

    return view


def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty:
        return view_df

    for k in merge_keys:
        if k in view_df.columns:
            view_df[k] = view_df[k].astype(str)
        if k in base_df.columns:
            base_df[k] = base_df[k].astype(str)

    val_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_conv', 'tot_sales']
    for c in val_cols:
        if c in base_df.columns:
            base_df[c] = pd.to_numeric(base_df[c], errors='coerce').fillna(0)

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
        if bc not in merged.columns:
            merged[bc] = 0
        merged[bc] = pd.to_numeric(merged[bc], errors='coerce').fillna(0)

    if 'b_avg_rank' not in merged.columns:
        merged['b_avg_rank'] = np.nan

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

    merged['이전 장바구니수'] = merged['b_cart_conv']
    merged['이전 장바구니 매출액'] = merged['b_cart_sales']
    merged['장바구니 증감'] = merged['장바구니수'] - merged['이전 장바구니수']
    merged['장바구니 증감(%)'] = np.where(merged['이전 장바구니수'] > 0, (merged['장바구니 증감'] / merged['이전 장바구니수']) * 100, np.where(merged['장바구니수'] > 0, 100.0, 0.0))
    merged['이전 장바구니 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 장바구니 매출액'] / merged['이전 광고비']) * 100, 0.0)
    merged['장바구니ROAS 증감'] = merged['장바구니 ROAS(%)'] - merged['이전 장바구니 ROAS(%)']

    merged['이전 위시리스트수'] = merged['b_wishlist_conv']
    merged['이전 위시리스트 매출액'] = merged['b_wishlist_sales']
    merged['위시리스트 증감'] = merged.get('위시리스트수', 0) - merged['이전 위시리스트수']
    merged['위시리스트 증감(%)'] = np.where(merged['이전 위시리스트수'] > 0, (merged['위시리스트 증감'] / merged['이전 위시리스트수']) * 100, np.where(merged.get('위시리스트수', 0) > 0, 100.0, 0.0))
    merged['이전 위시리스트 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 위시리스트 매출액'] / merged['이전 광고비']) * 100, 0.0)
    merged['위시리스트ROAS 증감'] = merged.get('위시리스트 ROAS(%)', 0) - merged['이전 위시리스트 ROAS(%)']

    merged['이전 구매완료수'] = merged['b_conv']
    merged['이전 구매완료 매출'] = merged['b_sales']
    merged['구매 증감'] = merged['구매완료수'] - merged['이전 구매완료수']
    merged['이전 구매 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 구매완료 매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['구매 ROAS 증감'] = merged['구매 ROAS(%)'] - merged['이전 구매 ROAS(%)']

    merged['이전 총 전환수'] = merged.get('b_tot_conv', merged['b_conv'] + merged['b_cart_conv'] + merged['b_wishlist_conv'])
    merged['이전 총 전환매출'] = merged.get('b_tot_sales', merged['b_sales'] + merged['b_cart_sales'] + merged['b_wishlist_sales'])
    merged['총 전환 증감'] = merged['총 전환수'] - merged['이전 총 전환수']
    merged['이전 통합 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 총 전환매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['통합 ROAS 증감'] = merged['통합 ROAS(%)'] - merged['이전 통합 ROAS(%)']

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns:
            merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['이전 평균순위'] = merged['b_avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)

    return merged


@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_keyword_view(kw_bundle, ad_bundle, meta):
    if (kw_bundle is None or kw_bundle.empty) and (ad_bundle is None or ad_bundle.empty):
        return pd.DataFrame()

    df_kw = _perf_common_merge_meta(kw_bundle, meta) if kw_bundle is not None and not kw_bundle.empty else pd.DataFrame()
    df_ad = _perf_common_merge_meta(ad_bundle, meta) if ad_bundle is not None and not ad_bundle.empty else pd.DataFrame()

    view_kw, view_ad = pd.DataFrame(), pd.DataFrame()

    rename_dict = {
        "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형",
        "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드/상품명",
        "imp": "노출", "clk": "클릭", "cost": "광고비",
        "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
        "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액",
        "conv": "구매완료수", "sales": "구매완료 매출"
    }

    if not df_kw.empty:
        for col in ["cart_sales", "cart_conv", "wishlist_sales", "wishlist_conv"]:
            if col not in df_kw.columns:
                df_kw[col] = 0
        view_kw = df_kw.rename(columns=rename_dict)

    if not df_ad.empty:
        if "ad_title" in df_ad.columns:
            df_ad["final_ad_name"] = df_ad["ad_title"].fillna("").astype(str).str.strip()
            mask_empty = df_ad["final_ad_name"].isin(["", "nan", "None"])
            df_ad.loc[mask_empty, "final_ad_name"] = df_ad.loc[mask_empty, "ad_name"].astype(str)
        else:
            df_ad["final_ad_name"] = df_ad["ad_name"].astype(str)

        for col in ["cart_sales", "cart_conv", "wishlist_sales", "wishlist_conv"]:
            if col not in df_ad.columns:
                df_ad[col] = 0
        rename_dict_ad = rename_dict.copy()
        rename_dict_ad["final_ad_name"] = "키워드/상품명"
        view_ad = df_ad.rename(columns=rename_dict_ad)
        view_ad = _filter_shopping_general_ads(view_ad, allow_unknown_type=True)

    if view_kw.empty and view_ad.empty:
        return pd.DataFrame()
    if view_kw.empty:
        view = view_ad.copy()
    elif view_ad.empty:
        view = view_kw.copy()
    else:
        view = pd.concat([view_kw, view_ad], ignore_index=True)

    view = _add_perf_metrics(view)
    if "avg_rank" in view.columns:
        view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)
    return view


def style_delta_str(val):
    val_str = str(val).strip()
    if val_str.startswith("+"):
        return 'color: #0528F2; font-weight: 600;'
    elif val_str.startswith("-"):
        return 'color: #F04438; font-weight: 600;'
    return ''


def style_delta_str_neg(val):
    val_str = str(val).strip()
    if val_str.startswith("+"):
        return 'color: #F04438; font-weight: 600;'
    elif val_str.startswith("-"):
        return 'color: #0528F2; font-weight: 600;'
    return ''




def _keyword_pinned_cfg(columns):
    cfg = {}
    pin_targets = ["업체명", "캠페인", "광고그룹", "키워드/상품명"]
    for col in pin_targets:
        if col in columns:
            cfg[col] = st.column_config.TextColumn(col, pinned=True, width="medium")
    return cfg

def _make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = []
    seen = {}
    for c in df.columns:
        base = str(c)
        if base not in seen:
            seen[base] = 0
            cols.append(base)
        else:
            seen[base] += 1
            cols.append(f"{base}_{seen[base]}")
    out = df.copy()
    out.columns = cols
    return out


def _render_keyword_sticky_table(styler_or_df, columns, *, height=550, hide_index=True):
    try:
        st.dataframe(
            styler_or_df,
            use_container_width=True,
            height=height,
            hide_index=hide_index,
            column_config=_keyword_pinned_cfg(columns),
        )
    except Exception:
        raw_df = styler_or_df.data if hasattr(styler_or_df, "data") else styler_or_df
        raw_df = _make_unique_columns(raw_df)
        safe_columns = list(raw_df.columns)
        st.dataframe(
            raw_df,
            use_container_width=True,
            height=height,
            hide_index=hide_index,
            column_config=_keyword_pinned_cfg(safe_columns),
        )

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    _inject_keyword_css()

    st.markdown("<div class='nv-sec-title'>키워드 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_keyword", 300))

    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)

    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터가 포함되어 있어 통합 전환 기준으로 성과가 표시됩니다.")

    with st.spinner("🔄 키워드 및 소재 데이터를 집계하고 있습니다... 잠시만 기다려주세요."):
        kw_bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)
        ad_bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=50000, top_k=50)
        view = compute_keyword_view(kw_bundle, ad_bundle, meta)

    tab_main, tab_cmp = st.tabs(["종합 성과", "기간 비교"])

    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "장바구니수": "{:,.1f}", "장바구니 매출액": "{:,.0f}원}", "장바구니 ROAS(%)": "{:,.1f}%",
        "위시리스트수": "{:,.1f}", "위시리스트 매출액": "{:,.0f}원}", "위시리스트 ROAS(%)": "{:,.1f}%",
        "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
        "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%", "CTR(%)": "{:,.1f}%",
        "순위 변화": "{:+.1f}"
    }
    # fix malformed brace
    fmt["장바구니 매출액"] = "{:,.0f}원"
    fmt["위시리스트 매출액"] = "{:,.0f}원"

    with tab_main:
        if view.empty:
            st.info("해당 기간의 키워드/소재 성과 데이터가 없습니다.")
        else:
            st.markdown("<div class='kw-toolbar'><div class='kw-toolbar-title'>조회 조건</div>", unsafe_allow_html=True)
            col_mode, col_camp, col_grp = st.columns([1, 1.2, 1.2])
            view_mode = col_mode.radio("조회 구분", ["파워링크", "쇼핑검색"], horizontal=True, key="kw_view_mode_main")
            filtered_mode = _filter_keyword_mode(view, view_mode)
            camps = ["전체"] + sorted([str(x) for x in filtered_mode["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in filtered_mode.columns else ["전체"]
            sel_camp = col_camp.selectbox("캠페인 필터", camps, key="kw_camp_filter_main")
            filtered_for_grp = filtered_mode.copy()
            if sel_camp != "전체":
                filtered_for_grp = filtered_for_grp[filtered_for_grp["캠페인"] == sel_camp]
            grps = ["전체"] + sorted([str(x) for x in filtered_for_grp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_for_grp.columns else ["전체"]
            sel_grp = col_grp.selectbox("광고그룹 필터", grps, key="kw_grp_filter_main")
            disp = filtered_for_grp.copy()
            if sel_grp != "전체":
                disp = disp[disp["광고그룹"] == sel_grp]
            st.markdown("</div>", unsafe_allow_html=True)

            base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드/상품명"]
            if "평균순위" in disp.columns:
                base_cols.append("평균순위")

            metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]

            final_cols = [c for c in base_cols + metrics_cols if c in disp.columns]
            disp = disp[final_cols].sort_values("광고비", ascending=False).head(top_n)
            with st.container(border=True):
                st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>{view_mode} 종합 성과 데이터</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='kw-section-caption'>표시 행 수: {len(disp):,}개</div>", unsafe_allow_html=True)
                _render_keyword_sticky_table(disp.style.format(fmt), list(disp.columns), height=550, hide_index=True)

    with tab_cmp:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="kw_cmp_mode")

        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

        with st.spinner("🔄 비교 기간의 데이터를 불러오는 중입니다..."):
            base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=50000)
            base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=50000, top_k=50)

        if view.empty:
            st.info("현재 기간의 키워드/소재 데이터가 없습니다.")
        else:
            base_kw = base_kw_bundle.rename(columns={"keyword": "키워드/상품명"}) if base_kw_bundle is not None and not base_kw_bundle.empty else pd.DataFrame()
            if base_ad_bundle is not None and not base_ad_bundle.empty:
                if "ad_title" in base_ad_bundle.columns:
                    base_ad_bundle["final_ad_name"] = base_ad_bundle["ad_title"].fillna("").astype(str).str.strip()
                    mask_empty = base_ad_bundle["final_ad_name"].isin(["", "nan", "None"])
                    base_ad_bundle.loc[mask_empty, "final_ad_name"] = base_ad_bundle.loc[mask_empty, "ad_name"].astype(str)
                else:
                    base_ad_bundle["final_ad_name"] = base_ad_bundle["ad_name"].astype(str)
                base_ad = base_ad_bundle.rename(columns={"final_ad_name": "키워드/상품명"})
                base_ad = _filter_shopping_general_ads(base_ad, allow_unknown_type=True)
            else:
                base_ad = pd.DataFrame()

            base_bundle = pd.concat([base_kw, base_ad], ignore_index=True)

            view_cmp = view.copy()
            if not base_bundle.empty:
                valid_keys = [k for k in ["customer_id", "adgroup_id", "키워드/상품명"] if k in view_cmp.columns and k in base_bundle.columns]
                if valid_keys:
                    view_cmp = _apply_comparison_metrics(view_cmp, base_bundle, valid_keys)
                else:
                    view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [])
            else:
                view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [])

            base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드/상품명"]
            if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
                base_cols_cmp.extend(["평균순위", "이전 평균순위", "순위 변화"])

            with st.container(border=True):
                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>개별 상세 비교</div>", unsafe_allow_html=True)
                render_item_comparison_search("키워드/소재", view_cmp, base_bundle, "키워드/상품명", f["start"], f["end"], b1, b2)

            st.markdown("<div class='kw-toolbar'><div class='kw-toolbar-title'>기간 비교 필터</div>", unsafe_allow_html=True)
            col_mode_cmp, col_camp_cmp, col_grp_cmp = st.columns([1, 1.2, 1.2])
            view_mode_cmp = col_mode_cmp.radio("조회 구분", ["파워링크", "쇼핑검색"], horizontal=True, key="kw_view_mode_cmp")
            filtered_mode_cmp = _filter_keyword_mode(view_cmp, view_mode_cmp)
            camps_cmp = ["전체"] + sorted([str(x) for x in filtered_mode_cmp["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in filtered_mode_cmp.columns else ["전체"]
            sel_camp_cmp = col_camp_cmp.selectbox("캠페인 필터", camps_cmp, key="kw_camp_filter_cmp")
            filtered_cmp = filtered_mode_cmp.copy()
            if sel_camp_cmp != "전체":
                filtered_cmp = filtered_cmp[filtered_cmp["캠페인"] == sel_camp_cmp]
            grps_cmp = ["전체"] + sorted([str(x) for x in filtered_cmp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_cmp.columns else ["전체"]
            sel_grp_cmp = col_grp_cmp.selectbox("광고그룹 필터", grps_cmp, key="kw_grp_filter_cmp")
            disp = filtered_cmp.copy()
            if sel_grp_cmp != "전체":
                disp = disp[disp["광고그룹"] == sel_grp_cmp]
            st.markdown("</div>", unsafe_allow_html=True)

            has_pre_patch_base = (b1 < patch_date) if b1 else False
            if has_pre_patch_base or has_pre_patch_cur:
                st.warning("⚠️ 비교 기간에 3월 11일 이전 데이터가 포함되어 있어 통합 전환 기준으로 표시합니다.")

            def _combine(r, c_val, c_pct):
                v = r.get(c_val)
                p = r.get(c_pct)
                if pd.isna(v) or v == 0:
                    return "-"
                v_str = f"{v:+,.0f}" if c_val in ["노출 증감", "클릭 증감", "광고비 증감", "CPC 증감"] else f"{v:+,.1f}"
                return f"{v_str} ({p:+.1f}%)"

            if show_mode == "integrated_only":
                disp["노출 증감/율"] = disp.apply(lambda r: _combine(r, "노출 증감", "노출 증감(%)"), axis=1)
                disp["클릭 증감/율"] = disp.apply(lambda r: _combine(r, "클릭 증감", "클릭 증감(%)"), axis=1)
                disp["광고비 증감/율"] = disp.apply(lambda r: _combine(r, "광고비 증감", "광고비 증감(%)"), axis=1)
                disp["CPC 증감/율"] = disp.apply(lambda r: _combine(r, "CPC 증감", "CPC 증감(%)"), axis=1)
                disp["총 전환 증감 "] = disp.get("총 전환 증감", pd.Series(0.0, index=disp.index)).apply(lambda x: f"{x:+.1f}" if pd.notna(x) and x != 0 else "-")
                disp["통합 ROAS 증감 "] = disp.get("통합 ROAS 증감", pd.Series(0.0, index=disp.index)).apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")

                metrics_cols_cmp = ["노출", "노출 증감/율", "클릭", "클릭 증감/율", "광고비", "광고비 증감/율", "CPC(원)", "CPC 증감/율", "총 전환수", "총 전환 증감 ", "총 전환매출", "통합 ROAS(%)", "통합 ROAS 증감 "]
                delta_cols = ["노출 증감/율", "클릭 증감/율", "광고비 증감/율", "CPC 증감/율", "총 전환 증감 ", "통합 ROAS 증감 "]
            else:
                if not funnel_toggle:
                    disp["노출 증감/율"] = disp.apply(lambda r: _combine(r, "노출 증감", "노출 증감(%)"), axis=1)
                    disp["클릭 증감/율"] = disp.apply(lambda r: _combine(r, "클릭 증감", "클릭 증감(%)"), axis=1)
                    disp["광고비 증감/율"] = disp.apply(lambda r: _combine(r, "광고비 증감", "광고비 증감(%)"), axis=1)
                    disp["CPC 증감/율"] = disp.apply(lambda r: _combine(r, "CPC 증감", "CPC 증감(%)"), axis=1)
                    disp["구매 증감 "] = disp.get("구매 증감", pd.Series(0.0, index=disp.index)).apply(lambda x: f"{x:+.1f}" if pd.notna(x) and x != 0 else "-")
                    disp["구매 ROAS 증감 "] = disp.get("구매 ROAS 증감", pd.Series(0.0, index=disp.index)).apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")

                    metrics_cols_cmp = ["노출", "노출 증감/율", "클릭", "클릭 증감/율", "광고비", "광고비 증감/율", "CPC(원)", "CPC 증감/율", "구매완료수", "구매 증감 ", "구매완료 매출", "구매 ROAS(%)", "구매 ROAS 증감 "]
                    delta_cols = ["노출 증감/율", "클릭 증감/율", "광고비 증감/율", "CPC 증감/율", "구매 증감 ", "구매 ROAS 증감 "]
                else:
                    metrics_cols_cmp = [
                        "이전 노출", "노출", "노출 증감", "노출 증감(%)",
                        "이전 클릭", "클릭", "클릭 증감", "클릭 증감(%)",
                        "이전 광고비", "광고비", "광고비 증감", "광고비 증감(%)",
                        "이전 구매완료수", "구매완료수", "구매 증감",
                        "이전 구매 ROAS(%)", "구매 ROAS(%)", "구매 ROAS 증감(%)",
                        "이전 장바구니수", "장바구니수", "장바구니 증감", "장바구니 증감(%)",
                        "이전 위시리스트수", "위시리스트수", "위시리스트 증감", "위시리스트 증감(%)",
                        "이전 총 전환수", "총 전환수", "총 전환 증감",
                        "이전 통합 ROAS(%)", "통합 ROAS(%)", "통합 ROAS 증감(%)"
                    ]
                    delta_cols = ["노출 증감(%)", "노출 증감", "클릭 증감(%)", "클릭 증감", "광고비 증감(%)", "광고비 증감", "장바구니 증감(%)", "장바구니 증감", "위시리스트 증감(%)", "위시리스트 증감", "구매 증감", "구매 ROAS 증감(%)", "총 전환 증감", "통합 ROAS 증감(%)"]

            if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
                if "순위 변화" not in metrics_cols_cmp:
                    metrics_cols_cmp.append("순위 변화")
                    delta_cols.append("순위 변화")

            final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in disp.columns]
            disp = disp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()

            fmt_cmp = {
                "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
                "장바구니수": "{:,.1f}", "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
                "위시리스트수": "{:,.1f}", "위시리스트 매출액": "{:,.0f}원", "위시리스트 ROAS(%)": "{:,.1f}%",
                "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%",
                "이전 노출": "{:,.0f}", "이전 클릭": "{:,.0f}", "이전 광고비": "{:,.0f}", "이전 CPC(원)": "{:,.0f}",
                "이전 장바구니수": "{:,.1f}", "이전 구매완료수": "{:,.1f}", "이전 총 전환수": "{:,.1f}", "이전 위시리스트수": "{:,.1f}",
                "노출 증감": "{:+,.0f}", "클릭 증감": "{:+,.0f}", "광고비 증감": "{:+,.0f}",
                "장바구니 증감": "{:+,.1f}", "구매 증감": "{:+,.1f}", "총 전환 증감": "{:+,.1f}", "위시리스트 증감": "{:+,.1f}",
                "노출 증감(%)": "{:+.1f}%", "클릭 증감(%)": "{:+.1f}%", "광고비 증감(%)": "{:+.1f}%",
                "장바구니 증감(%)": "{:+.1f}%", "위시리스트 증감(%)": "{:+.1f}%",
                "순위 변화": "{:+.1f}"
            }

            styled_cmp = disp.style.format(fmt_cmp)

            if delta_cols:
                target_delta_cols = [c for c in delta_cols if c in disp.columns]

                if not funnel_toggle or show_mode == "integrated_only":
                    pos_cols = [c for c in target_delta_cols if c not in ["광고비 증감/율", "CPC 증감/율"]]
                    neg_cols = [c for c in target_delta_cols if c in ["광고비 증감/율", "CPC 증감/율"]]

                    try:
                        if pos_cols:
                            styled_cmp = styled_cmp.map(style_delta_str, subset=pos_cols)
                        if neg_cols:
                            styled_cmp = styled_cmp.map(style_delta_str_neg, subset=neg_cols)
                    except Exception:
                        if pos_cols:
                            styled_cmp = styled_cmp.applymap(style_delta_str, subset=pos_cols)
                        if neg_cols:
                            styled_cmp = styled_cmp.applymap(style_delta_str_neg, subset=neg_cols)
                else:
                    if target_delta_cols:
                        try:
                            styled_cmp = styled_cmp.map(style_table_deltas, subset=target_delta_cols)
                        except Exception:
                            styled_cmp = styled_cmp.applymap(style_table_deltas, subset=target_delta_cols)

            with st.container(border=True):
                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>키워드/소재 기간 비교 표</div>", unsafe_allow_html=True)
                _render_keyword_sticky_table(styled_cmp, list(disp.columns), height=550, hide_index=True)
