# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import io
from typing import Dict
from datetime import date

from data import *
from ui import render_echarts_dual_axis
from page_helpers import get_dynamic_cmp_options, period_compare_range


def _inject_overview_css():
    st.markdown("""
    <style>
    .ov-headbar {
        background: var(--nv-bg);
        border: 1px solid var(--nv-line);
        border-radius: 12px;
        padding: 10px 12px;
        margin-bottom: 14px;
    }
    .ov-headmeta {
        display:flex;
        flex-wrap:wrap;
        gap:8px;
        align-items:center;
    }
    .ov-chip {
        background: transparent;
        color: var(--nv-text);
        border: 1px solid var(--nv-line);
        border-radius: 8px;
        padding: 5px 10px;
        font-size: 12px;
        font-weight: 600;
        line-height: 1.2;
    }
    .ov-chip.primary {
        background: var(--nv-primary-soft);
        color: var(--nv-primary);
        border-color: transparent;
    }
    .ov-chip.muted {
        color: var(--nv-muted);
        background: var(--nv-surface);
    }
    .ov-kpi-grid {
        display:grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 18px;
    }
    .ov-kpi-panel {
        background: var(--nv-bg);
        border: 1px solid var(--nv-line);
        border-radius: 12px;
        padding: 16px;
    }
    .ov-kpi-title {
        font-size: 13px;
        font-weight: 700;
        color: var(--nv-text);
        margin-bottom: 12px;
    }
    .ov-kpi-cells {
        display:grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
    }
    .ov-kpi-cell {
        background: var(--nv-surface);
        border-radius: 10px;
        padding: 12px;
        min-width: 0;
    }
    .ov-kpi-label {
        font-size: 12px;
        color: var(--nv-muted);
        margin-bottom: 6px;
    }
    .ov-kpi-value {
        font-size: 20px;
        font-weight: 800;
        color: var(--nv-text);
        line-height: 1.15;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .ov-kpi-delta {
        margin-top: 8px;
        font-size: 11px;
        font-weight: 700;
        display:inline-flex;
        padding: 4px 8px;
        border-radius: 999px;
    }
    .ov-kpi-delta.pos { background: var(--nv-primary-soft); color: var(--nv-primary); }
    .ov-kpi-delta.neg { background: #FEE4E2; color: #F04438; }
    .ov-kpi-delta.neu { background: var(--nv-surface); color: var(--nv-muted); border:1px solid var(--nv-line); }
    .ov-toolbar {
        background: var(--nv-surface);
        border: 1px solid var(--nv-line);
        border-radius: 12px;
        padding: 14px 16px 10px 16px;
        margin-bottom: 16px;
    }
    .ov-toolbar-title {
        font-size: 13px;
        font-weight: 700;
        color: var(--nv-text);
        margin-bottom: 10px;
    }
    @media (max-width: 1100px) {
        .ov-kpi-grid, .ov-kpi-cells { grid-template-columns: 1fr; }
    }
    </style>
    """, unsafe_allow_html=True)


def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"




def _sticky_cfg(first_col: str):
    return {
        first_col: st.column_config.TextColumn(first_col, pinned=True, width="medium")
    }


def _auto_table_height(data_obj, default_height: int = 420, min_height: int = 72, max_height: int = 560) -> int:
    try:
        df = data_obj.data if hasattr(data_obj, "data") else data_obj
        rows = len(df.index)
        if rows <= 0:
            return min_height
        if rows == 1:
            return 72
        if rows == 2:
            return 106
        calc = 36 + (rows * 34)
        return max(min_height, min(calc, max_height))
    except Exception:
        return default_height

def _render_overview_sticky_table(styler_or_df, first_col: str, height: int = 420, hide_index: bool = False):
    real_height = _auto_table_height(styler_or_df, default_height=height, max_height=height)
    st.dataframe(
        styler_or_df,
        use_container_width=True,
        height=real_height,
        hide_index=hide_index,
        column_config=_sticky_cfg(first_col),
    )
def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel:
        return "전체 유형"
    if len(type_sel) == 1:
        return type_sel[0]
    return ", ".join(type_sel)


@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=5000)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=0)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        ts = query_campaign_timeseries(_engine, start_dt, end_dt, cids, type_sel)
        return ts if ts is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def format_for_csv(df):
    out_df = df.copy()
    for col in out_df.columns:
        if out_df[col].dtype in ['float64', 'int64']:
            if col in ["노출수", "클릭수", "평균순위", "순위"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["장바구니 담기수", "위시리스트수", "구매완료수", "총 전환수"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["광고비", "구매완료 매출", "장바구니 매출액", "총 전환매출", "CPC"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}원" if pd.notnull(x) else "0원")
            elif "차이" in col:
                if "광고비" in col or "매출" in col or "CPC" in col:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                elif "노출" in col or "클릭" in col:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
                else:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+,.1f}" if pd.notnull(x) and x != 0 else "0.0")
            elif "증감" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "0.0%")
            elif "ROAS" in col or col == "클릭률(%)":
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.1f}%" if pd.notnull(x) else "0.0%")
    return out_df


def calc_pct_diff(c, b):
    diff = c - b
    if b == 0:
        return (100.0 if c > 0 else 0.0), diff
    return (diff / b * 100.0), diff


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


def _style_delta_numeric(val):
    try:
        v = float(val)
    except Exception:
        return ''
    if pd.isna(v) or v == 0:
        return ''
    return 'color: #0528F2; font-weight: 700;' if v > 0 else 'color: #F04438; font-weight: 700;'


def _style_delta_numeric_neg(val):
    try:
        v = float(val)
    except Exception:
        return ''
    if pd.isna(v) or v == 0:
        return ''
    return 'color: #F04438; font-weight: 700;' if v > 0 else 'color: #0528F2; font-weight: 700;'


def _apply_overview_delta_styles(styler, df: pd.DataFrame):
    positive_cols = [
        '노출 증감', '노출 차이', '클릭 증감', '클릭 차이',
        '위시리스트 증감', '위시리스트 차이',
        '장바구니 증감', '장바구니 차이',
        '구매 증감', '구매 차이',
        '구매 매출 증감', '구매 매출 차이',
        '구매 ROAS 증감',
        '총 전환 증감', '총 전환 차이',
        '총 매출 증감', '총 매출 차이',
        '통합 ROAS 증감'
    ]
    negative_cols = ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이']

    pos_subset = [c for c in positive_cols if c in df.columns]
    neg_subset = [c for c in negative_cols if c in df.columns]

    try:
        if pos_subset:
            styler = styler.map(_style_delta_numeric, subset=pos_subset)
        if neg_subset:
            styler = styler.map(_style_delta_numeric_neg, subset=neg_subset)
    except AttributeError:
        if pos_subset:
            styler = styler.applymap(_style_delta_numeric, subset=pos_subset)
        if neg_subset:
            styler = styler.applymap(_style_delta_numeric_neg, subset=neg_subset)
    return styler


def _build_comparison_df(cur_df, base_df, group_col, group_label, type_kor_map=None):
    if cur_df.empty and base_df.empty:
        return pd.DataFrame()

    base_cols = [group_col, 'imp', 'clk', 'cost', 'wishlist_conv', 'wishlist_sales', 'cart_conv', 'cart_sales', 'conv', 'sales', 'tot_conv', 'tot_sales']
    for c in base_cols[1:]:
        if not cur_df.empty and c not in cur_df.columns:
            cur_df[c] = 0.0
        if not base_df.empty and c not in base_df.columns:
            base_df[c] = 0.0

    cur_grp = cur_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not cur_df.empty else pd.DataFrame(columns=base_cols)
    base_grp = base_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not base_df.empty else pd.DataFrame(columns=base_cols)
    merged = pd.merge(cur_grp, base_grp, on=group_col, how='outer', suffixes=('_cur', '_base')).fillna(0)

    table_data = []
    for _, row in merged.iterrows():
        c_imp, c_clk, c_cost = row['imp_cur'], row['clk_cur'], row['cost_cur']
        c_wish, c_wsales = row['wishlist_conv_cur'], row['wishlist_sales_cur']
        c_cart, c_csales = row['cart_conv_cur'], row['cart_sales_cur']
        c_conv, c_sales = row['conv_cur'], row['sales_cur']
        b_imp, b_clk, b_cost = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0)
        b_wish, b_wsales = row.get('wishlist_conv_base', 0), row.get('wishlist_sales_base', 0)
        b_cart, b_csales = row.get('cart_conv_base', 0), row.get('cart_sales_base', 0)
        b_conv, b_sales = row.get('conv_base', 0), row.get('sales_base', 0)

        c_tot_conv = row.get('tot_conv_cur', c_conv + c_cart + c_wish)
        c_tot_sales = row.get('tot_sales_cur', c_sales + c_csales + c_wsales)
        b_tot_conv = row.get('tot_conv_base', b_conv + b_cart + b_wish)
        b_tot_sales = row.get('tot_sales_base', b_sales + b_csales + b_wsales)

        c_cpc = (c_cost / c_clk) if c_clk > 0 else 0
        b_cpc = (b_cost / b_clk) if b_clk > 0 else 0
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0
        b_troas = (b_tot_sales / b_cost * 100) if b_cost > 0 else 0

        pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
        pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
        pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
        pct_cpc, diff_cpc = calc_pct_diff(c_cpc, b_cpc)
        pct_wish, diff_wish = calc_pct_diff(c_wish, b_wish)
        pct_cart, diff_cart = calc_pct_diff(c_cart, b_cart)
        pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
        pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
        pct_tot_conv, diff_tot_conv = calc_pct_diff(c_tot_conv, b_tot_conv)
        pct_tot_sales, diff_tot_sales = calc_pct_diff(c_tot_sales, b_tot_sales)

        val = row[group_col]
        if type_kor_map:
            val = type_kor_map.get(str(val).upper(), val)

        table_data.append({
            group_label: val,
            "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
            "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
            "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
            "CPC": c_cpc, "CPC 증감": pct_cpc, "CPC 차이": diff_cpc,
            "위시리스트수": c_wish, "위시리스트 증감": pct_wish, "위시리스트 차이": diff_wish,
            "장바구니 담기수": c_cart, "장바구니 증감": pct_cart, "장바구니 차이": diff_cart,
            "장바구니 매출액": c_csales,
            "구매완료수": c_conv, "구매 증감": pct_conv, "구매 차이": diff_conv,
            "구매완료 매출": c_sales, "구매 매출 증감": pct_sales, "구매 매출 차이": diff_sales,
            "구매 ROAS(%)": c_roas, "구매 ROAS 증감": c_roas - b_roas,
            "총 전환수": c_tot_conv, "총 전환 증감": pct_tot_conv, "총 전환 차이": diff_tot_conv,
            "총 전환매출": c_tot_sales, "총 매출 증감": pct_tot_sales, "총 매출 차이": diff_tot_sales,
            "통합 ROAS(%)": c_troas, "통합 ROAS 증감": c_troas - b_troas
        })
    return pd.DataFrame(table_data).sort_values("광고비", ascending=False)


def _build_ts_df(df, group_col, group_label):
    if df is None or df.empty:
        return pd.DataFrame()

    grp_cols = ['imp', 'clk', 'cost', 'wishlist_conv', 'wishlist_sales', 'cart_conv', 'cart_sales', 'conv', 'sales']
    if 'tot_conv' in df.columns:
        grp_cols.extend(['tot_conv', 'tot_sales'])

    grp = df.groupby(group_col)[[c for c in grp_cols if c in df.columns]].sum().reset_index()
    table_data = []
    for _, row in grp.iterrows():
        c_imp, c_clk, c_cost = row.get('imp', 0), row.get('clk', 0), row.get('cost', 0)
        c_wish, c_wsales = row.get('wishlist_conv', 0), row.get('wishlist_sales', 0)
        c_cart, c_csales = row.get('cart_conv', 0), row.get('cart_sales', 0)
        c_conv, c_sales = row.get('conv', 0), row.get('sales', 0)
        c_tot_conv = row.get('tot_conv', c_conv + c_cart + c_wish)
        c_tot_sales = row.get('tot_sales', c_sales + c_csales + c_wsales)
        c_cpc = (c_cost / c_clk) if c_clk > 0 else 0
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0
        table_data.append({
            group_label: row[group_col],
            "노출수": c_imp, "클릭수": c_clk, "광고비": c_cost, "CPC": c_cpc,
            "위시리스트수": c_wish, "장바구니 담기수": c_cart,
            "구매완료수": c_conv, "구매완료 매출": c_sales, "구매 ROAS(%)": c_roas,
            "총 전환수": c_tot_conv, "총 전환매출": c_tot_sales, "통합 ROAS(%)": c_troas
        })
    return pd.DataFrame(table_data)



def _build_ts_compare_df(cur_df, base_df, group_col, group_label, align_mode="label"):
    cur_view = _build_ts_df(cur_df, group_col, group_label)
    if cur_view.empty:
        return pd.DataFrame()

    base_view = _build_ts_df(base_df, group_col, group_label) if base_df is not None and not base_df.empty else pd.DataFrame()

    if align_mode == "sequence":
        cur_view = cur_view.reset_index(drop=True).copy()
        base_view = base_view.reset_index(drop=True).copy() if not base_view.empty else base_view
        cur_view["_seq"] = range(len(cur_view))
        if not base_view.empty:
            base_view["_seq"] = range(len(base_view))
        merge_key = "_seq"
    else:
        merge_key = group_label

    if not base_view.empty:
        merged = pd.merge(cur_view, base_view, on=merge_key, how="left", suffixes=("", "_base"))
    else:
        merged = cur_view.copy()
        for c in cur_view.columns:
            if c != merge_key:
                merged[f"{c}_base"] = 0

    for metric in ["노출수", "클릭수", "광고비", "CPC", "위시리스트수", "장바구니 담기수", "구매완료수", "구매완료 매출", "구매 ROAS(%)", "총 전환수", "총 전환매출", "통합 ROAS(%)"]:
        if metric in merged.columns:
            base_col = f"{metric}_base"
            if base_col not in merged.columns:
                merged[base_col] = 0

    diff_pairs = [
        ("노출수", "노출 증감"),
        ("클릭수", "클릭 증감"),
        ("광고비", "광고비 증감"),
        ("CPC", "CPC 증감"),
        ("위시리스트수", "위시리스트 증감"),
        ("장바구니 담기수", "장바구니 증감"),
        ("구매완료수", "구매 증감"),
        ("구매완료 매출", "구매 매출 증감"),
        ("구매 ROAS(%)", "구매 ROAS 증감"),
        ("총 전환수", "총 전환 증감"),
        ("총 전환매출", "총 매출 증감"),
        ("통합 ROAS(%)", "통합 ROAS 증감"),
    ]
    for cur_col, diff_col in diff_pairs:
        if cur_col in merged.columns:
            base_col = f"{cur_col}_base"
            merged[diff_col] = pd.to_numeric(merged[cur_col], errors="coerce").fillna(0) - pd.to_numeric(merged.get(base_col, 0), errors="coerce").fillna(0)

    if align_mode == "sequence" and "_seq" in merged.columns:
        merged = merged.drop(columns=["_seq"])

    return merged


def _delta_chip(cur_val, base_val, improve_when_up=True):
    diff = pct_change(float(cur_val or 0), float(base_val or 0)) if base_val is not None else 0.0
    if abs(diff) < 5:
        cls = "neu"
        text = f"유지 ({diff:+.1f}%)"
    else:
        improved = diff > 0 if improve_when_up else diff < 0
        cls = "pos" if improved else "neg"
        text = pct_to_arrow(diff)
    return cls, text


def _render_kpi_group(title: str, items: list[dict]) -> str:
    cells = []
    for item in items:
        cls, text = _delta_chip(item["cur"], item["base"], item.get("improve_when_up", True))
        cells.append(
            f"<div class='ov-kpi-cell'>"
            f"<div class='ov-kpi-label'>{item['label']}</div>"
            f"<div class='ov-kpi-value' title='{item['value']}'>{item['value']}</div>"
            f"<div class='ov-kpi-delta {cls}'>{text}</div>"
            f"</div>"
        )
    return f"<div class='ov-kpi-panel'><div class='ov-kpi-title'>{title}</div><div class='ov-kpi-cells'>{''.join(cells)}</div></div>"


def _normalize_type_label(val) -> str:
    s = str(val or "").strip().upper()
    if not s:
        return ""
    if "쇼핑" in s or "SHOPPING" in s:
        return "쇼핑검색"
    if "파워링크" in s or "WEB_SITE" in s:
        return "파워링크"
    if "브랜드" in s or "BRAND" in s:
        return "브랜드검색"
    if "POWER_CONTENTS" in s or "파워컨텐츠" in s:
        return "파워컨텐츠"
    if "PLACE" in s or "플레이스" in s:
        return "플레이스"
    return str(val).strip()


def _infer_kpi_mode(type_sel: tuple, cur_camp: pd.DataFrame, is_split_only: bool) -> str:
    # 구매완료 KPI는 쇼핑검색 post-split(3/11 이후) 단독 조회일 때만 사용
    labels = {_normalize_type_label(x) for x in type_sel if str(x).strip()}

    if not labels and cur_camp is not None and not cur_camp.empty:
        for col in ["campaign_type_label", "campaign_type", "campaign_tp", "캠페인유형"]:
            if col in cur_camp.columns:
                vals = cur_camp[col].dropna().astype(str).tolist()
                labels = {_normalize_type_label(v) for v in vals if str(v).strip()}
                if labels:
                    break

    labels = {x for x in labels if x}
    if is_split_only and labels and labels == {"쇼핑검색"}:
        return "shopping_purchase"
    return "generic_conversion"


def _format_compact_currency(value: float) -> str:
    try:
        v = float(value or 0)
    except Exception:
        return "0원"
    abs_v = abs(v)
    if abs_v >= 100000000:
        return f"{v / 100000000:.2f}억"
    if abs_v >= 10000:
        return f"{v / 10000:.1f}만원"
    return f"{int(round(v)):,}원"


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f:
        return

    _inject_overview_css()

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    with st.spinner("데이터를 집계 중입니다..."):
        cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
        base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)
        cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
        base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
        kw_bundle = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
        daily_ts = _cached_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        base_daily_ts = _cached_campaign_timeseries(engine, b1, b2, cids, type_sel)

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    patch_date = date(2026, 3, 11)
    is_legacy_only = f["end"] < patch_date
    is_split_only = f["start"] >= patch_date
    is_mixed_period = (f["start"] < patch_date <= f["end"])
    combined_toggle = not is_split_only
    auto_kpi_mode = _infer_kpi_mode(type_sel, cur_camp, is_split_only)

    head_col_meta, head_col_toggle = st.columns([5.2, 1.6])
    with head_col_meta:
        st.markdown(
            f"<div class='ov-headbar'>"
            f"<div class='ov-headmeta'>"
            f"<div class='ov-chip primary'>{selected_type_label}</div>"
            f"<div class='ov-chip muted'>{f['start']} ~ {f['end']}</div>"
            f"<div class='ov-chip muted'>{cmp_mode} · {b1} ~ {b2}</div>"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    can_use_purchase_toggle = is_split_only

    with head_col_toggle:
        purchase_view = st.toggle(
            "구매완료 데이터로 보기",
            value=(auto_kpi_mode == "shopping_purchase"),
            key="overview_purchase_view_toggle",
            disabled=not can_use_purchase_toggle,
        )

    if is_mixed_period:
        st.info("안내: 3월 11일 이전 및 이후 데이터가 혼재되어 있어, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")
    elif is_legacy_only:
        st.info("안내: 3월 11일 이전 데이터 조회 시, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")

    if not is_split_only:
        st.caption("구매완료 데이터 보기는 분리 전환 데이터가 있는 기간에서만 사용할 수 있습니다.")

    cur = cur_summary or {}
    base = base_summary or {}

    cur['tot_conv'] = cur.get('tot_conv', cur.get('conv', 0))
    cur['tot_sales'] = cur.get('tot_sales', cur.get('sales', 0))
    cur['tot_roas'] = (cur['tot_sales'] / cur['cost'] * 100) if cur.get('cost', 0) > 0 else 0
    cur['cpm'] = (cur.get('cost', 0) / cur.get('imp', 0) * 1000) if cur.get('imp', 0) > 0 else 0
    cur['tot_cvr'] = (cur['tot_conv'] / cur['clk'] * 100) if cur.get('clk', 0) > 0 else 0
    cur['tot_cpa'] = (cur['cost'] / cur['tot_conv']) if cur.get('tot_conv', 0) > 0 else 0

    base['tot_conv'] = base.get('tot_conv', base.get('conv', 0))
    base['tot_sales'] = base.get('tot_sales', base.get('sales', 0))
    base['tot_roas'] = (base['tot_sales'] / base['cost'] * 100) if base.get('cost', 0) > 0 else 0
    base['cpm'] = (base.get('cost', 0) / base.get('imp', 0) * 1000) if base.get('imp', 0) > 0 else 0
    base['tot_cvr'] = (base['tot_conv'] / base['clk'] * 100) if base.get('clk', 0) > 0 else 0
    base['tot_cpa'] = (base['cost'] / base['tot_conv']) if base.get('tot_conv', 0) > 0 else 0

    kpi_mode = "shopping_purchase" if (purchase_view and is_split_only) else "generic_conversion"

    inflow_items = [
        {"label": "노출수", "value": format_number_commas(cur.get("imp", 0.0)), "cur": cur.get("imp", 0), "base": base.get("imp", 0)},
        {"label": "클릭수", "value": format_number_commas(cur.get("clk", 0.0)), "cur": cur.get("clk", 0), "base": base.get("clk", 0)},
        {"label": "클릭률", "value": f"{float(cur.get('ctr', 0.0) or 0.0):.1f}%", "cur": cur.get("ctr", 0), "base": base.get("ctr", 0)},
    ]
    cost_items = [
        {"label": "광고비", "value": _format_compact_currency(cur.get("cost", 0.0)), "cur": cur.get("cost", 0), "base": base.get("cost", 0), "improve_when_up": False},
        {"label": "CPC", "value": format_currency(cur.get("cpc", 0.0)), "cur": cur.get("cpc", 0), "base": base.get("cpc", 0), "improve_when_up": False},
        {"label": "CPM", "value": format_currency(cur.get("cpm", 0.0)), "cur": cur.get("cpm", 0), "base": base.get("cpm", 0), "improve_when_up": False},
    ]
    if kpi_mode == "shopping_purchase":
        perf_items = [
            {"label": "구매 ROAS", "value": f"{float(cur.get('roas', 0.0) or 0.0):.1f}%", "cur": cur.get("roas", 0), "base": base.get("roas", 0)},
            {"label": "구매완료수", "value": f"{float(cur.get('conv', 0.0)):.0f}", "cur": cur.get("conv", 0), "base": base.get("conv", 0)},
            {"label": "구매완료 매출", "value": _format_compact_currency(cur.get("sales", 0.0)), "cur": cur.get("sales", 0), "base": base.get("sales", 0)},
        ]
    else:
        perf_items = [
            {"label": "총 ROAS", "value": f"{float(cur.get('tot_roas', 0.0) or 0.0):.1f}%", "cur": cur.get("tot_roas", 0), "base": base.get("tot_roas", 0)},
            {"label": "총 전환수", "value": f"{float(cur.get('tot_conv', 0.0)):.0f}", "cur": cur.get("tot_conv", 0), "base": base.get("tot_conv", 0)},
            {"label": "총 전환매출", "value": _format_compact_currency(cur.get("tot_sales", 0.0)), "cur": cur.get("tot_sales", 0), "base": base.get("tot_sales", 0)},
        ]

    st.markdown(
        f"<div class='ov-kpi-grid'>"
        f"{_render_kpi_group('유입 지표', inflow_items)}"
        f"{_render_kpi_group('비용 지표', cost_items)}"
        f"{_render_kpi_group('성과 지표', perf_items)}"
        f"</div>",
        unsafe_allow_html=True,
    )

    with st.container(border=True):
        st.markdown("<div class='nv-sec-title' style='margin-top:0;'>일자별 성과 추이</div>", unsafe_allow_html=True)
        if daily_ts is not None and not daily_ts.empty:
            expected_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_sales', 'tot_conv']
            for c in expected_cols:
                if c not in daily_ts.columns:
                    daily_ts[c] = 0.0
            daily_ts_chart = daily_ts.groupby('dt')[expected_cols].sum().reset_index()
            tab_t1, tab_t2 = st.tabs(["비용 및 매출 추이", "유입 지표 추이"])
            with tab_t1:
                if combined_toggle:
                    render_echarts_dual_axis("비용 및 총 전환 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "tot_sales", "매출", height=320)
                else:
                    render_echarts_dual_axis("비용 및 구매 완료 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "sales", "매출", height=320)
            with tab_t2:
                render_echarts_dual_axis("노출 및 클릭 추이", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
        else:
            st.info("선택한 기간의 일자별 트렌드 데이터가 존재하지 않습니다.")

    with st.container(border=True):
        st.markdown("<div class='nv-sec-title' style='margin-top:0;'>캠페인별 목표 달성 현황</div>", unsafe_allow_html=True)
        show_integ_roas = st.toggle("🔄 통합 ROAS 수치 함께 보기 (장바구니, 위시리스트 포함)", value=False, key="ov_toggle_roas")
        if not cur_camp.empty and "target_roas" in cur_camp.columns and "min_roas" in cur_camp.columns:
            target_df = cur_camp.copy()
            target_df["target_roas"] = pd.to_numeric(target_df["target_roas"], errors="coerce").fillna(0.0)
            target_df["min_roas"] = pd.to_numeric(target_df["min_roas"], errors="coerce").fillna(0.0)
            target_df = target_df[(target_df["target_roas"] > 0) | (target_df["min_roas"] > 0)]
            if not target_df.empty:
                target_df["cost"] = pd.to_numeric(target_df.get("cost", 0), errors="coerce").fillna(0.0)
                target_df["sales"] = pd.to_numeric(target_df.get("sales", 0), errors="coerce").fillna(0.0)
                target_df["tot_sales"] = pd.to_numeric(target_df.get("tot_sales", 0), errors="coerce").fillna(0.0)

                target_df["base_roas"] = np.where(target_df["target_roas"] > 0, target_df["target_roas"], target_df["min_roas"])
                target_df["c_roas_purch"] = np.where(target_df["cost"] > 0, (target_df["sales"] / target_df["cost"]) * 100, 0.0)
                target_df["c_roas_integ"] = np.where(target_df["cost"] > 0, (target_df["tot_sales"] / target_df["cost"]) * 100, 0.0)
                target_df["achieve_raw"] = np.where(target_df["base_roas"] > 0, (target_df["c_roas_purch"] / target_df["base_roas"]) * 100, 0.0)
                target_df["achieve"] = target_df["achieve_raw"].clip(upper=100.0)
                target_df["achieve_diff"] = target_df["achieve_raw"] - 100.0
                target_df["status"] = np.where(
                    (target_df["target_roas"] > 0) & (target_df["c_roas_purch"] > target_df["target_roas"]), "초과 달성",
                    np.where(
                        (target_df["target_roas"] > 0) & (target_df["c_roas_purch"] == target_df["target_roas"]), "목표 달성",
                        np.where(
                            (target_df["min_roas"] > 0) & (target_df["c_roas_purch"] >= target_df["min_roas"]), "최소 달성",
                            "미달"
                        )
                    )
                )
                target_df["card_color"] = np.where(
                    target_df["status"].isin(["초과 달성", "목표 달성"]), "#0528F2",
                    np.where(target_df["status"] == "최소 달성", "#10B981", "#F79009")
                )

                ctrl1, ctrl2 = st.columns([1, 1.2])
                only_miss = ctrl1.checkbox("미달만 보기", value=False, key="ov_target_only_miss")
                sort_mode = ctrl2.selectbox(
                    "정렬 기준",
                    ["광고비 순", "이름 순", "달성격차 순"],
                    index=0,
                    key="ov_target_sort_mode"
                )

                if only_miss:
                    target_df = target_df[target_df["status"] == "미달"]

                if sort_mode == "이름 순":
                    target_df = target_df.sort_values(by="campaign_name", ascending=True)
                elif sort_mode == "달성격차 순":
                    target_df = target_df.sort_values(by="achieve_diff", ascending=False)
                else:
                    target_df = target_df.sort_values(by="cost", ascending=False)

                if not target_df.empty:
                    cards = ["<div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:16px; align-items:start;'>"]
                    for _, row in target_df.iterrows():
                        camp_name = row["campaign_name"]
                        t_roas = float(row["target_roas"])
                        m_roas = float(row["min_roas"])
                        c_roas_purch = float(row["c_roas_purch"])
                        c_roas_integ = float(row["c_roas_integ"])
                        achieve = float(row["achieve"])
                        achieve_diff = float(row["achieve_diff"])
                        color = row["card_color"]
                        status = row["status"]
                        integ_html = (
                            f"<div style='font-size:12px; color:var(--nv-muted); margin-top:8px;'>현재(통합) {c_roas_integ:,.1f}%</div>"
                            if show_integ_roas else ""
                        )
                        cards.append(
                            f"<div style='background:var(--nv-bg); border:1px solid var(--nv-line); border-radius:12px; padding:16px;'>"
                            f"<div style='display:flex; justify-content:space-between; gap:12px; align-items:flex-start;'>"
                            f"<div style='font-weight:700; line-height:1.4; color:var(--nv-text);'>{camp_name}</div>"
                            f"<div style='text-align:right; white-space:nowrap;'><div style='font-size:18px; font-weight:800; color:{color};'>{achieve_diff:+,.1f}%</div><div style='font-size:12px; color:{color}; font-weight:700;'>{status}</div></div>"
                            f"</div>"
                            f"<div style='height:8px; background:var(--nv-surface); border-radius:999px; overflow:hidden; margin:12px 0;'><div style='width:{achieve}%; height:100%; background:{color};'></div></div>"
                            f"<div style='font-size:13px; color:var(--nv-text); font-weight:700;'>현재(구매) {c_roas_purch:,.1f}%</div>"
                            f"<div style='font-size:12px; color:var(--nv-muted); margin-top:6px;'>최소 {m_roas:,.0f}% · 목표 {t_roas:,.0f}%</div>"
                            f"{integ_html}"
                            f"</div>"
                        )
                    cards.append("</div>")
                    st.markdown("".join(cards), unsafe_allow_html=True)
                    st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
                else:
                    st.info("조건에 맞는 캠페인이 없습니다.")
            else:
                st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다. 설정 메뉴에서 계정별 목표를 지정해주세요.")
        else:
            st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다. 설정 메뉴에서 계정별 목표를 지정해주세요.")

    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if not cur_camp.empty or not base_camp.empty:
        if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
            mapping = dict(zip(meta['customer_id'].astype(str), meta['account_name']))
            if not cur_camp.empty:
                cur_camp['account_name'] = cur_camp['customer_id'].astype(str).map(mapping).fillna(cur_camp['customer_id'].astype(str))
            if not base_camp.empty:
                base_camp['account_name'] = base_camp['customer_id'].astype(str).map(mapping).fillna(base_camp['customer_id'].astype(str))
        else:
            if not cur_camp.empty:
                cur_camp['account_name'] = cur_camp['customer_id'].astype(str)
            if not base_camp.empty:
                base_camp['account_name'] = base_camp['customer_id'].astype(str)

        df_display = _build_comparison_df(cur_camp, base_camp, 'account_name', '계정명')
        type_kor_map = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        type_col = 'campaign_tp' if 'campaign_tp' in cur_camp.columns else ('campaign_type' if 'campaign_type' in cur_camp.columns else None)
        if type_col:
            df_type_display = _build_comparison_df(cur_camp, base_camp, type_col, '캠페인 유형', type_kor_map)
        camp_col = 'campaign_name' if 'campaign_name' in cur_camp.columns else None
        if camp_col:
            camp_disp = _build_comparison_df(cur_camp, base_camp, camp_col, '캠페인명')

    if daily_ts is not None and not daily_ts.empty:
        daily_copy = daily_ts.copy()
        base_daily_copy = base_daily_ts.copy() if base_daily_ts is not None and not base_daily_ts.empty else pd.DataFrame()

        daily_copy['일자'] = daily_copy['dt'].dt.strftime('%Y-%m-%d')
        if not base_daily_copy.empty:
            base_daily_copy['일자'] = base_daily_copy['dt'].dt.strftime('%Y-%m-%d')
        daily_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '일자', '일자', align_mode="sequence").sort_values('일자', ascending=False)

        daily_copy['요일'] = daily_copy['dt'].dt.dayofweek
        if not base_daily_copy.empty:
            base_daily_copy['요일'] = base_daily_copy['dt'].dt.dayofweek
        dow_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '요일', '요일', align_mode="label").sort_values('요일')
        dow_map = {0: '월요일', 1: '화요일', 2: '수요일', 3: '목요일', 4: '금요일', 5: '토요일', 6: '일요일'}
        dow_disp['요일명'] = dow_disp['요일'].map(dow_map)

        daily_copy['주차'] = daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        if not base_daily_copy.empty:
            base_daily_copy['주차'] = base_daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        weekly_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '주차', '주차', align_mode="sequence").sort_values('주차', ascending=False)

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
        "CPC": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
        "위시리스트수": "{:,.0f}", "위시리스트 증감": "{:+.1f}%", "위시리스트 차이": "{:+,.0f}",
        "장바구니 담기수": "{:,.0f}", "장바구니 증감": "{:+.1f}%", "장바구니 차이": "{:+,.0f}",
        "구매완료수": "{:,.0f}", "구매 증감": "{:+.1f}%", "구매 차이": "{:+,.0f}",
        "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.1f}%", "구매 매출 차이": "{:+,.0f}원",
        "구매 ROAS(%)": "{:,.1f}%", "구매 ROAS 증감": "{:+.1f}%",
        "총 전환수": "{:,.0f}", "총 전환 증감": "{:+.1f}%", "총 전환 차이": "{:+,.0f}",
        "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.1f}%", "총 매출 차이": "{:+,.0f}원",
        "통합 ROAS(%)": "{:,.1f}%", "통합 ROAS 증감": "{:+.1f}%"
    }
    fmt_dict_ts = {
        "노출수": "{:,.0f}", "클릭수": "{:,.0f}", "광고비": "{:,.0f}원", "CPC": "{:,.0f}원",
        "위시리스트수": "{:,.0f}", "장바구니 담기수": "{:,.0f}",
        "구매완료수": "{:,.0f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
        "총 전환수": "{:,.0f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%",
        "노출 증감": "{:+,.0f}", "클릭 증감": "{:+,.0f}", "광고비 증감": "{:+,.0f}원", "CPC 증감": "{:+,.0f}원",
        "위시리스트 증감": "{:+,.0f}", "장바구니 증감": "{:+,.0f}", "구매 증감": "{:+,.0f}",
        "구매 매출 증감": "{:+,.0f}원", "구매 ROAS 증감": "{:+.1f}%",
        "총 전환 증감": "{:+,.0f}", "총 매출 증감": "{:+,.0f}원", "통합 ROAS 증감": "{:+.1f}%"
    }

    has_data_to_export = any([not df_display.empty, not df_type_display.empty, not camp_disp.empty, not daily_disp.empty])
    if has_data_to_export:
        with st.container(border=True):
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>내보내기</div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:12px; color:var(--nv-muted); margin-bottom:10px;'>계정/유형/캠페인/일자 상세 데이터를 한 번에 엑셀로 내려받습니다.</div>", unsafe_allow_html=True)
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer) as writer:
                if not df_display.empty:
                    format_for_csv(df_display).to_excel(writer, sheet_name='계정별_성과상세', index=False)
                if not df_type_display.empty:
                    format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과상세', index=False)
                if not camp_disp.empty:
                    format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_성과상세', index=False)
                if not daily_disp.empty:
                    format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_성과상세', index=False)
                if not dow_disp.empty:
                    dow_export = dow_disp.drop(columns=['요일']) if '요일' in dow_disp.columns else dow_disp
                    format_for_csv(dow_export).to_excel(writer, sheet_name='요일별_성과상세', index=False)
                if not weekly_disp.empty:
                    format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_성과상세', index=False)
            st.download_button(
                label="통합 데이터 전체 다운로드",
                data=excel_buffer.getvalue(),
                file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    def _display_ts_table(df, col_name, toggle_state_val):
        if df.empty:
            return
        if toggle_state_val:
            cols = [
                col_name,
                "노출수", "노출 증감",
                "클릭수", "클릭 증감",
                "광고비", "광고비 증감",
                "CPC", "CPC 증감",
                "총 전환수", "총 전환 증감",
                "총 전환매출", "총 매출 증감",
                "통합 ROAS(%)", "통합 ROAS 증감",
            ]
        else:
            cols = [
                col_name,
                "노출수", "노출 증감",
                "클릭수", "클릭 증감",
                "광고비", "광고비 증감",
                "CPC", "CPC 증감",
                "위시리스트수", "위시리스트 증감",
                "장바구니 담기수", "장바구니 증감",
                "구매완료수", "구매 증감",
                "구매완료 매출", "구매 매출 증감",
                "구매 ROAS(%)", "구매 ROAS 증감",
            ]
        cols = [c for c in cols if c in df.columns]
        disp_ts = df[cols].copy()
        styled_ts = disp_ts.style.format(fmt_dict_ts)
        styled_ts = _apply_overview_delta_styles(styled_ts, disp_ts)
        _render_overview_sticky_table(styled_ts, col_name, height=420, hide_index=True)

    if not df_display.empty:
        with st.expander("계정별 성과 상세", expanded=False):
            styled_df = df_display.style.format(fmt_dict_standard)
            styled_df = _apply_overview_delta_styles(styled_df, df_display)
            _render_overview_sticky_table(styled_df, "계정명", height=420, hide_index=True)

    if not df_type_display.empty:
        with st.expander("캠페인 유형별 성과 상세", expanded=False):
            styled_type_df = df_type_display.style.format(fmt_dict_standard)
            styled_type_df = _apply_overview_delta_styles(styled_type_df, df_type_display)
            _render_overview_sticky_table(styled_type_df, "캠페인 유형", height=420, hide_index=True)

    if not camp_disp.empty:
        with st.expander("캠페인별 성과 상세", expanded=False):
            styled_camp_df = camp_disp.style.format(fmt_dict_standard)
            styled_camp_df = _apply_overview_delta_styles(styled_camp_df, camp_disp)
            _render_overview_sticky_table(styled_camp_df, "캠페인명", height=460, hide_index=True)

    if not daily_disp.empty:
        with st.expander("일자별 성과 상세", expanded=False):
            _display_ts_table(daily_disp, "일자", combined_toggle)

    if not dow_disp.empty:
        with st.expander("요일별 성과 상세", expanded=False):
            _display_ts_table(dow_disp, "요일명", combined_toggle)

    if not weekly_disp.empty:
        with st.expander("주간 성과 상세", expanded=False):
            _display_ts_table(weekly_disp, "주차", combined_toggle)

    with st.expander("텍스트 보고서 내보내기", expanded=False):
        top_kw_str = "없음"
        if kw_bundle is not None and not kw_bundle.empty and "keyword" in kw_bundle.columns and "clk" in kw_bundle.columns:
            kw_agg = kw_bundle.groupby("keyword")["clk"].sum().reset_index()
            top_kws = kw_agg[kw_agg["clk"] > 0].sort_values("clk", ascending=False).head(5)
            if not top_kws.empty:
                top_kw_str = ", ".join([f"{row['keyword']}({int(row['clk']):,}회)" for _, row in top_kws.iterrows()])

        if combined_toggle:
            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("총 전환수", f"{float(cur.get('tot_conv', 0.0)):.1f}"),
                _format_report_line("총 전환매출", f"{int(float(cur.get('tot_sales', 0))):,}원"),
                _format_report_line("통합 ROAS", f"{float(cur.get('tot_roas', 0)):.1f}%"),
                _format_report_line("주요 유입 키워드", top_kw_str)
            ])
        else:
            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 (상세) ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("구매완료수", f"{float(cur.get('conv', 0.0)):.1f}"),
                _format_report_line("구매완료 매출", f"{int(float(cur.get('sales', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(cur.get('roas', 0)):.1f}%"),
                _format_report_line("주요 유입 키워드", top_kw_str)
            ])
        st.code(report_text, language="text")
