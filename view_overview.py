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
    .ov-summary-bar {
        display:flex;
        flex-wrap:wrap;
        gap:10px;
        margin-bottom:16px;
    }
    .ov-chip {
        background: var(--nv-primary-soft);
        color: var(--nv-primary);
        border-radius: 999px;
        padding: 6px 12px;
        font-size: 12px;
        font-weight: 700;
    }
    .ov-chip.muted {
        background: var(--nv-surface);
        color: var(--nv-muted);
        border: 1px solid var(--nv-line);
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
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.1f}" if pd.notnull(x) else "0.0")
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

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='ov-summary-bar'>"
        f"<div class='ov-chip'>{selected_type_label}</div>"
        f"<div class='ov-chip muted'>조회 기간 {f['start']} ~ {f['end']}</div>"
        f"<div class='ov-chip muted'>{cmp_mode} · {b1} ~ {b2}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    patch_date = date(2026, 3, 11)
    is_legacy_only = f["end"] < patch_date
    is_split_only = f["start"] >= patch_date
    is_mixed_period = (f["start"] < patch_date <= f["end"])
    combined_toggle = not is_split_only

    if is_mixed_period:
        st.info("안내: 3월 11일 이전 및 이후 데이터가 혼재되어 있어, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")
    elif is_legacy_only:
        st.info("안내: 3월 11일 이전 데이터 조회 시, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")

    cur = cur_summary or {}
    base = base_summary or {}

    cur['tot_conv'] = cur.get('tot_conv', cur.get('conv', 0))
    cur['tot_sales'] = cur.get('tot_sales', cur.get('sales', 0))
    cur['tot_roas'] = (cur['tot_sales'] / cur['cost'] * 100) if cur.get('cost', 0) > 0 else 0
    cur['cpm'] = (cur.get('cost', 0) / cur.get('imp', 0) * 1000) if cur.get('imp', 0) > 0 else 0

    base['tot_conv'] = base.get('tot_conv', base.get('conv', 0))
    base['tot_sales'] = base.get('tot_sales', base.get('sales', 0))
    base['tot_roas'] = (base['tot_sales'] / base['cost'] * 100) if base.get('cost', 0) > 0 else 0
    base['cpm'] = (base.get('cost', 0) / base.get('imp', 0) * 1000) if base.get('imp', 0) > 0 else 0

    inflow_items = [
        {"label": "노출수", "value": format_number_commas(cur.get("imp", 0.0)), "cur": cur.get("imp", 0), "base": base.get("imp", 0)},
        {"label": "클릭수", "value": format_number_commas(cur.get("clk", 0.0)), "cur": cur.get("clk", 0), "base": base.get("clk", 0)},
        {"label": "클릭률", "value": f"{float(cur.get('ctr', 0.0) or 0.0):.1f}%", "cur": cur.get("ctr", 0), "base": base.get("ctr", 0)},
    ]
    cost_items = [
        {"label": "광고비", "value": format_currency(cur.get("cost", 0.0)), "cur": cur.get("cost", 0), "base": base.get("cost", 0), "improve_when_up": False},
        {"label": "CPC", "value": format_currency(cur.get("cpc", 0.0)), "cur": cur.get("cpc", 0), "base": base.get("cpc", 0), "improve_when_up": False},
        {"label": "CPM", "value": format_currency(cur.get("cpm", 0.0)), "cur": cur.get("cpm", 0), "base": base.get("cpm", 0), "improve_when_up": False},
    ]
    if combined_toggle:
        perf_items = [
            {"label": "통합 ROAS", "value": f"{float(cur.get('tot_roas', 0.0) or 0.0):.1f}%", "cur": cur.get("tot_roas", 0), "base": base.get("tot_roas", 0)},
            {"label": "총 전환수", "value": f"{float(cur.get('tot_conv', 0.0)):.1f}", "cur": cur.get("tot_conv", 0), "base": base.get("tot_conv", 0)},
            {"label": "총 전환매출", "value": format_currency(cur.get("tot_sales", 0.0)), "cur": cur.get("tot_sales", 0), "base": base.get("tot_sales", 0)},
        ]
    else:
        perf_items = [
            {"label": "구매 ROAS", "value": f"{float(cur.get('roas', 0.0) or 0.0):.1f}%", "cur": cur.get("roas", 0), "base": base.get("roas", 0)},
            {"label": "구매완료수", "value": f"{float(cur.get('conv', 0.0)):.1f}", "cur": cur.get("conv", 0), "base": base.get("conv", 0)},
            {"label": "구매완료 매출", "value": format_currency(cur.get("sales", 0.0)), "cur": cur.get("sales", 0), "base": base.get("sales", 0)},
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
                cards = ["<div style='display:grid; grid-template-columns:repeat(auto-fill, minmax(320px, 1fr)); gap:16px;'>"]
                for _, row in target_df.sort_values(by="cost", ascending=False).iterrows():
                    camp_name = row["campaign_name"]
                    t_roas = float(row["target_roas"])
                    m_roas = float(row["min_roas"])
                    cost = float(row.get("cost", 0.0))
                    sales_purch = float(row.get("sales", 0.0))
                    sales_integ = float(row.get("tot_sales", 0.0))
                    c_roas_purch = (sales_purch / cost * 100) if cost > 0 else 0.0
                    c_roas_integ = (sales_integ / cost * 100) if cost > 0 else 0.0
                    base_roas = t_roas if t_roas > 0 else m_roas
                    achieve_raw = (c_roas_purch / base_roas * 100) if base_roas > 0 else 0.0
                    achieve = min(achieve_raw, 100.0)
                    achieve_diff = achieve_raw - 100.0
                    if t_roas > 0 and c_roas_purch > t_roas:
                        color, status = "#0528F2", "초과 달성"
                    elif t_roas > 0 and c_roas_purch == t_roas:
                        color, status = "#0528F2", "목표 달성"
                    elif m_roas > 0 and c_roas_purch >= m_roas:
                        color, status = "#10B981", "최소 달성"
                    else:
                        color, status = "#F79009", "미달"
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
        daily_copy['일자'] = daily_copy['dt'].dt.strftime('%Y-%m-%d')
        daily_disp = _build_ts_df(daily_copy, '일자', '일자').sort_values('일자', ascending=False)
        daily_copy['요일'] = daily_copy['dt'].dt.dayofweek
        dow_disp = _build_ts_df(daily_copy, '요일', '요일').sort_values('요일')
        dow_map = {0: '월요일', 1: '화요일', 2: '수요일', 3: '목요일', 4: '금요일', 5: '토요일', 6: '일요일'}
        dow_disp['요일명'] = dow_disp['요일'].map(dow_map)
        daily_copy['주차'] = daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        weekly_disp = _build_ts_df(daily_copy, '주차', '주차').sort_values('주차', ascending=False)

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
        "CPC": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
        "장바구니 담기수": "{:,.1f}", "장바구니 증감": "{:+.1f}%", "장바구니 차이": "{:+,.1f}",
        "구매완료수": "{:,.1f}", "구매 증감": "{:+.1f}%", "구매 차이": "{:+,.1f}",
        "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.1f}%", "구매 매출 차이": "{:+,.0f}원",
        "구매 ROAS(%)": "{:,.1f}%", "구매 ROAS 증감": "{:+.1f}%",
        "총 전환수": "{:,.1f}", "총 전환 증감": "{:+.1f}%", "총 전환 차이": "{:+,.1f}",
        "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.1f}%", "총 매출 차이": "{:+,.0f}원",
        "통합 ROAS(%)": "{:,.1f}%", "통합 ROAS 증감": "{:+.1f}%"
    }
    fmt_dict_ts = {
        "노출수": "{:,.0f}", "클릭수": "{:,.0f}", "광고비": "{:,.0f}원", "CPC": "{:,.0f}원",
        "위시리스트수": "{:,.1f}", "장바구니 담기수": "{:,.1f}",
        "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
        "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%"
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
            cols = [col_name, "노출수", "클릭수", "광고비", "CPC", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
        else:
            cols = [col_name, "노출수", "클릭수", "광고비", "CPC", "위시리스트수", "장바구니 담기수", "구매완료수", "구매완료 매출", "구매 ROAS(%)"]
        styled_ts = df[cols].style.format(fmt_dict_ts)
        _render_overview_sticky_table(styled_ts, col_name, height=420, hide_index=True)

    if not df_display.empty:
        with st.expander("계정별 성과 상세", expanded=False):
            styled_df = df_display.style.format(fmt_dict_standard)
            _render_overview_sticky_table(styled_df, "계정명", height=420, hide_index=True)

    if not df_type_display.empty:
        with st.expander("캠페인 유형별 성과 상세", expanded=False):
            styled_type_df = df_type_display.style.format(fmt_dict_standard)
            _render_overview_sticky_table(styled_type_df, "캠페인 유형", height=420, hide_index=True)

    if not camp_disp.empty:
        with st.expander("캠페인별 성과 상세", expanded=False):
            styled_camp_df = camp_disp.style.format(fmt_dict_standard)
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
