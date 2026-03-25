# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view (Hyper-Optimized & Fast Rendering)."""

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
    .ov-chip { background: transparent; color: var(--nv-text); border: 1px solid var(--nv-line); border-radius: 8px; padding: 5px 10px; font-size: 12px; font-weight: 600; line-height: 1.2; }
    .ov-chip.primary { background: var(--nv-primary-soft); color: var(--nv-primary); border-color: transparent; }
    .ov-chip.muted { color: var(--nv-muted); background: var(--nv-surface); }
    .ov-kpi-grid { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-bottom: 18px; }
    .ov-kpi-panel { background: var(--nv-bg); border: 1px solid var(--nv-line); border-radius: 12px; padding: 16px; }
    .ov-kpi-title { font-size: 13px; font-weight: 700; color: var(--nv-text); margin-bottom: 12px; }
    .ov-kpi-cells { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .ov-kpi-cell { background: var(--nv-surface); border-radius: 10px; padding: 12px; min-width: 0; }
    .ov-kpi-label { font-size: 12px; color: var(--nv-muted); margin-bottom: 6px; }
    .ov-kpi-value { font-size: 20px; font-weight: 800; color: var(--nv-text); line-height: 1.15; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ov-kpi-delta { margin-top: 8px; font-size: 11px; font-weight: 700; display:inline-flex; padding: 4px 8px; border-radius: 999px; }
    .ov-kpi-delta.pos { background: #E8F0FE; color: #1A73E8; }
    .ov-kpi-delta.neg { background: #FCE8E6; color: #EA4335; }
    .ov-kpi-delta.neu { background: var(--nv-surface); color: var(--nv-muted); border:1px solid var(--nv-line); }
    @media (max-width: 1100px) { .ov-kpi-grid, .ov-kpi-cells { grid-template-columns: 1fr; } }
    </style>
    """, unsafe_allow_html=True)


def _auto_table_height(df, default_height: int = 420, min_height: int = 72, max_height: int = 560) -> int:
    try:
        rows = len(df.index)
        if rows <= 0: return min_height
        if rows == 1: return 72
        if rows == 2: return 106
        return max(min_height, min(36 + rows * 34, max_height))
    except Exception: return default_height

# 통합 네이티브 컬럼 컨피그
FAST_TS_CONFIG = {
    "노출수": st.column_config.NumberColumn("노출수", format="%d"),
    "클릭수": st.column_config.NumberColumn("클릭수", format="%d"),
    "광고비": st.column_config.NumberColumn("광고비", format="%d 원"),
    "CPC": st.column_config.NumberColumn("CPC", format="%d 원"),
    "위시리스트수": st.column_config.NumberColumn("위시리스트수", format="%d"),
    "장바구니 담기수": st.column_config.NumberColumn("장바구니 담기수", format="%d"),
    "구매완료수": st.column_config.NumberColumn("구매완료수", format="%d"),
    "구매완료 매출": st.column_config.NumberColumn("구매완료 매출", format="%d 원"),
    "구매 ROAS(%)": st.column_config.NumberColumn("구매 ROAS(%)", format="%.1f %%"),
    "총 전환수": st.column_config.NumberColumn("총 전환수", format="%d"),
    "총 전환매출": st.column_config.NumberColumn("총 전환매출", format="%d 원"),
    "통합 ROAS(%)": st.column_config.NumberColumn("통합 ROAS(%)", format="%.1f %%"),
    "노출 증감": st.column_config.NumberColumn("노출 증감(%)", format="%+.1f %%"),
    "클릭 증감": st.column_config.NumberColumn("클릭 증감(%)", format="%+.1f %%"),
    "광고비 증감": st.column_config.NumberColumn("광고비 증감(%)", format="%+.1f %%"),
    "CPC 증감": st.column_config.NumberColumn("CPC 증감(%)", format="%+.1f %%"),
    "구매 증감": st.column_config.NumberColumn("구매 증감(%)", format="%+.1f %%"),
    "총 전환 증감": st.column_config.NumberColumn("총 전환 증감(%)", format="%+.1f %%"),
}

def _render_overview_sticky_table(df, first_col: str, height: int = 420, hide_index: bool = False):
    real_height = _auto_table_height(df, default_height=height, max_height=height)
    cfg = FAST_TS_CONFIG.copy()
    cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    st.dataframe(df, width="stretch", height=real_height, hide_index=hide_index, column_config=cfg)

def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel: return "전체 유형"
    return type_sel[0] if len(type_sel) == 1 else ", ".join(type_sel)

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=5000)
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=0)
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        ts = query_campaign_timeseries(_engine, start_dt, end_dt, cids, type_sel)
        return ts if ts is not None else pd.DataFrame()
    except Exception: return pd.DataFrame()


def format_for_csv(df):
    out_df = df.copy()
    for col in out_df.columns:
        if out_df[col].dtype in ['float64', 'int64']:
            if col in ["노출수", "클릭수", "평균순위", "순위", "장바구니 담기수", "위시리스트수", "구매완료수", "총 전환수"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["광고비", "구매완료 매출", "장바구니 매출액", "위시리스트 매출액", "총 전환매출", "CPC"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}원" if pd.notnull(x) else "0원")
            elif "차이" in col:
                if "광고비" in col or "매출" in col or "CPC" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                elif "노출" in col or "클릭" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
                else: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.1f}" if pd.notnull(x) and x != 0 else "0.0")
            elif "증감" in col or "ROAS" in col or col == "클릭률(%)":
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "0.0%")
    return out_df

def _build_comparison_df(cur_df, base_df, group_col, group_label, type_kor_map=None):
    if cur_df.empty and base_df.empty: return pd.DataFrame()

    base_cols = [group_col, 'imp', 'clk', 'cost', 'wishlist_conv', 'wishlist_sales', 'cart_conv', 'cart_sales', 'conv', 'sales']
    for c in base_cols[1:]:
        if not cur_df.empty and c not in cur_df.columns: cur_df[c] = 0.0
        if not base_df.empty and c not in base_df.columns: base_df[c] = 0.0

    cur_grp = cur_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not cur_df.empty else pd.DataFrame(columns=base_cols)
    base_grp = base_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not base_df.empty else pd.DataFrame(columns=base_cols)
    
    if not cur_df.empty:
        cur_grp['tot_conv'] = cur_df.groupby(group_col)['tot_conv'].sum().values if 'tot_conv' in cur_df.columns else cur_grp['conv'] + cur_grp['cart_conv'] + cur_grp['wishlist_conv']
        cur_grp['tot_sales'] = cur_df.groupby(group_col)['tot_sales'].sum().values if 'tot_sales' in cur_df.columns else cur_grp['sales'] + cur_grp['cart_sales'] + cur_grp['wishlist_sales']
    if not base_df.empty:
        base_grp['tot_conv'] = base_df.groupby(group_col)['tot_conv'].sum().values if 'tot_conv' in base_df.columns else base_grp['conv'] + base_grp['cart_conv'] + base_grp['wishlist_conv']
        base_grp['tot_sales'] = base_df.groupby(group_col)['tot_sales'].sum().values if 'tot_sales' in base_df.columns else base_grp['sales'] + base_grp['cart_sales'] + base_grp['wishlist_sales']

    merged = pd.merge(cur_grp, base_grp, on=group_col, how='outer', suffixes=('_cur', '_base')).fillna(0)

    c_imp, b_imp = merged.get('imp_cur', 0), merged.get('imp_base', 0)
    c_clk, b_clk = merged.get('clk_cur', 0), merged.get('clk_base', 0)
    c_cost, b_cost = merged.get('cost_cur', 0), merged.get('cost_base', 0)
    c_conv, b_conv = merged.get('conv_cur', 0), merged.get('conv_base', 0)
    c_sales, b_sales = merged.get('sales_cur', 0), merged.get('sales_base', 0)
    c_tot_conv, b_tot_conv = merged.get('tot_conv_cur', 0), merged.get('tot_conv_base', 0)
    c_tot_sales, b_tot_sales = merged.get('tot_sales_cur', 0), merged.get('tot_sales_base', 0)

    c_cpc = np.where(c_clk > 0, c_cost / c_clk, 0)
    b_cpc = np.where(b_clk > 0, b_cost / b_clk, 0)
    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    c_troas = np.where(c_cost > 0, (c_tot_sales / c_cost) * 100, 0)

    def _vec_pct_diff(c, b):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        return pct, diff

    pct_imp, _ = _vec_pct_diff(c_imp, b_imp)
    pct_clk, _ = _vec_pct_diff(c_clk, b_clk)
    pct_cost, _ = _vec_pct_diff(c_cost, b_cost)
    pct_cpc, _ = _vec_pct_diff(c_cpc, b_cpc)
    pct_conv, _ = _vec_pct_diff(c_conv, b_conv)
    pct_tot_conv, _ = _vec_pct_diff(c_tot_conv, b_tot_conv)

    out = pd.DataFrame()
    out[group_label] = merged[group_col].astype(str).str.upper().map(type_kor_map).fillna(merged[group_col]) if type_kor_map else merged[group_col]
    out['노출수'] = c_imp
    out['노출 증감'] = pct_imp
    out['클릭수'] = c_clk
    out['클릭 증감'] = pct_clk
    out['광고비'] = c_cost
    out['광고비 증감'] = pct_cost
    out['CPC'] = c_cpc
    out['CPC 증감'] = pct_cpc
    out['구매완료수'] = c_conv
    out['구매 증감'] = pct_conv
    out['구매완료 매출'] = c_sales
    out['구매 ROAS(%)'] = c_roas
    out['총 전환수'] = c_tot_conv
    out['총 전환 증감'] = pct_tot_conv
    out['총 전환매출'] = c_tot_sales
    out['통합 ROAS(%)'] = c_troas

    return out.sort_values("광고비", ascending=False).reset_index(drop=True)

def _build_ts_df(df, group_col, group_label):
    if df is None or df.empty: return pd.DataFrame()
    grp_cols = ['imp', 'clk', 'cost', 'wishlist_conv', 'wishlist_sales', 'cart_conv', 'cart_sales', 'conv', 'sales']
    has_tot = 'tot_conv' in df.columns
    if has_tot: grp_cols.extend(['tot_conv', 'tot_sales'])

    for c in grp_cols:
        if c not in df.columns: df[c] = 0.0

    grp = df.groupby(group_col)[grp_cols].sum().reset_index()
    out = pd.DataFrame()
    out[group_label] = grp[group_col]
    out['노출수'] = grp['imp']
    out['클릭수'] = grp['clk']
    out['광고비'] = grp['cost']
    out['CPC'] = np.where(grp['clk'] > 0, grp['cost'] / grp['clk'], 0)
    out['장바구니 담기수'] = grp['cart_conv']
    out['구매완료수'] = grp['conv']
    out['구매완료 매출'] = grp['sales']
    out['구매 ROAS(%)'] = np.where(grp['cost'] > 0, (grp['sales'] / grp['cost']) * 100, 0)
    out['총 전환수'] = grp['tot_conv'] if has_tot else grp['conv'] + grp['cart_conv'] + grp['wishlist_conv']
    out['총 전환매출'] = grp['tot_sales'] if has_tot else grp['sales'] + grp['cart_sales'] + grp['wishlist_sales']
    out['통합 ROAS(%)'] = np.where(grp['cost'] > 0, (out['총 전환매출'] / grp['cost']) * 100, 0)

    return out


def _build_ts_compare_df(cur_df, base_df, group_col, group_label, align_mode="label"):
    cur_view = _build_ts_df(cur_df, group_col, group_label)
    if cur_view.empty: return pd.DataFrame()
    base_view = _build_ts_df(base_df, group_col, group_label) if base_df is not None and not base_df.empty else pd.DataFrame()

    if align_mode == "sequence":
        cur_view = cur_view.reset_index(drop=True).copy()
        base_view = base_view.reset_index(drop=True).copy() if not base_view.empty else base_view
        cur_view["_seq"] = range(len(cur_view))
        if not base_view.empty: base_view["_seq"] = range(len(base_view))
        merge_key = "_seq"
    else: merge_key = group_label

    if not base_view.empty: merged = pd.merge(cur_view, base_view, on=merge_key, how="left", suffixes=("", "_base"))
    else:
        merged = cur_view.copy()
        for c in cur_view.columns:
            if c != merge_key: merged[f"{c}_base"] = 0

    diff_pairs = [("노출수", "노출 증감"), ("클릭수", "클릭 증감"), ("광고비", "광고비 증감"), ("CPC", "CPC 증감"), ("구매완료수", "구매 증감"), ("총 전환수", "총 전환 증감")]
    for cur_col, diff_col in diff_pairs:
        if cur_col in merged.columns:
            base_col = f"{cur_col}_base"
            c_val, b_val = pd.to_numeric(merged[cur_col], errors="coerce").fillna(0), pd.to_numeric(merged.get(base_col, 0), errors="coerce").fillna(0)
            safe_b = np.where(b_val == 0, 1, b_val)
            merged[diff_col] = np.where(b_val == 0, np.where(c_val > 0, 100.0, 0.0), ((c_val - b_val) / safe_b) * 100.0)

    if align_mode == "sequence" and "_seq" in merged.columns: merged = merged.drop(columns=["_seq"])
    return merged


def _delta_chip(cur_val, base_val, improve_when_up=True):
    diff = pct_change(float(cur_val or 0), float(base_val or 0)) if base_val is not None else 0.0
    if abs(diff) < 5: return "neu", f"유지 ({diff:+.1f}%)"
    improved = diff > 0 if improve_when_up else diff < 0
    return "pos" if improved else "neg", pct_to_arrow(diff)

def _render_kpi_group(title: str, items: list[dict]) -> str:
    cells = []
    for item in items:
        cls, text = _delta_chip(item["cur"], item["base"], item.get("improve_when_up", True))
        cells.append(f"<div class='ov-kpi-cell'><div class='ov-kpi-label'>{item['label']}</div><div class='ov-kpi-value'>{item['value']}</div><div class='ov-kpi-delta {cls}'>{text}</div></div>")
    return f"<div class='ov-kpi-panel'><div class='ov-kpi-title'>{title}</div><div class='ov-kpi-cells'>{''.join(cells)}</div></div>"

def _normalize_type_label(val) -> str:
    s = str(val or "").strip().upper()
    if not s: return ""
    if "쇼핑" in s or "SHOPPING" in s: return "쇼핑검색"
    if "파워링크" in s or "WEB_SITE" in s: return "파워링크"
    if "브랜드" in s or "BRAND" in s: return "브랜드검색"
    return str(val).strip()

def _infer_kpi_mode(type_sel: tuple, cur_camp: pd.DataFrame, is_split_only: bool) -> str:
    labels = {_normalize_type_label(x) for x in type_sel if str(x).strip()}
    if not labels and cur_camp is not None and not cur_camp.empty:
        for col in ["campaign_type_label", "campaign_type", "campaign_tp"]:
            if col in cur_camp.columns:
                vals = cur_camp[col].dropna().astype(str).tolist()
                labels = {_normalize_type_label(v) for v in vals if str(v).strip()}
                if labels: break
    if is_split_only and {x for x in labels if x} == {"쇼핑검색"}: return "shopping_purchase"
    return "generic_conversion"

def _format_compact_currency(value: float) -> str:
    try: v = float(value or 0)
    except: return "0원"
    abs_v = abs(v)
    if abs_v >= 100000000: return f"{v / 100000000:.2f}억"
    if abs_v >= 10000: return f"{v / 10000:.1f}만원"
    return f"{int(round(v)):,}원"


@st.fragment
def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    _inject_overview_css()

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    with st.spinner("데이터를 집계 중입니다... (최적화 모드)"):
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
        if len(acc_names) == 1: account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1: account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    patch_date = date(2026, 3, 11)
    is_split_only, combined_toggle = f["start"] >= patch_date, f["start"] < patch_date
    auto_kpi_mode = _infer_kpi_mode(type_sel, cur_camp, is_split_only)
    can_use_purchase_toggle = (f["end"] >= patch_date)

    head_col_meta, head_col_toggle = st.columns([5, 2])
    with head_col_meta:
        st.markdown(f"<div style='display:flex; flex-wrap:wrap; gap:8px; align-items:center; padding-top:4px; margin-bottom: 12px;'><div class='ov-chip primary'>{selected_type_label}</div><div class='ov-chip muted'>{f['start']} ~ {f['end']}</div><div class='ov-chip muted'>{cmp_mode} · {b1} ~ {b2}</div></div>", unsafe_allow_html=True)
    with head_col_toggle:
        purchase_view = st.toggle("구매완료 데이터로 보기", value=(auto_kpi_mode == "shopping_purchase"), key="overview_purchase_view_toggle", disabled=not can_use_purchase_toggle)

    cur, base = cur_summary or {}, base_summary or {}
    for dic in [cur, base]:
        dic['tot_conv'] = dic.get('tot_conv', dic.get('conv', 0))
        dic['tot_sales'] = dic.get('tot_sales', dic.get('sales', 0))
        dic['tot_roas'] = (dic['tot_sales'] / dic['cost'] * 100) if dic.get('cost', 0) > 0 else 0
        dic['cpm'] = (dic.get('cost', 0) / dic.get('imp', 0) * 1000) if dic.get('imp', 0) > 0 else 0

    kpi_mode = "shopping_purchase" if (purchase_view and can_use_purchase_toggle) else "generic_conversion"

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
    perf_items = [
        {"label": "구매 ROAS" if kpi_mode=="shopping_purchase" else "총 ROAS", "value": f"{float(cur.get('roas' if kpi_mode=='shopping_purchase' else 'tot_roas', 0.0)):.1f}%", "cur": cur.get("roas" if kpi_mode=="shopping_purchase" else "tot_roas", 0), "base": base.get("roas" if kpi_mode=="shopping_purchase" else "tot_roas", 0)},
        {"label": "구매완료수" if kpi_mode=="shopping_purchase" else "총 전환수", "value": f"{float(cur.get('conv' if kpi_mode=='shopping_purchase' else 'tot_conv', 0.0)):.0f}", "cur": cur.get("conv" if kpi_mode=="shopping_purchase" else "tot_conv", 0), "base": base.get("conv" if kpi_mode=="shopping_purchase" else "tot_conv", 0)},
        {"label": "구매완료 매출" if kpi_mode=="shopping_purchase" else "총 전환매출", "value": _format_compact_currency(cur.get("sales" if kpi_mode=="shopping_purchase" else "tot_sales", 0.0)), "cur": cur.get("sales" if kpi_mode=="shopping_purchase" else "tot_sales", 0), "base": base.get("sales" if kpi_mode=="shopping_purchase" else "tot_sales", 0)},
    ]

    st.markdown(f"<div class='ov-kpi-grid'>{_render_kpi_group('유입 지표', inflow_items)}{_render_kpi_group('비용 지표', cost_items)}{_render_kpi_group('성과 지표', perf_items)}</div>", unsafe_allow_html=True)

    # 🚀 핵심 속도 개선: st.tabs 대신 st.pills(지연 로딩) 적용
    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
    selected_tab = st.pills("분석 탭 선택", ["🏢 업체별 요약", "🏷️ 매체/유형별 요약", "📅 기간별 상세", "🔍 캠페인 상세 분석"], default="🏢 업체별 요약")

    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 데이터 프레임 전처리 
    if not cur_camp.empty or not base_camp.empty:
        mapping = dict(zip(meta['customer_id'].astype(str), meta['account_name'])) if not meta.empty and 'customer_id' in meta.columns else {}
        if not cur_camp.empty: cur_camp['account_name'] = cur_camp['customer_id'].astype(str).map(mapping).fillna(cur_camp['customer_id'].astype(str))
        if not base_camp.empty: base_camp['account_name'] = base_camp['customer_id'].astype(str).map(mapping).fillna(base_camp['customer_id'].astype(str))
        df_display = _build_comparison_df(cur_camp, base_camp, 'account_name', '계정명')
        type_kor_map = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        type_col = 'campaign_tp' if 'campaign_tp' in cur_camp.columns else ('campaign_type' if 'campaign_type' in cur_camp.columns else None)
        if type_col: df_type_display = _build_comparison_df(cur_camp, base_camp, type_col, '캠페인 유형', type_kor_map)
        camp_col = 'campaign_name' if 'campaign_name' in cur_camp.columns else None
        if camp_col: camp_disp = _build_comparison_df(cur_camp, base_camp, camp_col, '캠페인명')

    if daily_ts is not None and not daily_ts.empty:
        daily_copy, base_daily_copy = daily_ts.copy(), base_daily_ts.copy() if base_daily_ts is not None else pd.DataFrame()
        daily_copy['일자'] = daily_copy['dt'].dt.strftime('%Y-%m-%d')
        if not base_daily_copy.empty: base_daily_copy['일자'] = base_daily_copy['dt'].dt.strftime('%Y-%m-%d')
        daily_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '일자', '일자', align_mode="sequence").sort_values('일자', ascending=False)
        
        daily_copy['요일'] = daily_copy['dt'].dt.dayofweek
        if not base_daily_copy.empty: base_daily_copy['요일'] = base_daily_copy['dt'].dt.dayofweek
        dow_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '요일', '요일', align_mode="label").sort_values('요일')
        dow_disp['요일명'] = dow_disp['요일'].map({0: '월요일', 1: '화요일', 2: '수요일', 3: '목요일', 4: '금요일', 5: '토요일', 6: '일요일'})
        
        daily_copy['주차'] = daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        if not base_daily_copy.empty: base_daily_copy['주차'] = base_daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        weekly_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '주차', '주차', align_mode="sequence").sort_values('주차', ascending=False)

    def _display_ts_table(df, col_name, toggle_state_val):
        if df.empty: return
        cols = [col_name, "노출수", "노출 증감", "클릭수", "클릭 증감", "광고비", "광고비 증감", "CPC", "CPC 증감", "총 전환수", "총 전환 증감", "총 전환매출", "통합 ROAS(%)"] if toggle_state_val else [col_name, "노출수", "노출 증감", "클릭수", "클릭 증감", "광고비", "광고비 증감", "CPC", "CPC 증감", "구매완료수", "구매 증감", "구매완료 매출", "구매 ROAS(%)"]
        disp_ts = df[[c for c in cols if c in df.columns]].copy()
        _render_overview_sticky_table(disp_ts, col_name, height=420, hide_index=True)


    if selected_tab == "🏢 업체별 요약":
        if not df_display.empty: _render_overview_sticky_table(df_display, "계정명", height=420, hide_index=True)
        else: st.info("조건에 맞는 데이터가 없습니다.")

    elif selected_tab == "🏷️ 매체/유형별 요약":
        if not df_type_display.empty: _render_overview_sticky_table(df_type_display, "캠페인 유형", height=420, hide_index=True)
        else: st.info("조건에 맞는 데이터가 없습니다.")

    elif selected_tab == "📅 기간별 상세":
        if any(not df.empty for df in [daily_disp, dow_disp, weekly_disp]):
            period_tab = st.pills("기간 단위 선택", ["일자별", "주차별", "요일별"], default="일자별")
            if period_tab == "일자별": _display_ts_table(daily_disp, "일자", combined_toggle)
            elif period_tab == "주차별": _display_ts_table(weekly_disp, "주차", combined_toggle)
            elif period_tab == "요일별": _display_ts_table(dow_disp, "요일명", combined_toggle)
        else: st.info("조건에 맞는 데이터가 없습니다.")

    elif selected_tab == "🔍 캠페인 상세 분석":
        if not camp_disp.empty: _render_overview_sticky_table(camp_disp, "캠페인명", height=460, hide_index=True)
        else: st.info("조건에 맞는 데이터가 없습니다.")

    # 엑셀 다운로드 (유지)
    st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)
    has_data_to_export = any([not df_display.empty, not df_type_display.empty, not camp_disp.empty, not daily_disp.empty])
    if has_data_to_export:
        with st.container(border=True):
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>엑셀 데이터 일괄 다운로드</div>", unsafe_allow_html=True)
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer) as writer:
                if not df_display.empty: format_for_csv(df_display).to_excel(writer, sheet_name='계정별_성과상세', index=False)
                if not df_type_display.empty: format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과상세', index=False)
                if not camp_disp.empty: format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_성과상세', index=False)
                if not daily_disp.empty: format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_성과상세', index=False)
                if not dow_disp.empty: format_for_csv(dow_disp.drop(columns=['요일']) if '요일' in dow_disp.columns else dow_disp).to_excel(writer, sheet_name='요일별_성과상세', index=False)
                if not weekly_disp.empty: format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_성과상세', index=False)
            st.download_button("통합 엑셀 다운로드", data=excel_buffer.getvalue(), file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")
