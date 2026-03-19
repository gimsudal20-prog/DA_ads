# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import io
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta


def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"

def _build_periodic_report_text(campaign_type: str, report_cur: dict, top_keywords: str = "없음") -> str:
    lines = [
        f"[ {campaign_type} 성과 요약 ]",
        _format_report_line("노출수", f"{int(float(report_cur.get('imp', 0) or 0)):,}"),
        _format_report_line("클릭수", f"{int(float(report_cur.get('clk', 0) or 0)):,}"),
        _format_report_line("클릭률", f"{float(report_cur.get('ctr', 0) or 0):.1f}%"),
        _format_report_line("광고 소진비용", f"{int(float(report_cur.get('cost', 0) or 0)):,}원"),
    ]

    if campaign_type == "파워링크":
        tot_conv = float(report_cur.get('tot_conv', 0) or 0)
        tot_roas = float(report_cur.get('tot_roas', 0) or 0)
        if tot_conv >= 1:
            lines.append(_format_report_line("총 전환수", f"{tot_conv:.1f}"))
        if tot_roas > 0:
            lines.append(_format_report_line("총 ROAS", f"{tot_roas:.1f}%"))
        lines.append(_format_report_line("주요 유입 키워드 3건", top_keywords))
        return "\n".join(lines)

    if campaign_type == "쇼핑검색":
        lines.extend([
            _format_report_line("구매완료 수", f"{float(report_cur.get('conv', 0) or 0):.1f}"),
            _format_report_line("구매완료 매출", f"{int(float(report_cur.get('sales', 0) or 0)):,}원"),
            _format_report_line("구매완료 ROAS", f"{float(report_cur.get('roas', 0) or 0):.1f}%"),
        ])
        return "\n".join(lines)

    lines.extend([
        _format_report_line("구매완료수", f"{float(report_cur.get('conv', 0) or 0):.1f}"),
        _format_report_line("구매완료 매출", f"{int(float(report_cur.get('sales', 0) or 0)):,}원"),
        _format_report_line("구매 ROAS", f"{float(report_cur.get('roas', 0) or 0):.1f}%"),
        _format_report_line("총 전환수", f"{float(report_cur.get('tot_conv', 0) or 0):.1f}"),
        _format_report_line("총 전환매출", f"{int(float(report_cur.get('tot_sales', 0) or 0)):,}원"),
        _format_report_line("총 ROAS", f"{float(report_cur.get('tot_roas', 0) or 0):.1f}%"),
    ])
    if top_keywords:
        lines.append(_format_report_line("주요 유입 키워드", top_keywords))
    return "\n".join(lines)



def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel: return "전체 유형"
    if len(type_sel) == 1: return type_sel[0]
    return ", ".join(type_sel)

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=5000)
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=0)
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, trend_d1, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        ts = query_campaign_timeseries(_engine, trend_d1, end_dt, cids, type_sel)
        return ts if ts is not None else pd.DataFrame()
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_type_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        cid_str = ",".join([f"'{str(x)}'" for x in cids])
        where_cid = f"AND f.customer_id IN ({cid_str})" if cids else ""
        type_join_sql = "JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
        type_where_sql = ""
        if type_sel:
            rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENTS", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
            db_types = [rev_map.get(t, t) for t in type_sel]
            type_list_str = ",".join([f"'{x}'" for x in db_types])
            type_where_sql = f"AND c.campaign_tp IN ({type_list_str})"

        fact_cols = get_table_columns(_engine, "fact_campaign_daily")
        expr = _strict_conv_selects(fact_cols, alias="f")

        sql = f"""
            SELECT f.dt, c.campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost,
                   SUM({expr['purchase_conv_expr']}) as conv,
                   SUM({expr['purchase_sales_expr']}) as sales,
                   SUM({expr['cart_conv_expr']}) as cart_conv,
                   SUM({expr['cart_sales_expr']}) as cart_sales,
                   SUM({expr['wish_conv_expr']}) as wishlist_conv,
                   SUM({expr['wish_sales_expr']}) as wishlist_sales,
                   SUM({expr['total_conv_expr']}) as tot_conv,
                   SUM({expr['total_sales_expr']}) as tot_sales
            FROM fact_campaign_daily f
            {type_join_sql}
            WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
            GROUP BY f.dt, c.campaign_tp
        """
        df = pd.read_sql(sql, _engine)
        if not df.empty:
            df["dt"] = pd.to_datetime(df["dt"])
        return df
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
                if "광고비" in col or "매출" in col or "CPC" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                elif "노출" in col or "클릭" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
                else: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.1f}" if pd.notnull(x) and x != 0 else "0.0")
            elif "증감" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "0.0%")
            elif "ROAS" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.1f}%" if pd.notnull(x) else "0.0%")
            elif col == "클릭률(%)":
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.1f}%" if pd.notnull(x) else "0.0%")
    return out_df

def calc_pct_diff(c, b):
    diff = c - b
    if b == 0: return (100.0 if c > 0 else 0.0), diff
    return (diff / b * 100.0), diff


def color_delta_positive(val):
    if pd.isna(val) or val == 0: return 'color: #A8AFB7;'
    return 'color: #0528F2; font-weight: 600;' if val > 0 else 'color: #F04438; font-weight: 600;'

def color_delta_negative(val):
    if pd.isna(val) or val == 0: return 'color: #A8AFB7;'
    return 'color: #F04438; font-weight: 600;' if val > 0 else 'color: #0528F2; font-weight: 600;'

def style_delta_str(val):
    val_str = str(val).strip()
    if val_str.startswith("+"): return 'color: #0528F2; font-weight: 600;'
    elif val_str.startswith("-"): return 'color: #F04438; font-weight: 600;'
    return ''

def style_delta_str_neg(val):
    val_str = str(val).strip()
    if val_str.startswith("+"): return 'color: #F04438; font-weight: 600;'
    elif val_str.startswith("-"): return 'color: #0528F2; font-weight: 600;'
    return ''

# 🔥 중복 코드 제거를 위한 비교 데이터 생성 도우미 함수
def _build_comparison_df(cur_df, base_df, group_col, group_label, type_kor_map=None):
    if cur_df.empty and base_df.empty:
        return pd.DataFrame()

    base_cols = [
        group_col, 'imp', 'clk', 'cost', 'wishlist_conv', 'wishlist_sales',
        'cart_conv', 'cart_sales', 'conv', 'sales', 'tot_conv', 'tot_sales'
    ]
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
        b_imp, b_clk, b_cost = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0)
        c_wish, c_wsales = row['wishlist_conv_cur'], row['wishlist_sales_cur']
        b_wish, b_wsales = row.get('wishlist_conv_base', 0), row.get('wishlist_sales_base', 0)
        c_cart, c_csales = row['cart_conv_cur'], row['cart_sales_cur']
        b_cart, b_csales = row.get('cart_conv_base', 0), row.get('cart_sales_base', 0)
        c_conv, c_sales = row['conv_cur'], row['sales_cur']
        b_conv, b_sales = row.get('conv_base', 0), row.get('sales_base', 0)
        c_tot_conv = row.get('tot_conv_cur', c_conv + c_cart + c_wish)
        c_tot_sales = row.get('tot_sales_cur', c_sales + c_csales + c_wsales)
        b_tot_conv = row.get('tot_conv_base', b_conv + b_cart + b_wish)
        b_tot_sales = row.get('tot_sales_base', b_sales + b_csales + b_wsales)

        c_ctr = (c_clk / c_imp * 100) if c_imp > 0 else 0
        b_ctr = (b_clk / b_imp * 100) if b_imp > 0 else 0
        c_cpc = (c_cost / c_clk) if c_clk > 0 else 0
        b_cpc = (b_cost / b_clk) if b_clk > 0 else 0
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0
        b_troas = (b_tot_sales / b_cost * 100) if b_cost > 0 else 0

        pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
        pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
        pct_ctr, diff_ctr = calc_pct_diff(c_ctr, b_ctr)
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
            "클릭률(%)": c_ctr, "클릭률 증감": pct_ctr, "클릭률 차이": diff_ctr,
            "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
            "CPC": c_cpc, "CPC 증감": pct_cpc, "CPC 차이": diff_cpc,
            "위시리스트수": c_wish, "위시리스트 증감": pct_wish, "위시리스트 차이": diff_wish,
            "장바구니 담기수": c_cart, "장바구니 증감": pct_cart, "장바구니 차이": diff_cart,
            "구매완료수": c_conv, "구매 증감": pct_conv, "구매 차이": diff_conv,
            "구매완료 매출": c_sales, "구매 매출 증감": pct_sales, "구매 매출 차이": diff_sales,
            "구매 ROAS(%)": c_roas, "구매 ROAS 증감": c_roas - b_roas,
            "총 전환수": c_tot_conv, "총 전환 증감": pct_tot_conv, "총 전환 차이": diff_tot_conv,
            "총 전환매출": c_tot_sales, "총 매출 증감": pct_tot_sales, "총 매출 차이": diff_tot_sales,
            "통합 ROAS(%)": c_troas, "통합 ROAS 증감": c_troas - b_troas
        })
    return pd.DataFrame(table_data).sort_values("광고비", ascending=False)

# 🔥 시계열 데이터(일자/요일/주간) 생성 도우미 함수
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
        c_ctr = (c_clk / c_imp * 100) if c_imp > 0 else 0
        c_cpc = (c_cost / c_clk) if c_clk > 0 else 0
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0
        table_data.append({
            group_label: row[group_col],
            "노출수": c_imp,
            "클릭수": c_clk,
            "클릭률(%)": c_ctr,
            "광고비": c_cost,
            "CPC": c_cpc,
            "위시리스트수": c_wish,
            "장바구니 담기수": c_cart,
            "구매완료수": c_conv,
            "구매완료 매출": c_sales,
            "구매 ROAS(%)": c_roas,
            "총 전환수": c_tot_conv,
            "총 전환매출": c_tot_sales,
            "통합 ROAS(%)": c_troas,
        })
    return pd.DataFrame(table_data)


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return

    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
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
        type_weekly_ts = _cached_type_timeseries(engine, f["start"], f["end"], cids, type_sel)

    state_sig = f"{f['start']}|{f['end']}|{','.join(map(str, cids))}|{','.join(type_sel)}"
    state_hash = abs(hash(state_sig))
    report_loaded_key = f"overview_report_loaded_{state_hash}"

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1: account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1: account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
        "클릭률(%)": "{:,.1f}%", "클릭률 증감": "{:+.1f}%", "클릭률 차이": "{:+,.1f}%p",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
        "CPC": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
        "장바구니 담기수": "{:,.1f}", "장바구니 증감": "{:+.1f}%", "장바구니 차이": "{:+,.1f}",
        "장바구니 매출액": "{:,.0f}원", "장바구니 ROAS(%)": "{:,.1f}%", "장바구니ROAS 증감": "{:+.1f}%",
        "구매완료수": "{:,.1f}", "구매 증감": "{:+.1f}%", "구매 차이": "{:+,.1f}",
        "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.1f}%", "구매 매출 차이": "{:+,.0f}원",
        "구매 ROAS(%)": "{:,.1f}%", "구매 ROAS 증감": "{:+.1f}%",
        "총 전환수": "{:,.1f}", "총 전환 증감": "{:+.1f}%", "총 전환 차이": "{:+,.1f}",
        "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.1f}%", "총 매출 차이": "{:+,.0f}원",
        "통합 ROAS(%)": "{:,.1f}%", "통합 ROAS 증감": "{:+.1f}%",
        "위시리스트수": "{:,.1f}", "위시리스트 증감": "{:+.1f}%", "위시리스트 차이": "{:+,.1f}"
    }
    
    fmt_dict_ts = {
        "노출수": "{:,.0f}", "클릭수": "{:,.0f}", "클릭률(%)": "{:,.1f}%", "광고비": "{:,.0f}원", "CPC": "{:,.0f}원",
        "위시리스트수": "{:,.1f}", "위시리스트 매출액": "{:,.0f}원", "위시리스트 ROAS(%)": "{:,.1f}%",
        "장바구니 담기수": "{:,.1f}", "장바구니 매출액": "{:,.0f}원", "장바구니 ROAS(%)": "{:,.1f}%",
        "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
        "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%"
    }

    type_kor_map = {
        "WEB_SITE": "파워링크", 
        "SHOPPING": "쇼핑검색", 
        "POWER_CONTENTS": "파워컨텐츠", 
        "BRAND_SEARCH": "브랜드검색", 
        "PLACE": "플레이스"
    }

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약 ({selected_type_label})</div>", unsafe_allow_html=True)
    cmp_date_info = f"{cmp_mode} ({b1} ~ {b2})" if b1 and b2 else cmp_mode
    st.markdown(f"<div style='font-size:12px; font-weight:500; color:var(--nv-muted); margin-bottom:16px;'>비교 기준: <span style='color:var(--nv-primary); font-weight:600;'>{cmp_date_info}</span></div>", unsafe_allow_html=True)

    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)

    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터가 포함되어 있어 구매완료/장바구니/위시리스트 분리 데이터가 일부 비어 있을 수 있습니다. 총 전환 지표는 함께 확인해주세요.")

    cur = cur_summary
    base = base_summary

    cur['tot_conv'] = cur.get('tot_conv', cur.get('conv', 0))
    cur['tot_sales'] = cur.get('tot_sales', cur.get('sales', 0))
    cur['tot_roas'] = (cur['tot_sales'] / cur['cost'] * 100) if cur.get('cost', 0) > 0 else 0
    cur['cpm'] = ((cur.get('cost', 0) / cur.get('imp', 0)) * 1000) if cur.get('imp', 0) > 0 else 0

    base['tot_conv'] = base.get('tot_conv', base.get('conv', 0))
    base['tot_sales'] = base.get('tot_sales', base.get('sales', 0))
    base['tot_roas'] = (base['tot_sales'] / base['cost'] * 100) if base.get('cost', 0) > 0 else 0
    base['cpm'] = ((base.get('cost', 0) / base.get('imp', 0)) * 1000) if base.get('imp', 0) > 0 else 0

    def _delta_pct(key):
        try:
            return pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception:
            return None

    def _kpi_html(label, value, delta_text, delta_val, highlight=False, improve_when_up=True):
        delta_num = float(delta_val) if delta_val is not None else 0.0
        is_neutral = abs(delta_num) < 5
        if is_neutral:
            cls_delta = "neu"
            delta_text = f"유지 ({delta_num:+.1f}%)"
        else:
            improved = delta_num > 0 if improve_when_up else delta_num < 0
            cls_delta = "pos" if improved else "neg"
            delta_text = f"{pct_to_arrow(delta_num)}"
        cls_hl = " highlight-positive" if "ROAS" in label else (" highlight" if highlight else "")
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    kpi_html = f"""
    <div class='kpi-group-container'>
        <div class='kpi-group'>
            <div class='kpi-group-title'>유입 지표</div>
            <div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("클릭률", f"{float(cur.get('ctr', 0.0) or 0.0):.1f}%", f"{pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>비용 지표</div>
            <div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), improve_when_up=False)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"), improve_when_up=False)}
                {_kpi_html("CPM", format_currency(cur.get("cpm", 0.0)), f"{pct_to_arrow(_delta_pct('cpm'))}", _delta_pct("cpm"), improve_when_up=False)}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>성과 지표</div>
            <div style='font-size:12px; font-weight:700; color:var(--nv-primary); margin-bottom:8px;'>구매완료</div>
            <div class='kpi-row' style='margin-bottom:14px; padding-bottom:14px; border-bottom:1px dashed var(--nv-line);'>
                {_kpi_html("구매 ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.1f}%", f"{pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("구매완료수", f"{float(cur.get('conv', 0.0)):.1f}", f"{pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("구매완료 매출", format_currency(cur.get("sales", 0.0)), f"{pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"), highlight=True)}
            </div>
            <div style='font-size:12px; font-weight:700; color:var(--nv-primary); margin-bottom:8px;'>전체 전환</div>
            <div class='kpi-row' style='margin-bottom:14px; padding-bottom:14px; border-bottom:1px dashed var(--nv-line);'>
                {_kpi_html("총 ROAS", f"{float(cur.get('tot_roas', 0.0) or 0.0):.1f}%", f"{pct_to_arrow(_delta_pct('tot_roas'))}", _delta_pct("tot_roas"), highlight=True)}
                {_kpi_html("총 전환수", f"{float(cur.get('tot_conv', 0.0)):.1f}", f"{pct_to_arrow(_delta_pct('tot_conv'))}", _delta_pct("tot_conv"))}
                {_kpi_html("총 전환매출", format_currency(cur.get("tot_sales", 0.0)), f"{pct_to_arrow(_delta_pct('tot_sales'))}", _delta_pct("tot_sales"), highlight=True)}
            </div>
            <div style='font-size:12px; font-weight:700; color:var(--nv-primary); margin-bottom:8px;'>보조 전환</div>
            <div class='kpi-row'>
                {_kpi_html("장바구니수", f"{float(cur.get('cart_conv', 0.0)):.1f}", f"{pct_to_arrow(_delta_pct('cart_conv'))}", _delta_pct("cart_conv"))}
                {_kpi_html("위시리스트수", f"{float(cur.get('wishlist_conv', 0.0)):.1f}", f"{pct_to_arrow(_delta_pct('wishlist_conv'))}", _delta_pct("wishlist_conv"))}
                {_kpi_html("장바구니 매출", format_currency(cur.get("cart_sales", 0.0)), f"{pct_to_arrow(_delta_pct('cart_sales'))}", _delta_pct("cart_sales"))}
            </div>
        </div>
    </div>
    """
    st.markdown(kpi_html, unsafe_allow_html=True)

    st.markdown("<div class='nv-sec-title' style='margin-top:40px;'>📊 일자별 성과 추이</div>", unsafe_allow_html=True)
    if daily_ts is not None and not daily_ts.empty:
        expected_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_sales', 'tot_conv']
        for c in expected_cols:
            if c not in daily_ts.columns:
                daily_ts[c] = 0.0

        daily_ts_chart = daily_ts.groupby('dt')[expected_cols].sum().reset_index()

        tab_t1, tab_t2 = st.tabs(["비용 및 구매완료 매출 추이", "유입 지표 추이"])
        with tab_t1:
            render_echarts_dual_axis("비용 및 구매완료 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "sales", "구매완료 매출", height=320)
        with tab_t2:
            render_echarts_dual_axis("유입 지표 추이", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
    else:
        st.info("해당 기간의 일자별 트렌드 데이터가 없습니다.")


        df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if not cur_camp.empty or not base_camp.empty:
        if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
            mapping = dict(zip(meta['customer_id'].astype(str), meta['account_name']))
            if not cur_camp.empty: cur_camp['account_name'] = cur_camp['customer_id'].astype(str).map(mapping).fillna(cur_camp['customer_id'].astype(str))
            if not base_camp.empty: base_camp['account_name'] = base_camp['customer_id'].astype(str).map(mapping).fillna(base_camp['customer_id'].astype(str))
        else:
            if not cur_camp.empty: cur_camp['account_name'] = cur_camp['customer_id'].astype(str)
            if not base_camp.empty: base_camp['account_name'] = base_camp['customer_id'].astype(str)
            
        df_display = _build_comparison_df(cur_camp, base_camp, 'account_name', '업체명')
        
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
        dow_map = {0:'월요일', 1:'화요일', 2:'수요일', 3:'목요일', 4:'금요일', 5:'토요일', 6:'일요일'}
        dow_disp['요일명'] = dow_disp['요일'].map(dow_map)
        
        daily_copy['주차'] = daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
        weekly_disp = _build_ts_df(daily_copy, '주차', '주차').sort_values('주차', ascending=False)

    st.markdown("<div style='margin-top:40px; margin-bottom:10px;'></div>", unsafe_allow_html=True)
    has_data_to_export = any([not df_display.empty, not df_type_display.empty, not camp_disp.empty, not daily_disp.empty])
    if has_data_to_export:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer) as writer:
            if not df_display.empty: format_for_csv(df_display).to_excel(writer, sheet_name='업체별_전체요약', index=False)
            if not df_type_display.empty: format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과요약', index=False)
            if not camp_disp.empty: format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_성과요약', index=False)
            if not daily_disp.empty: format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_요약', index=False)
            if not dow_disp.empty: 
                dow_export = dow_disp.drop(columns=['요일']) if '요일' in dow_disp.columns else dow_disp
                format_for_csv(dow_export).to_excel(writer, sheet_name='요일별_요약', index=False)
            if not weekly_disp.empty: format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_요약', index=False)
        st.download_button(label="📥 통합 데이터 전체 다운로드", data=excel_buffer.getvalue(), file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    show_extra_metrics = st.checkbox(
        "장바구니 · 위시리스트 · 총전환 지표 보기",
        value=False,
        key=f"overview_show_extra_metrics_{state_hash}",
        help="기본 표에는 핵심 구매 지표만 보이고, 체크 시 장바구니/위시리스트/총전환 지표가 추가로 표시됩니다.",
    )

    def _apply_depth_toggle(df, base_cols, _unused=None):
        out = df.copy()

        def _combine(r, c_val, c_pct, is_currency=False, is_count=False, is_rate=False):
            v = r.get(c_val)
            p = r.get(c_pct)
            if pd.isna(v):
                return "-"
            if is_currency:
                v_str = f"{v:+,.0f}원"
            elif is_rate:
                v_str = f"{v:+,.1f}%p"
            elif is_count:
                v_str = f"{v:+,.0f}"
            else:
                v_str = f"{v:+,.1f}"
            return f"{v_str} ({p:+.1f}%)"

        metrics = [
            ("노출수", "노출 차이", "노출 증감", False, True, False),
            ("클릭수", "클릭 차이", "클릭 증감", False, True, False),
            ("클릭률(%)", "클릭률 차이", "클릭률 증감", False, False, True),
            ("광고비", "광고비 차이", "광고비 증감", True, False, False),
            ("CPC", "CPC 차이", "CPC 증감", True, False, False),
            ("구매완료수", "구매 차이", "구매 증감", False, False, False),
            ("구매완료 매출", "구매 매출 차이", "구매 매출 증감", True, False, False),
            ("구매 ROAS(%)", "구매 ROAS 증감", None, False, False, True),
        ]
        if show_extra_metrics:
            metrics.extend([
                ("장바구니 담기수", "장바구니 차이", "장바구니 증감", False, False, False),
                ("위시리스트수", "위시리스트 차이", "위시리스트 증감", False, False, False),
                ("총 전환수", "총 전환 차이", "총 전환 증감", False, False, False),
                ("총 전환매출", "총 매출 차이", "총 매출 증감", True, False, False),
                ("통합 ROAS(%)", "통합 ROAS 증감", None, False, False, True),
            ])

        display_cols = list(base_cols)
        delta_cols = []
        for label, diff_col, pct_col, is_currency, is_count, is_rate in metrics:
            combined_col = f"{label} 증감/율"
            if pct_col is None:
                out[combined_col] = out[diff_col].apply(lambda x: f"{x:+.1f}%p" if pd.notna(x) and x != 0 else "-")
            else:
                out[combined_col] = out.apply(lambda r: _combine(r, diff_col, pct_col, is_currency, is_count, is_rate), axis=1)
            display_cols.extend([label, combined_col])
            delta_cols.append(combined_col)

        return out[[c for c in display_cols if c in out.columns]], delta_cols

    def _display_ts_table(df, col_name, _unused=None):
        if df.empty:
            st.info("해당 기간의 데이터가 없습니다.")
            return
        cols = [
            col_name, "노출수", "클릭수", "클릭률(%)", "광고비", "CPC",
            "구매완료수", "구매완료 매출", "구매 ROAS(%)"
        ]
        if show_extra_metrics:
            cols.extend(["장바구니 담기수", "위시리스트수", "총 전환수", "총 전환매출", "통합 ROAS(%)"])
        st.dataframe(df[[c for c in cols if c in df.columns]].style.format(fmt_dict_ts), use_container_width=True, hide_index=True)

    with st.expander("🏢 업체별 전체 성과 요약", expanded=False):
        if not df_display.empty:
            disp_df, delta_cols_to_style = _apply_depth_toggle(df_display, ["업체명"])
            disp_df = disp_df.set_index(["업체명"])
            styled_df = disp_df.style.format(fmt_dict_standard)
            try:
                styled_df = styled_df.map(style_delta_str, subset=[c for c in delta_cols_to_style if c not in ["광고비 증감/율", "CPC 증감/율"] and c in disp_df.columns])
                styled_df = styled_df.map(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in disp_df.columns])
            except AttributeError:
                styled_df = styled_df.applymap(style_delta_str, subset=[c for c in delta_cols_to_style if c not in ["광고비 증감/율", "CPC 증감/율"] and c in disp_df.columns])
                styled_df = styled_df.applymap(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in disp_df.columns])
            st.dataframe(styled_df, use_container_width=True, hide_index=False)
        else: st.info("해당 기간의 데이터가 없습니다.")

    with st.expander("🏷️ 유형별 성과 요약", expanded=False):
        if not df_type_display.empty:
            disp_type_df, delta_cols_to_style_type = _apply_depth_toggle(df_type_display, ["캠페인 유형"])
            disp_type_df = disp_type_df.set_index(["캠페인 유형"])
            styled_type_df = disp_type_df.style.format(fmt_dict_standard)
            try:
                styled_type_df = styled_type_df.map(style_delta_str, subset=[c for c in delta_cols_to_style_type if c not in ["광고비 증감/율", "CPC 증감/율"] and c in disp_type_df.columns])
                styled_type_df = styled_type_df.map(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in disp_type_df.columns])
            except AttributeError:
                pass
            st.dataframe(styled_type_df, use_container_width=True, hide_index=False)

    with st.expander("📢 캠페인별 성과 요약", expanded=False):
        if not camp_disp.empty:
            disp_camp_df, delta_cols_to_style_camp = _apply_depth_toggle(camp_disp, ["캠페인명"])
            disp_camp_df = disp_camp_df.set_index(["캠페인명"])
            styled_camp_df = disp_camp_df.style.format(fmt_dict_standard)
            try:
                styled_camp_df = styled_camp_df.map(style_delta_str, subset=[c for c in delta_cols_to_style_camp if c not in ["광고비 증감/율", "CPC 증감/율"] and c in disp_camp_df.columns])
                styled_camp_df = styled_camp_df.map(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in disp_camp_df.columns])
            except AttributeError:
                pass
            st.dataframe(styled_camp_df, use_container_width=True, hide_index=False)

    with st.expander("📅 일자별 성과 요약", expanded=False):
        _display_ts_table(daily_disp, "일자")
        
    with st.expander("📆 요일별 성과 요약", expanded=False):
        _display_ts_table(dow_disp, "요일명")
        
    with st.expander("주간 성과 요약", expanded=False):
        _display_ts_table(weekly_disp, "주차")

    with st.expander("📝 보고서 내보내기", expanded=False):
        report_campaign_type = type_sel[0] if len(type_sel) == 1 else selected_type_label
        report_cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
        st.session_state[report_loaded_key] = True

        top_kw_str = "없음"
        if kw_bundle is not None and not kw_bundle.empty and "keyword" in kw_bundle.columns and "clk" in kw_bundle.columns:
            kw_agg = kw_bundle.groupby("keyword")["clk"].sum().reset_index()
            top_kws = kw_agg[kw_agg["clk"] > 0].sort_values("clk", ascending=False).head(3)
            if not top_kws.empty:
                top_kw_str = ", ".join([f"{row['keyword']}({int(row['clk']):,}회)" for _, row in top_kws.iterrows()])

        report_text = _build_periodic_report_text(report_campaign_type, report_cur, top_kw_str)
        st.code(report_text, language="text")
