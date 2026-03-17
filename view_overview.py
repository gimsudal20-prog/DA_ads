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

def _build_periodic_report_text(campaign_type: str, imp: float, clk: float, ctr: float, cost: float, tot_conv: float, tot_roas: float, tot_sales: float, top_keywords_label: str, top_keywords: str) -> str:
    return "\n".join([
        f"[ {campaign_type} 성과 요약 ]",
        _format_report_line("노출수", f"{int(imp):,}"),
        _format_report_line("클릭수", f"{int(clk):,}"),
        _format_report_line("클릭률", f"{float(ctr):.2f}%"),
        _format_report_line("광고 소진비용", f"{int(cost):,}원"),
        _format_report_line("총 전환수", f"{int(tot_conv):,}"),
        _format_report_line("총 전환매출", f"{int(tot_sales):,}원"),
        _format_report_line("통합 ROAS", f"{float(tot_roas):.2f}%"),
        _format_report_line(top_keywords_label, top_keywords),
    ])


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
        type_list_str = ""
        if type_sel:
            rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENTS", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
            db_types = [rev_map.get(t, t) for t in type_sel]
            type_list_str = ",".join([f"'{x}'" for x in db_types])
            type_where_sql = f"AND c.campaign_tp IN ({type_list_str})"

        sql = f"""
            SELECT f.dt, c.campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, SUM(f.cart_conv) as cart_conv, SUM(f.cart_sales) as cart_sales, SUM(f.conv) as conv, SUM(f.sales) as sales
            FROM fact_campaign_daily f
            {type_join_sql}
            WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
            GROUP BY f.dt, c.campaign_tp
        """
        df = pd.read_sql(sql, _engine)
        if not df.empty: df["dt"] = pd.to_datetime(df["dt"])
        return df
    except Exception: return pd.DataFrame()


def format_for_csv(df):
    out_df = df.copy()
    for col in out_df.columns:
        if out_df[col].dtype in ['float64', 'int64']:
            if col in ["노출수", "클릭수", "장바구니 담기수", "구매완료수", "총 전환수", "평균순위", "순위"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["광고비", "구매완료 매출", "장바구니 매출액", "총 전환매출", "CPC"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}원" if pd.notnull(x) else "0원")
            elif "차이" in col:
                if "광고비" in col or "매출" in col or "CPC" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                else: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
            elif "증감" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.0f}%" if pd.notnull(x) and x != 0 else "0%")
            elif "ROAS" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}%" if pd.notnull(x) else "0%")
            elif col == "클릭률(%)":
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.2f}%" if pd.notnull(x) else "0.00%")
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

@st.fragment
def render_account_campaign_detail(export_df_8, fmt_dict_standard, positive_cols, negative_cols):
    if not export_df_8.empty:
        accounts = export_df_8['업체명'].unique().tolist()
        selected_account = st.selectbox("상세 캠페인 성과를 확인할 업체 선택", options=accounts)

        if selected_account:
            sub_df = export_df_8[export_df_8['업체명'] == selected_account].drop(columns=['customer_id', '업체명'])
            sub_fmt_dict = fmt_dict_standard.copy()
            if '평균순위' in sub_df.columns: sub_fmt_dict["평균순위"] = "{:,.1f}위"; sub_fmt_dict["순위 변화"] = "{:+.1f}"
            styled_sub_df = sub_df.style.format(sub_fmt_dict)
            try:
                styled_sub_df = styled_sub_df.map(color_delta_positive, subset=positive_cols).map(color_delta_negative, subset=negative_cols)
            except AttributeError:
                styled_sub_df = styled_sub_df.applymap(color_delta_positive, subset=positive_cols).applymap(color_delta_negative, subset=negative_cols)
            st.dataframe(styled_sub_df, use_container_width=True, hide_index=True)
    else: st.info("해당 기간의 데이터가 없습니다.")


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
        "노출수": "{:,.0f}", "노출 증감": "{:+.0f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.0f}%", "클릭 차이": "{:+,.0f}",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.0f}%", "광고비 차이": "{:+,.0f}원",
        "CPC": "{:,.0f}원", "CPC 증감": "{:+.0f}%", "CPC 차이": "{:+,.0f}원",
        "장바구니 담기수": "{:,.0f}", "장바구니 증감": "{:+.0f}%", "장바구니 차이": "{:+,.0f}",
        "장바구니 매출액": "{:,.0f}원", "장바구니 ROAS(%)": "{:,.0f}%", "장바구니ROAS 증감": "{:+.0f}%",
        "구매완료수": "{:,.0f}", "구매 증감": "{:+.0f}%", "구매 차이": "{:+,.0f}",
        "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.0f}%", "구매 매출 차이": "{:+,.0f}원",
        "구매 ROAS(%)": "{:,.0f}%", "구매 ROAS 증감": "{:+.0f}%",
        "총 전환수": "{:,.0f}", "총 전환 증감": "{:+.0f}%", "총 전환 차이": "{:+,.0f}",
        "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.0f}%", "총 매출 차이": "{:+,.0f}원",
        "통합 ROAS(%)": "{:,.0f}%", "통합 ROAS 증감": "{:+.0f}%"
    }
    
    positive_cols = ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '장바구니 증감', '장바구니 차이', '장바구니ROAS 증감', '구매 증감', '구매 차이', '구매 매출 증감', '구매 매출 차이', '구매 ROAS 증감', '총 전환 증감', '총 전환 차이', '총 매출 증감', '총 매출 차이', '통합 ROAS 증감']
    negative_cols = ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이']

    # ✨ 에러의 원인이었던 type_kor_map을 함수 가장 바깥쪽으로 빼서 전역으로 설정했습니다.
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

    funnel_toggle = st.toggle("🔄 장바구니 / 구매완료 퍼널 분리해서 보기 (상세 모드)", value=False)

    cur = cur_summary
    base = base_summary

    cur['tot_conv'] = cur.get('conv', 0) + cur.get('cart_conv', 0)
    cur['tot_sales'] = cur.get('sales', 0) + cur.get('cart_sales', 0)
    cur['tot_roas'] = (cur['tot_sales'] / cur['cost'] * 100) if cur.get('cost', 0) > 0 else 0

    base['tot_conv'] = base.get('conv', 0) + base.get('cart_conv', 0)
    base['tot_sales'] = base.get('sales', 0) + base.get('cart_sales', 0)
    base['tot_roas'] = (base['tot_sales'] / base['cost'] * 100) if base.get('cost', 0) > 0 else 0

    def _delta_pct(key):
        try: return pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    def _kpi_html(label, value, delta_text, delta_val, highlight=False, improve_when_up=True):
        delta_num = float(delta_val) if delta_val is not None else 0.0
        is_neutral = abs(delta_num) < 5
        if is_neutral: cls_delta = "neu"; delta_text = f"유지 ({delta_num:+.1f}%)"
        else:
            improved = delta_num > 0 if improve_when_up else delta_num < 0
            cls_delta = "pos" if improved else "neg"
            delta_text = f"{pct_to_arrow(delta_num)}"
        cls_hl = " highlight-positive" if "ROAS" in label else (" highlight" if highlight else "")
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    if not funnel_toggle:
        kpi_html = f"""
        <div class='kpi-group-container'>
            <div class='kpi-group'><div class='kpi-group-title'>유입 지표</div><div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
            </div></div>
            <div class='kpi-group'><div class='kpi-group-title'>비용 지표</div><div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True, improve_when_up=False)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"), improve_when_up=False)}
            </div></div>
            <div class='kpi-group'><div class='kpi-group-title'>통합 전환 성과 (과거 호환)</div><div class='kpi-row'>
                {_kpi_html("통합 ROAS", f"{float(cur.get('tot_roas', 0.0) or 0.0):.0f}%", f"{pct_to_arrow(_delta_pct('tot_roas'))}", _delta_pct("tot_roas"), highlight=True)}
                {_kpi_html("총 전환수", format_number_commas(cur.get("tot_conv", 0.0)), f"{pct_to_arrow(_delta_pct('tot_conv'))}", _delta_pct("tot_conv"))}
                {_kpi_html("총 전환매출", format_currency(cur.get("tot_sales", 0.0)), f"{pct_to_arrow(_delta_pct('tot_sales'))}", _delta_pct("tot_sales"))}
            </div></div>
        </div>
        """
    else:
        kpi_html = f"""
        <div class='kpi-group-container'>
            <div class='kpi-group'><div class='kpi-group-title'>유입 및 비용</div><div class='kpi-row'>
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True, improve_when_up=False)}
            </div></div>
            <div class='kpi-group'><div class='kpi-group-title'>🛒 장바구니 전환 성과</div><div class='kpi-row'>
                {_kpi_html("장바구니 ROAS", f"{float(cur.get('cart_roas', 0.0) or 0.0):.0f}%", f"{pct_to_arrow(_delta_pct('cart_roas'))}", _delta_pct("cart_roas"), highlight=True)}
                {_kpi_html("장바구니수", format_number_commas(cur.get("cart_conv", 0.0)), f"{pct_to_arrow(_delta_pct('cart_conv'))}", _delta_pct("cart_conv"))}
                {_kpi_html("장바구니 매출액", format_currency(cur.get("cart_sales", 0.0)), f"{pct_to_arrow(_delta_pct('cart_sales'))}", _delta_pct("cart_sales"))}
            </div></div>
            <div class='kpi-group'><div class='kpi-group-title'>💰 순수 구매완료 성과</div><div class='kpi-row'>
                {_kpi_html("구매 ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("구매완료수", format_number_commas(cur.get("conv", 0.0)), f"{pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("구매완료 매출", format_currency(cur.get("sales", 0.0)), f"{pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"))}
            </div></div>
        </div>
        """
    st.markdown(kpi_html, unsafe_allow_html=True)


    st.markdown("<div class='nv-sec-title' style='margin-top:40px;'>📊 일자별 성과 추이</div>", unsafe_allow_html=True)
    if daily_ts is not None and not daily_ts.empty:
        daily_ts_chart = daily_ts.groupby('dt')[['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'conv', 'sales']].sum().reset_index()
        daily_ts_chart['tot_sales'] = daily_ts_chart['sales'] + daily_ts_chart['cart_sales']
        
        tab_t1, tab_t2 = st.tabs(["비용 및 매출 추이", "유입 지표 추이"])
        with tab_t1:
            y_col = "sales" if funnel_toggle else "tot_sales"
            y_name = "구매완료 매출" if funnel_toggle else "총 전환매출"
            render_echarts_dual_axis("", daily_ts_chart, "dt", "cost", "광고비", y_col, y_name, height=320)
        with tab_t2:
            render_echarts_dual_axis("", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
    else: st.info("해당 기간의 일자별 트렌드 데이터가 없습니다.")

    df_display, df_type_display, weekly_disp, weekly_tp_disp, dow_disp, camp_disp, daily_disp, export_df_8 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if not cur_camp.empty or not base_camp.empty:
        base_cols = ['customer_id', 'imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'conv', 'sales']
        for c in base_cols:
            if c not in cur_camp.columns: cur_camp[c] = 0.0
            if c not in base_camp.columns: base_camp[c] = 0.0

        cur_grp = cur_camp.groupby('customer_id')[base_cols[1:]].sum().reset_index() if not cur_camp.empty else pd.DataFrame(columns=base_cols)
        base_grp = base_camp.groupby('customer_id')[base_cols[1:]].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=base_cols)
        cur_grp['customer_id'] = cur_grp['customer_id'].astype(str)
        base_grp['customer_id'] = base_grp['customer_id'].astype(str)
        merged = pd.merge(cur_grp, base_grp, on='customer_id', how='outer', suffixes=('_cur', '_base')).fillna(0)
        
        if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
            meta_subset = meta[['customer_id', 'account_name']].copy()
            meta_subset['customer_id'] = meta_subset['customer_id'].astype(str)
            merged = merged.merge(meta_subset, on='customer_id', how='left')
            merged['account_name'] = merged['account_name'].fillna(merged['customer_id'])
        else: merged['account_name'] = merged['customer_id']
            
        merged = merged.sort_values('cost_cur', ascending=False)
        
        table_data = []
        for _, row in merged.iterrows():
            c_imp, c_clk, c_cost, c_cart, c_csales, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['cart_conv_cur'], row['cart_sales_cur'], row['conv_cur'], row['sales_cur']
            b_imp, b_clk, b_cost, b_cart, b_csales, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('cart_conv_base', 0), row.get('cart_sales_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
            
            c_tot_conv = c_conv + c_cart; c_tot_sales = c_sales + c_csales
            b_tot_conv = b_conv + b_cart; b_tot_sales = b_sales + b_csales

            c_cpc = (c_cost / c_clk) if c_clk > 0 else 0
            b_cpc = (b_cost / b_clk) if b_clk > 0 else 0
            
            c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
            b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
            c_croas = (c_csales / c_cost * 100) if c_cost > 0 else 0
            b_croas = (b_csales / b_cost * 100) if b_cost > 0 else 0
            c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0
            b_troas = (b_tot_sales / b_cost * 100) if b_cost > 0 else 0
            
            pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
            pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
            pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
            pct_cpc, diff_cpc = calc_pct_diff(c_cpc, b_cpc)
            
            pct_cart, diff_cart = calc_pct_diff(c_cart, b_cart)
            pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
            pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
            
            pct_tot_conv, diff_tot_conv = calc_pct_diff(c_tot_conv, b_tot_conv)
            pct_tot_sales, diff_tot_sales = calc_pct_diff(c_tot_sales, b_tot_sales)
            
            table_data.append({
                "업체명": row['account_name'],
                "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                "CPC": c_cpc, "CPC 증감": pct_cpc, "CPC 차이": diff_cpc,
                "장바구니 담기수": c_cart, "장바구니 증감": pct_cart, "장바구니 차이": diff_cart,
                "장바구니 매출액": c_csales, "장바구니 ROAS(%)": c_croas, "장바구니ROAS 증감": c_croas - b_croas,
                "구매완료수": c_conv, "구매 증감": pct_conv, "구매 차이": diff_conv,
                "구매완료 매출": c_sales, "구매 매출 증감": pct_sales, "구매 매출 차이": diff_sales,
                "구매 ROAS(%)": c_roas, "구매 ROAS 증감": c_roas - b_roas,
                "총 전환수": c_tot_conv, "총 전환 증감": pct_tot_conv, "총 전환 차이": diff_tot_conv,
                "총 전환매출": c_tot_sales, "총 매출 증감": pct_tot_sales, "총 매출 차이": diff_tot_sales,
                "통합 ROAS(%)": c_troas, "통합 ROAS 증감": c_troas - b_troas
            })
        df_display = pd.DataFrame(table_data)

    type_col = None
    if not cur_camp.empty and 'campaign_tp' in cur_camp.columns: type_col = 'campaign_tp'
    elif not cur_camp.empty and 'campaign_type' in cur_camp.columns: type_col = 'campaign_type'

    if type_col and (not cur_camp.empty or not base_camp.empty):
        cur_type_grp = cur_camp.groupby(type_col)[base_cols[1:]].sum().reset_index() if not cur_camp.empty else pd.DataFrame(columns=[type_col]+base_cols[1:])
        base_type_grp = base_camp.groupby(type_col)[base_cols[1:]].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=[type_col]+base_cols[1:])
        type_merged = pd.merge(cur_type_grp, base_type_grp, on=type_col, how='outer', suffixes=('_cur', '_base')).fillna(0)
        type_table_data = []
        for _, row in type_merged.iterrows():
            c_imp, c_clk, c_cost, c_cart, c_csales, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['cart_conv_cur'], row['cart_sales_cur'], row['conv_cur'], row['sales_cur']
            b_imp, b_clk, b_cost, b_cart, b_csales, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('cart_conv_base', 0), row.get('cart_sales_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
            
            c_tot_conv = c_conv + c_cart; c_tot_sales = c_sales + c_csales
            b_tot_conv = b_conv + b_cart; b_tot_sales = b_sales + b_csales

            c_cpc = (c_cost / c_clk) if c_clk > 0 else 0; b_cpc = (b_cost / b_clk) if b_clk > 0 else 0
            c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0; b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
            c_croas = (c_csales / c_cost * 100) if c_cost > 0 else 0; b_croas = (b_csales / b_cost * 100) if b_cost > 0 else 0
            c_troas = (c_tot_sales / c_cost * 100) if c_cost > 0 else 0; b_troas = (b_tot_sales / b_cost * 100) if b_cost > 0 else 0
            
            pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
            pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
            pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
            pct_cpc, diff_cpc = calc_pct_diff(c_cpc, b_cpc)
            pct_cart, diff_cart = calc_pct_diff(c_cart, b_cart)
            pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
            pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
            pct_tot_conv, diff_tot_conv = calc_pct_diff(c_tot_conv, b_tot_conv)
            pct_tot_sales, diff_tot_sales = calc_pct_diff(c_tot_sales, b_tot_sales)
            
            raw_tp = str(row[type_col]).upper() if pd.notnull(row[type_col]) else ""
            kor_tp = type_kor_map.get(raw_tp, raw_tp) if raw_tp else "기타"
            type_table_data.append({
                "캠페인 유형": kor_tp, "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                "CPC": c_cpc, "CPC 증감": pct_cpc, "CPC 차이": diff_cpc,
                "장바구니 담기수": c_cart, "장바구니 증감": pct_cart, "장바구니 차이": diff_cart,
                "장바구니 매출액": c_csales, "장바구니 ROAS(%)": c_croas, "장바구니ROAS 증감": c_croas - b_croas,
                "구매완료수": c_conv, "구매 증감": pct_conv, "구매 차이": diff_conv,
                "구매완료 매출": c_sales, "구매 매출 증감": pct_sales, "구매 매출 차이": diff_sales,
                "구매 ROAS(%)": c_roas, "구매 ROAS 증감": c_roas - b_roas,
                "총 전환수": c_tot_conv, "총 전환 증감": pct_tot_conv, "총 전환 차이": diff_tot_conv,
                "총 전환매출": c_tot_sales, "총 매출 증감": pct_tot_sales, "총 매출 차이": diff_tot_sales,
                "통합 ROAS(%)": c_troas, "통합 ROAS 증감": c_troas - b_troas
            })
        df_type_display = pd.DataFrame(type_table_data).sort_values("광고비", ascending=False)

    st.markdown("<div style='margin-top:40px; margin-bottom:10px;'></div>", unsafe_allow_html=True)
    has_data_to_export = any([not df_display.empty, not df_type_display.empty])
    if has_data_to_export:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer) as writer:
            if not df_display.empty: format_for_csv(df_display).to_excel(writer, sheet_name='업체별_전체요약', index=False)
            if not df_type_display.empty: format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과요약', index=False)
        st.download_button(label="📥 통합 데이터 다운로드", data=excel_buffer.getvalue(), file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    def _apply_depth_toggle(df, base_cols, toggle_state):
        out = df.copy()
        def _combine(r, c_val, c_pct, is_currency=False):
            v = r.get(c_val); p = r.get(c_pct)
            if pd.isna(v) or v == 0: return "-"
            v_str = f"{v:+,.0f}원" if is_currency else f"{v:+,.0f}"
            return f"{v_str} ({p:+.1f}%)"
            
        if not toggle_state:
            metrics = [
                ("노출수", "노출 차이", "노출 증감", False),
                ("클릭수", "클릭 차이", "클릭 증감", False),
                ("광고비", "광고비 차이", "광고비 증감", True),
                ("CPC", "CPC 차이", "CPC 증감", True),
                ("총 전환수", "총 전환 차이", "총 전환 증감", False),
                ("총 전환매출", "총 매출 차이", "총 매출 증감", True)
            ]
            for m in metrics: out[f"{m[0]} 증감/율"] = out.apply(lambda r: _combine(r, m[1], m[2], m[3]), axis=1)
            out["통합 ROAS 증감 "] = out["통합 ROAS 증감"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")
            
            display_cols = base_cols + ["노출수", "노출수 증감/율", "클릭수", "클릭수 증감/율", "광고비", "광고비 증감/율", "CPC", "CPC 증감/율", "총 전환수", "총 전환수 증감/율", "총 전환매출", "총 전환매출 증감/율", "통합 ROAS(%)", "통합 ROAS 증감 "]
            return out[[c for c in display_cols if c in out.columns]], [f"{m[0]} 증감/율" for m in metrics] + ["통합 ROAS 증감 "]
        else:
            metrics = [
                ("노출수", "노출 차이", "노출 증감", False),
                ("클릭수", "클릭 차이", "클릭 증감", False),
                ("광고비", "광고비 차이", "광고비 증감", True),
                ("CPC", "CPC 차이", "CPC 증감", True),
                ("장바구니 담기수", "장바구니 차이", "장바구니 증감", False),
                ("장바구니 매출액", "장바구니 매출액", "장바구니ROAS 증감", True), 
                ("구매완료수", "구매 차이", "구매 증감", False),
                ("구매완료 매출", "매출 차이", "매출 증감", True)
            ]
            for m in metrics: out[f"{m[0]} 증감/율"] = out.apply(lambda r: _combine(r, m[1], m[2], m[3]), axis=1)
            out["구매 ROAS 증감 "] = out["구매 ROAS 증감"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")
            
            display_cols = base_cols + ["노출수", "노출수 증감/율", "클릭수", "클릭수 증감/율", "광고비", "광고비 증감/율", "CPC", "CPC 증감/율", "장바구니 담기수", "장바구니 담기수 증감/율", "장바구니 매출액", "장바구니 매출액 증감/율", "구매완료수", "구매완료수 증감/율", "구매완료 매출", "구매완료 매출 증감/율", "구매 ROAS(%)", "구매 ROAS 증감 "]
            return out[[c for c in display_cols if c in out.columns]], [f"{m[0]} 증감/율" for m in metrics] + ["구매 ROAS 증감 "]

    with st.expander("🏢 업체별 전체 성과 요약", expanded=False):
        if not df_display.empty:
            disp_df, delta_cols_to_style = _apply_depth_toggle(df_display, ["업체명"], funnel_toggle)
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
            disp_type_df, delta_cols_to_style_type = _apply_depth_toggle(df_type_display, ["캠페인 유형"], funnel_toggle)
            disp_type_df = disp_type_df.set_index(["캠페인 유형"])
            styled_type_df = disp_type_df.style.format(fmt_dict_standard)
            try:
                styled_type_df = styled_type_df.map(style_delta_str, subset=[c for c in delta_cols_to_style_type if c not in ["광고비 증감/율", "CPC 증감/율"] and c in disp_type_df.columns])
                styled_type_df = styled_type_df.map(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in disp_type_df.columns])
            except AttributeError:
                pass
            st.dataframe(styled_type_df, use_container_width=True, hide_index=False)

    with st.expander("📝 보고서 내보내기", expanded=False):
        report_campaign_type = selected_type_label
        report_cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
        st.session_state[report_loaded_key] = True
        
        if not funnel_toggle:
            rep_conv = float(report_cur.get("conv", 0.0) or 0.0) + float(report_cur.get("cart_conv", 0.0) or 0.0)
            rep_sales = float(report_cur.get("sales", 0.0) or 0.0) + float(report_cur.get("cart_sales", 0.0) or 0.0)
            rep_roas = (rep_sales / float(report_cur.get("cost", 1))) * 100 if float(report_cur.get("cost", 0)) > 0 else 0
            
            report_text = "\n".join([
                f"[ {report_campaign_type} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(report_cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(report_cur.get('clk', 0))):,}"),
                _format_report_line("광고 소진비용", f"{int(float(report_cur.get('cost', 0))):,}원"),
                _format_report_line("총 전환수", f"{int(rep_conv):,}"),
                _format_report_line("총 전환매출", f"{int(rep_sales):,}원"),
                _format_report_line("통합 ROAS", f"{float(rep_roas):.2f}%")
            ])
        else:
            report_text = "\n".join([
                f"[ {report_campaign_type} 성과 요약 (상세) ]",
                _format_report_line("노출수", f"{int(float(report_cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(report_cur.get('clk', 0))):,}"),
                _format_report_line("광고 소진비용", f"{int(float(report_cur.get('cost', 0))):,}원"),
                _format_report_line("장바구니 담기수", f"{int(float(report_cur.get('cart_conv', 0))):,}"),
                _format_report_line("구매완료수", f"{int(float(report_cur.get('conv', 0))):,}"),
                _format_report_line("구매완료 매출", f"{int(float(report_cur.get('sales', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(report_cur.get('roas', 0)):.2f}%")
            ])
        st.code(report_text, language="text")
