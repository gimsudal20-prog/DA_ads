# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta


def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"

def _build_periodic_report_text(campaign_type: str, imp: float, clk: float, ctr: float, cost: float, roas: float, sales: float, top_keywords_label: str, top_keywords: str) -> str:
    return "\n".join([
        f"[ {campaign_type} 성과 요약 ]",
        _format_report_line("노출수", f"{int(imp):,}"),
        _format_report_line("클릭수", f"{int(clk):,}"),
        _format_report_line("클릭률", f"{float(ctr):.2f}%"),
        _format_report_line("광고 소진비용", f"{int(cost):,}원"),
        _format_report_line("전환매출", f"{int(sales):,}원"),
        _format_report_line("ROAS", f"{float(roas):.2f}%"),
        _format_report_line(top_keywords_label, top_keywords),
    ])


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
def _cached_campaign_timeseries(_engine, trend_d1, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        ts = query_campaign_timeseries(_engine, trend_d1, end_dt, cids, type_sel)
        return ts if ts is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


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
            SELECT f.dt, c.campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, SUM(f.conv) as conv, SUM(f.sales) as sales
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
        try:
            sql = f"""
                SELECT f.dt, c.campaign_type as campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, SUM(f.conv) as conv, SUM(f.sales) as sales
                FROM fact_campaign_daily f
                {type_join_sql}
                WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
                GROUP BY f.dt, c.campaign_type
            """
            df = pd.read_sql(sql, _engine)
            if not df.empty: 
                df["dt"] = pd.to_datetime(df["dt"])
            return df
        except Exception:
            pass
    return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_account_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    cid_str = ",".join([f"'{str(x)}'" for x in cids])
    where_cid = f"AND f.customer_id IN ({cid_str})" if cids else ""
    
    type_join_sql = "JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id" if type_sel else ""
    type_where_sql = ""
    if type_sel:
        rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENTS", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
        db_types = [rev_map.get(t, t) for t in type_sel]
        type_list_str = ",".join([f"'{x}'" for x in db_types])
        type_where_sql = f"AND c.campaign_tp IN ({type_list_str})"

    try:
        sql = f"""
            SELECT f.dt, f.customer_id, SUM(f.cost) as cost, SUM(f.sales) as sales, SUM(f.conv) as conv, SUM(f.imp) as imp, SUM(f.clk) as clk
            FROM fact_campaign_daily f
            {type_join_sql}
            WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
            GROUP BY f.dt, f.customer_id
        """
        df = pd.read_sql(sql, _engine)
        if not df.empty: df["dt"] = pd.to_datetime(df["dt"])
        return df
    except:
        try:
            if type_sel: type_where_sql = f"AND c.campaign_type IN ({type_list_str})"
            sql = f"""
                SELECT f.dt, f.customer_id, SUM(f.cost) as cost, SUM(f.sales) as sales, SUM(f.conv) as conv, SUM(f.imp) as imp, SUM(f.clk) as clk
                FROM fact_campaign_daily f
                {type_join_sql}
                WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
                GROUP BY f.dt, f.customer_id
            """
            df = pd.read_sql(sql, _engine)
            if not df.empty: df["dt"] = pd.to_datetime(df["dt"])
            return df
        except:
            return pd.DataFrame()


def format_for_csv(df):
    out_df = df.copy()
    for col in out_df.columns:
        if out_df[col].dtype in ['float64', 'int64']:
            if col in ["노출수", "클릭수", "전환수", "평균순위", "순위"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["광고비", "전환매출", "CPC"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}원" if pd.notnull(x) else "0원")
            elif "차이" in col:
                if "광고비" in col or "매출" in col:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                else:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
            elif "증감" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.0f}%" if pd.notnull(x) and x != 0 else "0%")
            elif col in ["ROAS", "ROAS(%)"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}%" if pd.notnull(x) else "0%")
            elif col == "클릭률(%)":
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.2f}%" if pd.notnull(x) else "0.00%")
            elif col == "순위 변화":
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.0f}" if pd.notnull(x) and x != 0 else "0")
    return out_df

def calc_pct_diff(c, b):
    diff = c - b
    if b == 0:
        pct = 100.0 if c > 0 else 0.0
    else:
        pct = (diff) / b * 100.0
    return pct, diff


def color_delta_positive(val):
    if pd.isna(val) or val == 0: return 'color: #A8AFB7;'
    return 'color: #0528F2; font-weight: 600;' if val > 0 else 'color: #F04438; font-weight: 600;'

def color_delta_negative(val):
    if pd.isna(val) or val == 0: return 'color: #A8AFB7;'
    return 'color: #F04438; font-weight: 600;' if val > 0 else 'color: #0528F2; font-weight: 600;'


@st.fragment
def render_account_campaign_detail(merged, cur_camp, base_camp, fmt_dict_standard, positive_cols, negative_cols, f_start, f_end):
    st.markdown("<div class='nv-sec-title'>업체별 캠페인 상세 분석</div>", unsafe_allow_html=True)
    
    if not merged.empty:
        selected_account = st.selectbox("상세 캠페인 성과를 확인할 업체 선택", options=merged['account_name'].tolist())

        if selected_account:
            selected_cid = merged[merged['account_name'] == selected_account]['customer_id'].iloc[0]
            
            cur_camp_sub = cur_camp[cur_camp['customer_id'].astype(str) == str(selected_cid)] if not cur_camp.empty else pd.DataFrame()
            base_camp_sub = base_camp[base_camp['customer_id'].astype(str) == str(selected_cid)] if not base_camp.empty else pd.DataFrame()
            
            has_rank = ('avg_rank' in cur_camp_sub.columns) or ('avg_rank' in base_camp_sub.columns)
            
            grp_cols = ['campaign_name']
            if has_rank: grp_cols.append('avg_rank')
            
            cur_sub_grp = cur_camp_sub.groupby(grp_cols)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not cur_camp_sub.empty else pd.DataFrame(columns=grp_cols + ['imp', 'clk', 'cost', 'conv', 'sales'])
            base_sub_grp = base_camp_sub.groupby(grp_cols)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not base_camp_sub.empty else pd.DataFrame(columns=grp_cols + ['imp', 'clk', 'cost', 'conv', 'sales'])
            
            sub_merged = pd.merge(cur_sub_grp, base_sub_grp, on='campaign_name', how='outer', suffixes=('_cur', '_base')).fillna(0)
            
            if has_rank:
                if 'avg_rank_cur' in sub_merged.columns: sub_merged['avg_rank'] = sub_merged['avg_rank_cur']
                elif 'avg_rank' not in sub_merged.columns: sub_merged['avg_rank'] = 0
            
            sub_merged = sub_merged.sort_values('cost_cur', ascending=False)
            
            sub_table_data = []
            for _, row in sub_merged.iterrows():
                c_imp, c_clk, c_cost, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['conv_cur'], row['sales_cur']
                b_imp, b_clk, b_cost, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
                
                c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
                b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
                
                pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
                pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
                pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
                pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
                pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
                
                row_data = {
                    "캠페인명": row['campaign_name'],
                    "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                    "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                    "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                    "전환수": c_conv, "전환 증감": pct_conv, "전환 차이": diff_conv,
                    "전환매출": c_sales, "매출 증감": pct_sales, "매출 차이": diff_sales,
                    "ROAS": c_roas, "ROAS 증감": c_roas - b_roas
                }
                
                if has_rank:
                    cur_rank = row.get('avg_rank_cur', 0)
                    base_rank = row.get('avg_rank_base', 0)
                    row_data["평균순위"] = cur_rank
                    row_data["순위 변화"] = (base_rank - cur_rank) if (base_rank > 0 and cur_rank > 0) else 0
                    
                sub_table_data.append(row_data)
                
            df_sub_display = pd.DataFrame(sub_table_data)
            
            sub_fmt_dict = fmt_dict_standard.copy()
            if has_rank:
                sub_fmt_dict["평균순위"] = "{:,.1f}위"
                sub_fmt_dict["순위 변화"] = "{:+.1f}"
            
            styled_sub_df = df_sub_display.style.format(sub_fmt_dict)
            try:
                styled_sub_df = styled_sub_df.map(color_delta_positive, subset=positive_cols)
                styled_sub_df = styled_sub_df.map(color_delta_negative, subset=negative_cols)
            except AttributeError:
                styled_sub_df = styled_sub_df.applymap(color_delta_positive, subset=positive_cols)
                styled_sub_df = styled_sub_df.applymap(color_delta_negative, subset=negative_cols)
                
            st.dataframe(styled_sub_df, use_container_width=True, hide_index=True)
            
            csv_sub_data = format_for_csv(df_sub_display).to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="해당 캠페인 CSV 다운로드",
                data=csv_sub_data,
                file_name=f"{selected_account}_캠페인_상세_{f_start}_{f_end}.csv",
                mime="text/csv",
                key=f"download_sub_csv_{selected_cid}"
            )


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f:
        return

    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    state_sig = f"{f['start']}|{f['end']}|{','.join(map(str, cids))}|{','.join(type_sel)}"
    state_hash = abs(hash(state_sig))
    report_loaded_key = f"overview_report_loaded_{state_hash}"

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.0f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.0f}%", "클릭 차이": "{:+,.0f}",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.0f}%", "광고비 차이": "{:+,.0f}원",
        "전환수": "{:,.0f}", "전환 증감": "{:+.0f}%", "전환 차이": "{:+,.0f}",
        "전환매출": "{:,.0f}원", "매출 증감": "{:+.0f}%", "매출 차이": "{:+,.0f}원",
        "ROAS": "{:,.0f}%", "ROAS 증감": "{:+.0f}%"
    }
    
    positive_cols = ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '전환 증감', '전환 차이', '매출 증감', '매출 차이', 'ROAS 증감']
    negative_cols = ['광고비 증감', '광고비 차이']

    type_kor_map = {
        "WEB_SITE": "파워링크", 
        "SHOPPING": "쇼핑검색", 
        "POWER_CONTENTS": "파워컨텐츠", 
        "BRAND_SEARCH": "브랜드검색", 
        "PLACE": "플레이스"
    }

    # ==========================================
    # 1. 전체 성과 요약 (KPI Box)
    # ==========================================
    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약 ({selected_type_label})</div>", unsafe_allow_html=True)
    
    cmp_date_info = f"{cmp_mode} ({b1} ~ {b2})" if b1 and b2 else cmp_mode
    st.markdown(f"<div style='font-size:12px; font-weight:500; color:var(--nv-muted); margin-bottom:16px;'>비교 기준: <span style='color:var(--nv-primary); font-weight:600;'>{cmp_date_info}</span></div>", unsafe_allow_html=True)

    cur = cur_summary
    base = base_summary

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
        
        if highlight:
            cls_hl = " highlight-positive" if label == "ROAS" else " highlight"
        else:
            cls_hl = ""
            
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    kpi_groups_html = f"""
    <div class='kpi-group-container'>
        <div class='kpi-group'>
            <div class='kpi-group-title'>유입 지표</div>
            <div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>비용 지표</div>
            <div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True, improve_when_up=False)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"), improve_when_up=False)}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>성과 지표</div>
            <div class='kpi-row'>
                {_kpi_html("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("전환수", format_number_commas(cur.get("conv", 0.0)), f"{pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("전환매출", format_currency(cur.get("sales", 0.0)), f"{pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"))}
            </div>
        </div>
    </div>
    """
    st.markdown(kpi_groups_html, unsafe_allow_html=True)


    with st.spinner("상세 성과를 로딩 중입니다..."):
        cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
        base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)


    # ==========================================
    # 2. 업체별 전체 성과 요약 (테이블)
    # ==========================================
    st.markdown("<div class='nv-sec-title'>업체별 전체 성과 요약</div>", unsafe_allow_html=True)

    merged = pd.DataFrame() 
    if not cur_camp.empty or not base_camp.empty:
        base_cols = ['customer_id', 'imp', 'clk', 'cost', 'conv', 'sales']
        cur_grp = cur_camp.groupby('customer_id')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not cur_camp.empty else pd.DataFrame(columns=base_cols)
        base_grp = base_camp.groupby('customer_id')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=base_cols)
        
        cur_grp['customer_id'] = cur_grp['customer_id'].astype(str)
        base_grp['customer_id'] = base_grp['customer_id'].astype(str)
        
        merged = pd.merge(cur_grp, base_grp, on='customer_id', how='outer', suffixes=('_cur', '_base')).fillna(0)
        
        if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
            meta_subset = meta[['customer_id', 'account_name']].copy()
            meta_subset['customer_id'] = meta_subset['customer_id'].astype(str)
            merged = merged.merge(meta_subset, on='customer_id', how='left')
            merged['account_name'] = merged['account_name'].fillna(merged['customer_id'])
        else:
            merged['account_name'] = merged['customer_id']
            
        merged = merged.sort_values('cost_cur', ascending=False)
        
        table_data = []
        for _, row in merged.iterrows():
            c_imp, c_clk, c_cost, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['conv_cur'], row['sales_cur']
            b_imp, b_clk, b_cost, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
            
            c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
            b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
            
            pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
            pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
            pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
            pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
            pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
            
            table_data.append({
                "업체명": row['account_name'],
                "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                "전환수": c_conv, "전환 증감": pct_conv, "전환 차이": diff_conv,
                "전환매출": c_sales, "매출 증감": pct_sales, "매출 차이": diff_sales,
                "ROAS": c_roas, "ROAS 증감": c_roas - b_roas
            })
            
        df_display = pd.DataFrame(table_data)
        
        styled_df = df_display.style.format(fmt_dict_standard)
        if hasattr(styled_df, 'map'):
            styled_df = styled_df.map(color_delta_positive, subset=positive_cols)
            styled_df = styled_df.map(color_delta_negative, subset=negative_cols)
        else:
            styled_df = styled_df.applymap(color_delta_positive, subset=positive_cols)
            styled_df = styled_df.applymap(color_delta_negative, subset=negative_cols)
            
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        csv_account_data = format_for_csv(df_display).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="업체별 요약 다운로드",
            data=csv_account_data,
            file_name=f"업체별_전체_성과_요약_{f['start']}_{f['end']}.csv",
            mime="text/csv",
            key="download_account_csv"
        )
    else:
        st.info("해당 기간의 캠페인 데이터가 없습니다.")

    # ==========================================
    # ✨ 업체별 KPI 달성 현황 및 트렌드 (스파크라인 그래프)
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top:40px;'>🎯 업체별 KPI 달성 현황 및 성과 트렌드</div>", unsafe_allow_html=True)
    st.caption("각 업체별 목표(KPI)를 설정하고 최근 일자별 비용과 성과의 흐름을 직관적으로 확인하세요.")
    
    acc_ts_df = _cached_account_timeseries(engine, f["start"], f["end"], cids, type_sel)
    
    if not acc_ts_df.empty:
        acc_ts_df['customer_id'] = acc_ts_df['customer_id'].astype(str)
        if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
            meta_subset = meta[['customer_id', 'account_name']].copy()
            meta_subset['customer_id'] = meta_subset['customer_id'].astype(str)
            acc_ts_df = acc_ts_df.merge(meta_subset, on='customer_id', how='left')
            acc_ts_df['account_name'] = acc_ts_df['account_name'].fillna(acc_ts_df['customer_id'])
        else:
            acc_ts_df['account_name'] = acc_ts_df['customer_id']
            
        acc_ts_df['roas'] = np.where(acc_ts_df['cost'] > 0, (acc_ts_df['sales'] / acc_ts_df['cost']) * 100, 0.0)
        acc_ts_df['dt'] = pd.to_datetime(acc_ts_df['dt'])
        acc_ts_df = acc_ts_df.sort_values('dt')
        
        acc_totals = acc_ts_df.groupby(['customer_id', 'account_name'])['cost'].sum().reset_index()
        acc_totals = acc_totals.sort_values('cost', ascending=False).head(10)
        
        for _, row in acc_totals.iterrows():
            cid_val = row['customer_id']
            acc_name = row['account_name']
            
            acc_data = acc_ts_df[acc_ts_df['customer_id'] == cid_val].copy()
            
            total_cost = acc_data['cost'].sum()
            total_sales = acc_data['sales'].sum()
            curr_roas = (total_sales / total_cost * 100) if total_cost > 0 else 0.0
            
            with st.container(border=True):
                c1, c2, c3 = st.columns([1, 1.5, 1.5])
                
                with c1:
                    st.markdown(f"<div style='font-size:15px; font-weight:700; margin-bottom:12px;'>🏢 {acc_name}</div>", unsafe_allow_html=True)
                    
                    tgt_key = f"tgt_roas_{cid_val}"
                    target_roas = st.number_input("🎯 목표 ROAS (%)", value=300, step=50, key=tgt_key, label_visibility="collapsed")
                    
                    color = "#0528F2" if curr_roas >= target_roas else "#F04438"
                    status_emoji = "🔥 달성" if curr_roas >= target_roas else "⚠️ 미달"
                    
                    st.markdown(f"""
                        <div style='background:var(--nv-surface); padding:10px 14px; border-radius:8px; margin-top:8px;'>
                            <div style='font-size:12px; color:var(--nv-muted); font-weight:600;'>현재 평균 ROAS</div>
                            <div style='font-size:22px; font-weight:800; color:{color};'>{curr_roas:,.0f}% <span style='font-size:13px; font-weight:600;'>{status_emoji}</span></div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                with c2:
                    st.markdown("<div style='font-size:12px; color:var(--nv-muted); font-weight:600;'>📉 비용(광고비) 소진 추이</div>", unsafe_allow_html=True)
                    fig_cost = px.bar(acc_data, x='dt', y='cost')
                    fig_cost.update_traces(marker_color='#A8AFB7', marker_line_width=0)
                    fig_cost.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0), 
                        height=110, 
                        xaxis=dict(visible=False, showgrid=False), 
                        yaxis=dict(visible=False, showgrid=False),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    # ✨ [FIX] 고유 Key 값 추가
                    st.plotly_chart(fig_cost, use_container_width=True, config={'displayModeBar': False}, key=f"cost_chart_{cid_val}")
                    
                with c3:
                    st.markdown("<div style='font-size:12px; color:var(--nv-muted); font-weight:600;'>📈 ROAS 성과 추이</div>", unsafe_allow_html=True)
                    fig_roas = px.line(acc_data, x='dt', y='roas')
                    fig_roas.update_traces(line_color='#0528F2', line_width=3)
                    
                    fig_roas.add_hline(
                        y=target_roas, 
                        line_dash="dot", 
                        line_color="#F04438", 
                        annotation_text="목표", 
                        annotation_position="bottom right",
                        annotation_font_color="#F04438"
                    )
                    
                    fig_roas.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0), 
                        height=110, 
                        xaxis=dict(visible=False, showgrid=False), 
                        yaxis=dict(visible=False, showgrid=False),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    # ✨ [FIX] 고유 Key 값 추가
                    st.plotly_chart(fig_roas, use_container_width=True, config={'displayModeBar': False}, key=f"roas_chart_{cid_val}")
    else:
        st.info("선택하신 기간 내 업체별 트렌드 데이터가 없습니다.")


    # ==========================================
    # 4. 유형별 성과 요약
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top:40px;'>유형별 성과 요약</div>", unsafe_allow_html=True)
    
    type_col = None
    if not cur_camp.empty and 'campaign_tp' in cur_camp.columns: type_col = 'campaign_tp'
    elif not cur_camp.empty and 'campaign_type' in cur_camp.columns: type_col = 'campaign_type'
    elif not base_camp.empty and 'campaign_tp' in base_camp.columns: type_col = 'campaign_tp'
    elif not base_camp.empty and 'campaign_type' in base_camp.columns: type_col = 'campaign_type'

    if type_col and (not cur_camp.empty or not base_camp.empty):
        cur_type_grp = cur_camp.groupby(type_col)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not cur_camp.empty else pd.DataFrame(columns=[type_col, 'imp', 'clk', 'cost', 'conv', 'sales'])
        base_type_grp = base_camp.groupby(type_col)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=[type_col, 'imp', 'clk', 'cost', 'conv', 'sales'])
        
        type_merged = pd.merge(cur_type_grp, base_type_grp, on=type_col, how='outer', suffixes=('_cur', '_base')).fillna(0)
        type_merged = type_merged.sort_values('cost_cur', ascending=False)
        
        type_table_data = []
        for _, row in type_merged.iterrows():
            c_imp, c_clk, c_cost, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['conv_cur'], row['sales_cur']
            b_imp, b_clk, b_cost, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
            
            c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
            b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
            
            pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
            pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
            pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
            pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
            pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
            
            raw_tp = str(row[type_col]).upper() if pd.notnull(row[type_col]) else ""
            kor_tp = type_kor_map.get(raw_tp, raw_tp) if raw_tp else "기타"
            
            type_table_data.append({
                "캠페인 유형": kor_tp,
                "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                "전환수": c_conv, "전환 증감": pct_conv, "전환 차이": diff_conv,
                "전환매출": c_sales, "매출 증감": pct_sales, "매출 차이": diff_sales,
                "ROAS": c_roas, "ROAS 증감": c_roas - b_roas
            })
            
        df_type_display = pd.DataFrame(type_table_data)
        
        styled_type_df = df_type_display.style.format(fmt_dict_standard)
        if hasattr(styled_type_df, 'map'):
            styled_type_df = styled_type_df.map(color_delta_positive, subset=positive_cols)
            styled_type_df = styled_type_df.map(color_delta_negative, subset=negative_cols)
        else:
            styled_type_df = styled_type_df.applymap(color_delta_positive, subset=positive_cols)
            styled_type_df = styled_type_df.applymap(color_delta_negative, subset=negative_cols)
            
        st.dataframe(styled_type_df, use_container_width=True, hide_index=True)

        csv_type_data = format_for_csv(df_type_display).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="유형별 요약 다운로드",
            data=csv_type_data,
            file_name=f"유형별_전체_성과_요약_{f['start']}_{f['end']}.csv",
            mime="text/csv",
            key="download_type_csv"
        )
        
    render_account_campaign_detail(merged, cur_camp, base_camp, fmt_dict_standard, positive_cols, negative_cols, f["start"], f["end"])
