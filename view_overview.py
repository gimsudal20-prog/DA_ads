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
def render_account_campaign_detail(export_df_8, fmt_dict_standard, positive_cols, negative_cols):
    if not export_df_8.empty:
        accounts = export_df_8['업체명'].unique().tolist()
        selected_account = st.selectbox("상세 캠페인 성과를 확인할 업체 선택", options=accounts)

        if selected_account:
            sub_df = export_df_8[export_df_8['업체명'] == selected_account].drop(columns=['customer_id', '업체명'])
            
            sub_fmt_dict = fmt_dict_standard.copy()
            if '평균순위' in sub_df.columns:
                sub_fmt_dict["평균순위"] = "{:,.1f}위"
                sub_fmt_dict["순위 변화"] = "{:+.1f}"
            
            styled_sub_df = sub_df.style.format(sub_fmt_dict)
            try:
                styled_sub_df = styled_sub_df.map(color_delta_positive, subset=positive_cols)
                styled_sub_df = styled_sub_df.map(color_delta_negative, subset=negative_cols)
            except AttributeError:
                styled_sub_df = styled_sub_df.applymap(color_delta_positive, subset=positive_cols)
                styled_sub_df = styled_sub_df.applymap(color_delta_negative, subset=negative_cols)
                
            st.dataframe(styled_sub_df, use_container_width=True, hide_index=True)
    else:
        st.info("해당 기간의 데이터가 없습니다.")


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f:
        return

    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    # ---------------------------------------------------------
    # 0. 핵심 데이터 백그라운드 연산 (Data Fetching & Pre-processing)
    # ---------------------------------------------------------
    with st.spinner("데이터를 집계 중입니다..."):
        cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
        base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)
        cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
        base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
        daily_ts = _cached_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        type_weekly_ts = _cached_type_timeseries(engine, f["start"], f["end"], cids, type_sel)

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
    # 1. 종합 성과 요약 (전체 유형)
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


    # ==========================================
    # 2. 일자별 성과 추이 (차트)
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top:40px;'>📊 일자별 성과 추이</div>", unsafe_allow_html=True)
    
    if daily_ts is not None and not daily_ts.empty:
        daily_ts_chart = daily_ts.groupby('dt')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
        daily_ts_chart['roas'] = np.where(daily_ts_chart['cost'] > 0, daily_ts_chart['sales'] / daily_ts_chart['cost'] * 100, 0)
        
        tab_t1, tab_t2 = st.tabs(["비용 및 매출 추이", "유입 지표 추이"])
        with tab_t1:
            render_echarts_dual_axis("", daily_ts_chart, "dt", "cost", "광고비", "sales", "전환매출", height=320)
        with tab_t2:
            render_echarts_dual_axis("", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
    else:
        st.info("해당 기간의 일자별 트렌드 데이터가 없습니다.")


    # ==========================================
    # 데이터 사전 생성 (아코디언 및 내보내기용)
    # ==========================================
    df_display = pd.DataFrame()
    df_type_display = pd.DataFrame()
    weekly_disp = pd.DataFrame()
    weekly_tp_disp = pd.DataFrame()
    dow_disp = pd.DataFrame()
    camp_disp = pd.DataFrame()
    daily_disp = pd.DataFrame()
    export_df_8 = pd.DataFrame()
    merged = pd.DataFrame()

    # 업체별 요약 데이터 연산
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

    # 유형별 요약 데이터 연산
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

    # 주간 성과 요약 데이터 연산
    def _get_week_info(dt_val):
        d = dt_val.date() if hasattr(dt_val, 'date') else dt_val
        start = d - timedelta(days=d.weekday())
        end = start + timedelta(days=6)
        thursday = start + timedelta(days=3)
        month = thursday.month
        week_num = (thursday.day - 1) // 7 + 1
        return f"{month}월 {week_num}주차 ({start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')})", start

    if daily_ts is not None and not daily_ts.empty:
        weekly_ts = daily_ts.groupby('dt')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
        weekly_ts['dt'] = pd.to_datetime(weekly_ts['dt'])
        week_info = weekly_ts['dt'].apply(_get_week_info)
        weekly_ts['week_label'] = [x[0] for x in week_info]
        weekly_ts['week_start'] = [x[1] for x in week_info]
        
        weekly_grp = weekly_ts.groupby(['week_start', 'week_label'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index().sort_values('week_start', ascending=True)
        weekly_grp['ctr'] = np.where(weekly_grp['imp'] > 0, weekly_grp['clk'] / weekly_grp['imp'] * 100, 0)
        weekly_grp['cpc'] = np.where(weekly_grp['clk'] > 0, weekly_grp['cost'] / weekly_grp['clk'], 0)
        weekly_grp['roas'] = np.where(weekly_grp['cost'] > 0, weekly_grp['sales'] / weekly_grp['cost'] * 100, 0)
        
        weekly_disp = weekly_grp[['week_label', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
        weekly_disp.columns = ['주간', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']

    if type_weekly_ts is not None and not type_weekly_ts.empty:
        type_weekly_ts['dt'] = pd.to_datetime(type_weekly_ts['dt'])
        week_info_tp = type_weekly_ts['dt'].apply(_get_week_info)
        type_weekly_ts['week_label'] = [x[0] for x in week_info_tp]
        type_weekly_ts['week_start'] = [x[1] for x in week_info_tp]
        
        weekly_tp_grp = type_weekly_ts.groupby(['week_start', 'week_label', 'campaign_tp'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index().sort_values(['week_start', 'cost'], ascending=[True, False])
        weekly_tp_grp['캠페인 유형'] = weekly_tp_grp['campaign_tp'].str.upper().map(type_kor_map).fillna(weekly_tp_grp['campaign_tp'])
        weekly_tp_grp['ctr'] = np.where(weekly_tp_grp['imp'] > 0, weekly_tp_grp['clk'] / weekly_tp_grp['imp'] * 100, 0)
        weekly_tp_grp['cpc'] = np.where(weekly_tp_grp['clk'] > 0, weekly_tp_grp['cost'] / weekly_tp_grp['clk'], 0)
        weekly_tp_grp['roas'] = np.where(weekly_tp_grp['cost'] > 0, weekly_tp_grp['sales'] / weekly_tp_grp['cost'] * 100, 0)
        
        weekly_tp_disp = weekly_tp_grp[['week_label', '캠페인 유형', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
        weekly_tp_disp.columns = ['주간', '캠페인 유형', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']

    # 상세 성과 데이터 연산 (캠페인별 / 일자별)
    if not cur_camp.empty:
        camp_disp = cur_camp.copy()
        camp_disp['roas'] = np.where(camp_disp['cost'] > 0, camp_disp['sales'] / camp_disp['cost'] * 100, 0)
        camp_disp['ctr'] = np.where(camp_disp['imp'] > 0, camp_disp['clk'] / camp_disp['imp'] * 100, 0)
        camp_disp['cpc'] = np.where(camp_disp['clk'] > 0, camp_disp['cost'] / camp_disp['clk'], 0)
        
        if 'campaign_type' in camp_disp.columns:
            camp_disp['campaign_type'] = camp_disp['campaign_type'].apply(lambda x: type_kor_map.get(str(x).upper(), x))
        elif 'campaign_tp' in camp_disp.columns:
            camp_disp['campaign_type'] = camp_disp['campaign_tp'].apply(lambda x: type_kor_map.get(str(x).upper(), x))
        
        cols = ['campaign_name', 'campaign_type', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']
        avail_cols = [c for c in cols if c in camp_disp.columns]
        camp_disp = camp_disp[avail_cols]
        
        kor_cols = []
        for c in avail_cols:
            if c == 'campaign_name': kor_cols.append('캠페인명')
            elif c == 'campaign_type': kor_cols.append('캠페인 유형')
            elif c == 'imp': kor_cols.append('노출수')
            elif c == 'clk': kor_cols.append('클릭수')
            elif c == 'ctr': kor_cols.append('클릭률(%)')
            elif c == 'cost': kor_cols.append('광고비')
            elif c == 'cpc': kor_cols.append('CPC')
            elif c == 'conv': kor_cols.append('전환수')
            elif c == 'sales': kor_cols.append('전환매출')
            elif c == 'roas': kor_cols.append('ROAS(%)')
            
        camp_disp.columns = kor_cols
        camp_disp = camp_disp.sort_values('광고비', ascending=False)

    if daily_ts is not None and not daily_ts.empty:
        daily_disp = daily_ts.copy()
        daily_disp['dt'] = pd.to_datetime(daily_disp['dt']).dt.strftime('%Y-%m-%d')
        daily_disp = daily_disp.groupby('dt')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
        daily_disp['roas'] = np.where(daily_disp['cost'] > 0, daily_disp['sales'] / daily_disp['cost'] * 100, 0)
        daily_disp['ctr'] = np.where(daily_disp['imp'] > 0, daily_disp['clk'] / daily_disp['imp'] * 100, 0)
        daily_disp['cpc'] = np.where(daily_disp['clk'] > 0, daily_disp['cost'] / daily_disp['clk'], 0)
        
        daily_disp = daily_disp[['dt', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']]
        daily_disp.columns = ['일자', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']
        daily_disp = daily_disp.sort_values('일자', ascending=False)

    # 요일별 성과 데이터 연산
    if daily_ts is not None and not daily_ts.empty:
        dow_ts = daily_ts.copy()
        dow_ts['dt'] = pd.to_datetime(dow_ts['dt'])
        dow_ts['day_of_week_num'] = dow_ts['dt'].dt.dayofweek
        dow_map = {0: '월요일', 1: '화요일', 2: '수요일', 3: '목요일', 4: '금요일', 5: '토요일', 6: '일요일'}
        dow_ts['요일'] = dow_ts['day_of_week_num'].map(dow_map)
        
        dow_grp = dow_ts.groupby(['day_of_week_num', '요일'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
        dow_grp['roas'] = np.where(dow_grp['cost'] > 0, dow_grp['sales'] / dow_grp['cost'] * 100, 0)
        dow_grp['ctr'] = np.where(dow_grp['imp'] > 0, dow_grp['clk'] / dow_grp['imp'] * 100, 0)
        dow_grp['cpc'] = np.where(dow_grp['clk'] > 0, dow_grp['cost'] / dow_grp['clk'], 0)
        
        dow_disp = dow_grp.sort_values('day_of_week_num')[['요일', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']]
        dow_disp.columns = ['요일', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']

    # 업체별 캠페인 상세 분석 데이터 연산 (For Export & UI)
    if not cur_camp.empty or not base_camp.empty:
        has_rank_cur = 'avg_rank' in cur_camp.columns
        has_rank_base = 'avg_rank' in base_camp.columns
        
        grp_cols_cur = ['customer_id', 'campaign_name']
        if has_rank_cur: grp_cols_cur.append('avg_rank')
        cur_sub_grp = cur_camp.groupby(grp_cols_cur)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not cur_camp.empty else pd.DataFrame(columns=grp_cols_cur+['imp', 'clk', 'cost', 'conv', 'sales'])
        
        grp_cols_base = ['customer_id', 'campaign_name']
        if has_rank_base: grp_cols_base.append('avg_rank')
        base_sub_grp = base_camp.groupby(grp_cols_base)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=grp_cols_base+['imp', 'clk', 'cost', 'conv', 'sales'])
        
        sub_merged = pd.merge(cur_sub_grp, base_sub_grp, on=['customer_id', 'campaign_name'], how='outer', suffixes=('_cur', '_base')).fillna(0)
        
        meta_mapping = merged[['customer_id', 'account_name']].drop_duplicates() if not merged.empty else pd.DataFrame(columns=['customer_id', 'account_name'])
        sub_merged = sub_merged.merge(meta_mapping, on='customer_id', how='left')
        sub_merged['account_name'] = sub_merged['account_name'].fillna(sub_merged['customer_id'])
        
        c_imp, c_clk, c_cost, c_conv, c_sales = sub_merged['imp_cur'], sub_merged['clk_cur'], sub_merged['cost_cur'], sub_merged['conv_cur'], sub_merged['sales_cur']
        b_imp, b_clk, b_cost, b_conv, b_sales = sub_merged['imp_base'], sub_merged['clk_base'], sub_merged['cost_base'], sub_merged['conv_base'], sub_merged['sales_base']
        
        roas_cur = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
        roas_base = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)
        
        def calc_pct_diff_vec(c, b):
            diff = c - b
            pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / b) * 100.0)
            return pct, diff
            
        pct_imp, diff_imp = calc_pct_diff_vec(c_imp, b_imp)
        pct_clk, diff_clk = calc_pct_diff_vec(c_clk, b_clk)
        pct_cost, diff_cost = calc_pct_diff_vec(c_cost, b_cost)
        pct_conv, diff_conv = calc_pct_diff_vec(c_conv, b_conv)
        pct_sales, diff_sales = calc_pct_diff_vec(c_sales, b_sales)
        
        export_df_8 = pd.DataFrame({
            "customer_id": sub_merged['customer_id'],
            "업체명": sub_merged['account_name'],
            "캠페인명": sub_merged['campaign_name'],
            "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
            "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
            "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
            "전환수": c_conv, "전환 증감": pct_conv, "전환 차이": diff_conv,
            "전환매출": c_sales, "매출 증감": pct_sales, "매출 차이": diff_sales,
            "ROAS": roas_cur, "ROAS 증감": roas_cur - roas_base
        })
        
        if has_rank_cur or has_rank_base:
            cur_rank = sub_merged.get('avg_rank_cur', pd.Series(0, index=sub_merged.index)).fillna(0)
            base_rank = sub_merged.get('avg_rank_base', pd.Series(0, index=sub_merged.index)).fillna(0)
            export_df_8["평균순위"] = cur_rank
            export_df_8["순위 변화"] = np.where((base_rank > 0) & (cur_rank > 0), base_rank - cur_rank, 0)

        export_df_8 = export_df_8.sort_values(['업체명', '광고비'], ascending=[True, False])


    # ==========================================
    # 📥 통합 데이터 다운로드 버튼 (엑셀 분할 내보내기)
    # ==========================================
    st.markdown("<div style='margin-top:40px; margin-bottom:10px;'></div>", unsafe_allow_html=True)
    
    excel_buffer = io.BytesIO()
    has_data_to_export = False
    
    with pd.ExcelWriter(excel_buffer) as writer:
        if not df_display.empty:
            format_for_csv(df_display).to_excel(writer, sheet_name='업체별_전체요약', index=False)
            has_data_to_export = True
        if not df_type_display.empty:
            format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과요약', index=False)
            has_data_to_export = True
        if not weekly_disp.empty:
            format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_전체합산', index=False)
            has_data_to_export = True
        if not weekly_tp_disp.empty:
            format_for_csv(weekly_tp_disp).to_excel(writer, sheet_name='주간_유형별상세', index=False)
            has_data_to_export = True
        if not dow_disp.empty:
            format_for_csv(dow_disp).to_excel(writer, sheet_name='요일별_요약', index=False)
            has_data_to_export = True
        if not camp_disp.empty:
            format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_상세', index=False)
            has_data_to_export = True
        if not daily_disp.empty:
            format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_상세', index=False)
            has_data_to_export = True
        if not export_df_8.empty:
            format_for_csv(export_df_8.drop(columns=['customer_id'])).to_excel(writer, sheet_name='업체별_캠페인상세', index=False)
            has_data_to_export = True
    
    if has_data_to_export:
        st.download_button(
            label="📥 통합 데이터 다운로드 (엑셀 시트 분리)",
            data=excel_buffer.getvalue(),
            file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )


    # ==========================================
    # 아코디언 메뉴 영역 (표 UI)
    # ==========================================
    with st.expander("🏢 업체별 전체 성과 요약", expanded=False):
        if not df_display.empty:
            styled_df = df_display.style.format(fmt_dict_standard)
            try:
                styled_df = styled_df.map(color_delta_positive, subset=positive_cols)
                styled_df = styled_df.map(color_delta_negative, subset=negative_cols)
            except AttributeError:
                styled_df = styled_df.applymap(color_delta_positive, subset=positive_cols)
                styled_df = styled_df.applymap(color_delta_negative, subset=negative_cols)
                
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 데이터가 없습니다.")


    with st.expander("🏷️ 유형별 성과 요약", expanded=False):
        if not df_type_display.empty:
            styled_type_df = df_type_display.style.format(fmt_dict_standard)
            try:
                styled_type_df = styled_type_df.map(color_delta_positive, subset=positive_cols)
                styled_type_df = styled_type_df.map(color_delta_negative, subset=negative_cols)
            except AttributeError:
                styled_type_df = styled_type_df.applymap(color_delta_positive, subset=positive_cols)
                styled_type_df = styled_type_df.applymap(color_delta_negative, subset=negative_cols)
                
            st.dataframe(styled_type_df, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 데이터가 없습니다.")


    with st.expander("📅 주간 성과 요약", expanded=False):
        if not weekly_disp.empty or not weekly_tp_disp.empty:
            tab_weekly_all, tab_weekly_type = st.tabs(["전체 합산", "유형별 상세"])
            
            with tab_weekly_all:
                if not weekly_disp.empty:
                    styled_weekly = weekly_disp.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원', '전환수': '{:,.0f}',
                        '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    })
                    st.dataframe(styled_weekly, use_container_width=True, hide_index=True)

            with tab_weekly_type:
                if not weekly_tp_disp.empty:
                    styled_weekly_tp = weekly_tp_disp.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원', '전환수': '{:,.0f}',
                        '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    })
                    st.dataframe(styled_weekly_tp, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 주간 트렌드 데이터가 없습니다.")


    with st.expander("📆 요일별 성과 요약", expanded=False):
        if not dow_disp.empty:
            styled_dow = dow_disp.style.format({
                '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원', '전환수': '{:,.0f}',
                '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
            })
            st.dataframe(styled_dow, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 요일별 성과 데이터가 없습니다.")


    with st.expander("📋 상세 성과 데이터 (캠페인별 / 일자별)", expanded=False):
        tab_det_camp, tab_det_daily = st.tabs(["캠페인별 상세", "일자별 상세"])
        
        with tab_det_camp:
            if not camp_disp.empty:
                st.dataframe(
                    camp_disp.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                        '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    }),
                    use_container_width=True, hide_index=True
                )
            else:
                st.info("캠페인별 상세 데이터가 없습니다.")

        with tab_det_daily:
            if not daily_disp.empty:
                st.dataframe(
                    daily_disp.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                        '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    }),
                    use_container_width=True, hide_index=True
                )
            else:
                st.info("일자별 상세 데이터가 없습니다.")


    with st.expander("🔍 업체별 캠페인 상세 분석", expanded=False):
        render_account_campaign_detail(export_df_8, fmt_dict_standard, positive_cols, negative_cols)


    # ==========================================
    # 보고서 내보내기 (텍스트 요약)
    # ==========================================
    with st.expander("📝 보고서 내보내기", expanded=False):
        report_campaign_type = selected_type_label
        report_cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)

        st.session_state[report_loaded_key] = True

        top_keywords_text = "-"
        is_shopping = False
        if type_sel and any("쇼핑" in t or "SHOPPING" in str(t).upper() for t in type_sel):
            is_shopping = True
            
        sort_col = "conv" if is_shopping else "clk"
        top_keywords_label = "전환이 많았던 키워드" if is_shopping else "클릭이 많았던 키워드"

        if st.session_state.get(report_loaded_key, False):
            with st.spinner("키워드 요약 중..."):
                kw_bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=100)
                
            if not kw_bundle.empty and {"keyword", sort_col}.issubset(kw_bundle.columns):
                kw_top = kw_bundle.copy()
                kw_top[sort_col] = pd.to_numeric(kw_top[sort_col], errors="coerce").fillna(0)
                kw_top = kw_top.groupby("keyword", as_index=False)[sort_col].sum().sort_values(sort_col, ascending=False).head(3)
                if not kw_top.empty:
                    top_keywords_text = ", ".join([str(x).strip() for x in kw_top["keyword"].tolist() if str(x).strip()]) or "-"
        
        report_text = _build_periodic_report_text(
            campaign_type=report_campaign_type,
            imp=float(report_cur.get("imp", 0.0) or 0.0),
            clk=float(report_cur.get("clk", 0.0) or 0.0),
            ctr=float(report_cur.get("ctr", 0.0) or 0.0),
            cost=float(report_cur.get("cost", 0.0) or 0.0),
            roas=float(report_cur.get("roas", 0.0) or 0.0),
            sales=float(report_cur.get("sales", 0.0) or 0.0),
            top_keywords_label=top_keywords_label,
            top_keywords=top_keywords_text,
        )
        st.code(report_text, language="text")

}
