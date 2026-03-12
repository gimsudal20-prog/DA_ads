# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
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
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_keyword_bundle(_engine, start_dt, end_dt, list(cids), type_sel, topn_cost=0)
    except Exception:
        return pd.DataFrame()


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
def _cached_trend_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        cid_str = ",".join([f"'{str(x)}'" for x in cids])
        where_cid = f"AND f.customer_id IN ({cid_str})" if cids else ""
        
        type_join_sql = "LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
        type_where_sql = ""
        type_list_str = ""
        if type_sel:
            rev_map = {"파워링크": "WEB_SITE", "쇼핑검색": "SHOPPING", "파워컨텐츠": "POWER_CONTENTS", "브랜드검색": "BRAND_SEARCH", "플레이스": "PLACE"}
            db_types = [rev_map.get(t, t) for t in type_sel]
            type_list_str = ",".join([f"'{x}'" for x in db_types])
            type_where_sql = f"AND c.campaign_tp IN ({type_list_str})"

        sql = f"""
            SELECT f.dt, f.customer_id, c.campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, SUM(f.conv) as conv, SUM(f.sales) as sales
            FROM fact_campaign_daily f
            {type_join_sql}
            WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
            GROUP BY f.dt, f.customer_id, c.campaign_tp
        """
        df = pd.read_sql(sql, _engine)
        if not df.empty: 
            df["dt"] = pd.to_datetime(df["dt"])
            df["customer_id"] = df["customer_id"].astype(str)
        return df
    except Exception:
        try:
            if type_sel:
                type_where_sql = f"AND c.campaign_type IN ({type_list_str})"
            sql = f"""
                SELECT f.dt, f.customer_id, c.campaign_type as campaign_tp, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost, SUM(f.conv) as conv, SUM(f.sales) as sales
                FROM fact_campaign_daily f
                {type_join_sql}
                WHERE f.dt >= '{start_dt}' AND f.dt <= '{end_dt}' {where_cid} {type_where_sql}
                GROUP BY f.dt, f.customer_id, c.campaign_type
            """
            df = pd.read_sql(sql, _engine)
            if not df.empty: 
                df["dt"] = pd.to_datetime(df["dt"])
                df["customer_id"] = df["customer_id"].astype(str)
            return df
        except Exception:
            pass
    return pd.DataFrame()


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

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)
    
    # ---------------------------------------------------------
    # 공통 헬퍼 변수 및 함수 선언
    # ---------------------------------------------------------
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

    def color_delta(val):
        if pd.isna(val) or val == 0: return 'color: #888888;'
        return 'color: #FC503D; font-weight: bold;' if val > 0 else 'color: #375FFF; font-weight: bold;'

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.0f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.0f}%", "클릭 차이": "{:+,.0f}",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.0f}%", "광고비 차이": "{:+,.0f}원",
        "전환수": "{:,.0f}", "전환 증감": "{:+.0f}%", "전환 차이": "{:+,.0f}",
        "전환매출": "{:,.0f}원", "매출 증감": "{:+.0f}%", "매출 차이": "{:+,.0f}원",
        "ROAS": "{:,.0f}%", "ROAS 증감": "{:+.0f}%"
    }
    
    color_cols_standard = ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '광고비 증감', '광고비 차이', '전환 증감', '전환 차이', '매출 증감', '매출 차이', 'ROAS 증감']

    type_kor_map = {
        "WEB_SITE": "파워링크", 
        "SHOPPING": "쇼핑검색", 
        "POWER_CONTENTS": "파워컨텐츠", 
        "BRAND_SEARCH": "브랜드검색", 
        "PLACE": "플레이스"
    }

    # 하위 상세 데이터 병합을 위한 공통 데이터 로드
    with st.spinner("성과 데이터 로딩 중..."):
        cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
        base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)

    # ==========================================
    # 1. 전체 성과 요약 (KPI Box)
    # ==========================================
    st.markdown(f"<div class='nv-sec-title'>📊 {account_name} 종합 성과 요약 ({selected_type_label})</div>", unsafe_allow_html=True)
    
    cmp_date_info = f"{cmp_mode} ({b1} ~ {b2})" if b1 and b2 else cmp_mode
    st.markdown(f"<div style='font-size:13px; font-weight:500; color:#474747; margin-bottom:12px;'>비교 기준: <span style='color:#375FFF; font-weight:700;'>{cmp_date_info}</span></div>", unsafe_allow_html=True)

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
            delta_text = f"● 유지 ({delta_num:+.1f}%)"
        else:
            improved = delta_num > 0 if improve_when_up else delta_num < 0
            cls_delta = "pos" if improved else "neg"
            trend_label = "✓ 개선" if improved else "✕ 악화"
            delta_text = f"{trend_label} {pct_to_arrow(delta_num)}"
        cls_hl = " highlight" if highlight else ""
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    kpi_groups_html = f"""
    <div class='kpi-group-container'>
        <div class='kpi-group'>
            <div class='kpi-group-title'>👀 유입 지표</div>
            <div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>💸 비용 지표</div>
            <div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True, improve_when_up=False)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"), improve_when_up=False)}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>🎯 성과 지표</div>
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
    # 1.5 일자별 성과 추이 (트렌드 차트 복구)
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top: 32px;'>📈 일자별 성과 추이</div>", unsafe_allow_html=True)
    
    with st.spinner("트렌드 차트 데이터 집계 중..."):
        daily_ts = _cached_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        
    if daily_ts is not None and not daily_ts.empty:
        daily_ts_chart = daily_ts.copy()
        # 누락된 파생 변수(ROAS 등) 계산
        daily_ts_chart['roas'] = np.where(daily_ts_chart['cost'] > 0, daily_ts_chart['sales'] / daily_ts_chart['cost'] * 100, 0)
        
        tab_t1, tab_t2 = st.tabs(["💸 비용 및 매출 추이", "👀 유입 지표 추이"])
        with tab_t1:
            render_echarts_dual_axis("", daily_ts_chart, "dt", "cost", "광고비", "sales", "전환매출", height=320)
        with tab_t2:
            render_echarts_dual_axis("", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
    else:
        st.info("해당 기간의 일자별 트렌드 데이터가 없습니다.")


    # ==========================================
    # 2. 업체별 전체 성과 요약 (테이블)
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top: 32px;'>🏢 업체별 전체 성과 요약</div>", unsafe_allow_html=True)

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
            styled_df = styled_df.map(color_delta, subset=color_cols_standard)
        else:
            styled_df = styled_df.applymap(color_delta, subset=color_cols_standard)
            
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        csv_account_data = format_for_csv(df_display).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 업체별 전체 성과 요약 CSV 다운로드",
            data=csv_account_data,
            file_name=f"업체별_전체_성과_요약_{f['start']}_{f['end']}.csv",
            mime="text/csv",
            key="download_account_csv"
        )
    else:
        st.info("해당 및 비교 기간의 캠페인 데이터가 모두 없습니다.")


    # ==========================================
    # 3. 유형별 성과 요약
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top: 32px;'>🏷️ 유형별 성과 요약</div>", unsafe_allow_html=True)
    
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
            styled_type_df = styled_type_df.map(color_delta, subset=color_cols_standard)
        else:
            styled_type_df = styled_type_df.applymap(color_delta, subset=color_cols_standard)
            
        st.dataframe(styled_type_df, use_container_width=True, hide_index=True)
        
        csv_type_data = format_for_csv(df_type_display).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 유형별 성과 요약 CSV 다운로드",
            data=csv_type_data,
            file_name=f"유형별_성과_요약_{f['start']}_{f['end']}.csv",
            mime="text/csv",
            key="download_type_csv"
        )
    else:
        st.info("캠페인 유형 정보가 없어 유형별 요약을 제공할 수 없습니다.")

    st.markdown("<br>", unsafe_allow_html=True)


    # ==========================================
    # 4. 주간 성과 요약 (전체 + 유형별 탭)
    # ==========================================
    st.markdown("<div class='nv-sec-title'>📅 주간 성과 요약</div>", unsafe_allow_html=True)
    with st.spinner("주간 데이터 집계 중..."):
        base_weekly_ts = _cached_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        type_weekly_ts = _cached_type_timeseries(engine, f["start"], f["end"], cids, type_sel)
        
        if base_weekly_ts is not None and not base_weekly_ts.empty:
            tab_weekly_all, tab_weekly_type = st.tabs(["전체 합산", "유형별 상세"])
            
            def _get_week_info(dt_val):
                d = dt_val.date() if hasattr(dt_val, 'date') else dt_val
                start = d - timedelta(days=d.weekday())
                end = start + timedelta(days=6)
                thursday = start + timedelta(days=3)
                month = thursday.month
                week_num = (thursday.day - 1) // 7 + 1
                return f"{month}월 {week_num}주차 ({start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')})", start
            
            with tab_weekly_all:
                weekly_ts = base_weekly_ts.groupby('dt')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
                weekly_ts['dt'] = pd.to_datetime(weekly_ts['dt'])
                
                week_info = weekly_ts['dt'].apply(_get_week_info)
                weekly_ts['week_label'] = [x[0] for x in week_info]
                weekly_ts['week_start'] = [x[1] for x in week_info]
                
                weekly_grp = weekly_ts.groupby(['week_start', 'week_label'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
                weekly_grp = weekly_grp.sort_values('week_start', ascending=True)
                
                weekly_grp['ctr'] = np.where(weekly_grp['imp'] > 0, weekly_grp['clk'] / weekly_grp['imp'] * 100, 0)
                weekly_grp['cpc'] = np.where(weekly_grp['clk'] > 0, weekly_grp['cost'] / weekly_grp['clk'], 0)
                weekly_grp['roas'] = np.where(weekly_grp['cost'] > 0, weekly_grp['sales'] / weekly_grp['cost'] * 100, 0)
                
                weekly_display = weekly_grp[['week_label', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
                weekly_display.columns = ['주차', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']
                
                st.dataframe(
                    weekly_display.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                        '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    }),
                    use_container_width=True, hide_index=True
                )
                
                csv_weekly_all = format_for_csv(weekly_display).to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 주간 합산 성과 요약 CSV 다운로드",
                    data=csv_weekly_all,
                    file_name=f"주간_합산_성과_{f['start']}_{f['end']}.csv",
                    mime="text/csv",
                    key="download_weekly_all_csv"
                )
                
            with tab_weekly_type:
                if type_weekly_ts is not None and not type_weekly_ts.empty:
                    type_weekly_ts['dt'] = pd.to_datetime(type_weekly_ts['dt'])
                    week_info_t = type_weekly_ts['dt'].apply(_get_week_info)
                    type_weekly_ts['week_label'] = [x[0] for x in week_info_t]
                    type_weekly_ts['week_start'] = [x[1] for x in week_info_t]
                    
                    t_col = 'campaign_tp' if 'campaign_tp' in type_weekly_ts.columns else 'campaign_type'
                    
                    type_weekly_grp = type_weekly_ts.groupby(['week_start', 'week_label', t_col])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
                    type_weekly_grp = type_weekly_grp.sort_values(['week_start', 'cost'], ascending=[True, False])
                    
                    type_weekly_grp['ctr'] = np.where(type_weekly_grp['imp'] > 0, type_weekly_grp['clk'] / type_weekly_grp['imp'] * 100, 0)
                    type_weekly_grp['cpc'] = np.where(type_weekly_grp['clk'] > 0, type_weekly_grp['cost'] / type_weekly_grp['clk'], 0)
                    type_weekly_grp['roas'] = np.where(type_weekly_grp['cost'] > 0, type_weekly_grp['sales'] / type_weekly_grp['cost'] * 100, 0)
                    
                    type_weekly_grp['campaign_tp_kor'] = type_weekly_grp[t_col].apply(lambda x: type_kor_map.get(str(x).upper(), x))
                    
                    type_weekly_display = type_weekly_grp[['week_label', 'campaign_tp_kor', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
                    type_weekly_display.columns = ['주차', '캠페인 유형', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']
                    
                    st.dataframe(
                        type_weekly_display.style.format({
                            '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                            '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                            '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                        }),
                        use_container_width=True, hide_index=True
                    )
                    
                    csv_weekly_type = format_for_csv(type_weekly_display).to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 유형별 주간 성과 요약 CSV 다운로드",
                        data=csv_weekly_type,
                        file_name=f"유형별_주간_성과_{f['start']}_{f['end']}.csv",
                        mime="text/csv",
                        key="download_weekly_type_csv"
                    )
                else:
                    st.info("캠페인 유형 정보가 없어 유형별 주간 요약을 제공할 수 없습니다.")
        else:
            st.info("해당 기간의 주간 데이터가 없습니다.")

    st.markdown("<br><br>", unsafe_allow_html=True)
