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
    report_loaded_key = f"overview_report_loaded_{state_hash}"
    alerts_loaded_key = f"overview_alerts_loaded_{state_hash}"
    trend_loaded_key = f"overview_trend_loaded_{state_hash}"

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
                
                weekly_disp = weekly_grp[['week_label', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
                weekly_disp.columns = ['주차', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']
                
                st.dataframe(
                    weekly_disp.style.format({
                        '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                        '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                        '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                    }),
                    use_container_width=True, hide_index=True
                )
                
                csv_weekly_all = format_for_csv(weekly_disp).to_csv(index=False).encode('utf-8-sig')
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
                    
                    type_weekly_disp = type_weekly_grp[['week_label', 'campaign_tp_kor', 'imp', 'clk', 'ctr', 'cost', 'cpc', 'conv', 'sales', 'roas']].copy()
                    type_weekly_disp.columns = ['주차', '캠페인 유형', '노출수', '클릭수', '클릭률(%)', '광고비', 'CPC', '전환수', '전환매출', 'ROAS(%)']
                    
                    st.dataframe(
                        type_weekly_disp.style.format({
                            '노출수': '{:,.0f}', '클릭수': '{:,.0f}', '클릭률(%)': '{:,.2f}%',
                            '광고비': '{:,.0f}원', 'CPC': '{:,.0f}원',
                            '전환수': '{:,.0f}', '전환매출': '{:,.0f}원', 'ROAS(%)': '{:,.0f}%'
                        }),
                        use_container_width=True, hide_index=True
                    )
                    
                    csv_weekly_type = format_for_csv(type_weekly_disp).to_csv(index=False).encode('utf-8-sig')
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

    st.markdown("<br>", unsafe_allow_html=True)


    # ==========================================
    # 5. 상세 분석 (특정 업체 선택 ➔ 캠페인 단위 분석)
    # ==========================================
    st.markdown("<div class='nv-sec-title'>🔍 업체별 캠페인 상세 분석</div>", unsafe_allow_html=True)
    
    if not merged.empty:
        selected_account = st.selectbox("상세 캠페인 성과를 확인할 업체를 선택하세요", options=merged['account_name'].tolist())

        if selected_account:
            selected_cid = merged[merged['account_name'] == selected_account]['customer_id'].iloc[0]
            
            cur_camp_sub = cur_camp[cur_camp['customer_id'].astype(str) == str(selected_cid)] if not cur_camp.empty else pd.DataFrame()
            base_camp_sub = base_camp[base_camp['customer_id'].astype(str) == str(selected_cid)] if not base_camp.empty else pd.DataFrame()
            
            has_rank = ('avg_rank' in cur_camp_sub.columns) or ('avg_rank' in base_camp_sub.columns)
            
            grp_cols = ['campaign_name']
            if type_col:
                grp_cols.insert(0, type_col)
                
            agg_cols = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
            if has_rank: 
                agg_cols['avg_rank'] = 'mean'
            
            base_camp_cols = grp_cols + ['imp', 'clk', 'cost', 'conv', 'sales'] + (['avg_rank'] if has_rank else [])
            
            cur_camp_grp = cur_camp_sub.groupby(grp_cols).agg(agg_cols).reset_index() if not cur_camp_sub.empty else pd.DataFrame(columns=base_camp_cols)
            base_camp_grp = base_camp_sub.groupby(grp_cols).agg(agg_cols).reset_index() if not base_camp_sub.empty else pd.DataFrame(columns=base_camp_cols)
                
            camp_merged = pd.merge(cur_camp_grp, base_camp_grp, on=grp_cols, how='outer', suffixes=('_cur', '_base')).fillna(0)
            camp_merged = camp_merged.sort_values('cost_cur', ascending=False).reset_index(drop=True)
            
            camp_table_data = []
            for rank, row in camp_merged.iterrows():
                c_imp, c_clk, c_cost, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['conv_cur'], row['sales_cur']
                b_imp, b_clk, b_cost, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
                
                c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
                b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
                
                pct_imp, diff_imp = calc_pct_diff(c_imp, b_imp)
                pct_clk, diff_clk = calc_pct_diff(c_clk, b_clk)
                pct_cost, diff_cost = calc_pct_diff(c_cost, b_cost)
                pct_conv, diff_conv = calc_pct_diff(c_conv, b_conv)
                pct_sales, diff_sales = calc_pct_diff(c_sales, b_sales)
                
                data_row = {"순위": rank + 1}
                
                if type_col:
                    raw_tp = str(row[type_col]).upper() if pd.notnull(row[type_col]) else ""
                    data_row["캠페인 유형"] = type_kor_map.get(raw_tp, raw_tp) if raw_tp else "알수없음"
                    
                data_row["캠페인명"] = row['campaign_name']
                
                if has_rank:
                    c_rank = row.get('avg_rank_cur', 0)
                    b_rank = row.get('avg_rank_base', 0)
                    data_row["평균순위"] = c_rank if c_rank > 0 else 0
                    
                    if b_rank > 0 and c_rank > 0:
                        data_row["순위 변화"] = c_rank - b_rank
                    else:
                        data_row["순위 변화"] = 0.0

                data_row.update({
                    "노출수": c_imp, "노출 증감": pct_imp, "노출 차이": diff_imp,
                    "클릭수": c_clk, "클릭 증감": pct_clk, "클릭 차이": diff_clk,
                    "광고비": c_cost, "광고비 증감": pct_cost, "광고비 차이": diff_cost,
                    "전환수": c_conv, "전환 증감": pct_conv, "전환 차이": diff_conv,
                    "전환매출": c_sales, "매출 증감": pct_sales, "매출 차이": diff_sales,
                    "ROAS": c_roas, "ROAS 증감": c_roas - b_roas
                })
                
                camp_table_data.append(data_row)
                
            camp_df_display = pd.DataFrame(camp_table_data)
            
            fmt_dict_camp = fmt_dict_standard.copy()
            color_cols_camp = color_cols_standard.copy()
            
            if has_rank:
                fmt_dict_camp["평균순위"] = "{:.0f}"
                fmt_dict_camp["순위 변화"] = "{:+.0f}"
                color_cols_camp.append("순위 변화")
                
            styled_camp_df = camp_df_display.style.format(fmt_dict_camp)
            
            if hasattr(styled_camp_df, 'map'):
                styled_camp_df = styled_camp_df.map(color_delta, subset=color_cols_camp)
            else:
                styled_camp_df = styled_camp_df.applymap(color_delta, subset=color_cols_camp)
                
            st.dataframe(styled_camp_df, use_container_width=True, hide_index=True)

            csv_camp_data = format_for_csv(camp_df_display).to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label=f"📥 {selected_account} 캠페인 상세 데이터 CSV 다운로드",
                data=csv_camp_data,
                file_name=f"{selected_account}_캠페인_상세_{f['start']}_{f['end']}.csv",
                mime="text/csv",
                key="download_camp_csv"
            )
    else:
        st.info("상세 분석을 위한 업체 데이터가 없습니다.")

    st.markdown("<br>", unsafe_allow_html=True)


    # ==========================================
    # 6. 기타 기능 (보고서 내보내기, 알림, 트렌드 분석)
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
            with st.spinner("키워드 집계 중..."):
                kw_bundle = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
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
        st.download_button(
            "📥 요약 보고서 txt 내보내기",
            data=report_text,
            file_name=f"요약보고서_{f['start']}_{f['end']}.txt",
            mime="text/plain",
            use_container_width=True,
        )

    with st.expander("🚨 계정 내 점검 알림", expanded=False):
        load_alerts = st.button("⚡ 알림 분석 불러오기", key=f"btn_load_alerts_{state_hash}", use_container_width=True)
        if load_alerts:
            st.session_state[alerts_loaded_key] = True

        if not st.session_state.get(alerts_loaded_key, False):
            st.caption("초기 로딩 속도를 위해 알림 분석은 필요할 때만 실행합니다.")
        else:
            with st.spinner("알림 항목 분석 중..."):
                camp_bndl = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)

            alerts = []
            cur_roas = cur_summary.get('roas', 0)
            cur_cost = cur_summary.get('cost', 0)

            if cur_cost > 0 and cur_roas < 100:
                alerts.append(f"⚠️ 수익성 적자: 현재 조회 기간의 평균 ROAS가 {cur_roas:.2f}%로 낮습니다.")
            if base_summary.get('cost', 0) > 0:
                cost_surge = (cur_cost - base_summary['cost']) / base_summary['cost'] * 100
                if cost_surge >= 150:
                    alerts.append(f"🔥 비용 폭증: 이전 기간 대비 전체 광고비 소진율이 {cost_surge:.0f}% 증가했습니다.")

            hippos = pd.DataFrame()
            if not camp_bndl.empty:
                hippos = camp_bndl[(camp_bndl['cost'] >= 50000) & (camp_bndl['conv'] == 0)].sort_values('cost', ascending=False)
                if not hippos.empty:
                    alerts.append(f"💸 비용 누수: 비용 5만 원 이상 소진 중이나 전환이 없는 캠페인이 {len(hippos)}개 발견되었습니다.")

            if alerts:
                for a in alerts:
                    st.markdown(f"- {a}")

                if not hippos.empty:
                    disp_hippos = _perf_common_merge_meta(hippos, meta)
                    disp_hippos = disp_hippos.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명", "cost": "광고비", "clk": "클릭수"})
                    cols_to_show = [c for c in ["업체명", "캠페인명", "광고비", "클릭수"] if c in disp_hippos.columns]
                    df_show = disp_hippos[cols_to_show].copy()
                    for c in ["광고비", "클릭수"]:
                        if c in df_show.columns:
                            df_show[c] = df_show[c].apply(lambda x: format_currency(x) if c == "광고비" else format_number_commas(x))

                    st.markdown("<div style='margin-top: 16px; font-weight: 700; color: #FC503D; font-size: 14px;'>비용 누수 캠페인 목록</div>", unsafe_allow_html=True)
                    st.dataframe(df_show, width="stretch", hide_index=True)
            else:
                st.success("✨ 모니터링 결과: 특이한 이상 징후나 비용 누수가 없습니다. 계정이 건강하게 운영되고 정기적인 점검이 완료되었습니다!")

    st.divider()

    st.markdown("<div class='nv-sec-title'>📈 트렌드 및 요일별 효율 분석</div>", unsafe_allow_html=True)
    with st.expander("차트 펼쳐보기", expanded=False):
        st.session_state[trend_loaded_key] = True

        trend_d1 = min(f["start"], date.today() - timedelta(days=7))
        with st.spinner("트렌드 데이터 집계 중..."):
            ts = _cached_trend_timeseries(engine, trend_d1, f["end"], cids, type_sel)

        if ts is not None and not ts.empty:
            if not meta.empty and 'customer_id' in ts.columns:
                meta_subset = meta[['customer_id', 'account_name']].copy()
                meta_subset['customer_id'] = meta_subset['customer_id'].astype(str)
                ts['customer_id'] = ts['customer_id'].astype(str)
                ts = ts.merge(meta_subset, on='customer_id', how='left')
                ts['account_name'] = ts['account_name'].fillna(ts['customer_id'])
            elif 'customer_id' in ts.columns:
                ts['account_name'] = ts['customer_id']
            else:
                ts['account_name'] = '전체 계정 (합산)'
                
            if 'campaign_tp' not in ts.columns:
                ts['campaign_tp'] = ts.get('campaign_type', '알수없음')
            ts['캠페인 유형'] = ts['campaign_tp'].str.upper().map(type_kor_map).fillna(ts['campaign_tp'])
            
            st.markdown("<div style='margin-top:8px; margin-bottom:16px;'>", unsafe_allow_html=True)
            
            unique_accounts = ts['account_name'].dropna().unique().tolist()
            if "전체 계정 (합산)" in unique_accounts: unique_accounts.remove("전체 계정 (합산)")
            account_options = ["전체 계정 (합산)"] + sorted(unique_accounts)
            
            unique_types = ts['캠페인 유형'].dropna().unique().tolist()
            if "전체 유형 (합산)" in unique_types: unique_types.remove("전체 유형 (합산)")
            type_options = ["전체 유형 (합산)"] + sorted(unique_types)
            
            col1, col2 = st.columns(2)
            with col1:
                selected_trend_account = st.selectbox("🏢 계정 선택", options=account_options, key="trend_account_selector")
            with col2:
                selected_trend_type = st.selectbox("🏷️ 캠페인 유형 선택", options=type_options, key="trend_type_selector")
                
            st.markdown("</div>", unsafe_allow_html=True)

            analysis_ts = ts.copy()
            if selected_trend_account != "전체 계정 (합산)":
                analysis_ts = analysis_ts[analysis_ts['account_name'] == selected_trend_account]
            if selected_trend_type != "전체 유형 (합산)":
                analysis_ts = analysis_ts[analysis_ts['캠페인 유형'] == selected_trend_type]
                
            analysis_ts = analysis_ts.groupby('dt')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()

            if analysis_ts.empty:
                st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
            else:
                tab_trend, tab_dow = st.tabs(["전체 트렌드", "요일별 히트맵"])
                with tab_trend:
                    trend_ts = analysis_ts.copy()
                    trend_ts["roas"] = np.where(
                        pd.to_numeric(trend_ts["cost"], errors="coerce").fillna(0) > 0,
                        round((pd.to_numeric(trend_ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(trend_ts["cost"], errors="coerce").fillna(0) * 100.0), 1),
                        0.0
                    )

                    trend_metric_options = {
                        "광고비 + ROAS": {"col": "cost", "label": "광고비(원)", "mode": "dual"},
                        "클릭수": {"col": "clk", "label": "클릭수", "mode": "single"},
                        "노출수": {"col": "imp", "label": "노출수", "mode": "single"},
                        "전환수": {"col": "conv", "label": "전환수", "mode": "single"},
                    }

                    selected_trend_metric = st.selectbox(
                        "전체트렌드 지표 선택",
                        list(trend_metric_options.keys()),
                        index=0,
                        key="overview_trend_metric_selector"
                    )
                    selected_cfg = trend_metric_options[selected_trend_metric]
                    trend_ts[selected_cfg["col"]] = pd.to_numeric(trend_ts[selected_cfg["col"]], errors="coerce").fillna(0)

                    if selected_cfg["mode"] == "dual":
                        if HAS_ECHARTS:
                            render_echarts_dual_axis("일자별 광고비 및 ROAS", trend_ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                        else:
                            st.line_chart(trend_ts.set_index("dt")[["cost", "roas"]], height=320)
                    else:
                        chart_title = f"일자별 {selected_cfg['label']}"
                        if HAS_ECHARTS:
                            render_echarts_single_axis(chart_title, trend_ts, "dt", selected_cfg["col"], selected_cfg["label"], height=320)
                        else:
                            st.line_chart(trend_ts.set_index("dt")[[selected_cfg["col"]]], height=320)

                with tab_dow:
                    ts_dow = analysis_ts.copy()
                    ts_dow["요일"] = ts_dow["dt"].dt.day_name()
                    dow_map = {'Monday': '월', 'Tuesday': '화', 'Wednesday': '수', 'Thursday': '목', 'Friday': '금', 'Saturday': '토', 'Sunday': '일'}
                    ts_dow["요일"] = ts_dow["요일"].map(dow_map)

                    dow_df = ts_dow.groupby("요일")[["cost", "conv", "sales"]].sum().reset_index()
                    dow_df["ROAS(%)"] = np.where(dow_df["cost"] > 0, dow_df["sales"] / dow_df["cost"] * 100, 0)

                    cat_dtype = pd.CategoricalDtype(categories=['월', '화', '수', '목', '금', '토', '일'], ordered=True)
                    dow_df["요일"] = dow_df["요일"].astype(cat_dtype)
                    dow_df = dow_df.sort_values("요일")

                    dow_disp = dow_df.rename(columns={"cost": "광고비", "conv": "전환수", "sales": "전환매출"})

                    styled_df = dow_disp.style.background_gradient(cmap='Blues', subset=['광고비']).background_gradient(cmap='Purples', subset=['ROAS(%)']).format({
                        '광고비': '{:,.0f}', '전환수': '{:,.0f}', '전환매출': '{:,.0f}', 'ROAS(%)': '{:,.0f}%'
                    })

                    st.dataframe(styled_df, width="stretch", hide_index=True)
        else:
            st.info("선택한 조건에 대한 트렌드 데이터가 없습니다.")
