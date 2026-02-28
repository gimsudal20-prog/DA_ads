# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date

from data import *
from ui import *
from page_helpers import *

def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("<div class='nv-sec-title'>요약 및 인사이트</div>", unsafe_allow_html=True)
        st.caption(f"조회 기간: {f['start']} ~ {f['end']}")
    with col2:
        cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
        with st.spinner("보고서 생성 중..."):
            cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
            df_summary = pd.DataFrame([cur_summary])
            camp_bndl = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=50)
            camp_df = _perf_common_merge_meta(add_rates(camp_bndl), meta) if not camp_bndl.empty else pd.DataFrame()
            kw_bndl = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=200)
            kw_df = _perf_common_merge_meta(add_rates(kw_bndl), meta) if not kw_bndl.empty else pd.DataFrame()
            df_pl_kw = kw_df[kw_df['campaign_type_label'] == '파워링크'] if not kw_df.empty and 'campaign_type_label' in kw_df.columns else pd.DataFrame()

            excel_data = generate_full_report_excel(df_summary, camp_df, df_pl_kw)
            st.download_button(label="📥 보고서(Excel) 다운로드", data=excel_data, file_name=f"광고보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")

    st.markdown("<div class='nv-sec-title'>📊 종합 성과 요약</div>", unsafe_allow_html=True)
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_opts = [o for o in opts if o != "비교 안함"]
    if not cmp_opts: cmp_opts = ["이전 같은 기간 대비"]
    
    pm = f.get("period_mode", "어제")
    cmp_mode = st.radio("비교 기준 선택", cmp_opts, horizontal=True, key=f"ov_cmp_mode_{pm}", label_visibility="collapsed")
    cur = cur_summary
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    base = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    def _delta_pct(key):
        try: return _pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    def _kpi_html(label, value, delta_text, delta_val):
        cls = "pos" if delta_val and float(delta_val) > 0 else ("neg" if delta_val and float(delta_val) < 0 else "neu")
        return f"<div class='kpi'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls}'>{delta_text}</div></div>"

    items = [
        ("노출수", format_number_commas(cur.get("imp", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp")),
        ("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk")),
        ("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost")),
        ("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales")),
        ("전환수", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv")),
        ("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas")),
        ("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr")),
        ("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc")),
    ]
    st.markdown("<div class='kpi-row' style='margin-top: 5px;'>" + "".join(_kpi_html(*i) for i in items) + "</div>", unsafe_allow_html=True)
    st.divider()

    st.markdown("<div class='nv-sec-title'>💡 주요 최적화 포인트 (파워링크)</div>", unsafe_allow_html=True)
    if not df_pl_kw.empty:
        df_pl_kw_fmt = df_pl_kw.copy()
        df_pl_kw_fmt["광고비"] = pd.to_numeric(df_pl_kw_fmt["cost"], errors="coerce").fillna(0)
        df_pl_kw_fmt["전환"] = pd.to_numeric(df_pl_kw_fmt["conv"], errors="coerce").fillna(0)
        df_pl_kw_fmt["ROAS(%)"] = pd.to_numeric(df_pl_kw_fmt["roas"], errors="coerce").fillna(0)
        render_insight_cards(df_pl_kw_fmt, "키워드", "keyword")
    else:
        st.info("파워링크 데이터가 수집되지 않아 분석할 수 없습니다.")
        
    st.divider()

    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            st.markdown("### 📅 트렌드 및 요일별 효율 분석")
            tab_trend, tab_dow = st.tabs(["전체 트렌드", "요일별 분석"])
            with tab_trend:
                ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
                if HAS_ECHARTS: render_echarts_dual_axis("전체 트렌드", ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
            with tab_dow:
                st.caption("💡 주말(토/일)과 평일의 효율(ROAS) 차이를 확인하고 요일별 입찰 가중치를 조절하세요.")
                if HAS_ECHARTS: render_echarts_dow_bar(ts, height=320)
    except Exception as e:
        st.info(f"추세 데이터를 불러오는 중 오류가 발생했습니다: {e}")
