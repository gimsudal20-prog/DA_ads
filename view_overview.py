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


def _build_periodic_report_text(report_type: str, campaign_type: str, imp: float, clk: float, ctr: float, cost: float, top_keywords: str) -> str:
    return "\n".join([
        report_type,
        f"- {campaign_type} (전체)",
        _format_report_line("노출수", f"{int(imp):,}"),
        _format_report_line("클릭수", f"{int(clk):,}"),
        _format_report_line("클릭률", f"{float(ctr):.2f}%"),
        _format_report_line("클릭이 많았던 키워드", top_keywords),
        _format_report_line("광고 소진비용", f"{int(cost):,}원"),
    ])

def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    
    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    camp_bndl = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=5000)
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1: account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1: account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"
    
    st.markdown(f"<div class='nv-sec-title'>📊 {account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:13px; font-weight:500; color:#474747; margin-bottom:12px;'>비교 기준: <span style='color:#375FFF; font-weight:700;'>{cmp_mode}</span></div>", unsafe_allow_html=True)

    cur = cur_summary
    base = base_summary

    def _delta_pct(key):
        try: return pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    def _kpi_html(label, value, delta_text, delta_val, highlight=False, improve_when_up=True):
        delta_num = float(delta_val) if delta_val is not None else 0.0
        is_neutral = abs(delta_num) < 5
        if is_neutral:
            cls_delta = "neu"
            delta_text = f"● 유지 ({delta_num:+.1f}%)"
        else:
            improved = delta_num > 0 if improve_when_up else delta_num < 0
            cls_delta = "pos" if improved else "neg"
            trend_label = "✓ 개선" if improved else "▲ 악화"
            delta_text = f"{trend_label} {pct_to_arrow(delta_num)}"
        cls_hl = " highlight" if highlight else ""
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    kpi_groups_html = f"""
    <style>
    .kpi-group {{
        background-color: rgba(55, 95, 255, 0.06) !important; 
        border: 1px solid rgba(55, 95, 255, 0.15) !important;
    }}
    .kpi-group-title {{
        color: #2b52ff !important;
    }}
    </style>
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
                {_kpi_html("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.2f}%", f"{pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("전환수", format_number_commas(cur.get("conv", 0.0)), f"{pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("전환매출", format_currency(cur.get("sales", 0.0)), f"{pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"))}
            </div>
        </div>
    </div>
    """
    st.markdown(kpi_groups_html, unsafe_allow_html=True)

    cost_delta = _delta_pct("cost") or 0.0
    conv_delta = _delta_pct("conv") or 0.0
    if abs(cost_delta) < 5 and abs(conv_delta) < 5:
        insight = "광고비/전환 모두 큰 변동이 없어 안정적으로 운영 중입니다."
    elif cost_delta > conv_delta + 10:
        insight = f"광고비 증감({cost_delta:+.1f}%) 대비 전환 증감({conv_delta:+.1f}%)이 낮아 효율 악화 가능성이 있습니다."
    elif conv_delta > cost_delta + 10:
        insight = f"전환 증감({conv_delta:+.1f}%)이 광고비 증감({cost_delta:+.1f}%)보다 커 효율 개선 흐름입니다."
    else:
        insight = f"광고비({cost_delta:+.1f}%)와 전환({conv_delta:+.1f}%)이 유사하게 움직이고 있습니다."
    st.info(f"🧭 KPI 해석: {insight}")

    top_keywords_text = "-"
    kw_bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=0)
    if kw_bundle is not None and not kw_bundle.empty and {"keyword", "clk"}.issubset(kw_bundle.columns):
        kw_top = kw_bundle.copy()
        kw_top["clk"] = pd.to_numeric(kw_top["clk"], errors="coerce").fillna(0)
        kw_top = kw_top.groupby("keyword", as_index=False)["clk"].sum().sort_values("clk", ascending=False).head(3)
        if not kw_top.empty:
            top_keywords_text = ", ".join([str(x).strip() for x in kw_top["keyword"].tolist() if str(x).strip()]) or "-"

    with st.expander("📝 주간/월간 보고서 내보내기", expanded=False):
        report_type = st.radio("보고서 타입", ["주간보고서", "월간보고서"], horizontal=True, key="overview_report_type")
        campaign_label = ", ".join(type_sel) if type_sel else "전체"
        report_text = _build_periodic_report_text(
            report_type=report_type,
            campaign_type=campaign_label,
            imp=float(cur.get("imp", 0.0) or 0.0),
            clk=float(cur.get("clk", 0.0) or 0.0),
            ctr=float(cur.get("ctr", 0.0) or 0.0),
            cost=float(cur.get("cost", 0.0) or 0.0),
            top_keywords=top_keywords_text,
        )
        st.code(report_text, language="text")
        st.download_button(
            "📥 요약 보고서 txt 내보내기",
            data=report_text,
            file_name=f"{report_type}_요약_{f['start']}_{f['end']}.txt",
            mime="text/plain",
            use_container_width=True,
        )
    
    alerts = []
    cur_roas = cur_summary.get('roas', 0)
    cur_cost = cur_summary.get('cost', 0)
    
    if cur_cost > 0 and cur_roas < 100: alerts.append(f"⚠️ 수익성 적자: 현재 조회 기간의 평균 ROAS가 {cur_roas:.2f}%로 낮습니다.")
    if base_summary.get('cost', 0) > 0:
        cost_surge = (cur_cost - base_summary['cost']) / base_summary['cost'] * 100
        if cost_surge >= 150: alerts.append(f"🔥 비용 폭증: 이전 기간 대비 전체 광고비 소진율이 {cost_surge:.0f}% 증가했습니다.")
    
    hippos = pd.DataFrame()
    if not camp_bndl.empty:
        hippos = camp_bndl[(camp_bndl['cost'] >= 50000) & (camp_bndl['conv'] == 0)].sort_values('cost', ascending=False)
        if not hippos.empty: alerts.append(f"💸 비용 누수: 비용 5만 원 이상 소진 중이나 전환이 없는 캠페인이 {len(hippos)}개 발견되었습니다.")

    if alerts:
        with st.expander(f"🚨 계정 내 점검이 필요한 {len(alerts)}건의 알림이 있습니다.", expanded=False):
            for a in alerts: st.markdown(f"- {a}")
            
            if not hippos.empty:
                disp_hippos = _perf_common_merge_meta(hippos, meta)
                disp_hippos = disp_hippos.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명", "cost": "광고비", "clk": "클릭수"})
                cols_to_show = [c for c in ["업체명", "캠페인명", "광고비", "클릭수"] if c in disp_hippos.columns]
                df_show = disp_hippos[cols_to_show].copy()
                for c in ["광고비", "클릭수"]:
                    if c in df_show.columns: df_show[c] = df_show[c].apply(lambda x: format_currency(x) if c == "광고비" else format_number_commas(x))
                
                st.markdown("<div style='margin-top: 16px; font-weight: 700; color: #FC503D; font-size: 14px;'>비용 누수 캠페인 목록</div>", unsafe_allow_html=True)
                st.dataframe(df_show, width="stretch", hide_index=True)
    else:
        st.success("✨ 모니터링 결과: 특이한 이상 징후나 비용 누수가 없습니다. 계정이 건강하게 운영되고 있습니다!")
    
    st.divider()

    try:
        trend_d1 = min(f["start"], date.today() - timedelta(days=7))
        ts = query_campaign_timeseries(engine, trend_d1, f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            st.markdown("<div class='nv-sec-title'>트렌드 및 요일별 효율 분석</div>", unsafe_allow_html=True)
            tab_trend, tab_dow = st.tabs(["전체 트렌드", "요일별 히트맵"])
            with tab_trend:
                ts["roas"] = np.where(
                    pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, 
                    round((pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0), 1), 
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
                trend_ts = ts.copy()
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
                ts_dow = ts.copy()
                ts_dow["요일"] = ts_dow["dt"].dt.day_name()
                dow_map = {'Monday': '월', 'Tuesday': '화', 'Wednesday': '수', 'Thursday': '목', 'Friday': '금', 'Saturday': '토', 'Sunday': '일'}
                ts_dow["요일"] = ts_dow["요일"].map(dow_map)
                
                dow_df = ts_dow.groupby("요일")[["cost", "conv", "sales"]].sum().reset_index()
                dow_df["ROAS(%)"] = np.where(dow_df["cost"] > 0, dow_df["sales"]/dow_df["cost"]*100, 0)
                
                cat_dtype = pd.CategoricalDtype(categories=['월', '화', '수', '목', '금', '토', '일'], ordered=True)
                dow_df["요일"] = dow_df["요일"].astype(cat_dtype)
                dow_df = dow_df.sort_values("요일")
                
                dow_disp = dow_df.rename(columns={"cost": "광고비", "conv": "전환수", "sales": "전환매출"})
                
                styled_df = dow_disp.style.background_gradient(cmap='Blues', subset=['광고비']).background_gradient(cmap='Purples', subset=['ROAS(%)']).format({
                    '광고비': '{:,.0f}', '전환수': '{:,.1f}', '전환매출': '{:,.0f}', 'ROAS(%)': '{:,.2f}%'
                })
                
                st.dataframe(styled_df, width="stretch", hide_index=True)
    except Exception as e:
        pass
