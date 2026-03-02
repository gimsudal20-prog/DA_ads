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

@st.cache_data
def convert_df_to_csv(df):
    """데이터프레임을 CSV로 변환하는 캐시 함수 (다운로드용)"""
    return df.to_csv(index=False).encode('utf-8-sig')

def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    
    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    camp_bndl = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=5000)
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    # ✨ [NEW] 1 & 2: 컨텍스트 인식 - 현재 보고 있는 계정(업체) 이름 가져오기
    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"
    
    st.markdown(f"<div class='nv-sec-title'>📊 {account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    st.caption("가장 중요한 핵심 성과(KPI)를 직관적으로 확인하세요.")
    st.markdown(f"<div style='font-size:13px; font-weight:600; color:#475569; margin-bottom:12px;'>🔄 자동 비교 기준: <span style='color:#2563EB;'>{cmp_mode}</span></div>", unsafe_allow_html=True)

    cur = cur_summary
    base = base_summary

    def _delta_pct(key):
        try: return pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    def _kpi_html(label, value, delta_text, delta_val, highlight=False):
        cls_delta = "pos" if delta_val and float(delta_val) > 0 else ("neg" if delta_val and float(delta_val) < 0 else "neu")
        cls_hl = " highlight" if highlight else ""
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    # ✨ [NEW] 4: KPI 지표의 논리적 그룹화 (유입/비용/성과)
    kpi_groups_html = f"""
    <div class='kpi-group-container'>
        <div class='kpi-group'>
            <div class='kpi-group-title'>👀 유입 지표</div>
            <div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>💸 비용 지표</div>
            <div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>🎯 성과 지표</div>
            <div class='kpi-row'>
                {_kpi_html("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.2f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("전환수", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"))}
            </div>
        </div>
    </div>
    """
    st.markdown(kpi_groups_html, unsafe_allow_html=True)
    
    # ---------------------------------------------------------
    # ✨ [NEW] 3: 알림 피로도 감소 및 액션 유도 (배지 및 진단 리포트 묶음)
    # ---------------------------------------------------------
    alerts = []
    cur_roas = cur_summary.get('roas', 0)
    cur_cost = cur_summary.get('cost', 0)
    
    if cur_cost > 0 and cur_roas < 100:
        alerts.append(f"⚠️ **수익성 적자 경고:** 현재 조회 기간의 평균 ROAS가 **{cur_roas:.2f}%**로 낮습니다.")
        
    if base_summary.get('cost', 0) > 0:
        cost_surge = (cur_cost - base_summary['cost']) / base_summary['cost'] * 100
        if cost_surge >= 150:
            alerts.append(f"🔥 **비용 폭증 알림:** 이전 기간 대비 전체 광고비 소진율이 **{cost_surge:.0f}% 폭증**했습니다.")
    
    hippos = pd.DataFrame()
    if not camp_bndl.empty:
        hippos = camp_bndl[(camp_bndl['cost'] >= 50000) & (camp_bndl['conv'] == 0)].sort_values('cost', ascending=False)
        if not hippos.empty:
            alerts.append(f"💸 **비용 누수 경고:** 비용 5만 원 이상 소진 중이나 전환이 없는 캠페인이 **{len(hippos)}개** 발견되었습니다.")

    st.markdown("<div class='nv-sec-title'>🚨 실시간 AI 진단 리포트</div>", unsafe_allow_html=True)
    if alerts:
        # 경고 요약 배지
        st.error(f"⚠️ **주의 필요!** 계정 내 점검이 필요한 **{len(alerts)}건**의 중요 알림이 있습니다.")
        
        # 상세 내용은 Expander로 감추어 인지 부하 감소
        with st.expander("상세 진단 리포트 및 조치하기 열기", expanded=True):
            for a in alerts:
                st.markdown(f"- {a}")
            
            # 액션 유도 버튼 (네이버 광고 시스템으로 이동)
            st.markdown("<br><a href='https://searchad.naver.com/' target='_blank' style='display:inline-block; padding:8px 16px; background:#2563EB; color:#fff; text-decoration:none; border-radius:6px; font-weight:600; font-size:13px;'>🔗 네이버 광고시스템에서 수정하기</a>", unsafe_allow_html=True)
            
            if not hippos.empty:
                disp_hippos = _perf_common_merge_meta(hippos, meta)
                disp_hippos = disp_hippos.rename(columns={
                    "account_name": "업체명", "campaign_name": "캠페인명", "cost": "광고비", "clk": "클릭수"
                })
                cols_to_show = [c for c in ["업체명", "캠페인명", "광고비", "클릭수"] if c in disp_hippos.columns]
                df_show = disp_hippos[cols_to_show].copy()
                for c in ["광고비", "클릭수"]:
                    if c in df_show.columns:
                        df_show[c] = df_show[c].apply(lambda x: format_currency(x) if c == "광고비" else format_number_commas(x))
                
                st.markdown("<div style='margin-top: 20px; font-weight: 700; color: #B91C1C;'>🚨 비용 누수 캠페인 목록</div>", unsafe_allow_html=True)
                st.dataframe(df_show, use_container_width=True, hide_index=True)
    else:
        st.success("✨ 모니터링 결과: 특이한 이상 징후나 비용 누수가 없습니다. 계정이 매우 건강하게 운영되고 있습니다!")
    
    st.divider()

    try:
        trend_d1 = min(f["start"], date.today() - timedelta(days=7))
        ts = query_campaign_timeseries(engine, trend_d1, f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            st.markdown("### 📅 트렌드 및 요일별 효율 분석")
            tab_trend, tab_dow = st.tabs(["📉 전체 트렌드 차트", "🌡️ 요일별 성과 히트맵"])
            with tab_trend:
                ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
                if HAS_ECHARTS: render_echarts_dual_axis("전체 트렌드", ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                
                # ✨ [NEW] 5: 보고서 작성을 위한 다운로드 버튼 추가
                st.download_button(
                    label="📥 트렌드 데이터 CSV 다운로드",
                    data=convert_df_to_csv(ts),
                    file_name=f'{account_name}_trend_data.csv',
                    mime='text/csv',
                    key='dl_trend'
                )

            with tab_dow:
                st.caption("💡 **활용법:** 붉은색이 진할수록 광고비 지출이 많고, 녹색이 진할수록 수익성(ROAS)이 좋습니다. 녹색이 진한 요일의 예산을 늘려보세요.")
                
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
                
                # 우측 상단 배치 느낌으로 다운로드 버튼
                col1, col2 = st.columns([8, 2])
                with col2:
                    st.download_button(
                        label="📥 히트맵 CSV 다운로드",
                        data=convert_df_to_csv(dow_disp),
                        file_name=f'{account_name}_dow_data.csv',
                        mime='text/csv',
                        key='dl_dow'
                    )

                styled_df = dow_disp.style.background_gradient(cmap='Reds', subset=['광고비']).background_gradient(cmap='Greens', subset=['ROAS(%)']).format({
                    '광고비': '{:,.0f}', '전환수': '{:,.1f}', '전환매출': '{:,.0f}', 'ROAS(%)': '{:,.2f}%'
                })
                
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.info(f"추세 데이터를 불러오는 중 오류가 발생했습니다: {e}")
