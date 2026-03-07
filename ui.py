# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import base64
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

from styles import apply_global_css

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

from data import format_currency

def render_hero(latest_dates: dict | None, build_tag: str, dashboard_title: str = "마케팅 통합 대시보드") -> None:
    """
    메인 화면의 큰 공간을 차지하던 기존 Hero 섹션을 제거하고,
    핵심 정보(최근 수집일)만 좌측 사이드바 상단에 컴팩트하게 표출합니다.
    """
    # 1. 최근 수집일 날짜만 추출
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    # 2. 불필요한 환경/상태 KPI 카드는 모두 제거하고 좌측 메뉴 최상단에 삽입
    st.sidebar.markdown(f"""
    <div style='padding: 12px 14px; border-radius: 10px; background: #FFFFFF; border: 1px solid var(--nv-line); margin-bottom: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.02);'>
        <div style='font-size: 11px; color: var(--nv-muted); font-weight: 700; margin-bottom: 4px; display: flex; align-items: center; gap: 4px;'>
            <span>📊</span> {dashboard_title}
        </div>
        <div style='font-size: 14px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em;'>
            데이터 기준일: <span style='color: var(--nv-primary);'>{dt_str}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    html = f"""
    <div class="nv-metric-card">
        <div class="nv-metric-card-title">{title}</div>
        <div class="nv-metric-card-value">{value}</div>
        <div class="nv-metric-card-desc">{desc}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_big_table(df, key: str, height: int = 400) -> None:
    if df is None: return
    is_styler = hasattr(df, "data")
    check_df = df.data if is_styler else df
    if check_df.empty: return
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)

def render_budget_month_table_with_bars(df: pd.DataFrame, key: str, height: int = 400) -> None:
    if df is None or df.empty: return
    df_disp = df.copy()

    def _pbar(val):
        try: v = float(val) if pd.notna(val) else 0.0
        except Exception: v = 0.0
        w = min(v, 100)
        # 인디고 색상으로 통일
        c = "#6366F1"
        if v >= 100: c = "#EF4444"
        elif v >= 90: c = "#F59E0B"
        return f"<div class='nv-pbar'><div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{w}%; background:{c};'></div></div><div class='nv-pbar-txt'>{v:.1f}%</div></div>"

    if "집행률(%)" in df_disp.columns:
        df_disp["집행률 바"] = df_disp["집행률(%)"].apply(_pbar)

    html_rows = []
    cols = [c for c in df_disp.columns if c != "집행률(%)"]
    th_html = "".join(f"<th>{c}</th>" for c in cols)
    for _, row in df_disp.iterrows():
        tds = []
        for c in cols:
            val = row[c]
            if c == "상태":
                v_str = str(val)
                bg, text, icon = "#F1F5F9", "#475569", "●"
                if "적정" in v_str: bg, text, icon = "#D1FAE5", "#047857", "✓"
                elif "주의" in v_str: bg, text, icon = "#FEF3C7", "#B45309", "!"
                elif "초과" in v_str: bg, text, icon = "#FEE2E2", "#B91C1C", "▲"
                
                tds.append(f"<td><span style='background:{bg}; color:{text}; padding:5px 10px; border-radius:12px; font-weight:700; font-size:12px; letter-spacing:-0.3px;'>{icon} {v_str}</span></td>")
            else:
                tds.append(f"<td>{val}</td>")
        html_rows.append(f"<tr>{''.join(tds)}</tr>")

    table_html = f"<div style='height:{height}px; overflow-y:auto;'><table class='nv-table'><thead><tr>{th_html}</tr></thead><tbody>{''.join(html_rows)}</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)

def render_echarts_dual_axis(title: str, df: pd.DataFrame, x_col: str, y1_col: str, y1_name: str, y2_col: str, y2_name: str, height: int = 300):
    if df.empty: return
    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y1_data = df[y1_col].fillna(0).tolist()
    y2_data = df[y2_col].fillna(0).tolist()

    options = {
        "title": {"text": title, "textStyle": {"fontSize": 15, "color": "#0F172A", "fontWeight": 700}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [y1_name, y2_name], "bottom": 0},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}, "axisLine": {"lineStyle": {"color": "#E2E8F0"}}}],
        "yAxis": [
            {"type": "value", "name": y1_name, "splitLine": {"lineStyle": {"type": "solid", "color": "#F1F5F9"}}},
            {"type": "value", "name": y2_name, "splitLine": {"show": False}}
        ],
        "series": [
            # 인디고 색상으로 통일
            {"name": y1_name, "type": "bar", "data": y1_data, "itemStyle": {"color": "#6366F1", "borderRadius": [4,4,0,0]}}, 
            # 꺾은선 색상 슬레이트로 매칭
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "itemStyle": {"color": "#0F172A"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")


def render_echarts_single_axis(title: str, df: pd.DataFrame, x_col: str, y_col: str, y_name: str, height: int = 300):
    if df.empty: return
    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y_data = df[y_col].fillna(0).tolist()

    options = {
        "title": {"text": title, "textStyle": {"fontSize": 15, "color": "#0F172A", "fontWeight": 700}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "line"}},
        "legend": {"data": [y_name], "bottom": 0},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisLine": {"lineStyle": {"color": "#E2E8F0"}}}],
        "yAxis": [{"type": "value", "name": y_name, "splitLine": {"lineStyle": {"type": "solid", "color": "#F1F5F9"}}}],
        "series": [
            {"name": y_name, "type": "line", "data": y_data, "itemStyle": {"color": "#6366F1"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")
