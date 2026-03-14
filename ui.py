# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import pandas as pd
import streamlit as st

from styles import apply_global_css

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

from data import format_currency

def render_hero(latest_dates: dict | None, build_tag: str, dashboard_title: str = "마케팅 통합 대시보드") -> None:
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    st.sidebar.markdown(f"""
    <div class='sidebar-info-box'>
        <div class='sidebar-info-label'>{dashboard_title}</div>
        <div class='sidebar-info-value'>최근 수집: <span>{dt_str}</span></div>
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
        c = "#34C9DA" # 메인 민트 컬러
        if v >= 100: c = "#F04438" # 초과 시 (빨강)
        elif v >= 90: c = "#F79009" # 주의 시 (주황)
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
                bg, text = "#F8F9FB", "#62686F"
                if "적정" in v_str: bg, text = "#DFF6F9", "#17B26A"
                elif "주의" in v_str: bg, text = "#FEF0C7", "#F79009"
                elif "초과" in v_str or "악화" in v_str: bg, text = "#FEE4E2", "#F04438"
                
                tds.append(f"<td><span style='background:{bg}; color:{text}; padding:4px 8px; border-radius:6px; font-weight:600; font-size:12px;'>{v_str}</span></td>")
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
        "title": {"text": title, "textStyle": {"fontSize": 14, "color": "#19191A", "fontWeight": 600}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [y1_name, y2_name], "bottom": 0},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}, "axisLine": {"lineStyle": {"color": "#DEE2E5"}}}],
        "yAxis": [
            {"type": "value", "name": y1_name, "splitLine": {"lineStyle": {"type": "solid", "color": "#F8F9FB"}}},
            {"type": "value", "name": y2_name, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": y1_name, "type": "bar", "data": y1_data, "itemStyle": {"color": "#DFF6F9", "borderRadius": [4,4,0,0]}}, 
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "itemStyle": {"color": "#34C9DA"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")

def render_echarts_single_axis(title: str, df: pd.DataFrame, x_col: str, y_col: str, y_name: str, height: int = 300):
    if df.empty: return
    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y_data = df[y_col].fillna(0).tolist()

    options = {
        "title": {"text": title, "textStyle": {"fontSize": 14, "color": "#19191A", "fontWeight": 600}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "line"}},
        "legend": {"data": [y_name], "bottom": 0},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisLine": {"lineStyle": {"color": "#DEE2E5"}}}],
        "yAxis": [{"type": "value", "name": y_name, "splitLine": {"lineStyle": {"type": "solid", "color": "#F8F9FB"}}}],
        "series": [
            {"name": y_name, "type": "line", "data": y_data, "itemStyle": {"color": "#34C9DA"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")
