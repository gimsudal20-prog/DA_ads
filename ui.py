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
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    # 그림자 제거, 플랫한 타이포그래피 강조 헤더
    html_str = f"""
    <div style='padding: 24px 0 32px 0; display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid #19191A; margin-bottom: 32px;'>
        <div>
            <p style='margin: 0 0 8px 0; color: #375FFF; font-size: 13px; font-weight: 700; letter-spacing: 0.5px;'>OVERVIEW</p>
            <h1 style='margin: 0; font-size: 28px; font-weight: 800; letter-spacing: -0.5px; color: #19191A;'>
                {dashboard_title}
            </h1>
        </div>
        <div>
            <p style='margin: 0; color: #A0A0A0; font-size: 12px; font-weight: 500; text-align: right;'>
                최신 동기화 <br><span style='color: #19191A; font-weight: 600; font-size: 14px;'>{dt_str}</span>
            </p>
        </div>
    </div>
    """
    st.markdown(html_str, unsafe_allow_html=True)

def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    # 29CM 무드의 깔끔한 라인 메트릭 카드
    html = f"""
    <div style="background: #FFFFFF; padding: 20px; border-radius: 6px; border: 1px solid #E4E4E4; margin-bottom: 16px;">
        <div style="color: #474747; font-size: 13px; font-weight: 600; margin-bottom: 8px;">{title}</div>
        <div style="color: #19191A; font-size: 24px; font-weight: 700; letter-spacing: -0.5px;">{value}</div>
        <div style="color: #A0A0A0; font-size: 12px; font-weight: 500; margin-top: 8px;">{desc}</div>
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
        # 플랫한 블랙 바
        c = "#19191A" 
        if v >= 100: c = "#FC503D"
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
                color = "#3CD333" if "적정" in v_str else ("#FC503D" if "초과" in v_str else ("#F67514" if "주의" in v_str else "#A0A0A0"))
                tds.append(f"<td><span style='color:{color}; font-weight:700; font-size:13px;'>{v_str}</span></td>")
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
        "title": {"text": title, "textStyle": {"fontSize": 15, "color": "#19191A", "fontWeight": 700}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [y1_name, y2_name], "bottom": 0},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}, "axisLine": {"lineStyle": {"color": "#E4E4E4"}}}],
        "yAxis": [
            {"type": "value", "name": y1_name, "splitLine": {"lineStyle": {"type": "solid", "color": "#F4F4F4"}}},
            {"type": "value", "name": y2_name, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": y1_name, "type": "bar", "data": y1_data, "itemStyle": {"color": "#E4E4E4"}}, # 배경 같은 느낌의 막대
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "itemStyle": {"color": "#19191A"}, "lineStyle": {"width": 2}, "symbol": "circle", "symbolSize": 6}
        ]
    }
    st_echarts(options=options, height=f"{height}px")
