# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import re
import io
import html
import math
import base64
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import altair as alt

from styles import apply_global_css

# Optional UI components
try:
    import streamlit_shadcn_ui as ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None
    HAS_SHADCN_UI = False

# Optional ECharts
try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

from data import (
    format_currency,
    format_number_commas,
    format_roas,
    finalize_ctr_col,
    finalize_display_cols,
    _period_compare_range,
    get_entity_totals,
    _pct_change,
    _pct_to_str,
)

try: alt.data_transformers.enable("vegafusion")
except Exception: pass

def render_hero(latest_dates: dict | None, build_tag: str) -> None:
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    logo_html = "<span style='font-size: 32px;'>🏢</span>"
    for ext in ['png', 'jpg', 'jpeg', 'webp']:
        path = f"logo.{ext}"
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                mime = "image/jpeg" if ext in ['jpg', 'jpeg'] else f"image/{ext}"
                logo_html = f"<img src='data:{mime};base64,{encoded}' style='max-height: 46px; object-fit: contain;' />"
                break
            except Exception:
                pass

    html_str = f"""
    <div style='background: #FFFFFF; border: 1px solid #E2E8F0; padding: 20px 32px; border-radius: 16px; display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);'>
        <div style='display: flex; align-items: center; gap: 20px;'>
            <div>{logo_html}</div>
            <div style='border-left: 2px solid #F1F5F9; padding-left: 20px;'>
                <h1 style='margin: 0; font-size: 22px; font-weight: 800; letter-spacing: -0.5px; color: #0F172A;'>
                    마케팅 대시보드
                </h1>
                <p style='margin: 6px 0 0 0; color: #64748B; font-size: 13.5px; font-weight: 500;'>
                    최신 데이터 기준일: <span style='color: #2563EB; font-weight: 700;'>{dt_str}</span>
                </p>
            </div>
        </div>
    </div>
    """
    st.markdown(html_str, unsafe_allow_html=True)

def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    html = f"""
    <div style="background: white; padding: 20px; border-radius: 12px; border: 1px solid #E2E8F0; box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 16px;">
        <div style="color: #64748B; font-size: 13px; font-weight: 700; margin-bottom: 8px;">{title}</div>
        <div style="color: #0F172A; font-size: 24px; font-weight: 800; letter-spacing: -0.5px;">{value}</div>
        <div style="color: #3B82F6; font-size: 12px; font-weight: 600; margin-top: 6px; background: #EFF6FF; display: inline-block; padding: 2px 8px; border-radius: 4px;">{desc}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# ✨ [NEW] Styler 포맷(소수점 유지)을 지원하도록 테이블 렌더링 함수 수정
def render_big_table(df, key: str, height: int = 400) -> None:
    if df is None:
        st.info("데이터가 없습니다.")
        return
    is_styler = hasattr(df, "data")
    check_df = df.data if is_styler else df
    if check_df.empty:
        st.info("데이터가 없습니다.")
        return
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)

def render_budget_month_table_with_bars(df: pd.DataFrame, key: str, height: int = 400) -> None:
    if df is None or df.empty:
        st.info("데이터가 없습니다.")
        return
    df_disp = df.copy()

    def _pbar(val):
        try: v = float(val) if pd.notna(val) else 0.0
        except Exception: v = 0.0
        w = min(v, 100)
        c = "var(--nv-green)"
        if v >= 100: c = "var(--nv-red)"
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
                color = "#10B981" if "적정" in v_str else ("#EF4444" if "초과" in v_str else ("#F59E0B" if "주의" in v_str else "#9CA3AF"))
                tds.append(f"<td><span style='background:{color}15; color:{color}; padding:4px 8px; border-radius:6px; font-weight:700; font-size:13px;'>{v_str}</span></td>")
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
        "title": {"text": title, "textStyle": {"fontSize": 15, "color": "#1f2937"}, "left": "center", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [y1_name, y2_name], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": y1_name, "splitLine": {"lineStyle": {"type": "dashed", "color": "#f3f4f6"}}},
            {"type": "value", "name": y2_name, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": y1_name, "type": "bar", "data": y1_data, "itemStyle": {"color": "#3B82F6", "borderRadius": [4,4,0,0]}},
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "itemStyle": {"color": "#10B981"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")
