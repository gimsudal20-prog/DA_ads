# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations
import os
import re
import io
import html
import math
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

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

from data import (
    format_currency, format_number_commas, format_roas,
    finalize_ctr_col, finalize_display_cols, _period_compare_range,
    get_entity_totals, _pct_change, _pct_to_str,
)

try: alt.data_transformers.enable("vegafusion")
except Exception: pass

def render_hero(latest_dates: dict | None, build_tag: str) -> None:
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    html_str = f"""
    <div style='background: linear-gradient(135deg, #0F172A 0%, #1E3A8A 100%); padding: 28px 32px; border-radius: 16px; color: #fff; display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1);'>
        <div>
            <h1 style='margin: 0; font-size: 26px; font-weight: 800; letter-spacing: -0.5px; display: flex; align-items: center; gap: 10px;'>
                🎯 퍼포먼스 마케팅 센터
            </h1>
            <p style='margin: 8px 0 0 0; color: #94A3B8; font-size: 14px; font-weight: 500;'>
                데이터 기준일: <span style='background: rgba(255,255,255,0.1); padding: 4px 8px; border-radius: 6px; color: #fff; font-weight: 700; margin-left: 4px;'>{dt_str}</span>
            </p>
        </div>
        <div style='text-align: right;'>
            <div style='background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2); padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; color: #E2E8F0;'>{build_tag}</div>
        </div>
    </div>
    """
    st.markdown(html_str, unsafe_allow_html=True)

# ✨ [UI 개선] Native Streamlit metric 대신 예쁘고 통일감 있는 자체 CSS 카드로 교체
def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    html = f"""
    <div style="background: white; padding: 20px; border-radius: 12px; border: 1px solid #E2E8F0; box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 16px;">
        <div style="color: #64748B; font-size: 13px; font-weight: 700; margin-bottom: 8px;">{title}</div>
        <div style="color: #0F172A; font-size: 24px; font-weight: 800; letter-spacing: -0.5px;">{value}</div>
        <div style="color: #3B82F6; font-size: 12px; font-weight: 600; margin-top: 6px; background: #EFF6FF; display: inline-block; padding: 2px 8px; border-radius: 4px;">{desc}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_big_table(df: pd.DataFrame, key: str, height: int = 400) -> None:
    if df is None or df.empty:
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

def render_echarts_dow_bar(ts_df: pd.DataFrame, height: int = 300):
    if ts_df.empty or "cost" not in ts_df.columns: return
    df = ts_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["dt"]): df["dt"] = pd.to_datetime(df["dt"])
    df["dow"] = df["dt"].dt.dayofweek
    dow_map = {0:"월", 1:"화", 2:"수", 3:"목", 4:"금", 5:"토", 6:"일"}
    
    grp = df.groupby("dow").agg({"cost": "sum", "sales": "sum", "conv": "sum"}).reset_index()
    grp["dow_str"] = grp["dow"].map(dow_map)
    grp["roas"] = np.where(grp["cost"] > 0, grp["sales"] / grp["cost"] * 100, 0)
    
    all_dows = pd.DataFrame({"dow": range(7), "dow_str": ["월","화","수","목","금","토","일"]})
    grp = pd.merge(all_dows, grp, on=["dow", "dow_str"], how="left").fillna(0).sort_values("dow")

    x_data = grp["dow_str"].tolist()
    roas_data = grp["roas"].round(0).tolist()
    cost_data = grp["cost"].tolist()

    options = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"data": ["광고비", "ROAS"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "top": "10%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data}],
        "yAxis": [
            {"type": "value", "name": "광고비", "splitLine": {"lineStyle": {"type": "dashed", "color": "#f3f4f6"}}},
            {"type": "value", "name": "ROAS(%)", "splitLine": {"show": False}}
        ],
        "series": [
            {"name": "광고비", "type": "bar", "data": cost_data, "itemStyle": {"color": "#94A3B8", "borderRadius": [4,4,0,0]}},
            {"name": "ROAS", "type": "line", "yAxisIndex": 1, "data": roas_data, "itemStyle": {"color": "#EF4444"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")

def generate_full_report_excel(overview_df: pd.DataFrame, camp_df: pd.DataFrame, kw_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if overview_df is not None and not overview_df.empty:
                overview_df.to_excel(writer, index=False, sheet_name="요약_현황")
            else:
                pd.DataFrame({"결과": ["데이터 없음"]}).to_excel(writer, index=False, sheet_name="요약_현황")
            if camp_df is not None and not camp_df.empty:
                camp_df.to_excel(writer, index=False, sheet_name="캠페인_현황")
            if kw_df is not None and not kw_df.empty:
                kw_df.to_excel(writer, index=False, sheet_name="파워링크_현황")
    except Exception as e:
        pd.DataFrame({"Error": [str(e)]}).to_excel(output, index=False, sheet_name="Error")
    return output.getvalue()
