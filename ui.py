# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import html
import pandas as pd
import streamlit as st

from styles import apply_global_css

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, GridUpdateMode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

from data import format_currency

# ==========================================
# 🎨 Theme Constants (STITCH Enterprise UI 로 업데이트 완료)
# ==========================================
THEME = {
    "primary": "#4f46e5",        # 세련된 인디고 블루 (메인)
    "primary_soft": "#e5eeff",   # 연한 블루 (차트 배경 등)
    "bg": "#f8f9ff",             # 대시보드 배경색
    "surface": "#ffffff",        # 카드 배경색 (순백색)
    "line": "#e2e8f0",           # 테두리 라인
    "text": "#0b1c30",           # 메인 텍스트 (블랙 대신 딥 네이비)
    "muted": "#565e74",          # 서브 텍스트 (슬레이트 그레이)
    "success": "#10b981",        # 긍정/최적 (에메랄드 그린)
    "warning": "#f59e0b",        # 주의/검토 (앰버 노랑)
    "warning_bg": "#fef3c7",     # 주의 배경
    "danger": "#ba1a1a",         # 에러/위험 (엔터프라이즈 레드)
    "danger_bg": "#ffdad6"       # 에러 배경
}

# ==========================================
# 🧩 UI Components
# ==========================================

def render_empty_state(message: str = "조회된 데이터가 없습니다.", height: int = 300) -> None:
    """데이터가 없을 때 표시하는 범용 Empty State 컴포넌트"""
    safe_msg = html.escape(message)
    empty_html = f"""
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: {height}px; width: 100%; background-color: {THEME['surface']}; border: 1px dashed {THEME['line']}; border-radius: 24px; color: {THEME['muted']}; text-align: center; box-shadow: 0px 10px 30px rgba(11, 28, 48, 0.03);">
        <div style="font-size: 32px; margin-bottom: 12px;">📭</div>
        <div style="font-size: 14px; font-weight: 700;">{safe_msg}</div>
        <div style="font-size: 12px; margin-top: 4px; opacity: 0.7;">조건을 변경하거나 동기화를 확인해주세요.</div>
    </div>
    """
    st.markdown(empty_html, unsafe_allow_html=True)

def render_hero(latest_dates: dict | None, build_tag: str, dashboard_title: str = "마케팅 통합 대시보드") -> None:
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    safe_title = html.escape(dashboard_title)
    safe_dt = html.escape(dt_str)

    st.sidebar.markdown(f"""
    <div class='sidebar-info-box'>
        <div class='sidebar-info-label'>{safe_title}</div>
        <div class='sidebar-info-value'>최근 수집: <span>{safe_dt}</span></div>
    </div>
    """, unsafe_allow_html=True)


def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    safe_title = html.escape(str(title))
    safe_value = html.escape(str(value))
    safe_desc = html.escape(str(desc))
    
    html_str = f"""
    <div class="nv-metric-card">
        <div class="nv-metric-card-title">{safe_title}</div>
        <div class="nv-metric-card-value">{safe_value}</div>
        <div class="nv-metric-card-desc">{safe_desc}</div>
    </div>
    """
    st.markdown(html_str, unsafe_allow_html=True)

def render_big_table(df, key: str, height: int = 400) -> None:
    """대용량 데이터를 위한 AgGrid 렌더러 (AgGrid 미설치 시 기본 dataframe으로 폴백)"""
    if df is None:
        render_empty_state("데이터 로드 실패", height)
        return
        
    is_styler = hasattr(df, "data")
    check_df = df.data if is_styler else df
    
    if check_df.empty:
        render_empty_state("조회된 데이터가 없습니다.", height)
        return

    if HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(check_df)
        gb.configure_default_column(resizable=True, filterable=True, sortable=True)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=15)
        gb.configure_selection('single')
        gridOptions = gb.build()
        
        AgGrid(
            check_df,
            gridOptions=gridOptions,
            height=height,
            width='100%',
            theme='alpine',
            update_mode=GridUpdateMode.NO_UPDATE,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
            allow_unsafe_jscode=True,
            key=f"aggrid_{key}"
        )
    else:
        try:
            st.dataframe(df, use_container_width=True, height=height, hide_index=True)
        except Exception:
            st.dataframe(check_df, use_container_width=True, height=height, hide_index=True)

def render_budget_month_table_with_bars(df: pd.DataFrame, key: str, height: int = 400) -> None:
    if df is None or df.empty:
        render_empty_state("예산 데이터가 없습니다.", height)
        return
        
    df_disp = df.copy()

    def _pbar(val):
        try: v = float(val) if pd.notna(val) else 0.0
        except Exception: v = 0.0
        w = min(v, 100)
        c = THEME['primary']
        if v >= 100: c = THEME['danger']
        elif v >= 90: c = THEME['warning']
        return f"<div class='nv-pbar' style='background-color:{THEME['bg']}; border-radius:999px; height:20px; width:100%; position:relative; overflow:hidden;'><div class='nv-pbar-fill' style='width:{w}%; background:{c}; height:100%; border-radius:999px;'></div><div class='nv-pbar-txt' style='position:absolute; width:100%; text-align:center; font-size:10px; font-weight:800; top:3px; color:#ffffff; mix-blend-mode:difference;'>{v:.1f}%</div></div>"

    if "집행률(%)" in df_disp.columns:
        df_disp["집행률 바"] = df_disp["집행률(%)"].apply(_pbar)

    html_rows = []
    cols = [c for c in df_disp.columns if c != "집행률(%)"]
    th_html = "".join(f"<th style='text-align:left; padding:12px 16px; border-bottom:1px solid {THEME['line']}; font-weight:800; font-size:12px; color:{THEME['muted']}; text-transform:uppercase;'>{html.escape(str(c))}</th>" for c in cols)
    
    for _, row in df_disp.iterrows():
        tds = []
        for c in cols:
            val = row[c]
            if c == "상태":
                v_str = html.escape(str(val))
                bg, text = THEME['surface'], THEME['muted']
                if "적정" in v_str: bg, text = THEME['success'] + "1A", THEME['success']
                elif "주의" in v_str: bg, text = THEME['warning_bg'], THEME['warning']
                elif "초과" in v_str or "악화" in v_str: bg, text = THEME['danger_bg'], THEME['danger']

                tds.append(f"<td style='padding:12px 16px; border-bottom:1px solid {THEME['line']}20;'><span style='background:{bg}; color:{text}; padding:4px 12px; border-radius:999px; font-weight:800; font-size:11px; text-transform:uppercase;'>{v_str}</span></td>")
            elif c == "집행률 바":
                tds.append(f"<td style='padding:12px 16px; border-bottom:1px solid {THEME['line']}20;'>{val}</td>")
            else:
                tds.append(f"<td style='padding:12px 16px; border-bottom:1px solid {THEME['line']}20; font-size:14px; font-weight:600; color:{THEME['text']};'>{html.escape(str(val))}</td>")
        html_rows.append(f"<tr style='transition:background-color 0.2s;' onmouseover='this.style.backgroundColor=\"{THEME['bg']}\"' onmouseout='this.style.backgroundColor=\"transparent\"'>{''.join(tds)}</tr>")

    table_html = f"<div style='height:{height}px; overflow-y:auto; background-color:{THEME['surface']}; border-radius:24px; box-shadow:0px 10px 30px rgba(11,28,48,0.03); border:1px solid {THEME['line']};'><table style='width:100%; border-collapse:collapse;'><thead><tr>{th_html}</tr></thead><tbody>{''.join(html_rows)}</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)

def render_echarts_dual_axis(title: str, df: pd.DataFrame, x_col: str, y1_col: str, y1_name: str, y2_col: str, y2_name: str, height: int = 300):
    if df.empty:
        render_empty_state("차트를 그릴 데이터가 부족합니다.", height)
        return
        
    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y1_data = df[y1_col].fillna(0).tolist()
    y2_data = df[y2_col].fillna(0).tolist()

    options = {
        "title": {"text": title, "textStyle": {"fontSize": 14, "color": THEME['text'], "fontWeight": 800}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [y1_name, y2_name], "bottom": 0, "textStyle": {"color": THEME['muted'], "fontWeight": 600}},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}, "axisLine": {"lineStyle": {"color": THEME['line']}}, "axisLabel": {"color": THEME['muted'], "fontWeight": 600}}],
        "yAxis": [
            {"type": "value", "name": y1_name, "splitLine": {"lineStyle": {"type": "dashed", "color": THEME['line']}}, "axisLabel": {"color": THEME['muted'], "fontWeight": 600}},
            {"type": "value", "name": y2_name, "splitLine": {"show": False}, "axisLabel": {"color": THEME['muted'], "fontWeight": 600}}
        ],
        "series": [
            {"name": y1_name, "type": "bar", "data": y1_data, "itemStyle": {"color": THEME['primary_soft'], "borderRadius": [8,8,0,0]}},
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "itemStyle": {"color": THEME['primary']}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")

def render_echarts_single_axis(title: str, df: pd.DataFrame, x_col: str, y_col: str, y_name: str, height: int = 300):
    if df.empty:
        render_empty_state("차트를 그릴 데이터가 부족합니다.", height)
        return
        
    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y_data = df[y_col].fillna(0).tolist()

    options = {
        "title": {"text": title, "textStyle": {"fontSize": 14, "color": THEME['text'], "fontWeight": 800}, "left": "left", "top": 0},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "line"}},
        "legend": {"data": [y_name], "bottom": 0, "textStyle": {"color": THEME['muted'], "fontWeight": 600}},
        "grid": {"left": "0%", "right": "0%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisLine": {"lineStyle": {"color": THEME['line']}}, "axisLabel": {"color": THEME['muted'], "fontWeight": 600}}],
        "yAxis": [{"type": "value", "name": y_name, "splitLine": {"lineStyle": {"type": "dashed", "color": THEME['line']}}, "axisLabel": {"color": THEME['muted'], "fontWeight": 600}}],
        "series": [
            {"name": y_name, "type": "line", "data": y_data, "itemStyle": {"color": THEME['primary']}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
        ]
    }
    st_echarts(options=options, height=f"{height}px")
