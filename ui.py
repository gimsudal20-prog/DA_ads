import os
import re
import math
from datetime import date, datetime
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

# 옵셔널 라이브러리 임포트 (shadcn-ui, AgGrid, echarts)
# ... 기존 라이브러리 임포트 try-except 블록 복사 ...

BUILD_TAG = "v8.6.11 (Bootstrap Settings+Sync+Speed Hotfix, 2026-02-20)"

GLOBAL_UI_CSS = """
/* 여기에 기존의 200줄짜리 GLOBAL_UI_CSS 전체 복사 */
"""

# ---- Formatters ----
def _safe_int(x, default: int = 0) -> int:
    try:
        if pd.isna(x) or x == "": return default
        return int(float(x))
    except Exception: return default

def format_currency(val) -> str:
    return f"{_safe_int(val):,}원"

def format_number_commas(val) -> str:
    return f"{_safe_int(val):,}"

def format_roas(val) -> str:
    try:
        if pd.isna(val): return "-"
        return f"{float(val):.0f}%"
    except Exception: return "-"

# ---- UI Components ----
def render_hero(latest: dict, build_tag: str = BUILD_TAG) -> None:
    # 기존 render_hero 복사
    pass

def render_timeseries_chart(ts: pd.DataFrame, entity: str = "campaign", key_prefix: str = "") -> None:
    # 기존 render_timeseries_chart 복사
    pass

def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    # 기존 KPI 카드 복사
    pass

def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    # 기존 테이블 폴백 함수 복사
    pass

def render_budget_month_table_with_bars(table_df: pd.DataFrame, key: str, height: int = 520) -> None:
    # 기존 예산바 테이블 복사
    pass

def render_big_table(df: pd.DataFrame, key: str, height: int = 560) -> None:
    # 기존 AgGrid / st.dataframe 폴백 복사
    pass

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    # 다운로드 UI 및 변환기 복사 (csv, xlsx)
    pass

# 기타 ECharts 및 Altair 관련 _chart_* 및 render_echarts_* 함수들도 여기에 모두 위치시킵니다.