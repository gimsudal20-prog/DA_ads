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

# ▼ 이 부분이 누락되어서 발생한 에러입니다! ▼
from styles import apply_global_css

# ... (생략: 기존 import 문 및 예외 처리, 데이터 변환 로직 유지) ...

def _aggrid_grid_options(cols: List[str], pinned_rows: Optional[list] = None, right_cols: Optional[set] = None, quick_filter: str = "", enable_filter: bool = False, cond_thresholds: Optional[dict] = None) -> dict:
    # ... (생략: 기존 _aggrid_grid_options 함수 유지) ...
    return grid

def render_hero(latest: dict, build_tag: str = "") -> None:
    apply_global_css()
    latest = latest or {}
    def _dt(key_a: str, key_b: str) -> str:
        v = latest.get(key_a) or latest.get(key_b) or "—"
        if isinstance(v, (datetime, date)): v = v.strftime("%Y-%m-%d")
        return "—" if v is None else str(v).strip()

    st.markdown(
        f"""
        <div class="nv-topbar">
          <div class="inner">
            <div>
                <div class="nv-brand">
                    <span class="nv-dot"></span>네이버 검색광고 대시보드
                </div>
                <div class="nv-sub text-gray-500 mt-1">{build_tag}</div>
            </div>
            <div style="display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end;">
              <span class="nv-pill">캠페인 <b>{_dt("campaign_dt", "campaign")}</b></span>
              <span class="nv-pill">키워드 <b>{_dt("keyword_dt", "keyword")}</b></span>
              <span class="nv-pill">소재 <b>{_dt("ad_dt", "ad")}</b></span>
              <span class="nv-pill bg-blue-50 text-blue-600">잔액 <b>{_dt("bizmoney_dt", "bizmoney")}</b></span>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True
    )

def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    if os.getenv("USE_SHADCN_METRICS", "0").strip() == "1" and HAS_SHADCN_UI and ui is not None:
        try: ui.metric_card(title=title, content=value, description=desc, key=key); return
        except Exception: pass

    label = (desc or "").strip()
    delta_html = f"<div class='d neu'><span class='chip'>{label}</span></div>" if label else "<div class='d neu'></div>"

    # 상승/하락에 따른 색상 분기 (Tailwind 느낌의 뱃지)
    m = re.search(r"([+-])\s*([0-9]+(?:\.[0-9]+)?)\s*%", label)
    if m:
        sign, num = m.group(1), m.group(2)
        arrow, cls = ("▲", "pos") if sign == "+" else ("▼", "neg")
        label2 = (label.replace(m.group(0), "").replace("  ", " ").strip()) or ""
        chip = f"<span class='chip'>{label2}</span>" if label2 else ""
        delta_html = f"<div class='d {cls}'>{chip}{arrow} {num}%</div>"

    _formula = ""
    if "ROAS" in title: _formula = "ROAS = 전환매출 / 광고비 × 100"
    elif "CTR" in title: _formula = "CTR = 클릭 / 노출 × 100"
    elif "CPC" in title: _formula = "CPC = 광고비 / 클릭"
    elif "CPA" in title: _formula = "CPA = 광고비 / 전환"

    _tip = f"<span class='kpi-tip' title='{html.escape(_formula)}'>?</span>" if _formula else ""
    
    st.markdown(
        f"<div class='kpi' id='{key}'>"
        f"  <div class='k'>{html.escape(str(title))}{_tip}</div>"
        f"  <div class='v'>{value}</div>"
        f"  {delta_html}"
        f"</div>", 
        unsafe_allow_html=True
    )

def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    # ... (생략: 기존 ui_table_or_dataframe 함수 유지) ...
    pass

def render_echarts_dual_axis(title: str, ts: pd.DataFrame, x_col: str, bar_col: str, bar_name: str, line_col: str, line_name: str, *, height: int = 320) -> None:
    # ... (생략: 기존 차트 및 테이블 렌더링 함수들 모두 유지) ...
    pass

# ... (생략: 파일 끝부분의 기타 함수들 모두 유지) ...

def generate_full_report_excel(overview_df: pd.DataFrame, camp_df: pd.DataFrame, kw_df: pd.DataFrame) -> bytes:
    # ... (생략: 기존 엑셀 다운로드 함수 유지) ...
    return output.getvalue()
