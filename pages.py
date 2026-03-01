# -*- coding: utf-8 -*-
"""pages.py - Page routing and navigation."""

from __future__ import annotations
import streamlit as st
import pandas as pd
from typing import Dict, Any

from page_helpers import build_filters
from view_overview import page_overview
from view_budget import page_budget
from view_campaign import page_perf_campaign
from view_keyword import page_perf_keyword
from view_ad import page_perf_ad
from view_settings import page_settings
from view_trend import page_trend  # ✨ 신규 임포트

def render_sidebar_menu() -> str:
    st.sidebar.title("메뉴")
    pages = {
        "요약 (실시간 알림)": "overview",
        "예산 및 잔액": "budget",
        "시장 트렌드 분석": "trend", # ✨ 메뉴 추가
        "성과 (캠페인)": "perf_campaign",
        "성과 (그룹/키워드)": "perf_keyword",
        "성과 (소재/랜딩)": "perf_ad",
        "설정 및 연결": "settings"
    }
    
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "overview"

    # Shadcn UI (있을 경우)
    try:
        import streamlit_shadcn_ui as ui
        labels = list(pages.keys())
        current_label = next((k for k, v in pages.items() if v == st.session_state["current_page"]), labels[0])
        sel_label = ui.tabs(options=labels, default_value=current_label, key="main_nav_tabs")
        st.session_state["current_page"] = pages.get(sel_label, "overview")
    except Exception:
        # 일반 Radio 버튼 (Fallback)
        sel_label = st.sidebar.radio("이동할 메뉴를 선택하세요", list(pages.keys()), index=list(pages.values()).index(st.session_state["current_page"]))
        st.session_state["current_page"] = pages.get(sel_label, "overview")
        
    return st.session_state["current_page"]

def route_page(page: str, meta: pd.DataFrame, engine: Any, type_opts: list) -> None:
    if page == "settings":
        page_settings(meta, engine)
        return
        
    f = build_filters(meta, type_opts, engine)
    
    if page == "overview": page_overview(meta, engine, f)
    elif page == "budget": page_budget(meta, engine, f)
    elif page == "trend": page_trend(meta, engine, f) # ✨ 라우팅 추가
    elif page == "perf_campaign": page_perf_campaign(meta, engine, f)
    elif page == "perf_keyword": page_perf_keyword(meta, engine, f)
    elif page == "perf_ad": page_perf_ad(meta, engine, f)
    else: st.error("알 수 없는 페이지입니다.")
