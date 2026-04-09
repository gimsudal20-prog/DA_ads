# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
import streamlit as st

from data import *
from ui import render_hero
from page_helpers import BUILD_TAG, build_filters
from perf_utils import reset_perf_events, timed_block, render_perf_panel

def main():
    reset_perf_events()
    try:
        with timed_block("bootstrap.engine", kind="page"):
            engine = get_engine()
        with timed_block("bootstrap.latest_dates", kind="page"):
            latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None, BUILD_TAG)
        st.error(str(e))
        return

    try:
        for ext in ['png', 'jpg', 'jpeg', 'webp']:
            if os.path.exists(f"logo.{ext}"):
                st.logo(f"logo.{ext}")
                break
    except Exception:
        pass

    render_hero(latest, BUILD_TAG)
    with timed_block("bootstrap.meta", kind="page"):
        meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("<div class='nav-sidebar-title'>Menu</div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("동기화가 필요합니다.")

        nav_items = [
            "요약",
            "예산 및 잔액",
            "매체(지면) 분석",
            "성과 분석 · 캠페인",
            "성과 분석 · 키워드",
            "성과 분석 · 소재",
            "쇼핑 검색어 분석",
            "설정 및 연결"
        ] if meta_ready else ["설정 및 연결"]

        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")
        st.checkbox("속도 진단 보기", key="_show_perf_diag", help="현재 페이지의 DB 조회/렌더 시간을 기록해서 하단에 표시합니다.")

    st.markdown(
        f"""
        <div class='nv-page-head'>
            <div class='nv-page-head-left'>
                <div class='nv-h1'>{nav}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    f = None
    if nav != "설정 및 연결":
        if not meta_ready:
            st.error("설정 메뉴에서 동기화를 진행해주세요.")
            return
        with timed_block("filters.build", kind="page"):
            f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    requires_selection_pages = {
        "요약",
        "예산 및 잔액",
        "매체(지면) 분석",
        "성과 분석 · 캠페인",
        "성과 분석 · 키워드",
        "성과 분석 · 소재",
        "쇼핑 검색어 분석",
    }

    if nav in requires_selection_pages and not (f.get("manager") or f.get("account")):
        st.info("담당자 또는 광고주(계정) 필터를 먼저 1개 이상 선택하면 데이터가 표시됩니다.")
        st.stop()

    if nav == "요약":
        with timed_block("render.overview", kind="page"):
            from view_overview import page_overview
            page_overview(meta, engine, f)
    elif nav == "예산 및 잔액":
        with timed_block("render.budget", kind="page"):
            from view_budget import page_budget
            page_budget(meta, engine, f)
    elif nav == "매체(지면) 분석":
        with timed_block("render.media", kind="page"):
            from view_media import page_media
            page_media(engine, f)
    elif nav == "성과 분석 · 캠페인":
        with timed_block("render.campaign", kind="page"):
            from view_campaign import page_perf_campaign
            page_perf_campaign(meta, engine, f)
    elif nav == "성과 분석 · 키워드":
        with timed_block("render.keyword", kind="page"):
            from view_keyword import page_perf_keyword
            page_perf_keyword(meta, engine, f)
    elif nav == "쇼핑 검색어 분석":
        with timed_block("render.shopping_query", kind="page"):
            from view_shopping_query import page_perf_shopping_query
            page_perf_shopping_query(meta, engine, f)
    elif nav == "성과 분석 · 소재":
        with timed_block("render.ad", kind="page"):
            from view_ad import page_perf_ad
            page_perf_ad(meta, engine, f)
    else:
        with timed_block("render.settings", kind="page"):
            from view_settings import page_settings
            page_settings(engine)

    render_perf_panel()

if __name__ == "__main__":
    main()
