# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
import streamlit as st

from data import *
from ui import render_hero
from page_helpers import BUILD_TAG, build_filters
from view_overview import page_overview
from view_budget import page_budget
from view_campaign import page_perf_campaign
from view_keyword import page_perf_keyword
from view_ad import page_perf_ad
from view_settings import page_settings
from view_media import page_media
from view_shopping_query import page_perf_shopping_query
from perf_utils import perf_enabled, perf_reset, perf_span, render_perf_panel, set_perf_enabled


def main():
    # 이전 선택 또는 env 기반으로 현재 렌더를 프로파일링한다.
    perf_reset("bootstrap")

    try:
        with perf_span("bootstrap.get_engine", kind="bootstrap"):
            engine = get_engine()
        with perf_span("bootstrap.get_latest_dates", kind="bootstrap"):
            latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None, BUILD_TAG)
        st.error(str(e))
        render_perf_panel(expanded=True)
        return

    try:
        with perf_span("bootstrap.logo", kind="bootstrap"):
            for ext in ['png', 'jpg', 'jpeg', 'webp']:
                if os.path.exists(f"logo.{ext}"):
                    st.logo(f"logo.{ext}")
                    break
    except Exception:
        pass

    with perf_span("bootstrap.render_hero", kind="bootstrap"):
        render_hero(latest, BUILD_TAG)
    with perf_span("bootstrap.get_meta", kind="bootstrap"):
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
        perf_toggle = st.checkbox("속도 진단 보기", value=bool(st.session_state.get("_perf_enabled", perf_enabled())), key="_perf_enabled_ui")
        set_perf_enabled(perf_toggle)

    # 토글 변경이 반영되도록 페이지 선택 확정 후 로그를 다시 초기화한다.
    perf_reset(nav)

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
            render_perf_panel(expanded=True)
            return
        with perf_span("filters.build", kind="ui"):
            type_opts = get_campaign_type_options(load_dim_campaign(engine))
            f = build_filters(meta, type_opts, engine)

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
        render_perf_panel(expanded=True)
        st.stop()

    if nav == "요약":
        with perf_span("page.render.overview", kind="page"):
            page_overview(meta, engine, f)
    elif nav == "예산 및 잔액":
        with perf_span("page.render.budget", kind="page"):
            page_budget(meta, engine, f)
    elif nav == "매체(지면) 분석":
        with perf_span("page.render.media", kind="page"):
            page_media(engine, f)
    elif nav == "성과 분석 · 캠페인":
        with perf_span("page.render.campaign", kind="page"):
            page_perf_campaign(meta, engine, f)
    elif nav == "성과 분석 · 키워드":
        with perf_span("page.render.keyword", kind="page"):
            page_perf_keyword(meta, engine, f)
    elif nav == "쇼핑 검색어 분석":
        with perf_span("page.render.shopping_query", kind="page"):
            page_perf_shopping_query(meta, engine, f)
    elif nav == "성과 분석 · 소재":
        with perf_span("page.render.ad", kind="page"):
            page_perf_ad(meta, engine, f)
    else:
        with perf_span("page.render.settings", kind="page"):
            page_settings(engine)

    render_perf_panel(expanded=False)


if __name__ == "__main__":
    main()
