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
from view_trend import page_trend
from view_media import page_media  # ✨ 신규 임포트: 매체(지면) 분석 추가

def main():
    try: engine = get_engine(); latest = get_latest_dates(engine)
    except Exception as e: render_hero(None, BUILD_TAG); st.error(str(e)); return

    try:
        for ext in ['png', 'jpg', 'jpeg', 'webp']:
            if os.path.exists(f"logo.{ext}"):
                st.logo(f"logo.{ext}")
                break
    except Exception:
        pass

    render_hero(latest, BUILD_TAG)
    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("""
        <div class='nav-sidebar-title'>📌 메뉴 이동</div>
        <div class='nav-sidebar-caption'>보고서 섹션을 선택하세요</div>
        """, unsafe_allow_html=True)
        if not meta_ready: st.warning("동기화가 필요합니다.")

        nav_items = [
            "📋 요약",
            "💳 예산 및 잔액",
            "🧭 시장/매체 통합 분석",
            "📈 성과 분석 · 캠페인",
            "🔍 성과 분석 · 키워드",
            "🎨 성과 분석 · 소재",
            "⚙️ 설정 및 연결"
        ] if meta_ready else ["⚙️ 설정 및 연결"]

        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")

    st.markdown(f"<div class='nv-h1'>{nav}</div><div style='height:8px'></div>", unsafe_allow_html=True)
    f = None
    if nav != "⚙️ 설정 및 연결":
        if not meta_ready: st.error("설정 메뉴에서 동기화를 진행해주세요."); return
        f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    # ✨ [라우팅 연결] 메뉴 선택 시 해당 화면으로 이동하도록 연결
    if nav == "📋 요약":
        page_overview(meta, engine, f)
    elif nav == "💳 예산 및 잔액":
        page_budget(meta, engine, f)
    elif nav == "🧭 시장/매체 통합 분석":
        tab_trend, tab_media = st.tabs(["시장 트렌드", "매체(지면) 분석"])
        with tab_trend:
            page_trend(meta, engine, f)
        with tab_media:
            page_media(engine, f)
    elif nav == "📈 성과 분석 · 캠페인":
        page_perf_campaign(meta, engine, f)
    elif nav in ["🔍 성과 분석 · 키워드", "🎨 성과 분석 · 소재"]:
        if not (f.get("manager") or f.get("account")):
            st.info("담당자 또는 광고주(계정) 필터를 먼저 1개 이상 선택하면 데이터가 표시됩니다.")
            st.stop()
        if nav == "🔍 성과 분석 · 키워드":
            page_perf_keyword(meta, engine, f)
        else:
            page_perf_ad(meta, engine, f)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
