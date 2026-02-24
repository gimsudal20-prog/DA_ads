# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (리팩토링 버전)
"""
from __future__ import annotations

import streamlit as st

# 페이지 설정을 가장 먼저 해야 함
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

from db import get_engine
from ui import render_hero
from data import get_latest_dates, get_meta, load_dim_campaign, get_campaign_type_options
import pages

def main():
    try:
        engine = get_engine()
        latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None)
        st.error(str(e))
        return

    render_hero(latest)

    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("### 메뉴")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("처음 1회: accounts.xlsx 동기화가 필요합니다.")

        nav_items = ["요약(한눈에)", "예산/잔액", "캠페인", "키워드", "소재", "설정/연결"]
        if not meta_ready:
            nav_items = ["설정/연결"]
            st.session_state["nav_page"] = "설정/연결"

        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    st.markdown(f"<div class='nv-h1'>{nav}</div>", unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    f = None
    if nav != "설정/연결":
        if not meta_ready:
            st.error("설정 메뉴에서 accounts.xlsx 동기화를 진행해주세요.")
            return
        dim_campaign = load_dim_campaign(engine)
        type_opts = get_campaign_type_options(dim_campaign)
        f = pages.build_filters(meta, type_opts, engine)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # 라우팅 처리
    if nav == "요약(한눈에)":
        pages.page_overview(meta, engine, f)
    elif nav == "예산/잔액":
        pages.page_budget(meta, engine, f)
    elif nav == "캠페인":
        pages.page_perf_campaign(meta, engine, f)
    elif nav == "키워드":
        pages.page_perf_keyword(meta, engine, f)
    elif nav == "소재":
        pages.page_perf_ad(meta, engine, f)
    else:
        pages.page_settings(engine)

if __name__ == "__main__":
    main()