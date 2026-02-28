# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

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

def main():
    try: engine = get_engine(); latest = get_latest_dates(engine)
    except Exception as e: render_hero(None, BUILD_TAG); st.error(str(e)); return

    render_hero(latest, BUILD_TAG)
    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("### 메뉴")
        if not meta_ready: st.warning("동기화가 필요합니다.")
        
        nav_items = ["요약(한눈에)", "예산/잔액", "캠페인", "키워드", "소재", "설정/연결"] if meta_ready else ["설정/연결"]
        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")

    st.markdown(f"<div class='nv-h1'>{nav}</div><div style='height:8px'></div>", unsafe_allow_html=True)
    f = None
    if nav != "설정/연결":
        if not meta_ready: st.error("설정 메뉴에서 동기화를 진행해주세요."); return
        f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    # 선택된 메뉴에 따라 각 파일에 분리된 뷰(화면)를 호출합니다.
    if nav == "요약(한눈에)": page_overview(meta, engine, f)
    elif nav == "예산/잔액": page_budget(meta, engine, f)
    elif nav == "캠페인": page_perf_campaign(meta, engine, f)
    elif nav == "키워드": page_perf_keyword(meta, engine, f)
    elif nav == "소재": page_perf_ad(meta, engine, f)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
