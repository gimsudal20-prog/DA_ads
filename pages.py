# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
import streamlit as st

try:
    import streamlit_antd_components as sac
    HAS_SAC = True
except Exception:
    sac = None
    HAS_SAC = False

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
from view_media import page_media
from view_shopping_query import page_perf_shopping_query


def _render_sidebar_nav(nav_items: list[str]) -> str:
    default_nav = st.session_state.get("nav_page", nav_items[0] if nav_items else "")
    if default_nav not in nav_items and nav_items:
        default_nav = nav_items[0]

    if HAS_SAC and nav_items:
        try:
            icon_map = {
                "요약": "house",
                "예산 및 잔액": "wallet2",
                "시장 및 매체 분석": "bar-chart",
                "성과 분석 · 캠페인": "bullseye",
                "성과 분석 · 키워드": "search",
                "성과 분석 · 소재": "image",
                "쇼핑 검색어 분석": "cart",
                "설정 및 연결": "gear",
            }
            items = [sac.MenuItem(label, icon=icon_map.get(label, "app")) for label in nav_items]
            picked = sac.menu(
                items=items,
                index=nav_items.index(default_nav),
                key="nav_page_sac",
                open_all=True,
                indent=14,
            )
            if picked in nav_items:
                st.session_state["nav_page"] = picked
                return picked
        except Exception:
            pass

    picked = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")
    st.session_state["nav_page"] = picked
    return picked


def main():
    try:
        engine = get_engine()
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
    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("<div class='nav-sidebar-title'>Menu</div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("동기화가 필요합니다.")

        nav_items = [
            "요약",
            "예산 및 잔액",
            "시장 및 매체 분석",
            "성과 분석 · 캠페인",
            "성과 분석 · 키워드",
            "성과 분석 · 소재",
            "쇼핑 검색어 분석",
            "설정 및 연결"
        ] if meta_ready else ["설정 및 연결"]

        nav = _render_sidebar_nav(nav_items)

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
        f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    if nav == "요약":
        page_overview(meta, engine, f)
    elif nav == "예산 및 잔액":
        page_budget(meta, engine, f)
    elif nav == "시장 및 매체 분석":
        analysis_view = st.segmented_control(
            "분석 보기",
            ["시장 트렌드", "매체(지면) 분석"],
            default="시장 트렌드",
            key="market_media_view",
            label_visibility="collapsed",
        )
        if analysis_view == "매체(지면) 분석":
            page_media(engine, f)
        else:
            page_trend(meta, engine, f)
    elif nav == "성과 분석 · 캠페인":
        page_perf_campaign(meta, engine, f)
    elif nav in ["성과 분석 · 키워드", "성과 분석 · 소재", "쇼핑 검색어 분석"]:
        if not (f.get("manager") or f.get("account")):
            st.info("담당자 또는 광고주(계정) 필터를 먼저 1개 이상 선택하면 데이터가 표시됩니다.")
            st.stop()
        if nav == "성과 분석 · 키워드":
            page_perf_keyword(meta, engine, f)
        elif nav == "쇼핑 검색어 분석":
            page_perf_shopping_query(meta, engine, f)
        else:
            page_perf_ad(meta, engine, f)
    else:
        page_settings(engine)

if __name__ == "__main__":
    main()
