# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
from importlib import import_module

import streamlit as st

from data import get_campaign_type_options, get_engine, get_latest_dates, get_meta, load_dim_campaign
from page_helpers import BUILD_TAG, build_filters
from ui import render_hero


@st.cache_data(ttl=43200, max_entries=4, show_spinner=False)
def _cached_type_options(dim_campaign):
    return tuple(get_campaign_type_options(dim_campaign))


def _lazy_page(module_name: str, func_name: str):
    module = import_module(module_name)
    return getattr(module, func_name)


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
            "매체(지면) 분석",
            "성과 분석 · 캠페인",
            "성과 분석 · 키워드",
            "성과 분석 · 소재",
            "쇼핑 검색어 분석",
            "설정 및 연결"
        ] if meta_ready else ["설정 및 연결"]

        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")

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
        dim_campaign = load_dim_campaign(engine)
        type_opts = list(_cached_type_options(dim_campaign))
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
        st.stop()

    if nav == "요약":
        _lazy_page("view_overview", "page_overview")(meta, engine, f)
    elif nav == "예산 및 잔액":
        _lazy_page("view_budget", "page_budget")(meta, engine, f)
    elif nav == "매체(지면) 분석":
        _lazy_page("view_media", "page_media")(engine, f)
    elif nav == "성과 분석 · 캠페인":
        _lazy_page("view_campaign", "page_perf_campaign")(meta, engine, f)
    elif nav == "성과 분석 · 키워드":
        _lazy_page("view_keyword", "page_perf_keyword")(meta, engine, f)
    elif nav == "쇼핑 검색어 분석":
        _lazy_page("view_shopping_query", "page_perf_shopping_query")(meta, engine, f)
    elif nav == "성과 분석 · 소재":
        _lazy_page("view_ad", "page_perf_ad")(meta, engine, f)
    else:
        _lazy_page("view_settings", "page_settings")(engine)


if __name__ == "__main__":
    main()
