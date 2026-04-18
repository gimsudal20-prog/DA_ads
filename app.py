# -*- coding: utf-8 -*-
"""app.py - Entry point (kept intentionally small)."""

from __future__ import annotations

import streamlit as st
from datetime import date

# Streamlit page config MUST be the first Streamlit command
st.set_page_config(
    page_title="네이버 검색광고 통합 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)

def clear_cache_daily():
    """
    날짜가 바뀐 뒤에도 앱이 과하게 무거워지지 않도록 필요한 캐시만 선택적으로 비웁니다.

    이전 구현은 자정이 지나면 session_state 전체를 날려 위젯 상태/선택값까지 모두 초기화했고,
    첫 진입 시 불필요한 재연산이 연쇄적으로 발생할 수 있었습니다.
    여기서는 날짜 기준 마커만 갱신하고, 무거운 데이터 캐시/DB 리소스만 정리합니다.
    """
    today = date.today()
    previous_date = st.session_state.get("current_date")
    if previous_date is None:
        st.session_state["current_date"] = today
        return

    if previous_date != today:
        st.cache_data.clear()
        st.cache_resource.clear()

        volatile_prefixes = (
            "_table_names_cache",
            "overview_text_kw::",
            "overview_text_kw_powerlink::",
        )
        volatile_exact_keys = {
            "latest_dates_cache",
        }
        for key in list(st.session_state.keys()):
            if key in volatile_exact_keys or any(str(key).startswith(prefix) for prefix in volatile_prefixes):
                del st.session_state[key]

        st.session_state["current_date"] = today

clear_cache_daily()

from styles import apply_global_css  # noqa: E402
apply_global_css()

import pages  # noqa: E402

pages.main()
