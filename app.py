# -*- coding: utf-8 -*-
"""app.py - Entry point (kept intentionally small)."""

from __future__ import annotations

import streamlit as st
from datetime import date, datetime
from zoneinfo import ZoneInfo

# Streamlit page config MUST be the first Streamlit command
st.set_page_config(
    page_title="네이버 검색광고 통합 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)

def _today_kst_for_cache_reset() -> date:
    try:
        return datetime.now(ZoneInfo("Asia/Seoul")).date()
    except Exception:
        return date.today()


def clear_cache_daily():
    """
    ✨ 밤새 누적된 메모리 누수 방지 및 Stale Connection 해결
    자정이 지나 날짜가 바뀌면 기존에 쌓인 메모리(캐시)와 DB 연결 리소스를 
    자동으로 완전히 비워주어 아침마다 앱이 뻗는 현상을 차단합니다.
    """
    today_kst = _today_kst_for_cache_reset()
    if "current_date" not in st.session_state:
        st.session_state["current_date"] = today_kst

    # 세션에 기록된 날짜와 현재 날짜가 다르면 (밤이 지났으면)
    if st.session_state["current_date"] != today_kst:
        st.cache_data.clear()      # 데이터프레임 캐시 초기화 (메모리 확보)
        st.cache_resource.clear()  # DB 엔진 객체 캐시 초기화 (끊긴 연결 강제 리셋)
        st.session_state.clear()   # 기타 세션 찌꺼기 정리
        st.session_state["current_date"] = today_kst

clear_cache_daily()

from styles import apply_global_css  # noqa: E402
apply_global_css()

import pages  # noqa: E402

pages.main()
