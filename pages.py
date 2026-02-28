# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
import traceback
import streamlit as st

from data import *
from ui import render_hero
from page_helpers import BUILD_TAG, build_filters
from view_overview import page_overview
from view_budget import page_budget
from view_campaign import page_perf_campaign
from view_keyword import page_perf_keyword

# NOTE: view_ad.py 안의 SyntaxError 등으로 앱 전체가 죽는 것을 막기 위해,
#       소재 페이지 import를 안전하게 감쌉니다.
try:
    from view_ad import page_perf_ad  # type: ignore
except Exception:
    # Python 3에서는 except 블록의 예외 변수(e)가 블록 종료 후 자동으로 삭제되므로,
    # 나중에 페이지 렌더 시 NameError가 날 수 있습니다. 문자열로 캡처해 고정합니다.
    _view_ad_import_error = traceback.format_exc()

    def page_perf_ad(meta, engine, f, _err=_view_ad_import_error):  # type: ignore
        st.error("❌ 'view_ad.py' 로딩 실패로 소재 분석 페이지를 열 수 없습니다.")
        st.caption("아래 오류는 view_ad.py의 문법 오류(SyntaxError) 또는 import 오류일 가능성이 큽니다.")
        st.code(_err)
        st.info("view_ad.py 파일을 이 대화에 업로드해주면, 문법 오류를 직접 고쳐서 전체 코드로 다시 드릴게요.")

from view_settings import page_settings

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
        st.markdown("### 📌 메뉴 이동")
        if not meta_ready: st.warning("동기화가 필요합니다.")
        
        # ✨ [수정] 메뉴 이름 간소화 반영
        nav_items = [
            "📊 요약", 
            "💰 예산 및 잔액", 
            "🚀 캠페인 분석", 
            "🔎 키워드 분석", 
            "🧩 소재 분석", 
            "⚙️ 설정 및 연결"
        ] if meta_ready else ["⚙️ 설정 및 연결"]
        
        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")

    st.markdown(f"<div class='nv-h1'>{nav}</div><div style='height:8px'></div>", unsafe_allow_html=True)
    f = None
    if nav != "⚙️ 설정 및 연결":
        if not meta_ready: st.error("설정 메뉴에서 동기화를 진행해주세요."); return
        f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    # ✨ [수정] 변경된 이름으로 라우팅 연결
    if nav == "📊 요약": page_overview(meta, engine, f)
    elif nav == "💰 예산 및 잔액": page_budget(meta, engine, f)
    elif nav == "🚀 캠페인 분석": page_perf_campaign(meta, engine, f)
    elif nav == "🔎 키워드 분석": page_perf_keyword(meta, engine, f)
    elif nav == "🧩 소재 분석": page_perf_ad(meta, engine, f)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
