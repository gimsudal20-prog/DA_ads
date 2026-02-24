import streamlit as st
from utils import init_page
from state import FilterState
from database import get_engine

init_page()

st.markdown("# 📊 네이버 검색광고 대시보드")
st.markdown("""
이 앱은 사이드바를 통해 여러 페이지로 탐색할 수 있습니다. 
왼쪽 사이드바에서 메뉴를 선택해주세요!
""")

FilterState.init()
engine = get_engine()

st.info("👈 좌측 'Pages' 메뉴를 이용해 '요약', '캠페인' 등으로 이동하세요.")