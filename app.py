import streamlit as st
from utils import init_page
from state import FilterState
from database import get_engine

init_page()
FilterState.init()
engine = get_engine()

# 대문 페이지를 보여주지 않고 바로 '01_요약' 페이지로 강제 이동시킵니다.
try:
    st.switch_page("pages/01_요약.py")
except Exception as e:
    st.error("🚨 pages 폴더 내부의 파일을 찾지 못했습니다. 파일명과 폴더명을 다시 확인해 주세요.")
