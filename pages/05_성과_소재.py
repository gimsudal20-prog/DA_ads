import streamlit as st
import pandas as pd
from utils import init_page
from state import FilterState
from ui_sidebar import render_sidebar
from database import get_engine, sql_read, table_exists
from ui_performance import render_performance_page

init_page()
engine = get_engine()

# 메타 조회 (공통)
meta = sql_read(engine, "SELECT * FROM dim_account_meta") if table_exists(engine, "dim_account_meta") else pd.DataFrame()

# 사이드바 렌더링 및 필터 가져오기
render_sidebar(meta, engine)
filters = FilterState.get()

# 공통 성과 페이지 렌더러 호출 (entity_type='ad' 지정)
render_performance_page("ad", meta, engine, filters)
