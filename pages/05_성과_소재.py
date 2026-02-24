import streamlit as st
import pandas as pd
from utils import init_page
from state import FilterState
from ui_sidebar import render_sidebar
from database import get_engine, sql_read, table_exists, get_meta
from ui_performance import render_performance_page

init_page()
engine = get_engine()
meta = get_meta(engine)

render_sidebar(meta, engine)
filters = FilterState.get()

# 공통 성과 페이지 렌더러 호출 (entity_type='ad' 지정)
render_performance_page("ad", meta, engine, filters)
