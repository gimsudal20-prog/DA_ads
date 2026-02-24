import streamlit as st
from utils import init_page
from state import FilterState
from ui_sidebar import render_sidebar
from database import get_engine, get_meta
from ui_performance import render_performance_page

init_page()
engine = get_engine()
meta = get_meta(engine)

render_sidebar(meta, engine)
filters = FilterState.get()

render_performance_page("campaign", meta, engine, filters)
