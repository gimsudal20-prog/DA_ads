import streamlit as st
from utils import init_page, format_currency, format_number_commas
from state import FilterState
from ui_sidebar import render_sidebar
from database import get_engine, get_meta
from queries import query_timeseries_common

init_page()
engine = get_engine()
meta = get_meta(engine)

render_sidebar(meta, engine)
f = FilterState.get()

if not f.get("ready"):
    st.info("왼쪽 사이드바에서 검색조건을 설정하세요.")
else:
    st.markdown("## 요약 (Overview)")
    ts = query_timeseries_common(engine, "fact_campaign_daily", f["d1"], f["d2"], tuple(f["customer_ids"]))
    if ts.empty:
        st.warning("선택하신 조건에 데이터가 존재하지 않습니다.")
    else:
        st.metric("총 비용", format_currency(ts['cost'].sum()))
        st.metric("총 클릭수", format_number_commas(ts['clk'].sum()))
