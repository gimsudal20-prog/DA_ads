import streamlit as st
from datetime import date, timedelta

class FilterState:
    """싱글 소스 오브 트루스(SSOT)를 위한 세션 상태 래퍼 클래스"""
    
    @staticmethod
    def init():
        if "filters_v8" not in st.session_state:
            today = date.today()
            default_end = today - timedelta(days=1)
            st.session_state["filters_v8"] = {
                "q": "",
                "manager": [],
                "account": [],
                "type_sel": [],
                "period_mode": "어제",
                "d1": default_end,
                "d2": default_end,
                "top_n_keyword": 300,
                "top_n_ad": 200,
                "top_n_campaign": 200,
                "prefetch_warm": True,
                "customer_ids": [],
                "selected_customer_ids": [],
                "ready": False
            }
            
    @staticmethod
    def get() -> dict:
        FilterState.init()
        return st.session_state["filters_v8"]

    @staticmethod
    def update(**kwargs):
        FilterState.init()
        st.session_state["filters_v8"].update(kwargs)
        st.session_state["filters_v8"]["ready"] = True