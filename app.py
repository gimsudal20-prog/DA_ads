# -*- coding: utf-8 -*-
"""app.py - Entry point (kept intentionally small).

✅ Split files (<=5):
- app.py     : entry + page config
- styles.py  : ALL CSS
- data.py    : DB + cached queries + shared formatters
- ui.py      : UI components (tables/charts/downloads)
- pages.py   : pages + router
"""

from __future__ import annotations

import streamlit as st

# Streamlit page config MUST be the first Streamlit command
st.set_page_config(
    page_title="네이버 검색광고 통합 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from styles import apply_global_css  # noqa: E402
apply_global_css()

import pages  # noqa: E402

pages.main()
