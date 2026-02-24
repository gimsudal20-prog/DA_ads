import os
import re
import math
import logging
from datetime import date, timedelta
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# 공통 페이지 초기화
def init_page(page_title="네이버 검색광고 통합 대시보드"):
    st.set_page_config(page_title=page_title, page_icon="📊", layout="wide", initial_sidebar_state="expanded")
    
    # CSS 로드
    css_path = os.path.join(os.path.dirname(__file__), "assets", "style.css")
    if os.path.exists(css_path):
        with open(css_path, "r", encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        logger.warning("style.css 파일을 찾을 수 없습니다.")

def _safe_int(x, default: int = 0) -> int:
    try:
        if pd.isna(x) or x == "":
            return default
        return int(float(x))
    except Exception:
        return default

def format_currency(val) -> str:
    return f"{_safe_int(val):,}원"

def format_number_commas(val) -> str:
    return f"{_safe_int(val):,}"

def format_roas(val) -> str:
    try:
        if pd.isna(val): return "-"
        return f"{float(val):.0f}%"
    except Exception:
        return "-"

def _pct_change(curr: float, prev: float):
    if prev == 0:
        return 0.0 if curr == 0 else None
    return (float(curr) - float(prev)) / float(prev) * 100.0

def _pct_to_str(p) -> str:
    try:
        if pd.isna(p) or p is None: return "—"
        return f"{float(p):+.0f}%"
    except Exception: return "—"

def _pct_to_arrow(p) -> str:
    try:
        if pd.isna(p) or p is None: return "—"
        p = float(p)
        if p > 0: return f"▲ {abs(p):.0f}%"
        if p < 0: return f"▼ {abs(p):.0f}%"
        return f"• {abs(p):.0f}%"
    except Exception: return "—"

def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns: return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")
    out[col] = s.map(lambda x: "" if pd.isna(x) else ("0%" if float(x) == 0.0 else f"{float(x):.0f}%"))
    return out

# 날짜 로직
def _shift_month(d: date, months: int) -> date:
    base = (d.year * 12) + (d.month - 1) + int(months)
    y, m = base // 12, (base % 12) + 1
    nxt_d = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    last_day = (nxt_d - timedelta(days=1)).day
    return date(y, m, min(int(d.day), last_day))

def _period_compare_range(d1: date, d2: date, mode: str):
    if mode == "전일대비": return d1 - timedelta(days=1), d2 - timedelta(days=1)
    if mode == "전주대비": return d1 - timedelta(days=7), d2 - timedelta(days=7)
    return _shift_month(d1, -1), _shift_month(d2, -1)