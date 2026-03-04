# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
/* 29CM 감성의 모노톤 기반 플랫 디자인 (그림자 제거, 라인 강조) */
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  --nv-bg: #FFFFFF; 
  --nv-panel: #F8FAFC;
  --nv-line: #E4E4E4; 
  --nv-line2: #C4C4C4; 
  --nv-text: #19191A; 
  --nv-muted: #474747; 
  --nv-muted-light: #A0A0A0; 
  --nv-primary: #375FFF;
  --nv-green: #3CD333; 
  --nv-red: #FC503D; 
  --nv-radius: 8px; 
}

/* Base Typography */
html, body, [class*="css"] {
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, 'Helvetica Neue', 'Segoe UI', sans-serif !important;
    color: var(--nv-text);
}

/* Titles & Headers */
h1, h2, h3, h4, h5, h6 { font-weight: 700 !important; letter-spacing: -0.02em !important; color: var(--nv-text); }
.nv-sec-title { font-size: 18px; font-weight: 700; margin-top: 24px; margin-bottom: 8px; color: var(--nv-text); display: flex; align-items: center; gap: 8px; }

/* KPI Grid */
.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 16px; }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-muted); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }

/* KPI Card */
.kpi { background: var(--nv-bg); border: 1px solid var(--nv-line); padding: 14px; border-radius: var(--nv-radius); transition: border-color 0.2s ease; }
.kpi:hover { border-color: var(--nv-primary); background: #F8FAFF; }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 600; margin-top: 6px; display: inline-block; padding: 2px 6px; border-radius: 4px; }

/* KPI Highlight */
.kpi.highlight { border-color: var(--nv-primary); background: #F5F8FF; }
.kpi.highlight .v { color: var(--nv-primary); font-size: 22px; }
.kpi .d.pos { background: #EBF8FF; color: var(--nv-primary); }
.kpi .d.neg { background: #FFF0EE; color: var(--nv-red); }
.kpi .d.neu { background: #F4F4F4; color: var(--nv-muted); }

/* Table Styling */
table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; }
table.nv-table th { background: #F8FAFC; padding: 14px 16px; font-weight: 600; color: var(--nv-muted); border-bottom: 1px solid var(--nv-line); border-top: 2px solid var(--nv-primary); }
table.nv-table td { padding: 14px 16px; border-bottom: 1px solid #F4F4F4; vertical-align: middle; color: var(--nv-text); transition: all 0.2s ease; }
table.nv-table tr:hover td { background: #F5F8FF; color: var(--nv-primary); }

/* Progress Bar */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: #EDF2F7; overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

/* Tabs */
[data-baseweb="tab-list"] { gap: 16px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 500; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: var(--nv-primary) !important; font-weight: 700 !important; border-bottom: 2px solid var(--nv-primary) !important; box-shadow: none !important; }

/* Expander */
[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; background: var(--nv-bg) !important; }
[data-testid="stExpander"] summary { padding: 16px !important; background-color: var(--nv-panel) !important; border-radius: var(--nv-radius) !important;}
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }

/* Sidebar */
[data-testid="stSidebar"] { background: #F5F8FF !important; border-right: 1px solid #DCE6FF !important; }
[data-testid="stSidebar"] .block-container { padding-top: 1rem !important; }
[data-testid="stSidebar"] [role="radiogroup"] {
  background: #FFFFFF;
  border: 1px solid #DCE6FF;
  border-radius: 12px;
  padding: 8px;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 10px 12px !important;
  border-radius: var(--nv-radius) !important;
  border: 1px solid transparent;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover {
  background: #375FFF !important;
  border-color: #2748C9;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover p {
  color: #FFFFFF !important;
  font-weight: 700 !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background-color: #375FFF !important;
  border-color: #2748C9 !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
  color: #FFFFFF !important;
  font-weight: 700 !important;
}

/* Filter dropdown hover/selected consistency */
/* Streamlit/BaseWeb 멀티셀렉트 메뉴의 hover/선택 색상 가독성 개선 */
div[role="listbox"] ul li,
[data-baseweb="menu"] [role="option"] {
  border-radius: 6px !important;
  box-shadow: none !important;
}

div[role="listbox"] ul li:hover,
div[role="listbox"] ul li[aria-selected="true"],
[data-baseweb="menu"] [role="option"]:hover,
[data-baseweb="menu"] [role="option"][aria-selected="true"] {
  background-color: #EAF0FF !important;
  color: #1F3FBF !important;
}

div[role="listbox"] ul li:hover *,
div[role="listbox"] ul li[aria-selected="true"] *,
[data-baseweb="menu"] [role="option"]:hover *,
[data-baseweb="menu"] [role="option"][aria-selected="true"] * {
  color: #1F3FBF !important;
  font-weight: 700 !important;
}

/* 선택된 멀티셀렉트 태그(칩) 색상도 동일 톤으로 정리 */
[data-baseweb="tag"] {
  background-color: #EAF0FF !important;
  border: 1px solid #C9D6FF !important;
}
[data-baseweb="tag"] * {
  color: #1F3FBF !important;
}

</style>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
