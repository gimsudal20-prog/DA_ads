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
  --nv-panel: #F8FAFC; /* 아주 연한 쿨그레이 톤으로 그룹 구분 */
  --nv-line: #E4E4E4; 
  --nv-line2: #C4C4C4; 
  --nv-text: #19191A; 
  --nv-muted: #474747; 
  --nv-muted-light: #A0A0A0; 
  --nv-primary: #375FFF; /* 강조용 쨍한 블루 */
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

/* ✨ [레이아웃 원복] 이전처럼 박스형 그리드로 한눈에 보이게 배치 */
.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 16px; }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-muted); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }

/* 격자(Grid) 구조로 KPI 나열 */
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }

/* 플랫한 KPI 카드 */
.kpi { 
    background: var(--nv-bg); 
    border: 1px solid var(--nv-line); 
    padding: 14px; 
    border-radius: var(--nv-radius); 
    transition: border-color 0.2s ease; 
}
.kpi:hover { border-color: var(--nv-text); }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 600; margin-top: 6px; display: inline-block; padding: 2px 6px; border-radius: 4px; }

/* 핵심 지표 강조 (ROAS, 광고비) */
.kpi.highlight { border-color: var(--nv-primary); background: #F5F8FF; }
.kpi.highlight .v { color: var(--nv-primary); font-size: 22px; }

.kpi .d.pos { background: #EBF8FF; color: var(--nv-primary); }
.kpi .d.neg { background: #FFF0EE; color: var(--nv-red); }
.kpi .d.neu { background: #F4F4F4; color: var(--nv-muted); }

/* ✨ [NEW] Table Styling - 세련된 파스텔 호버 및 라인 정리 */
table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; }
table.nv-table th { background: #F8FAFC; padding: 14px 16px; font-weight: 600; color: var(--nv-muted); border-bottom: 1px solid var(--nv-line); border-top: 2px solid var(--nv-text); }
table.nv-table td { padding: 14px 16px; border-bottom: 1px solid #F4F4F4; vertical-align: middle; color: var(--nv-text); transition: all 0.2s ease; }
table.nv-table tr:hover td { background: #F5F8FF; color: var(--nv-primary); } /* 호버 액션 */

/* Progress Bar */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: #EDF2F7; overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

/* Streamlit Tabs Styling */
[data-baseweb="tab-list"] { gap: 16px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 500; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: var(--nv-text) !important; font-weight: 700 !important; border-bottom: 2px solid var(--nv-text) !important; box-shadow: none !important; }

/* Expander Styling */
[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; background: var(--nv-bg) !important; }
[data-testid="stExpander"] summary { padding: 16px !important; background-color: var(--nv-panel) !important; border-radius: var(--nv-radius) !important;}
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }

/* 사이드바 메뉴 */
[data-testid="stSidebar"] [role="radiogroup"] label { padding: 10px 12px !important; border-radius: var(--nv-radius) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background-color: var(--nv-text) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p { color: #FFFFFF !important; font-weight: 600 !important; }

/* 필터 패널 박스 */
.filter-panel { background-color: var(--nv-panel); padding: 20px; border-radius: var(--nv-radius); margin-bottom: 24px; border: 1px solid var(--nv-line); }
</style>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
