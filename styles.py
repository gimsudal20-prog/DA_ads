# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard (29CM Editorial Vibe)."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
/* 29CM Style Editorial UI Shell - 모노톤 기반의 세련된 플랫 디자인 */
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  --nv-bg: #FFFFFF; 
  --nv-panel: #F4F4F4; /* 29CM 특유의 밝은 회색 배경 */
  --nv-line: #E4E4E4; 
  --nv-line2: #C4C4C4; 
  --nv-text: #19191A; 
  --nv-muted: #474747; 
  --nv-muted-light: #A0A0A0; 
  --nv-primary: #375FFF; /* 강조용 쨍한 블루 */
  --nv-primary-hover: #2A4BCC; 
  --nv-green: #3CD333; 
  --nv-red: #FC503D; 
  --nv-radius: 6px; /* 둥근 모서리 대신 약간 각진 모서리로 세련미 강조 */
}

/* Base Typography */
html, body, [class*="css"] {
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, 'Helvetica Neue', 'Segoe UI', sans-serif !important;
    color: var(--nv-text);
}

/* Titles & Headers */
h1, h2, h3, h4, h5, h6 { font-weight: 700 !important; letter-spacing: -0.02em !important; color: var(--nv-text); }
.nv-h1 { font-size: 24px; font-weight: 800; margin-bottom: 20px; border-bottom: 1px solid var(--nv-line); padding-bottom: 12px; }
.nv-sec-title { font-size: 20px; font-weight: 700; margin-top: 32px; margin-bottom: 12px; color: var(--nv-text); display: flex; align-items: center; gap: 8px; }

/* ✨ [NEW] 모바일 친화적 가로 스와이프 (Scroll Snap) UI */
.kpi-group-container { margin-bottom: 32px; }
.kpi-group { margin-bottom: 24px; }
.kpi-group-title { font-size: 15px; font-weight: 700; color: var(--nv-text); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }

/* 가로로 넘겨보는(스와이프) 영역 */
.kpi-row { 
    display: flex; 
    flex-wrap: nowrap; 
    overflow-x: auto; 
    -webkit-overflow-scrolling: touch; 
    scroll-snap-type: x mandatory; 
    gap: 12px; 
    padding-bottom: 16px; /* 스크롤바 영역 확보 */
    scrollbar-width: none; /* Firefox */
}
.kpi-row::-webkit-scrollbar { display: none; /* Chrome, Safari */ }

/* 플랫한 KPI 카드 */
.kpi { 
    flex: 0 0 auto; 
    width: 150px; /* 고정 너비로 가로 스크롤 유도 */
    scroll-snap-align: start;
    background: var(--nv-bg); 
    border: 1px solid var(--nv-line); 
    padding: 16px; 
    border-radius: var(--nv-radius); 
    transition: border-color 0.2s ease; 
}
.kpi:hover { border-color: var(--nv-text); }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 500; margin-bottom: 6px; }
.kpi .v { font-size: 20px; font-weight: 700; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 600; margin-top: 8px; display: inline-block; }

/* 핵심 지표 강조 */
.kpi.highlight { border-color: var(--nv-text); background: #FAFAFA; }
.kpi.highlight .v { color: var(--nv-text); font-size: 24px; }

.kpi .d.pos { color: var(--nv-primary); }
.kpi .d.neg { color: var(--nv-red); }
.kpi .d.neu { color: var(--nv-muted-light); }

/* Table Styling - 불필요한 선 제거 및 깔끔한 플랫 스타일 */
table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left;}
table.nv-table th { background: var(--nv-bg); padding: 14px 8px; font-weight: 600; color: var(--nv-muted); border-bottom: 1px solid var(--nv-line); border-top: 1px solid var(--nv-text); }
table.nv-table td { padding: 14px 8px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); font-weight: 400; }
table.nv-table tr:hover td { background: #FAFAFA; }

/* Progress Bar */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 4px; border-radius: 2px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; background: var(--nv-text); }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 600; color: var(--nv-text); font-size: 12px; }

/* Streamlit Tabs Styling - 라인 형태의 스티키(Sticky) 느낌 탭 */
[data-baseweb="tab-list"] { gap: 16px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 500; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: var(--nv-text) !important; font-weight: 700 !important; border-bottom: 2px solid var(--nv-text) !important; box-shadow: none !important; }

/* ✨ [NEW] Expander (아코디언) Styling - 29CM 유의사항 탭처럼 보더리스/미니멀하게 */
[data-testid="stExpander"] { border: none !important; border-bottom: 1px solid var(--nv-line) !important; border-radius: 0 !important; box-shadow: none !important; background: transparent !important; }
[data-testid="stExpander"] summary { padding: 16px 0 !important; background-color: transparent !important; }
[data-testid="stExpander"] summary:hover { color: var(--nv-text) !important; }
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }
[data-testid="stExpander"] .streamlit-expanderContent { padding: 16px 0 24px 0 !important; font-size: 13px !important; color: var(--nv-muted) !important; line-height: 1.6 !important; }

/* 사이드바 메뉴 - 미니멀 스타일 */
[data-testid="stSidebar"] [role="radiogroup"] label { padding: 10px 12px !important; border-radius: var(--nv-radius) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background-color: var(--nv-text) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p { color: #FFFFFF !important; }

/* 필터 패널 박스 (29CM 옅은 회색 배경 스타일) */
.filter-panel { background-color: var(--nv-panel); padding: 20px; border-radius: var(--nv-radius); margin-bottom: 24px; border: none; }
</style>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
