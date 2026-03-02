# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
/* Modern SaaS UI Shell - 눈이 편안하고 세련된 UI */
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  --nv-bg: #F8FAFC; 
  --nv-panel: #FFFFFF;
  --nv-line: #E2E8F0; 
  --nv-line2: #CBD5E1; 
  --nv-text: #0F172A; 
  --nv-muted: #475569; 
  --nv-muted-light: #94A3B8; 
  --nv-primary: #2563EB; 
  --nv-primary-hover: #1D4ED8; 
  --nv-green: #10B981; 
  --nv-red: #EF4444; 
}

/* Base Typography */
html, body, [class*="css"] {
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, 'Helvetica Neue', 'Segoe UI', sans-serif !important;
    color: var(--nv-text);
}

/* Titles & Headers */
h1, h2, h3, h4, h5, h6 { font-weight: 700 !important; letter-spacing: -0.02em !important; color: var(--nv-text); }
.nv-h1 { font-size: 26px; font-weight: 800; margin-bottom: 20px; border-bottom: 2px solid var(--nv-line); padding-bottom: 12px; }
.nv-sec-title { font-size: 18px; font-weight: 700; margin-top: 24px; margin-bottom: 8px; color: #1E293B; display: flex; align-items: center; gap: 8px; }

/* Custom KPI Row - 부드러운 카드 UI */
.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: #F8FAFC; border: 1px solid var(--nv-line); border-radius: 16px; padding: 16px; }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-muted); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }
.kpi { background: var(--nv-panel); border: 1px solid var(--nv-line); padding: 14px; border-radius: 10px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); transition: transform 0.2s ease, box-shadow 0.2s ease; }
.kpi:hover { transform: translateY(-2px); box-shadow: 0 4px 6px rgba(0,0,0,0.06); }
.kpi .k { font-size: 12px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 600; margin-top: 6px; display: inline-block; padding: 2px 6px; border-radius: 4px; }

/* 핵심 지표 강조 (ROAS, 광고비 등) */
.kpi.highlight { border-color: var(--nv-primary); background: #EFF6FF; }
.kpi.highlight .v { color: var(--nv-primary); font-size: 22px; }

.kpi .d.pos { background: #DCFCE7; color: #047857; }
.kpi .d.neg { background: #FEE2E2; color: #B91C1C; }
.kpi .d.neu { background: #F1F5F9; color: #475569; }

/* Table Styling */
table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-panel); font-size: 13px; border-radius: 8px; overflow: hidden; }
table.nv-table th { background: #F8FAFC; text-align: left; padding: 12px 16px; font-weight: 700; color: var(--nv-muted); border-bottom: 2px solid var(--nv-line); }
table.nv-table td { padding: 12px 16px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); font-weight: 500; }
table.nv-table tr:hover td { background: #F1F5F9; }

/* Progress Bar */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 8px; border-radius: 999px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 999px; transition: width 0.5s ease; }
.nv-pbar-txt { min-width: 48px; text-align: right; font-weight: 700; color: var(--nv-muted); font-size: 12px; }

/* Streamlit Tabs Styling */
[data-baseweb="tab-list"] { gap: 8px; padding-bottom: 8px; }
[data-baseweb="tab"] { background: var(--nv-panel); border-radius: 8px; border: 1px solid var(--nv-line2); font-weight: 600; padding: 10px 16px; margin: 0 !important; color: var(--nv-muted); transition: all 0.2s; }
[data-baseweb="tab"]:hover { background: #F1F5F9; }
[aria-selected="true"] { background: var(--nv-text) !important; color: #FFFFFF !important; border-color: var(--nv-text) !important; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }

/* Expander Styling */
.streamlit-expanderHeader { font-weight: 700 !important; color: var(--nv-text) !important; background-color: #F8FAFC !important; border-radius: 8px; }

/* 사이드바 메뉴 */
[data-testid="stSidebar"] [role="radiogroup"] { gap: 6px !important; }
[data-testid="stSidebar"] [role="radiogroup"] label { padding: 10px 14px !important; background-color: transparent !important; border-radius: 8px !important; transition: all 0.2s ease !important; margin: 0 !important; cursor: pointer !important; border: none !important; display: block !important; }
[data-testid="stSidebar"] [role="radiogroup"] label > div:first-child { display: none !important; }
[data-testid="stSidebar"] [role="radiogroup"] label p { font-weight: 500 !important; font-size: 15px !important; color: #64748B !important; margin: 0 !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:hover { background-color: #F1F5F9 !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:hover p { color: #0F172A !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background-color: #2563EB !important; box-shadow: 0 2px 4px rgba(37, 99, 235, 0.2) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p { color: #FFFFFF !important; font-weight: 600 !important; }
</style>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
