# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
/* Modern SaaS UI Shell (Inspired by Tailwind CSS & Self-Marketing Dashboard) */
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  --nv-bg: #F9FAFB; /* Tailwind gray-50 */
  --nv-panel: #FFFFFF;
  --nv-line: #F3F4F6; /* Tailwind gray-100 */
  --nv-line2: #E5E7EB; /* Tailwind gray-200 */
  --nv-text: #111827; /* Tailwind gray-900 */
  --nv-muted: #4B5563; /* Tailwind gray-600 */
  --nv-muted-light: #9CA3AF; /* Tailwind gray-400 */
  --nv-primary: #2563EB; /* Tailwind blue-600 */
  --nv-primary-hover: #1D4ED8; /* Tailwind blue-700 */
  --nv-green: #10B981; /* Tailwind emerald-500 */
  --nv-red: #EF4444; /* Tailwind red-500 */
  --nv-shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
  --nv-shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  --nv-radius-lg: 0.75rem; /* 12px */
  --nv-radius-xl: 1rem; /* 16px */
  
  /* Streamlit Native Overrides */
  --primary-color: #2563EB;
  --background-color: #F9FAFB;
  --secondary-background-color: #FFFFFF;
  --text-color: #111827;
}

/* Kill Streamlit chrome */
#MainMenu, footer {visibility:hidden;}
header[data-testid="stHeader"] {background: transparent; height:0px;}
div[data-testid="stToolbar"] {visibility:hidden; height:0;}

/* Page background + base font */
html, body, [data-testid="stAppViewContainer"] {
  background: var(--nv-bg) !important;
  font-family: Pretendard, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", sans-serif;
  color: var(--nv-text);
  -webkit-font-smoothing: antialiased;
}

/* Glassmorphism Sidebar */
section[data-testid="stSidebar"] {
  background-color: rgba(247, 250, 252, 0.85) !important;
  backdrop-filter: blur(20px) saturate(180%) !important;
  border-right: 1px solid var(--nv-line) !important;
}
section[data-testid="stSidebar"] .stMarkdown, 
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span {
  font-size: 14px;
  font-weight: 600;
  color: var(--nv-text);
}

/* Sidebar Radio/Select styles */
section[data-testid="stSidebar"] div[role="radiogroup"] > label {
  border: none !important;
  background: transparent !important;
  padding: 10px 12px !important;
  margin-bottom: 4px !important;
  border-radius: 8px !important;
  transition: all 0.2s ease-out;
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover {
  background: var(--nv-bg) !important;
  transform: scale(1.02);
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) {
  background: rgba(37, 99, 235, 0.1) !important; /* light blue */
  color: var(--nv-primary) !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) p {
  color: var(--nv-primary) !important;
}

/* Wide layout padding */
.main .block-container {
  max-width: 100% !important;
  padding-left: 2.5rem !important;
  padding-right: 2.5rem !important;
  padding-top: 20px !important;
  padding-bottom: 40px !important;
}

/* Topbar Design */
.nv-topbar {
  position: sticky; top: 0; z-index: 999;
  background: rgba(255,255,255,0.9);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--nv-line);
  padding: 16px 0;
  margin: -20px -2.5rem 24px -2.5rem;
}
.nv-topbar .inner {
  max-width: 100%; margin: 0 auto;
  display: flex; align-items: center; justify-content: space-between;
  padding: 0 2.5rem;
}
.nv-brand {
  display: flex; align-items: center; gap: 10px;
  font-weight: 800; font-size: 18px; color: var(--nv-text);
}
.nv-dot {
  width: 10px; height: 10px; border-radius: 50%;
  background: var(--nv-primary);
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.2);
}
.nv-sub { font-weight: 500; font-size: 13px; color: var(--nv-muted-light); }
.nv-pill {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 6px 12px; border-radius: 999px;
  background: var(--nv-panel);
  border: 1px solid var(--nv-line2);
  box-shadow: var(--nv-shadow-sm);
  font-size: 12px; font-weight: 600; color: var(--nv-muted);
}

/* Typography & Headings */
.nv-h1 { font-size: 24px; font-weight: 800; color: var(--nv-text); margin: 0; letter-spacing: -0.5px; }
.nv-sec-title {
  font-size: 18px; font-weight: 700; color: var(--nv-text);
  margin: 24px 0 16px 0; letter-spacing: -0.3px;
}

/* KPI Cards (Matches HTML structure) */
.kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
.kpi { 
  background: var(--nv-panel); 
  border: 1px solid var(--nv-line2); 
  border-radius: var(--nv-radius-xl); 
  padding: 20px 24px; 
  box-shadow: var(--nv-shadow-sm); 
  transition: all 0.2s ease-out;
}
.kpi:hover { transform: translateY(-3px); box-shadow: var(--nv-shadow-md); }
.kpi .k { font-size: 14px; color: var(--nv-muted); font-weight: 600; display: flex; align-items: center; }
.kpi .k .kpi-tip { margin-left: 6px; font-size: 12px; opacity: 0.6; cursor: help; background: var(--nv-line); padding: 2px 6px; border-radius: 4px; }
.kpi .v { margin-top: 8px; font-size: 28px; font-weight: 800; letter-spacing: -0.5px; color: var(--nv-text); }
.kpi .d { margin-top: 12px; font-size: 13px; font-weight: 700; display: inline-flex; align-items: center; gap: 4px; padding: 4px 10px; border-radius: 8px; }
.kpi .d.pos { background: rgba(16, 185, 129, 0.1); color: var(--nv-green); } /* Emerald */
.kpi .d.neg { background: rgba(239, 68, 68, 0.1); color: var(--nv-red); }
.kpi .d.neu { background: var(--nv-line); color: var(--nv-muted); }
.kpi .chip { font-size: 12px; font-weight: 600; opacity: 0.9; }

/* Delta chips */
.delta-chip-row { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 12px 0 20px 0; }
.delta-chip { background: var(--nv-panel); border: 1px solid var(--nv-line2); border-radius: var(--nv-radius-lg); padding: 14px 16px; box-shadow: var(--nv-shadow-sm); }
.delta-chip .l { font-size: 13px; color: var(--nv-muted); font-weight: 600; }
.delta-chip .v { margin-top: 8px; font-size: 16px; font-weight: 800; letter-spacing: -0.2px; }
.delta-chip .v .arr { display: inline-block; width: 18px; font-weight: 900; }
.delta-chip .v .p { font-weight: 600; color: var(--nv-muted-light); margin-left: 6px; font-size: 13px; }
.delta-chip.pos .v { color: var(--nv-primary); } 
.delta-chip.neg .v { color: var(--nv-red); }
.delta-chip.zero .v { color: var(--nv-muted); } 

/* Tables & DataFrames */
[data-testid="stDataFrame"] { 
  border: 1px solid var(--nv-line2) !important; 
  border-radius: var(--nv-radius-xl) !important; 
  overflow: hidden; 
  box-shadow: var(--nv-shadow-sm);
}
[data-testid="stDataFrame"] * { font-size: 13px !important; }

/* Buttons Overrides */
.stButton > button { 
  border-radius: 8px; 
  font-weight: 600; 
  transition: all 0.2s ease-out; 
  box-shadow: var(--nv-shadow-sm);
}
.stButton > button:hover { transform: scale(1.02); }
.stButton > button[kind="primary"] { background-color: var(--nv-primary); border-color: var(--nv-primary); }
.stButton > button[kind="primary"]:hover { background-color: var(--nv-primary-hover); }

/* Inputs */
.stSelectbox > div > div, .stMultiSelect > div > div, .stTextInput > div > div, .stDateInput > div > div { 
  border-radius: 8px; border-color: var(--nv-line2); font-size: 14px;
}

/* Custom Table (Budget) */
.nv-table-wrap { border: 1px solid var(--nv-line2); border-radius: var(--nv-radius-xl); overflow: auto; background: var(--nv-panel); box-shadow: var(--nv-shadow-sm); }
table.nv-table { width: 100%; border-collapse: collapse; font-size: 13px; }
table.nv-table th { position: sticky; top: 0; background: var(--nv-bg); z-index: 2; text-align: left; padding: 12px 16px; border-bottom: 1px solid var(--nv-line2); font-weight: 700; color: var(--nv-muted); }
table.nv-table td { padding: 12px 16px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); font-weight: 500; }
table.nv-table tr:hover td { background: var(--nv-bg); }

/* Progress Bar */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 8px; border-radius: 999px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 999px; transition: width 0.5s ease; }
.nv-pbar-txt { min-width: 48px; text-align: right; font-weight: 700; color: var(--nv-muted); font-size: 12px; }

/* Streamlit Tabs Styling */
[data-baseweb="tab-list"] { gap: 8px; }
[data-baseweb="tab"] { background: var(--nv-panel); border-radius: 8px; border: 1px solid var(--nv-line2); font-weight: 600; padding: 10px 16px; margin: 0 !important; }
[aria-selected="true"] { background: var(--nv-bg) !important; border-bottom-color: transparent !important; color: var(--nv-primary) !important; }
</style>
"""

def apply_global_css() -> None:
    try: st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    except Exception: pass
