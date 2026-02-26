# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations

import streamlit as st

GLOBAL_UI_CSS = """
<style>
/* Naver-like admin UI shell: compact, table-first, minimal cards */
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root{
  --nv-bg:#F5F6F7;
  --nv-panel:#FFFFFF;
  --nv-line:rgba(0,0,0,.08);
  --nv-line2:rgba(0,0,0,.12);
  --nv-text:#1A1C20;
  --nv-muted:rgba(26,28,32,.62);
  --nv-green:#03C75A;
  --nv-up:#EF4444; /* up(증가)=빨강(국내표준) */
  --nv-blue:#2563EB;
  --nv-red:#EF4444;
  --nv-shadow:0 4px 12px rgba(0,0,0,.03);
  --nv-radius:12px;
}

/* Kill Streamlit chrome */
#MainMenu, footer {visibility:hidden;}
header[data-testid="stHeader"]{height:0px;}
div[data-testid="stToolbar"]{visibility:hidden;height:0;}

/* Page background + base font */
html, body, [data-testid="stAppViewContainer"]{
  background: var(--nv-bg) !important;
  font-family: Pretendard, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", sans-serif;
  color: var(--nv-text);
}

/* Sidebar as Naver left nav */
section[data-testid="stSidebar"]{
  background: var(--nv-panel) !important;
  border-right: 1px solid var(--nv-line);
}
section[data-testid="stSidebar"] .stMarkdown, 
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div{
  font-size: 13px;
}
section[data-testid="stSidebar"] .block-container{
  padding-top: 10px !important;
}
/* Sidebar collapse control (keep visible) */
[data-testid="stSidebarCollapsedControl"]{
  display: flex;
  align-items: center;
  justify-content: center;
  width: 38px; height: 38px;
  border-radius: 10px;
  border: 1px solid var(--nv-line);
  background: rgba(255,255,255,.86);
  box-shadow: var(--nv-shadow);
  position: fixed;
  top: 10px;
  left: 10px;
  z-index: 10001;
}

@media (min-width: 900px){
  section[data-testid="stSidebar"]{
    transform: translateX(0) !important;
    margin-left: 0 !important;
    min-width: 260px !important;
    width: 260px !important;
  }
  section[data-testid="stSidebar"][aria-expanded="false"]{
    transform: translateX(0) !important;
    min-width: 260px !important;
    width: 260px !important;
  }
  section[data-testid="stSidebar"] > div:first-child{
    width: 260px !important;
  }
}

/* 🚀 화면 전체를 와이드하게 쓰도록 수정 */
.main .block-container{
  max-width: 100% !important;
  padding-left: 2.5rem !important;
  padding-right: 2.5rem !important;
  padding-top: 14px !important;
  padding-bottom: 40px !important;
}

/* Topbar */
.nv-topbar{
  position: sticky; top: 0; z-index: 999;
  background: rgba(245,246,247,.86);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--nv-line);
  padding: 10px 0 10px 0;
  margin: -14px -1px 12px -1px;
}
.nv-topbar .inner{
  max-width: 100%; margin: 0 auto;
  display:flex; align-items:center; justify-content:space-between;
  padding: 0 14px;
}
.nv-brand{
  display:flex; align-items:center; gap:10px;
  font-weight: 800; font-size: 16px;
}
.nv-dot{
  width:10px;height:10px;border-radius:50%;
  background: var(--nv-green);
  box-shadow: 0 0 0 3px rgba(3,199,90,.14);
}
.nv-sub{
  font-weight: 600; font-size: 12px; color: var(--nv-muted);
}
.nv-pill{
  display:inline-flex; align-items:center; gap:6px;
  padding:6px 10px; border-radius:999px;
  background: var(--nv-panel);
  border: 1px solid var(--nv-line);
  box-shadow: var(--nv-shadow);
  font-size: 12px; color: var(--nv-muted);
}

.nv-h1{font-size:20px;font-weight:900;color:var(--nv-text);margin:4px 0 0 0;}
.nv-panel{
  background: var(--nv-panel);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius);
  box-shadow: var(--nv-shadow);
}
.nv-sec-title{
  font-size: 16px;
  font-weight: 900;
  margin: 16px 0 12px 0;
  letter-spacing: -0.2px;
}
div[data-baseweb="select"] > div{ min-height: 38px !important; }
input[type="text"], textarea{ min-height: 38px !important; }

/* 🚀 KPI row 디자인 대폭 개선 */
.kpi-row{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
.kpi{ 
  background: var(--nv-panel); 
  border: 1px solid rgba(0,0,0,0.05); 
  border-radius: 12px; 
  padding: 18px 20px; 
  box-shadow: 0 4px 12px rgba(0,0,0,0.03); 
  transition: transform 0.2s, box-shadow 0.2s;
}
.kpi:hover { transform: translateY(-2px); box-shadow: 0 8px 16px rgba(0,0,0,0.06); }
.kpi .k{font-size:14px;color:#64748B;font-weight:700;}
.kpi .k .kpi-tip{margin-left:6px; font-size:12px; opacity:.55; cursor:help;}
.kpi .v{margin-top:8px;font-size:26px;font-weight:900;letter-spacing:-0.5px;color:#0F172A;}
.kpi .d{margin-top:10px;font-size:13px;font-weight:800;display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:6px;}
.kpi .d.pos{background:rgba(239, 68, 68, 0.1); color:var(--nv-red);}
.kpi .d.neg{background:rgba(37, 99, 235, 0.1); color:var(--nv-blue);}
.kpi .d.neu{background:rgba(100, 116, 139, 0.1); color:#64748B;}
.kpi .chip{ font-size:11px; padding:2px 6px; border-radius:999px; border:1px solid var(--nv-line); color:var(--nv-muted); }

/* Delta chips */
.delta-chip-row{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin: 10px 0 14px 0; }
.delta-chip{ background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: 12px; padding: 10px 12px; box-shadow: 0 1px 6px rgba(0,0,0,.04); }
.delta-chip .l{ font-size: 12px; color: var(--nv-muted); font-weight: 800; }
.delta-chip .v{ margin-top: 6px; font-size: 14px; font-weight: 900; letter-spacing: -0.15px; }
.delta-chip .v .arr{display:inline-block; width: 18px; font-weight: 900;}
.delta-chip .v .p{font-weight: 800; color: var(--nv-muted); margin-left: 4px;}
.delta-chip.pos .v{color: var(--nv-red);} 
.delta-chip.neg .v{color: var(--nv-blue);}
.delta-chip.zero .v{color: rgba(26,28,32,.72);} 

div[role="radiogroup"] > label{ border: 1px solid var(--nv-line); background: var(--nv-panel); border-radius: 8px; padding: 6px 10px; margin-right: 6px; }
div[role="radiogroup"] > label:hover{border-color: var(--nv-line2);}

[data-testid="stDataFrame"]{ border: 1px solid var(--nv-line); border-radius: 10px; overflow: hidden; }
[data-testid="stDataFrame"] *{ font-size: 12px !important; }
.stButton > button{ border-radius: 8px; border: 1px solid var(--nv-line); background: var(--nv-panel); padding: 6px 10px; font-weight: 800; }
.stButton > button:hover{ border-color: var(--nv-line2); }
.stSelectbox, .stMultiSelect, .stTextInput, .stDateInput{ font-size: 12px; }

/* Sidebar fix */
section[data-testid="stSidebar"] div[role="radiogroup"]{gap:6px;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label{ border: 0 !important; background: transparent !important; padding: 8px 12px !important; margin: 0 !important; border-radius: 10px !important; width: 100%; }
section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover{ background: rgba(0,0,0,.04) !important; }
section[data-testid="stSidebar"] div[role="radiogroup"] > label > div:first-child{ display:none !important; }
section[data-testid="stSidebar"] div[role="radiogroup"] > label p{ margin:0 !important; font-size: 13px !important; font-weight: 800 !important; color: var(--nv-text) !important; }
section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked){ background: rgba(3,199,90,.10) !important; border: 1px solid rgba(3,199,90,.24) !important; }

/* Progress bar cell */
.nv-pbar{display:flex; align-items:center; gap:10px; min-width:160px;}
.nv-pbar-bg{position:relative; flex:1; height:10px; border-radius:999px; background: rgba(0,0,0,.08); overflow:hidden;}
.nv-pbar-fill{position:absolute; left:0; top:0; bottom:0; border-radius:999px;}
.nv-pbar-txt{min-width:56px; text-align:right; font-weight:800; color: var(--nv-text); font-size:12px;}
.nv-table-wrap{border:1px solid var(--nv-line); border-radius: 12px; overflow:auto; background: var(--nv-panel);}
table.nv-table{width:100%; border-collapse:collapse; font-size:13px;}
table.nv-table th{position:sticky; top:0; background: rgba(245,246,247,.98); z-index:2; text-align:left; padding:10px 12px; border-bottom:1px solid var(--nv-line2); font-weight:900;}
table.nv-table td{padding:10px 12px; border-bottom:1px solid var(--nv-line); vertical-align:middle;}
table.nv-table tr:hover td{background: rgba(0,0,0,.02);}
table.nv-table td.num{text-align:right; font-variant-numeric: tabular-nums;}
</style>
"""

EXTRA_UI_CSS = ""

def apply_global_css() -> None:
    try: st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    except Exception: pass
