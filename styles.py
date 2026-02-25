# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard.

- Keeps ALL CSS in one place (승훈 요청).
- app.py calls apply_global_css() once on each rerun.
"""

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
  --nv-shadow:0 2px 10px rgba(0,0,0,.06);
  --nv-radius:10px;
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

/* Force sidebar visible + sane width on desktop (avoid squished menu when Streamlit remembers collapsed) */
@media (min-width: 900px){
  section[data-testid="stSidebar"]{
    transform: translateX(0) !important;
    margin-left: 0 !important;
    min-width: 260px !important;
    width: 260px !important;
  }
  /* Some Streamlit versions keep aria-expanded="false" even when we force translateX(0) */
  section[data-testid="stSidebar"][aria-expanded="false"]{
    transform: translateX(0) !important;
    min-width: 260px !important;
    width: 260px !important;
  }
  section[data-testid="stSidebar"] > div:first-child{
    width: 260px !important;
  }
}
/* Main container spacing (compact) */
.main .block-container{
  padding-top: 14px !important;
  padding-bottom: 40px !important;
  max-width: 1600px;
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
  max-width: 1600px; margin: 0 auto;
  display:flex; align-items:center; justify-content:space-between;
  padding: 0 6px;
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
/* Panels */
.nv-panel{
  background: var(--nv-panel);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius);
  box-shadow: var(--nv-shadow);
}
.nv-panel .hd{
  padding: 12px 14px;
  border-bottom: 1px solid var(--nv-line);
  display:flex; align-items:center; justify-content:space-between;
}
.nv-panel .hd .t{
  font-size: 14px; font-weight: 800;
}
.nv-panel .bd{
  padding: 12px 14px;
}


/* Section title (compact, admin-like) */
.nv-sec-title{
  font-size: 16px;
  font-weight: 900;
  margin: 8px 0 8px 0;
  letter-spacing: -0.2px;
}

/* Slightly wider like admin */
.main .block-container{
  max-width: 1320px;
}

/* Compact pills to align with inputs */
.nv-pill{
  display:inline-flex;
  align-items:center;
  height: 38px;
  padding: 0 10px;
}

/* Compact select/text input heights (safe baseweb selectors) */
div[data-baseweb="select"] > div{
  min-height: 38px !important;
}
input[type="text"], textarea{
  min-height: 38px !important;
}

/* KPI row (Naver summary style) */
.kpi-row{
  display:grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 10px;
}
.kpi{
  background: var(--nv-panel);
  border: 1px solid var(--nv-line);
  border-radius: 10px;
  padding: 10px 12px;
}
.kpi .k{font-size:12px;color:var(--nv-muted);font-weight:700;}
.kpi .k .kpi-tip{margin-left:6px; font-size:12px; opacity:.55; cursor:help;}
.kpi .k .kpi-tip:hover{opacity:.9;}
.kpi .v{margin-top:4px;font-size:18px;font-weight:900;letter-spacing:-.2px;}
.kpi .d{margin-top:6px;font-size:12px;font-weight:800;display:flex;align-items:center;gap:6px;}
.kpi .d.pos{color:var(--nv-red);} /* 증가(▲) = 빨강(국내표준) */
.kpi .d.neg{color:var(--nv-blue);}   /* 감소(▼) = 파랑(국내표준) */
.kpi .chip{
  font-size:11px; padding:2px 6px; border-radius:999px;
  border:1px solid var(--nv-line); color:var(--nv-muted);
}

/* Delta chips (period compare) */
.delta-chip-row{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin: 10px 0 14px 0;
}
.delta-chip{
  background: var(--nv-panel);
  border: 1px solid var(--nv-line);
  border-radius: 12px;
  padding: 10px 12px;
  box-shadow: 0 1px 6px rgba(0,0,0,.04);
}
.delta-chip .l{
  font-size: 12px;
  color: var(--nv-muted);
  font-weight: 800;
}
.delta-chip .v{
  margin-top: 6px;
  font-size: 14px;
  font-weight: 900;
  letter-spacing: -0.15px;
}
.delta-chip .v .arr{display:inline-block; width: 18px; font-weight: 900;}
.delta-chip .v .p{font-weight: 800; color: var(--nv-muted); margin-left: 4px;}
.delta-chip.pos .v{color: var(--nv-red);} /* 증가 = 빨강(국내표준) */
.delta-chip.neg .v{color: var(--nv-blue);}   /* 감소 = 파랑(국내표준) */
.delta-chip.zero .v{color: rgba(26,28,32,.72);} 
@media (max-width: 1200px){
  .delta-chip-row{grid-template-columns: repeat(2, minmax(0, 1fr));}
}

/* Tab strip look */
div[role="radiogroup"] > label{
  border: 1px solid var(--nv-line);
  background: var(--nv-panel);
  border-radius: 8px;
  padding: 6px 10px;
  margin-right: 6px;
}
div[role="radiogroup"] > label:hover{border-color: var(--nv-line2);}

/* Dataframe table compact */
[data-testid="stDataFrame"]{
  border: 1px solid var(--nv-line);
  border-radius: 10px;
  overflow: hidden;
}
[data-testid="stDataFrame"] *{
  font-size: 12px !important;
}

/* Buttons -> compact */
.stButton > button{
  border-radius: 8px;
  border: 1px solid var(--nv-line);
  background: var(--nv-panel);
  padding: 6px 10px;
  font-weight: 800;
}
.stButton > button:hover{
  border-color: var(--nv-line2);
}

/* Inputs compact */
.stSelectbox, .stMultiSelect, .stTextInput, .stDateInput{
  font-size: 12px;
}

/* ---- Fix: Sidebar radio should look like nav list (no circles, no pills) ---- */
section[data-testid="stSidebar"] div[role="radiogroup"]{gap:6px;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label{
  border: 0 !important;
  background: transparent !important;
  padding: 8px 12px !important;
  margin: 0 !important;
  border-radius: 10px !important;
  width: 100%;
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover{
  background: rgba(0,0,0,.04) !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label > div:first-child{
  display:none !important; /* hide radio circle */
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label p{
  margin:0 !important;
  font-size: 13px !important;
  font-weight: 800 !important;
  color: var(--nv-text) !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked){
  background: rgba(3,199,90,.10) !important;
  border: 1px solid rgba(3,199,90,.24) !important;
}

/* ---- Fix: '기간'에서 자동 계산 시 날짜가 박스 밖으로 튀어나오는 문제 ---- */
.nv-field{display:flex;flex-direction:column;gap:6px;min-width:0;}
.nv-lbl{font-size:12px;font-weight:800;color:var(--nv-muted);line-height:1;}
.nv-ro{
  height: 38px;
  display:flex; align-items:center;
  padding: 0 10px;
  border-radius: 8px;
  border: 1px solid var(--nv-line);
  background: #fff;
  color: var(--nv-text);
  font-size: 13px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}


/* ---- Progress bar cell (월 예산 집행률) ---- */
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

EXTRA_UI_CSS = """

"""


def apply_global_css() -> None:
    """Inject global CSS (safe to call multiple times)."""
    try:
        st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    except Exception:
        pass
    if EXTRA_UI_CSS.strip():
        try:
            st.markdown(EXTRA_UI_CSS, unsafe_allow_html=True)
        except Exception:
            pass