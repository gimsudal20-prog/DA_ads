import os
import re
import io
import math
import numpy as np
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import streamlit as st
import altair as alt

from db import _HASH_FUNCS

# -----------------------------
# Streamlit helpers & Optionals
# -----------------------------
_ST_DATAFRAME = st.dataframe
def st_dataframe_safe(df, **kwargs):
    try:
        return _ST_DATAFRAME(df, **kwargs)
    except Exception:
        kwargs.pop("hide_index", None)
        try:
            return _ST_DATAFRAME(df, **kwargs)
        except Exception:
            kwargs.pop("column_config", None)
            return _ST_DATAFRAME(df, **kwargs)

try:
    import streamlit_shadcn_ui as ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None
    HAS_SHADCN_UI = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
    try:
        from st_aggrid.shared import GridUpdateMode, DataReturnMode
    except Exception:
        GridUpdateMode = None
        DataReturnMode = None
    HAS_AGGRID = True
except Exception:
    AgGrid = None
    GridOptionsBuilder = None
    JsCode = None
    HAS_AGGRID = False

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

def _aggrid_mode(name: str):
    if name == "no_update":
        return GridUpdateMode.NO_UPDATE if 'GridUpdateMode' in globals() and GridUpdateMode is not None else "NO_UPDATE"
    if name == "as_input":
        return DataReturnMode.AS_INPUT if 'DataReturnMode' in globals() and DataReturnMode is not None else "AS_INPUT"
    return None

_AGGRID_COLDEF_CACHE: dict = {}

def _aggrid_coldefs(cols: List[str], right_cols: set, enable_filter: bool) -> list:
    key = (tuple(cols), tuple(sorted(right_cols)), int(bool(enable_filter)))
    cache = _AGGRID_COLDEF_CACHE
    if key in cache:
        return cache[key]
    out = []
    for c in cols:
        cd = {"headerName": c, "field": c, "sortable": True, "filter": bool(enable_filter), "resizable": True}
        if c in right_cols:
            cd["cellStyle"] = {"textAlign": "right"}
        out.append(cd)
    if len(cache) > 64:
        cache.clear()
    cache[key] = out
    return out

def _aggrid_grid_options(cols: List[str], pinned_rows: Optional[list] = None, right_cols: Optional[set] = None, quick_filter: str = "", enable_filter: bool = False) -> dict:
    right_cols = right_cols or set()
    pinned_rows = pinned_rows or []
    grid = {
        "defaultColDef": {"sortable": True, "filter": bool(enable_filter), "resizable": True},
        "columnDefs": _aggrid_coldefs(cols, right_cols, enable_filter),
        "pinnedTopRowData": pinned_rows,
        "suppressRowClickSelection": True,
        "animateRows": False,
    }
    if quick_filter:
        grid["quickFilterText"] = quick_filter
    if JsCode is not None:
        try:
            grid["getRowStyle"] = JsCode("""
            function(params){
              if(params.node.rowPinned){
                return {backgroundColor:'rgba(148,163,184,0.18)', fontWeight:'700'};
              }
              return {};
            }
            """)
        except Exception:
            pass
    return grid

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

def _altair_dashline_theme():
    return {
        "config": {
            "background": "transparent",
            "view": {"stroke": "transparent"},
            "axis": {
                "gridColor": "#EBEEF2",
                "gridOpacity": 1,
                "domain": False,
                "labelColor": "#475569",
                "titleColor": "#0f172a",
                "tickColor": "#CBD5E1",
            },
            "legend": {"labelColor": "#475569", "titleColor": "#0f172a"},
            "range": {"category": ["#0528F2", "#056CF2", "#3D9DF2", "#B4C4D9"]},
        }
    }

try:
    alt.themes.register("dashline", _altair_dashline_theme)
    alt.themes.enable("dashline")
except Exception:
    pass

BUILD_TAG = "v8.6.11 (Bootstrap Settings+Sync+Speed Hotfix, 2026-02-20)"

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");
:root{
  --nv-bg:#F5F6F7; --nv-panel:#FFFFFF; --nv-line:rgba(0,0,0,.08); --nv-line2:rgba(0,0,0,.12);
  --nv-text:#1A1C20; --nv-muted:rgba(26,28,32,.62); --nv-green:#03C75A;
  --nv-up:#EF4444; --nv-blue:#2563EB; --nv-red:#EF4444;
  --nv-shadow:0 2px 10px rgba(0,0,0,.06); --nv-radius:10px;
}
#MainMenu, footer {visibility:hidden;}
header[data-testid="stHeader"]{height:0px;}
div[data-testid="stToolbar"]{visibility:hidden;height:0;}
html, body, [data-testid="stAppViewContainer"]{
  background: var(--nv-bg) !important;
  font-family: Pretendard, sans-serif;
  color: var(--nv-text);
}
section[data-testid="stSidebar"]{
  background: var(--nv-panel) !important;
  border-right: 1px solid var(--nv-line);
}
section[data-testid="stSidebar"] .stMarkdown, 
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div{font-size: 13px;}
section[data-testid="stSidebar"] .block-container{padding-top: 10px !important;}
[data-testid="stSidebarCollapsedControl"]{
  display: flex; align-items: center; justify-content: center;
  width: 38px; height: 38px; border-radius: 10px; border: 1px solid var(--nv-line);
  background: rgba(255,255,255,.86); box-shadow: var(--nv-shadow);
  position: fixed; top: 10px; left: 10px; z-index: 10001;
}
@media (min-width: 900px){
  section[data-testid="stSidebar"]{
    transform: translateX(0) !important; margin-left: 0 !important;
    min-width: 260px !important; width: 260px !important;
  }
  section[data-testid="stSidebar"][aria-expanded="false"]{
    transform: translateX(0) !important; min-width: 260px !important; width: 260px !important;
  }
  section[data-testid="stSidebar"] > div:first-child{width: 260px !important;}
}
.main .block-container{padding-top: 14px !important; padding-bottom: 40px !important; max-width: 1320px;}
.nv-topbar{
  position: sticky; top: 0; z-index: 999;
  background: rgba(245,246,247,.86); backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--nv-line); padding: 10px 0 10px 0; margin: -14px -1px 12px -1px;
}
.nv-topbar .inner{
  max-width: 1600px; margin: 0 auto; display:flex; align-items:center; justify-content:space-between; padding: 0 6px;
}
.nv-brand{display:flex; align-items:center; gap:10px; font-weight: 800; font-size: 16px;}
.nv-dot{width:10px;height:10px;border-radius:50%; background: var(--nv-green); box-shadow: 0 0 0 3px rgba(3,199,90,.14);}
.nv-sub{font-weight: 600; font-size: 12px; color: var(--nv-muted);}
.nv-pill{
  display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px;
  background: var(--nv-panel); border: 1px solid var(--nv-line); box-shadow: var(--nv-shadow);
  font-size: 12px; color: var(--nv-muted); height: 38px;
}
.nv-h1{font-size:20px;font-weight:900;color:var(--nv-text);margin:4px 0 0 0;}
.nv-panel{background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: var(--nv-radius); box-shadow: var(--nv-shadow);}
.nv-panel .hd{padding: 12px 14px; border-bottom: 1px solid var(--nv-line); display:flex; align-items:center; justify-content:space-between;}
.nv-panel .hd .t{font-size: 14px; font-weight: 800;}
.nv-panel .bd{padding: 12px 14px;}
.nv-sec-title{font-size: 16px; font-weight: 900; margin: 8px 0 8px 0; letter-spacing: -0.2px;}
div[data-baseweb="select"] > div{min-height: 38px !important;}
input[type="text"], textarea{min-height: 38px !important;}
.kpi-row{display:grid; grid-template-columns: repeat(6, 1fr); gap: 10px;}
.kpi{background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: 10px; padding: 10px 12px;}
.kpi .k{font-size:12px;color:var(--nv-muted);font-weight:700;}
.kpi .v{margin-top:4px;font-size:18px;font-weight:900;letter-spacing:-.2px;}
.kpi .d{margin-top:6px;font-size:12px;font-weight:800;display:flex;align-items:center;gap:6px;}
.kpi .d.pos{color:var(--nv-red);}
.kpi .d.neg{color:var(--nv-blue);}
.kpi .chip{font-size:11px; padding:2px 6px; border-radius:999px; border:1px solid var(--nv-line); color:var(--nv-muted);}
.delta-chip-row{display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin: 10px 0 14px 0;}
.delta-chip{background: var(--nv-panel); border: 1px solid var(--nv-line); border-radius: 12px; padding: 10px 12px; box-shadow: 0 1px 6px rgba(0,0,0,.04);}
.delta-chip .l{font-size: 12px; color: var(--nv-muted); font-weight: 800;}
.delta-chip .v{margin-top: 6px; font-size: 14px; font-weight: 900; letter-spacing: -0.15px;}
.delta-chip .v .arr{display:inline-block; width: 18px; font-weight: 900;}
.delta-chip .v .p{font-weight: 800; color: var(--nv-muted); margin-left: 4px;}
.delta-chip.pos .v{color: var(--nv-red);}
.delta-chip.neg .v{color: var(--nv-blue);}
.delta-chip.zero .v{color: rgba(26,28,32,.72);}
@media (max-width: 1200px){.delta-chip-row{grid-template-columns: repeat(2, minmax(0, 1fr));}}
div[role="radiogroup"] > label{border: 1px solid var(--nv-line); background: var(--nv-panel); border-radius: 8px; padding: 6px 10px; margin-right: 6px;}
div[role="radiogroup"] > label:hover{border-color: var(--nv-line2);}
[data-testid="stDataFrame"]{border: 1px solid var(--nv-line); border-radius: 10px; overflow: hidden;}
[data-testid="stDataFrame"] *{font-size: 12px !important;}
.stButton > button{border-radius: 8px; border: 1px solid var(--nv-line); background: var(--nv-panel); padding: 6px 10px; font-weight: 800;}
.stButton > button:hover{border-color: var(--nv-line2);}
.stSelectbox, .stMultiSelect, .stTextInput, .stDateInput{font-size: 12px;}
section[data-testid="stSidebar"] div[role="radiogroup"]{gap:6px;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label{border: 0 !important; background: transparent !important; padding: 8px 12px !important; margin: 0 !important; border-radius: 10px !important; width: 100%;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover{background: rgba(0,0,0,.04) !important;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label > div:first-child{display:none !important;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label p{margin:0 !important; font-size: 13px !important; font-weight: 800 !important; color: var(--nv-text) !important;}
section[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked){background: rgba(3,199,90,.10) !important; border: 1px solid rgba(3,199,90,.24) !important;}
.nv-field{display:flex;flex-direction:column;gap:6px;min-width:0;}
.nv-lbl{font-size:12px;font-weight:800;color:var(--nv-muted);line-height:1;}
.nv-ro{height: 38px; display:flex; align-items:center; padding: 0 10px; border-radius: 8px; border: 1px solid var(--nv-line); background: #fff; color: var(--nv-text); font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;}
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
div[data-testid="stExpander"]{background: var(--nv-panel) !important; border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; overflow: hidden !important;}
div[data-testid="stExpander"] > details{border: 0 !important;}
div[data-testid="stExpander"] > details > summary{padding: 12px 14px !important; font-weight: 800 !important; color:
