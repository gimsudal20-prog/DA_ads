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
div[data-testid="stExpander"] > details > summary{padding: 12px 14px !important; font-weight: 800 !important; color: var(--nv-text) !important; background: #fff !important;}
div[data-testid="stExpander"] > details > summary svg{ display:none !important; }
div[data-testid="stExpander"] > details > div{padding: 12px 14px 14px 14px !important; border-top: 1px solid var(--nv-line) !important; background: #fff !important;}
div[data-testid="stTextInput"] input[disabled]{background: #F3F4F6 !important; color: var(--nv-text) !important; border: 1px solid var(--nv-line) !important;}
div[data-testid="stSidebar"] [data-testid="stRadio"] svg{ display:none !important; }
div[data-testid="stSidebar"] [data-testid="stRadio"] label{ padding-left: 10px !important; }
</style>
"""

def render_hero(latest: dict, build_tag: str = BUILD_TAG) -> None:
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    latest = latest or {}
    def _dt(key_a: str, key_b: str) -> str:
        v = latest.get(key_a) or latest.get(key_b) or "—"
        try:
            if isinstance(v, (pd.Timestamp,)): v = v.to_pydatetime()
        except Exception: pass
        if isinstance(v, (datetime, date)): v = v.strftime("%Y-%m-%d")
        v = "—" if v is None else str(v)
        return v.strip()

    camp = _dt("campaign_dt", "campaign")
    kw = _dt("keyword_dt", "keyword")
    ad = _dt("ad_dt", "ad")
    biz = _dt("bizmoney_dt", "bizmoney")

    st.markdown(
        f"""
        <div class="nv-topbar">
          <div class="inner">
            <div>
              <div class="nv-brand"><span class="nv-dot"></span>네이버 검색광고 리포트</div>
              <div class="nv-sub">{build_tag}</div>
            </div>
            <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
              <span class="nv-pill">캠페인 최신 · <b>{camp}</b></span>
              <span class="nv-pill">키워드 최신 · <b>{kw}</b></span>
              <span class="nv-pill">소재 최신 · <b>{ad}</b></span>
              <span class="nv-pill">비즈머니 최신 · <b>{biz}</b></span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_timeseries_chart(ts: pd.DataFrame, entity: str = "campaign", key_prefix: str = "") -> None:
    if ts is None or ts.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    df = ts.copy()
    if "dt" in df.columns:
        dt = pd.to_datetime(df["dt"], errors="coerce")
        df["dt"] = dt.dt.strftime("%Y-%m-%d")
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if "imp" in df.columns and "clk" in df.columns:
        denom = df["imp"].replace(0, np.nan)
        df["ctr"] = pd.to_numeric((df["clk"] / denom * 100.0), errors="coerce").fillna(0.0)
    if "clk" in df.columns and "cost" in df.columns:
        denom = df["clk"].replace(0, np.nan)
        df["cpc"] = pd.to_numeric((df["cost"] / denom), errors="coerce").fillna(0.0)
    if "conv" in df.columns and "cost" in df.columns:
        denom = df["conv"].replace(0, np.nan)
        df["cpa"] = pd.to_numeric((df["cost"] / denom), errors="coerce").fillna(0.0)
    if "cost" in df.columns and "sales" in df.columns:
        denom = df["cost"].replace(0, np.nan)
        df["roas"] = pd.to_numeric((df["sales"] / denom * 100.0), errors="coerce").fillna(0.0)

    order = [c for c in ["dt", "imp", "clk", "ctr", "cpc", "cost", "conv", "cpa", "sales", "roas"] if c in df.columns]
    view = df[order].copy().rename(columns={"dt": "일자", "imp": "노출", "clk": "클릭", "ctr": "CTR(%)", "cpc": "CPC", "cost": "광고비", "conv": "전환", "cpa": "CPA", "sales": "매출", "roas": "ROAS(%)"})
    
    disp = pd.DataFrame()
    if "일자" in view.columns: disp["일자"] = view["일자"].astype(str)
    for col in ["노출", "클릭", "전환"]:
        if col in view.columns: disp[col] = view[col].apply(lambda x: f"{int(round(float(x))):,}" if pd.notna(x) else "0")
    if "CTR(%)" in view.columns: disp["CTR(%)"] = view["CTR(%)"].apply(lambda x: f"{float(x):.1f}%" if pd.notna(x) else "0.0%")
    if "CPC" in view.columns: disp["CPC"] = view["CPC"].apply(lambda x: f"{int(round(float(x))):,}원" if pd.notna(x) else "0원")
    if "광고비" in view.columns: disp["광고비"] = view["광고비"].apply(lambda x: f"{int(round(float(x))):,}원" if pd.notna(x) else "0원")
    if "CPA" in view.columns: disp["CPA"] = view["CPA"].apply(lambda x: f"{int(round(float(x))):,}원" if pd.notna(x) else "0원")
    if "매출" in view.columns: disp["매출"] = view["매출"].apply(lambda x: f"{int(round(float(x))):,}원" if pd.notna(x) else "0원")
    if "ROAS(%)" in view.columns: disp["ROAS(%)"] = view["ROAS(%)"].apply(lambda x: f"{float(x):.0f}%" if pd.notna(x) else "0%")
    st_dataframe_safe(disp, use_container_width=True, hide_index=True, height=360)

def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    use_shadcn = os.getenv("USE_SHADCN_METRICS", "0").strip() == "1"
    if use_shadcn and HAS_SHADCN_UI and ui is not None:
        try:
            ui.metric_card(title=title, content=value, description=desc, key=key)
            return
        except Exception: pass
    label = (desc or "").strip()
    delta_html = f"<div class='d'><span class='chip'>{label}</span></div>" if label else "<div class='d'></div>"
    m = re.search(r"([+-])\s*([0-9]+(?:\.[0-9]+)?)\s*%", label)
    if m:
        sign, num = m.group(1), m.group(2)
        arrow, cls = ("▲", "pos") if sign == "+" else ("▼", "neg")
        label2 = (label.replace(m.group(0), "").replace("  ", " ").strip()) or ""
        chip = f"<span class='chip'>{label2}</span>" if label2 else ""
        delta_html = f"<div class='d {cls}'>{chip}{arrow} {num}%</div>"
    st.markdown(f"<div class='kpi' id='{key}'><div class='k'>{title}</div><div class='v'>{value}</div>{delta_html}</div>", unsafe_allow_html=True)

def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.table(df, maxHeight=height, key=key)
            return
        except Exception: pass
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)

def render_budget_month_table_with_bars(table_df: pd.DataFrame, key: str, height: int = 520) -> None:
    if table_df is None or table_df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    df = table_df.copy()
    def _bar(pct, status) -> str:
        try: pv = float(pct)
        except Exception: pv = 0.0
        pv = 0.0 if math.isnan(pv) else pv
        width = max(0.0, min(pv, 120.0))
        stt = str(status or "")
        fill = "var(--nv-red)" if stt.startswith("🔴") else "#F59E0B" if stt.startswith("🟡") else "var(--nv-green)" if stt.startswith("🟢") else "rgba(0,0,0,.25)"
        return f"<div class='nv-pbar'><div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{width:.2f}%;background:{fill};'></div></div><div class='nv-pbar-txt'>{pv:.1f}%</div></div>"
    
    if "집행률(%)" in df.columns:
        df["집행률"] = [_bar(p, s) for p, s in zip(df["집행률(%)"].tolist(), df.get("상태", "").tolist())]
        df = df.drop(columns=["집행률(%)"])
        cols = list(df.columns)
        if "상태" in cols and "집행률" in cols:
            cols.remove("집행률")
            cols.insert(cols.index("상태"), "집행률")
            df = df[cols]
    html = df.to_html(index=False, escape=False, classes="nv-table")
    html = re.sub(r"<td>([\d,]+원)</td>", r"<td class='num'>\1</td>", html)
    html = re.sub(r"<td>([\d,]+)</td>", r"<td class='num'>\1</td>", html)
    st.markdown(f"<div class='nv-table-wrap' style='max-height:{height}px'>{html}</div>", unsafe_allow_html=True)

def render_pinned_summary_grid(detail_df: pd.DataFrame, summary_df: Optional[pd.DataFrame], key: str, height: int = 520) -> None:
    if detail_df is None: detail_df = pd.DataFrame()
    if summary_df is None: summary_df = pd.DataFrame()
    if not summary_df.empty and list(summary_df.columns) != list(detail_df.columns):
        summary_df = summary_df.reindex(columns=list(detail_df.columns))
    
    if HAS_AGGRID and AgGrid is not None:
        pinned = summary_df.to_dict("records") if summary_df is not None and not summary_df.empty else []
        right_cols = {"노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"}
        grid = _aggrid_grid_options(cols=list(detail_df.columns), pinned_rows=pinned, right_cols=right_cols, enable_filter=False)
        AgGrid(detail_df, gridOptions=grid, height=height, fit_columns_on_grid_load=False, theme="alpine", allow_unsafe_jscode=True, update_mode=_aggrid_mode("no_update"), data_return_mode=_aggrid_mode("as_input"), key=key)
        return

    if summary_df is not None and not summary_df.empty:
        st_dataframe_safe(style_summary_rows(summary_df, len(summary_df)), use_container_width=True, hide_index=True, height=min(220, 60 + 35 * len(summary_df)))
    st_dataframe_safe(detail_df, use_container_width=True, hide_index=True, height=height)

def render_echarts_donut(title: str, data: pd.DataFrame, label_col: str, value_col: str, height: int = 260) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if data is None or data.empty or label_col not in data.columns or value_col not in data.columns: return
    d = data.copy()
    d[label_col] = d[label_col].astype(str)
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)
    items = [{"name": n, "value": float(v)} for n, v in zip(d[label_col].tolist(), d[value_col].tolist()) if float(v) > 0]
    if not items: return
    option = {
        "title": {"text": title, "left": "center", "top": 6, "textStyle": {"fontSize": 13}},
        "tooltip": {"trigger": "item", "formatter": "{b}<br/>{c:,} ({d}%)"},
        "legend": {"type": "scroll", "bottom": 0},
        "series": [{"name": title, "type": "pie", "radius": ["55%", "78%"], "avoidLabelOverlap": True, "itemStyle": {"borderRadius": 10, "borderColor": "#fff", "borderWidth": 2}, "label": {"show": False}, "emphasis": {"label": {"show": True, "fontSize": 13, "fontWeight": "bold"}}, "labelLine": {"show": False}, "data": items}],
    }
    st_echarts(option, height=f"{height}px")

def render_echarts_line(title: str, ts: pd.DataFrame, x_col: str, y_col: str, y_name: str, *, height: int = 260, smooth: bool = True) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if ts is None or ts.empty or x_col not in ts.columns or y_col not in ts.columns: return
    df = ts[[x_col, y_col]].copy()
    if np.issubdtype(df[x_col].dtype, np.datetime64): df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
    else:
        try: df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
        except Exception: df[x_col] = df[x_col].astype(str)
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce").fillna(0.0).round(0)
    x = df[x_col].astype(str).tolist()
    y = df[y_col].astype(float).round(0).astype(int).tolist()
    option = {
        "title": {"show": False},
        "grid": {"left": 54, "right": 18, "top": 44, "bottom": 34},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": x, "axisTick": {"alignWithLabel": True}},
        "yAxis": {"type": "value", "name": y_name, "nameTextStyle": {"padding": [0, 0, 0, 6]}},
        "series": [{"type": "line", "data": y, "smooth": smooth, "showSymbol": False, "lineStyle": {"width": 3, "color": "#2563EB"}, "areaStyle": {"opacity": 0.06}}],
    }
    st_echarts(option, height=f"{int(height)}px")

def render_echarts_delta_bars(delta_df: pd.DataFrame, *, height: int = 260) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if delta_df is None or delta_df.empty or "metric" not in delta_df.columns or "change_pct" not in delta_df.columns: return
    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["v"] = pd.to_numeric(d["change_pct"], errors="coerce")
    if d["v"].notna().sum() == 0:
        st.info("비교기간 데이터가 없어 증감율을 계산할 수 없습니다.")
        return
    d["v"] = d["v"].fillna(0.0).round(0).astype(int)
    lim = max(float(max(d["v"].abs().max(), 1.0)) * 1.15 + 0.5, 5.0)
    cats, vals = d["metric"].tolist()[::-1], d["v"].tolist()[::-1]
    data = []
    for m, v in zip(cats, vals):
        if v > 0: color, br, pos, fmt = "#2563EB", [0, 10, 10, 0], "right", f"+{int(round(v))}%"
        elif v < 0: color, br, pos, fmt = "#EF4444", [10, 0, 0, 10], "left", f"{int(round(v))}%"
        else: color, br, pos, fmt = "#B4C4D9", [0, 0, 0, 0], "right", "+0%"
        data.append({"value": v, "label": {"show": True, "position": pos, "formatter": fmt, "fontWeight": "bold"}, "itemStyle": {"color": color, "borderRadius": br}})
    option = {
        "grid": {"left": 70, "right": 24, "top": 12, "bottom": 26},
        "xAxis": {"type": "value", "min": -lim, "max": lim, "axisLabel": {"formatter": "{value}"}, "splitLine": {"lineStyle": {"color": "#EBEEF2"}}},
        "yAxis": {"type": "category", "data": cats, "axisTick": {"show": False}},
        "series": [{"type": "bar", "data": data, "barWidth": 20, "silent": True, "markLine": {"symbol": "none", "label": {"show": False}, "lineStyle": {"color": "#CBD5E1", "width": 2}, "data": [{"xAxis": 0}]}}],
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
    }
    st_echarts(option, height=f"{int(height)}px")

def render_big_table(df: pd.DataFrame, key: str, height: int = 560) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_AGGRID and AgGrid is not None:
        q = st.text_input("검색", value="", placeholder="테이블 내 검색", key=f"{key}_q")
        right_cols = {c for c in df.columns if any(k in c for k in ["노출", "클릭", "광고비", "전환", "매출", "CTR", "CPC", "CPA", "ROAS"])}
        grid = _aggrid_grid_options(cols=list(df.columns), pinned_rows=[], right_cols=right_cols, quick_filter=q or "", enable_filter=True)
        AgGrid(df, gridOptions=grid, height=height, fit_columns_on_grid_load=False, theme="alpine", allow_unsafe_jscode=True, update_mode=_aggrid_mode("no_update"), data_return_mode=_aggrid_mode("as_input"), key=key)
        return
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)

def _df_json_to_csv_bytes(df_json: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    return df.to_csv(index=False).encode("utf-8-sig")

@st.cache_data(hash_funcs=_HASH_FUNCS, show_spinner=False)
def _df_json_to_xlsx_bytes(df_json: str, sheet_name: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    if df is None or df.empty: return
    df_json = df.to_json(orient="split")
    st.markdown("<style>.stDownloadButton button {padding: 0.15rem 0.55rem !important; font-size: 0.82rem !important; line-height: 1.2 !important; min-height: 28px !important;}</style>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 8])
    with c1: st.download_button("CSV", data=_df_json_to_csv_bytes(df_json), file_name=f"{filename_base}.csv", mime="text/csv", key=f"{key_prefix}_csv", use_container_width=True)
    with c2: st.download_button("XLSX", data=_df_json_to_xlsx_bytes(df_json, sheet_name), file_name=f"{filename_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"{key_prefix}_xlsx", use_container_width=True)
    with c3: st.caption("다운로드")

def _safe_int(x, default: int = 0) -> int:
    try:
        if pd.isna(x) or x == "": return default
        return int(float(x))
    except Exception: return default

def format_currency(val) -> str: return f"{_safe_int(val):,}원"
def format_number_commas(val) -> str: return f"{_safe_int(val):,}"
def format_roas(val) -> str:
    try:
        if pd.isna(val): return "-"
        return f"{float(val):.0f}%"
    except Exception: return "-"

def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns: return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")
    out[col] = s.map(lambda x: "" if pd.isna(x) else ("0%" if float(x) == 0.0 else f"{float(x):.1f}%"))
    return out

def _safe_div(a: float, b: float) -> float:
    try:
        if b == 0: return 0.0
        return float(a) / float(b)
    except Exception: return 0.0

def finalize_display_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    out = df.copy()
    def _num(s):
        try: return pd.to_numeric(s, errors="coerce")
        except Exception: return pd.Series([None] * len(out))
    if "노출" in out.columns and "클릭" in out.columns and "CTR(%)" not in out.columns:
        out["CTR(%)"] = _safe_div(_num(out["클릭"]), _num(out["노출"])) * 100.0
    if "광고비" in out.columns and "클릭" in out.columns and "CPC(원)" not in out.columns:
        out["CPC(원)"] = _safe_div(_num(out["광고비"]), _num(out["클릭"]))
    if "광고비" in out.columns and "전환" in out.columns and "CPA(원)" not in out.columns:
        out["CPA(원)"] = _safe_div(_num(out["광고비"]), _num(out["전환"]))
    if "매출" in out.columns and "광고비" in out.columns and "ROAS(%)" not in out.columns and "ROAS" not in out.columns:
        out["ROAS(%)"] = _safe_div(_num(out["매출"]), _num(out["광고비"])) * 100.0
    for col in ["노출", "클릭", "전환"]:
        if col in out.columns:
            if pd.api.types.is_numeric_dtype(out[col]): out[col] = out[col].fillna(0).round(0)
            out[col] = out[col].apply(format_number_commas)
    for col in ["광고비", "매출", "CPC(원)", "CPA(원)"]:
        if col in out.columns:
            if not pd.api.types.is_numeric_dtype(out[col]): out[col] = pd.to_numeric(out[col], errors="coerce")
            out[col] = out[col].apply(format_currency)
    if "CTR(%)" in out.columns: out = finalize_ctr_col(out, "CTR(%)")
    if "ROAS(%)" in out.columns: out["ROAS(%)"] = pd.to_numeric(out["ROAS(%)"], errors="coerce").apply(format_roas)
    if "ROAS" in out.columns: out["ROAS"] = pd.to_numeric(out["ROAS"], errors="coerce").apply(format_roas)
    return out

def build_campaign_summary_rows_from_numeric(df_numeric: pd.DataFrame, campaign_type_col: str = "campaign_type", campaign_name_col: str = "campaign_name") -> pd.DataFrame:
    if df_numeric is None or df_numeric.empty: return pd.DataFrame()
    x = df_numeric.copy()
    if campaign_type_col not in x.columns: return pd.DataFrame()
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c not in x.columns: x[c] = 0
        x[c] = pd.to_numeric(x[c], errors="coerce").fillna(0)
    x[campaign_type_col] = x[campaign_type_col].fillna("").astype(str).str.strip()
    x = x[x[campaign_type_col] != ""].copy()
    if x.empty: return pd.DataFrame()
    if "campaign_id" in x.columns and "customer_id" in x.columns: x["_camp_key"] = x["customer_id"].astype(str) + ":" + x["campaign_id"].astype(str)
    else: x["_camp_key"] = x.get(campaign_name_col, "").astype(str)

    def _make_row(label_type: str, g: pd.DataFrame) -> dict:
        n, imp, clk, cost = int(g["_camp_key"].nunique()), float(g["imp"].sum()), float(g["clk"].sum()), float(g["cost"].sum())
        conv, sales = float(g["conv"].sum()), float(g["sales"].sum()) if "sales" in g.columns else 0.0
        ctr = (clk / imp * 100.0) if imp > 0 else 0.0
        cpc = (cost / clk) if clk > 0 else 0.0
        cpa = (cost / conv) if conv > 0 else 0.0
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        return {"업체명": "", "담당자": "", "광고유형": label_type, "캠페인": f"캠페인 {n}개 결과", "노출": int(imp), "클릭": int(clk), "CTR(%)": float(ctr), "CPC": format_currency(cpc), "광고비": format_currency(cost), "전환": int(conv), "CPA": format_currency(cpa), "전환매출": format_currency(sales), "ROAS(%)": format_roas(roas)}

    rows = [_make_row("종합", x)]
    for tp, g in x.groupby(campaign_type_col, dropna=False):
        rows.append(_make_row(str(tp).strip() or "기타", g))
    out = pd.DataFrame(rows)
    out["CTR(%)"] = pd.to_numeric(out["CTR(%)"], errors="coerce").fillna(0).astype(float)
    out = finalize_ctr_col(out, "CTR(%)")
    return out[["업체명", "담당자", "광고유형", "캠페인", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]].copy()

def style_summary_rows(df_view: pd.DataFrame, summary_rows: int):
    if df_view is None or df_view.empty or summary_rows <= 0: return df_view
    summary_idx = set(range(int(summary_rows)))
    def _style_row(row): return ["font-weight:700; background-color: rgba(148,163,184,0.18);"] * len(row) if row.name in summary_idx else [""] * len(row)
    try: return df_view.style.apply(_style_row, axis=1)
    except Exception: return df_view

def parse_currency(val_str) -> int:
    if pd.isna(val_str): return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0

def ui_badges_or_html(items, key_prefix: str = "") -> None:
    pills = [f"<div class='pill'><span class='dot on'></span>{label}: {str(value) if value is not None else '—'}</div>" for label, value in items]
    html = "<div class='freshness-pills'>" + "\n".join(pills) + "</div>"
    st.markdown("\n".join([ln.strip() for ln in html.splitlines() if ln.strip()]), unsafe_allow_html=True)

def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    try: return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception: return col.multiselect(label, options, default=default, key=key)

def _chart_timeseries(df: pd.DataFrame, y_col: str, y_title: str = "", *, x_col: str = "dt", y_format: str = ",.0f", height: int = 320):
    if df is None or df.empty or x_col not in df.columns or y_col not in df.columns: return None
    d = df[[x_col, y_col]].copy()
    d[x_col] = pd.to_datetime(d[x_col], errors="coerce")
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
    d = d.dropna(subset=[x_col]).sort_values(x_col).reset_index(drop=True)
    wk = ["월", "화", "수", "목", "금", "토", "일"]
    d["_wk"] = d[x_col].dt.weekday.map(lambda i: wk[int(i)] if pd.notna(i) else "")
    d["_x_label"] = d[x_col].dt.strftime("%m/%d") + "(" + d["_wk"] + ")"
    d["_dt_str"] = d[x_col].dt.strftime("%Y-%m-%d") + " (" + d["_wk"] + ")"
    title = (y_title or "").strip()
    y_axis = alt.Axis(title=title if title else None, format=y_format, grid=True, gridColor="#EBEEF2")
    x_axis = alt.Axis(title=None, grid=False, labelAngle=0, labelOverlap="greedy")
    base = alt.Chart(d).encode(x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending"), axis=x_axis), y=alt.Y(f"{y_col}:Q", axis=y_axis), tooltip=[alt.Tooltip("_dt_str:N", title="날짜"), alt.Tooltip(f"{y_col}:Q", title=title or y_col, format=y_format)])
    area = base.mark_area(interpolate="monotone", opacity=0.08)
    line = base.mark_line(interpolate="monotone", strokeWidth=3)
    pts = base.mark_point(size=40, filled=True)
    last = d.tail(1)
    last_label = alt.Chart(last).mark_text(align="left", dx=8, dy=-8).encode(x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending")), y=alt.Y(f"{y_col}:Q"), text=alt.Text(f"{y_col}:Q", format=y_format))
    return (area + line + pts + last_label).properties(height=int(height))

def _disambiguate_label(df: pd.DataFrame, base_col: str, parts: List[str], id_col: Optional[str] = None, max_len: int = 38) -> pd.Series:
    if df is None or df.empty or base_col not in df.columns: return pd.Series([], dtype=str)
    label = df[base_col].fillna("").astype(str)
    for p in parts:
        dup = label.duplicated(keep=False)
        if not bool(dup.any()): break
        if p in df.columns: label = label.where(~dup, (label + " · " + df[p].fillna("").astype(str)).str.strip())
    dup2 = label.duplicated(keep=False)
    if bool(dup2.any()):
        if id_col and id_col in df.columns: label = label + " #" + df[id_col].fillna("").astype(str).str[-4:]
        else: label = label + " #" + df.reset_index().index.astype(str)
    return label.astype(str).str.slice(0, int(max_len))

def _chart_progress_bars(df: pd.DataFrame, label_col: str, value_col: str, x_title: str = "", top_n: int = 10, height: int = 420):
    if df is None or df.empty: return None
    unit = "원" if ("원" in str(x_title)) else ("%" if ("%" in str(x_title)) else "")
    d = df[[label_col, value_col]].copy()
    d[label_col] = d[label_col].astype(str).map(str.strip)
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0)
    d = d.groupby(label_col, as_index=False)[value_col].sum().sort_values(value_col, ascending=False).head(int(top_n)).sort_values(value_col, ascending=True)
    vals = d[value_col].tolist()
    d["__max"] = float(max(vals)) if vals else 0.0
    def _fmt(v: float) -> str: return f"{format_number_commas(v)}원" if unit == "원" else (f"{v:.1f}%" if unit == "%" else f"{format_number_commas(v)}{unit}")
    d["__label"] = d[value_col].map(lambda v: _fmt(float(v)))
    y = alt.Y(f"{label_col}:N", sort=None, title=None, axis=alt.Axis(labelLimit=260, ticks=False))
    x_bg = alt.X("__max:Q", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False))
    x_fg = alt.X(f"{value_col}:Q", title=None, axis=alt.Axis(grid=False))
    base = alt.Chart(d).encode(y=y)
    bg = base.mark_bar(cornerRadiusEnd=10, opacity=0.25, color="#B4C4D9").encode(x=x_bg)
    fg = base.mark_bar(cornerRadiusEnd=10, color="#3D9DF2").encode(x=x_fg)
    txt_layer = base.mark_text(align="left", dx=6, dy=0).encode(x=alt.X(f"{value_col}:Q"), text="__label:N")
    return (bg + fg + txt_layer).properties(height=int(height))

def _pct_to_str(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)): return "—"
        return f"{float(p):+.1f}%"
    except Exception: return "—"

def _pct_to_arrow(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)): return "—"
        p = float(p)
        return f"▲ {abs(p):.1f}%" if p > 0 else (f"▼ {abs(p):.1f}%" if p < 0 else f"• {abs(p):.1f}%")
    except Exception: return "—"

def _fmt_point(p: Optional[float]) -> str:
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)): return "—"
        return f"{float(p):+.1f}p"
    except Exception: return "—"

def _chart_delta_bars(delta_df: pd.DataFrame, height: int = 260):
    if delta_df is None or delta_df.empty: return None
    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["change_pct"] = pd.to_numeric(d["change_pct"], errors="coerce").fillna(0)
    d["dir"] = d["change_pct"].apply(lambda x: "up" if x > 0 else ("down" if x < 0 else "flat"))
    d["label"] = d["change_pct"].map(_pct_to_str)
    y_sort = alt.SortField(field="order", order="descending") if "order" in d.columns else None
    if "order" in d.columns: d = d.sort_values("order", ascending=False)
    m_abs = max(abs(float(d["change_pct"].min())), abs(float(d["change_pct"].max())))
    m_abs = m_abs if m_abs > 0 else 5.0
    domain = [-(m_abs + max(2.0, m_abs * 0.12)), (m_abs + max(2.0, m_abs * 0.12))]
    flat = (d["change_pct"].abs() * 0.6).clip(lower=0.0, upper=2.0)
    d["flat_end"] = flat.where(d["change_pct"] >= 0, -flat)
    d["zero"] = 0.0
    d_main, d_cap = d.copy(), d.copy()
    d_main["val"], d_cap["val"] = d_main["change_pct"], d_cap["flat_end"]
    color_scale = alt.Scale(domain=["up", "down", "flat"], range=["#EF4444", "#2563EB", "#B4C4D9"])
    y_enc = alt.Y("metric:N", sort=y_sort, title=None, axis=alt.Axis(labelLimit=260))
    x_axis = alt.Axis(grid=True, gridColor="#EBEEF2")
    bars = alt.Chart(d_main).mark_bar(cornerRadius=10).encode(y=y_enc, x=alt.X("val:Q", title="증감율(%)", scale=alt.Scale(domain=domain), axis=x_axis), x2=alt.X2("zero:Q"), color=alt.Color("dir:N", scale=color_scale, legend=None), tooltip=[alt.Tooltip("metric:N", title="지표"), alt.Tooltip("change_pct:Q", title="증감율", format="+.1f")])
    cap = alt.Chart(d_cap).mark_bar(cornerRadius=0).encode(y=y_enc, x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None), x2=alt.X2("zero:Q"), color=alt.Color("dir:N", scale=color_scale, legend=None))
    zero = alt.Chart(pd.DataFrame({"val": [0.0]})).mark_rule(color="#CBD5E1").encode(x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None))
    pos_text = alt.Chart(d_main).transform_filter("datum.val >= 0").mark_text(align="left", dx=6).encode(y=y_enc, x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None), text="label:N", color=alt.Color("dir:N", scale=color_scale, legend=None))
    neg_text = alt.Chart(d_main).transform_filter("datum.val < 0").mark_text(align="right", dx=-6).encode(y=y_enc, x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None), text="label:N", color=alt.Color("dir:N", scale=color_scale, legend=None))
    return (bars + cap + zero + pos_text + neg_text).properties(height=int(height))

def render_chart(obj, *, height: int | None = None) -> None:
    if obj is None: return
    try: mod = obj.__class__.__module__
    except Exception: mod = ""
    if mod.startswith("altair"): st.altair_chart(obj, use_container_width=True)
    else:
        try: st.write(obj)
        except Exception: pass
