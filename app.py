# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.9.0)

✅ 이번 버전 핵심 (승훈 요청 반영)
- 체감 속도 개선(1초 내 목표): 불필요한 자동 동기화 제거 + 쿼리 수 최소화 + 다운로드(xlsx) 생성 캐시
- UI 개선(옵션2): streamlit-shadcn-ui 탭/메트릭카드/테이블 적용 (미설치 시 자동 폴백)
- iOS Safari 프론트 오류( TypeError: ... e[s].sticky ) 회피:
  * Streamlit 내부 DOM을 건드리던 data-testid 기반 CSS 제거
  * st.data_editor 제거(프론트 grid 의존도 낮춤) → 안정적인 폼 기반 예산 업데이트로 변경
- customer_id 타입 혼재(TEXT vs BIGINT) 안전:
  * 모든 fact/dim 조인/필터에서 customer_id::text 로 통일
  * IN 필터는 문자열 리터럴('420332')로 구성 (TEXT/BIGINT 모두 안전)

"""

from __future__ import annotations

import os
import time
import re
import io
import math
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# -----------------------------
# Streamlit helpers (compat)
# -----------------------------
_ST_DATAFRAME = st.dataframe
def st_dataframe_safe(df, **kwargs):
    """st.dataframe 호환성 래퍼: Streamlit 버전 차이로 인한 TypeError를 안전하게 폴백 처리."""
    try:
        return _ST_DATAFRAME(df, **kwargs)
    except Exception:
        # 1차 폴백: hide_index 제거
        kwargs.pop("hide_index", None)
        try:
            return _ST_DATAFRAME(df, **kwargs)
        except Exception:
            # 2차 폴백: column_config 제거(구버전)
            kwargs.pop("column_config", None)
            return _ST_DATAFRAME(df, **kwargs)

import altair as alt

# Charts: Altair (vega-lite)


# Optional UI components (shadcn-ui style)
try:
    import streamlit_shadcn_ui as ui  # pip install streamlit-shadcn-ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None  # type: ignore
    HAS_SHADCN_UI = False

# Optional grid component (AgGrid) - enables pinned top rows + stable sorting
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # pip install streamlit-aggrid
    try:
        from st_aggrid.shared import GridUpdateMode, DataReturnMode
    except Exception:
        GridUpdateMode = None  # type: ignore
        DataReturnMode = None  # type: ignore
    HAS_AGGRID = True
except Exception:
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore
    HAS_AGGRID = False

# Optional charts component (ECharts)
try:
    from streamlit_echarts import st_echarts  # pip install streamlit-echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None  # type: ignore
    HAS_ECHARTS = False


# -----------------------------
# AgGrid tuning: keep rich grid but avoid triggering reruns on sort/filter/scroll
# -----------------------------
def _aggrid_mode(name: str):
    """Return GridUpdateMode/DataReturnMode value across versions."""
    # st-aggrid versions differ: enums may be absent; string fallbacks are accepted.
    if name == "no_update":
        return GridUpdateMode.NO_UPDATE if 'GridUpdateMode' in globals() and GridUpdateMode is not None else "NO_UPDATE"
    if name == "as_input":
        return DataReturnMode.AS_INPUT if 'DataReturnMode' in globals() and DataReturnMode is not None else "AS_INPUT"
    return None


# -----------------------------
# AgGrid fast gridOptions (cache) - keeps features but avoids rebuilding GridOptionsBuilder every rerun
# -----------------------------
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

def _aggrid_grid_options(
    cols: List[str],
    pinned_rows: Optional[list] = None,
    right_cols: Optional[set] = None,
    quick_filter: str = "",
    enable_filter: bool = False,
) -> dict:
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

    # pinned row styling (grey summary)
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

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Streamlit cache hashing (Engine)
# -----------------------------
_HASH_FUNCS = {Engine: lambda e: e.url.render_as_string(hide_password=True)}

# Altair (charts)
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


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

BUILD_TAG = "v8.6.11 (Bootstrap Settings+Sync+Speed Hotfix, 2026-02-20)"

# -----------------------------
# Thresholds (Budget)
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

# -----------------------------
# Minimal CSS (avoid fragile DOM hooks)
# -----------------------------
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

st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)



def render_hero(latest: dict, build_tag: str = BUILD_TAG) -> None:
    """Naver-like topbar (sticky)."""
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    latest = latest or {}

    def _dt(key_a: str, key_b: str) -> str:
        v = latest.get(key_a) or latest.get(key_b) or "—"
        # dt는 date/datetime/Timestamp로 올 수 있어 .strip()이 터질 수 있음
        try:
            import pandas as _pd  # local import (optional)
            if isinstance(v, (_pd.Timestamp,)):
                v = v.to_pydatetime()
        except Exception:
            pass
        if isinstance(v, (datetime, date)):
            v = v.strftime("%Y-%m-%d")
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
    """기간 '추세' 표를 한글/가독성 좋게 렌더링.

    - dt/imp/clk/cost/conv/sales -> 한글 헤더
    - 날짜는 YYYY-MM-DD
    - 숫자는 콤마/단위(원, %, p) 적용
    - CTR/CPC/CPA/ROAS 보조지표 추가

    NOTE: 화면용(표시용) 문자열 컬럼을 만들어 보여줍니다.
    """
    if ts is None or ts.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    df = ts.copy()

    # --- normalize columns ---
    if "dt" in df.columns:
        dt = pd.to_datetime(df["dt"], errors="coerce")
        df["dt"] = dt.dt.strftime("%Y-%m-%d")

    # make sure numeric
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # derived metrics
    if "imp" in df.columns and "clk" in df.columns:
        denom = df["imp"].replace(0, np.nan)
        df["ctr"] = (df["clk"] / denom * 100.0)
        df["ctr"] = pd.to_numeric(df["ctr"], errors="coerce").fillna(0.0)
    if "clk" in df.columns and "cost" in df.columns:
        denom = df["clk"].replace(0, np.nan)
        df["cpc"] = (df["cost"] / denom)
        df["cpc"] = pd.to_numeric(df["cpc"], errors="coerce").fillna(0.0)
    if "conv" in df.columns and "cost" in df.columns:
        denom = df["conv"].replace(0, np.nan)
        df["cpa"] = (df["cost"] / denom)
        df["cpa"] = pd.to_numeric(df["cpa"], errors="coerce").fillna(0.0)
    if "cost" in df.columns and "sales" in df.columns:
        denom = df["cost"].replace(0, np.nan)
        df["roas"] = (df["sales"] / denom * 100.0)
        df["roas"] = pd.to_numeric(df["roas"], errors="coerce").fillna(0.0)

    # display formatting helpers
    def _fmt_int(x) -> str:
        try:
            return f"{int(round(float(x))):,}"
        except Exception:
            return "0"

    def _fmt_won(x) -> str:
        try:
            return f"{int(round(float(x))):,}원"
        except Exception:
            return "0원"

    def _fmt_pct1(x) -> str:
        try:
            return f"{float(x):.1f}%"
        except Exception:
            return "0.0%"

    def _fmt_pct0(x) -> str:
        try:
            return f"{float(x):.0f}%"
        except Exception:
            return "0%"

    # choose order
    order = []
    for c in ["dt", "imp", "clk", "ctr", "cpc", "cost", "conv", "cpa", "sales", "roas"]:
        if c in df.columns:
            order.append(c)

    view = df[order].copy()

    # rename to Korean
    rename = {
        "dt": "일자",
        "imp": "노출",
        "clk": "클릭",
        "ctr": "CTR(%)",
        "cpc": "CPC",
        "cost": "광고비",
        "conv": "전환",
        "cpa": "CPA",
        "sales": "매출",
        "roas": "ROAS(%)",
    }
    view = view.rename(columns=rename)

    # format for display (strings)
    disp = pd.DataFrame()
    if "일자" in view.columns:
        disp["일자"] = view["일자"].astype(str)

    for col in ["노출", "클릭", "전환"]:
        if col in view.columns:
            disp[col] = view[col].apply(_fmt_int)

    if "CTR(%)" in view.columns:
        disp["CTR(%)"] = view["CTR(%)"].apply(_fmt_pct1)

    if "CPC" in view.columns:
        disp["CPC"] = view["CPC"].apply(_fmt_won)

    if "광고비" in view.columns:
        disp["광고비"] = view["광고비"].apply(_fmt_won)

    if "CPA" in view.columns:
        disp["CPA"] = view["CPA"].apply(_fmt_won)

    if "매출" in view.columns:
        disp["매출"] = view["매출"].apply(_fmt_won)

    if "ROAS(%)" in view.columns:
        disp["ROAS(%)"] = view["ROAS(%)"].apply(_fmt_pct0)

    st_dataframe_safe(disp, use_container_width=True, hide_index=True, height=360)




def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    """Naver-like KPI card: compact, ▲/▼ delta.
    desc examples: "전주 대비 +12.3%" / "전일 대비 -3.1%" / "선택 기간 합계"
    """
    # Optional escape hatch
    use_shadcn = os.getenv("USE_SHADCN_METRICS", "0").strip() == "1"
    if use_shadcn and HAS_SHADCN_UI and ui is not None:
        try:
            ui.metric_card(title=title, content=value, description=desc, key=key)
            return
        except Exception:
            pass

    label = (desc or "").strip()
    delta_html = f"<div class='d'><span class='chip'>{label}</span></div>" if label else "<div class='d'></div>"

    m = re.search(r"([+-])\s*([0-9]+(?:\.[0-9]+)?)\s*%", label)
    if m:
        sign = m.group(1)
        num = m.group(2)
        arrow = "▲" if sign == "+" else "▼"
        cls = "pos" if sign == "+" else "neg"
        # remove the matched "+12.3%" part from label
        label2 = (label.replace(m.group(0), "").replace("  ", " ").strip()) or ""
        chip = f"<span class='chip'>{label2}</span>" if label2 else ""
        delta_html = f"<div class='d {cls}'>{chip}{arrow} {num}%</div>"

    st.markdown(
        f"""<div class='kpi' id='{key}'>
            <div class='k'>{title}</div>
            <div class='v'>{value}</div>
            {delta_html}
        </div>""",
        unsafe_allow_html=True,
    )



def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    """Small tables: shadcn table if available; else st.dataframe."""
    if df is None:
        df = pd.DataFrame()
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.table(df, maxHeight=height, key=key)
            return
        except Exception:
            pass
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)


def render_budget_month_table_with_bars(table_df: pd.DataFrame, key: str, height: int = 520) -> None:
    """
    월 예산 관리 표: '집행률(%)'을 숫자+막대바로 표시.
    - Streamlit 버전/컴포넌트 의존성을 피하기 위해 HTML 테이블로 렌더링(항상 동작).
    - 다운로드용 DF는 원본을 사용하고, 화면 표시만 bar 컬럼을 만든다.
    """
    if table_df is None or table_df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    df = table_df.copy()

    # numeric columns right align
    for c in df.columns:
        if c in ("월 예산(원)", f"{datetime.now().month}월 사용액", "집행률(%)"):
            pass

    # Build bar html
    def _bar(pct, status) -> str:
        try:
            pv = float(pct)
        except Exception:
            pv = 0.0
        pv = 0.0 if math.isnan(pv) else pv
        width = max(0.0, min(pv, 120.0))  # allow slight overrun visibility
        stt = str(status or "")
        if stt.startswith("🔴"):
            fill = "var(--nv-red)"
        elif stt.startswith("🟡"):
            fill = "#F59E0B"
        elif stt.startswith("🟢"):
            fill = "var(--nv-green)"
        else:
            fill = "rgba(0,0,0,.25)"
        return (
            f"<div class='nv-pbar'>"
            f"  <div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{width:.2f}%;background:{fill};'></div></div>"
            f"  <div class='nv-pbar-txt'>{pv:.1f}%</div>"
            f"</div>"
        )

    if "집행률(%)" in df.columns:
        df["집행률"] = [
            _bar(p, s) for p, s in zip(df["집행률(%)"].tolist(), df.get("상태", "").tolist())
        ]
        df = df.drop(columns=["집행률(%)"])
        # place '집행률' where the old column was
        cols = list(df.columns)
        # try to move right before '상태'
        if "상태" in cols and "집행률" in cols:
            cols.remove("집행률")
            idx = cols.index("상태")
            cols.insert(idx, "집행률")
            df = df[cols]

    # Convert numeric-looking money strings to right aligned using CSS class via HTML (add <td class="num">)
    # Pandas to_html doesn't let per-cell classes easily; small post-process for known columns.
    html = df.to_html(index=False, escape=False, classes="nv-table")
    # Add num alignment for known columns by injecting class into <td> for those columns.
    # We'll do a light regex pass on the generated table.
    num_cols = ["월 예산(원)", "2월 사용액", "3월 사용액", "4월 사용액", "5월 사용액", "6월 사용액", "7월 사용액", "8월 사용액", "9월 사용액", "10월 사용액", "11월 사용액", "12월 사용액", "집행률"]
    # But our '집행률' is html; skip.
    # We'll right-align any cell that endswith '원' or is purely digits/commas.
    html = re.sub(r"<td>([\d,]+원)</td>", r"<td class='num'>\1</td>", html)
    html = re.sub(r"<td>([\d,]+)</td>", r"<td class='num'>\1</td>", html)

    st.markdown(f"<div class='nv-table-wrap' style='max-height:{height}px'>{html}</div>", unsafe_allow_html=True)




def render_pinned_summary_grid(
    detail_df: pd.DataFrame,
    summary_df: Optional[pd.DataFrame],
    key: str,
    height: int = 520,
) -> None:
    """Render a large sortable table where 'summary' rows stay pinned at the top.

    - If streamlit-aggrid is installed: pinnedTopRowData keeps the summary fixed (even on sort/scroll).
    - Otherwise: fallback to two tables (summary above, detail below).
    """
    if detail_df is None:
        detail_df = pd.DataFrame()
    if summary_df is None:
        summary_df = pd.DataFrame()

    # Normalize columns
    if not summary_df.empty and list(summary_df.columns) != list(detail_df.columns):
        # try align to detail columns
        summary_df = summary_df.reindex(columns=list(detail_df.columns))

    
    if HAS_AGGRID and AgGrid is not None:
        # Pinned rows
        pinned = summary_df.to_dict("records") if summary_df is not None and not summary_df.empty else []

        # Right-align numeric-ish columns
        right_cols = {
            "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"
        }

        grid = _aggrid_grid_options(
            cols=list(detail_df.columns),
            pinned_rows=pinned,
            right_cols=right_cols,
            enable_filter=False,
        )

        AgGrid(
            detail_df,
            gridOptions=grid,
            height=height,
            fit_columns_on_grid_load=False,
            theme="alpine",
            allow_unsafe_jscode=True,
            update_mode=_aggrid_mode("no_update"),
            data_return_mode=_aggrid_mode("as_input"),
            key=key,
        )
        return


    # Fallback: summary above + detail below (summary stays on top structurally)
    if summary_df is not None and not summary_df.empty:
        # keep it compact
        st_dataframe_safe(style_summary_rows(summary_df, len(summary_df)), use_container_width=True, hide_index=True, height=min(220, 60 + 35 * len(summary_df)))
    st_dataframe_safe(detail_df, use_container_width=True, hide_index=True, height=height)


def render_echarts_donut(title: str, data: pd.DataFrame, label_col: str, value_col: str, height: int = 260) -> None:
    """ECharts 도넛 차트(선택): streamlit-echarts 설치 시만 렌더."""
    if not (HAS_ECHARTS and st_echarts is not None):
        return
    if data is None or data.empty or label_col not in data.columns or value_col not in data.columns:
        return

    d = data.copy()
    d[label_col] = d[label_col].astype(str)
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)

    items = [{"name": n, "value": float(v)} for n, v in zip(d[label_col].tolist(), d[value_col].tolist()) if float(v) > 0]
    if not items:
        return

    option = {
        "title": {"text": title, "left": "center", "top": 6, "textStyle": {"fontSize": 13}},
        "tooltip": {"trigger": "item", "formatter": "{b}<br/>{c:,} ({d}%)"},
        "legend": {"type": "scroll", "bottom": 0},
        "series": [
            {
                "name": title,
                "type": "pie",
                "radius": ["55%", "78%"],
                "avoidLabelOverlap": True,
                "itemStyle": {"borderRadius": 10, "borderColor": "#fff", "borderWidth": 2},
                "label": {"show": False},
                "emphasis": {"label": {"show": True, "fontSize": 13, "fontWeight": "bold"}},
                "labelLine": {"show": False},
                "data": items,
            }
        ],
    }
    st_echarts(option, height=f"{height}px")



def render_echarts_line(
    title: str,
    ts: pd.DataFrame,
    x_col: str,
    y_col: str,
    y_name: str,
    *,
    height: int = 260,
    smooth: bool = True,
) -> None:
    """ECharts 라인차트(기본 트렌드). streamlit-echarts 설치 시만 렌더."""
    if not (HAS_ECHARTS and st_echarts is not None):
        return

    if ts is None or ts.empty or x_col not in ts.columns or y_col not in ts.columns:
        return

    df = ts[[x_col, y_col]].copy()
    # x
    if np.issubdtype(df[x_col].dtype, np.datetime64):
        df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
    else:
        # try parse
        try:
            df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
        except Exception:
            df[x_col] = df[x_col].astype(str)

    # y
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce").fillna(0.0).round(0)

    x = df[x_col].astype(str).tolist()
    y = df[y_col].astype(float).round(0).astype(int).tolist()

    option = {
        "title": {"show": False},
        "grid": {"left": 54, "right": 18, "top": 44, "bottom": 34},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": x, "axisTick": {"alignWithLabel": True}},
        "yAxis": {"type": "value", "name": y_name, "nameTextStyle": {"padding": [0, 0, 0, 6]}},
        "series": [
            {
                "type": "line",
                "data": y,
                "smooth": smooth,
                "showSymbol": False,
                "lineStyle": {"width": 3, "color": "#2563EB"},
                "areaStyle": {"opacity": 0.06},
            }
        ],
    }
    st_echarts(option, height=f"{int(height)}px")


def render_echarts_delta_bars(delta_df: pd.DataFrame, *, height: int = 260) -> None:
    """증감율(%) 막대그래프 - 0을 중앙에 고정 + 0쪽은 평평/끝쪽은 둥글게."""
    if not (HAS_ECHARTS and st_echarts is not None):
        return
    if delta_df is None or delta_df.empty:
        return

    d = delta_df.copy()
    if "metric" not in d.columns or "change_pct" not in d.columns:
        return

    # keep original order (as provided)
    d["metric"] = d["metric"].astype(str)
    d["v"] = pd.to_numeric(d["change_pct"], errors="coerce")

    # 비교기간 데이터가 없어서 계산 불가(None/NaN)인 경우 -> 차트 대신 안내
    if d["v"].notna().sum() == 0:
        st.info("비교기간 데이터가 없어 증감율을 계산할 수 없습니다.")
        return

    d["v"] = d["v"].fillna(0.0).round(0).astype(int)

    lim = float(max(d["v"].abs().max(), 1.0))
    lim = lim * 1.15 + 0.5  # padding
    # cap for readability
    lim = max(lim, 5.0)

    cats = d["metric"].tolist()[::-1]
    vals = d["v"].tolist()[::-1]

    data = []
    for m, v in zip(cats, vals):
        if v > 0:
            color = "#2563EB"  # up=blue
            br = [0, 10, 10, 0]  # round right only
            pos = "right"
            fmt = f"+{int(round(v))}%"
        elif v < 0:
            color = "#EF4444"  # down=red
            br = [10, 0, 0, 10]  # round left only
            pos = "left"
            fmt = f"{int(round(v))}%"
        else:
            color = "#B4C4D9"
            br = [0, 0, 0, 0]
            pos = "right"
            fmt = "+0%"
        data.append(
            {
                "value": v,
                "label": {"show": True, "position": pos, "formatter": fmt, "fontWeight": "bold"},
                "itemStyle": {"color": color, "borderRadius": br},
            }
        )

    option = {
        "grid": {"left": 70, "right": 24, "top": 12, "bottom": 26},
        "xAxis": {
            "type": "value",
            "min": -lim,
            "max": lim,
            "axisLabel": {"formatter": "{value}"},
            "splitLine": {"lineStyle": {"color": "#EBEEF2"}},
        },
        "yAxis": {"type": "category", "data": cats, "axisTick": {"show": False}},
        "series": [
            {
                "type": "bar",
                "data": data,
                "barWidth": 20,
                "silent": True,
                "markLine": {
                    "symbol": "none",
                    "label": {"show": False},
                    "lineStyle": {"color": "#CBD5E1", "width": 2},
                    "data": [{"xAxis": 0}],
                },
            }
        ],
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
    }
    st_echarts(option, height=f"{int(height)}px")



def render_big_table(df: pd.DataFrame, key: str, height: int = 560) -> None:
    """대용량 테이블: AgGrid(설치 시) 우선, 미설치 시 st.dataframe 폴백."""
    if df is None:
        df = pd.DataFrame()

    
    if HAS_AGGRID and AgGrid is not None:
        q = st.text_input("검색", value="", placeholder="테이블 내 검색", key=f"{key}_q")

        # right-align numeric-like columns if present
        right_cols = {c for c in df.columns if any(k in c for k in ["노출", "클릭", "광고비", "전환", "매출", "CTR", "CPC", "CPA", "ROAS"])}

        grid = _aggrid_grid_options(
            cols=list(df.columns),
            pinned_rows=[],
            right_cols=right_cols,
            quick_filter=q or "",
            enable_filter=True,
        )

        AgGrid(
            df,
            gridOptions=grid,
            height=height,
            fit_columns_on_grid_load=False,
            theme="alpine",
            allow_unsafe_jscode=True,
            update_mode=_aggrid_mode("no_update"),
            data_return_mode=_aggrid_mode("as_input"),
            key=key,
        )
        return

    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)




# -----------------------------
# DB helpers
# -----------------------------
def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        try:
            db_url = str(st.secrets.get("DATABASE_URL", "")).strip()
        except Exception:
            db_url = ""

    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. (.env env var or Streamlit secrets)")

    if "sslmode=" not in db_url:
        joiner = "&" if "?" in db_url else "?"
        db_url = db_url + f"{joiner}sslmode=require"

    return db_url


@st.cache_resource(show_spinner=False)
def get_engine():
    # Supabase/PGBouncer 환경에서 SSL 연결이 중간에 끊기는 케이스를 줄이기 위한 설정
    # - pool_pre_ping: checkout 시 SELECT 1로 연결 상태 확인
    # - pool_recycle: 오래된 커넥션 재사용 방지(서버/풀러 idle timeout 회피)
    # - pool_use_lifo: 최근 사용 커넥션 우선(죽은 커넥션 확률 감소)
    url = get_database_url()
    connect_args = {
        "sslmode": "require",
        "connect_timeout": 10,
        # TCP keepalive (psycopg2)
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }

    # 안정성이 더 중요하면 NullPool로 전환 가능 (각 쿼리마다 새 연결)
    use_nullpool = False

    if use_nullpool:
        return create_engine(url, connect_args=connect_args, poolclass=NullPool, future=True)

    return create_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=300,
        pool_use_lifo=True,
        future=True,
    )




def _reset_engine_cache() -> None:
    """DB 연결이 끊긴 경우(get_engine 캐시 포함) 재생성을 유도."""
    try:
        get_engine.clear()  # type: ignore[attr-defined]
    except Exception:
        pass

def sql_read(engine, sql: str, params: Optional[dict] = None, retries: int = 2) -> pd.DataFrame:
    """DB read with retry for transient connection errors (SSL closed, idle timeout, etc.)."""
    last_err: Exception | None = None
    _engine = engine

    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except Exception as e:
            last_err = e
            # 1) 풀 내부 죽은 커넥션 제거
            try:
                _engine.dispose()
            except Exception:
                pass

            # 2) 캐시된 엔진 자체가 꼬였으면 재생성
            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine

            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err


def sql_exec(engine, sql: str, params: Optional[dict] = None, retries: int = 1) -> None:
    last_err = None
    for i in range(retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), params or {})
            return
        except Exception as e:
            last_err = e
            try:
                engine.dispose()
            except Exception:
                pass
            if i < retries:
                time.sleep(0.25 * (2 ** i))
                continue
            raise last_err

def db_ping(engine, retries: int = 2) -> None:
    """가벼운 DB 헬스체크. pandas를 거치지 않고 SELECT 1만 실행."""
    last_err: Exception | None = None
    _engine = engine
    for i in range(retries + 1):
        try:
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            try:
                _engine.dispose()
            except Exception:
                pass
            if i == 0:
                _reset_engine_cache()
                try:
                    _engine = get_engine()
                except Exception:
                    _engine = engine
            if i < retries:
                time.sleep(0.35 * (2 ** i))
                continue
            raise last_err


def _get_table_names_cached(engine, schema: str = "public") -> set:
    """Inspector 호출은 매우 느립니다. 세션 단위로 table list를 캐시합니다."""
    cache = st.session_state.setdefault("_table_names_cache", {})
    if schema in cache:
        return cache[schema]
    try:
        insp = inspect(engine)
        names = set(insp.get_table_names(schema=schema))
    except Exception:
        names = set()
    cache[schema] = names
    return names


def table_exists(engine, table: str, schema: str = "public") -> bool:
    return table in _get_table_names_cached(engine, schema=schema)


def get_table_columns(engine, table: str, schema: str = "public") -> set:
    """Inspector는 느리므로 세션 단위 캐시."""
    cache = st.session_state.setdefault("_table_cols_cache", {})
    key = f"{schema}.{table}"
    if key in cache:
        return cache[key]
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        out = {str(c.get("name", "")).lower() for c in cols}
    except Exception:
        out = set()
    cache[key] = out
    return out


def _sql_in_str_list(values: List[int]) -> str:
    """TEXT/BIGINT 혼재를 안전하게 처리하려고, 항상 문자열 리터럴로 IN 리스트를 만듭니다."""
    safe = []
    for v in values:
        try:
            safe.append(f"'{int(v)}'")
        except Exception:
            continue
    return ",".join(safe) if safe else "''"


# -----------------------------
# Download helpers (cached)
# -----------------------------
@st.cache_data(hash_funcs=_HASH_FUNCS, show_spinner=False)


def _fact_has_sales(engine, fact_table: str) -> bool:
    """fact 테이블에 sales 컬럼이 있는지(계정별 스키마 차이 대응)."""
    return "sales" in get_table_columns(engine, fact_table)


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=180, show_spinner=False)
def query_budget_bundle(
    _engine,
    cids: Tuple[int, ...],
    yesterday: date,
    avg_d1: date,
    avg_d2: date,
    month_d1: date,
    month_d2: date,
    avg_days: int,
) -> pd.DataFrame:
    """예산/비즈머니/전일소진/최근N일평균/당월소진을 계정 단위로 한 번에 가져옵니다."""
    if not (
        table_exists(_engine, "dim_account_meta")
        and table_exists(_engine, "fact_campaign_daily")
        and table_exists(_engine, "fact_bizmoney_daily")
    ):
        return pd.DataFrame()

    where_cid = ""
    if cids:
        where_cid = f"WHERE m.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    WITH meta AS (
      SELECT customer_id::text AS customer_id, account_name, manager, COALESCE(monthly_budget,0) AS monthly_budget
      FROM dim_account_meta m
      {where_cid}
    ),
    biz AS (
      SELECT DISTINCT ON (customer_id::text)
        customer_id::text AS customer_id,
        bizmoney_balance,
        dt AS last_update
      FROM fact_bizmoney_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
      ORDER BY customer_id::text, dt DESC
    ),
    camp AS (
      SELECT
        customer_id::text AS customer_id,
        SUM(cost) FILTER (WHERE dt = :y) AS y_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :a1 AND :a2) AS avg_sum_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :m1 AND :m2) AS month_cost
      FROM fact_campaign_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
        AND dt BETWEEN :min_dt AND :max_dt
      GROUP BY customer_id::text
    )
    SELECT
      meta.customer_id,
      meta.account_name,
      meta.manager,
      meta.monthly_budget,
      COALESCE(biz.bizmoney_balance,0) AS bizmoney_balance,
      biz.last_update,
      COALESCE(camp.y_cost,0) AS y_cost,
      COALESCE(camp.avg_sum_cost,0) AS avg_sum_cost,
      COALESCE(camp.month_cost,0) AS current_month_cost
    FROM meta
    LEFT JOIN biz ON meta.customer_id = biz.customer_id
    LEFT JOIN camp ON meta.customer_id = camp.customer_id
    ORDER BY meta.account_name
    """

    min_dt = min(yesterday, avg_d1, month_d1)
    max_dt = max(yesterday, avg_d2, month_d2)

    df = sql_read(
        _engine,
        sql,
        {
            "y": str(yesterday),
            "a1": str(avg_d1),
            "a2": str(avg_d2),
            "m1": str(month_d1),
            "m2": str(month_d2),
            "min_dt": str(min_dt),
            "max_dt": str(max_dt),
        },
    )
    if df is None or df.empty:
        return pd.DataFrame()

    # typing
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["monthly_budget", "bizmoney_balance", "y_cost", "avg_sum_cost", "current_month_cost"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    df["avg_cost"] = df["avg_sum_cost"].astype(float) / float(max(avg_days, 1))
    return df


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
    """렌더링 속도를 위해 CSV는 기본 제공, XLSX는 캐시된 bytes 사용."""
    if df is None or df.empty:
        return

    df_json = df.to_json(orient="split")

    st.markdown(
        """
        <style>
        .stDownloadButton button {
            padding: 0.15rem 0.55rem !important;
            font-size: 0.82rem !important;
            line-height: 1.2 !important;
            min-height: 28px !important;
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



/* ---- 검색조건 박스(Expander) 네이버형: '붙어 보임/튀어나옴' 정리 ---- */
div[data-testid="stExpander"]{
  background: var(--nv-panel) !important;
  border: 1px solid var(--nv-line) !important;
  border-radius: var(--nv-radius) !important;
  box-shadow: none !important;
  overflow: hidden !important;
}
div[data-testid="stExpander"] > details{
  border: 0 !important;
}
div[data-testid="stExpander"] > details > summary{
  padding: 12px 14px !important;
  font-weight: 800 !important;
  color: var(--nv-text) !important;
  background: #fff !important;
}
div[data-testid="stExpander"] > details > summary svg{ display:none !important; }
div[data-testid="stExpander"] > details > div{
  padding: 12px 14px 14px 14px !important;
  border-top: 1px solid var(--nv-line) !important;
  background: #fff !important;
}

/* Disabled text inputs (read-only dates) look like admin fields */
div[data-testid="stTextInput"] input[disabled]{
  background: #F3F4F6 !important;
  color: var(--nv-text) !important;
  border: 1px solid var(--nv-line) !important;
}

/* Sidebar radio: hide the circle icon & make it look like a nav list */
div[data-testid="stSidebar"] [data-testid="stRadio"] svg{ display:none !important; }
div[data-testid="stSidebar"] [data-testid="stRadio"] label{ padding-left: 10px !important; }

</style>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns([1, 1, 8])
    with c1:
        st.download_button(
            "CSV",
            data=_df_json_to_csv_bytes(df_json),
            file_name=f"{filename_base}.csv",
            mime="text/csv",
            key=f"{key_prefix}_csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "XLSX",
            data=_df_json_to_xlsx_bytes(df_json, sheet_name),
            file_name=f"{filename_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            use_container_width=True,
        )
    with c3:
        st.caption("다운로드")


# -----------------------------
# Formatters (lightweight)
# -----------------------------
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
        if pd.isna(val):
            return "-"
        return f"{float(val):.0f}%"
    except Exception:
        return "-"


def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")

    def _fmt(x):
        if pd.isna(x):
            return ""
        if float(x) == 0.0:
            return "0%"
        return f"{float(x):.1f}%"

    out[col] = s.map(_fmt)
    return out




# -----------------------------
# Campaign summary rows (Naver-like)
# -----------------------------
def build_campaign_summary_rows_from_numeric(
    df_numeric: pd.DataFrame,
    campaign_type_col: str = "campaign_type",
    campaign_name_col: str = "campaign_name",
) -> pd.DataFrame:
    """상단에 '캠페인 N개 결과' 요약행을 생성합니다.
    - df_numeric에는 최소: imp, clk, cost, conv, (optional) sales, campaign_type, campaign_name 가 있어야 합니다.
    - 반환 DF는 캠페인 테이블(view_df) 컬럼과 동일한 형태로 반환합니다.
    """
    if df_numeric is None or df_numeric.empty:
        return pd.DataFrame()

    x = df_numeric.copy()
    if campaign_type_col not in x.columns:
        return pd.DataFrame()

    # numeric safety
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c not in x.columns:
            x[c] = 0
        x[c] = pd.to_numeric(x[c], errors="coerce").fillna(0)

    x[campaign_type_col] = x[campaign_type_col].fillna("").astype(str).str.strip()
    x = x[x[campaign_type_col] != ""].copy()
    if x.empty:
        return pd.DataFrame()

    # count campaigns: unique by (customer_id, campaign_id) if present, else by campaign_name
    if "campaign_id" in x.columns and "customer_id" in x.columns:
        x["_camp_key"] = x["customer_id"].astype(str) + ":" + x["campaign_id"].astype(str)
    else:
        x["_camp_key"] = x.get(campaign_name_col, "").astype(str)

    def _make_row(label_type: str, g: pd.DataFrame) -> dict:
        n = int(g["_camp_key"].nunique())
        imp = float(g["imp"].sum())
        clk = float(g["clk"].sum())
        cost = float(g["cost"].sum())
        conv = float(g["conv"].sum())
        sales = float(g["sales"].sum()) if "sales" in g.columns else 0.0

        ctr = (clk / imp * 100.0) if imp > 0 else 0.0
        cpc = (cost / clk) if clk > 0 else 0.0
        cpa = (cost / conv) if conv > 0 else 0.0
        roas = (sales / cost * 100.0) if cost > 0 else 0.0

        return {
            "업체명": "",
            "담당자": "",
            "광고유형": label_type,
            "캠페인": f"캠페인 {n}개 결과",
            "노출": int(imp),
            "클릭": int(clk),
            "CTR(%)": float(ctr),
            "CPC": format_currency(cpc),
            "광고비": format_currency(cost),
            "전환": int(conv),
            "CPA": format_currency(cpa),
            "전환매출": format_currency(sales),
            "ROAS(%)": format_roas(roas),
        }

    rows = []

    # total row first (always)
    rows.append(_make_row("종합", x))

    # by campaign type
    for tp, g in x.groupby(campaign_type_col, dropna=False):
        tp = str(tp).strip() or "기타"
        rows.append(_make_row(tp, g))

    out = pd.DataFrame(rows)
    out["CTR(%)"] = pd.to_numeric(out["CTR(%)"], errors="coerce").fillna(0).astype(float)
    out = finalize_ctr_col(out, "CTR(%)")

    cols = ["업체명", "담당자", "광고유형", "캠페인", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    return out[cols].copy()


def style_summary_rows(df_view: pd.DataFrame, summary_rows: int):
    """상단 요약행을 다른 행과 구분되게 스타일링합니다."""
    if df_view is None or df_view.empty or summary_rows <= 0:
        return df_view

    summary_idx = set(range(int(summary_rows)))

    def _style_row(row):
        if row.name in summary_idx:
            return ["font-weight:700; background-color: rgba(148,163,184,0.18);"] * len(row)
        return [""] * len(row)

    try:
        return df_view.style.apply(_style_row, axis=1)
    except Exception:
        return df_view


def parse_currency(val_str) -> int:
    if pd.isna(val_str):
        return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0


# -----------------------------
# Campaign type label
# -----------------------------
_CAMPAIGN_TP_LABEL = {
    "web_site": "파워링크",
    "website": "파워링크",
    "power_link": "파워링크",
    "shopping": "쇼핑검색",
    "shopping_search": "쇼핑검색",
    "power_content": "파워콘텐츠",
    "power_contents": "파워콘텐츠",
    "powercontent": "파워콘텐츠",
    "place": "플레이스",
    "place_search": "플레이스",
    "brand_search": "브랜드검색",
    "brandsearch": "브랜드검색",
}
_LABEL_TO_TP_KEYS: Dict[str, List[str]] = {}
for k, v in _CAMPAIGN_TP_LABEL.items():
    _LABEL_TO_TP_KEYS.setdefault(v, []).append(k)


def campaign_tp_to_label(tp: str) -> str:
    t = (tp or "").strip()
    if not t:
        return ""
    key = t.lower()
    return _CAMPAIGN_TP_LABEL.get(key, t)


def label_to_tp_keys(labels: Tuple[str, ...]) -> List[str]:
    keys: List[str] = []
    for lab in labels:
        keys.extend(_LABEL_TO_TP_KEYS.get(str(lab), []))
    out = []
    seen = set()
    for x in keys:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df["campaign_tp"] = df.get("campaign_tp", "").fillna("")
    df["campaign_type_label"] = df["campaign_tp"].astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df


def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []
    raw = dim_campaign.get("campaign_tp", pd.Series([], dtype=str))
    present = set()
    for x in raw.dropna().astype(str).tolist():
        lab = campaign_tp_to_label(x)
        lab = str(lab).strip()
        if lab and lab not in ("미분류", "종합", "기타"):
            present.add(lab)
    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    opts = [x for x in order if x in present]
    extra = sorted([x for x in present if x not in set(order)])
    return opts + extra


# -----------------------------
# Accounts / Meta
# -----------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))


def normalize_accounts_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    def find_col(cands: List[str]) -> Optional[str]:
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                cc = cand.lower().replace(" ", "").replace("_", "")
                if lc == cc:
                    return c
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                if cand in lc:
                    return c
        return None

    cid_col = find_col(["customer_id", "customerid", "커스텀id", "커스텀 id", "커스텀ID"])
    name_col = find_col(["account_name", "accountname", "업체명", "업체"])
    mgr_col = find_col(["manager", "담당자", "담당"])

    if not cid_col or not name_col:
        raise ValueError(f"accounts.xlsx is missing columns. Available: {list(df.columns)}")

    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(df[cid_col], errors="coerce").astype("Int64")
    out["account_name"] = df[name_col].astype(str).str.strip()
    out["manager"] = df[mgr_col].astype(str).str.strip() if mgr_col else ""
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    out["manager"] = out["manager"].fillna("").astype(str)
    out = out.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)
    return out


def ensure_meta_table(engine) -> None:
    sql_exec(
        engine,
        """CREATE TABLE IF NOT EXISTS dim_account_meta (
          customer_id BIGINT PRIMARY KEY,
          account_name TEXT NOT NULL,
          manager TEXT DEFAULT '',
          monthly_budget BIGINT DEFAULT 0,
          updated_at TIMESTAMPTZ DEFAULT now()
        );""",
    )


def seed_from_accounts_xlsx(engine, df: Optional[pd.DataFrame] = None) -> Dict[str, int]:
    """✅ 자동 실행 제거(속도 목적). 설정 페이지에서만 호출."""
    ensure_meta_table(engine)

    if df is None:
        if not os.path.exists(ACCOUNTS_XLSX):
            return {"meta": 0}
        df = pd.read_excel(ACCOUNTS_XLSX)

    acc = normalize_accounts_columns(df)

    upsert_meta = """
    INSERT INTO dim_account_meta (customer_id, account_name, manager, updated_at)
    VALUES (:customer_id, :account_name, :manager, now())
    ON CONFLICT (customer_id) DO UPDATE SET
      account_name = EXCLUDED.account_name,
      manager = EXCLUDED.manager,
      updated_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_meta), acc.to_dict(orient="records"))

    return {"meta": int(len(acc))}


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=600, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_account_meta"):
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df = sql_read(
        _engine,
        """
        SELECT customer_id, account_name, manager, monthly_budget, updated_at
        FROM dim_account_meta
        ORDER BY account_name
        """,
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    df["manager"] = df.get("manager", "").fillna("").astype(str).str.strip()
    df["account_name"] = df.get("account_name", "").fillna("").astype(str).str.strip()
    return df


def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
    if not table_exists(engine, "dim_account_meta"):
        return
    sql_exec(
        engine,
        """
        UPDATE dim_account_meta
        SET monthly_budget = :b, updated_at = now()
        WHERE customer_id = :cid
        """,
        {"b": int(monthly_budget), "cid": int(customer_id)},
    )


# -----------------------------
# Data freshness (single query)
# -----------------------------
@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=600, show_spinner=False)
def query_latest_dates(_engine) -> Dict[str, str]:
    """최근 적재일을 반환합니다.

    ✅ 안정성 개선 (v7.8.5)
    - Inspector(table_exists)는 간헐적으로 실패/지연될 수 있어서,
      테이블 존재 체크 없이 MAX(dt) 조회를 시도하고 예외를 무시합니다.
    """
    tables = ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily"]
    out: Dict[str, str] = {}
    for t in tables:
        try:
            df = sql_read(_engine, f"SELECT MAX(dt) AS mx FROM {t}")
            mx = df.iloc[0, 0] if (df is not None and not df.empty) else None
            out[str(t)] = str(mx)[:10] if mx is not None else "-"
        except Exception:
            continue
    return out



@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=180, show_spinner=False)

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=60, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    """최근 데이터 날짜를 1회 쿼리로 가져옵니다 (왕복/로딩 체감 개선)."""
    parts = []
    params = {}
    def _add(label: str, table: str):
        if table_exists(_engine, table):
            parts.append(f"SELECT '{label}' AS k, MAX(dt) AS dt FROM {table}")
    _add("campaign", "fact_campaign_daily")
    _add("keyword", "fact_keyword_daily")
    _add("ad", "fact_ad_daily")
    _add("bizmoney", "fact_bizmoney_daily")

    if not parts:
        return {"campaign": None, "keyword": None, "ad": None, "bizmoney": None}

    sql = " UNION ALL ".join(parts)
    df = sql_read(_engine, sql, params)
    out = {"campaign": None, "keyword": None, "ad": None, "bizmoney": None}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        k = str(r.get("k"))
        out[k] = r.get("dt")
    return out


def ui_badges_or_html(items, key_prefix: str = "") -> None:
    """간단한 배지/칩 UI (HTML 기반). items: List[Tuple[str, Any]]"""
    pills = []
    for label, value in items:
        v = str(value) if value is not None else "—"
        pills.append(f"<div class='pill'><span class='dot on'></span>{label}: {v}</div>")
    html = "<div class='freshness-pills'>" + "\n".join(pills) + "</div>"
    html = "\n".join([ln.strip() for ln in html.splitlines() if ln.strip()])
    st.markdown(html, unsafe_allow_html=True)

def render_data_freshness(engine) -> None:
    latest = query_latest_dates(engine)
    if not latest:
        return
    label_map = {
        "fact_campaign_daily": "캠페인",
        "fact_keyword_daily": "키워드",
        "fact_ad_daily": "소재",
        "fact_bizmoney_daily": "비즈머니",
    }
    items = [(f"{label_map.get(k,k)} 최신: {v}", "secondary") for k, v in latest.items()]
    ui_badges_or_html(items, key_prefix="freshness_badges")




def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    """필터(담당자/계정) 선택값을 customer_id 리스트로 변환"""
    if meta is None or meta.empty:
        return []
    if (not manager_sel) and (not account_sel):
        return []

    df = meta.copy()

    if manager_sel and "manager" in df.columns:
        sel = [str(x).strip() for x in manager_sel if str(x).strip()]
        if sel:
            df = df[df["manager"].astype(str).str.strip().isin(sel)]

    if account_sel and "account_name" in df.columns:
        sel = [str(x).strip() for x in account_sel if str(x).strip()]
        if sel:
            df = df[df["account_name"].astype(str).str.strip().isin(sel)]

    if "customer_id" not in df.columns:
        return []

    s = pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64")
    return sorted(s.drop_duplicates().tolist())





def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    """Streamlit multiselect with Korean placeholder (compatible across Streamlit versions)."""
    try:
        return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception:
        # Older Streamlit without placeholder=
        return col.multiselect(label, options, default=default, key=key)

def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    """Naver-like '검색조건' panel. No '적용' 버튼: 변경 즉시 반영되지만,
    쿼리는 cache_data로 막아서 체감 속도를 확보합니다.
    """
    today = date.today()
    default_end = today - timedelta(days=1)  # 기본: 어제
    default_start = default_end

    # persist defaults
    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "",
            "manager": [],
            "account": [],
            "type_sel": [],
            "period_mode": "어제",
            "d1": default_start,
            "d2": default_end,
            "top_n_keyword": 300,
            "top_n_ad": 200,
            "top_n_campaign": 200,
            "prefetch_warm": True,
        }

    sv = st.session_state["filters_v8"]

    # Options from meta
    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    # --- 검색조건 패널 (네이버 느낌) ---

    with st.expander("검색조건", expanded=True):

        r1 = st.columns([1.1, 1.2, 1.2, 2.2], gap="small")

        period_mode = r1[0].selectbox(

            "기간",

            ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"],

            index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")),

            key="f_period_mode",

        )


        if period_mode == "직접 선택":

            d1 = r1[1].date_input("시작일", sv.get("d1", default_start), key="f_d1")

            d2 = r1[2].date_input("종료일", sv.get("d2", default_end), key="f_d2")

        else:

            # compute dates from mode (no extra widgets)

            if period_mode == "오늘":

                d2 = today

                d1 = today

            elif period_mode == "어제":

                d2 = today - timedelta(days=1)

                d1 = d2

            elif period_mode == "최근 7일":

                d2 = today - timedelta(days=1)

                d1 = d2 - timedelta(days=6)

            elif period_mode == "이번 달":

                d2 = today

                d1 = date(today.year, today.month, 1)

            elif period_mode == "지난 달":

                first_this = date(today.year, today.month, 1)

                d2 = first_this - timedelta(days=1)

                d1 = date(d2.year, d2.month, 1)

            else:

                d2 = sv.get("d2", default_end)

                d1 = sv.get("d1", default_start)


            # show read-only dates (consistent height, no '튀어나옴')

            r1[1].text_input("시작일", str(d1), disabled=True, key="f_d1_ro")

            r1[2].text_input("종료일", str(d2), disabled=True, key="f_d2_ro")


        q = r1[3].text_input("검색", sv.get("q", ""), key="f_q", placeholder="계정/키워드/소재 검색")


        r2 = st.columns([1.2, 1.6, 1.2], gap="small")

        manager_sel = ui_multiselect(r2[0], "담당자", managers, default=sv.get("manager", []), key="f_manager")

        # ✅ 담당자 선택 시: 해당 담당자 계정만 노출 (네이버 관리자 UX)
        accounts_by_mgr = accounts
        if manager_sel:
            try:
                dfm = meta.copy()
                # normalize (공백/개행) - 담당자/업체 필터 정확도 향상
                dfm['manager'] = dfm.get('manager','').astype(str).fillna('').str.strip()
                dfm['account_name'] = dfm.get('account_name','').astype(str).fillna('').str.strip()
                if "manager" in dfm.columns and "account_name" in dfm.columns:
                    dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                    accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
            except Exception:
                accounts_by_mgr = accounts

        # 기존 선택값 중 유효한 것만 유지
        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]

        account_sel = ui_multiselect(r2[1], "계정", accounts_by_mgr, default=prev_acc, key="f_account")

        type_sel = ui_multiselect(r2[2], "캠페인 유형", type_opts, default=sv.get("type_sel", []), key="f_type_sel")


    # persist back
    sv.update(
        {
            "q": q or "",
            "manager": manager_sel or [],
            "account": account_sel or [],
            "type_sel": type_sel or [],
            "period_mode": period_mode,
            "d1": d1,
            "d2": d2,
        }
    )
    st.session_state["filters_v8"] = sv

    # Customer ids resolve
    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    # Return the same shape other pages expect
    f = {
        "q": sv["q"],
        "manager": sv["manager"],
        "account": sv["account"],
        "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1,
        "end": d2,
        "period_mode": period_mode,
        "customer_ids": cids,
        "selected_customer_ids": cids,  # alias for older pages (campaign/budget)
        "top_n_keyword": int(sv.get("top_n_keyword", 300)),
        "top_n_ad": int(sv.get("top_n_ad", 200)),
        "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "prefetch_warm": bool(sv.get("prefetch_warm", True)),
        "ready": True,
    }
    return f



def query_campaign_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트
    - fact → (필요 시) dim_campaign 타입 필터를 먼저 적용한 뒤 집계
    - 집계 결과에서 cost 기준 TOP N만 뽑고, 그 다음에 이름/타입 조인
    """
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          c.campaign_name,
          c.campaign_tp
        FROM top t
        JOIN c_f c
          ON t.customer_id = c.customer_id
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM top t
        LEFT JOIN dim_campaign c
          ON t.customer_id = c.customer_id::text
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")

    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    # df = df[df["campaign_type"].astype(str).str.strip() != "기타"]  # keep 기타 rows

    return df.reset_index(drop=True)


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    ✅ 1회 쿼리로 캠페인 탭에 필요한 데이터 동시 확보
    - 광고비 기준 TopN (topn_cost)
    - 클릭 TopK, 전환 TopK (top_k)
    * 비용 TopN만 뽑는 쿼리로는 클릭/전환 TopK가 누락될 수 있어서,
      base 집계 후 (cost/clk/conv) 각각 LIMIT로 뽑아 UNION 합니다.
    """
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          c.campaign_name,
          c.campaign_tp
        FROM picked p
        JOIN c_f c
          ON p.customer_id = c.customer_id
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM picked p
        LEFT JOIN dim_campaign c
          ON p.customer_id = c.customer_id::text
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    # df = df[df["campaign_type"].astype(str).str.strip() != "기타"]  # keep 기타 rows

    return df.reset_index(drop=True)




@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_daily_slice(_engine, d1: date, d2: date) -> pd.DataFrame:
    """캠페인 탭용: (일자 x 캠페인) 슬라이스를 1회 조회해 캐시합니다.
    날짜 범위만 바뀔 때 DB를 치고, 이후 담당자/업체/유형/캠페인 선택은 pandas 필터로 처리합니다.
    """
    if not table_exists(_engine, "fact_campaign_daily") or not table_exists(_engine, "dim_account_meta"):
        return pd.DataFrame(columns=["dt","customer_id","account_name","manager","campaign_id","campaign_name","campaign_tp","campaign_type","imp","clk","cost","conv","sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    join_campaign = ""
    select_campaign = "''::text AS campaign_name, ''::text AS campaign_tp"
    group_campaign = ""
    if table_exists(_engine, "dim_campaign"):
        join_campaign = "LEFT JOIN dim_campaign c ON c.customer_id::text = f.customer_id::text AND c.campaign_id = f.campaign_id"
        select_campaign = "COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name, COALESCE(NULLIF(c.campaign_tp,''),'') AS campaign_tp"
        group_campaign = ", c.campaign_name, c.campaign_tp"

    sql = f"""
    SELECT
      f.dt::date AS dt,
      f.customer_id::text AS customer_id,
      COALESCE(NULLIF(m.account_name,''),'') AS account_name,
      COALESCE(NULLIF(m.manager,''),'') AS manager,
      f.campaign_id,
      {select_campaign},
      SUM(f.imp)  AS imp,
      SUM(f.clk)  AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      {sales_expr} AS sales
    FROM fact_campaign_daily f
    JOIN dim_account_meta m
      ON m.customer_id = f.customer_id::text
    {join_campaign}
    WHERE f.dt BETWEEN :d1 AND :d2
    GROUP BY f.dt::date, f.customer_id::text, m.account_name, m.manager, f.campaign_id{group_campaign}
    ORDER BY f.dt::date
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt","customer_id","account_name","manager","campaign_id","campaign_name","campaign_tp","campaign_type","imp","clk","cost","conv","sales"])

    # types
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["imp","clk","cost","conv","sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["campaign_tp"] = df.get("campaign_tp", "").astype(str)
    df["campaign_type"] = df["campaign_tp"].map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    # df = df[df["campaign_type"].astype(str).str.strip() != "기타"]  # keep 기타 rows
    return df.reset_index(drop=True)
# -----------------------------
# Timeseries Queries (for charts)
# -----------------------------

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """캠페인(전체) 일별 추세. (그래프용: row 수 적음)"""
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        JOIN c_f c
          ON f.customer_id::text = c.customer_id
         AND f.campaign_id = c.campaign_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df




@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_campaign_one_timeseries(_engine, d1: date, d2: date, customer_id: int, campaign_id: int) -> pd.DataFrame:
    """선택 캠페인 1개 일별 추세 (아주 가벼운 쿼리)."""
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    sql = f"""
    SELECT
      f.dt::date AS dt,
      SUM(f.imp)  AS imp,
      SUM(f.clk)  AS clk,
      SUM(f.cost) AS cost,
      SUM(f.conv) AS conv,
      {sales_expr} AS sales
    FROM fact_campaign_daily f
    WHERE f.dt BETWEEN :d1 AND :d2
      AND f.customer_id::text = :cid
      AND f.campaign_id = :camp_id
    GROUP BY f.dt::date
    ORDER BY f.dt::date
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "cid": str(customer_id), "camp_id": int(campaign_id)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_ad_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """소재(전체) 일별 추세."""
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_ad"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        a_f AS (
          SELECT a.customer_id::text AS customer_id, a.ad_id::text AS ad_id
          FROM dim_ad a
          JOIN g_f g ON a.customer_id::text = g.customer_id AND a.adgroup_id::text = g.adgroup_id
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        JOIN a_f a ON f.customer_id::text = a.customer_id AND f.ad_id::text = a.ad_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_keyword_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """키워드(전체) 일별 추세. type_sel 없으면 join 없이 fact만 집계."""
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_keyword"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        k_f AS (
          SELECT k.customer_id::text AS customer_id, k.keyword_id::text AS keyword_id
          FROM dim_keyword k
          JOIN g_f g ON k.customer_id::text = g.customer_id AND k.adgroup_id::text = g.adgroup_id
        )
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        JOIN k_f k ON fk.customer_id::text = k.customer_id AND fk.keyword_id::text = k.keyword_id
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


# -----------------------------
# Altair Charts (rounded / smooth)
# -----------------------------

def _chart_timeseries(
    df: pd.DataFrame,
    y_col: str,
    y_title: str = "",
    *,
    x_col: str = "dt",
    y_format: str = ",.0f",
    height: int = 320,
):
    """Time-series line (Altair). Korean date labels + better readability."""
    if df is None or df.empty:
        return None
    if x_col not in df.columns or y_col not in df.columns:
        return None

    d = df[[x_col, y_col]].copy()
    d[x_col] = pd.to_datetime(d[x_col], errors="coerce")
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
    d = d.dropna(subset=[x_col]).sort_values(x_col).reset_index(drop=True)

    # --- Korean date labels (no English month/weekday) ---
    wk = ["월", "화", "수", "목", "금", "토", "일"]  # pandas weekday: Mon=0
    d["_wk"] = d[x_col].dt.weekday.map(lambda i: wk[int(i)] if pd.notna(i) else "")
    d["_x_label"] = d[x_col].dt.strftime("%m/%d") + "(" + d["_wk"] + ")"
    d["_dt_str"] = d[x_col].dt.strftime("%Y-%m-%d") + " (" + d["_wk"] + ")"

    title = (y_title or "").strip()
    y_axis = alt.Axis(title=title if title else None, format=y_format, grid=True, gridColor="#EBEEF2")
    x_axis = alt.Axis(title=None, grid=False, labelAngle=0, labelOverlap="greedy")

    base = (
        alt.Chart(d)
        .encode(
            x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending"), axis=x_axis),
            y=alt.Y(f"{y_col}:Q", axis=y_axis),
            tooltip=[
                alt.Tooltip("_dt_str:N", title="날짜"),
                alt.Tooltip(f"{y_col}:Q", title=title or y_col, format=y_format),
            ],
        )
    )

    # Layered chart: subtle area + thicker line + points + last-value label
    area = base.mark_area(interpolate="monotone", opacity=0.08)
    line = base.mark_line(interpolate="monotone", strokeWidth=3)
    pts = base.mark_point(size=40, filled=True)

    last = d.tail(1)
    last_label = (
        alt.Chart(last)
        .mark_text(align="left", dx=8, dy=-8)
        .encode(
            x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending")),
            y=alt.Y(f"{y_col}:Q"),
            text=alt.Text(f"{y_col}:Q", format=y_format),
        )
    )

    return (area + line + pts + last_label).properties(height=int(height))



def _disambiguate_label(df: pd.DataFrame, base_col: str, parts: List[str], id_col: Optional[str] = None, max_len: int = 38) -> pd.Series:
    """축 라벨 중복을 줄이기 위해 (키워드/캠페인/소재명) + (업체명/그룹/ID) 를 단계적으로 붙입니다."""
    if df is None or df.empty or base_col not in df.columns:
        return pd.Series([], dtype=str)

    label = df[base_col].fillna("").astype(str)

    for p in parts:
        dup = label.duplicated(keep=False)
        if not bool(dup.any()):
            break
        if p in df.columns:
            addon = df[p].fillna("").astype(str)
            label = label.where(~dup, (label + " · " + addon).str.strip())

    # still duplicated -> append short id
    dup2 = label.duplicated(keep=False)
    if bool(dup2.any()):
        if id_col and id_col in df.columns:
            sid = df[id_col].fillna("").astype(str).str[-4:]
            label = label + " #" + sid
        else:
            # fallback: append row index
            label = label + " #" + df.reset_index().index.astype(str)

    return label.astype(str).str.slice(0, int(max_len))


def _attach_account_name(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or meta is None or meta.empty:
        return df
    out = df.copy()
    if "customer_id" in out.columns:
        out["customer_id"] = pd.to_numeric(out["customer_id"], errors="coerce").astype("Int64")
        out = out.dropna(subset=["customer_id"]).copy()
        out["customer_id"] = out["customer_id"].astype("int64")
        out = out.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
    return out

def _chart_progress_bars(df: pd.DataFrame, label_col: str, value_col: str, x_title: str = "", top_n: int = 10, height: int = 420):
    """Rounded progress-style bars (Altair layered bars). x_title kept for backward-compatibility."""
    if df is None or df.empty:
        return None

    # unit inference
    unit = "원" if ("원" in str(x_title)) else ("%" if ("%" in str(x_title)) else "")

    d = df[[label_col, value_col]].copy()
    d[label_col] = d[label_col].astype(str).map(str.strip)
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0)

    # 같은 라벨이 여러 줄이면 합산(중복 제거)
    d = d.groupby(label_col, as_index=False)[value_col].sum()

    d = d.sort_values(value_col, ascending=False).head(int(top_n))
    d = d.sort_values(value_col, ascending=True)  # 위에서부터 큰값 보이게(가독)

    vals = d[value_col].tolist()
    max_val = float(max(vals)) if vals else 0.0
    d["__max"] = max_val

    def _fmt(v: float) -> str:
        if unit == "원":
            return f"{format_number_commas(v)}원"
        if unit == "%":
            return f"{v:.1f}%"
        return f"{format_number_commas(v)}{unit}"

    d["__label"] = d[value_col].map(lambda v: _fmt(float(v)))

    y = alt.Y(f"{label_col}:N", sort=None, title=None, axis=alt.Axis(labelLimit=260, ticks=False))
    x_bg = alt.X("__max:Q", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False))
    x_fg = alt.X(f"{value_col}:Q", title=None, axis=alt.Axis(grid=False))

    base = alt.Chart(d).encode(y=y)

    bg = base.mark_bar(cornerRadiusEnd=10, opacity=0.25, color="#B4C4D9").encode(x=x_bg)
    fg = base.mark_bar(cornerRadiusEnd=10, color="#3D9DF2").encode(x=x_fg)

    txt_layer = base.mark_text(align="left", dx=6, dy=0).encode(
        x=alt.X(f"{value_col}:Q"),
        text="__label:N",
    )

    chart = (bg + fg + txt_layer).properties(height=int(height))
    return chart



def query_ad_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트
    - fact_ad_daily를 먼저 집계 → cost TOP N만 뽑고 → 그 후 dim 조인
    - 캠페인 유형 필터가 있을 때만 dim 경유(scope)를 만들어 fact를 좁혀서 집계
    """
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    cols = get_table_columns(_engine, "dim_ad")
    ad_text_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id,
                 COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
                 COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        ad_scope AS (
          SELECT
            a.customer_id::text AS customer_id,
            a.ad_id,
            a.adgroup_id,
            {ad_text_expr} AS ad_name,
            COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
            c.campaign_name,
            c.campaign_tp
          FROM dim_ad a
          JOIN dim_adgroup g
            ON a.customer_id::text = g.customer_id::text
           AND a.adgroup_id = g.adgroup_id
          JOIN c_f c
            ON g.customer_id::text = c.customer_id
           AND g.campaign_id = c.campaign_id
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          JOIN ad_scope s
            ON f.customer_id::text = s.customer_id
           AND f.ad_id = s.ad_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          s.ad_name,
          s.adgroup_name,
          s.campaign_name,
          s.campaign_tp
        FROM top t
        JOIN ad_scope s
          ON t.customer_id = s.customer_id
         AND t.ad_id = s.ad_id
        ORDER BY t.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          {ad_text_expr} AS ad_name,
          COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM top t
        LEFT JOIN dim_ad a
          ON t.customer_id = a.customer_id::text
         AND t.ad_id = a.ad_id
        LEFT JOIN dim_adgroup g
          ON a.customer_id::text = g.customer_id::text
         AND a.adgroup_id = g.adgroup_id
        LEFT JOIN dim_campaign c
          ON g.customer_id::text = c.customer_id::text
         AND g.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    # df = df[df["campaign_type"].astype(str).str.strip() != "기타"]  # keep 기타 rows

    return df.reset_index(drop=True)


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_ad_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    ✅ 1회 쿼리로 소재 탭에 필요한 데이터 동시 확보
    - 광고비 기준 TopN (topn_cost)
    - 클릭 TopK, 전환 TopK (top_k)
    * base 집계 후 (cost/clk/conv) 각각 LIMIT로 뽑아 UNION → 그 다음 DIM 조인
    """
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    cols = get_table_columns(_engine, "dim_ad")
    ad_text_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id,
                 COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
                 COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        ad_scope AS (
          SELECT
            a.customer_id::text AS customer_id,
            a.ad_id,
            a.adgroup_id,
            {ad_text_expr} AS ad_name,
            COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
            c.campaign_name,
            c.campaign_tp
          FROM dim_ad a
          JOIN dim_adgroup g
            ON a.customer_id::text = g.customer_id::text
           AND a.adgroup_id = g.adgroup_id
          JOIN c_f c
            ON g.customer_id::text = c.customer_id
           AND g.campaign_id = c.campaign_id
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          JOIN ad_scope s
            ON f.customer_id::text = s.customer_id
           AND f.ad_id = s.ad_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          s.ad_name,
          s.adgroup_name,
          s.campaign_name,
          s.campaign_tp
        FROM picked p
        JOIN ad_scope s
          ON p.customer_id = s.customer_id
         AND p.ad_id = s.ad_id
        ORDER BY p.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          {ad_text_expr} AS ad_name,
          COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM picked p
        LEFT JOIN dim_ad a
          ON p.customer_id = a.customer_id::text
         AND p.ad_id = a.ad_id
        LEFT JOIN dim_adgroup g
          ON a.customer_id::text = g.customer_id::text
         AND a.adgroup_id = g.adgroup_id
        LEFT JOIN dim_campaign c
          ON g.customer_id::text = c.customer_id::text
         AND g.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    # df = df[df["campaign_type"].astype(str).str.strip() != "기타"]  # keep 기타 rows

    return df.reset_index(drop=True)

@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def query_keyword_bundle(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 300,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트 (v7.3.1)
    - fact_keyword_daily는 "집계(base)"만 하고,
      cost TOP N / clk TOP10 / conv TOP10만 골라서(dim 조인 포함) 반환
    - dim 조인은 선택된 keyword_id들에 대해서만 수행 → 대폭 가벼움
    """
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_keyword") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_sum = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"

    # dim_keyword 키워드 컬럼명 호환
    kw_cols = get_table_columns(_engine, "dim_keyword")
    if "keyword" in kw_cols:
        kw_expr = "k.keyword"
    elif "keyword_name" in kw_cols:
        kw_expr = "k.keyword_name"
    else:
        kw_expr = "''::text"

    # cid filter (TEXT/BIGINT 모두 안전)
    cid_clause = ""
    if customer_ids:
        cid_clause = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(customer_ids))})"

    cid_scope_clause = ""
    if customer_ids:
        cid_scope_clause = f"AND k.customer_id::text IN ({_sql_in_str_list(list(customer_ids))})"

    # type filter는 campaign_tp 키로 (더 빠름)
    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    type_clause = ""
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        type_clause = f"AND (LOWER(COALESCE(c.campaign_tp,'')) IN ({tp_list}) OR COALESCE(NULLIF(trim(c.campaign_tp),''),'') = '')"

    sql = f"""
    WITH scope AS (
      SELECT
        k.customer_id::text AS customer_id,
        k.keyword_id::text  AS keyword_id,
        COALESCE(NULLIF(TRIM({kw_expr}),''),'') AS keyword,
        k.adgroup_id::text  AS adgroup_id,
        COALESCE(NULLIF(TRIM(g.adgroup_name),''),'') AS adgroup_name,
        g.campaign_id::text AS campaign_id,
        COALESCE(NULLIF(TRIM(c.campaign_name),''),'') AS campaign_name,
        COALESCE(NULLIF(TRIM(c.campaign_tp),''),'')   AS campaign_tp,
        CASE
          WHEN lower(trim(c.campaign_tp)) IN ('web_site','website','power_link','powerlink') THEN '파워링크'
          WHEN lower(trim(c.campaign_tp)) IN ('shopping','shopping_search') THEN '쇼핑검색'
          WHEN lower(trim(c.campaign_tp)) IN ('power_content','power_contents','powercontent') THEN '파워콘텐츠'
          WHEN lower(trim(c.campaign_tp)) IN ('place','place_search') THEN '플레이스'
          WHEN lower(trim(c.campaign_tp)) IN ('brand_search','brandsearch') THEN '브랜드검색'
          ELSE '기타'
        END AS campaign_type_label
      FROM dim_keyword k
      LEFT JOIN dim_adgroup g
        ON k.customer_id::text = g.customer_id::text
       AND k.adgroup_id::text = g.adgroup_id::text
      LEFT JOIN dim_campaign c
        ON g.customer_id::text = c.customer_id::text
       AND g.campaign_id::text = c.campaign_id::text
      WHERE 1=1
        {type_clause}
        {cid_scope_clause}
    ),
    base AS (
      SELECT
        fk.customer_id::text AS customer_id,
        fk.keyword_id::text  AS keyword_id,
        SUM(fk.imp)  AS imp,
        SUM(fk.clk)  AS clk,
        SUM(fk.cost) AS cost,
        SUM(fk.conv) AS conv,
        {sales_sum}  AS sales
      FROM fact_keyword_daily fk
      JOIN scope s
        ON fk.customer_id::text = s.customer_id
       AND fk.keyword_id::text  = s.keyword_id
      WHERE fk.dt BETWEEN :d1 AND :d2
        {cid_clause}
      GROUP BY fk.customer_id::text, fk.keyword_id::text
    ),
    top_cost0 AS (
      SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :topn_cost
    ),
    top_cost AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY cost DESC NULLS LAST) AS rn_cost
      FROM top_cost0
    ),
    top_clk0 AS (
      SELECT * FROM base ORDER BY clk DESC NULLS LAST LIMIT 10
    ),
    top_clk AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY clk DESC NULLS LAST) AS rn_clk
      FROM top_clk0
    ),
    top_conv0 AS (
      SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT 10
    ),
    top_conv AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY conv DESC NULLS LAST) AS rn_conv
      FROM top_conv0
    ),
    picked AS (
      SELECT
        customer_id,
        keyword_id,
        MIN(rn_cost) AS rn_cost,
        MIN(rn_clk)  AS rn_clk,
        MIN(rn_conv) AS rn_conv
      FROM (
        SELECT customer_id, keyword_id, rn_cost, NULL::int rn_clk, NULL::int rn_conv FROM top_cost
        UNION ALL
        SELECT customer_id, keyword_id, NULL::int rn_cost, rn_clk, NULL::int rn_conv FROM top_clk
        UNION ALL
        SELECT customer_id, keyword_id, NULL::int rn_cost, NULL::int rn_clk, rn_conv FROM top_conv
      ) u
      GROUP BY customer_id, keyword_id
    )
    SELECT
      p.customer_id,
      p.keyword_id,
      b.imp, b.clk, b.cost, b.conv, b.sales,
      p.rn_cost, p.rn_clk, p.rn_conv,
      s.keyword, s.adgroup_name, s.campaign_name, s.campaign_tp, s.campaign_type_label
    FROM picked p
    JOIN base b
      ON p.customer_id = b.customer_id
     AND p.keyword_id  = b.keyword_id
    LEFT JOIN scope s
      ON b.customer_id = s.customer_id
     AND b.keyword_id  = s.keyword_id
    ORDER BY COALESCE(p.rn_cost, 999999), b.cost DESC NULLS LAST
    """

    params = {"d1": str(d1), "d2": str(d2), "topn_cost": int(topn_cost)}
    df = sql_read(_engine, sql, params)
    return df if df is not None else pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0)) AS sales" if "sales" in fk_cols else "0::bigint AS sales"

    kw_cols = get_table_columns(_engine, "dim_keyword")
    if "keyword" in kw_cols:
        kw_expr = "k.keyword"
    elif "keyword_name" in kw_cols:
        kw_expr = "k.keyword_name"
    else:
        kw_expr = "''::text"

    in_clause = ""
    if customer_ids:
        in_clause = f" AND fk.customer_id::text IN ({_sql_in_str_list(list(customer_ids))}) "

    # type filter는 alias 참조 문제 때문에 마지막에 적용
    type_filter = ""
    if type_sel:
        tquoted = ",".join(["'" + str(t).replace("'", "''") + "'" for t in type_sel])
        type_filter = f" AND campaign_type_label IN ({tquoted}) "

    sql = f"""
    WITH base AS (
        SELECT
            fk.customer_id::text AS customer_id,
            fk.keyword_id::text AS keyword_id,
            SUM(fk.imp) AS imp,
            SUM(fk.clk) AS clk,
            SUM(fk.cost) AS cost,
            SUM(fk.conv) AS conv,
            {sales_expr}
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2
        {in_clause}
        GROUP BY fk.customer_id::text, fk.keyword_id::text
    ),
    joined AS (
        SELECT
            b.*,
            COALESCE(NULLIF(TRIM({kw_expr}),''),'') AS keyword,
            COALESCE(NULLIF(TRIM(g.adgroup_name),''),'') AS adgroup_name,
            COALESCE(NULLIF(TRIM(c.campaign_name),''),'') AS campaign_name,
            CASE
                WHEN lower(trim(c.campaign_tp)) IN ('web_site','website','power_link','powerlink') THEN '파워링크'
                WHEN lower(trim(c.campaign_tp)) IN ('shopping','shopping_search') THEN '쇼핑검색'
                WHEN lower(trim(c.campaign_tp)) IN ('power_content','power_contents','powercontent') THEN '파워콘텐츠'
                WHEN lower(trim(c.campaign_tp)) IN ('place','place_search') THEN '플레이스'
                WHEN lower(trim(c.campaign_tp)) IN ('brand_search','brandsearch') THEN '브랜드검색'
                ELSE '기타'
            END AS campaign_type_label
        FROM base b
        LEFT JOIN dim_keyword k
            ON b.customer_id = k.customer_id::text AND b.keyword_id = k.keyword_id::text
        LEFT JOIN dim_adgroup g
            ON k.customer_id::text = g.customer_id::text AND k.adgroup_id::text = g.adgroup_id::text
        LEFT JOIN dim_campaign c
            ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text
        WHERE 1=1
    ),
    ranked AS (
        SELECT
            j.*,
            ROW_NUMBER() OVER (ORDER BY j.cost DESC NULLS LAST) AS rn_cost,
            ROW_NUMBER() OVER (ORDER BY j.clk DESC NULLS LAST) AS rn_clk,
            ROW_NUMBER() OVER (ORDER BY j.conv DESC NULLS LAST) AS rn_conv
        FROM joined j
        {type_filter}
    )
    SELECT *
    FROM ranked
    WHERE rn_cost <= :topn_cost OR rn_clk <= 10 OR rn_conv <= 10
    ORDER BY rn_cost ASC
    """

    params = {"d1": str(d1), "d2": str(d2), "topn_cost": int(topn_cost)}
    df = sql_read(_engine, sql, params)
    return df if df is not None else pd.DataFrame()


# -----------------------------
# Rates
# -----------------------------
def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    out["ctr"] = (out["clk"] / out["imp"].replace(0, np.nan)) * 100
    out["cpc"] = out["cost"] / out["clk"].replace(0, np.nan)
    out["cpa"] = out["cost"] / out["conv"].replace(0, np.nan)
    out["roas"] = (out["sales"] / out["cost"].replace(0, np.nan)) * 100

    return out




# -----------------------------
# Period comparison (DoD / WoW / MoM)
# -----------------------------

def _last_day_of_month(y: int, m: int) -> int:
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    return (nxt - timedelta(days=1)).day


def _shift_month(d: date, months: int) -> date:
    """Shift month while clamping day (e.g. Mar 31 -> Feb 28/29)."""
    base = (d.year * 12) + (d.month - 1) + int(months)
    y = base // 12
    m = (base % 12) + 1
    day = min(int(d.day), _last_day_of_month(int(y), int(m)))
    return date(int(y), int(m), int(day))


def _period_compare_range(d1: date, d2: date, mode: str) -> Tuple[date, date]:
    mode = str(mode or "").strip()
    if mode == "전일대비":
        return d1 - timedelta(days=1), d2 - timedelta(days=1)
    if mode == "전주대비":
        return d1 - timedelta(days=7), d2 - timedelta(days=7)
    # 전월대비 (default)
    return _shift_month(d1, -1), _shift_month(d2, -1)


def _safe_div(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return float(a) / float(b)
    except Exception:
        return 0.0


def _pct_change(curr: float, prev: float) -> Optional[float]:
    """Percent change. If prev==0 and curr>0 -> None (N/A)."""
    if prev == 0:
        return 0.0 if curr == 0 else None
    return (float(curr) - float(prev)) / float(prev) * 100.0


def _pct_to_str(p: Optional[float]) -> str:
    """Signed percent string. Robust to None/NaN."""
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        return f"{float(p):+.1f}%"
    except Exception:
        return "—"


def _pct_to_arrow(p: Optional[float]) -> str:
    """Arrow percent string (▲/▼). Robust to None/NaN."""
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        p = float(p)
        if p > 0:
            return f"▲ {abs(p):.1f}%"
        if p < 0:
            return f"▼ {abs(p):.1f}%"
        return f"• {abs(p):.1f}%"
    except Exception:
        return "—"


def _fmt_point(p: Optional[float]) -> str:
    """Point change string like +1.2p. Robust to None/NaN."""
    try:
        if p is None or (isinstance(p, float) and math.isnan(p)) or (hasattr(pd, "isna") and pd.isna(p)):
            return "—"
        return f"{float(p):+.1f}p"
    except Exception:
        return "—"


@st.cache_data(hash_funcs=_HASH_FUNCS, ttl=300, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> Dict[str, float]:
    entity = str(entity or "").lower().strip()
    try:
        if entity == "campaign":
            ts = query_campaign_timeseries(_engine, d1, d2, cids, type_sel)
        elif entity == "keyword":
            ts = query_keyword_timeseries(_engine, d1, d2, cids, type_sel)
        else:
            ts = query_ad_timeseries(_engine, d1, d2, cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is None or ts.empty:
        return {"imp": 0.0, "clk": 0.0, "cost": 0.0, "conv": 0.0, "sales": 0.0, "ctr": 0.0, "cpc": 0.0, "cpa": 0.0, "roas": 0.0}

    def _sum(col: str) -> float:
        if col not in ts.columns:
            return 0.0
        return float(pd.to_numeric(ts[col], errors="coerce").fillna(0).sum())

    imp = _sum("imp")
    clk = _sum("clk")
    cost = _sum("cost")
    conv = _sum("conv")
    sales = _sum("sales")
    ctr = _safe_div(clk, imp) * 100.0
    cpc = _safe_div(cost, clk)
    cpa = _safe_div(cost, conv)
    roas = _safe_div(sales, cost) * 100.0

    return {"imp": imp, "clk": clk, "cost": cost, "conv": conv, "sales": sales, "ctr": ctr, "cpc": cpc, "cpa": cpa, "roas": roas}


def _chart_delta_bars(delta_df: pd.DataFrame, height: int = 260):
    """Delta bar chart (Altair).
    - 증가(+) = 빨강, 감소(-) = 파랑
    - 기준점(0) 쪽은 '평평', 끝쪽은 '둥글게' 보이도록 처리
      (전체 바를 라운드로 그리고, 0 근처만 사각 오버레이로 덮어 baseline을 평평하게 만듦)
    """
    if delta_df is None or delta_df.empty:
        return None

    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["change_pct"] = pd.to_numeric(d["change_pct"], errors="coerce").fillna(0)
    d["dir"] = d["change_pct"].apply(lambda x: "up" if x > 0 else ("down" if x < 0 else "flat"))
    d["label"] = d["change_pct"].map(_pct_to_str)

    # 원하는 순서 유지
    if "order" in d.columns:
        d = d.sort_values("order", ascending=False)
        y_sort = alt.SortField(field="order", order="descending")
    else:
        y_sort = None

    mn = float(d["change_pct"].min())
    mx = float(d["change_pct"].max())

    # 0축을 항상 중앙에 오도록 대칭 도메인
    m_abs = max(abs(mn), abs(mx))
    if not (m_abs > 0):
        m_abs = 5.0
    pad = max(2.0, m_abs * 0.12)
    lim = m_abs + pad
    domain = [-lim, lim]

    # baseline(0) 근처를 '평평'하게 덮을 오버레이 길이(데이터 단위, %p)
    # - 너무 길면 정보가 깨지니 2%p 상한
    # - 변화폭이 작을 때는 abs의 60%만 덮어서 끝(rounded)이 남도록
    abs_pct = d["change_pct"].abs()
    flat = (abs_pct * 0.6).clip(lower=0.0, upper=2.0)
    d["flat_end"] = flat.where(d["change_pct"] >= 0, -flat)

    d["zero"] = 0.0
    d_main = d.copy()
    d_main["val"] = d_main["change_pct"]

    d_cap = d.copy()
    d_cap["val"] = d_cap["flat_end"]

    color_scale = alt.Scale(domain=["up", "down", "flat"], range=["#EF4444", "#2563EB", "#B4C4D9"])

    y_enc = alt.Y("metric:N", sort=y_sort, title=None, axis=alt.Axis(labelLimit=260))
    x_axis = alt.Axis(grid=True, gridColor="#EBEEF2")

    # 1) 전체 바(양끝 둥글게)
    bars = (
        alt.Chart(d_main)
        .mark_bar(cornerRadius=10)
        .encode(
            y=y_enc,
            x=alt.X("val:Q", title="증감율(%)", scale=alt.Scale(domain=domain), axis=x_axis),
            x2=alt.X2("zero:Q"),
            color=alt.Color("dir:N", scale=color_scale, legend=None),
            tooltip=[
                alt.Tooltip("metric:N", title="지표"),
                alt.Tooltip("change_pct:Q", title="증감율", format="+.1f"),
            ],
        )
    )

    # 2) 0 근처만 사각 오버레이로 덮어서 기준점 쪽을 평평하게
    cap = (
        alt.Chart(d_cap)
        .mark_bar(cornerRadius=0)
        .encode(
            y=y_enc,
            x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None),
            x2=alt.X2("zero:Q"),
            color=alt.Color("dir:N", scale=color_scale, legend=None),
        )
    )

    # 3) 0 기준선
    zero = (
        alt.Chart(pd.DataFrame({"val": [0.0]}))
        .mark_rule(color="#CBD5E1")
        .encode(x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None))
    )

    # 4) 라벨
    pos_text = (
        alt.Chart(d_main)
        .transform_filter("datum.val >= 0")
        .mark_text(align="left", dx=6)
        .encode(
            y=y_enc,
            x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None),
            text="label:N",
            color=alt.Color("dir:N", scale=color_scale, legend=None),
        )
    )
    neg_text = (
        alt.Chart(d_main)
        .transform_filter("datum.val < 0")
        .mark_text(align="right", dx=-6)
        .encode(
            y=y_enc,
            x=alt.X("val:Q", scale=alt.Scale(domain=domain), axis=None),
            text="label:N",
            color=alt.Color("dir:N", scale=color_scale, legend=None),
        )
    )

    chart = (bars + cap + zero + pos_text + neg_text).properties(height=int(height))
    return chart
def render_chart(obj, *, height: int | None = None) -> None:
    """Render a chart object with Streamlit (Altair-first)."""
    if obj is None:
        return
    try:
        mod = obj.__class__.__module__
    except Exception:
        mod = ""

    if mod.startswith("altair"):
        st.altair_chart(obj, use_container_width=True)
        return

    # Fallback
    try:
        st.write(obj)
    except Exception:
        pass


def render_period_compare_panel(
    engine,
    entity: str,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    key_prefix: str,
    expanded: bool = False,
) -> None:
    """Reusable panel: DoD/WoW/MoM comparison + delta bar chart."""
    with st.expander("🔁 전일/전주/전월 비교", expanded=expanded):
        # ensure CSS always applied (prevents 'KPI text spill' on rerun)
        st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
        mode = st.radio(
            "비교 기준",
            ["전일대비", "전주대비", "전월대비"],
            horizontal=True,
            index=1,
            key=f"{key_prefix}_{entity}_pcmp_mode",
        )

        b1, b2 = _period_compare_range(d1, d2, mode)

        # 비교 기간 표기 (몇 일 / 어떤 기간과 비교인지)
        try:
            n_cur = int((d2 - d1).days) + 1
            n_base = int((b2 - b1).days) + 1
        except Exception:
            n_cur, n_base = 0, 0
        st.caption(f"현재기간: {d1} ~ {d2} ({n_cur}일) · 비교기간({mode}): {b1} ~ {b2} ({n_base}일)")


        cur = get_entity_totals(engine, entity, d1, d2, cids, type_sel)
        base = get_entity_totals(engine, entity, b1, b2, cids, type_sel)


        # Quick delta summary (no duplicated KPI cards)

        dcost = cur["cost"] - base["cost"]

        dclk = cur["clk"] - base["clk"]

        dconv = cur["conv"] - base["conv"]
        dcost_pct = _pct_change(cur["cost"], base["cost"])

        dclk_pct = _pct_change(cur["clk"], base["clk"])

        dconv_pct = _pct_change(cur["conv"], base["conv"])

        droas_pct = _pct_change(cur["roas"], base["roas"])


        def _delta_chip(label: str, value: str, sign: Optional[float]) -> str:

            if sign is None:

                cls = "zero"

            elif sign > 0:

                cls = "pos"

            elif sign < 0:

                cls = "neg"

            else:

                cls = "zero"

            arrow = "•"
            if sign is not None:
                try:
                    if float(sign) > 0:
                        arrow = "▲"
                    elif float(sign) < 0:
                        arrow = "▼"
                except Exception:
                    pass

            # emphasize the percent part
            vhtml = re.sub(r"\(([^)]*)\)", r"<span class='p'>(\1)</span>", str(value))
            return f"<div class='delta-chip {cls}'><div class='l'>{label}</div><div class='v'><span class='arr'>{arrow}</span>{vhtml}</div></div>"


        chips = [

            _delta_chip("광고비", f"{format_currency(dcost)} ({_pct_to_str(dcost_pct)})", dcost_pct),

            _delta_chip("클릭", f"{format_number_commas(dclk)} ({_pct_to_str(dclk_pct)})", dclk_pct),

            _delta_chip("전환", f"{format_number_commas(dconv)} ({_pct_to_str(dconv_pct)})", dconv_pct),

            _delta_chip("ROAS", f"{_pct_to_str(droas_pct)}", droas_pct),

        ]

        st.markdown("<div class='delta-chip-row'>" + "".join(chips) + "</div>", unsafe_allow_html=True)


        # Delta bar chart
        delta_df = pd.DataFrame(
            [
                {"metric": "광고비", "change_pct": _pct_change(cur["cost"], base["cost"])},
                {"metric": "클릭", "change_pct": _pct_change(cur["clk"], base["clk"])},
                {"metric": "전환", "change_pct": _pct_change(cur["conv"], base["conv"])},
                {"metric": "매출", "change_pct": _pct_change(cur["sales"], base["sales"])},
                {"metric": "ROAS", "change_pct": _pct_change(cur["roas"], base["roas"])},
            ]
        )
        st.markdown("#### 📊 증감율(%) 막대그래프")
        # ECharts (preferred)
        if HAS_ECHARTS and st_echarts is not None:
            render_echarts_delta_bars(delta_df, height=260)
        else:
            ch = _chart_delta_bars(delta_df, height=260)
            if ch is not None:
                render_chart(ch)
# Mini table (current vs baseline)
        mini = pd.DataFrame(
            [
                ["광고비", format_currency(cur["cost"]), format_currency(base["cost"]), f"{_pct_to_str(_pct_change(cur['cost'], base['cost']))}"],
                ["클릭", format_number_commas(cur["clk"]), format_number_commas(base["clk"]), f"{_pct_to_str(_pct_change(cur['clk'], base['clk']))}"],
                ["전환", format_number_commas(cur["conv"]), format_number_commas(base["conv"]), f"{_pct_to_str(_pct_change(cur['conv'], base['conv']))}"],
                ["매출", format_currency(cur["sales"]), format_currency(base["sales"]), _pct_to_str(_pct_change(cur["sales"], base["sales"]))],
                ["ROAS(%)", format_roas(cur["roas"]), format_roas(base["roas"]), f"{_fmt_point((cur.get('roas',0.0) or 0.0) - (base.get('roas',0.0) or 0.0))}"],
            ],
            columns=["지표", "현재", "비교기간", "증감"],
        )
        ui_table_or_dataframe(mini, key=f"{key_prefix}_{entity}_pcmp_table", height=210)

# -----------------------------
# Pages
# -----------------------------

def render_filter_summary_bar(f: Dict, meta: pd.DataFrame) -> None:
    """Compact one-line summary shown on the main area (keeps the UI 'report-like')."""
    try:
        n_total = int(meta["customer_id"].nunique()) if meta is not None and not meta.empty else 0
    except Exception:
        n_total = 0

    sel = f.get("selected_customer_ids", []) or []
    n_sel = len(sel) if sel else n_total
    period = f"{f.get('start')} ~ {f.get('end')}"
    type_sel = list(f.get("type_sel", tuple()) or [])
    type_txt = "전체" if not type_sel else ", ".join(type_sel[:3]) + (" 외" if len(type_sel) > 3 else "")

    st.markdown(
        f"""
        <div class="panel" style="display:flex; align-items:center; justify-content:space-between; gap:12px; padding:12px 14px;">
          <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
            <span class="badge b-blue">선택 계정 {n_sel} / {n_total}</span>
            <span class="badge b-gray">기간 {period}</span>
            <span class="badge b-gray">유형 {type_txt}</span>
          </div>
          <div style="font-size:12px; color: rgba(2,8,23,0.55);">왼쪽 사이드바에서 필터를 바꿀 수 있어요</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    """요약(한눈에): 네이버 리포트 느낌으로 KPI를 먼저 보여주고, 상세는 아래로."""
    if not f:
        st.info("검색조건을 설정하면 요약이 표시됩니다.")
        return

    st.markdown("<div class='nv-sec-title'>요약</div>", unsafe_allow_html=True)
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids = tuple((f.get("selected_customer_ids") or f.get("customer_ids") or []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    cmp_mode = st.radio(
        "비교 기준",
        ["전일대비", "전주대비", "전월대비"],
        horizontal=True,
        index=1,
        key="ov_cmp_mode",
    )

    cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    b1, b2 = _period_compare_range(f["start"], f["end"], cmp_mode)
    base = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    def _delta_pct(key: str) -> Optional[float]:
        try:
            return _pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception:
            return None

    def _kpi_html(label: str, value: str, delta_text: str, delta_val: Optional[float]) -> str:
        cls = "neu"
        try:
            if delta_val is None or (isinstance(delta_val, float) and math.isnan(delta_val)):
                cls = "neu"
            elif float(delta_val) > 0:
                cls = "pos"
            elif float(delta_val) < 0:
                cls = "neg"
            else:
                cls = "neu"
        except Exception:
            cls = "neu"

        return f"""<div class='kpi'>
            <div class='k'>{label}</div>
            <div class='v'>{value}</div>
            <div class='d {cls}'>{delta_text}</div>
        </div>"""

    items = [
        ("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost")),
        ("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales")),
        ("전환", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv")),
        ("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{cmp_mode} {_pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas")),
        ("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {_pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr")),
        ("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc")),
    ]

    kpi_html = "<div class='kpi-row'>" + "".join(_kpi_html(a, b, c, d) for a, b, c, d in items) + "</div>"
    st.markdown(kpi_html, unsafe_allow_html=True)

    st.divider()

    # 상세(추세/Top) - 오류가 나도 KPI는 유지
    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is None or ts.empty:
            st.info("표시할 데이터가 없습니다.")
            return

        st.markdown("<div class='nv-sec-title'>추세</div>", unsafe_allow_html=True)
        render_timeseries_chart(ts, entity="campaign", key_prefix="ov_ts")
    except Exception:
        st.info("추세 데이터를 불러오는 중 오류가 발생했습니다. (KPI는 정상 표시)")

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)

    # 평균소진(최근 TOPUP_AVG_DAYS일) 계산 구간: (end - 1) 기준으로 과거 TOPUP_AVG_DAYS
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    # 월 누적 구간
    month_d1 = end_dt.replace(day=1)
    if end_dt.month == 12:
        month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_d2 = date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    if bundle is None or bundle.empty:
        st.warning("예산/잔액 데이터를 불러올 수 없습니다. (fact_bizmoney_daily/fact_campaign_daily 확인)")
        return

    biz_view = bundle.copy()
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")

    # days_cover & threshold
    biz_view["days_cover"] = pd.NA
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)

    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    # display columns (small cost)
    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)
    biz_view["전일 소진액"] = biz_view["y_cost"].map(format_currency)

    def _fmt_days(d):
        if pd.isna(d) or d is None:
            return "-"
        try:
            dd = float(d)
        except Exception:
            return "-"
        if dd > 99:
            return "99+일"
        return f"{dd:.1f}일"

    biz_view["D-소진"] = biz_view["days_cover"].map(_fmt_days)
    biz_view["확인일자"] = biz_view["last_update"]

    # summary
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    with c1:
        ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', key='m_total_balance')
    with c2:
        ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3:
        ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', key='m_need_topup')

    st.divider()

    need_topup = count_low_balance
    ok_topup = int(len(biz_view) - need_topup)
    st.markdown(
        f"<span class='badge b-red'>충전필요 {need_topup}건</span>"
        f"<span class='badge b-green'>여유 {ok_topup}건</span>",
        unsafe_allow_html=True,
    )

    show_only_topup = st.checkbox("충전필요만 보기", value=False)

    biz_view["_rank"] = biz_view["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    view_cols = ["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]
    display_df = biz_view[view_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    ui_table_or_dataframe(display_df, key="budget_biz_table", height=520)
    render_download_compact(display_df, f"예산_잔액_{f['start']}_{f['end']}", "budget", "budget")

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")

    # budget status
    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

    def _status(rate: float, budget: int):
        if budget == 0:
            return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0:
            return ("🔴 초과", "초과", 0)
        if rate >= 0.9:
            return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)

    tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
    budget_view["상태"] = tmp[0]
    budget_view["status_text"] = tmp[1]
    budget_view["_rank"] = tmp[2].astype(int)

    cnt_over = int((budget_view["status_text"] == "초과").sum())
    cnt_warn = int((budget_view["status_text"] == "주의").sum())
    cnt_unset = int((budget_view["status_text"] == "미설정").sum())

    st.markdown(
        f"<span class='badge b-red'>초과 {cnt_over}건</span>"
        f"<span class='badge b-yellow'>주의 {cnt_warn}건</span>"
        f"<span class='badge b-gray'>미설정 {cnt_unset}건</span>",
        unsafe_allow_html=True,
    )

    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

    budget_view_disp = budget_view.copy()
    budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
    budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
    budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].map(lambda x: round(float(x), 1) if pd.notna(x) else 0.0)

    disp_cols = ["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]
    table_df = budget_view_disp[disp_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    c1, c2 = st.columns([3, 1])
    with c1:
        render_budget_month_table_with_bars(table_df, key="budget_month_table", height=520)
        render_download_compact(table_df, f"월예산_{f['start']}_{f['end']}", "monthly_budget", "mb")

    with c2:
        st.markdown(
            """
            <div class="panel" style="line-height:1.85; font-size:14px; background: rgba(235,238,242,0.75);">
              <b>상태 가이드</b><br><br>
              🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
              🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
              🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
              ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ✅ 안정적인 폼 기반 업데이트 (data_editor 제거)
    st.markdown("#### ✍️ 월 예산 수정 (선택 → 입력 → 저장)")
    opts = budget_view_disp[["customer_id", "account_name"]].copy()
    opts["label"] = opts["account_name"].astype(str) + "  (" + opts["customer_id"].astype(str) + ")"
    labels = opts["label"].tolist()
    label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

    with st.form("budget_update_form", clear_on_submit=False):
        sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
        cur_budget = 0
        if labels:
            cid = int(label_to_cid.get(sel, 0))
            cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == cid, "monthly_budget_val"].iloc[0])
        new_budget = st.text_input("새 월 예산(원) (예: 500000 또는 500,000)", value=format_number_commas(cur_budget) if labels else "0")
        submitted = st.form_submit_button("💾 저장", use_container_width=True)

    if submitted and labels:
        cid = int(label_to_cid.get(sel, 0))
        nb = parse_currency(new_budget)
        update_monthly_budget(engine, cid, nb)
        st.success("수정 완료. (캐시 갱신)")
        st.cache_data.clear()
        st.rerun()


def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")



def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🚀 성과 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_campaign", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)')
        return

    type_sel = tuple(f.get("type_sel", []) or [])

    # -----------------------------
    # 1) Main list: 캠페인 단위 "번들 집계" (빠르고 DB 부담 적음)
    # -----------------------------
    try:
        bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 200), top_k=10)
    except Exception:
        bundle = pd.DataFrame()

    if bundle is None or bundle.empty:
        st.warning("데이터 없음 (오늘 데이터는 수집 지연으로 비어있을 수 있어요. 기본값인 **어제**로 확인해보세요.)")
        return

    # 메타(업체명/담당자) 부착
    bundle = bundle.copy()
    bundle["customer_id"] = pd.to_numeric(bundle["customer_id"], errors="coerce").astype("Int64")
    bundle = bundle.dropna(subset=["customer_id"]).copy()
    bundle["customer_id"] = bundle["customer_id"].astype("int64")
    bundle = _attach_account_name(bundle, meta)
    if "manager" in meta.columns:
        try:
            m_map = meta.set_index("customer_id")["manager"].to_dict()
            bundle["manager"] = bundle["customer_id"].map(m_map)
        except Exception:
            bundle["manager"] = ""

    bundle = add_rates(bundle)

    # TOP5
    top_cost = bundle[pd.to_numeric(bundle.get("rn_cost", np.nan), errors="coerce").between(1,5)].sort_values("rn_cost") if "rn_cost" in bundle.columns else bundle.sort_values("cost", ascending=False).head(5)
    top_clk = bundle[pd.to_numeric(bundle.get("rn_clk", np.nan), errors="coerce").between(1,5)].sort_values("rn_clk") if "rn_clk" in bundle.columns else bundle.sort_values("clk", ascending=False).head(5)
    top_conv = bundle[pd.to_numeric(bundle.get("rn_conv", np.nan), errors="coerce").between(1,5)].sort_values("rn_conv") if "rn_conv" in bundle.columns else bundle.sort_values("conv", ascending=False).head(5)

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["업체명", "캠페인", metric])
        x = df.copy()
        if metric == "광고비":
            x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else:
            x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        x = x.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"})
        keep_cols = [c for c in ["업체명", "캠페인", metric] if c in x.columns]
        return x[keep_cols]

    with st.expander("📌 성과별 TOP5 (캠페인)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='camp_top5_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='camp_top5_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='camp_top5_conv', height=240)

    st.divider()

    # -----------------------------
    # 2) Trend / Compare (전체/선택 캠페인)
    #    - 상세 토글 ON일 때만 시계열 쿼리 수행
    # -----------------------------
    st.markdown("### 📈 기간 추세")
    render_period_compare_panel(engine, "campaign", f["start"], f["end"], cids, type_sel, key_prefix="camp", expanded=False)

    show_detail = st.toggle("상세(캠페인 추세/표) 보기", value=False, key="camp_detail_toggle")

    # 캠페인 선택
    multi_acc = bundle["customer_id"].nunique() > 1
    bundle["label"] = bundle.apply(lambda r: f'{r.get("account_name","")} · {r.get("campaign_name","")}' if multi_acc else str(r.get("campaign_name","")), axis=1)
    options = ["(전체 캠페인)"] + bundle["label"].dropna().astype(str).unique().tolist()
    sel = st.selectbox("캠페인 선택", options, index=0, key="camp_select")

    ts = pd.DataFrame()
    if show_detail:
        try:
            if sel == "(전체 캠페인)":
                ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
            else:
                # label -> customer_id/campaign_id 찾기
                row = bundle[bundle["label"] == sel].head(1)
                if not row.empty:
                    cid = int(row.iloc[0]["customer_id"])
                    camp_id = int(row.iloc[0]["campaign_id"])
                    ts = query_campaign_one_timeseries(engine, f["start"], f["end"], cid, camp_id)
        except Exception:
            ts = pd.DataFrame()

    if show_detail and ts is not None and not ts.empty:
        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="camp_trend_metric",
        )
        ts2 = ts.copy()
        # ROAS 계산
        if "sales" in ts2.columns and "cost" in ts2.columns:
            ts2["roas"] = np.where(pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) > 0,
                                   pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) * 100.0,
                                   0.0)
        else:
            ts2["roas"] = 0.0

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None:
                    render_chart(ch)

        if metric_sel == '광고비':
            _render('cost', '광고비(원)')
        elif metric_sel == '클릭':
            _render('clk', '클릭')
        elif metric_sel == '전환':
            _render('conv', '전환')
        else:
            _render('roas', 'ROAS(%)')

    # -----------------------------
    # 3) Main table: 비용 TOP N
    # -----------------------------
    df = bundle.copy()
    if "rn_cost" in df.columns:
        df = df[pd.to_numeric(df["rn_cost"], errors="coerce").between(1, top_n)]
        df = df.sort_values("rn_cost")
    else:
        df = df.sort_values("cost", ascending=False).head(top_n)

    # 출력용(표)
    display_df = df.rename(
        columns={
            "account_name": "업체명",
            "campaign_type": "캠페인유형",
            "campaign_name": "캠페인",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "sales": "매출",
        }
    )
    # 우측 정렬/퍼센트/원 표기 등은 기존 헬퍼가 처리
    display_df = finalize_display_cols(display_df)

    render_big_table(display_df, key="camp_main_grid", height=560)
    render_download_compact(display_df, f"성과_캠페인_TOP{top_n}_{f['start']}_{f['end']}", "campaign", "camp")


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🔎 성과 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)')
        return

    type_sel = tuple(f.get("type_sel", []) or [])
    top_n = int(f.get("top_n_keyword", 300))

    bundle = query_keyword_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음 (오늘 데이터는 수집 지연으로 비어있을 수 있어요. 기본값인 **어제**로 확인해보세요.)")
        return

    # TOP10
    top_cost = bundle[pd.to_numeric(bundle["rn_cost"], errors="coerce").between(1,10)].sort_values("rn_cost")
    top_clk = bundle[pd.to_numeric(bundle["rn_clk"], errors="coerce").between(1,10)].sort_values("rn_clk")
    top_conv = bundle[pd.to_numeric(bundle["rn_conv"], errors="coerce").between(1,10)].sort_values("rn_conv")

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["업체명", "키워드", metric])
        x = df.copy()
        x["customer_id"] = pd.to_numeric(x["customer_id"], errors="coerce").astype("Int64")
        x = x.dropna(subset=["customer_id"]).copy()
        x["customer_id"] = x["customer_id"].astype("int64")
        x = x.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
        if metric == "광고비":
            x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else:
            x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        return x.rename(columns={"account_name": "업체명", "keyword": "키워드"})[["업체명", "키워드", metric]]

    with st.expander("📌 성과별 TOP10 키워드", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP10")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='kw_top10_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP10")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='kw_top10_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP10")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='kw_top10_conv', height=240)

    
    # (중복 그래프 제거)

    st.divider()
    # Top N list (광고비 기준)
    df = bundle[bundle["rn_cost"] <= top_n].sort_values("rn_cost").copy()
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df = add_rates(df)

    # -----------------------------
    # 📈 Trend (Altair)
    # -----------------------------
    try:
        ts = query_keyword_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum())
        total_clk = float(ts["clk"].sum())
        total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_kw_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_kw_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_kw_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_kw_roas")

        render_period_compare_panel(engine, "keyword", f["start"], f["end"], cids, type_sel, key_prefix="kw", expanded=False)

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="kw_trend_metric",
        )
        ts2 = ts.copy()

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None:
                    render_chart(ch)

        if metric_sel == '광고비':
            _render('cost', '광고비(원)')
        elif metric_sel == '클릭':
            _render('clk', '클릭')
        elif metric_sel == '전환':
            _render('conv', '전환')
        else:
            sales_s = pd.to_numeric(ts2['sales'], errors='coerce').fillna(0) if 'sales' in ts2.columns else pd.Series([0.0] * len(ts2))
            ts2['roas'] = (sales_s / ts2['cost'].replace(0, np.nan)) * 100
            ts2['roas'] = pd.to_numeric(ts2['roas'], errors='coerce').fillna(0)
            _render('roas', 'ROAS(%)')

        st.divider()


    df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    view = df.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_type_label": "캠페인유형",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "keyword": "키워드",
            "imp": "노출",
            "clk": "클릭",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cost": "비용",
            "conv": "전환",
            "cpa": "CPA",
            "sales": "매출",
            "roas": "ROAS(%)",
        }
    )

    view["비용"] = pd.to_numeric(view["비용"], errors="coerce").fillna(0).map(format_currency)
    view["CPC"] = pd.to_numeric(view["CPC"], errors="coerce").fillna(0).map(format_currency)
    view["CPA"] = pd.to_numeric(view["CPA"], errors="coerce").fillna(0).map(format_currency)
    view["매출"] = pd.to_numeric(view.get("매출", 0), errors="coerce").fillna(0).map(format_currency)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드", "노출", "클릭", "CTR(%)", "CPC", "비용", "전환", "CPA", "매출", "ROAS(%)"]
    out_df = view[cols].copy()
    out_df["노출"] = pd.to_numeric(out_df["노출"], errors="coerce").fillna(0).astype(int)
    out_df["클릭"] = pd.to_numeric(out_df["클릭"], errors="coerce").fillna(0).astype(int)
    out_df["전환"] = pd.to_numeric(out_df["전환"], errors="coerce").fillna(0).astype(int)

    render_big_table(out_df, key='kw_big_table', height=620)
    render_download_compact(out_df, f"키워드성과_TOP{top_n}_{f['start']}_{f['end']}", "keyword", "kw")


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🧩 성과 (소재)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_ad", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)')
        return

    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음 (dim_ad/dim_adgroup/dim_campaign 또는 fact_ad_daily 확인)")
        return

    df = _perf_common_merge_meta(bundle, meta)
    df = add_rates(df)

    # -----------------------------
    # 📈 Trend (Altair)
    # -----------------------------
    try:
        ts = query_ad_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum())
        total_clk = float(ts["clk"].sum())
        total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_ad_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_ad_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_ad_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_ad_roas")

        render_period_compare_panel(engine, "ad", f["start"], f["end"], cids, type_sel, key_prefix="ad", expanded=False)

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="ad_trend_metric",
        )
        ts2 = ts.copy()

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None:
                    render_chart(ch)

        if metric_sel == '광고비':
            _render('cost', '광고비(원)')
        elif metric_sel == '클릭':
            _render('clk', '클릭')
        elif metric_sel == '전환':
            _render('conv', '전환')
        else:
            sales_s = pd.to_numeric(ts2['sales'], errors='coerce').fillna(0) if 'sales' in ts2.columns else pd.Series([0.0] * len(ts2))
            ts2['roas'] = (sales_s / ts2['cost'].replace(0, np.nan)) * 100
            ts2['roas'] = pd.to_numeric(ts2['roas'], errors='coerce').fillna(0)
            _render('roas', 'ROAS(%)')

        st.divider()



    # -----------------
    # TOP5 (비용/클릭/전환)
    # -----------------
    top_cost = df.sort_values("cost", ascending=False).head(5)
    top_clk = df.sort_values("clk", ascending=False).head(5)
    top_conv = df.sort_values("conv", ascending=False).head(5)

    def _fmt_top(dfx: pd.DataFrame, metric: str) -> pd.DataFrame:
        if dfx is None or dfx.empty:
            return pd.DataFrame(columns=["업체명", "캠페인", "소재내용", metric])
        x = dfx.copy()
        x["업체명"] = x.get("account_name", "")
        x["캠페인"] = x.get("campaign_name", "")
        x["소재내용"] = x.get("ad_name", "")
        if metric == "광고비":
            x[metric] = x.get("cost", 0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else:
            x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)
        return x[["업체명", "캠페인", "소재내용", metric]]

    with st.expander("📌 성과별 TOP5 (소재)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='ad_top5_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='ad_top5_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='ad_top5_conv', height=240)
    # (삭제) 📊 소재 광고비 TOP10 그래프 - 중복/불필요로 제거


    st.divider()
    # -----------------
    # Main table (비용 TOP N)
    # -----------------
    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()

    disp = main_df.copy()
    disp["cost"] = disp["cost"].apply(format_currency)
    disp["sales"] = disp["sales"].apply(format_currency)
    disp["cpc"] = disp["cpc"].apply(format_currency)
    disp["cpa"] = disp["cpa"].apply(format_currency)
    disp["roas_disp"] = disp["roas"].apply(format_roas)

    disp = disp.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "ad_id": "소재ID",
            "ad_name": "소재내용",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "sales": "전환매출",
            "roas_disp": "ROAS(%)",
        }
    )

    disp["노출"] = pd.to_numeric(disp["노출"], errors="coerce").fillna(0).astype(int)
    disp["클릭"] = pd.to_numeric(disp["클릭"], errors="coerce").fillna(0).astype(int)
    disp["전환"] = pd.to_numeric(disp["전환"], errors="coerce").fillna(0).astype(int)
    disp["CTR(%)"] = disp["CTR(%)"].astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = disp[cols].copy()


    render_big_table(view_df, key='ad_big_table', height=620)

    # 다운로드 (표 렌더 후 같은 scope에서 호출되어야 함)
    render_download_compact(view_df, f"성과_소재_TOP{top_n}_{f['start']}_{f['end']}", "ad", "ad")


def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 / 연결")

    # --- DB Ping ---
    try:
        db_ping(engine)
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    # --- accounts.xlsx sync (FIRST) ---
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    st.caption("처음 1회 동기화가 필요합니다. (업체명/커스텀 ID/담당자)")

    # repo 파일 유무 표시
    repo_exists = os.path.exists(ACCOUNTS_XLSX)
    st.caption(f"기본 경로: `{ACCOUNTS_XLSX}` {'✅' if repo_exists else '❌ (파일 없음)'}")

    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"], accept_multiple_files=False)

    colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
    with colA:
        do_sync = st.button("🔁 동기화 실행", use_container_width=True)
    with colB:
        if st.button("🧹 캐시 비우기", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.session_state.pop("_table_cols_cache", None)
            st.session_state.pop("_table_names_cache", None)
            st.success("캐시를 비웠습니다.")
            st.rerun()
    with colC:
        st.caption("필터/조회가 이상하거나 최신일이 안 바뀌면 캐시 비우기 후 재시도")

    if do_sync:
        try:
            df_src = None
            if up is not None:
                df_src = pd.read_excel(up)
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: meta {res.get('meta', 0)}건")
            # meta cache bust
            st.session_state["meta_ver"] = int(time.time())
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"동기화 실패: {e}")

    # --- Meta Preview ---
    st.divider()
    st.markdown("### 🔎 현재 dim_account_meta 상태")
    try:
        dfm = get_meta(engine)
        st.write(f"- 건수: **{len(dfm)}**")
        if dfm is None or dfm.empty:
            st.warning("dim_account_meta가 비어있습니다. 위에서 accounts.xlsx 동기화를 먼저 해주세요.")
        else:
            st_dataframe_safe(dfm.head(50), use_container_width=True, height=360)
    except Exception as e:
        st.error(f"meta 조회 실패: {e}")

    # --- Optional: index tuning ---
    st.divider()
    with st.expander("⚡ 속도 튜닝 (권장 인덱스 · 선택)", expanded=False):
        st.caption("최초 1회만 실행하면 이후 TOPN/기간 조회가 확 빨라집니다. (권한/정책에 따라 실패할 수 있음)")

        def _create_perf_indexes(_engine) -> List[str]:
            stmts = [
                "CREATE INDEX IF NOT EXISTS idx_f_campaign_dt_cid_txt_camp ON fact_campaign_daily (dt, (customer_id::text), campaign_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_keyword_dt_cid_txt_kw   ON fact_keyword_daily (dt, (customer_id::text), keyword_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_ad_dt_cid_txt_ad        ON fact_ad_daily      (dt, (customer_id::text), ad_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_biz_dt_cid_txt          ON fact_bizmoney_daily(dt, (customer_id::text));",
                "CREATE INDEX IF NOT EXISTS idx_d_campaign_cid_txt_camp   ON dim_campaign ((customer_id::text), campaign_id, campaign_tp);",
                "CREATE INDEX IF NOT EXISTS idx_d_adgroup_cid_txt_adg     ON dim_adgroup  ((customer_id::text), adgroup_id, campaign_id);",
                "CREATE INDEX IF NOT EXISTS idx_d_keyword_cid_txt_kw      ON dim_keyword  ((customer_id::text), keyword_id, adgroup_id);",
                "CREATE INDEX IF NOT EXISTS idx_d_ad_cid_txt_ad           ON dim_ad       ((customer_id::text), ad_id, adgroup_id);",
            ]
            results: List[str] = []
            with _engine.begin() as conn:
                for s in stmts:
                    try:
                        conn.execute(text(s))
                        results.append(f"✅ {s}")
                    except Exception as e:
                        results.append(f"⚠️ {s}  -> {e}")
            return results

        if st.button("⚡ 인덱스 생성 실행", use_container_width=True):
            try:
                logs = _create_perf_indexes(engine)
                for line in logs:
                    st.write(line)
                st.success("완료! 캐시 비우고 다시 조회해보세요.")
            except Exception as e:
                st.error(f"실패: {e}")


# -----------------------------
# Main
# -----------------------------



# -----------------------------
# Main
# -----------------------------

def main():
    try:
        engine = get_engine()
        latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None)
        st.error(str(e))
        return

    render_hero(latest)

    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    # --- Sidebar: navigation (desktop-first, always visible on PC) ---
    with st.sidebar:
        st.markdown("### 메뉴")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("처음 1회: accounts.xlsx 동기화가 필요합니다. 아래 '설정/연결'에서 동기화하세요.")

        nav_items = [
            "요약(한눈에)",
            "예산/잔액",
            "캠페인",
            "키워드",
            "소재",
            "설정/연결",
        ]
        if not meta_ready:
            nav_items = ["설정/연결"]

        # keep selection stable
        if not meta_ready:
            st.session_state["nav_page"] = "설정/연결"

        nav = st.radio(
            "menu",
            nav_items,
            key="nav_page",
            label_visibility="collapsed",
        )

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # Page title
    st.markdown(f"<div class='nv-h1'>{nav}</div>", unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Filters (skip on settings)
    f = None
    if nav != "설정/연결":
        if not meta_ready:
            st.error("dim_account_meta가 비어있습니다. 좌측 메뉴의 '설정/연결'에서 accounts.xlsx 동기화를 먼저 해주세요.")
            return
        dim_campaign = load_dim_campaign(engine)
        type_opts = get_campaign_type_options(dim_campaign)
        f = build_filters(meta, type_opts, engine)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Route
    if nav == "요약(한눈에)":
        page_overview(meta, engine, f)
    elif nav == "예산/잔액":
        page_budget(meta, engine, f)
    elif nav == "캠페인":
        page_perf_campaign(meta, engine, f)
    elif nav == "키워드":
        page_perf_keyword(meta, engine, f)
    elif nav == "소재":
        page_perf_ad(meta, engine, f)
    else:
        page_settings(engine)


if __name__ == "__main__":
    main()
