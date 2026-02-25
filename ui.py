# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import re
import io
import math
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import altair as alt

from styles import apply_global_css

# Optional UI components (shadcn-ui style)
try:
    import streamlit_shadcn_ui as ui  # pip install streamlit-shadcn-ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None  # type: ignore
    HAS_SHADCN_UI = False

# Optional AgGrid (large tables)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder  # pip install streamlit-aggrid
    from st_aggrid.shared import GridUpdateMode, DataReturnMode
    HAS_AGGRID = True
except Exception:
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    GridUpdateMode = None  # type: ignore
    DataReturnMode = None  # type: ignore
    HAS_AGGRID = False

# Optional ECharts (fast charts)
try:
    from streamlit_echarts import st_echarts  # pip install streamlit-echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None  # type: ignore
    HAS_ECHARTS = False

# Pull shared helpers from data.py (formatters + period compare math + freshness query)
from data import (
    format_currency,
    format_number_commas,
    format_roas,
    finalize_ctr_col,
    finalize_display_cols,
    build_campaign_summary_rows_from_numeric,
    query_latest_dates,
    _period_compare_range,
    get_entity_totals,
    _pct_change,
    _pct_to_str,
    _fmt_point,
)

# -----------------------------
# Altair (charts) - safe defaults
# -----------------------------
try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

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

        # Conditional formatting (ROAS/CTR): 목표 미달(옅은 붉은색) / 초과(옅은 푸른색)
        # - df가 문자열(예: "123%", "1,234")로 들어와도 파싱되게 처리
        if JsCode is not None and any(k in str(c) for k in ["ROAS", "CTR(%)", "CTR"]):
            try:
                cd["cellStyle"] = JsCode(
                    """
function(params){
  var v = params.value;
  var n = null;
  if(v === null || v === undefined){ n = null; }
  else if(typeof v === 'number'){ n = v; }
  else {
    var s = String(v);
    s = s.replace(/,/g,'').replace(/\s/g,'').replace('%','');
    var p = parseFloat(s);
    n = isNaN(p) ? null : p;
  }

  var style = {textAlign: 'right'};

  // ROAS
  if(String(params.colDef.field).indexOf('ROAS') !== -1){
    if(n !== null){
      style.backgroundColor = (n < 100) ? 'rgba(239,68,68,0.10)' : 'rgba(37,99,235,0.10)';
    }
    return style;
  }

  // CTR
  if(String(params.colDef.field).indexOf('CTR') !== -1){
    if(n !== null){
      if(n < 1){ style.backgroundColor = 'rgba(239,68,68,0.10)'; }
      else if(n >= 3){ style.backgroundColor = 'rgba(37,99,235,0.10)'; }
    }
    return style;
  }

  return style;
}
"""
                )
            except Exception:
                # Fall back to alignment only
                if c in right_cols:
                    cd["cellStyle"] = {"textAlign": "right"}
        elif c in right_cols:
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
# Thresholds (Budget)
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

# -----------------------------
# CSS moved to styles.py
# -----------------------------


def render_hero(latest: dict, build_tag: str = "") -> None:
    """Naver-like topbar (sticky)."""
    apply_global_css()
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

    def _kpi_tooltip(t: str) -> str:
        tt = str(t or "")
        if "ROAS" in tt:
            return "ROAS = 전환매출 ÷ 광고비 × 100"
        if "CTR" in tt:
            return "CTR = 클릭 ÷ 노출 × 100"
        if "CPC" in tt:
            return "CPC = 광고비 ÷ 클릭"
        if "CPA" in tt:
            return "CPA = 광고비 ÷ 전환"
        if "전환" in tt and "총" in tt:
            return "선택 기간 내 전환수 합계"
        if "광고비" in tt:
            return "선택 기간 내 광고비 합계"
        if "클릭" in tt:
            return "선택 기간 내 클릭수 합계"
        if "노출" in tt:
            return "선택 기간 내 노출수 합계"
        return ""

    tip = _kpi_tooltip(title)
    tip_html = f"<span class='nv-tip' data-tip='{tip}'>ⓘ</span>" if tip else ""

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
            <div class='k'>{title}{tip_html}</div>
            <div class='v'>{value}</div>
            {delta_html}
        </div>""",
        unsafe_allow_html=True,
    )


def set_filter_period(mode: str) -> Tuple[date, date]:
    """Update filters_v8 period + widget states (used by empty-state CTA buttons)."""
    today = date.today()
    sv = st.session_state.get("filters_v8", {}) or {}

    if mode == "오늘":
        d2 = today
        d1 = today
    elif mode == "어제":
        d2 = today - timedelta(days=1)
        d1 = d2
    elif mode == "최근 7일":
        d2 = today - timedelta(days=1)
        d1 = d2 - timedelta(days=6)
    elif mode == "이번 달":
        d2 = today
        d1 = date(today.year, today.month, 1)
    elif mode == "지난 달":
        first_this = date(today.year, today.month, 1)
        d2 = first_this - timedelta(days=1)
        d1 = date(d2.year, d2.month, 1)
    else:
        # direct select or unknown: keep current
        d1 = sv.get("d1") or (today - timedelta(days=1))
        d2 = sv.get("d2") or (today - timedelta(days=1))

    sv.update({"period_mode": mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv

    # sync widget states (best-effort)
    st.session_state["f_period_mode"] = mode
    st.session_state["f_d1"] = d1
    st.session_state["f_d2"] = d2
    st.session_state["f_d1_ro"] = str(d1)
    st.session_state["f_d2_ro"] = str(d2)
    return d1, d2


def render_empty_state(
    title: str = "데이터가 없습니다",
    message: str = "선택한 기간/필터 조합에서 조회된 데이터가 없어요.",
    action_period_mode: str = "최근 7일",
    action_label: str = "기간을 '최근 7일'로 변경",
    key: str = "empty_state",
) -> None:
    """Friendly empty state with a CTA button to fix common cause."""
    st.markdown(
        f"""
<div class='nv-empty'>
  <div class='ic'>📭</div>
  <div class='t'>{title}</div>
  <div class='m'>{message}</div>
</div>
""",
        unsafe_allow_html=True,
    )

    cols = st.columns([1, 3, 1])
    with cols[1]:
        if st.button(action_label, key=f"{key}_cta"):
            set_filter_period(action_period_mode)
            st.rerun()


def render_top_tabs(
    cost_df: pd.DataFrame,
    click_df: pd.DataFrame,
    conv_df: pd.DataFrame,
    key_prefix: str,
    height: int = 240,
    labels: Tuple[str, str, str] = ("💸 광고비 TOP", "🖱️ 클릭 TOP", "✅ 전환 TOP"),
) -> None:
    """TOP tables UI: use tabs instead of 3-column narrow layout."""
    t1, t2, t3 = st.tabs(list(labels))
    with t1:
        ui_table_or_dataframe(cost_df, key=f"{key_prefix}_cost", height=height)
    with t2:
        ui_table_or_dataframe(click_df, key=f"{key_prefix}_clk", height=height)
    with t3:
        ui_table_or_dataframe(conv_df, key=f"{key_prefix}_conv", height=height)


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
        apply_global_css()
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


