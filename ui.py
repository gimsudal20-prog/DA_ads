# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import os
import re
import io
import html
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
    import streamlit_shadcn_ui as ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None
    HAS_SHADCN_UI = False

# Optional AgGrid (large tables)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
    from st_aggrid.shared import GridUpdateMode, DataReturnMode
    HAS_AGGRID = True
except Exception:
    AgGrid = None
    GridOptionsBuilder = None
    JsCode = None
    GridUpdateMode = None
    DataReturnMode = None
    HAS_AGGRID = False

# Optional ECharts (fast charts)
try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

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

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

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

def _aggrid_mode(name: str):
    if name == "no_update":
        return GridUpdateMode.NO_UPDATE if 'GridUpdateMode' in globals() and GridUpdateMode is not None else "NO_UPDATE"
    if name == "as_input":
        return DataReturnMode.AS_INPUT if 'DataReturnMode' in globals() and DataReturnMode is not None else "AS_INPUT"
    return None

_AGGRID_COLDEF_CACHE: dict = {}

def _aggrid_coldefs(cols: List[str], right_cols: set, enable_filter: bool, cond_thresholds: Optional[dict] = None) -> list:
    cond_thresholds = cond_thresholds or {}
    th_key = tuple(sorted((k, round(v.get("low", 0.0), 4), round(v.get("high", 0.0), 4)) for k, v in cond_thresholds.items()))
    key = (tuple(cols), tuple(sorted(right_cols)), int(bool(enable_filter)), th_key)
    cache = _AGGRID_COLDEF_CACHE
    if key in cache:
        return cache[key]

    out = []
    for c in cols:
        cd = {"headerName": c, "field": c, "sortable": True, "filter": bool(enable_filter), "resizable": True}
        base_align = {"textAlign": "right"} if c in right_cols else {}
        
        # 숫자인 경우 1,000 단위 콤마를 찍어주는 포매터 추가
        if c in right_cols and JsCode is not None:
            cd["valueFormatter"] = JsCode("""
            function(params) {
                if (params.value == null) return '';
                if (!isNaN(params.value) && typeof params.value === 'number') {
                    return Number(params.value).toLocaleString('ko-KR');
                }
                return params.value;
            }
            """)

        th = cond_thresholds.get(c)
        if th and JsCode is not None:
            low = float(th.get("low", 0.0))
            high = float(th.get("high", 0.0))
            align_stmt = 'style.textAlign = "right";' if c in right_cols else ""
            try:
                cd["cellStyle"] = JsCode(f"""
function(params){{
  const v = params.value;
  let n = NaN;
  if(typeof v === 'number'){{ n = v; }}
  else if(v !== null && v !== undefined){{
    const s = String(v).replace(/[^0-9\\.\\-]/g,'');
    n = parseFloat(s);
  }}
  let bg = '';
  if(!isNaN(n)){{
    if(n < {low}) bg = 'rgba(239,68,68,0.10)';
    else if(n >= {high}) bg = 'rgba(37,99,235,0.10)';
  }}
  const style = {{}};
  if(bg) style.backgroundColor = bg;
  {align_stmt}
  return style;
}}
""")
            except Exception:
                cd["cellStyle"] = base_align
        else:
            if base_align:
                cd["cellStyle"] = base_align
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
    cond_thresholds: Optional[dict] = None,
) -> dict:
    right_cols = right_cols or set()
    pinned_rows = pinned_rows or []
    grid = {
        "defaultColDef": {"sortable": True, "filter": bool(enable_filter), "resizable": True},
        "columnDefs": _aggrid_coldefs(cols, right_cols, enable_filter, cond_thresholds=cond_thresholds),
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

def render_hero(latest: dict, build_tag: str = "") -> None:
    apply_global_css()
    latest = latest or {}

    def _dt(key_a: str, key_b: str) -> str:
        v = latest.get(key_a) or latest.get(key_b) or "—"
        try:
            import pandas as _pd
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

def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
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
        label2 = (label.replace(m.group(0), "").replace("  ", " ").strip()) or ""
        chip = f"<span class='chip'>{label2}</span>" if label2 else ""
        delta_html = f"<div class='d {cls}'>{chip}{arrow} {num}%</div>"

    def _kpi_formula(t: str) -> str:
        t = (t or "").strip()
        if not t: return ""
        if "ROAS" in t: return "ROAS = 전환매출 / 광고비 × 100"
        if "CTR" in t: return "CTR = 클릭 / 노출 × 100"
        if "CPC" in t: return "CPC = 광고비 / 클릭"
        if "CPA" in t: return "CPA = 광고비 / 전환"
        if "CVR" in t or "전환율" in t: return "전환율(CVR) = 전환 / 클릭 × 100"
        return ""

    _formula = _kpi_formula(title)
    _title_esc = html.escape(str(title))
    _tip_html = f"<span class='kpi-tip' title='{html.escape(_formula)}'>ⓘ</span>" if _formula else ""
    title_html = f"{_title_esc}{_tip_html}"

    st.markdown(
        f"""<div class='kpi' id='{key}'>
            <div class='k'>{title_html}</div>
            <div class='v'>{value}</div>
            {delta_html}
        </div>""",
        unsafe_allow_html=True,
    )

def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.table(df, maxHeight=height, key=key)
            return
        except Exception:
            pass
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
        if stt.startswith("🔴"): fill = "var(--nv-red)"
        elif stt.startswith("🟡"): fill = "#F59E0B"
        elif stt.startswith("🟢"): fill = "var(--nv-green)"
        else: fill = "rgba(0,0,0,.25)"
        return (
            f"<div class='nv-pbar'>"
            f"  <div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{width:.2f}%;background:{fill};'></div></div>"
            f"  <div class='nv-pbar-txt'>{pv:.1f}%</div>"
            f"</div>"
        )

    if "집행률(%)" in df.columns:
        df["집행률"] = [_bar(p, s) for p, s in zip(df["집행률(%)"].tolist(), df.get("상태", "").tolist())]
        df = df.drop(columns=["집행률(%)"])
        cols = list(df.columns)
        if "상태" in cols and "집행률" in cols:
            cols.remove("집행률")
            idx = cols.index("상태")
            cols.insert(idx, "집행률")
            df = df[cols]

    html = df.to_html(index=False, escape=False, classes="nv-table")
    html = re.sub(r"<td>([\d,]+원)</td>", r"<td class='num'>\1</td>", html)
    html = re.sub(r"<td>([\d,]+)</td>", r"<td class='num'>\1</td>", html)
    st.markdown(f"<div class='nv-table-wrap' style='max-height:{height}px'>{html}</div>", unsafe_allow_html=True)


# ==========================================
# [NEW] Dual Axis Charts (비용 vs 효율 비교)
# ==========================================
def render_echarts_dual_axis(title: str, ts: pd.DataFrame, x_col: str, bar_col: str, bar_name: str, line_col: str, line_name: str, *, height: int = 320) -> None:
    """ECharts 듀얼 축 차트: 비용(막대)과 전환/ROAS(꺾은선)를 동시 비교"""
    if not (HAS_ECHARTS and st_echarts is not None):
        return
    if ts is None or ts.empty or x_col not in ts.columns or bar_col not in ts.columns or line_col not in ts.columns:
        return

    df = ts.copy()
    if np.issubdtype(df[x_col].dtype, np.datetime64):
        df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
    else:
        try: df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
        except Exception: df[x_col] = df[x_col].astype(str)

    df[bar_col] = pd.to_numeric(df[bar_col], errors="coerce").fillna(0.0).round(0)
    df[line_col] = pd.to_numeric(df[line_col], errors="coerce").fillna(0.0).round(1)

    x = df[x_col].astype(str).tolist()
    y_bar = df[bar_col].astype(int).tolist()
    y_line = df[line_col].astype(float).tolist()

    option = {
        "title": {"show": False},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [bar_name, line_name], "bottom": 0},
        "grid": {"left": 60, "right": 60, "top": 40, "bottom": 40},
        "xAxis": [{"type": "category", "data": x, "axisTick": {"alignWithLabel": True}}],
        "yAxis": [
            {"type": "value", "name": bar_name, "position": "left", "axisLine": {"show": True, "lineStyle": {"color": "#B4C4D9"}}},
            {"type": "value", "name": line_name, "position": "right", "axisLine": {"show": True, "lineStyle": {"color": "#2563EB"}}, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": bar_name, "type": "bar", "data": y_bar, "itemStyle": {"color": "rgba(180, 196, 217, 0.6)", "borderRadius": [4, 4, 0, 0]}},
            {"name": line_name, "type": "line", "yAxisIndex": 1, "data": y_line, "smooth": True, "lineStyle": {"width": 3, "color": "#2563EB"}, "itemStyle": {"color": "#2563EB"}}
        ]
    }
    st_echarts(option, height=f"{height}px")

def _chart_dual_axis(df: pd.DataFrame, x_col: str, bar_col: str, bar_name: str, line_col: str, line_name: str, height: int = 320):
    """Altair 듀얼 축 폴백"""
    if df is None or df.empty: return None
    d = df.copy()
    d[x_col] = pd.to_datetime(d[x_col], errors="coerce").dt.strftime("%m/%d")
    
    base = alt.Chart(d).encode(x=alt.X(f"{x_col}:N", title=None, axis=alt.Axis(labelAngle=0)))
    bar = base.mark_bar(opacity=0.5, color='#B4C4D9').encode(
        y=alt.Y(f"{bar_col}:Q", title=bar_name, axis=alt.Axis(grid=False)),
        tooltip=[alt.Tooltip(f"{x_col}:N"), alt.Tooltip(f"{bar_col}:Q", title=bar_name, format=",.0f")]
    )
    line = base.mark_line(color='#2563EB', strokeWidth=3).encode(
        y=alt.Y(f"{line_col}:Q", title=line_name, axis=alt.Axis(grid=False)),
        tooltip=[alt.Tooltip(f"{x_col}:N"), alt.Tooltip(f"{line_col}:Q", title=line_name, format=",.1f")]
    )
    return alt.layer(bar, line).resolve_scale(y='independent').properties(height=height)

# 기존 라인차트 렌더러
def render_echarts_line(title: str, ts: pd.DataFrame, x_col: str, y_col: str, y_name: str, *, height: int = 260, smooth: bool = True) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if ts is None or ts.empty or x_col not in ts.columns or y_col not in ts.columns: return

    df = ts[[x_col, y_col]].copy()
    if np.issubdtype(df[x_col].dtype, np.datetime64):
        df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
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
        "series": [{"type": "line", "data": y, "smooth": smooth, "showSymbol": False, "lineStyle": {"width": 3, "color": "#2563EB"}, "areaStyle": {"opacity": 0.06}}]
    }
    st_echarts(option, height=f"{int(height)}px")

def _chart_timeseries(df: pd.DataFrame, y_col: str, y_title: str = "", *, x_col: str = "dt", y_format: str = ",.0f", height: int = 320):
    if df is None or df.empty: return None
    if x_col not in df.columns or y_col not in df.columns: return None

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

    base = alt.Chart(d).encode(
        x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending"), axis=x_axis),
        y=alt.Y(f"{y_col}:Q", axis=y_axis),
        tooltip=[alt.Tooltip("_dt_str:N", title="날짜"), alt.Tooltip(f"{y_col}:Q", title=title or y_col, format=y_format)]
    )

    area = base.mark_area(interpolate="monotone", opacity=0.08)
    line = base.mark_line(interpolate="monotone", strokeWidth=3)
    pts = base.mark_point(size=40, filled=True)
    return (area + line + pts).properties(height=int(height))

def render_echarts_delta_bars(delta_df: pd.DataFrame, *, height: int = 260) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if delta_df is None or delta_df.empty: return

    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["v"] = pd.to_numeric(d["change_pct"], errors="coerce")
    if d["v"].notna().sum() == 0:
        st.info("비교기간 데이터가 없어 증감율을 계산할 수 없습니다.")
        return

    d["v"] = d["v"].fillna(0.0).round(0).astype(int)
    lim = max(float(max(d["v"].abs().max(), 1.0)) * 1.15 + 0.5, 5.0)
    cats = d["metric"].tolist()[::-1]
    vals = d["v"].tolist()[::-1]

    data = []
    for m, v in zip(cats, vals):
        if v > 0:
            color, br, pos, fmt = "#2563EB", [0, 10, 10, 0], "right", f"+{int(round(v))}%"
        elif v < 0:
            color, br, pos, fmt = "#EF4444", [10, 0, 0, 10], "left", f"{int(round(v))}%"
        else:
            color, br, pos, fmt = "#B4C4D9", [0, 0, 0, 0], "right", "+0%"
        data.append({"value": v, "label": {"show": True, "position": pos, "formatter": fmt, "fontWeight": "bold"}, "itemStyle": {"color": color, "borderRadius": br}})

    option = {
        "grid": {"left": 70, "right": 24, "top": 12, "bottom": 26},
        "xAxis": {"type": "value", "min": -lim, "max": lim, "axisLabel": {"formatter": "{value}"}, "splitLine": {"lineStyle": {"color": "#EBEEF2"}}},
        "yAxis": {"type": "category", "data": cats, "axisTick": {"show": False}},
        "series": [{"type": "bar", "data": data, "barWidth": 20, "silent": True, "markLine": {"symbol": "none", "label": {"show": False}, "lineStyle": {"color": "#CBD5E1", "width": 2}, "data": [{"xAxis": 0}]}}],
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}}
    }
    st_echarts(option, height=f"{int(height)}px")

def _chart_delta_bars(delta_df: pd.DataFrame, height: int = 260):
    if delta_df is None or delta_df.empty: return None
    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["change_pct"] = pd.to_numeric(d["change_pct"], errors="coerce").fillna(0)
    d["dir"] = d["change_pct"].apply(lambda x: "up" if x > 0 else ("down" if x < 0 else "flat"))
    d["label"] = d["change_pct"].map(_pct_to_str)

    m_abs = max(abs(float(d["change_pct"].min())), abs(float(d["change_pct"].max())))
    lim = (m_abs if m_abs > 0 else 5.0) + max(2.0, m_abs * 0.12)
    domain = [-lim, lim]

    color_scale = alt.Scale(domain=["up", "down", "flat"], range=["#EF4444", "#2563EB", "#B4C4D9"])
    y_enc = alt.Y("metric:N", sort=None, title=None, axis=alt.Axis(labelLimit=260))

    bars = alt.Chart(d).mark_bar(cornerRadius=10).encode(
        y=y_enc,
        x=alt.X("change_pct:Q", title="증감율(%)", scale=alt.Scale(domain=domain)),
        color=alt.Color("dir:N", scale=color_scale, legend=None)
    )
    return bars.properties(height=int(height))

def render_big_table(df: pd.DataFrame, key: str, height: int = 560) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_AGGRID and AgGrid is not None:
        q = st.text_input("검색", value="", placeholder="테이블 내 검색", key=f"{key}_q")
        right_cols = {c for c in df.columns if any(k in c for k in ["노출", "클릭", "광고비", "전환", "매출", "CTR", "CPC", "CPA", "ROAS"])}

        def _to_num_series(s: pd.Series) -> pd.Series:
            if s is None: return pd.Series(dtype="float64")
            if pd.api.types.is_numeric_dtype(s): return pd.to_numeric(s, errors="coerce")
            x = s.astype(str).str.replace(r"[^0-9\.-]", "", regex=True)
            return pd.to_numeric(x, errors="coerce")

        _cond_thresholds = {}
        for _c in [c for c in df.columns if any(k in c for k in ["ROAS", "CTR"])]:
            _num = _to_num_series(df[_c]).dropna()
            if len(_num) >= 12:
                _cond_thresholds[_c] = {"low": float(_num.quantile(0.33)), "high": float(_num.quantile(0.67))}

        grid = _aggrid_grid_options(cols=list(df.columns), pinned_rows=[], right_cols=right_cols, quick_filter=q or "", enable_filter=True, cond_thresholds=_cond_thresholds)
        AgGrid(df, gridOptions=grid, height=height, fit_columns_on_grid_load=False, theme="alpine", allow_unsafe_jscode=True, update_mode=_aggrid_mode("no_update"), data_return_mode=_aggrid_mode("as_input"), key=key)
        return
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)

def render_chart(obj, *, height: int | None = None) -> None:
    if obj is None: return
    if obj.__class__.__module__.startswith("altair"): st.altair_chart(obj, use_container_width=True); return
    try: st.write(obj)
    except Exception: pass

def _df_json_to_csv_bytes(df_json: str) -> bytes:
    return pd.read_json(io.StringIO(df_json), orient="split").to_csv(index=False).encode("utf-8-sig")

def _df_json_to_xlsx_bytes(df_json: str, sheet_name: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
    return output.getvalue()

def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    if df is None or df.empty: return
    df_json = df.to_json(orient="split")
    st.markdown("<style>.stDownloadButton button { padding: 0.15rem 0.55rem !important; font-size: 0.82rem !important; min-height: 28px !important; }</style>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 8])
    with c1: st.download_button("CSV", data=_df_json_to_csv_bytes(df_json), file_name=f"{filename_base}.csv", mime="text/csv", key=f"{key_prefix}_csv", use_container_width=True)
    with c2: st.download_button("XLSX", data=_df_json_to_xlsx_bytes(df_json, sheet_name), file_name=f"{filename_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"{key_prefix}_xlsx", use_container_width=True)

def render_period_compare_panel(engine, entity: str, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], key_prefix: str, expanded: bool = False) -> None:
    with st.expander("🔁 전일/전주/전월 비교", expanded=expanded):
        apply_global_css()
        mode = st.radio("비교 기준", ["전일대비", "전주대비", "전월대비"], horizontal=True, index=1, key=f"{key_prefix}_{entity}_pcmp_mode")
        b1, b2 = _period_compare_range(d1, d2, mode)
        n_cur = int((d2 - d1).days) + 1 if d1 and d2 else 0
        n_base = int((b2 - b1).days) + 1 if b1 and b2 else 0
        st.caption(f"현재기간: {d1} ~ {d2} ({n_cur}일) · 비교기간({mode}): {b1} ~ {b2} ({n_base}일)")

        cur = get_entity_totals(engine, entity, d1, d2, cids, type_sel)
        base = get_entity_totals(engine, entity, b1, b2, cids, type_sel)

        dcost, dclk, dconv = cur["cost"] - base["cost"], cur["clk"] - base["clk"], cur["conv"] - base["conv"]
        dcost_pct, dclk_pct, dconv_pct, droas_pct = _pct_change(cur["cost"], base["cost"]), _pct_change(cur["clk"], base["clk"]), _pct_change(cur["conv"], base["conv"]), _pct_change(cur["roas"], base["roas"])

        def _delta_chip(label: str, value: str, sign: Optional[float]) -> str:
            cls = "zero" if sign is None else ("pos" if sign > 0 else "neg")
            arrow = "•" if sign is None else ("▲" if sign > 0 else "▼")
            vhtml = re.sub(r"\\(([^)]*)\\)", r"<span class='p'>(\1)</span>", str(value))
            return f"<div class='delta-chip {cls}'><div class='l'>{label}</div><div class='v'><span class='arr'>{arrow}</span>{vhtml}</div></div>"

        chips = [
            _delta_chip("광고비", f"{format_currency(dcost)} ({_pct_to_str(dcost_pct)})", dcost_pct),
            _delta_chip("클릭", f"{format_number_commas(dclk)} ({_pct_to_str(dclk_pct)})", dclk_pct),
            _delta_chip("전환", f"{format_number_commas(dconv)} ({_pct_to_str(dconv_pct)})", dconv_pct),
            _delta_chip("ROAS", f"{_pct_to_str(droas_pct)}", droas_pct),
        ]
        st.markdown("<div class='delta-chip-row'>" + "".join(chips) + "</div>", unsafe_allow_html=True)

        delta_df = pd.DataFrame([
            {"metric": "광고비", "change_pct": dcost_pct},
            {"metric": "클릭", "change_pct": dclk_pct},
            {"metric": "전환", "change_pct": dconv_pct},
            {"metric": "매출", "change_pct": _pct_change(cur["sales"], base["sales"])},
            {"metric": "ROAS", "change_pct": droas_pct},
        ])
        st.markdown("#### 📊 증감율(%) 막대그래프")
        if HAS_ECHARTS and st_echarts is not None: render_echarts_delta_bars(delta_df, height=260)
        else:
            ch = _chart_delta_bars(delta_df, height=260)
            if ch is not None: render_chart(ch)
