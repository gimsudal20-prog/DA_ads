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

# Optional UI components
try:
    import streamlit_shadcn_ui as ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None
    HAS_SHADCN_UI = False

# Optional AgGrid
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

# Optional ECharts
try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

# Import ONLY needed helpers explicitly from data.py to avoid circular/missing imports
from data import (
    format_currency,
    format_number_commas,
    format_roas,
    finalize_ctr_col,
    finalize_display_cols,
    _period_compare_range,
    get_entity_totals,
    _pct_change,
    _pct_to_str,
)

try: alt.data_transformers.disable_max_rows()
except Exception: pass

_ST_DATAFRAME = st.dataframe

def st_dataframe_safe(df, **kwargs):
    try: return _ST_DATAFRAME(df, **kwargs)
    except Exception:
        kwargs.pop("hide_index", None)
        try: return _ST_DATAFRAME(df, **kwargs)
        except Exception:
            kwargs.pop("column_config", None)
            return _ST_DATAFRAME(df, **kwargs)

def _aggrid_mode(name: str):
    if name == "no_update": return GridUpdateMode.NO_UPDATE if 'GridUpdateMode' in globals() and GridUpdateMode is not None else "NO_UPDATE"
    if name == "as_input": return DataReturnMode.AS_INPUT if 'DataReturnMode' in globals() and DataReturnMode is not None else "AS_INPUT"
    return None

_AGGRID_COLDEF_CACHE: dict = {}

def _aggrid_coldefs(cols: List[str], right_cols: set, enable_filter: bool, cond_thresholds: Optional[dict] = None) -> list:
    cond_thresholds = cond_thresholds or {}
    th_key = tuple(sorted((k, round(v.get("low", 0.0), 4), round(v.get("high", 0.0), 4)) for k, v in cond_thresholds.items()))
    key = (tuple(cols), tuple(sorted(right_cols)), int(bool(enable_filter)), th_key)
    cache = _AGGRID_COLDEF_CACHE
    if key in cache: return cache[key]

    out = []
    for c in cols:
        cd = {"headerName": c, "field": c, "sortable": True, "filter": bool(enable_filter), "resizable": True}
        base_align = {"textAlign": "right"} if c in right_cols else {}
        
        # 1,000 단위 콤마
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
            low, high = float(th.get("low", 0.0)), float(th.get("high", 0.0))
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
            except Exception: cd["cellStyle"] = base_align
        else:
            if base_align: cd["cellStyle"] = base_align
        out.append(cd)

    if len(cache) > 64: cache.clear()
    cache[key] = out
    return out

def _aggrid_grid_options(cols: List[str], pinned_rows: Optional[list] = None, right_cols: Optional[set] = None, quick_filter: str = "", enable_filter: bool = False, cond_thresholds: Optional[dict] = None) -> dict:
    right_cols, pinned_rows = right_cols or set(), pinned_rows or []
    grid = {
        "defaultColDef": {"sortable": True, "filter": bool(enable_filter), "resizable": True},
        "columnDefs": _aggrid_coldefs(cols, right_cols, enable_filter, cond_thresholds=cond_thresholds),
        "pinnedTopRowData": pinned_rows,
        "suppressRowClickSelection": True,
        "animateRows": False,
    }
    if quick_filter: grid["quickFilterText"] = quick_filter
    if JsCode is not None:
        try:
            grid["getRowStyle"] = JsCode("""function(params){ if(params.node.rowPinned){ return {backgroundColor:'rgba(148,163,184,0.18)', fontWeight:'700'}; } return {}; }""")
        except Exception: pass
    return grid

def render_hero(latest: dict, build_tag: str = "") -> None:
    apply_global_css()
    latest = latest or {}
    def _dt(key_a: str, key_b: str) -> str:
        v = latest.get(key_a) or latest.get(key_b) or "—"
        if isinstance(v, (datetime, date)): v = v.strftime("%Y-%m-%d")
        return "—" if v is None else str(v).strip()

    st.markdown(
        f"""
        <div class="nv-topbar">
          <div class="inner">
            <div><div class="nv-brand"><span class="nv-dot"></span>네이버 검색광고 리포트</div><div class="nv-sub">{build_tag}</div></div>
            <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
              <span class="nv-pill">캠페인 최신 · <b>{_dt("campaign_dt", "campaign")}</b></span>
              <span class="nv-pill">키워드 최신 · <b>{_dt("keyword_dt", "keyword")}</b></span>
              <span class="nv-pill">소재 최신 · <b>{_dt("ad_dt", "ad")}</b></span>
              <span class="nv-pill">비즈머니 최신 · <b>{_dt("bizmoney_dt", "bizmoney")}</b></span>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True
    )

def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    if os.getenv("USE_SHADCN_METRICS", "0").strip() == "1" and HAS_SHADCN_UI and ui is not None:
        try: ui.metric_card(title=title, content=value, description=desc, key=key); return
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

    _formula = ""
    if "ROAS" in title: _formula = "ROAS = 전환매출 / 광고비 × 100"
    elif "CTR" in title: _formula = "CTR = 클릭 / 노출 × 100"
    elif "CPC" in title: _formula = "CPC = 광고비 / 클릭"
    elif "CPA" in title: _formula = "CPA = 광고비 / 전환"

    _tip = f"<span class='kpi-tip' title='{html.escape(_formula)}'>ⓘ</span>" if _formula else ""
    st.markdown(f"<div class='kpi' id='{key}'><div class='k'>{html.escape(str(title))}{_tip}</div><div class='v'>{value}</div>{delta_html}</div>", unsafe_allow_html=True)

def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_SHADCN_UI and ui is not None:
        try: ui.table(df, maxHeight=height, key=key); return
        except Exception: pass
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)

def render_echarts_dual_axis(title: str, ts: pd.DataFrame, x_col: str, bar_col: str, bar_name: str, line_col: str, line_name: str, *, height: int = 320) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if ts is None or ts.empty or x_col not in ts.columns or bar_col not in ts.columns or line_col not in ts.columns: return

    df = ts.copy()
    if np.issubdtype(df[x_col].dtype, np.datetime64): df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
    else:
        try: df[x_col] = pd.to_datetime(df[x_col], errors="coerce").dt.strftime("%m/%d")
        except Exception: df[x_col] = df[x_col].astype(str)

    df[bar_col] = pd.to_numeric(df[bar_col], errors="coerce").fillna(0.0).round(0)
    df[line_col] = pd.to_numeric(df[line_col], errors="coerce").fillna(0.0).round(1)

    option = {
        "title": {"show": False}, "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [bar_name, line_name], "bottom": 0},
        "grid": {"left": 60, "right": 60, "top": 40, "bottom": 40},
        "xAxis": [{"type": "category", "data": df[x_col].astype(str).tolist(), "axisTick": {"alignWithLabel": True}}],
        "yAxis": [
            {"type": "value", "name": bar_name, "position": "left", "axisLine": {"show": True, "lineStyle": {"color": "#B4C4D9"}}},
            {"type": "value", "name": line_name, "position": "right", "axisLine": {"show": True, "lineStyle": {"color": "#2563EB"}}, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": bar_name, "type": "bar", "data": df[bar_col].astype(int).tolist(), "itemStyle": {"color": "rgba(180, 196, 217, 0.6)", "borderRadius": [4, 4, 0, 0]}},
            {"name": line_name, "type": "line", "yAxisIndex": 1, "data": df[line_col].astype(float).tolist(), "smooth": True, "lineStyle": {"width": 3, "color": "#2563EB"}, "itemStyle": {"color": "#2563EB"}}
        ]
    }
    st_echarts(option, height=f"{height}px")

def render_echarts_dow_bar(ts: pd.DataFrame, height: int = 300) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if ts is None or ts.empty or "dt" not in ts.columns: return

    df = ts.copy()
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"])
    df["weekday"] = df["dt"].dt.weekday 
    
    dow_map = {0:"월", 1:"화", 2:"수", 3:"목", 4:"금", 5:"토", 6:"일"}
    grouped = df.groupby("weekday")[["cost", "sales", "clk", "conv"]].sum().reset_index()
    
    all_days = pd.DataFrame({"weekday": range(7)})
    grouped = pd.merge(all_days, grouped, on="weekday", how="left").fillna(0)
    grouped["dow_name"] = grouped["weekday"].map(dow_map)
    
    grouped["roas"] = np.where(grouped["cost"] > 0, (grouped["sales"] / grouped["cost"]) * 100, 0)
    
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": ["광고비(원)", "ROAS(%)"], "bottom": 0},
        "grid": {"left": 60, "right": 60, "top": 40, "bottom": 40},
        "xAxis": [{"type": "category", "data": grouped["dow_name"].tolist(), "axisTick": {"alignWithLabel": True}}],
        "yAxis": [
            {"type": "value", "name": "광고비", "position": "left", "axisLine": {"show": True, "lineStyle": {"color": "#B4C4D9"}}},
            {"type": "value", "name": "ROAS(%)", "position": "right", "axisLine": {"show": True, "lineStyle": {"color": "#2563EB"}}, "splitLine": {"show": False}}
        ],
        "series": [
            {"name": "광고비(원)", "type": "bar", "data": grouped["cost"].astype(int).tolist(), "itemStyle": {"color": "rgba(180, 196, 217, 0.6)", "borderRadius": [4, 4, 0, 0]}},
            {"name": "ROAS(%)", "type": "line", "yAxisIndex": 1, "data": grouped["roas"].round(1).tolist(), "smooth": True, "lineStyle": {"width": 3, "color": "#2563EB"}, "itemStyle": {"color": "#2563EB"}}
        ]
    }
    st_echarts(option, height=f"{height}px")

def render_echarts_delta_bars(delta_df: pd.DataFrame, *, height: int = 260) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if delta_df is None or delta_df.empty: return

    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["v"] = pd.to_numeric(d["change_pct"], errors="coerce")
    if d["v"].notna().sum() == 0: return

    d["v"] = d["v"].fillna(0.0).round(0).astype(int)
    lim = max(float(max(d["v"].abs().max(), 1.0)) * 1.15 + 0.5, 5.0)
    cats = d["metric"].tolist()[::-1]
    vals = d["v"].tolist()[::-1]

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
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}}
    }
    st_echarts(option, height=f"{int(height)}px")

def render_big_table(df: pd.DataFrame, key: str, height: int = 560) -> None:
    if df is None: df = pd.DataFrame()
    if HAS_AGGRID and AgGrid is not None:
        q = st.text_input("검색", value="", placeholder="테이블 내 검색", key=f"{key}_q")
        right_cols = {c for c in df.columns if any(k in c for k in ["노출", "클릭", "광고비", "전환", "매출", "CTR", "CPC", "CPA", "ROAS"])}
        
        _cond_thresholds = {}
        for _c in [c for c in df.columns if any(k in c for k in ["ROAS", "CTR"])]:
            _num = pd.to_numeric(df[_c], errors="coerce").dropna()
            if len(_num) >= 12: _cond_thresholds[_c] = {"low": float(_num.quantile(0.33)), "high": float(_num.quantile(0.67))}

        grid = _aggrid_grid_options(cols=list(df.columns), right_cols=right_cols, quick_filter=q or "", enable_filter=True, cond_thresholds=_cond_thresholds)
        AgGrid(df, gridOptions=grid, height=height, fit_columns_on_grid_load=False, theme="alpine", allow_unsafe_jscode=True, update_mode=_aggrid_mode("no_update"), data_return_mode=_aggrid_mode("as_input"), key=key)
        return
    st_dataframe_safe(df, use_container_width=True, hide_index=True, height=height)

def render_chart(obj, *, height: int | None = None) -> None:
    if obj is None: return
    if obj.__class__.__module__.startswith("altair"): st.altair_chart(obj, use_container_width=True); return
    try: st.write(obj)
    except Exception: pass

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
            vhtml = re.sub(r"\(([^)]*)\)", r"<span class='p'>(\1)</span>", str(value))
            return f"<div class='delta-chip {cls}'><div class='l'>{label}</div><div class='v'><span class='arr'>{arrow}</span>{vhtml}</div></div>"

        chips = [
            _delta_chip("광고비", f"{format_currency(dcost)} ({_pct_to_str(dcost_pct)})", dcost_pct),
            _delta_chip("클릭", f"{format_number_commas(dclk)} ({_pct_to_str(dclk_pct)})", dclk_pct),
            _delta_chip("전환", f"{format_number_commas(dconv)} ({_pct_to_str(dconv_pct)})", dconv_pct),
            _delta_chip("ROAS", f"{_pct_to_str(droas_pct)}", droas_pct),
        ]
        st.markdown("<div class='delta-chip-row'>" + "".join(chips) + "</div>", unsafe_allow_html=True)

        delta_df = pd.DataFrame([
            {"metric": "광고비", "change_pct": dcost_pct}, {"metric": "클릭", "change_pct": dclk_pct},
            {"metric": "전환", "change_pct": dconv_pct}, {"metric": "매출", "change_pct": _pct_change(cur["sales"], base["sales"])},
            {"metric": "ROAS", "change_pct": droas_pct},
        ])
        st.markdown("#### 📊 증감율(%) 막대그래프")
        if HAS_ECHARTS and st_echarts is not None: render_echarts_delta_bars(delta_df, height=260)

# [NEW] 엑셀 시트 4개로 완전 분리 저장
def generate_full_report_excel(overview_df: pd.DataFrame, camp_df: pd.DataFrame, kw_df: pd.DataFrame, st_df: pd.DataFrame = None) -> bytes:
    output = io.BytesIO()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if overview_df is not None and not overview_df.empty:
                overview_df.to_excel(writer, index=False, sheet_name="요약_현황")
            else:
                pd.DataFrame({"결과": ["데이터 없음"]}).to_excel(writer, index=False, sheet_name="요약_현황")
            
            if camp_df is not None and not camp_df.empty:
                camp_df.to_excel(writer, index=False, sheet_name="캠페인_상세")
                
            if kw_df is not None and not kw_df.empty:
                kw_df.to_excel(writer, index=False, sheet_name="파워링크_키워드_상세")
                
            if st_df is not None and not st_df.empty:
                st_df.to_excel(writer, index=False, sheet_name="쇼핑검색어_상세")
    except Exception as e:
        return overview_df.to_csv(index=False).encode("utf-8-sig") if overview_df is not None else b""
        
    return output.getvalue()
