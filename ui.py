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
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # pip install streamlit-aggrid
    from st_aggrid.shared import GridUpdateMode, DataReturnMode
    HAS_AGGRID = True
except Exception:
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    GridUpdateMode = None  # type: ignore
    DataReturnMode = None  # type: ignore
    JsCode = None  # type: ignore
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

from sqlalchemy.engine import Engine

# -----------------------------
# Streamlit cache hashing (Engine)
# -----------------------------
_HASH_FUNCS = {Engine: lambda e: e.url.render_as_string(hide_password=True)}

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


def render_hero(latest: dict, build_tag: str = "") -> None:
    """Naver-like topbar (sticky)."""
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


def render_empty_state(msg: str, sub: str = "좌측 사이드바에서 기간이나 필터 조건을 변경해보세요.") -> None:
    """데이터가 없을 때 표시하는 친절한 빈 상태(Empty State) UI"""
    st.markdown(
        f"""
        <div class='nv-empty'>
            <div class='icon'>📭</div>
            <div class='msg'>{msg}</div>
            <div class='sub'>{sub}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str, tooltip: str = "") -> None:
    """Naver-like KPI card: compact, ▲/▼ delta, 툴팁 지원."""
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

    tooltip_html = f"<span class='tooltip'>{tooltip}</span>" if tooltip else ""

    st.markdown(
        f"""<div class='kpi' id='{key}'>
            {tooltip_html}
            <div class='k'>{title}</div>
            <div class='v'>{value}</div>
            {delta_html}
        </div>""",
        unsafe_allow_html=True,
    )


def render_timeseries_chart(ts: pd.DataFrame, entity: str = "campaign", key_prefix: str = "") -> None:
    if ts is None or ts.empty:
        render_empty_state("표시할 트렌드 데이터가 없습니다.")
        return

    df = ts.copy()
    if "dt" in df.columns:
        dt = pd.to_datetime(df["dt"], errors="coerce")
        df["dt"] = dt.dt.strftime("%Y-%m-%d")

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

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

    def _fmt_int(x) -> str:
        try: return f"{int(round(float(x))):,}"
        except Exception: return "0"
    def _fmt_won(x) -> str:
        try: return f"{int(round(float(x))):,}원"
        except Exception: return "0원"
    def _fmt_pct1(x) -> str:
        try: return f"{float(x):.1f}%"
        except Exception: return "0.0%"
    def _fmt_pct0(x) -> str:
        try: return f"{float(x):.0f}%"
        except Exception: return "0%"

    order = []
    for c in ["dt", "imp", "clk", "ctr", "cpc", "cost", "conv", "cpa", "sales", "roas"]:
        if c in df.columns:
            order.append(c)

    view = df[order].copy()
    rename = {"dt": "일자", "imp": "노출", "clk": "클릭", "ctr": "CTR(%)", "cpc": "CPC", "cost": "광고비", "conv": "전환", "cpa": "CPA", "sales": "매출", "roas": "ROAS(%)"}
    view = view.rename(columns=rename)

    disp = pd.DataFrame()
    if "일자" in view.columns: disp["일자"] = view["일자"].astype(str)
    for col in ["노출", "클릭", "전환"]:
        if col in view.columns: disp[col] = view[col].apply(_fmt_int)
    if "CTR(%)" in view.columns: disp["CTR(%)"] = view["CTR(%)"].apply(_fmt_pct1)
    if "CPC" in view.columns: disp["CPC"] = view["CPC"].apply(_fmt_won)
    if "광고비" in view.columns: disp["광고비"] = view["광고비"].apply(_fmt_won)
    if "CPA" in view.columns: disp["CPA"] = view["CPA"].apply(_fmt_won)
    if "매출" in view.columns: disp["매출"] = view["매출"].apply(_fmt_won)
    if "ROAS(%)" in view.columns: disp["ROAS(%)"] = view["ROAS(%)"].apply(_fmt_pct0)

    st_dataframe_safe(disp, use_container_width=True, hide_index=True, height=360)


def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
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
    if table_df is None or table_df.empty:
        render_empty_state("예산 데이터가 없습니다.")
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


def render_echarts_line(
    title: str, ts: pd.DataFrame, x_col: str, y_col: str, y_name: str, *, height: int = 260, smooth: bool = True
) -> None:
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
        "series": [{"type": "line", "data": y, "smooth": smooth, "showSymbol": False, "lineStyle": {"width": 3, "color": "#2563EB"}, "areaStyle": {"opacity": 0.06}}],
    }
    st_echarts(option, height=f"{int(height)}px")


def render_echarts_delta_bars(delta_df: pd.DataFrame, *, height: int = 260) -> None:
    if not (HAS_ECHARTS and st_echarts is not None): return
    if delta_df is None or delta_df.empty: return
    d = delta_df.copy()
    if "metric" not in d.columns or "change_pct" not in d.columns: return

    d["metric"] = d["metric"].astype(str)
    d["v"] = pd.to_numeric(d["change_pct"], errors="coerce")
    if d["v"].notna().sum() == 0:
        render_empty_state("비교기간 데이터가 없어 증감율을 계산할 수 없습니다.", sub="비교 기간을 다른 일자로 설정해 보세요.")
        return

    d["v"] = d["v"].fillna(0.0).round(0).astype(int)
    lim = float(max(d["v"].abs().max(), 1.0))
    lim = lim * 1.15 + 0.5
    lim = max(lim, 5.0)

    cats = d["metric"].tolist()[::-1]
    vals = d["v"].tolist()[::-1]

    data = []
    for m, v in zip(cats, vals):
        if v > 0:
            color = "#2563EB"; br = [0, 10, 10, 0]; pos = "right"; fmt = f"+{int(round(v))}%"
        elif v < 0:
            color = "#EF4444"; br = [10, 0, 0, 10]; pos = "left"; fmt = f"{int(round(v))}%"
        else:
            color = "#B4C4D9"; br = [0, 0, 0, 0]; pos = "right"; fmt = "+0%"
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
    st.markdown(
        """<style>.stDownloadButton button { padding: 0.15rem 0.55rem !important; font-size: 0.82rem !important; line-height: 1.2 !important; min-height: 28px !important; }</style>""",
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([1, 1, 8])
    with c1: st.download_button("CSV", data=_df_json_to_csv_bytes(df_json), file_name=f"{filename_base}.csv", mime="text/csv", key=f"{key_prefix}_csv", use_container_width=True)
    with c2: st.download_button("XLSX", data=_df_json_to_xlsx_bytes(df_json, sheet_name), file_name=f"{filename_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"{key_prefix}_xlsx", use_container_width=True)
    with c3: st.caption("다운로드")


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

    base = alt.Chart(d).encode(x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending"), axis=x_axis), y=alt.Y(f"{y_col}:Q", axis=y_axis), tooltip=[alt.Tooltip("_dt_str:N", title="날짜"), alt.Tooltip(f"{y_col}:Q", title=title or y_col, format=y_format)])
    area = base.mark_area(interpolate="monotone", opacity=0.08)
    line = base.mark_line(interpolate="monotone", strokeWidth=3)
    pts = base.mark_point(size=40, filled=True)

    last = d.tail(1)
    last_label = alt.Chart(last).mark_text(align="left", dx=8, dy=-8).encode(x=alt.X("_x_label:N", sort=alt.SortField(x_col, order="ascending")), y=alt.Y(f"{y_col}:Q"), text=alt.Text(f"{y_col}:Q", format=y_format))
    return (area + line + pts + last_label).properties(height=int(height))


def _chart_delta_bars(delta_df: pd.DataFrame, height: int = 260):
    if delta_df is None or delta_df.empty: return None
    d = delta_df.copy()
    d["metric"] = d["metric"].astype(str)
    d["change_pct"] = pd.to_numeric(d["change_pct"], errors="coerce").fillna(0)
    d["dir"] = d["change_pct"].apply(lambda x: "up" if x > 0 else ("down" if x < 0 else "flat"))
    d["label"] = d["change_pct"].map(_pct_to_str)
    if "order" in d.columns:
        d = d.sort_values("order", ascending=False)
        y_sort = alt.SortField(field="order", order="descending")
    else: y_sort = None

    mn, mx = float(d["change_pct"].min()), float(d["change_pct"].max())
    m_abs = max(abs(mn), abs(mx))
    if not (m_abs > 0): m_abs = 5.0
    pad = max(2.0, m_abs * 0.12)
    domain = [-(m_abs + pad), m_abs + pad]

    abs_pct = d["change_pct"].abs()
    flat = (abs_pct * 0.6).clip(lower=0.0, upper=2.0)
    d["flat_end"] = flat.where(d["change_pct"] >= 0, -flat)
    d["zero"], d_main, d_cap = 0.0, d.copy(), d.copy()
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
    if mod.startswith("altair"): st.altair_chart(obj, use_container_width=True); return
    try: st.write(obj)
    except Exception: pass


def render_period_compare_panel(engine, entity: str, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], key_prefix: str, expanded: bool = False) -> None:
    with st.expander("🔁 전일/전주/전월 비교", expanded=expanded):
        apply_global_css()
        mode = st.radio("비교 기준", ["전일대비", "전주대비", "전월대비"], horizontal=True, index=1, key=f"{key_prefix}_{entity}_pcmp_mode")
        b1, b2 = _period_compare_range(d1, d2, mode)
        try: n_cur = int((d2 - d1).days) + 1; n_base = int((b2 - b1).days) + 1
        except Exception: n_cur, n_base = 0, 0
        st.caption(f"현재기간: {d1} ~ {d2} ({n_cur}일) · 비교기간({mode}): {b1} ~ {b2} ({n_base}일)")

        cur = get_entity_totals(engine, entity, d1, d2, cids, type_sel)
        base = get_entity_totals(engine, entity, b1, b2, cids, type_sel)

        dcost, dclk, dconv = cur["cost"] - base["cost"], cur["clk"] - base["clk"], cur["conv"] - base["conv"]
        dcost_pct, dclk_pct, dconv_pct, droas_pct = _pct_change(cur["cost"], base["cost"]), _pct_change(cur["clk"], base["clk"]), _pct_change(cur["conv"], base["conv"]), _pct_change(cur["roas"], base["roas"])

        def _delta_chip(label: str, value: str, sign: Optional[float]) -> str:
            if sign is None: cls = "zero"
            elif sign > 0: cls = "pos"
            elif sign < 0: cls = "neg"
            else: cls = "zero"
            arrow = "•"
            if sign is not None:
                try:
                    if float(sign) > 0: arrow = "▲"
                    elif float(sign) < 0: arrow = "▼"
                except Exception: pass
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
            {"metric": "광고비", "change_pct": _pct_change(cur["cost"], base["cost"])},
            {"metric": "클릭", "change_pct": _pct_change(cur["clk"], base["clk"])},
            {"metric": "전환", "change_pct": _pct_change(cur["conv"], base["conv"])},
            {"metric": "매출", "change_pct": _pct_change(cur["sales"], base["sales"])},
            {"metric": "ROAS", "change_pct": _pct_change(cur["roas"], base["roas"])},
        ])
        st.markdown("#### 📊 증감율(%) 막대그래프")
        if HAS_ECHARTS and st_echarts is not None: render_echarts_delta_bars(delta_df, height=260)
        else: ch = _chart_delta_bars(delta_df, height=260); render_chart(ch)

        mini = pd.DataFrame([
            ["광고비", format_currency(cur["cost"]), format_currency(base["cost"]), f"{_pct_to_str(_pct_change(cur['cost'], base['cost']))}"],
            ["클릭", format_number_commas(cur["clk"]), format_number_commas(base["clk"]), f"{_pct_to_str(_pct_change(cur['clk'], base['clk']))}"],
            ["전환", format_number_commas(cur["conv"]), format_number_commas(base["conv"]), f"{_pct_to_str(_pct_change(cur['conv'], base['conv']))}"],
            ["매출", format_currency(cur["sales"]), format_currency(base["sales"]), _pct_to_str(_pct_change(cur["sales"], base["sales"]))],
            ["ROAS(%)", format_roas(cur["roas"]), format_roas(base["roas"]), f"{_fmt_point((cur.get('roas',0.0) or 0.0) - (base.get('roas',0.0) or 0.0))}"],
        ], columns=["지표", "현재", "비교기간", "증감"])
        ui_table_or_dataframe(mini, key=f"{key_prefix}_{entity}_pcmp_table", height=210)
