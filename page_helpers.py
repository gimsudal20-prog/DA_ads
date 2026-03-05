# -*- coding: utf-8 -*-
"""page_helpers.py - Shared UI helpers, filters, and rendering logic for pages."""

from __future__ import annotations

import os
import textwrap
import numpy as np
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from typing import Dict, List

from data import *
from ui import *
from data import pct_change, pct_to_arrow

BUILD_TAG = os.getenv("APP_BUILD", "")
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty: return []
    df = meta.copy()
    if manager_sel and "manager" in df.columns:
        sel = [str(x).strip() for x in manager_sel if str(x).strip()]
        if sel: df = df[df["manager"].astype(str).str.strip().isin(sel)]
    if account_sel and "account_name" in df.columns:
        sel = [str(x).strip() for x in account_sel if str(x).strip()]
        if sel: df = df[df["account_name"].astype(str).str.strip().isin(sel)]
    if "customer_id" not in df.columns: return []
    s = pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64")
    return sorted(s.drop_duplicates().tolist())

def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    try: return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception: return col.multiselect(label, options, default=default, key=key)

def get_dynamic_cmp_options(d1: date, d2: date) -> List[str]:
    delta = (d2 - d1).days + 1
    if delta == 1: return ["비교 안함", "전일대비"]
    elif delta == 7: return ["비교 안함", "전주대비"]
    elif 28 <= delta <= 31: return ["비교 안함", "전월대비"]
    else: return ["비교 안함", "이전 같은 기간 대비"]

# ✨ [FIX] 실수로 누락되었던 기간 비교 계산 함수 복구!
def period_compare_range(d1: date, d2: date, cmp_mode: str):
    delta = (d2 - d1).days + 1
    if cmp_mode == "전일대비":
        return d1 - timedelta(days=1), d2 - timedelta(days=1)
    elif cmp_mode == "전주대비":
        return d1 - timedelta(days=7), d2 - timedelta(days=7)
    else:
        # "이전 같은 기간 대비" 또는 "전월대비" (선택된 일수만큼 과거로 이동)
        return d1 - timedelta(days=delta), d1 - timedelta(days=1)

def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    today = date.today()
    default_end = today - timedelta(days=1)
    default_start = default_end

    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "", "manager": [], "account": [], "type_sel": [],
            "period_mode": "어제", "d1": default_start, "d2": default_end,
            "top_n_keyword": 300, "top_n_ad": 200, "top_n_campaign": 200, "prefetch_warm": True,
        }
    if "filters_expanded" not in st.session_state:
        st.session_state["filters_expanded"] = True
    sv = st.session_state["filters_v8"]

    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    with st.expander("🔍 조회 기간 및 필터 설정", expanded=st.session_state.get("filters_expanded", True)):
        st.caption("💡 기본 필터에서 빠르게 조회하고, 필요할 때만 고급 필터를 여세요.")

        manager_sel = sv.get("manager", [])

        basic_col1, basic_col2, basic_col3 = st.columns([1.5, 1.8, 1.7], gap="medium")
        period_mode = basic_col1.selectbox(
            "📅 기간 선택",
            ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"],
            index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")),
            key="f_period_mode"
        )

        if period_mode == "직접 선택":
            d1 = basic_col2.date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = basic_col3.date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달": d2 = date(today.year, today.month, 1) - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            basic_col2.text_input("시작일", str(d1), disabled=True, key="f_d1_ro")
            basic_col3.text_input("종료일", str(d2), disabled=True, key="f_d2_ro")

        if period_mode == "오늘":
            st.warning("⚠️ '오늘' 데이터는 매체/API 수집 지연으로 일부 지표가 덜 집계될 수 있습니다.")

        if period_mode != "직접 선택":
            st.caption(f"📅 선택 기간: {d1} ~ {d2}")

        try:
            basic_filter_container = st.container(border=True)
        except TypeError:
            # 구버전 Streamlit 호환: border 파라미터 미지원
            basic_filter_container = st.container()

        with basic_filter_container:
            st.markdown("**기본 필터**")
            manager_sel = ui_multiselect(st, "담당자 필터", managers, default=sv.get("manager", []), key="f_manager", placeholder="모든 담당자")

            accounts_by_mgr = accounts
            if manager_sel:
                try:
                    dfm = meta.copy()
                    if "manager" in dfm.columns and "account_name" in dfm.columns:
                        dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                        accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
                except Exception:
                    pass

            prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
            account_sel = ui_multiselect(st, "광고주(계정) 필터", accounts_by_mgr, default=prev_acc, key="f_account", placeholder="전체 계정 합산보기")

            if st.button("✅ 필터 적용", key="btn_apply_filters", use_container_width=True):
                # 사용자가 명시적으로 접지 않는 한 필터 박스는 유지한다.
                st.session_state["filters_expanded"] = True
                st.rerun()

        with st.expander("고급 필터 (검색/유형)", expanded=False):
            q = st.text_input("텍스트 검색", sv.get("q", ""), key="f_q", placeholder="찾고 싶은 키워드나 캠페인 이름을 입력하세요")
            type_sel = ui_multiselect(st, "광고 유형 필터", type_opts, default=sv.get("type_sel", []), key="f_type_sel", placeholder="모든 광고 보기")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv

    prev_manager_count = len(st.session_state.get("_prev_manager_sel", []))
    prev_account_count = len(st.session_state.get("_prev_account_sel", []))
    cur_manager_count = len(manager_sel or [])
    cur_account_count = len(account_sel or [])
    st.session_state["_prev_manager_sel"] = manager_sel or []
    st.session_state["_prev_account_sel"] = account_sel or []

    # 담당자/계정 선택 시 필터가 자동으로 접히지 않도록 동작 제거

    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "ready": True,
    }

def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or meta is None or meta.empty: return df
    out = df.copy()
    out["customer_id"] = pd.to_numeric(out["customer_id"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    meta_copy = meta.copy()
    meta_copy["customer_id"] = pd.to_numeric(meta_copy["customer_id"], errors="coerce").astype("int64")
    return out.merge(meta_copy[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

def append_comparison_data(df_cur: pd.DataFrame, df_prev: pd.DataFrame, join_keys: list) -> pd.DataFrame:
    if df_prev is None or df_prev.empty or df_cur is None or df_cur.empty:
        return df_cur
        
    df_cur_copy = df_cur.copy()
    valid_join_keys = [k for k in join_keys if k in df_cur_copy.columns and k in df_prev.columns]
    if not valid_join_keys: return df_cur_copy
    
    for k in valid_join_keys:
        df_cur_copy[k] = df_cur_copy[k].astype(str)
        df_prev[k] = df_prev[k].astype(str)
        
    val_cols = [c for c in ['cost', 'sales', 'conv', 'clk', 'imp'] if c in df_prev.columns]
    base_tmp = df_prev[valid_join_keys + val_cols].copy()
    
    for c in val_cols:
        base_tmp[c] = pd.to_numeric(base_tmp[c], errors='coerce').fillna(0)
        
    base_tmp = base_tmp.groupby(valid_join_keys, as_index=False).sum()
    base_tmp.rename(columns={'cost':'p_cost', 'sales':'p_sales', 'conv':'p_conv', 'clk':'p_clk', 'imp':'p_imp'}, inplace=True)
    
    out = df_cur_copy.merge(base_tmp, on=valid_join_keys, how='left')
    for c in ['p_cost', 'p_sales', 'p_conv', 'p_clk', 'p_imp']:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors='coerce').fillna(0)
        else: out[c] = 0
        
    cur_cost = pd.to_numeric(out.get("광고비", 0), errors='coerce').fillna(0)
    cur_sales = pd.to_numeric(out.get("전환매출", 0), errors='coerce').fillna(0)
    cur_conv = pd.to_numeric(out.get("전환", 0), errors='coerce').fillna(0)
    cur_roas = pd.to_numeric(out.get("ROAS(%)", 0), errors='coerce').fillna(0)
    
    out["광고비 증감(%)"] = np.where(out["p_cost"] > 0, (cur_cost - out["p_cost"]) / out["p_cost"] * 100, np.where(cur_cost > 0, 100.0, 0.0))
    p_roas = np.where(out["p_cost"] > 0, (out["p_sales"] / out["p_cost"]) * 100, 0.0)
    out["p_roas"] = p_roas  
    
    out["ROAS 증감(%)"] = cur_roas - p_roas
    out["전환 증감"] = cur_conv - out["p_conv"]
    
    def fmt_pct(x):
        if pd.isna(x) or x == 0: return "-"
        return f"▲ {x:.2f}%" if x > 0 else (f"▼ {abs(x):.2f}%" if x < 0 else "-")
    def fmt_diff(x):
        if pd.isna(x) or x == 0: return "-"
        return f"▲ {int(x)}" if x > 0 else (f"▼ {abs(int(x))}" if x < 0 else "-")
        
    roas_delta_col = "ROAS 증감(%)"
    # 배포 버전/수정 과정에서 컬럼명이 오타(ROAS 증(%))로 생성되어도 안전하게 흡수
    if roas_delta_col not in out.columns and "ROAS 증(%)" in out.columns:
        out[roas_delta_col] = out["ROAS 증(%)"]
    if roas_delta_col not in out.columns:
        out[roas_delta_col] = 0

    out["광고비 증감(%)"] = out["광고비 증감(%)"].apply(fmt_pct)
    out[roas_delta_col] = pd.to_numeric(out[roas_delta_col], errors='coerce').fillna(0).apply(fmt_pct)
    out["전환 증감"] = out["전환 증감"].apply(fmt_diff)
    
    return out

def style_table_deltas(val):
    if pd.isna(val) or val == "-": return ""
    if isinstance(val, str):
        if "▲" in val: return "color: #e11d48; font-weight: 700;" # Red (상승)
        if "▼" in val: return "color: #2563eb; font-weight: 700;" # Blue (하락)
    return ""

def render_side_by_side_metrics(row: pd.Series, prev_label: str, cur_label: str, deltas: dict = None):
    pass # 사용하지 않는 예전 위젯

def render_comparison_section(df: pd.DataFrame, cmp_mode: str, b1: date, b2: date, d1: date, d2: date, section_title: str = "선택 항목 상세 비교"):
    pass # 사용하지 않는 예전 위젯

def _render_ab_test_sbs(df_grp: pd.DataFrame, d1: date, d2: date):
    st.markdown("<div class='nv-sec-title'>📊 소재 A/B 비교 (선택한 그룹 내 상위 2개)</div>", unsafe_allow_html=True)
    st.caption(f"조회 기간: {d1} ~ {d2}")
    
    valid_ads = df_grp.sort_values(by=['노출', '광고비'], ascending=[False, False])
    if len(valid_ads) < 2:
        st.info("해당 그룹에 비교 가능한 소재가 2개 이상 없습니다.")
        st.divider()
        return
        
    ad1, ad2 = valid_ads.iloc[0], valid_ads.iloc[1]
    c1, c2 = st.columns(2)
    
    def _card(row, label):
        return f"""
        <div style='background:#F8FAFC; padding:20px; border-radius:12px; border:2px solid #E2E8F0;'>
            <div style='text-align:center; font-size:13px; font-weight:800; color:#475569; margin-bottom:8px;'>{label}</div>
            <h4 style='text-align:center; margin-top:0; margin-bottom:16px; color:#1E40AF; font-size:15px; font-weight:700;'>{row['소재내용']}</h4>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>광고비</span>
                <span style='font-weight:700; color:#0F172A;'>{format_currency(row.get('광고비',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:#64748B; font-weight:600;'>전환매출</span>
                <span style='font-weight:700; color:#0F172A;'>{format_currency(row.get('전환매출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:12px; padding-bottom:12px; border-bottom:1px dashed #CBD5E1;'>
                <span style='color:#64748B; font-weight:600;'>ROAS</span>
                <span style='font-weight:800; color:#EF4444; font-size:15px;'>{row.get('ROAS(%)',0):.2f}%</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>노출수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{format_number_commas(row.get('노출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                <span style='color:#64748B; font-size:13px;'>클릭수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{format_number_commas(row.get('클릭',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between;'>
                <span style='color:#64748B; font-size:13px;'>전환수</span>
                <span style='color:#334155; font-size:13px; font-weight:600;'>{row.get('전환',0):.1f}</span>
            </div>
        </div>
        """
    
    with c1: st.markdown(_card(ad1, "💡 소재 A"), unsafe_allow_html=True)
    with c2: st.markdown(_card(ad2, "💡 소재 B"), unsafe_allow_html=True)
    st.divider()

def render_item_comparison_search(entity_label: str, df_cur: pd.DataFrame, df_base: pd.DataFrame, name_col: str, d1: date, d2: date, b1: date, b2: date):
    import streamlit as st
    import pandas as pd
    
    items_cur = set(df_cur[name_col].dropna().astype(str).unique()) if not df_cur.empty and name_col in df_cur.columns else set()
    items_base = set(df_base[name_col].dropna().astype(str).unique()) if not df_base.empty and name_col in df_base.columns else set()
    
    all_items = sorted([x for x in list(items_cur | items_base) if str(x).strip() != ''])
    
    if not all_items: return
        
    st.markdown(f"<div style='font-size:15px; font-weight:700; margin-top:20px; color:#111;'>🎯 상세 분석할 {entity_label}을 선택하세요</div>", unsafe_allow_html=True)
    selected = st.selectbox("항목 선택 (표 하이라이트 연동)", ["- 표만 보기 (선택 안함) -"] + all_items, key=f"search_detail_{entity_label}_{name_col}")
    
    if selected != "- 표만 보기 (선택 안함) -":
        c_df = df_cur[df_cur[name_col] == selected] if not df_cur.empty else pd.DataFrame()
        b_df = df_base[df_base[name_col] == selected] if not df_base.empty else pd.DataFrame()

        def _sum_col(df: pd.DataFrame, candidates: list[str]) -> float:
            if df is None or df.empty:
                return 0.0
            for col in candidates:
                if col in df.columns:
                    return float(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
            return 0.0

        def _cur_val(candidates_kr: list[str], candidates_en: list[str]) -> float:
            return _sum_col(c_df, candidates_kr + candidates_en)

        def _base_val(base_cols: list[str], prev_cols: list[str]) -> float:
            # 1) 비교기간 raw dataframe(df_base) 우선
            val = _sum_col(b_df, base_cols)
            if val > 0:
                return val
            # 2) append_comparison_data로 붙인 p_* 컬럼 fallback
            return _sum_col(c_df, prev_cols)

        c_cost = _cur_val(["광고비"], ["cost"])
        c_sales = _cur_val(["전환매출"], ["sales"])
        c_clk = _cur_val(["클릭", "클릭수"], ["clk"])
        c_imp = _cur_val(["노출", "노출수"], ["imp"])
        c_conv = _cur_val(["전환", "전환수"], ["conv"])
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0

        b_cost = _base_val(["광고비", "cost"], ["p_cost"])
        b_sales = _base_val(["전환매출", "sales"], ["p_sales"])
        b_clk = _base_val(["클릭", "클릭수", "clk"], ["p_clk"])
        b_imp = _base_val(["노출", "노출수", "imp"], ["p_imp"])
        b_conv = _base_val(["전환", "전환수", "conv"], ["p_conv"])
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        
        def fmt_krw(v): return f"{int(v):,}원"
        def fmt_num(v): return f"{int(v):,}"
        def fmt_pct(v): return f"{v:.1f}%"

        def calc_detail_delta(c, b, is_currency=False, is_pct=False):
            diff = c - b
            if b == 0 and c > 0:
                return "신규"
            if diff == 0:
                return "변동 없음"

            sign = "▲" if diff > 0 else "▼"
            if is_currency:
                abs_val = f"{int(abs(diff)):,}원"
            elif is_pct:
                abs_val = f"{abs(diff):.1f}%p"
            else:
                abs_val = f"{int(abs(diff)):,}" if diff.is_integer() else f"{abs(diff):.1f}"

            word = "증가" if diff > 0 else "감소"
            return f"{sign} {abs_val} {word}"

        def calc_delta_rate(c, b):
            if b == 0 and c > 0:
                return "신규"
            if b == 0:
                return "0.0%"
            return f"{((c - b) / b) * 100:+.1f}%"

        def delta_chip_text(delta_text: str):
            if delta_text == "변동 없음":
                return "<span style='color:#64748b; font-weight:700;'>변동 없음</span>"
            if delta_text == "신규":
                return "<span style='color:#e11d48; font-weight:800;'>▲ 신규</span>"
            if delta_text.startswith("▲"):
                return f"<span style='color:#e11d48; font-weight:800;'>{delta_text}</span>"
            return f"<span style='color:#2563eb; font-weight:800;'>{delta_text}</span>"

        rows = [
            {
                "label": "광고비",
                "base": fmt_krw(b_cost),
                "curr": fmt_krw(c_cost),
                "delta": calc_detail_delta(c_cost, b_cost, is_currency=True),
                "rate": calc_delta_rate(c_cost, b_cost),
            },
            {
                "label": "전환매출",
                "base": fmt_krw(b_sales),
                "curr": fmt_krw(c_sales),
                "delta": calc_detail_delta(c_sales, b_sales, is_currency=True),
                "rate": calc_delta_rate(c_sales, b_sales),
            },
            {
                "label": "ROAS",
                "base": fmt_pct(b_roas),
                "curr": f"<span style='font-weight:800; color:#dc2626;'>{fmt_pct(c_roas)}</span>",
                "delta": calc_detail_delta(c_roas, b_roas, is_pct=True),
                "rate": calc_delta_rate(c_roas, b_roas),
            },
            {
                "label": "노출수",
                "base": fmt_num(b_imp),
                "curr": fmt_num(c_imp),
                "delta": calc_detail_delta(c_imp, b_imp),
                "rate": calc_delta_rate(c_imp, b_imp),
            },
            {
                "label": "클릭수",
                "base": fmt_num(b_clk),
                "curr": fmt_num(c_clk),
                "delta": calc_detail_delta(c_clk, b_clk),
                "rate": calc_delta_rate(c_clk, b_clk),
            },
            {
                "label": "전환수",
                "base": fmt_num(b_conv),
                "curr": fmt_num(c_conv),
                "delta": calc_detail_delta(c_conv, b_conv),
                "rate": calc_delta_rate(c_conv, b_conv),
            },
        ]

        def _board_rows(items: list[dict], is_right: bool = False) -> str:
            html_rows = ""
            for r in items:
                if is_right:
                    html_rows += f"""
                    <div class='cmp-row'>
                        <div class='cmp-top'>
                            <span class='cmp-label'>{r['label']}</span>
                            <span class='cmp-value'>{r['curr']}</span>
                        </div>
                        <div class='cmp-sub'>
                            {delta_chip_text(r['delta'])}
                            <span class='rate'>({r['rate']})</span>
                        </div>
                    </div>
                    """
                else:
                    html_rows += f"""
                    <div class='cmp-row'>
                        <div class='cmp-top'>
                            <span class='cmp-label'>{r['label']}</span>
                            <span class='cmp-value'>{r['base']}</span>
                        </div>
                    </div>
                    """
            return html_rows

        left_rows = _board_rows(rows, is_right=False)
        right_rows = _board_rows(rows, is_right=True)

        html = textwrap.dedent(f"""\
        <div style='background-color:#f8fafc; border:2px solid #e2e8f0; border-radius:12px; padding:16px; margin-top:8px; margin-bottom:24px; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);'>
            <div style='font-size:16px; font-weight:800; color:#0f172a; margin-bottom:14px; text-align:center;'>✨ [{selected}] 성과 비교 상세 요약</div>
            <div class='cmp-boards'>
                <div class='cmp-board left'>
                    <div class='cmp-board-head'>⚪ 비교 기간 ({b1} ~ {b2})</div>
                    {left_rows}
                </div>
                <div class='cmp-board right'>
                    <div class='cmp-board-head'>🔵 선택 기간 ({d1} ~ {d2})</div>
                    {right_rows}
                </div>
            </div>
        </div>
        <style>
            .cmp-boards {{
                display:grid;
                grid-template-columns: 1fr 1fr;
                gap:12px;
            }}
            .cmp-board {{
                border-radius:10px;
                padding:12px 14px;
            }}
            .cmp-board.left {{
                background:#ffffff;
                border:1px solid #cbd5e1;
            }}
            .cmp-board.right {{
                background:#eff6ff;
                border:2px solid #bfdbfe;
            }}
            .cmp-board-head {{
                font-size:13px;
                font-weight:800;
                text-align:center;
                margin-bottom:6px;
            }}
            .cmp-board.left .cmp-board-head {{ color:#64748b; }}
            .cmp-board.right .cmp-board-head {{ color:#1d4ed8; }}

            .cmp-row {{
                border-top:1px dashed #dbe4f0;
                padding:9px 0;
            }}
            .cmp-top {{
                display:flex;
                justify-content:space-between;
                align-items:center;
            }}
            .cmp-board.left .cmp-label {{ color:#334155; font-weight:800; }}
            .cmp-board.left .cmp-value {{ color:#0f172a; font-weight:700; }}
            .cmp-board.right .cmp-label {{ color:#1e40af; font-weight:800; }}
            .cmp-board.right .cmp-value {{ color:#0f172a; font-weight:800; }}
            .cmp-sub {{
                margin-top:2px;
                text-align:right;
                font-size:13px;
            }}
            .rate {{ color:#64748b; font-weight:700; }}
        </style>
        """).strip()
        # Markdown 파서 상태에 따라 HTML이 코드블록으로 노출되는 이슈를 피하기 위해
        # 컴포넌트 HTML 렌더러를 사용한다.
        st.components.v1.html(html, height=560, scrolling=False)
