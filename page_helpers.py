# -*- coding: utf-8 -*-
"""page_helpers.py - Shared UI helpers, filters, and rendering logic for pages."""

from __future__ import annotations

import os
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
    sv = st.session_state["filters_v8"]

    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    with st.expander("🔍 조회 기간 및 필터 설정", expanded=True):
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

        with st.container(border=True):
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

        with st.expander("고급 필터 (검색/유형)", expanded=False):
            q = st.text_input("텍스트 검색", sv.get("q", ""), key="f_q", placeholder="찾고 싶은 키워드나 캠페인 이름을 입력하세요")
            type_sel = ui_multiselect(st, "광고 유형 필터", type_opts, default=sv.get("type_sel", []), key="f_type_sel", placeholder="모든 광고 보기")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv
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
        
    out["광고비 증감(%)"] = out["광고비 증감(%)"].apply(fmt_pct)
    out["ROAS 증감(%)"] = out["ROAS 증감(%)"].apply(fmt_pct)
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
        
        def _get(df, c_kr, c_en): 
            if not df.empty:
                if c_kr in df.columns: return float(pd.to_numeric(df[c_kr], errors='coerce').fillna(0).sum())
                if c_en in df.columns: return float(pd.to_numeric(df[c_en], errors='coerce').fillna(0).sum())
            return 0.0
        
        c_cost = _get(c_df, "광고비", "cost")
        c_sales = _get(c_df, "전환매출", "sales")
        c_clk = _get(c_df, "클릭", "clk")
        c_imp = _get(c_df, "노출", "imp")
        c_conv = _get(c_df, "전환", "conv")
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        
        b_cost = _get(b_df, "광고비", "cost")
        b_sales = _get(b_df, "전환매출", "sales")
        b_clk = _get(b_df, "클릭", "clk")
        b_imp = _get(b_df, "노출", "imp")
        b_conv = _get(b_df, "전환", "conv")
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        
        def _delta(a,b): return pct_change(a,b)
        d_cost, d_sales, d_clk, d_imp, d_conv, d_roas = _delta(c_cost,b_cost), _delta(c_sales,b_sales), _delta(c_clk,b_clk), _delta(c_imp,b_imp), _delta(c_conv,b_conv), _delta(c_roas,b_roas)
        
        cur_label = f"{d1} ~ {d2}"
        prev_label = f"{b1} ~ {b2}"
        
        st.markdown("<div style='margin-top:16px; margin-bottom:6px; font-size:14px; font-weight:700; color:#111;'>📌 선택 항목 비교</div>", unsafe_allow_html=True)
        
        cards = [
            ("광고비", format_currency(c_cost), d_cost), ("전환매출", format_currency(c_sales), d_sales), ("ROAS", f"{c_roas:.2f}%", d_roas),
            ("클릭", format_number_commas(c_clk), d_clk), ("노출", format_number_commas(c_imp), d_imp), ("전환", f"{c_conv:.1f}", d_conv),
        ]
        cols = st.columns(3)
        for i,(label,val,d) in enumerate(cards):
            with cols[i%3]:
                color = "#0f766e" if (d is not None and d>=0) else "#be123c"
                arrow = "▲" if (d is not None and d>=0) else "▼"
                d_txt = "-" if d is None else f"{arrow} {abs(d):.2f}%"
                st.markdown(
                    f\"\"\"\n                    <div style='border:1px solid #e5e7eb;border-radius:10px;padding:10px 12px;margin-bottom:10px;background:#fff;'>\n                        <div style='font-size:12px;color:#6b7280;font-weight:600;'>{label}</div>\n                        <div style='font-size:20px;color:#111827;font-weight:800;line-height:1.2;margin-top:2px;'>{val}</div>\n                        <div style='font-size:12px;color:{color};font-weight:700;margin-top:4px;'>{prev_label} 대비 {d_txt}</div>\n                    </div>\n                    \"\"\", unsafe_allow_html=True
                )
