# -*- coding: utf-8 -*-
"""pages.py - Page functions + router for the Streamlit dashboard."""

from __future__ import annotations

import os
import math
import time
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from data import *
from data import period_compare_range, pct_to_arrow
from ui import *

BUILD_TAG = os.getenv("APP_BUILD", "v9.0 (인사이트 자동화 도입)")
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty: return []
    if (not manager_sel) and (not account_sel): return []
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

    with st.expander("검색조건", expanded=True):
        r1 = st.columns([1.1, 1.2, 1.2, 2.2], gap="small")
        period_mode = r1[0].selectbox("기간", ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"], index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")), key="f_period_mode")
        
        if period_mode == "직접 선택":
            d1 = r1[1].date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = r1[2].date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달": d2 = date(today.year, today.month, 1) - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            r1[1].text_input("시작일", str(d1), disabled=True, key="f_d1_ro")
            r1[2].text_input("종료일", str(d2), disabled=True, key="f_d2_ro")

        q = r1[3].text_input("검색", sv.get("q", ""), key="f_q", placeholder="계정/키워드/소재 검색")

        r2 = st.columns([1.2, 1.6, 1.2], gap="small")
        manager_sel = ui_multiselect(r2[0], "담당자", managers, default=sv.get("manager", []), key="f_manager")

        accounts_by_mgr = accounts
        if manager_sel:
            try:
                dfm = meta.copy()
                dfm['manager'] = dfm.get('manager','').astype(str).fillna('').str.strip()
                dfm['account_name'] = dfm.get('account_name','').astype(str).fillna('').str.strip()
                if "manager" in dfm.columns and "account_name" in dfm.columns:
                    dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                    accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
            except Exception: pass

        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
        account_sel = ui_multiselect(r2[1], "계정", accounts_by_mgr, default=prev_acc, key="f_account")
        type_sel = ui_multiselect(r2[2], "캠페인 유형", type_opts, default=sv.get("type_sel", []), key="f_type_sel")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv
    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "prefetch_warm": bool(sv.get("prefetch_warm", True)), "ready": True,
    }

def _render_empty_state_no_data(key: str = "empty") -> None:
    st.markdown("### 🫥 데이터가 없습니다")
    st.caption("오늘 데이터는 수집 지연이 있을 수 있어요. 아래 버튼으로 기간을 **최근 7일(오늘 제외)**로 바꿔 다시 조회해보세요.")
    c1, c2 = st.columns([1, 3])
    if c1.button("📅 최근 7일로", key=f"{key}_set7", type="primary"):
        try:
            if "filters_v8" in st.session_state: st.session_state["filters_v8"]["period_mode"] = "최근 7일"
            st.cache_data.clear()
        except Exception: pass
        st.rerun()
    with c2:
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        st.write("• 담당자/계정 필터를 풀어보거나, accounts.xlsx 동기화를 확인해보세요.")

def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or meta is None or meta.empty: 
        return df
    
    out = df.copy()
    out["customer_id"] = pd.to_numeric(out["customer_id"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    
    meta_copy = meta.copy()
    meta_copy["customer_id"] = pd.to_numeric(meta_copy["customer_id"], errors="coerce").astype("int64")
    
    return out.merge(meta_copy[["customer_id", "account_name", "manager"]], on="customer_id", how="left")


# --- 페이지 로직 ---
def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    st.markdown("<div class='nv-sec-title'>요약</div>", unsafe_allow_html=True)
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    cmp_mode = st.radio("비교 기준", ["전일대비", "전주대비", "전월대비"], horizontal=True, index=1, key="ov_cmp_mode")

    cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    base = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    def _delta_pct(key):
        try: return _pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    def _kpi_html(label, value, delta_text, delta_val):
        cls = "pos" if delta_val and float(delta_val) > 0 else ("neg" if delta_val and float(delta_val) < 0 else "neu")
        return f"<div class='kpi'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls}'>{delta_text}</div></div>"

    items = [
        ("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost")),
        ("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales")),
        ("전환", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv")),
        ("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas")),
        ("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr")),
        ("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc")),
    ]
    st.markdown("<div class='kpi-row'>" + "".join(_kpi_html(*i) for i in items) + "</div>", unsafe_allow_html=True)
    st.divider()

    # ==========================================
    # [NEW] AI 액션 제안 (돈 먹는 하마 vs 우수 키워드)
    # ==========================================
    st.markdown("### 🚨 AI 최적화 인사이트")
    
    # 500등까지의 키워드 묶음을 가져와서 인사이트 분석
    kw_df = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=500)
    
    if kw_df is not None and not kw_df.empty:
        kw_df = _perf_common_merge_meta(add_rates(kw_df), meta)
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.error("#### 💸 돈 먹는 하마 (비용 누수)")
            st.caption("비용은 3만 원 이상 소진되었으나 전환이 0건인 매체/키워드입니다. (제외/입찰가 하향 권장)")
            
            # 비용 3만 이상, 전환 0
            hippos = kw_df[(kw_df['cost'] >= 30000) & (kw_df['conv'] == 0)].sort_values('cost', ascending=False)
            if not hippos.empty:
                disp_hippos = hippos[['account_name', 'campaign_type_label', 'keyword', 'cost', 'clk']].rename(
                    columns={'account_name': '업체명', 'campaign_type_label': '매체', 'keyword': '키워드', 'cost': '비용', 'clk': '클릭'}
                )
                disp_hippos['비용'] = disp_hippos['비용'].apply(format_currency)
                disp_hippos['클릭'] = disp_hippos['클릭'].apply(format_number_commas)
                ui_table_or_dataframe(disp_hippos.head(5), "hippos_table", 200)
            else:
                st.success("🎉 현재 심각한 비용 누수가 발생하는 키워드가 없습니다!")

        with c2:
            st.info("#### ⭐ 예산 증액 추천 (효율 우수)")
            st.caption("비용은 5만 원 미만이지만 ROAS 500% 이상을 기록 중인 알짜 키워드입니다.")
            
            # 비용 5만 이하, 전환 1 이상, ROAS 500 이상
            stars = kw_df[(kw_df['cost'] <= 50000) & (kw_df['conv'] >= 1) & (kw_df['roas'] >= 500)].sort_values('roas', ascending=False)
            if not stars.empty:
                disp_stars = stars[['account_name', 'campaign_type_label', 'keyword', 'roas', 'conv']].rename(
                    columns={'account_name': '업체명', 'campaign_type_label': '매체', 'keyword': '키워드', 'roas': 'ROAS(%)', 'conv': '전환'}
                )
                disp_stars['ROAS(%)'] = disp_stars['ROAS(%)'].apply(format_roas)
                disp_stars['전환'] = disp_stars['전환'].apply(format_number_commas)
                ui_table_or_dataframe(disp_stars.head(5), "stars_table", 200)
            else:
                st.write("발굴된 고효율(저비용 고ROAS) 키워드가 없습니다.")
    else:
        st.write("키워드 데이터를 불러올 수 없어 인사이트를 분석할 수 없습니다.")
        
    st.divider()

    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            st.markdown("<div class='nv-sec-title'>추세 (비용 vs 효율)</div>", unsafe_allow_html=True)
            ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_dual_axis("전체 트렌드", ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
            else:
                ch = _chart_dual_axis(ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                if ch is not None: render_chart(ch)
    except Exception as e:
        st.info(f"추세 데이터를 불러오는 중 오류가 발생했습니다: {e}")

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    if bundle is None or bundle.empty:
        st.warning("예산/잔액 데이터를 불러올 수 없습니다.")
        return

    biz_view = bundle.copy()
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"]
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"]
    biz_view["전일 소진액"] = biz_view["y_cost"]
    biz_view["D-소진"] = biz_view["days_cover"].map(lambda d: "-" if pd.isna(d) else ("99+일" if float(d)>99 else f"{float(d):.1f}일"))
    biz_view["확인일자"] = biz_view["last_update"]

    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3: ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', key='m_need_topup')

    display_df = biz_view[["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
    ui_table_or_dataframe(display_df, key="budget_biz_table", height=520)


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🚀 성과 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_campaign", 200))
    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 200), top_k=10)
    if bundle is None or bundle.empty:
        _render_empty_state_no_data("empty_camp")
        return

    bundle = _perf_common_merge_meta(bundle, meta)
    bundle = add_rates(bundle)

    st.markdown("### 📈 기간 추세 (상관관계)")
    render_period_compare_panel(engine, "campaign", f["start"], f["end"], cids, type_sel, key_prefix="camp", expanded=False)

    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_dual_axis("캠페인 트렌드", ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
            else:
                ch = _chart_dual_axis(ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                if ch is not None: render_chart(ch)
    except Exception as e:
        st.warning(f"트렌드 로드 실패: {e}")

    st.divider()
    df = bundle.sort_values("cost", ascending=False).head(top_n).rename(columns={"account_name": "업체명", "campaign_type": "캠페인유형", "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "매출"})
    df = finalize_display_cols(df)
    
    for c in ["광고비", "매출", "CPC(원)", "CPA(원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r'[^0-9\.-]', '', regex=True), errors='coerce')
    for c in ["노출", "클릭", "전환"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r'[^0-9\.-]', '', regex=True), errors='coerce')

    render_big_table(df, key="camp_main_grid", height=560)


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False): return
    st.markdown("## 🔎 성과 (매체별 키워드/검색어)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_keyword", 300))
    bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        _render_empty_state_no_data("empty_kw")
        return

    ts_total = query_keyword_timeseries(engine, f["start"], f["end"], cids, type_sel)

    def _prepare_main_table(df_in: pd.DataFrame, shopping_first: bool) -> pd.DataFrame:
        if df_in.empty: return pd.DataFrame()
        df = _perf_common_merge_meta(add_rates(df_in), meta)
        view = df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "ctr": "CTR(%)", "cpc": "CPC", "cost": "광고비", "conv": "전환", "cpa": "CPA", "sales": "전환매출", "roas": "ROAS(%)"})
        
        for c in ["광고비", "CPC", "CPA", "전환매출", "노출", "클릭", "전환"]: 
            view[c] = pd.to_numeric(view.get(c, 0), errors="coerce").fillna(0)

        view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
        view["CTR(%)"] = pd.to_numeric(view.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
        view = finalize_ctr_col(view, "CTR(%)")

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드"]
        cols = base_cols + ["전환매출", "ROAS(%)", "광고비", "전환", "CPA", "클릭", "CTR(%)", "CPC", "노출"] if shopping_first else base_cols + ["노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
        return view[[c for c in cols if c in view.columns]].copy()

    tab_pl, tab_shop = st.tabs(["🎯 파워링크 (등록 키워드 관리)", "🛒 쇼핑검색 (사용자 검색어 분석)"])

    with tab_pl:
        st.info("💡 **파워링크 인사이트:** 등록하신 키워드의 입찰가 최적화를 위해 비용 대비 전환(ROAS)을 주로 확인하세요.")
        df_pl = bundle[bundle["campaign_type_label"] == "파워링크"] if "campaign_type_label" in bundle.columns else bundle
        if df_pl.empty: st.warning("데이터 없음")
        else:
            if ts_total is not None and not ts_total.empty:
                ts_pl = ts_total.copy()
                ts_pl["roas"] = np.where(pd.to_numeric(ts_pl["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts_pl["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts_pl["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
                if HAS_ECHARTS: render_echarts_dual_axis("파워링크 트렌드", ts_pl, "dt", "cost", "비용(원)", "roas", "ROAS(%)", height=280)
            render_big_table(_prepare_main_table(df_pl.sort_values("cost", ascending=False).head(top_n), False), "pl_grid", 500)

    with tab_shop:
        st.info("💡 **쇼핑검색 인사이트:** 사용자가 실제 검색한 **'검색어(Search Term)'**입니다. 효율이 좋은 검색어는 상품명에 추가하고, 비용만 소진하는 검색어는 네이버에서 '제외 키워드'로 설정하세요.")
        df_shop = bundle[bundle["campaign_type_label"] == "쇼핑검색"] if "campaign_type_label" in bundle.columns else bundle
        if df_shop.empty: st.warning("데이터 없음")
        else:
            render_big_table(_prepare_main_table(df_shop.sort_values("cost", ascending=False).head(top_n), True), "shop_grid", 500)


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🧩 성과 (광고 소재 A/B 분석)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    st.markdown("#### 🎯 정확한 메시지 테스트를 위한 필터")
    exclude_meaningless = st.toggle("✨ 기본 이미지/상품소재 번호 텍스트 제외하고 보기 (확장소재, 홍보문구만 분석)", value=True, key="ad_exclude_meaningless")

    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_ad", 200))
    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    
    if bundle is None or bundle.empty:
        _render_empty_state_no_data("empty_ad")
        return

    if exclude_meaningless:
        txt = bundle.get("ad_name", pd.Series([""] * len(bundle))).fillna("").astype(str).str.strip()
        norm = txt.str.replace(r"\s+", "", regex=True).str.lower()
        banned = {"상품소재", "상품", "이미지", "이미지소재", "기본", "기본소재", "소재"}
        id_only = (txt != "") & (txt == bundle.get("ad_id", "").astype(str).str.strip())
        keep = (txt != "") & (~norm.isin({b.lower() for b in banned})) & (~id_only)
        bundle = bundle[keep].copy()

    if bundle.empty:
        st.info("필터(의미 없는 소재 제외) 적용 후 표시할 유의미한 텍스트/홍보문구 데이터가 없습니다.")
        return

    df = _perf_common_merge_meta(add_rates(bundle), meta)

    st.markdown("### ⚖️ 소재 인사이트 (Winner vs Loser)")
    c1, c2 = st.columns(2)
    valid_ads = df[df["clk"] >= 10].copy() if not df[df["clk"] >= 10].empty else df.copy()
    
    with c1:
        st.markdown("#### 🏆 고효율 소재 (ROAS 우수)")
        st.caption("고객 반응이 좋아 예산을 더 밀어주면 좋은 소재입니다.")
        top_roas = valid_ads.sort_values("roas", ascending=False).head(3)
        ui_table_or_dataframe(top_roas[["ad_name", "roas", "cost"]].rename(columns={"ad_name":"소재 문구", "roas":"ROAS(%)", "cost":"사용금액"}), "ad_winner", 180)

    with c2:
        st.markdown("#### 💸 비용 집중 소재 (개선 필요)")
        st.caption("비용 소진은 많으나 효율이 떨어져 문구 교체가 필요한 소재입니다.")
        bad_roas = valid_ads.sort_values("cost", ascending=False).head(10).sort_values("roas", ascending=True).head(3)
        ui_table_or_dataframe(bad_roas[["ad_name", "roas", "cost"]].rename(columns={"ad_name":"소재 문구", "roas":"ROAS(%)", "cost":"사용금액"}), "ad_loser", 180)

    st.divider()
    
    st.markdown("### 📈 추세 및 상세 리포트")
    try:
        ts = query_ad_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
            if HAS_ECHARTS: render_echarts_dual_axis("소재 트렌드", ts, "dt", "cost", "비용(원)", "roas", "ROAS(%)", height=300)
    except Exception: pass

    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()
    disp = main_df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_id": "소재ID", "ad_name": "소재내용", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "ctr": "CTR(%)", "cpc": "CPC", "cpa": "CPA", "sales": "전환매출", "roas": "ROAS(%)"})
    
    for c in ["노출", "클릭", "전환", "광고비", "CPC", "CPA", "전환매출"]: 
        disp[c] = pd.to_numeric(disp.get(c, 0), errors="coerce").fillna(0)

    disp["ROAS(%)"] = disp["ROAS(%)"].map(format_roas)
    disp["CTR(%)"] = pd.to_numeric(disp.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "전환매출", "ROAS(%)"]
    render_big_table(disp[[c for c in cols if c in disp.columns]], "ad_big_table", 500)


def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 / 연결")
    try: db_ping(engine); st.success("DB 연결 성공 ✅")
    except Exception as e: st.error(f"DB 연결 실패: {e}"); return
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"])
    colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
    with colA: do_sync = st.button("🔁 동기화 실행", use_container_width=True)
    with colB: 
        if st.button("🧹 캐시 비우기", use_container_width=True): st.cache_data.clear(); st.rerun()
    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: {res.get('meta', 0)}건"); st.cache_data.clear(); st.rerun()
        except Exception as e: st.error(f"실패: {e}")

def main():
    try: engine = get_engine(); latest = get_latest_dates(engine)
    except Exception as e: render_hero(None, BUILD_TAG); st.error(str(e)); return

    render_hero(latest, BUILD_TAG)
    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("### 메뉴")
        if not meta_ready: st.warning("동기화가 필요합니다.")
        nav_items = ["요약(한눈에)", "예산/잔액", "캠페인", "키워드", "소재", "설정/연결"] if meta_ready else ["설정/연결"]
        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")

    st.markdown(f"<div class='nv-h1'>{nav}</div><div style='height:8px'></div>", unsafe_allow_html=True)
    f = None
    if nav != "설정/연결":
        if not meta_ready: st.error("설정 메뉴에서 동기화를 진행해주세요."); return
        f = build_filters(meta, get_campaign_type_options(load_dim_campaign(engine)), engine)

    if nav == "요약(한눈에)": page_overview(meta, engine, f)
    elif nav == "예산/잔액": page_budget(meta, engine, f)
    elif nav == "캠페인": page_perf_campaign(meta, engine, f)
    elif nav == "키워드": page_perf_keyword(meta, engine, f)
    elif nav == "소재": page_perf_ad(meta, engine, f)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
