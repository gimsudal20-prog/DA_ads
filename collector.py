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
from data import period_compare_range, pct_to_arrow, _get_table_names_cached, _pct_change
from ui import *

BUILD_TAG = os.getenv("APP_BUILD", "v10.3 (쇼핑검색 우회처리 및 UI 최적화)")
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

def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("<div class='nv-sec-title'>요약 및 인사이트</div>", unsafe_allow_html=True)
        st.caption(f"기간: {f['start']} ~ {f['end']}")
    with col2:
        cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
        
        with st.spinner("보고서 생성 중..."):
            cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
            df_summary = pd.DataFrame([cur_summary])
            
            camp_bndl = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=50)
            camp_df = _perf_common_merge_meta(add_rates(camp_bndl), meta) if not camp_bndl.empty else pd.DataFrame()
            
            kw_bndl = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50)
            kw_df = _perf_common_merge_meta(add_rates(kw_bndl), meta) if not kw_bndl.empty else pd.DataFrame()

            excel_data = generate_full_report_excel(df_summary, camp_df, kw_df)
            
            st.download_button(
                label="📥 보고서(Excel) 다운로드",
                data=excel_data,
                file_name=f"광고보고서_{f['start']}_{f['end']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )

    cmp_mode = st.radio("비교 기준", ["전일대비", "전주대비", "전월대비"], horizontal=True, index=1, key="ov_cmp_mode")

    cur = cur_summary
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

    st.markdown("<div class='nv-sec-title'>💡 주요 최적화 포인트</div>", unsafe_allow_html=True)
    
    if kw_df is not None and not kw_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            with st.container(border=True):
                st.markdown("<h4 style='margin-bottom: 4px; margin-top: 0;'>🚨 저효율 키워드 (개선 필요)</h4>", unsafe_allow_html=True)
                st.caption("비용 3만 원 이상 소진 중이나 전환이 0건인 키워드입니다. (제외 권장)")
                hippos = kw_df[(kw_df['cost'] >= 30000) & (kw_df['conv'] == 0)].sort_values('cost', ascending=False)
                if not hippos.empty:
                    disp_h = hippos[['account_name', 'keyword', 'cost']].rename(columns={'account_name': '업체명', 'keyword': '키워드', 'cost': '비용'})
                    disp_h['비용'] = disp_h['비용'].apply(format_currency)
                    st_dataframe_safe(disp_h.head(5), hide_index=True, use_container_width=True)
                else: 
                    st.success("✅ 해당되는 저효율 키워드가 없습니다.")

        with c2:
            with st.container(border=True):
                st.markdown("<h4 style='margin-bottom: 4px; margin-top: 0;'>⭐ 고효율 키워드 (기회 발굴)</h4>", unsafe_allow_html=True)
                st.caption("비용 5만 원 미만 소진, ROAS 500% 이상 기록 중인 우수 키워드입니다. (입찰가 상향 권장)")
                stars = kw_df[(kw_df['cost'] <= 50000) & (kw_df['conv'] >= 1) & (kw_df['roas'] >= 500)].sort_values('roas', ascending=False)
                if not stars.empty:
                    disp_s = stars[['account_name', 'keyword', 'roas']].rename(columns={'account_name': '업체명', 'keyword': '키워드', 'roas': 'ROAS(%)'})
                    disp_s['ROAS(%)'] = disp_s['ROAS(%)'].apply(format_roas)
                    st_dataframe_safe(disp_s.head(5), hide_index=True, use_container_width=True)
                else: 
                    st.info("해당되는 고효율 키워드가 없습니다.")
    st.divider()

    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is not None and not ts.empty:
            st.markdown("### 📅 트렌드 및 요일별 효율 분석")
            tab_trend, tab_dow = st.tabs(["전체 트렌드", "요일별 분석"])
            
            with tab_trend:
                ts["roas"] = np.where(pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
                if HAS_ECHARTS: render_echarts_dual_axis("전체 트렌드", ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                
            with tab_dow:
                st.caption("💡 주말(토/일)과 평일의 효율(ROAS) 차이를 확인하고 요일별 입찰 가중치를 조절하세요.")
                if HAS_ECHARTS: render_echarts_dow_bar(ts, height=320)
                
    except Exception as e:
        st.info(f"추세 데이터를 불러오는 중 오류가 발생했습니다: {e}")

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 및 목표 KPI 관리")
    
    target_roas = st.slider("🎯 전사 목표 ROAS (%) 설정", min_value=100, max_value=1000, value=300, step=50, help="이 목표치에 따라 아래 표의 기상도가 동적으로 변합니다.")
    
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
    
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    biz_view["잔액상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "잔액상태"] = "🔴 충전요망"

    biz_view["current_roas"] = np.where(biz_view["current_month_cost"] > 0, (biz_view["current_month_sales"] / biz_view["current_month_cost"]) * 100, 0)
    
    def get_weather(roas, target):
        if roas >= target: return "☀️ 맑음"
        elif roas >= target * 0.8: return "☁️ 흐림" 
        else: return "☔ 비상"
        
    biz_view["ROAS 기상도"] = biz_view["current_roas"].apply(lambda x: get_weather(x, target_roas))
    biz_view["당월 ROAS"] = biz_view["current_roas"].apply(format_roas)

    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 소진"] = biz_view["avg_cost"].map(format_currency)
    biz_view["D-소진"] = biz_view["days_cover"].map(lambda d: "-" if pd.isna(d) else ("99+일" if float(d)>99 else f"{float(d):.1f}일"))

    st.markdown("<div class='nv-sec-title'>🔍 전체 계정 현황 및 기상도</div>", unsafe_allow_html=True)
    
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_rain = int(biz_view["ROAS 기상도"].astype(str).str.contains("비상").sum())

    c1, c2, c3 = st.columns(3)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 합산', key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3: ui_metric_or_stmetric('효율 ☔ 비상 계정', f"{count_rain}건", f'목표 ROAS {target_roas}% 미달', key='m_need_opt')

    display_df = biz_view[["account_name", "manager", "비즈머니 잔액", "잔액상태", "당월 ROAS", "ROAS 기상도"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
    render_big_table(display_df, key="budget_biz_table", height=450)

    st.divider()
    st.markdown(f"### 📅 당월 예산 설정 및 집행률 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")

    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

    def _status(rate: float, budget: int):
        if budget == 0: return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0: return ("🔴 초과", "초과", 0)
        if rate >= 0.9: return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)

    tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
    budget_view["상태"] = tmp[0]
    budget_view["status_text"] = tmp[1]
    budget_view["_rank"] = tmp[2].astype(int)

    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

    budget_view_disp = budget_view.copy()
    budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
    budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
    budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].map(lambda x: round(float(x), 1) if pd.notna(x) else 0.0)

    disp_cols = ["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]
    table_df = budget_view_disp[disp_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    c_table, c_form = st.columns([3, 1])
    with c_table:
        render_budget_month_table_with_bars(table_df, key="budget_month_table", height=520)

    with c_form:
        st.markdown("#### ✍️ 월 예산 설정/수정")
        st.caption("예산을 입력하면 좌측 표에 즉시 반영됩니다.")
        
        opts = budget_view_disp[["customer_id", "account_name"]].copy()
        opts["label"] = opts["account_name"].astype(str) + " (" + opts["customer_id"].astype(str) + ")"
        labels = opts["label"].tolist()
        label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

        with st.form("budget_update_form", clear_on_submit=False):
            sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
            cur_budget = 0
            if labels:
                cid = int(label_to_cid.get(sel, 0))
                cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == cid, "monthly_budget_val"].iloc[0])
            
            new_budget = st.text_input("새 월 예산 (예: 500,000)", value=format_number_commas(cur_budget) if labels else "0")
            submitted = st.form_submit_button("💾 저장", type="primary", use_container_width=True)

        if submitted and labels:
            cid = int(label_to_cid.get(sel, 0))
            nb = parse_currency(new_budget)
            update_monthly_budget(engine, cid, nb)
            st.success("예산 수정 완료! (새로고침 됩니다)")
            st.cache_data.clear()
            time.sleep(0.5)
            st.rerun()

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🚀 성과 (캠페인)")
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_campaign", 200))
    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 200), top_k=10)
    if bundle is None or bundle.empty: return

    bundle = _perf_common_merge_meta(bundle, meta)
    bundle = add_rates(bundle)

    df = bundle.sort_values("cost", ascending=False).head(top_n).rename(columns={"account_name": "업체명", "campaign_type": "캠페인유형", "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "매출"})
    df = finalize_display_cols(df)
    
    for c in ["광고비", "매출", "CPC(원)", "CPA(원)", "노출", "클릭", "전환"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c].astype(str).str.replace(r'[^0-9\.-]', '', regex=True), errors='coerce')

    render_big_table(df, key="camp_main_grid", height=560)

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False): return
    st.markdown("## 🔎 성과 (매체별 키워드/검색어)")
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_keyword", 300))
    
    bundle = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=top_n)

    def _prepare_main_table(df_in: pd.DataFrame, shopping_first: bool) -> pd.DataFrame:
        if df_in is None or df_in.empty: return pd.DataFrame()
        df = _perf_common_merge_meta(add_rates(df_in), meta)
        view = df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "ctr": "CTR(%)", "cpc": "CPC", "cost": "광고비", "conv": "전환", "cpa": "CPA", "sales": "전환매출", "roas": "ROAS(%)"})
        for c in ["광고비", "CPC", "CPA", "전환매출", "노출", "클릭", "전환"]: view[c] = pd.to_numeric(view.get(c, 0), errors="coerce").fillna(0)
        view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
        view["CTR(%)"] = pd.to_numeric(view.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
        view = finalize_ctr_col(view, "CTR(%)")

        base_cols = ["업체명", "캠페인유형", "캠페인", "광고그룹", "키워드"]
        cols = base_cols + ["전환매출", "ROAS(%)", "광고비", "전환", "CPA", "클릭", "CTR(%)", "CPC", "노출"] if shopping_first else base_cols + ["노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
        return view[[c for c in cols if c in view.columns]].copy()

    tab_pl, tab_shop = st.tabs(["🎯 파워링크", "🛒 쇼핑검색 (검색어)"])
    
    with tab_pl:
        df_pl = bundle[bundle["campaign_type_label"] == "파워링크"] if bundle is not None and not bundle.empty and "campaign_type_label" in bundle.columns else bundle
        if df_pl is not None and not df_pl.empty: 
            render_big_table(_prepare_main_table(df_pl.sort_values("cost", ascending=False).head(top_n), shopping_first=False), "pl_grid", 500)
        else:
            st.info("파워링크 데이터가 없습니다.")
            
    with tab_shop:
        st.info("💡 **쇼핑검색 인사이트:** 사용자가 실제 검색한 **'검색어(Search Term)'**입니다. 불필요한 검색어는 비용 낭비를 막기 위해 제외 키워드로 설정하세요.")
        
        shop_bundle = query_search_term_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=top_n)
        
        if shop_bundle is not None and "_debug_msg" in shop_bundle.columns:
            # DB에 전용 검색어 테이블이 없는 경우 -> 에러를 띄우지 않고 일반 키워드 테이블에서 우회 처리(Fallback)
            df_shop_fb = bundle[bundle["campaign_type_label"] == "쇼핑검색"] if bundle is not None and not bundle.empty and "campaign_type_label" in bundle.columns else pd.DataFrame()
            if df_shop_fb is not None and not df_shop_fb.empty: 
                render_big_table(_prepare_main_table(df_shop_fb.sort_values("cost", ascending=False).head(top_n), shopping_first=True), "shop_grid_fb", 500)
            else:
                st.info("조회된 쇼핑검색 데이터가 없습니다.")
                
        elif shop_bundle is not None and not shop_bundle.empty:
            render_big_table(_prepare_main_table(shop_bundle.sort_values("cost", ascending=False).head(top_n), shopping_first=True), "shop_grid", 500)
        else:
            st.info("조회된 쇼핑검색(검색어) 데이터가 없습니다.")

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🧩 성과 (광고 소재 분석)")
    cids, type_sel, top_n = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), int(f.get("top_n_ad", 200))
    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty: return

    df = _perf_common_merge_meta(add_rates(bundle), meta)
    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()
    disp = main_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인", "ad_name": "소재내용", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "ctr": "CTR(%)", "cpc": "CPC", "cpa": "CPA", "sales": "전환매출", "roas": "ROAS(%)"})
    
    for c in ["노출", "클릭", "전환", "광고비", "CPC", "CPA", "전환매출"]: disp[c] = pd.to_numeric(disp.get(c, 0), errors="coerce").fillna(0)
    disp["ROAS(%)"] = disp["ROAS(%)"].map(format_roas)
    disp["CTR(%)"] = pd.to_numeric(disp.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "캠페인", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "전환매출", "ROAS(%)"]
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

    st.divider()

    st.markdown("### 🗑️ 강제 삭제 도구 (수동 DB 소각)")
    st.caption("동기화 후에도 계속 뜨는 악성 '유령 계정'이 있다면 커스텀 ID(숫자)를 입력해 과거 데이터까지 DB에서 완전히 소각하세요.")
    
    col_del1, col_del2 = st.columns([2, 1])
    with col_del1:
        del_cid = st.text_input("삭제할 커스텀 ID 입력", placeholder="예: 12345678", label_visibility="collapsed")
    with col_del2:
        if st.button("🗑️ 완전 삭제", type="primary", use_container_width=True):
            if del_cid.strip() and del_cid.strip().isdigit():
                try:
                    cid_val = str(del_cid.strip())
                    sql_exec(engine, "DELETE FROM dim_account_meta WHERE customer_id = :cid", {"cid": int(cid_val)})
                    for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_search_term_daily", "fact_ad_daily", "fact_bizmoney_daily"]:
                        try: sql_exec(engine, f"DELETE FROM {table} WHERE customer_id::text = :cid", {"cid": cid_val})
                        except Exception: pass
                            
                    st.success(f"✅ ID '{del_cid}' 업체의 모든 데이터가 영구 소각되었습니다.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 중 오류 발생: {e}")
            else:
                st.warning("유효한 숫자 형태의 커스텀 ID를 입력해주세요.")

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
