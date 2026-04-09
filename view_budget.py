# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view (Safely Optimized with Original UI)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
import calendar
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *


def _inject_budget_editor_js_once() -> None:
    if st.session_state.get("_budget_editor_comma_js_injected"):
        return
    _inject_budget_editor_js_once()

    st.data_editor(
        editor_df,
        key="budget_table_editor",
        on_change=update_budget_from_table,
        hide_index=True,
        use_container_width=True,
        height=550,
        column_config={
            "customer_id": None, 
            "monthly_budget_val": None, 
            "prev_month_cost_val": None,
            "current_month_cost_val": None,
            "업체명": st.column_config.TextColumn("업체명", disabled=True),
            "담당자": st.column_config.TextColumn("담당자", disabled=True),
            "월 예산": st.column_config.TextColumn(
                "월 예산(원)", 
                help="더블클릭하여 예산을 바로 수정하세요.",
                required=True
            ),
            f"{end_dt.month}월 사용액": st.column_config.TextColumn(
                f"{end_dt.month}월 사용액", 
                disabled=True
            ),
            f"{prev_m_num}월 사용액": st.column_config.TextColumn(
                f"{prev_m_num}월 사용액", 
                disabled=True
            ),
            "집행률(%)": st.column_config.ProgressColumn(
                "집행률(%)",
                help="월 예산 대비 현재 사용액 비율",
                format="%.1f%%",
                min_value=0,
                max_value=100
            ),
            "상태": st.column_config.TextColumn("상태", disabled=True)
        }
    )


@st.fragment
def render_alert_table(alert_view: pd.DataFrame):
    alert_view["_sort_days"] = pd.to_numeric(alert_view["days_cover"], errors="coerce").fillna(9999)
    alert_view = alert_view.sort_values(by="_sort_days", ascending=True).reset_index(drop=True)

    def get_depletion_date(days_left):
        if pd.isna(days_left) or float(days_left) >= 99: return "여유"
        days = float(days_left)
        if days <= 0: return "즉시 충전 필요"
        deplete_date = date.today() + timedelta(days=int(days))
        return deplete_date.strftime("%m월 %d일 (임박)") if days <= 3 else deplete_date.strftime("%m월 %d일")

    alert_view["예상 중단일"] = alert_view["days_cover"].apply(get_depletion_date)
    
    display_df = alert_view[["account_name", "manager", "bizmoney_balance", "avg_cost", "예상 중단일"]].copy()
    display_df["비즈머니 잔액"] = display_df["bizmoney_balance"].apply(lambda x: format_currency(x))
    
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    display_df[avg_days_label] = display_df["avg_cost"].apply(lambda x: format_currency(x))
    
    display_df = display_df[["account_name", "manager", "비즈머니 잔액", avg_days_label, "예상 중단일"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
    
    def color_alert(val):
        if isinstance(val, str) and '충전 필요' in val:
            return 'color: white; font-weight: 800; background-color: #EF4444;' 
        elif isinstance(val, str) and '임박' in val:
            return 'color: #9A3412; font-weight: 700; background-color: #FFEDD5;' 
        return ''

    try:
        styled_df = display_df.style.map(color_alert, subset=['예상 중단일'])
    except AttributeError:
        styled_df = display_df.style.applymap(color_alert, subset=['예상 중단일'])
    
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>비즈머니 잔액 관리 계정</div>", unsafe_allow_html=True)
    st.dataframe(styled_df, use_container_width=True, hide_index=True, height=500)


@st.fragment
def render_budget_kpis(biz_view: pd.DataFrame, end_dt: date):
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"].astype(str).str.replace(r'[^\d]', '', regex=True), errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())

    c1, c2 = st.columns(2)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), key='m_month_cost')


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)

    view_mode = st.radio(
        "예산 보기 모드",
        ["월 예산 현황", "비즈머니 관리"],
        horizontal=True,
        label_visibility="collapsed",
        key="budget_view_mode",
    )

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday

    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    prev_month_last_day = month_d1 - timedelta(days=1)
    prev_month_d1 = prev_month_last_day.replace(day=1)
    prev_month_d2 = prev_month_last_day

    _, days_in_month = calendar.monthrange(end_dt.year, end_dt.month)
    target_pacing_rate = end_dt.day / days_in_month

    if view_mode == "월 예산 현황":
        avg_d2 = end_dt - timedelta(days=1)
        avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
        bundle = _cached_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        biz_view = _prepare_biz_view(bundle)
        if biz_view.empty:
            st.info("예산 현황 데이터가 없습니다.")
            return

        render_budget_kpis(biz_view.copy(), end_dt)
        st.divider()

        budget_view = _build_budget_editor_view(biz_view, target_pacing_rate)
        local_overrides = st.session_state.get("local_budget_overrides", {})
        if local_overrides and not budget_view.empty:
            for cid, new_val in local_overrides.items():
                m_cid = budget_view["customer_id"].astype(str) == str(cid)
                if not bool(m_cid.any()):
                    continue
                budget_view.loc[m_cid, "monthly_budget"] = new_val
                budget_view.loc[m_cid, "monthly_budget_val"] = int(new_val)
                current_cost = pd.to_numeric(budget_view.loc[m_cid, "current_month_cost_val"], errors="coerce").fillna(0)
                new_budget_float = float(new_val) if float(new_val) > 0 else 0.0
                usage_rate = (current_cost / new_budget_float) if new_budget_float > 0 else 0.0
                budget_view.loc[m_cid, "usage_rate"] = usage_rate
                budget_view.loc[m_cid, "usage_pct"] = usage_rate * 100.0
                budget_view.loc[m_cid, "상태"] = np.select(
                    [
                        budget_view.loc[m_cid, "monthly_budget_val"] == 0,
                        budget_view.loc[m_cid, "usage_rate"] >= 1.0,
                        budget_view.loc[m_cid, "usage_rate"] > target_pacing_rate + 0.1,
                        budget_view.loc[m_cid, "usage_rate"] < target_pacing_rate - 0.1,
                    ],
                    ["미설정", "예산 초과", "과속 소진", "과소 소진"],
                    default="적정 페이스",
                )
                budget_view.loc[m_cid, "_rank"] = np.select(
                    [
                        budget_view.loc[m_cid, "monthly_budget_val"] == 0,
                        budget_view.loc[m_cid, "usage_rate"] >= 1.0,
                        budget_view.loc[m_cid, "usage_rate"] > target_pacing_rate + 0.1,
                        budget_view.loc[m_cid, "usage_rate"] < target_pacing_rate - 0.1,
                    ],
                    [4, 0, 1, 3],
                    default=2,
                )
            budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

        render_budget_editor(budget_view, engine, end_dt, target_pacing_rate)
        return

    alert_avg_d2 = yesterday
    alert_avg_d1 = alert_avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    alert_bundle = _cached_budget_bundle(engine, cids, yesterday, alert_avg_d1, alert_avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
    alert_view = _prepare_alert_view(alert_bundle)
    if alert_view.empty:
        st.info("비즈머니 관리 데이터가 없습니다.")
    else:
        render_alert_table(alert_view)
