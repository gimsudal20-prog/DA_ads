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

# ⚡ 고속 렌더링을 위한 DB 데이터 캐싱 래퍼 함수 (재렌더링 시 DB 조회 생략)
@st.cache_data(ttl=300, show_spinner=False, max_entries=20)
def _cached_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, topup_avg_days: int) -> pd.DataFrame:
    try:
        return query_budget_bundle(_engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, topup_avg_days)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _prepare_biz_view(bundle: pd.DataFrame) -> pd.DataFrame:
    if bundle is None or bundle.empty:
        return pd.DataFrame()
    biz_view = bundle.copy()
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    return biz_view


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _prepare_alert_view(bundle: pd.DataFrame) -> pd.DataFrame:
    if bundle is None or bundle.empty:
        return pd.DataFrame()
    alert_view = bundle.copy()
    m_alert = alert_view["avg_cost"].astype(float) > 0
    alert_view.loc[m_alert, "days_cover"] = alert_view.loc[m_alert, "bizmoney_balance"].astype(float) / alert_view.loc[m_alert, "avg_cost"].astype(float)
    return alert_view


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _build_alert_display(alert_view: pd.DataFrame) -> pd.DataFrame:
    if alert_view is None or alert_view.empty:
        return pd.DataFrame()

    df = alert_view.copy()
    df["_sort_days"] = pd.to_numeric(df.get("days_cover"), errors="coerce").fillna(9999)
    df = df.sort_values(by="_sort_days", ascending=True).reset_index(drop=True)

    def get_depletion_date(days_left):
        if pd.isna(days_left) or float(days_left) >= 99:
            return "여유"
        days = float(days_left)
        if days <= 0:
            return "즉시 충전 필요"
        deplete_date = date.today() + timedelta(days=int(days))
        return deplete_date.strftime("%m월 %d일 (임박)") if days <= 3 else deplete_date.strftime("%m월 %d일")

    df["예상 중단일"] = df["days_cover"].apply(get_depletion_date)
    df["비즈머니 잔액"] = df["bizmoney_balance"].apply(lambda x: format_currency(x))
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    df[avg_days_label] = df["avg_cost"].apply(lambda x: format_currency(x))
    df["담당자"] = df.get("manager", "미배정")
    df["업체명"] = df.get("account_name", df.get("customer_id", "-"))
    df["알림"] = np.select(
        [
            df["예상 중단일"].astype(str).str.contains("충전 필요", na=False),
            df["예상 중단일"].astype(str).str.contains("임박", na=False),
        ],
        ["🔴", "🟠"],
        default="",
    )
    return df[["알림", "업체명", "담당자", "비즈머니 잔액", avg_days_label, "예상 중단일"]]


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _build_budget_editor_view(biz_view: pd.DataFrame, target_pacing_rate: float) -> pd.DataFrame:
    if biz_view is None or biz_view.empty:
        return pd.DataFrame()
    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "prev_month_cost", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["prev_month_cost_val"] = pd.to_numeric(budget_view.get("prev_month_cost", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)
    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)
    cond_zero = budget_view["monthly_budget_val"] == 0
    cond_over = budget_view["usage_rate"] >= 1.0
    cond_fast = budget_view["usage_rate"] > target_pacing_rate + 0.1
    cond_slow = budget_view["usage_rate"] < target_pacing_rate - 0.1
    budget_view["상태"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        ["미설정", "예산 초과", "과속 소진", "과소 소진"],
        default="적정 페이스",
    )
    budget_view["_rank"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        [4, 0, 1, 3],
        default=2,
    )
    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)
    return budget_view




def _ensure_budget_input_js_once():
    if st.session_state.get("_budget_input_js_once"):
        return
    # no-op guard: legacy hook kept for compatibility, but avoid recursive rerun cost
    st.session_state["_budget_input_js_once"] = True


@st.fragment
def render_budget_editor(budget_view: pd.DataFrame, engine, end_dt: date, target_pacing_rate: float):
    prev_month_dt = (end_dt.replace(day=1) - timedelta(days=1))
    prev_m_num = prev_month_dt.month
    
    editor_df = budget_view[["customer_id", "account_name", "manager", "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val", "usage_pct", "상태"]].copy()
    
    editor_df["월 예산"] = editor_df["monthly_budget_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{end_dt.month}월 사용액"] = editor_df["current_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{prev_m_num}월 사용액"] = editor_df["prev_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    
    editor_df = editor_df.rename(columns={
        "account_name": "업체명", 
        "manager": "담당자", 
        "usage_pct": "집행률(%)"
    })

    # ✨ 화면에 보여질 컬럼의 순서를 '월 예산 -> 당월 사용액 -> 전월 사용액 -> 집행률' 순서로 고정
    ordered_cols = [
        "customer_id", "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val", # 숨김 처리용
        "업체명", "담당자", "월 예산", f"{end_dt.month}월 사용액", f"{prev_m_num}월 사용액", "집행률(%)", "상태"
    ]
    editor_df = editor_df[ordered_cols]

    def update_budget_from_table():
        if "budget_table_editor" in st.session_state:
            edits = st.session_state["budget_table_editor"].get("edited_rows", {})
            updated_count = 0
            
            if "local_budget_overrides" not in st.session_state:
                st.session_state["local_budget_overrides"] = {}
                
            for row_idx, col_data in edits.items():
                if "월 예산" in col_data:
                    raw_input = str(col_data["월 예산"]).replace(",", "").replace("원", "").strip()
                    if raw_input.isdigit():
                        new_budget = int(raw_input)
                        cid = str(editor_df.iloc[row_idx]["customer_id"])
                        
                        update_monthly_budget(engine, cid, new_budget)
                        st.session_state["local_budget_overrides"][cid] = new_budget
                        updated_count += 1
            
            if updated_count > 0:
                st.toast("예산이 저장되었습니다.")

    st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:4px;'>{end_dt.strftime('%Y년 %m월')} 예산 집행률 (현재 권장 소진율: <span style='color:#0528F2;'>{target_pacing_rate*100:.0f}%</span>)</div>", unsafe_allow_html=True)
    st.caption("표의 '월 예산(원)' 칸을 더블클릭하여 수정하세요. 권장 소진율 대비 10% 이상 차이가 나면 과속/과소 상태로 진단됩니다.")
    _ensure_budget_input_js_once()

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
    display_df = _build_alert_display(alert_view)
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>비즈머니 잔액 관리 계정</div>", unsafe_allow_html=True)
    if display_df.empty:
        st.info("비즈머니 관리 데이터가 없습니다.")
        return
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "알림": st.column_config.TextColumn(" ", width="small"),
            "업체명": st.column_config.TextColumn("업체명"),
            "담당자": st.column_config.TextColumn("담당자"),
            "비즈머니 잔액": st.column_config.TextColumn("비즈머니 잔액"),
            avg_days_label: st.column_config.TextColumn(avg_days_label),
            "예상 중단일": st.column_config.TextColumn("예상 중단일"),
        },
    )


@st.fragment
def render_budget_kpis(biz_view: pd.DataFrame, end_dt: date):
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"].astype(str).str.replace(r'[^\d]', '', regex=True), errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())

    c1, c2 = st.columns(2)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), key='m_month_cost')


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)
    
    selected_view = st.radio("보기", ["월 예산 현황", "비즈머니 관리"], horizontal=True, label_visibility="collapsed", key="budget_view_mode")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    prev_month_last_day = month_d1 - timedelta(days=1)
    prev_month_d1 = prev_month_last_day.replace(day=1)
    prev_month_d2 = prev_month_last_day

    _, days_in_month = calendar.monthrange(end_dt.year, end_dt.month)
    current_day = end_dt.day
    target_pacing_rate = current_day / days_in_month

    if selected_view == "월 예산 현황":
        bundle = _cached_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        biz_view = _prepare_biz_view(bundle)
        if biz_view.empty:
            st.info("예산 현황 데이터가 없습니다.")
        else:
            render_budget_kpis(biz_view.copy(), end_dt)
            st.divider()

            budget_view = _build_budget_editor_view(biz_view, target_pacing_rate)

            if "local_budget_overrides" in st.session_state and not budget_view.empty:
                for cid, new_val in st.session_state["local_budget_overrides"].items():
                    m_cid = budget_view["customer_id"].astype(str) == str(cid)
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
    else:
        alert_avg_d2 = yesterday
        alert_avg_d1 = alert_avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
        alert_bundle = _cached_budget_bundle(engine, cids, yesterday, alert_avg_d1, alert_avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        alert_view = _prepare_alert_view(alert_bundle)
        if alert_view.empty:
            st.info("비즈머니 관리 데이터가 없습니다.")
        else:
            render_alert_table(alert_view)
