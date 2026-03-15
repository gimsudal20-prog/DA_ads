# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view."""

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

@st.fragment
def render_budget_editor(budget_view: pd.DataFrame, engine, end_dt: date, target_pacing_rate: float):
    editor_df = budget_view[["customer_id", "account_name", "manager", "monthly_budget_val", "current_month_cost_val", "usage_pct", "상태"]].copy()
    
    editor_df["월 예산"] = editor_df["monthly_budget_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{end_dt.month}월 사용액"] = editor_df["current_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    
    editor_df = editor_df.rename(columns={
        "account_name": "업체명", 
        "manager": "담당자", 
        "usage_pct": "집행률(%)"
    })

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

    components.html("""
    <script>
    const doc = window.parent.document;
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            mutation.addedNodes.forEach((node) => {
                if (node.nodeType === 1) { 
                    let inputs = (node.tagName === 'INPUT' || node.tagName === 'TEXTAREA') ? [node] : node.querySelectorAll('input, textarea');
                    inputs.forEach(targetInput => {
                        if (targetInput && !targetInput.dataset.commaAttached) {
                            targetInput.addEventListener('input', function(e) {
                                let rawValue = this.value.replace(/[^0-9]/g, '');
                                if (rawValue !== '') {
                                    let formatted = parseInt(rawValue, 10).toLocaleString('ko-KR');
                                    if (this.value !== formatted) {
                                        let cursorPosition = this.selectionStart;
                                        let oldLength = this.value.length;
                                        
                                        let setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
                                        if (this.tagName === 'TEXTAREA') {
                                            setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value")?.set;
                                        }
                                        
                                        if (setter) {
                                            setter.call(this, formatted);
                                            this.dispatchEvent(new Event('input', { bubbles: true }));
                                        } else {
                                            this.value = formatted;
                                        }
                                        
                                        let newLength = this.value.length;
                                        let newPos = cursorPosition + (newLength - oldLength);
                                        try { this.setSelectionRange(newPos, newPos); } catch(err) {}
                                    }
                                }
                            });
                            targetInput.dataset.commaAttached = "true";
                        }
                    });
                }
            });
        });
    });
    observer.observe(doc.body, { childList: true, subtree: true });
    </script>
    """, height=0, width=0)

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
    biz_view["current_roas"] = np.where(biz_view["current_month_cost"] > 0, (biz_view["current_month_sales"] / biz_view["current_month_cost"]) * 100, 0)
    
    target_roas = st.slider("전사 목표 ROAS (%)", min_value=100, max_value=1000, value=300, step=50, key="budget_roas_slider")
    
    def get_weather(roas, target):
        if roas >= target: return "목표 달성"
        elif roas >= target * 0.8: return "모니터링" 
        else: return "효율 비상"
        
    biz_view["ROAS 기상도"] = biz_view["current_roas"].apply(lambda x: get_weather(x, target_roas))
    
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"].astype(str).str.replace(r'[^\d]', '', regex=True), errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_rain = int(biz_view["ROAS 기상도"].astype(str).str.contains("비상").sum())

    c1, c2, c3 = st.columns(3)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), key='m_month_cost')
    with c3: ui_metric_or_stmetric('효율 비상 계정', f"{count_rain}건", key='m_need_opt')


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)
    
    tab_budget, tab_alert = st.tabs(["월 예산 현황", "비즈머니 관리"])
    
    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    _, days_in_month = calendar.monthrange(end_dt.year, end_dt.month)
    current_day = end_dt.day
    target_pacing_rate = current_day / days_in_month

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)

    alert_avg_d2 = yesterday
    alert_avg_d1 = alert_avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    alert_bundle = query_budget_bundle(engine, cids, yesterday, alert_avg_d1, alert_avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    
    if bundle is None or bundle.empty:
        biz_view = pd.DataFrame()
    else:
        biz_view = bundle.copy()
        m = biz_view["avg_cost"].astype(float) > 0
        biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
        biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
        biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    if alert_bundle is None or alert_bundle.empty:
        alert_view = pd.DataFrame()
    else:
        alert_view = alert_bundle.copy()
        m_alert = alert_view["avg_cost"].astype(float) > 0
        alert_view.loc[m_alert, "days_cover"] = alert_view.loc[m_alert, "bizmoney_balance"].astype(float) / alert_view.loc[m_alert, "avg_cost"].astype(float)

    with tab_budget:
        if biz_view.empty:
            st.info("예산 현황 데이터가 없습니다.")
        else:
            render_budget_kpis(biz_view.copy(), end_dt)
            st.divider()

            budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
            
            if "local_budget_overrides" in st.session_state:
                for cid, new_val in st.session_state["local_budget_overrides"].items():
                    m_cid = budget_view["customer_id"].astype(str) == str(cid)
                    budget_view.loc[m_cid, "monthly_budget"] = new_val

            budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
            budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

            budget_view["usage_rate"] = 0.0
            m2 = budget_view["monthly_budget_val"] > 0
            budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
            budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

            def _status(rate: float, budget: int):
                if budget == 0: return ("미설정", 4)
                if rate >= 1.0: return ("예산 초과", 0)
                if rate > target_pacing_rate + 0.1: return ("과속 소진", 1)
                if rate < target_pacing_rate - 0.1: return ("과소 소진", 3)
                return ("적정 페이스", 2)

            tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
            budget_view["상태"] = tmp[0]
            budget_view["_rank"] = tmp[1].astype(int)

            budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

            render_budget_editor(budget_view, engine, end_dt, target_pacing_rate)

    with tab_alert:
        if alert_view.empty:
            st.info("비즈머니 관리 데이터가 없습니다.")
        else:
            render_alert_table(alert_view)
