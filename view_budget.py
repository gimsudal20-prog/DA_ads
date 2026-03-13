# -*- coding: utf-8 -*-
"""view_budget.py - Budget management and monitoring page view."""

import pandas as pd
import numpy as np
import streamlit as st
import time
from datetime import date, timedelta
import calendar

from data import query_budget_bundle, update_monthly_budget

def page_budget(meta: pd.DataFrame, engine, f: dict):
    st.markdown("<div class='nv-sec-title'>💰 예산 및 비즈머니 관리</div>", unsafe_allow_html=True)

    if meta.empty:
        st.info("등록된 업체 정보가 없습니다. 설정 메뉴에서 동기화를 진행해주세요.")
        return

    today = date.today()
    yesterday = today - timedelta(days=1)
    
    current_month_start = date(today.year, today.month, 1)
    _, last_day = calendar.monthrange(today.year, today.month)
    current_month_end = date(today.year, today.month, last_day)

    avg_d2 = yesterday
    avg_d1 = avg_d2 - timedelta(days=6)
    avg_days = 7

    cids = tuple(f.get("selected_customer_ids", []))

    with st.spinner("예산 및 비즈머니 현황을 불러오는 중..."):
        df = query_budget_bundle(
            engine,
            cids,
            yesterday,
            avg_d1,
            avg_d2,
            current_month_start,
            yesterday,
            avg_days
        )

    if df.empty:
        st.warning("조회된 예산 데이터가 없습니다.")
        return

    days_remaining = max(1, (current_month_end - today).days + 1)

    # 안전한 숫자 변환
    df["monthly_budget"] = pd.to_numeric(df["monthly_budget"], errors="coerce").fillna(0)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce").fillna(0)
    df["current_month_cost"] = pd.to_numeric(df["current_month_cost"], errors="coerce").fillna(0)
    df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0)

    # 지표 계산
    df["projected_cost"] = df["current_month_cost"] + (df["avg_cost"] * days_remaining)
    df["budget_usage_pct"] = np.where(df["monthly_budget"] > 0, (df["current_month_cost"] / df["monthly_budget"]) * 100, 0.0)
    df["projected_usage_pct"] = np.where(df["monthly_budget"] > 0, (df["projected_cost"] / df["monthly_budget"]) * 100, 0.0)
    df["remaining_budget"] = df["monthly_budget"] - df["current_month_cost"]
    df["recommended_daily_cost"] = np.where(df["remaining_budget"] > 0, df["remaining_budget"] / days_remaining, 0.0)

    editor_df = df[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost", "budget_usage_pct", "avg_cost", "projected_cost", "projected_usage_pct", "remaining_budget", "recommended_daily_cost", "bizmoney_balance"]].copy()
    
    editor_df.columns = ["커스텀ID", "업체명", "담당자", "월 목표 예산(수정가능)", "현재 누적 소진액", "소진율(%)", "최근 7일 평균 소진", "월 예상 소진액", "예상 소진율(%)", "남은 예산", "권장 일일 소진액", "비즈머니 잔액"]

    st.markdown("##### 📝 업체별 예산 관리 표")
    st.caption("💡 **'월 목표 예산'** 칸을 더블 클릭하여 금액을 숫자로 입력하고 Enter를 누르면 실시간으로 저장 및 재계산됩니다.")

    # 기본 설정만 유지한 깔끔한 데이터 에디터
    edited_df = st.data_editor(
        editor_df,
        hide_index=True,
        use_container_width=True,
        key="budget_editor"
    )

    if "budget_editor" in st.session_state:
        changes = st.session_state["budget_editor"].get("edited_rows", {})
        if changes:
            has_updates = False
            for row_idx, cols in changes.items():
                if "월 목표 예산(수정가능)" in cols:
                    returned_budget = cols["월 목표 예산(수정가능)"]
                    cid = editor_df.iloc[row_idx]["커스텀ID"]

                    # 🚀 [오류 해결 핵심] 어떤 이상한 값이 들어와도 에러 없이 처리하는 완벽 방어막
                    try:
                        if returned_budget is None or str(returned_budget).strip() == "":
                            safe_budget = 0
                        else:
                            clean_val = str(returned_budget).replace(',', '').replace('원', '').replace('₩', '').strip()
                            safe_budget = int(float(clean_val))
                    except (ValueError, TypeError):
                        safe_budget = -1 

                    if safe_budget >= 0:
                        update_monthly_budget(engine, int(cid), safe_budget)
                        has_updates = True

            if has_updates:
                st.toast("✅ 예산이 성공적으로 업데이트되었습니다!", icon="💾")
                time.sleep(0.5)
                st.rerun()
