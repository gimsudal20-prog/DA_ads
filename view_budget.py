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
    st.caption("월별 목표 예산을 설정하고, 현재 소진 페이스와 비즈머니 잔액을 모니터링합니다.")

    if meta.empty:
        st.info("등록된 업체 정보가 없습니다. 설정 메뉴에서 동기화를 진행해주세요.")
        return

    today = date.today()
    yesterday = today - timedelta(days=1)

    # 현재 월의 1일과 마지막 날 구하기
    current_month_start = date(today.year, today.month, 1)
    _, last_day = calendar.monthrange(today.year, today.month)
    current_month_end = date(today.year, today.month, last_day)

    # 최근 7일 평균 소진액 계산용 (어제 기준)
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

    # 남은 일수 계산 (오늘부터 말일까지)
    days_remaining = (current_month_end - today).days + 1
    if days_remaining <= 0:
        days_remaining = 1

    # 지표 계산을 위한 안전한 숫자 변환
    df["monthly_budget"] = pd.to_numeric(df["monthly_budget"], errors="coerce").fillna(0)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce").fillna(0)
    df["current_month_cost"] = pd.to_numeric(df["current_month_cost"], errors="coerce").fillna(0)
    df["bizmoney_balance"] = pd.to_numeric(df["bizmoney_balance"], errors="coerce").fillna(0)

    # 예상 소진액 = 현재까지 소진액 + (최근 평균 소진액 * 남은 일수)
    df["projected_cost"] = df["current_month_cost"] + (df["avg_cost"] * days_remaining)

    # 예산 소진율
    df["budget_usage_pct"] = np.where(
        df["monthly_budget"] > 0,
        (df["current_month_cost"] / df["monthly_budget"]) * 100,
        0.0
    )

    # 예상 소진율
    df["projected_usage_pct"] = np.where(
        df["monthly_budget"] > 0,
        (df["projected_cost"] / df["monthly_budget"]) * 100,
        0.0
    )

    # 남은 예산
    df["remaining_budget"] = df["monthly_budget"] - df["current_month_cost"]

    # 권장 일일 소진액 = 남은 예산 / 남은 일수
    df["recommended_daily_cost"] = np.where(
        df["remaining_budget"] > 0,
        df["remaining_budget"] / days_remaining,
        0.0
    )

    # 화면 출력을 위한 데이터프레임 복사
    disp_df = df.copy()

    # 데이터 에디터를 위한 컬럼 구성
    editor_df = disp_df[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost", "budget_usage_pct", "avg_cost", "projected_cost", "projected_usage_pct", "remaining_budget", "recommended_daily_cost", "bizmoney_balance"]].copy()

    editor_df.columns = ["커스텀ID", "업체명", "담당자", "월 목표 예산(수정가능)", "현재 누적 소진액", "소진율(%)", "최근 7일 평균 소진", "월 예상 소진액", "예상 소진율(%)", "남은 예산", "권장 일일 소진액", "비즈머니 잔액"]

    # 에러 방지: 데이터 타입 명시적 변환
    for col in ["월 목표 예산(수정가능)", "현재 누적 소진액", "최근 7일 평균 소진", "월 예상 소진액", "남은 예산", "권장 일일 소진액", "비즈머니 잔액"]:
        editor_df[col] = pd.to_numeric(editor_df[col], errors="coerce").fillna(0).astype(int)

    for col in ["소진율(%)", "예상 소진율(%)"]:
        editor_df[col] = pd.to_numeric(editor_df[col], errors="coerce").fillna(0.0).astype(float)

    st.markdown("##### 📝 업체별 예산 관리 표")
    st.caption("💡 **'월 목표 예산'** 칸을 더블 클릭하여 금액을 숫자로 입력하고 Enter를 누르면 실시간으로 저장 및 재계산됩니다.")

    # st.data_editor 사용
    edited_df = st.data_editor(
        editor_df,
        column_config={
            "커스텀ID": st.column_config.TextColumn("커스텀ID", disabled=True),
            "업체명": st.column_config.TextColumn("업체명", disabled=True),
            "담당자": st.column_config.TextColumn("담당자", disabled=True),
            "월 목표 예산(수정가능)": st.column_config.NumberColumn(
                "월 목표 예산(수정가능)",
                help="이번 달 목표 예산을 입력하세요. (숫자만 입력)",
                min_value=0,
                step=10000,
                format="%d ₩"
            ),
            "현재 누적 소진액": st.column_config.NumberColumn("현재 누적 소진액", disabled=True, format="%d ₩"),
            "소진율(%)": st.column_config.ProgressColumn("소진율(%)", help="현재 누적 소진액 / 목표 예산", format="%.1f%%", min_value=0, max_value=100),
            "최근 7일 평균 소진": st.column_config.NumberColumn("최근 7일 평균 소진", disabled=True, format="%d ₩"),
            "월 예상 소진액": st.column_config.NumberColumn("월 예상 소진액", disabled=True, format="%d ₩"),
            "예상 소진율(%)": st.column_config.NumberColumn("예상 소진율(%)", disabled=True, format="%.1f%%"),
            "남은 예산": st.column_config.NumberColumn("남은 예산", disabled=True, format="%d ₩"),
            "권장 일일 소진액": st.column_config.NumberColumn("권장 일일 소진액", disabled=True, format="%d ₩"),
            "비즈머니 잔액": st.column_config.NumberColumn("비즈머니 잔액", disabled=True, format="%d ₩"),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="budget_editor"
    )

    # 변경사항 감지 및 DB 업데이트 로직
    if "budget_editor" in st.session_state:
        changes = st.session_state["budget_editor"].get("edited_rows", {})
        if changes:
            has_updates = False
            for row_idx, cols in changes.items():
                if "월 목표 예산(수정가능)" in cols:
                    returned_budget = cols["월 목표 예산(수정가능)"]
                    cid = editor_df.iloc[row_idx]["커스텀ID"]

                    # 🚀 [에러 해결 핵심] 빈 문자열, None, 포맷팅된 문자열 등을 모두 안전하게 정수로 변환하는 절대 방어막
                    try:
                        if returned_budget is None or pd.isna(returned_budget) or str(returned_budget).strip() == "":
                            safe_budget = 0
                        else:
                            # 콤마, 원, ₩ 등 불필요한 문자 제거 후 float를 거쳐 int로 변환 (완벽 방어)
                            clean_val = str(returned_budget).replace(',', '').replace('원', '').replace('₩', '').strip()
                            safe_budget = int(float(clean_val))
                    except (ValueError, TypeError):
                        safe_budget = -1 # 변환 완전 실패 시 무시하도록 음수 처리

                    if safe_budget >= 0:
                        update_monthly_budget(engine, int(cid), safe_budget)
                        has_updates = True

            if has_updates:
                st.toast("✅ 예산이 성공적으로 업데이트되었습니다!", icon="💾")
                time.sleep(0.5)
                st.rerun()
