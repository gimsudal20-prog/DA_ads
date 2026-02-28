# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view."""

from __future__ import annotations
import re
import os
import time
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date, timedelta, datetime

from data import *
from ui import *
from page_helpers import *

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 및 목표 KPI 관리")
    
    # ✨ [핵심 수정] 예측을 지우고 "실제 꺼짐 기록부(History)" 탭으로 바꿨습니다!
    tab_budget, tab_alert, tab_history = st.tabs(["💰 월 예산 및 집행 현황", "🚨 잔액 소진(계정) 예측", "📅 일자별 캠페인 꺼짐(소진) 기록"])
    
    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    
    with tab_budget:
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
        
        target_roas = st.slider("🎯 전사 목표 ROAS (%) 설정", min_value=100, max_value=1000, value=300, step=50, help="이 목표치에 따라 아래 표의 기상도가 동적으로 변합니다.")
        
        def get_weather(roas, target):
            if roas >= target: return "☀️ 맑음"
            elif roas >= target * 0.8: return "☁️ 흐림" 
            else: return "☔ 비상"
            
        biz_view["ROAS 기상도"] = biz_view["current_roas"].apply(lambda x: get_weather(x, target_roas))
        biz_view["당월 ROAS"] = biz_view["current_roas"].apply(format_roas)
        biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
        biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)

        total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"].astype(str).str.replace(r'[^\d]', '', regex=True), errors="coerce").fillna(0).sum())
        total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
        count_rain = int(biz_view["ROAS 기상도"].astype(str).str.contains("비상").sum())

        c1, c2, c3 = st.columns(3)
        with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 합산', key='m_total_balance')
        with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
        with c3: ui_metric_or_stmetric('효율 ☔ 비상 계정', f"{count_rain}건", f'목표 ROAS {target_roas}% 미달', key='m_need_opt')

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
            st.caption("원하는 단위를 클릭하거나 직접 금액을 입력하세요.")
            opts = budget_view_disp[["customer_id", "account_name"]].copy()
            opts["label"] = opts["account_name"].astype(str) + " (" + opts["customer_id"].astype(str) + ")"
            labels = opts["label"].tolist()
            label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

            sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
            if labels:
                cid = int(label_to_cid.get(sel, 0))
                sk = f"budget_input_{cid}"
                
                if sk not in st.session_state:
                    cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == cid, "monthly_budget_val"].iloc[0])
                    st.session_state[sk] = f"{cur_budget:,}" if cur_budget > 0 else "0"
                
                def format_budget_on_change(key_name):
                    val = st.session_state.get(key_name, "0")
                    cleaned = re.sub(r"[^\d]", "", str(val))
                    if cleaned: st.session_state[key_name] = f"{int(cleaned):,}"
                    else: st.session_state[key_name] = "0"
                
                def add_amount_callback(key_name, amount):
                    val = st.session_state.get(key_name, "0")
                    cleaned = int(re.sub(r"[^\d]", "", str(val)) or 0)
                    st.session_state[key_name] = f"{cleaned + amount:,}"

                def reset_amount_callback(key_name):
                    st.session_state[key_name] = "0"

                st.text_input("새 월 예산 (원)", key=sk, on_change=format_budget_on_change, args=(sk,))
                raw_val = int(re.sub(r"[^\d]", "", str(st.session_state.get(sk, "0"))) or 0)
                
                b1, b2, b3, b4 = st.columns(4)
                b1.button("+10만", key=f"btn_10_{cid}", on_click=add_amount_callback, args=(sk, 100000), use_container_width=True)
                b2.button("+100만", key=f"btn_100_{cid}", on_click=add_amount_callback, args=(sk, 1000000), use_container_width=True)
                b3.button("+1000만", key=f"btn_1000_{cid}", on_click=add_amount_callback, args=(sk, 10000000), use_container_width=True)
                b4.button("초기화", key=f"btn_0_{cid}", on_click=reset_amount_callback, args=(sk,), use_container_width=True)
                    
                if st.button("💾 예산 저장", type="primary", use_container_width=True):
                    update_monthly_budget(engine, cid, raw_val)
                    st.success("✅ 예산이 안전하게 저장되었습니다!")
                    if sk in st.session_state: del st.session_state[sk]
                    st.cache_data.clear()
                    time.sleep(0.5)
                    st.rerun()

    with tab_alert:
        if bundle is None or bundle.empty: return
        
        def get_depletion_date(days_left):
            if pd.isna(days_left) or float(days_left) >= 99:
                return "🟢 여유 (한 달 이상)"
            days = float(days_left)
            if days <= 0: return "🔴 즉시 충전 필요"
                
            deplete_date = date.today() + timedelta(days=int(days))
            date_str = deplete_date.strftime("%m월 %d일")
            
            if days <= 3: return f"🚨 {date_str} (위험)"
            elif days <= 7: return f"🟡 {date_str} (주의)"
            else: return f"🟢 {date_str}"

        biz_view["예상 광고중단일"] = biz_view["days_cover"].apply(get_depletion_date)
        
        st.markdown("<br>", unsafe_allow_html=True)
        display_df = biz_view[["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "예상 광고중단일"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
        display_df = display_df.sort_values(by="예상 광고중단일", ascending=False)
        render_big_table(display_df, key="budget_alert_table", height=500)

    # ✨ [핵심 기능] DB에 영구 기록된 일자별 캠페인 꺼짐(EXHAUSTED) 히스토리 뷰
    with tab_history:
        st.markdown("### 📅 캠페인 일자별 꺼짐(소진) 시간 기록부")
        st.caption("조회 기간 동안 각 캠페인이 정확히 몇 시 몇 분에 예산이 소진되어 노출이 중단되었는지 보여주는 달력입니다. (빈칸은 하루 종일 꺼지지 않았음을 의미합니다)")
        
        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty:
            st.success("🎉 조회 기간 동안 예산 부족으로 꺼진 캠페인 기록이 전혀 없습니다! 완벽한 예산 관리가 이루어지고 있습니다.")
        else:
            # 캠페인 이름 가져오기
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty:
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                off_log["campaign_id"] = off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
            else:
                off_log["campaign_name"] = off_log["campaign_id"]
                
            # 업체명 가져오기
            if not meta.empty:
                meta_copy = meta.copy()
                meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
                off_log["customer_id"] = off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            else:
                off_log["account_name"] = off_log["customer_id"]
            
            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            
            # 피벗 테이블 생성 (가로: 날짜, 세로: 업체+캠페인, 값: 꺼진 시간)
            pivot_df = off_log.pivot_table(
                index=["account_name", "campaign_name"], 
                columns="dt_str", 
                values="off_time", 
                aggfunc='first' # 꺼진 시간 텍스트 그대로 가져오기
            ).reset_index()
            
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명"})
            pivot_df = pivot_df.fillna("-") # 안 꺼진 날은 하이픈 처리
            
            # 스타일링: 시간이 적혀있는(꺼진) 날짜 셀에 붉은색 포인트 주기
            def highlight_off_time(val):
                if val != "-":
                    return "background-color: #FEE2E2; color: #B91C1C; font-weight: bold;"
                return ""
            
            cols_to_style = [c for c in pivot_df.columns if c not in ["업체명", "캠페인명"]]
            styled_pivot = pivot_df.style.map(highlight_off_time, subset=cols_to_style)
            
            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(styled_pivot, use_container_width=True, hide_index=True)
