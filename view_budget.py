# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view."""

from __future__ import annotations
import re
import time
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)
    
    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    
    biz_view = pd.DataFrame()
    
    if bundle is not None and not bundle.empty:
        biz_view = bundle.copy()
        m = biz_view["avg_cost"].astype(float) > 0
        biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
        biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
        biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

        # ✨ [NEW] 예산관리 위험 지수 (Risk Index) 계산 및 대시보드 최상단 패널 노출
        total_accounts = len(biz_view)
        
        # 위험 조건 1: 이번 달 예산 집행률 90% 이상 (예산이 0보다 큰 경우에만)
        budget_risk_mask = (biz_view["monthly_budget"].astype(float) > 0) & (biz_view["current_month_cost"].astype(float) / biz_view["monthly_budget"].astype(float) >= 0.9)
        # 위험 조건 2: 비즈머니 잔액으로 버틸 수 있는 예상일이 3일 이하
        balance_risk_mask = biz_view["days_cover"].fillna(999).astype(float) <= 3.0
        
        risk_accounts = biz_view[budget_risk_mask | balance_risk_mask]
        risk_count = len(risk_accounts)
        
        # 지수 산출 (위험 계정 비율)
        risk_index = int((risk_count / total_accounts) * 100) if total_accounts > 0 else 0
        
        # 점수에 따른 상태 색상 및 텍스트 매핑
        if risk_index >= 50: 
            risk_color = "#FC503D" # Red
            risk_status = "위험 (계정 모니터링 시급)"
        elif risk_index >= 20: 
            risk_color = "#FF9F0A" # Orange
            risk_status = "주의 (잔액 및 예산 확인 필요)"
        else: 
            risk_color = "#32D74B" # Green
            risk_status = "안전 (안정적인 소진 상태)"
            
        st.markdown(f"""
        <div style='padding: 20px; border-radius: 12px; background-color: rgba(55, 95, 255, 0.04); border: 1px solid rgba(55, 95, 255, 0.15); margin-bottom: 24px;'>
            <div style='font-size: 14px; color: #2b52ff; font-weight: 800; margin-bottom: 12px;'>🚨 전체 예산관리 위험 지수</div>
            <div style='display: flex; align-items: center; gap: 24px;'>
                <div style='font-size: 38px; font-weight: 900; color: {risk_color};'>{risk_index}점</div>
                <div>
                    <div style='font-size: 16px; font-weight: 700; color: #333; margin-bottom: 4px;'>현재 상태: <span style='color: {risk_color};'>{risk_status}</span></div>
                    <div style='font-size: 13px; color: #666;'>위험 계정 <span style='font-weight:700; color:#333;'>{risk_count}개</span> / 전체 <span style='font-weight:700; color:#333;'>{total_accounts}개</span></div>
                </div>
            </div>
            <div style='margin-top: 12px; font-size: 12px; color: #888;'>* <b>예산관리 위험 지수</b>: 전체 계정 중 '예산 집행률 90% 초과' 또는 '비즈머니 3일 이내 고갈'이 예상되는 위험 계정의 비율(%)입니다.</div>
        </div>
        """, unsafe_allow_html=True)

    tab_budget, tab_alert, tab_history = st.tabs(["월 예산 현황", "소진 예상일", "꺼짐 기록"])
    
    with tab_budget:
        if biz_view.empty: return

        biz_view["current_roas"] = np.where(biz_view["current_month_cost"] > 0, (biz_view["current_month_sales"] / biz_view["current_month_cost"]) * 100, 0)
        
        target_roas = st.slider("전사 목표 ROAS (%)", min_value=100, max_value=1000, value=300, step=50)
        
        def get_weather(roas, target):
            if roas >= target: return "☀️ 맑음"
            elif roas >= target * 0.8: return "☁️ 흐림" 
            else: return "☔ 비상"
            
        biz_view["ROAS 기상도"] = biz_view["current_roas"].apply(lambda x: get_weather(x, target_roas))
        
        total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"].astype(str).str.replace(r'[^\d]', '', regex=True), errors="coerce").fillna(0).sum())
        total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
        count_rain = int(biz_view["ROAS 기상도"].astype(str).str.contains("비상").sum())

        c1, c2, c3 = st.columns(3)
        with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), key='m_total_balance')
        with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), key='m_month_cost')
        with c3: ui_metric_or_stmetric('효율 비상 계정', f"{count_rain}건", key='m_need_opt')

        st.divider()

        budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
        budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
        budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

        budget_view["usage_rate"] = 0.0
        m2 = budget_view["monthly_budget_val"] > 0
        budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
        budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

        def _status(rate: float, budget: int):
            if budget == 0: return ("미설정", 3)
            if rate >= 1.0: return ("초과", 0)
            if rate >= 0.9: return ("주의", 1)
            return ("적정", 2)

        tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
        budget_view["상태"] = tmp[0]
        budget_view["_rank"] = tmp[1].astype(int)

        budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

        budget_view_disp = budget_view.copy()
        budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
        budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
        budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].map(lambda x: round(float(x), 1) if pd.notna(x) else 0.0)

        disp_cols = ["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]
        table_df = budget_view_disp[disp_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

        c_table, c_form = st.columns([3, 1.2]) 
        with c_table:
            st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:12px;'>{end_dt.strftime('%Y년 %m월')} 예산 집행률</div>", unsafe_allow_html=True)
            render_budget_month_table_with_bars(table_df, key="budget_month_table", height=520)

        with c_form:
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px;'>월 예산 설정</div>", unsafe_allow_html=True)
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

                st.text_input("새 예산 (원)", key=sk, on_change=format_budget_on_change, args=(sk,))
                raw_val = int(re.sub(r"[^\d]", "", str(st.session_state.get(sk, "0"))) or 0)
                
                b1, b2, b3, b4 = st.columns(4)
                b1.button("+10만", key=f"btn_10_{cid}", on_click=add_amount_callback, args=(sk, 100000))
                b2.button("+100만", key=f"btn_100_{cid}", on_click=add_amount_callback, args=(sk, 1000000))
                b3.button("+1000만", key=f"btn_1000_{cid}", on_click=add_amount_callback, args=(sk, 10000000))
                b4.button("초기화", key=f"btn_0_{cid}", on_click=reset_amount_callback, args=(sk,))
                
                st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
                if st.button("저장하기", type="primary", use_container_width=True):
                    update_monthly_budget(engine, cid, raw_val)
                    st.success("저장 완료!")
                    if sk in st.session_state: del st.session_state[sk]
                    st.cache_data.clear()
                    time.sleep(0.5)
                    st.rerun()

    with tab_alert:
        if biz_view.empty: return
        
        def get_depletion_date(days_left):
            if pd.isna(days_left) or float(days_left) >= 99: return "여유"
            days = float(days_left)
            if days <= 0: return "즉시 충전"
            deplete_date = date.today() + timedelta(days=int(days))
            return deplete_date.strftime("%m월 %d일")

        biz_view["예상 중단일"] = biz_view["days_cover"].apply(get_depletion_date)
        
        display_df = biz_view[["account_name", "manager", "bizmoney_balance", "avg_cost", "예상 중단일"]].copy()
        display_df["비즈머니 잔액"] = display_df["bizmoney_balance"].apply(lambda x: format_currency(x))
        display_df["최근 평균소진"] = display_df["avg_cost"].apply(lambda x: format_currency(x))
        
        display_df = display_df[["account_name", "manager", "비즈머니 잔액", "최근 평균소진", "예상 중단일"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
        
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>잔액 소진 예상 계정</div>", unsafe_allow_html=True)
        render_big_table(display_df, key="budget_alert_table", height=500)

    with tab_history:
        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty:
            st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        else:
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty:
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                off_log["campaign_id"] = off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
            else:
                off_log["campaign_name"] = off_log["campaign_id"]
                
            if not meta.empty:
                meta_copy = meta.copy()
                meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
                off_log["customer_id"] = off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            else:
                off_log["account_name"] = off_log["customer_id"]
            
            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            
            pivot_df = off_log.pivot_table(
                index=["account_name", "campaign_name"], 
                columns="dt_str", 
                values="off_time", 
                aggfunc='first'
            ).reset_index()
            
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명"}).fillna("-")
            
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>일자별 꺼짐 기록</div>", unsafe_allow_html=True)
            st.dataframe(pivot_df, use_container_width=True, hide_index=True)
