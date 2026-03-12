# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view."""

from __future__ import annotations
import re
import time
import pandas as pd
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *

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
                opts["customer_id"] = opts["customer_id"].astype(str)
                opts["label"] = opts["account_name"].astype(str) + " (" + opts["customer_id"].astype(str) + ")"
                labels = opts["label"].tolist()
                label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

                if not labels:
                    st.info("설정 가능한 업체가 없습니다.")
                else:
                    sel = st.selectbox("업체 선택", labels, index=0)
                    cid = str(label_to_cid.get(sel, ""))
                    
                    selected_budget = budget_view_disp.loc[budget_view_disp["customer_id"].astype(str) == cid, "monthly_budget_val"]
                    cur_budget = int(selected_budget.iloc[0]) if not selected_budget.empty else 0
                    
                    # ✨ [핵심 수정] 실시간 콤마 반영 및 버튼 즉시 작동을 위한 HTML/JS 폼 도입
                    html_code = f"""
                    <div style="font-family: sans-serif; background: #F8FAFC; padding: 16px; border-radius: 8px; border: 1px solid #E2E8F0;">
                        <label style="font-size: 13px; font-weight: 600; color: #444; margin-bottom: 6px; display: block;">새 예산 (원)</label>
                        <input type="text" id="budgetInput" value="{cur_budget:,}" 
                               style="width: 100%; padding: 8px 12px; font-size: 16px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; text-align: right; font-weight: bold; color: #111;">
                        
                        <div style="display: flex; gap: 4px; margin-top: 10px; margin-bottom: 16px;">
                            <button onclick="addAmount(100000)" style="flex:1; padding:6px 0; font-size:12px; background:#fff; border:1px solid #ccc; border-radius:4px; cursor:pointer;">+10만</button>
                            <button onclick="addAmount(1000000)" style="flex:1; padding:6px 0; font-size:12px; background:#fff; border:1px solid #ccc; border-radius:4px; cursor:pointer;">+100만</button>
                            <button onclick="addAmount(10000000)" style="flex:1; padding:6px 0; font-size:12px; background:#fff; border:1px solid #ccc; border-radius:4px; cursor:pointer;">+1000만</button>
                            <button onclick="resetAmount()" style="flex:1; padding:6px 0; font-size:12px; background:#f1f3f5; border:1px solid #ccc; border-radius:4px; cursor:pointer; color:#555;">초기화</button>
                        </div>
                        
                        <button onclick="submitBudget()" style="width: 100%; padding: 10px; font-size: 15px; font-weight: bold; background: #4876EF; color: white; border: none; border-radius: 6px; cursor: pointer;">저장하기</button>
                    </div>

                    <script>
                        const input = document.getElementById('budgetInput');
                        
                        function formatNumber(num) {{
                            return num.toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ",");
                        }}
                        
                        function unformatNumber(str) {{
                            return parseInt(str.replace(/[^\\d]/g, '') || 0);
                        }}

                        input.addEventListener('input', function(e) {{
                            let val = unformatNumber(e.target.value);
                            e.target.value = val === 0 ? "" : formatNumber(val);
                        }});
                        
                        input.addEventListener('blur', function(e) {{
                            if(e.target.value === "") e.target.value = "0";
                        }});

                        function addAmount(amount) {{
                            let current = unformatNumber(input.value);
                            input.value = formatNumber(current + amount);
                        }}

                        function resetAmount() {{
                            input.value = "0";
                        }}

                        function submitBudget() {{
                            let finalAmount = unformatNumber(input.value);
                            // Streamlit에 데이터 전달
                            window.parent.postMessage({{
                                type: 'streamlit:setComponentValue',
                                value: finalAmount
                            }}, '*');
                        }}
                    </script>
                    """
                    
                    # 폼을 그리고 입력된 결과값 받기
                    returned_budget = components.html(html_code, height=220)
                    
                    if returned_budget is not None and int(returned_budget) >= 0:
                        # 저장 실행
                        update_monthly_budget(engine, cid, int(returned_budget))
                        st.success("✅ 예산이 즉시 저장되었습니다!")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()

    with tab_alert:
        if alert_view.empty:
            st.info("비즈머니 관리 데이터가 없습니다.")
        else:
            def get_depletion_date(days_left):
                if pd.isna(days_left) or float(days_left) >= 99: return "여유"
                days = float(days_left)
                if days <= 0: return "🚨 즉시 충전"
                deplete_date = date.today() + timedelta(days=int(days))
                return deplete_date.strftime("⚠️ %m월 %d일") if days <= 3 else deplete_date.strftime("%m월 %d일")

            alert_view["예상 중단일"] = alert_view["days_cover"].apply(get_depletion_date)
            
            display_df = alert_view[["account_name", "manager", "bizmoney_balance", "avg_cost", "예상 중단일"]].copy()
            display_df["비즈머니 잔액"] = display_df["bizmoney_balance"].apply(lambda x: format_currency(x))
            
            avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
            display_df[avg_days_label] = display_df["avg_cost"].apply(lambda x: format_currency(x))
            
            display_df = display_df[["account_name", "manager", "비즈머니 잔액", avg_days_label, "예상 중단일"]].rename(columns={"account_name": "업체명", "manager": "담당자"})
            
            def color_alert(val):
                if isinstance(val, str) and '🚨' in val:
                    return 'color: white; font-weight: 800; background-color: #EF4444;' 
                elif isinstance(val, str) and '⚠️' in val:
                    return 'color: #9A3412; font-weight: 700; background-color: #FFEDD5;' 
                return ''

            try:
                styled_df = display_df.style.map(color_alert, subset=['예상 중단일'])
            except AttributeError:
                styled_df = display_df.style.applymap(color_alert, subset=['예상 중단일'])
            
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>비즈머니 잔액 관리 계정</div>", unsafe_allow_html=True)
            st.dataframe(styled_df, use_container_width=True, hide_index=True, height=500)
