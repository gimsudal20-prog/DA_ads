# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view."""

from __future__ import annotations
import re
import os
import time
import hmac
import hashlib
import base64
import requests
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
    
    # ✨ [핵심 기능] 캠페인 실시간 예측 탭 추가
    tab_budget, tab_alert, tab_realtime = st.tabs(["💰 월 예산 및 집행 현황", "🚨 잔액 소진(계정) 예측", "⏱️ 실시간 캠페인 꺼짐 시간 예측"])
    
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

    # ✨ [신규 기능] 네이버 실시간 통신을 통한 캠페인 꺼짐 분 단위 예측
    with tab_realtime:
        st.markdown("### ⏱️ 실시간 캠페인 예산 소진(꺼짐) 시간 예측")
        st.caption("버튼을 누르는 즉시 네이버 서버와 통신하여, 캠페인의 실시간 지출 속도를 계산하고 **정확히 오늘 몇 시 몇 분에 예산이 모두 소진될지** 예측합니다. 예산 증액 타이밍을 잡을 때 활용하세요.")
        
        if st.button("🔄 실시간 캠페인 소진 시간 체크 (API 통신)", type="primary"):
            api_key = os.getenv("NAVER_API_KEY")
            secret = os.getenv("NAVER_API_SECRET")
            
            if not api_key or not secret:
                st.error("API 연동 키(.env)가 설정되어 있지 않아 실시간 통신이 불가능합니다.")
            elif not cids:
                st.warning("선택된 계정이 없습니다. 왼쪽 필터에서 계정을 선택해주세요.")
            else:
                with st.spinner("🚀 네이버 실시간 서버에서 예산 데이터를 분석 중입니다..."):
                    results = []
                    now = datetime.now()
                    today_str = now.strftime("%Y-%m-%d")
                    # 오늘 자정부터 지금까지 몇 시간이 지났는지 소수점 계산
                    hours_passed = now.hour + (now.minute / 60.0)
                    if hours_passed < 0.1: hours_passed = 0.1 

                    for cid in cids:
                        ts = str(int(time.time() * 1000))
                        msg = f"{ts}.GET./ncc/campaigns".encode("utf-8")
                        sig = base64.b64encode(hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()).decode("utf-8")
                        headers = {"X-Timestamp": ts, "X-API-KEY": api_key, "X-Customer": str(cid), "X-Signature": sig}
                        
                        try:
                            # 캠페인 하루 예산 가져오기
                            res_camp = requests.get("https://api.searchad.naver.com/ncc/campaigns", headers=headers, timeout=5)
                            if res_camp.status_code != 200: continue
                            
                            camps = res_camp.json()
                            target_camps = []
                            for c in camps:
                                db_obj = c.get("dailyBudget", {})
                                budget = int(db_obj.get("amount", db_obj.get("budgetAmount", 0))) if isinstance(db_obj, dict) else int(db_obj) if str(db_obj).isdigit() else 0
                                if budget > 0:
                                    target_camps.append((c, budget))
                            
                            if not target_camps: continue
                            camp_ids = [str(c[0]["nccCampaignId"]) for c in target_camps]
                            
                            # 오늘 현재까지 쓴 비용(cost) 실시간 조회
                            stat_map = {}
                            for i in range(0, len(camp_ids), 50):
                                chunk = camp_ids[i:i+50]
                                ts2 = str(int(time.time() * 1000))
                                msg_stat = f"{ts2}.GET./stats".encode("utf-8")
                                sig_stat = base64.b64encode(hmac.new(secret.encode("utf-8"), msg_stat, hashlib.sha256).digest()).decode("utf-8")
                                headers["X-Timestamp"] = ts2
                                headers["X-Signature"] = sig_stat
                                
                                params = {"ids": ",".join(chunk), "fields": '["salesAmt"]', "timeRange": f'{{"since":"{today_str}","until":"{today_str}"}}'}
                                res_stat = requests.get("https://api.searchad.naver.com/stats", headers=headers, params=params, timeout=5)
                                if res_stat.status_code == 200:
                                    for s in res_stat.json().get("data", []):
                                        # 부가세 포함 1.1배
                                        stat_map[str(s["id"])] = int(round(float(s.get("salesAmt", 0)) * 1.1))
                                        
                            # 소진 속도 계산 및 예측
                            for c, budget in target_camps:
                                camp_id = str(c["nccCampaignId"])
                                cost = stat_map.get(camp_id, 0)
                                status = c.get("status", "")
                                status_reason = c.get("statusReason", "")
                                
                                burn_rate = cost / hours_passed
                                
                                if "EXHAUSTED" in status or "LIMIT" in status_reason or cost >= budget:
                                    state = "🔴 예산 초과 (현재 꺼짐)"
                                    est_time = "이미 소진됨"
                                else:
                                    if burn_rate > 0:
                                        hours_to_deplete = budget / burn_rate
                                        if hours_to_deplete <= 24:
                                            est_dt = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=hours_to_deplete)
                                            est_time = est_dt.strftime("오늘 %H시 %M분 경")
                                            if est_dt < now: est_time = "곧 소진 예정"
                                            state = "🟡 소진 진행중"
                                        else:
                                            est_time = "오늘 자정까지 여유"
                                            state = "🟢 여유 (느린 속도)"
                                    else:
                                        est_time = "소진 없음"
                                        state = "🟢 여유"
                                        
                                # 어떤 업체인지 메타 데이터에서 찾기
                                acc_name = str(cid)
                                if not meta.empty and 'customer_id' in meta.columns:
                                    match = meta[meta['customer_id'] == cid]
                                    if not match.empty:
                                        acc_name = match.iloc[0]['account_name']

                                results.append({
                                    "업체명": acc_name,
                                    "캠페인명": c.get("name", ""),
                                    "상태": state,
                                    "예상 꺼짐 시간": est_time,
                                    "하루 예산": budget,
                                    "현재 소진액": cost,
                                })
                                
                        except Exception as e:
                            continue
                    
                    if results:
                        df_res = pd.DataFrame(results)
                        df_res = df_res.sort_values(by=["상태", "업체명"], ascending=[True, True])
                        df_res["하루 예산"] = df_res["하루 예산"].apply(format_currency)
                        df_res["현재 소진액"] = df_res["현재 소진액"].apply(format_currency)
                        
                        st.success("✅ 실시간 예측 분석이 완료되었습니다!")
                        render_big_table(df_res, "realtime_camp_predict", 500)
                    else:
                        st.info("예산이 설정된 활성 캠페인이 없거나 통신에 실패했습니다.")
