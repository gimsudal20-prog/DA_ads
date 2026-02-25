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

# Shared logic & queries
from data import * # noqa
# NOTE: Public aliases live in data.py, so import * is safe (period_compare_range / pct_to_arrow).
from data import period_compare_range, pct_to_arrow  # noqa: F401
from ui import * # noqa

# -----------------------------
# Build / Thresholds (Budget)
# -----------------------------
BUILD_TAG = os.getenv("APP_BUILD", "v8.6.13 (NameError Hotfix)")

TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    """필터(담당자/계정) 선택값을 customer_id 리스트로 변환"""
    if meta is None or meta.empty:
        return []
    if (not manager_sel) and (not account_sel):
        return []

    df = meta.copy()

    if manager_sel and "manager" in df.columns:
        sel = [str(x).strip() for x in manager_sel if str(x).strip()]
        if sel:
            df = df[df["manager"].astype(str).str.strip().isin(sel)]

    if account_sel and "account_name" in df.columns:
        sel = [str(x).strip() for x in account_sel if str(x).strip()]
        if sel:
            df = df[df["account_name"].astype(str).str.strip().isin(sel)]

    if "customer_id" not in df.columns:
        return []

    s = pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64")
    return sorted(s.drop_duplicates().tolist())


def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    """Streamlit multiselect with Korean placeholder (compatible across Streamlit versions)."""
    try:
        return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception:
        # Older Streamlit without placeholder=
        return col.multiselect(label, options, default=default, key=key)


def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    """Naver-like '검색조건' panel. No '적용' 버튼: 변경 즉시 반영되지만,
    쿼리는 cache_data로 막아서 체감 속도를 확보합니다.
    """
    today = date.today()
    default_end = today - timedelta(days=1)  # 기본: 어제
    default_start = default_end

    # persist defaults
    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "",
            "manager": [],
            "account": [],
            "type_sel": [],
            "period_mode": "어제",
            "d1": default_start,
            "d2": default_end,
            "top_n_keyword": 300,
            "top_n_ad": 200,
            "top_n_campaign": 200,
            "prefetch_warm": True,
        }

    sv = st.session_state["filters_v8"]

    # Options from meta
    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    # --- 검색조건 패널 (네이버 느낌) ---

    with st.expander("검색조건", expanded=True):

        r1 = st.columns([1.1, 1.2, 1.2, 2.2], gap="small")

        period_mode = r1[0].selectbox(
            "기간",
            ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"],
            index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")),
            key="f_period_mode",
        )

        if period_mode == "직접 선택":
            d1 = r1[1].date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = r1[2].date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            # compute dates from mode (no extra widgets)
            if period_mode == "오늘":
                d2 = today
                d1 = today
            elif period_mode == "어제":
                d2 = today - timedelta(days=1)
                d1 = d2
            elif period_mode == "최근 7일":
                d2 = today - timedelta(days=1)
                d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달":
                d2 = today
                d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달":
                first_this = date(today.year, today.month, 1)
                d2 = first_this - timedelta(days=1)
                d1 = date(d2.year, d2.month, 1)
            else:
                d2 = sv.get("d2", default_end)
                d1 = sv.get("d1", default_start)

            # show read-only dates (consistent height, no '튀어나옴')
            r1[1].text_input("시작일", str(d1), disabled=True, key="f_d1_ro")
            r1[2].text_input("종료일", str(d2), disabled=True, key="f_d2_ro")


        q = r1[3].text_input("검색", sv.get("q", ""), key="f_q", placeholder="계정/키워드/소재 검색")


        r2 = st.columns([1.2, 1.6, 1.2], gap="small")

        manager_sel = ui_multiselect(r2[0], "담당자", managers, default=sv.get("manager", []), key="f_manager")

        # ✅ 담당자 선택 시: 해당 담당자 계정만 노출 (네이버 관리자 UX)
        accounts_by_mgr = accounts
        if manager_sel:
            try:
                dfm = meta.copy()
                # normalize (공백/개행) - 담당자/업체 필터 정확도 향상
                dfm['manager'] = dfm.get('manager','').astype(str).fillna('').str.strip()
                dfm['account_name'] = dfm.get('account_name','').astype(str).fillna('').str.strip()
                if "manager" in dfm.columns and "account_name" in dfm.columns:
                    dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                    accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
            except Exception:
                accounts_by_mgr = accounts

        # 기존 선택값 중 유효한 것만 유지
        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]

        account_sel = ui_multiselect(r2[1], "계정", accounts_by_mgr, default=prev_acc, key="f_account")

        type_sel = ui_multiselect(r2[2], "캠페인 유형", type_opts, default=sv.get("type_sel", []), key="f_type_sel")


    # persist back
    sv.update(
        {
            "q": q or "",
            "manager": manager_sel or [],
            "account": account_sel or [],
            "type_sel": type_sel or [],
            "period_mode": period_mode,
            "d1": d1,
            "d2": d2,
        }
    )
    st.session_state["filters_v8"] = sv

    # Customer ids resolve
    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    # Return the same shape other pages expect
    f = {
        "q": sv["q"],
        "manager": sv["manager"],
        "account": sv["account"],
        "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1,
        "end": d2,
        "period_mode": period_mode,
        "customer_ids": cids,
        "selected_customer_ids": cids,  # alias for older pages (campaign/budget)
        "top_n_keyword": int(sv.get("top_n_keyword", 300)),
        "top_n_ad": int(sv.get("top_n_ad", 200)),
        "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "prefetch_warm": bool(sv.get("prefetch_warm", True)),
        "ready": True,
    }
    return f


def _render_empty_state_no_data(key: str = "empty") -> None:
    st.markdown("### 🫥 데이터가 없습니다")
    st.caption("오늘 데이터는 수집 지연이 있을 수 있어요. 아래 버튼으로 기간을 **최근 7일(오늘 제외)**로 바꿔 다시 조회해보세요.")
    c1, c2 = st.columns([1, 3])
    if c1.button("📅 최근 7일로", key=f"{key}_set7", type="primary"):
        try:
            if "filters_v8" in st.session_state and isinstance(st.session_state["filters_v8"], dict):
                st.session_state["filters_v8"]["period_mode"] = "최근 7일"
            st.cache_data.clear()
        except Exception:
            pass
        st.rerun()
    with c2:
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        st.write("• 담당자/계정 필터를 풀어보거나, accounts.xlsx 동기화를 확인해보세요.")

def render_filter_summary_bar(f: Dict, meta: pd.DataFrame) -> None:
    """Compact one-line summary shown on the main area (keeps the UI 'report-like')."""
    try:
        n_total = int(meta["customer_id"].nunique()) if meta is not None and not meta.empty else 0
    except Exception:
        n_total = 0

    sel = f.get("selected_customer_ids", []) or []
    n_sel = len(sel) if sel else n_total
    period = f"{f.get('start')} ~ {f.get('end')}"
    type_sel = list(f.get("type_sel", tuple()) or [])
    type_txt = "전체" if not type_sel else ", ".join(type_sel[:3]) + (" 외" if len(type_sel) > 3 else "")

    st.markdown(
        f"""
        <div class="panel" style="display:flex; align-items:center; justify-content:space-between; gap:12px; padding:12px 14px;">
          <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
            <span class="badge b-blue">선택 계정 {n_sel} / {n_total}</span>
            <span class="badge b-gray">기간 {period}</span>
            <span class="badge b-gray">유형 {type_txt}</span>
          </div>
          <div style="font-size:12px; color: rgba(2,8,23,0.55);">왼쪽 사이드바에서 필터를 바꿀 수 있어요</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    """요약(한눈에): 네이버 리포트 느낌으로 KPI를 먼저 보여주고, 상세는 아래로."""
    if not f:
        st.info("검색조건을 설정하면 요약이 표시됩니다.")
        return

    st.markdown("<div class='nv-sec-title'>요약</div>", unsafe_allow_html=True)
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids = tuple((f.get("selected_customer_ids") or f.get("customer_ids") or []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    cmp_mode = st.radio(
        "비교 기준",
        ["전일대비", "전주대비", "전월대비"],
        horizontal=True,
        index=1,
        key="ov_cmp_mode",
    )

    cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    base = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    def _delta_pct(key: str) -> Optional[float]:
        try:
            return _pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception:
            return None

    def _kpi_html(label: str, value: str, delta_text: str, delta_val: Optional[float]) -> str:
        cls = "neu"
        try:
            if delta_val is None or (isinstance(delta_val, float) and math.isnan(delta_val)):
                cls = "neu"
            elif float(delta_val) > 0:
                cls = "pos"
            elif float(delta_val) < 0:
                cls = "neg"
            else:
                cls = "neu"
        except Exception:
            cls = "neu"

        return f"""<div class='kpi'>
            <div class='k'>{label}</div>
            <div class='v'>{value}</div>
            <div class='d {cls}'>{delta_text}</div>
        </div>"""

    items = [
        ("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost")),
        ("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales")),
        ("전환", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv")),
        ("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas")),
        ("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr")),
        ("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc")),
    ]

    kpi_html = "<div class='kpi-row'>" + "".join(_kpi_html(a, b, c, d) for a, b, c, d in items) + "</div>"
    st.markdown(kpi_html, unsafe_allow_html=True)

    st.divider()

    # 상세(추세/Top) - 오류가 나도 KPI는 유지
    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is None or ts.empty:
            st.info("표시할 데이터가 없습니다.")
            return

        st.markdown("<div class='nv-sec-title'>추세</div>", unsafe_allow_html=True)
        render_timeseries_chart(ts, entity="campaign", key_prefix="ov_ts")
    except Exception:
        st.info("추세 데이터를 불러오는 중 오류가 발생했습니다. (KPI는 정상 표시)")


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)

    # 평균소진(최근 TOPUP_AVG_DAYS일) 계산 구간: (end - 1) 기준으로 과거 TOPUP_AVG_DAYS
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    # 월 누적 구간
    month_d1 = end_dt.replace(day=1)
    if end_dt.month == 12:
        month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_d2 = date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    if bundle is None or bundle.empty:
        st.warning("예산/잔액 데이터를 불러올 수 없습니다. (fact_bizmoney_daily/fact_campaign_daily 확인)")
        return

    biz_view = bundle.copy()
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")

    # days_cover & threshold
    biz_view["days_cover"] = pd.NA
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)

    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    # display columns (small cost)
    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)
    biz_view["전일 소진액"] = biz_view["y_cost"].map(format_currency)

    def _fmt_days(d):
        if pd.isna(d) or d is None:
            return "-"
        try:
            dd = float(d)
        except Exception:
            return "-"
        if dd > 99:
            return "99+일"
        return f"{dd:.1f}일"

    biz_view["D-소진"] = biz_view["days_cover"].map(_fmt_days)
    biz_view["확인일자"] = biz_view["last_update"]

    # summary
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    with c1:
        ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', key='m_total_balance')
    with c2:
        ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3:
        ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', key='m_need_topup')

    st.divider()

    need_topup = count_low_balance
    ok_topup = int(len(biz_view) - need_topup)
    st.markdown(
        f"<span class='badge b-red'>충전필요 {need_topup}건</span>"
        f"<span class='badge b-green'>여유 {ok_topup}건</span>",
        unsafe_allow_html=True,
    )

    show_only_topup = st.checkbox("충전필요만 보기", value=False)

    biz_view["_rank"] = biz_view["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    view_cols = ["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]
    display_df = biz_view[view_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    ui_table_or_dataframe(display_df, key="budget_biz_table", height=520)
    render_download_compact(display_df, f"예산_잔액_{f['start']}_{f['end']}", "budget", "budget")

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")

    # budget status
    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

    def _status(rate: float, budget: int):
        if budget == 0:
            return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0:
            return ("🔴 초과", "초과", 0)
        if rate >= 0.9:
            return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)

    tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
    budget_view["상태"] = tmp[0]
    budget_view["status_text"] = tmp[1]
    budget_view["_rank"] = tmp[2].astype(int)

    cnt_over = int((budget_view["status_text"] == "초과").sum())
    cnt_warn = int((budget_view["status_text"] == "주의").sum())
    cnt_unset = int((budget_view["status_text"] == "미설정").sum())

    st.markdown(
        f"<span class='badge b-red'>초과 {cnt_over}건</span>"
        f"<span class='badge b-yellow'>주의 {cnt_warn}건</span>"
        f"<span class='badge b-gray'>미설정 {cnt_unset}건</span>",
        unsafe_allow_html=True,
    )

    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

    budget_view_disp = budget_view.copy()
    budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
    budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
    budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].map(lambda x: round(float(x), 1) if pd.notna(x) else 0.0)

    disp_cols = ["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]
    table_df = budget_view_disp[disp_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    c1, c2 = st.columns([3, 1])
    with c1:
        render_budget_month_table_with_bars(table_df, key="budget_month_table", height=520)
        render_download_compact(table_df, f"월예산_{f['start']}_{f['end']}", "monthly_budget", "mb")

    with c2:
        st.markdown(
            """
            <div class="panel" style="line-height:1.85; font-size:14px; background: rgba(235,238,242,0.75);">
              <b>상태 가이드</b><br><br>
              🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
              🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
              🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
              ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ✅ 안정적인 폼 기반 업데이트 (data_editor 제거)
    st.markdown("#### ✍️ 월 예산 수정 (선택 → 입력 → 저장)")
    opts = budget_view_disp[["customer_id", "account_name"]].copy()
    opts["label"] = opts["account_name"].astype(str) + "  (" + opts["customer_id"].astype(str) + ")"
    labels = opts["label"].tolist()
    label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

    with st.form("budget_update_form", clear_on_submit=False):
        sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
        cur_budget = 0
        if labels:
            cid = int(label_to_cid.get(sel, 0))
            cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == cid, "monthly_budget_val"].iloc[0])
        new_budget = st.text_input("새 월 예산(원) (예: 500000 또는 500,000)", value=format_number_commas(cur_budget) if labels else "0")
        submitted = st.form_submit_button("💾 저장", use_container_width=True)

    if submitted and labels:
        cid = int(label_to_cid.get(sel, 0))
        nb = parse_currency(new_budget)
        update_monthly_budget(engine, cid, nb)
        st.success("수정 완료. (캐시 갱신)")
        st.cache_data.clear()
        st.rerun()


def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

def _attach_account_name(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    """메타 정보에서 업체명을 가져와 병합하는 헬퍼 함수"""
    if df is None or df.empty or meta is None or meta.empty:
        return df
    out = df.copy()
    if "customer_id" in out.columns and "customer_id" in meta.columns and "account_name" in meta.columns:
        meta_map = meta.set_index("customer_id")["account_name"].to_dict()
        out["account_name"] = out["customer_id"].map(meta_map).fillna("")
    return out


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🚀 성과 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_campaign", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)')
        return

    type_sel = tuple(f.get("type_sel", []) or [])

    # -----------------------------
    # 1) Main list: 캠페인 단위 "번들 집계" (빠르고 DB 부담 적음)
    # -----------------------------
    try:
        bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 200), top_k=10)
    except Exception:
        bundle = pd.DataFrame()

    if bundle is None or bundle.empty:
        _render_empty_state_no_data(key="empty_bundle")
        return

    # 메타(업체명/담당자) 부착
    bundle = bundle.copy()
    bundle["customer_id"] = pd.to_numeric(bundle["customer_id"], errors="coerce").astype("Int64")
    bundle = bundle.dropna(subset=["customer_id"]).copy()
    bundle["customer_id"] = bundle["customer_id"].astype("int64")
    bundle = _attach_account_name(bundle, meta)
    if "manager" in meta.columns:
        try:
            m_map = meta.set_index("customer_id")["manager"].to_dict()
            bundle["manager"] = bundle["customer_id"].map(m_map)
        except Exception:
            bundle["manager"] = ""

    bundle = add_rates(bundle)

    # TOP5
    top_cost = bundle[pd.to_numeric(bundle.get("rn_cost", np.nan), errors="coerce").between(1,5)].sort_values("rn_cost") if "rn_cost" in bundle.columns else bundle.sort_values("cost", ascending=False).head(5)
    top_clk = bundle[pd.to_numeric(bundle.get("rn_clk", np.nan), errors="coerce").between(1,5)].sort_values("rn_clk") if "rn_clk" in bundle.columns else bundle.sort_values("clk", ascending=False).head(5)
    top_conv = bundle[pd.to_numeric(bundle.get("rn_conv", np.nan), errors="coerce").between(1,5)].sort_values("rn_conv") if "rn_conv" in bundle.columns else bundle.sort_values("conv", ascending=False).head(5)

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["업체명", "캠페인", metric])
        x = df.copy()
        if metric == "광고비":
            x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else:
            x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        x = x.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"})
        keep_cols = [c for c in ["업체명", "캠페인", metric] if c in x.columns]
        return x[keep_cols]

    with st.expander("📌 성과별 TOP5 (캠페인)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='camp_top5_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='camp_top5_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='camp_top5_conv', height=240)

    st.divider()

    # -----------------------------
    # 2) Trend / Compare (전체/선택 캠페인)
    #    - 상세 토글 ON일 때만 시계열 쿼리 수행
    # -----------------------------
    st.markdown("### 📈 기간 추세")
    render_period_compare_panel(engine, "campaign", f["start"], f["end"], cids, type_sel, key_prefix="camp", expanded=False)

    show_detail = st.toggle("상세(캠페인 추세/표) 보기", value=False, key="camp_detail_toggle")

    # 캠페인 선택
    multi_acc = bundle["customer_id"].nunique() > 1
    bundle["label"] = bundle.apply(lambda r: f'{r.get("account_name","")} · {r.get("campaign_name","")}' if multi_acc else str(r.get("campaign_name","")), axis=1)
    options = ["(전체 캠페인)"] + bundle["label"].dropna().astype(str).unique().tolist()
    sel = st.selectbox("캠페인 선택", options, index=0, key="camp_select")

    ts = pd.DataFrame()
    if show_detail:
        try:
            if sel == "(전체 캠페인)":
                ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
            else:
                # label -> customer_id/campaign_id 찾기
                row = bundle[bundle["label"] == sel].head(1)
                if not row.empty:
                    cid = int(row.iloc[0]["customer_id"])
                    camp_id = int(row.iloc[0]["campaign_id"])
                    ts = query_campaign_one_timeseries(engine, f["start"], f["end"], cid, camp_id)
        except Exception:
            ts = pd.DataFrame()

    if show_detail and ts is not None and not ts.empty:
        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="camp_trend_metric",
        )
        ts2 = ts.copy()
        # ROAS 계산
        if "sales" in ts2.columns and "cost" in ts2.columns:
            ts2["roas"] = np.where(pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) > 0,
                                   pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) * 100.0,
                                   0.0)
        else:
            ts2["roas"] = 0.0

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None:
                    render_chart(ch)

        if metric_sel == '광고비':
            _render('cost', '광고비(원)')
        elif metric_sel == '클릭':
            _render('clk', '클릭')
        elif metric_sel == '전환':
            _render('conv', '전환')
        else:
            _render('roas', 'ROAS(%)')

    # -----------------------------
    # 3) Main table: 비용 TOP N
    # -----------------------------
    df = bundle.copy()
    if "rn_cost" in df.columns:
        df = df[pd.to_numeric(df["rn_cost"], errors="coerce").between(1, top_n)]
        df = df.sort_values("rn_cost")
    else:
        df = df.sort_values("cost", ascending=False).head(top_n)

    # 출력용(표)
    display_df = df.rename(
        columns={
            "account_name": "업체명",
            "campaign_type": "캠페인유형",
            "campaign_name": "캠페인",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "sales": "매출",
        }
    )
    # 우측 정렬/퍼센트/원 표기 등은 기존 헬퍼가 처리
    display_df = finalize_display_cols(display_df)

    render_big_table(display_df, key="camp_main_grid", height=560)
    render_download_compact(display_df, f"성과_캠페인_TOP{top_n}_{f['start']}_{f['end']}", "campaign", "camp")



def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🔎 성과 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get("manager") or f.get("account")) and not cids:
        st.warning("선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)")
        return

    type_sel = tuple(f.get("type_sel", []) or [])
    top_n = int(f.get("top_n_keyword", 300))

    bundle = query_keyword_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        _render_empty_state_no_data(key="empty_bundle")
        return

    # --- timeseries (for 전체 멀티라인 비교) ---
    try:
        ts_by_type = query_keyword_timeseries_by_type(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts_by_type = pd.DataFrame()

    ts_total = pd.DataFrame()
    if ts_by_type is not None and not ts_by_type.empty and "dt" in ts_by_type.columns:
        tmp = ts_by_type.copy()
        tmp["dt"] = pd.to_datetime(tmp["dt"], errors="coerce")
        sum_cols = [c for c in ["imp", "clk", "cost", "conv", "sales"] if c in tmp.columns]
        ts_total = tmp.groupby("dt", as_index=False)[sum_cols].sum()
    else:
        try:
            ts_total = query_keyword_timeseries(engine, f["start"], f["end"], cids, type_sel)
        except Exception:
            ts_total = pd.DataFrame()

    def _filter_bundle_by_label(df: pd.DataFrame, label: Optional[str]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        if not label:
            return df.copy()
        if "campaign_type_label" not in df.columns:
            return pd.DataFrame()
        return df[df["campaign_type_label"].astype(str).str.strip() == label].copy()

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["업체명", "키워드", metric])

        x = df.copy()
        x["customer_id"] = pd.to_numeric(x["customer_id"], errors="coerce").astype("Int64")
        x = x.dropna(subset=["customer_id"]).copy()
        x["customer_id"] = x["customer_id"].astype("int64")
        x = x.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")

        if metric == "광고비":
            x[metric] = pd.to_numeric(x.get("cost", 0), errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else:
            x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)

        return x.rename(columns={"account_name": "업체명", "keyword": "키워드"})[["업체명", "키워드", metric]]

    def _prepare_main_table(df_in: pd.DataFrame, *, shopping_first: bool) -> pd.DataFrame:
        if df_in is None or df_in.empty:
            return pd.DataFrame()

        df = df_in.copy()
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["customer_id"]).copy()
        df["customer_id"] = df["customer_id"].astype("int64")

        df = add_rates(df)
        df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

        view = df.rename(
            columns={
                "account_name": "업체명",
                "manager": "담당자",
                "campaign_type_label": "캠페인유형",
                "campaign_name": "캠페인",
                "adgroup_name": "광고그룹",
                "keyword": "키워드",
                "imp": "노출",
                "clk": "클릭",
                "ctr": "CTR(%)",
                "cpc": "CPC",
                "cost": "광고비",
                "conv": "전환",
                "cpa": "CPA",
                "sales": "전환매출",
                "roas": "ROAS(%)",
            }
        )

        view["광고비"] = pd.to_numeric(view.get("광고비", 0), errors="coerce").fillna(0).map(format_currency)
        view["CPC"] = pd.to_numeric(view.get("CPC", 0), errors="coerce").fillna(0).map(format_currency)
        view["CPA"] = pd.to_numeric(view.get("CPA", 0), errors="coerce").fillna(0).map(format_currency)
        view["전환매출"] = pd.to_numeric(view.get("전환매출", 0), errors="coerce").fillna(0).map(format_currency)
        view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)

        view["CTR(%)"] = pd.to_numeric(view.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
        view = finalize_ctr_col(view, "CTR(%)")

        # int cols
        for c in ["노출", "클릭", "전환"]:
            if c in view.columns:
                view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0).astype(int)

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드"]
        tail_cols = ["노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]

        if shopping_first:
            # 쇼핑검색은 "전환매출/ROAS"를 앞쪽으로 당겨 맥락화
            cols = base_cols + ["전환매출", "ROAS(%)", "광고비", "전환", "CPA", "클릭", "CTR(%)", "CPC", "노출"]
        else:
            cols = base_cols + tail_cols

        cols = [c for c in cols if c in view.columns]
        return view[cols].copy()

    def _render_multiline(ts_src: pd.DataFrame, ycol: str, yname: str) -> None:
        """전체 탭에서 파워링크 vs 쇼핑검색을 같은 차트에서 비교."""
        if ts_src is None or ts_src.empty:
            return

        d = ts_src.copy()
        if "dt" not in d.columns:
            return
        if "campaign_type_label" not in d.columns:
            # fallback: single line
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line("트렌드", ts_total, "dt", ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts_total, ycol, yname, y_format=",.0f", height=260)
                if ch is not None:
                    render_chart(ch)
            return

        d["dt"] = pd.to_datetime(d["dt"], errors="coerce")
        d[ycol] = pd.to_numeric(d.get(ycol, 0), errors="coerce").fillna(0)

        # only show the two key media types
        d = d[d["campaign_type_label"].isin(["파워링크", "쇼핑검색"])].copy()
        if d.empty:
            # fallback
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line("트렌드", ts_total, "dt", ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts_total, ycol, yname, y_format=",.0f", height=260)
                if ch is not None:
                    render_chart(ch)
            return

        if HAS_ECHARTS and st_echarts is not None:
            # pivot for aligned x-axis
            p = d.pivot_table(index="dt", columns="campaign_type_label", values=ycol, aggfunc="sum").fillna(0)
            x = pd.to_datetime(p.index, errors="coerce").strftime("%m/%d").tolist()
            series = []
            for col in p.columns.tolist():
                y = pd.to_numeric(p[col], errors="coerce").fillna(0).round(0).astype(int).tolist()
                series.append({"name": str(col), "type": "line", "data": y, "smooth": True, "showSymbol": False, "lineStyle": {"width": 3}, "areaStyle": {"opacity": 0.04}})
            option = {
                "title": {"show": False},
                "grid": {"left": 54, "right": 18, "top": 44, "bottom": 34},
                "tooltip": {"trigger": "axis"},
                "legend": {"top": 6},
                "xAxis": {"type": "category", "data": x, "axisTick": {"alignWithLabel": True}},
                "yAxis": {"type": "value", "name": yname, "nameTextStyle": {"padding": [0, 0, 0, 6]}},
                "series": series,
            }
            st_echarts(option, height="260px")
        else:
            # Altair multi-line
            wk = ["월", "화", "수", "목", "금", "토", "일"]
            dd = d[["dt", "campaign_type_label", ycol]].copy()
            dd["dt"] = pd.to_datetime(dd["dt"], errors="coerce")
            dd["_wk"] = dd["dt"].dt.weekday.map(lambda i: wk[int(i)] if pd.notna(i) else "")
            dd["_x_label"] = dd["dt"].dt.strftime("%m/%d") + "(" + dd["_wk"] + ")"
            dd["_dt_str"] = dd["dt"].dt.strftime("%Y-%m-%d") + " (" + dd["_wk"] + ")"
            ch = (
                alt.Chart(dd.dropna(subset=["dt"]))
                .mark_line(point=False)
                .encode(
                    x=alt.X("dt:T", title=None, axis=alt.Axis(format="%m/%d", labelAngle=0)),
                    y=alt.Y(f"{ycol}:Q", title=yname),
                    color=alt.Color("campaign_type_label:N", title=None),
                    tooltip=[
                        alt.Tooltip("_dt_str:N", title="날짜"),
                        alt.Tooltip("campaign_type_label:N", title="매체"),
                        alt.Tooltip(f"{ycol}:Q", title=yname, format=",.0f"),
                    ],
                )
                .properties(height=260)
            )
            render_chart(ch)

    def _render_tab(tab_label: str, df_tab: pd.DataFrame) -> None:
        # TOP10 summary
        st.markdown("### 📌 성과별 TOP10")
        t1, t2, t3 = st.tabs(["💸 광고비 TOP10", "🖱️ 클릭 TOP10", "✅ 전환 TOP10"])
        with t1:
            ui_table_or_dataframe(_fmt_top(df_tab.sort_values("cost", ascending=False).head(10), "광고비"), key=f"kw_top_cost_{tab_label}", height=260)
        with t2:
            ui_table_or_dataframe(_fmt_top(df_tab.sort_values("clk", ascending=False).head(10), "클릭"), key=f"kw_top_clk_{tab_label}", height=260)
        with t3:
            ui_table_or_dataframe(_fmt_top(df_tab.sort_values("conv", ascending=False).head(10), "전환"), key=f"kw_top_conv_{tab_label}", height=260)

        st.divider()

        # Main table
        main_src = df_tab.sort_values("cost", ascending=False).head(top_n).copy()
        shopping_first = (tab_label == "쇼핑검색")
        out_df = _prepare_main_table(main_src, shopping_first=shopping_first)

        if out_df is None or out_df.empty:
            st.info("표시할 데이터가 없습니다.")
            return

        render_big_table(out_df, key=f"kw_big_{tab_label}", height=620)

        filename_suffix = tab_label if tab_label != "전체" else "ALL"
        render_download_compact(out_df, f"키워드성과_{filename_suffix}_TOP{top_n}_{f['start']}_{f['end']}", "keyword", f"kw_{filename_suffix}")

    # -----------------------------
    # TABS: 전체 / 파워링크 / 쇼핑검색
    # -----------------------------
    tab_all, tab_pl, tab_shop = st.tabs(["전체", "파워링크", "쇼핑검색"])

    with tab_all:
        # Trend (multi-line compare)
        if ts_total is not None and not ts_total.empty:
            total_cost = float(pd.to_numeric(ts_total.get("cost", 0), errors="coerce").fillna(0).sum())
            total_clk = float(pd.to_numeric(ts_total.get("clk", 0), errors="coerce").fillna(0).sum())
            total_conv = float(pd.to_numeric(ts_total.get("conv", 0), errors="coerce").fillna(0).sum())
            total_sales = float(pd.to_numeric(ts_total.get("sales", 0), errors="coerce").fillna(0).sum()) if "sales" in ts_total.columns else 0.0
            total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

            st.markdown("### 📈 기간 추세 (파워링크 vs 쇼핑검색 비교)")
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_kw_cost_all")
            with k2:
                ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_kw_clk_all")
            with k3:
                ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_kw_conv_all")
            with k4:
                ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_kw_roas_all")

            render_period_compare_panel(engine, "keyword", f["start"], f["end"], cids, type_sel, key_prefix="kw_all", expanded=False)

            metric_sel = st.radio(
                "트렌드 지표",
                ["광고비", "클릭", "전환", "ROAS"],
                horizontal=True,
                index=0,
                key="kw_trend_metric_all",
            )

            # build y-series
            if metric_sel == "광고비":
                _render_multiline(ts_by_type, "cost", "광고비(원)")
            elif metric_sel == "클릭":
                _render_multiline(ts_by_type, "clk", "클릭")
            elif metric_sel == "전환":
                _render_multiline(ts_by_type, "conv", "전환")
            else:
                if ts_by_type is not None and not ts_by_type.empty:
                    tss = ts_by_type.copy()
                    tss["sales"] = pd.to_numeric(tss.get("sales", 0), errors="coerce").fillna(0)
                    tss["cost"] = pd.to_numeric(tss.get("cost", 0), errors="coerce").fillna(0)
                    tss["roas"] = (tss["sales"] / tss["cost"].replace(0, np.nan)) * 100
                    tss["roas"] = pd.to_numeric(tss["roas"], errors="coerce").fillna(0)
                    _render_multiline(tss, "roas", "ROAS(%)")
                else:
                    # fallback
                    tsx = ts_total.copy()
                    tsx["sales"] = pd.to_numeric(tsx.get("sales", 0), errors="coerce").fillna(0)
                    tsx["cost"] = pd.to_numeric(tsx.get("cost", 0), errors="coerce").fillna(0)
                    tsx["roas"] = (tsx["sales"] / tsx["cost"].replace(0, np.nan)) * 100
                    tsx["roas"] = pd.to_numeric(tsx["roas"], errors="coerce").fillna(0)
                    if HAS_ECHARTS and st_echarts is not None:
                        render_echarts_line("트렌드", tsx, "dt", "roas", "ROAS(%)", height=260)
                    else:
                        ch = _chart_timeseries(tsx, "roas", "ROAS(%)", y_format=",.0f", height=260)
                        if ch is not None:
                            render_chart(ch)

            st.divider()

        _render_tab("전체", bundle)

    with tab_pl:
        df_pl = _filter_bundle_by_label(bundle, "파워링크")
        if df_pl.empty:
            st.info("파워링크 데이터가 없습니다. (필터의 캠페인 유형 선택을 확인해주세요)")
        else:
            _render_tab("파워링크", df_pl)

    with tab_shop:
        df_shop = _filter_bundle_by_label(bundle, "쇼핑검색")
        if df_shop.empty:
            st.info("쇼핑검색 데이터가 없습니다. (필터의 캠페인 유형 선택을 확인해주세요)")
        else:
            _render_tab("쇼핑검색", df_shop)


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return

    st.markdown("## 🧩 성과 (소재)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    # UI toggle (default: ON)
    try:
        exclude_meaningless = st.toggle("✓ 의미 없는 기본/상품 소재 제외하기", value=True, key="ad_exclude_meaningless")
    except Exception:
        exclude_meaningless = st.checkbox("✓ 의미 없는 기본/상품 소재 제외하기", value=True, key="ad_exclude_meaningless_cb")

    top_n = int(f.get("top_n_ad", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get("manager") or f.get("account")) and not cids:
        st.warning("선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다. (accounts.xlsx 동기화/메타 확인 필요)")
        return

    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty:
        _render_empty_state_no_data(key="empty_ad")
        return

    # -----------------------------
    # pandas 전처리: 의미 없는 소재 제외 (토글 ON)
    # -----------------------------
    if exclude_meaningless:
        x = bundle.copy()
        txt = x.get("ad_name", pd.Series([""] * len(x))).fillna("").astype(str).str.strip()
        norm = txt.str.replace(r"\s+", "", regex=True).str.lower()

        banned = {
            "상품소재",
            "상품",
            "이미지",
            "이미지소재",
            "기본",
            "기본소재",
            "소재",
        }
        # remove id-only (fallback)
        if "ad_id" in x.columns:
            adid = x["ad_id"].fillna("").astype(str).str.strip()
            id_only = (txt != "") & (txt == adid)
        else:
            id_only = pd.Series([False] * len(x))

        keep = (txt != "") & (~norm.isin({b.lower() for b in banned})) & (~id_only)
        bundle = x[keep].copy()

    if bundle is None or bundle.empty:
        st.info("필터(의미 없는 소재 제외) 적용 후 표시할 데이터가 없습니다.")
        return

    df = _perf_common_merge_meta(bundle, meta)
    df = add_rates(df)

    # -----------------------------
    # 📈 Trend
    # -----------------------------
    try:
        ts = query_ad_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(pd.to_numeric(ts.get("cost", 0), errors="coerce").fillna(0).sum())
        total_clk = float(pd.to_numeric(ts.get("clk", 0), errors="coerce").fillna(0).sum())
        total_conv = float(pd.to_numeric(ts.get("conv", 0), errors="coerce").fillna(0).sum())
        total_sales = float(pd.to_numeric(ts.get("sales", 0), errors="coerce").fillna(0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_ad_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_ad_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_ad_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_ad_roas")

        render_period_compare_panel(engine, "ad", f["start"], f["end"], cids, type_sel, key_prefix="ad", expanded=False)

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="ad_trend_metric",
        )
        ts2 = ts.copy()

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None:
                render_echarts_line("트렌드", ts2, "dt", ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=",.0f", height=260)
                if ch is not None:
                    render_chart(ch)

        if metric_sel == "광고비":
            _render("cost", "광고비(원)")
        elif metric_sel == "클릭":
            _render("clk", "클릭")
        elif metric_sel == "전환":
            _render("conv", "전환")
        else:
            sales_s = pd.to_numeric(ts2.get("sales", 0), errors="coerce").fillna(0)
            cost_s = pd.to_numeric(ts2.get("cost", 0), errors="coerce").fillna(0)
            ts2["roas"] = (sales_s / cost_s.replace(0, np.nan)) * 100
            ts2["roas"] = pd.to_numeric(ts2["roas"], errors="coerce").fillna(0)
            _render("roas", "ROAS(%)")

        st.divider()

    # -----------------
    # TOP5 (비용/클릭/전환)
    # -----------------
    top_cost = df.sort_values("cost", ascending=False).head(5)
    top_clk = df.sort_values("clk", ascending=False).head(5)
    top_conv = df.sort_values("conv", ascending=False).head(5)

    def _fmt_top(dfx: pd.DataFrame, metric: str) -> pd.DataFrame:
        if dfx is None or dfx.empty:
            return pd.DataFrame(columns=["업체명", "캠페인", "소재내용", metric])
        x = dfx.copy()
        x["업체명"] = x.get("account_name", "")
        x["캠페인"] = x.get("campaign_name", "")
        x["소재내용"] = x.get("ad_name", "")
        if metric == "광고비":
            x[metric] = pd.to_numeric(x.get("cost", 0), errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else:
            x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)
        return x[["업체명", "캠페인", "소재내용", metric]]

    with st.expander("📌 성과별 TOP5 (소재)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key="ad_top5_cost", height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key="ad_top5_clk", height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key="ad_top5_conv", height=240)

    st.divider()

    # -----------------
    # Main table (비용 TOP N)
    # -----------------
    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()

    disp = main_df.copy()
    disp["cost"] = pd.to_numeric(disp.get("cost", 0), errors="coerce").fillna(0).map(format_currency)
    disp["sales"] = pd.to_numeric(disp.get("sales", 0), errors="coerce").fillna(0).map(format_currency)
    disp["cpc"] = pd.to_numeric(disp.get("cpc", 0), errors="coerce").fillna(0).map(format_currency)
    disp["cpa"] = pd.to_numeric(disp.get("cpa", 0), errors="coerce").fillna(0).map(format_currency)
    disp["roas_disp"] = disp.get("roas", 0).map(format_roas)

    disp = disp.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "ad_id": "소재ID",
            "ad_name": "소재내용",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "sales": "전환매출",
            "roas_disp": "ROAS(%)",
        }
    )

    for c in ["노출", "클릭", "전환"]:
        if c in disp.columns:
            disp[c] = pd.to_numeric(disp[c], errors="coerce").fillna(0).astype(int)

    disp["CTR(%)"] = pd.to_numeric(disp.get("CTR(%)", 0), errors="coerce").fillna(0).astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    cols = [c for c in cols if c in disp.columns]
    view_df = disp[cols].copy()

    render_big_table(view_df, key="ad_big_table", height=620)

    # 다운로드
    render_download_compact(view_df, f"성과_소재_TOP{top_n}_{f['start']}_{f['end']}", "ad", "ad")

def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 / 연결")

    # --- DB Ping ---
    try:
        db_ping(engine)
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    # --- accounts.xlsx sync (FIRST) ---
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    st.caption("처음 1회 동기화가 필요합니다. (업체명/커스텀 ID/담당자)")

    # repo 파일 유무 표시
    repo_exists = os.path.exists(ACCOUNTS_XLSX)
    st.caption(f"기본 경로: `{ACCOUNTS_XLSX}` {'✅' if repo_exists else '❌ (파일 없음)'}")

    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"], accept_multiple_files=False)

    colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
    with colA:
        do_sync = st.button("🔁 동기화 실행", use_container_width=True)
    with colB:
        if st.button("🧹 캐시 비우기", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.session_state.pop("_table_cols_cache", None)
            st.session_state.pop("_table_names_cache", None)
            st.success("캐시를 비웠습니다.")
            st.rerun()
    with colC:
        st.caption("필터/조회가 이상하거나 최신일이 안 바뀌면 캐시 비우기 후 재시도")

    if do_sync:
        try:
            df_src = None
            if up is not None:
                df_src = pd.read_excel(up)
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: meta {res.get('meta', 0)}건")
            # meta cache bust
            st.session_state["meta_ver"] = int(time.time())
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"동기화 실패: {e}")

    # --- Meta Preview ---
    st.divider()
    st.markdown("### 🔎 현재 dim_account_meta 상태")
    try:
        dfm = get_meta(engine)
        st.write(f"- 건수: **{len(dfm)}**")
        if dfm is None or dfm.empty:
            st.warning("dim_account_meta가 비어있습니다. 위에서 accounts.xlsx 동기화를 먼저 해주세요.")
        else:
            st_dataframe_safe(dfm.head(50), use_container_width=True, height=360)
    except Exception as e:
        st.error(f"meta 조회 실패: {e}")

    # --- Optional: index tuning ---
    st.divider()
    with st.expander("⚡ 속도 튜닝 (권장 인덱스 · 선택)", expanded=False):
        st.caption("최초 1회만 실행하면 이후 TOPN/기간 조회가 확 빨라집니다. (권한/정책에 따라 실패할 수 있음)")

        def _create_perf_indexes(_engine) -> List[str]:
            stmts = [
                "CREATE INDEX IF NOT EXISTS idx_f_campaign_dt_cid_txt_camp ON fact_campaign_daily (dt, (customer_id::text), campaign_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_keyword_dt_cid_txt_kw   ON fact_keyword_daily (dt, (customer_id::text), keyword_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_ad_dt_cid_txt_ad        ON fact_ad_daily      (dt, (customer_id::text), ad_id);",
                "CREATE INDEX IF NOT EXISTS idx_f_biz_dt_cid_txt          ON fact_bizmoney_daily(dt, (customer_id::text));",
                "CREATE INDEX IF NOT EXISTS idx_d_campaign_cid_txt_camp   ON dim_campaign ((customer_id::text), campaign_id, campaign_tp);",
                "CREATE INDEX IF NOT EXISTS idx_d_adgroup_cid_txt_adg     ON dim_adgroup  ((customer_id::text), adgroup_id, campaign_id);",
                "CREATE INDEX IF NOT EXISTS idx_d_keyword_cid_txt_kw      ON dim_keyword  ((customer_id::text), keyword_id, adgroup_id);",
                "CREATE INDEX IF NOT EXISTS idx_d_ad_cid_txt_ad           ON dim_ad       ((customer_id::text), ad_id, adgroup_id);",
            ]
            results: List[str] = []
            with _engine.begin() as conn:
                for s in stmts:
                    try:
                        conn.execute(text(s))
                        results.append(f"✅ {s}")
                    except Exception as e:
                        results.append(f"⚠️ {s}  -> {e}")
            return results

        if st.button("⚡ 인덱스 생성 실행", use_container_width=True):
            try:
                logs = _create_perf_indexes(engine)
                for line in logs:
                    st.write(line)
                st.success("완료! 캐시 비우고 다시 조회해보세요.")
            except Exception as e:
                st.error(f"실패: {e}")


    st.divider()
    st.markdown("### 🗑️ 중복/미사용 계정 DB에서 삭제")
    st.caption("비용이 0원으로 뜨는 예전 커스텀 ID를 DB(dim_account_meta)에서 영구 삭제합니다.")

    del_cid = st.text_input("삭제할 계정의 커스텀 ID (숫자만 입력)", placeholder="예: 1234567")
    if st.button("🗑️ 해당 ID 삭제", type="primary"):
        if del_cid.strip() and del_cid.strip().isdigit():
            try:
                # data.py에 있는 sql_exec를 이용해 삭제 쿼리 실행
                sql_exec(engine, "DELETE FROM dim_account_meta WHERE customer_id = :cid", {"cid": int(del_cid.strip())})
                st.success(f"커스텀 ID {del_cid} 삭제 완료! (캐시를 비우고 다시 확인해주세요)")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"삭제 실패: {e}")
        else:
            st.warning("유효한 숫자 ID를 입력해주세요.")


# -----------------------------
# Main
# -----------------------------


def main():
    try:
        engine = get_engine()
        latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None, BUILD_TAG)
        st.error(str(e))
        return

    render_hero(latest, BUILD_TAG)

    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    # --- Sidebar: navigation (desktop-first, always visible on PC) ---
    with st.sidebar:
        st.markdown("### 메뉴")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("처음 1회: accounts.xlsx 동기화가 필요합니다. 아래 '설정/연결'에서 동기화하세요.")

        nav_items = [
            "요약(한눈에)",
            "예산/잔액",
            "캠페인",
            "키워드",
            "소재",
            "설정/연결",
        ]
        if not meta_ready:
            nav_items = ["설정/연결"]

        # keep selection stable
        if not meta_ready:
            st.session_state["nav_page"] = "설정/연결"

        nav = st.radio(
            "menu",
            nav_items,
            key="nav_page",
            label_visibility="collapsed",
        )

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # Page title
    st.markdown(f"<div class='nv-h1'>{nav}</div>", unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Filters (skip on settings)
    f = None
    if nav != "설정/연결":
        if not meta_ready:
            st.error("dim_account_meta가 비어있습니다. 좌측 메뉴의 '설정/연결'에서 accounts.xlsx 동기화를 먼저 해주세요.")
            return
        dim_campaign = load_dim_campaign(engine)
        type_opts = get_campaign_type_options(dim_campaign)
        f = build_filters(meta, type_opts, engine)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Route
    if nav == "요약(한눈에)":
        page_overview(meta, engine, f)
    elif nav == "예산/잔액":
        page_budget(meta, engine, f)
    elif nav == "캠페인":
        page_perf_campaign(meta, engine, f)
    elif nav == "키워드":
        page_perf_keyword(meta, engine, f)
    elif nav == "소재":
        page_perf_ad(meta, engine, f)
    else:
        page_settings(engine)


if __name__ == "__main__":
    main()
