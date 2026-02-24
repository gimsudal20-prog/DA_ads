# -*- coding: utf-8 -*-
"""pages.py - Page functions + router for the Streamlit dashboard."""

from __future__ import annotations

import os
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import numpy as np

# Shared logic & queries
from data import * # noqa
from ui import * # noqa

# -----------------------------
# Build / Thresholds (Budget)
# -----------------------------
BUILD_TAG = os.getenv("APP_BUILD", "v8.7.0 (UX Improvement Update)")

TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty: return []
    if (not manager_sel) and (not account_sel): return []
    df = meta.copy()
    if manager_sel and "manager" in df.columns:
        sel = [str(x).strip() for x in manager_sel if str(x).strip()]
        if sel: df = df[df["manager"].astype(str).str.strip().isin(sel)]
    if account_sel and "account_name" in df.columns:
        sel = [str(x).strip() for x in account_sel if str(x).strip()]
        if sel: df = df[df["account_name"].astype(str).str.strip().isin(sel)]
    if "customer_id" not in df.columns: return []
    s = pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype("int64")
    return sorted(s.drop_duplicates().tolist())

def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    try: return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception: return col.multiselect(label, options, default=default, key=key)

def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    today = date.today()
    default_end = today - timedelta(days=1)
    default_start = default_end

    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "", "manager": [], "account": [], "type_sel": [], "period_mode": "어제",
            "d1": default_start, "d2": default_end, "top_n_keyword": 300, "top_n_ad": 200,
            "top_n_campaign": 200, "prefetch_warm": True,
        }
    sv = st.session_state["filters_v8"]
    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    with st.sidebar:
        st.divider()
        st.markdown("### 🔍 데이터 검색 조건")
        
        period_mode = st.selectbox(
            "기간", ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"],
            index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")),
            key="f_period_mode",
        )

        if period_mode == "직접 선택":
            d1 = st.date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = st.date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달": 
                first_this = date(today.year, today.month, 1)
                d2 = first_this - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            st.caption(f"선택 기간: {d1} ~ {d2}")

        q = st.text_input("검색어 (계정/키워드 등)", sv.get("q", ""), key="f_q")
        manager_sel = ui_multiselect(st, "담당자", managers, default=sv.get("manager", []), key="f_manager")
        
        accounts_by_mgr = accounts
        if manager_sel:
            try:
                dfm = meta.copy()
                dfm['manager'] = dfm.get('manager','').astype(str).fillna('').str.strip()
                dfm['account_name'] = dfm.get('account_name','').astype(str).fillna('').str.strip()
                dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
            except Exception: pass
        
        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
        account_sel = ui_multiselect(st, "계정", accounts_by_mgr, default=prev_acc, key="f_account")
        type_sel = ui_multiselect(st, "캠페인 유형", type_opts, default=sv.get("type_sel", []), key="f_type_sel")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv
    cids = resolve_customer_ids(meta, manager_sel, account_sel)
    
    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "prefetch_warm": bool(sv.get("prefetch_warm", True)), "ready": True,
    }

def render_filter_summary_bar(f: Dict, meta: pd.DataFrame) -> None:
    try: n_total = int(meta["customer_id"].nunique()) if meta is not None and not meta.empty else 0
    except Exception: n_total = 0
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
        """, unsafe_allow_html=True
    )

def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: st.info("검색조건을 설정하면 요약이 표시됩니다."); return
    st.markdown("<div class='nv-sec-title'>요약</div>", unsafe_allow_html=True)
    render_filter_summary_bar(f, meta)

    cids = tuple((f.get("selected_customer_ids") or f.get("customer_ids") or []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())
    cmp_mode = st.radio("비교 기준", ["전일대비", "전주대비", "전월대비"], horizontal=True, index=1, key="ov_cmp_mode")
    cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    b1, b2 = _period_compare_range(f["start"], f["end"], cmp_mode)
    base = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    def _delta_pct(key: str) -> Optional[float]:
        try: return _pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception: return None

    items = [
        ("광고비", format_currency(cur.get("cost", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost")),
        ("전환매출", format_currency(cur.get("sales", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales")),
        ("전환", format_number_commas(cur.get("conv", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv")),
        ("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.0f}%", f"{cmp_mode} {_pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas")),
        ("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{cmp_mode} {_pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr")),
        ("CPC", format_currency(cur.get("cpc", 0.0)), f"{cmp_mode} {_pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc")),
    ]

    def _kpi_html(label: str, value: str, delta_text: str, delta_val: Optional[float]) -> str:
        cls = "neu"
        try:
            if delta_val is None or (isinstance(delta_val, float) and math.isnan(delta_val)): cls = "neu"
            elif float(delta_val) > 0: cls = "pos"
            elif float(delta_val) < 0: cls = "neg"
        except Exception: cls = "neu"
        tooltip = "해당 지표의 비교 수치입니다." # 기본 툴팁
        if label == "ROAS": tooltip = "ROAS = 전환매출 / 광고비 * 100"
        return f"<div class='kpi'><span class='tooltip'>{tooltip}</span><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls}'>{delta_text}</div></div>"

    kpi_html = "<div class='kpi-row'>" + "".join(_kpi_html(a, b, c, d) for a, b, c, d in items) + "</div>"
    st.markdown(kpi_html, unsafe_allow_html=True)
    st.divider()

    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
        if ts is None or ts.empty:
            render_empty_state("표시할 추세 데이터가 없습니다.")
            return
        st.markdown("<div class='nv-sec-title'>추세</div>", unsafe_allow_html=True)
        render_timeseries_chart(ts, entity="campaign", key_prefix="ov_ts")
    except Exception:
        st.info("추세 데이터를 불러오는 중 오류가 발생했습니다.")

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")
    render_filter_summary_bar(f, meta)

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
    month_d1 = end_dt.replace(day=1)
    if end_dt.month == 12: month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1)
    else: month_d2 = date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    if bundle is None or bundle.empty:
        render_empty_state("예산/잔액 데이터가 없습니다.", "fact_bizmoney_daily 테이블 갱신을 확인하세요.")
        return

    biz_view = bundle.copy()
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")
    biz_view["days_cover"] = pd.NA
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)
    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)
    biz_view["전일 소진액"] = biz_view["y_cost"].map(format_currency)

    def _fmt_days(d):
        if pd.isna(d) or d is None: return "-"
        try: dd = float(d)
        except Exception: return "-"
        if dd > 99: return "99+일"
        return f"{dd:.1f}일"

    biz_view["D-소진"] = biz_view["days_cover"].map(_fmt_days)
    biz_view["확인일자"] = biz_view["last_update"]

    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', key='m_total_balance')
    with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3: ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', key='m_need_topup')
    st.divider()

    ok_topup = int(len(biz_view) - count_low_balance)
    st.markdown(f"<span class='badge b-red'>충전필요 {count_low_balance}건</span><span class='badge b-green'>여유 {ok_topup}건</span>", unsafe_allow_html=True)
    show_only_topup = st.checkbox("충전필요만 보기", value=False)
    biz_view["_rank"] = biz_view["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup: biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    view_cols = ["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]
    display_df = biz_view[view_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()
    ui_table_or_dataframe(display_df, key="budget_biz_table", height=520)
    render_download_compact(display_df, f"예산_잔액_{f['start']}_{f['end']}", "budget", "budget")
    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")
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
    budget_view["상태"] = tmp[0]; budget_view["status_text"] = tmp[1]; budget_view["_rank"] = tmp[2].astype(int)
    cnt_over = int((budget_view["status_text"] == "초과").sum()); cnt_warn = int((budget_view["status_text"] == "주의").sum()); cnt_unset = int((budget_view["status_text"] == "미설정").sum())

    st.markdown(f"<span class='badge b-red'>초과 {cnt_over}건</span><span class='badge b-yellow'>주의 {cnt_warn}건</span><span class='badge b-gray'>미설정 {cnt_unset}건</span>", unsafe_allow_html=True)
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
        st.markdown("""<div class="panel" style="line-height:1.85; font-size:14px; background: rgba(235,238,242,0.75);"><b>상태 가이드</b><br><br>🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>⚪ <b>미설정</b> : 월 예산 <b>0원</b></div>""", unsafe_allow_html=True)

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
    if df is None or df.empty: return df
    return df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🚀 성과 (캠페인)")
    render_filter_summary_bar(f, meta)

    top_n = int(f.get("top_n_campaign", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다.')
        return
    type_sel = tuple(f.get("type_sel", []) or [])

    try: bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=max(top_n, 200), top_k=10)
    except Exception: bundle = pd.DataFrame()

    if bundle is None or bundle.empty:
        render_empty_state("해당 기간의 캠페인 데이터가 없습니다.", "최근 수집 지연일 수 있으니 '어제' 날짜로 변경해 보세요.")
        return

    bundle["customer_id"] = pd.to_numeric(bundle["customer_id"], errors="coerce").astype("Int64")
    bundle = bundle.dropna(subset=["customer_id"]).copy()
    bundle["customer_id"] = bundle["customer_id"].astype("int64")
    bundle = _attach_account_name(bundle, meta)
    if "manager" in meta.columns:
        try:
            m_map = meta.set_index("customer_id")["manager"].to_dict()
            bundle["manager"] = bundle["customer_id"].map(m_map)
        except Exception: bundle["manager"] = ""
    bundle = add_rates(bundle)

    top_cost = bundle[pd.to_numeric(bundle.get("rn_cost", np.nan), errors="coerce").between(1,5)].sort_values("rn_cost") if "rn_cost" in bundle.columns else bundle.sort_values("cost", ascending=False).head(5)
    top_clk = bundle[pd.to_numeric(bundle.get("rn_clk", np.nan), errors="coerce").between(1,5)].sort_values("rn_clk") if "rn_clk" in bundle.columns else bundle.sort_values("clk", ascending=False).head(5)
    top_conv = bundle[pd.to_numeric(bundle.get("rn_conv", np.nan), errors="coerce").between(1,5)].sort_values("rn_conv") if "rn_conv" in bundle.columns else bundle.sort_values("conv", ascending=False).head(5)

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame(columns=["업체명", "캠페인", metric])
        x = df.copy()
        if metric == "광고비": x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭": x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else: x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        x = x.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"})
        keep_cols = [c for c in ["업체명", "캠페인", metric] if c in x.columns]
        return x[keep_cols]

    st.markdown("### 🏆 성과별 TOP 5 (캠페인)")
    t1, t2, t3 = st.tabs(["💸 광고비 TOP5", "🖱️ 클릭 TOP5", "✅ 전환 TOP5"])
    with t1: ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='camp_top5_cost', height=240)
    with t2: ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='camp_top5_clk', height=240)
    with t3: ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='camp_top5_conv', height=240)
    st.divider()

    st.markdown("### 📈 기간 추세")
    render_period_compare_panel(engine, "campaign", f["start"], f["end"], cids, type_sel, key_prefix="camp", expanded=False)
    show_detail = st.toggle("상세(캠페인 추세/표) 보기", value=False, key="camp_detail_toggle")
    multi_acc = bundle["customer_id"].nunique() > 1
    bundle["label"] = bundle.apply(lambda r: f'{r.get("account_name","")} · {r.get("campaign_name","")}' if multi_acc else str(r.get("campaign_name","")), axis=1)
    options = ["(전체 캠페인)"] + bundle["label"].dropna().astype(str).unique().tolist()
    sel = st.selectbox("캠페인 선택", options, index=0, key="camp_select")

    ts = pd.DataFrame()
    if show_detail:
        try:
            if sel == "(전체 캠페인)": ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
            else:
                row = bundle[bundle["label"] == sel].head(1)
                if not row.empty:
                    ts = query_campaign_one_timeseries(engine, f["start"], f["end"], int(row.iloc[0]["customer_id"]), int(row.iloc[0]["campaign_id"]))
        except Exception: ts = pd.DataFrame()

    if show_detail and ts is not None and not ts.empty:
        metric_sel = st.radio("트렌드 지표", ["광고비", "클릭", "전환", "ROAS"], horizontal=True, index=0, key="camp_trend_metric")
        ts2 = ts.copy()
        if "sales" in ts2.columns and "cost" in ts2.columns: ts2["roas"] = np.where(pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) > 0, pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts2["cost"], errors="coerce").fillna(0) * 100.0, 0.0)
        else: ts2["roas"] = 0.0

        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None: render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None: render_chart(ch)

        if metric_sel == '광고비': _render('cost', '광고비(원)')
        elif metric_sel == '클릭': _render('clk', '클릭')
        elif metric_sel == '전환': _render('conv', '전환')
        else: _render('roas', 'ROAS(%)')

    df = bundle.copy()
    if "rn_cost" in df.columns: df = df[pd.to_numeric(df["rn_cost"], errors="coerce").between(1, top_n)].sort_values("rn_cost")
    else: df = df.sort_values("cost", ascending=False).head(top_n)

    display_df = df.rename(columns={"account_name": "업체명", "campaign_type": "캠페인유형", "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "매출"})
    display_df = finalize_display_cols(display_df)

    render_big_table(display_df, key="camp_main_grid", height=560)
    render_download_compact(display_df, f"성과_캠페인_TOP{top_n}_{f['start']}_{f['end']}", "campaign", "camp")

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False): return
    st.markdown("## 🔎 성과 (키워드)")
    render_filter_summary_bar(f, meta)

    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids:
        st.warning('선택한 담당자/계정에 매칭되는 customer_id를 찾지 못했습니다.')
        return
    type_sel = tuple(f.get("type_sel", []) or [])
    top_n = int(f.get("top_n_keyword", 300))

    bundle = query_keyword_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        render_empty_state("해당 기간의 키워드 데이터가 없습니다.")
        return

    top_cost = bundle[pd.to_numeric(bundle["rn_cost"], errors="coerce").between(1,10)].sort_values("rn_cost")
    top_clk = bundle[pd.to_numeric(bundle["rn_clk"], errors="coerce").between(1,10)].sort_values("rn_clk")
    top_conv = bundle[pd.to_numeric(bundle["rn_conv"], errors="coerce").between(1,10)].sort_values("rn_conv")

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame(columns=["업체명", "키워드", metric])
        x = df.copy()
        x["customer_id"] = pd.to_numeric(x["customer_id"], errors="coerce").astype("Int64")
        x = x.dropna(subset=["customer_id"]).copy()
        x["customer_id"] = x["customer_id"].astype("int64")
        x = x.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
        if metric == "광고비": x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭": x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else: x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        return x.rename(columns={"account_name": "업체명", "keyword": "키워드"})[["업체명", "키워드", metric]]

    st.markdown("### 🏆 성과별 TOP 10 (키워드)")
    t1, t2, t3 = st.tabs(["💸 광고비 TOP10", "🖱️ 클릭 TOP10", "✅ 전환 TOP10"])
    with t1: ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='kw_top10_cost', height=240)
    with t2: ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='kw_top10_clk', height=240)
    with t3: ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='kw_top10_conv', height=240)
    st.divider()

    df = bundle[bundle["rn_cost"] <= top_n].sort_values("rn_cost").copy()
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")
    df = add_rates(df)

    try: ts = query_keyword_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception: ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum()); total_clk = float(ts["clk"].sum()); total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1: ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_kw_cost")
        with k2: ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_kw_clk")
        with k3: ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_kw_conv")
        with k4: ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_kw_roas", tooltip="ROAS = 전환매출 / 광고비 * 100")

        render_period_compare_panel(engine, "keyword", f["start"], f["end"], cids, type_sel, key_prefix="kw", expanded=False)

        metric_sel = st.radio("트렌드 지표", ["광고비", "클릭", "전환", "ROAS"], horizontal=True, index=0, key="kw_trend_metric")
        ts2 = ts.copy()
        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None: render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None: render_chart(ch)

        if metric_sel == '광고비': _render('cost', '광고비(원)')
        elif metric_sel == '클릭': _render('clk', '클릭')
        elif metric_sel == '전환': _render('conv', '전환')
        else:
            sales_s = pd.to_numeric(ts2['sales'], errors='coerce').fillna(0) if 'sales' in ts2.columns else pd.Series([0.0] * len(ts2))
            ts2['roas'] = (sales_s / ts2['cost'].replace(0, np.nan)) * 100
            ts2['roas'] = pd.to_numeric(ts2['roas'], errors='coerce').fillna(0)
            _render('roas', 'ROAS(%)')
        st.divider()

    df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")
    view = df.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "ctr": "CTR(%)", "cpc": "CPC", "cost": "비용", "conv": "전환", "cpa": "CPA", "sales": "매출", "roas": "ROAS(%)"})
    view["비용"] = pd.to_numeric(view["비용"], errors="coerce").fillna(0).map(format_currency)
    view["CPC"] = pd.to_numeric(view["CPC"], errors="coerce").fillna(0).map(format_currency)
    view["CPA"] = pd.to_numeric(view["CPA"], errors="coerce").fillna(0).map(format_currency)
    view["매출"] = pd.to_numeric(view.get("매출", 0), errors="coerce").fillna(0).map(format_currency)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드", "노출", "클릭", "CTR(%)", "CPC", "비용", "전환", "CPA", "매출", "ROAS(%)"]
    out_df = view[cols].copy()
    out_df["노출"] = pd.to_numeric(out_df["노출"], errors="coerce").fillna(0).astype(int)
    out_df["클릭"] = pd.to_numeric(out_df["클릭"], errors="coerce").fillna(0).astype(int)
    out_df["전환"] = pd.to_numeric(out_df["전환"], errors="coerce").fillna(0).astype(int)

    render_big_table(out_df, key='kw_big_table', height=620)
    render_download_compact(out_df, f"키워드성과_TOP{top_n}_{f['start']}_{f['end']}", "keyword", "kw")

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    st.markdown("## 🧩 성과 (소재)")
    render_filter_summary_bar(f, meta)

    top_n = int(f.get("top_n_ad", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    if (f.get('manager') or f.get('account')) and not cids: return
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty:
        render_empty_state("해당 기간의 소재 데이터가 없습니다.")
        return

    df = _perf_common_merge_meta(bundle, meta)
    df = add_rates(df)

    try: ts = query_ad_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception: ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum()); total_clk = float(ts["clk"].sum()); total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1: ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_ad_cost")
        with k2: ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_ad_clk")
        with k3: ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_ad_conv")
        with k4: ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_ad_roas")

        render_period_compare_panel(engine, "ad", f["start"], f["end"], cids, type_sel, key_prefix="ad", expanded=False)

        metric_sel = st.radio("트렌드 지표", ["광고비", "클릭", "전환", "ROAS"], horizontal=True, index=0, key="ad_trend_metric")
        ts2 = ts.copy()
        def _render(ycol: str, yname: str):
            if HAS_ECHARTS and st_echarts is not None: render_echarts_line('트렌드', ts2, 'dt', ycol, yname, height=260)
            else:
                ch = _chart_timeseries(ts2, ycol, yname, y_format=',.0f', height=260)
                if ch is not None: render_chart(ch)

        if metric_sel == '광고비': _render('cost', '광고비(원)')
        elif metric_sel == '클릭': _render('clk', '클릭')
        elif metric_sel == '전환': _render('conv', '전환')
        else:
            sales_s = pd.to_numeric(ts2['sales'], errors='coerce').fillna(0) if 'sales' in ts2.columns else pd.Series([0.0] * len(ts2))
            ts2['roas'] = (sales_s / ts2['cost'].replace(0, np.nan)) * 100
            ts2['roas'] = pd.to_numeric(ts2['roas'], errors='coerce').fillna(0)
            _render('roas', 'ROAS(%)')
        st.divider()

    top_cost = df.sort_values("cost", ascending=False).head(5)
    top_clk = df.sort_values("clk", ascending=False).head(5)
    top_conv = df.sort_values("conv", ascending=False).head(5)

    def _fmt_top(dfx: pd.DataFrame, metric: str) -> pd.DataFrame:
        if dfx is None or dfx.empty: return pd.DataFrame(columns=["업체명", "캠페인", "소재내용", metric])
        x = dfx.copy()
        x["업체명"] = x.get("account_name", "")
        x["캠페인"] = x.get("campaign_name", "")
        x["소재내용"] = x.get("ad_name", "")
        if metric == "광고비": x[metric] = x.get("cost", 0).map(format_currency)
        elif metric == "클릭": x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else: x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)
        return x[["업체명", "캠페인", "소재내용", metric]]

    st.markdown("### 🏆 성과별 TOP 5 (소재)")
    t1, t2, t3 = st.tabs(["💸 광고비 TOP5", "🖱️ 클릭 TOP5", "✅ 전환 TOP5"])
    with t1: ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='ad_top5_cost', height=240)
    with t2: ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='ad_top5_clk', height=240)
    with t3: ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='ad_top5_conv', height=240)
    st.divider()

    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()
    disp = main_df.copy()
    disp["cost"] = disp["cost"].apply(format_currency); disp["sales"] = disp["sales"].apply(format_currency)
    disp["cpc"] = disp["cpc"].apply(format_currency); disp["cpa"] = disp["cpa"].apply(format_currency)
    disp["roas_disp"] = disp["roas"].apply(format_roas)

    disp = disp.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_id": "소재ID", "ad_name": "소재내용", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "ctr": "CTR(%)", "cpc": "CPC", "cpa": "CPA", "sales": "전환매출", "roas_disp": "ROAS(%)"})
    disp["노출"] = pd.to_numeric(disp["노출"], errors="coerce").fillna(0).astype(int)
    disp["클릭"] = pd.to_numeric(disp["클릭"], errors="coerce").fillna(0).astype(int)
    disp["전환"] = pd.to_numeric(disp["전환"], errors="coerce").fillna(0).astype(int)
    disp["CTR(%)"] = disp["CTR(%)"].astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = disp[cols].copy()

    render_big_table(view_df, key='ad_big_table', height=620)
    render_download_compact(view_df, f"성과_소재_TOP{top_n}_{f['start']}_{f['end']}", "ad", "ad")

def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 / 연결")
    try: db_ping(engine); st.success("DB 연결 성공 ✅")
    except Exception as e: st.error(f"DB 연결 실패: {e}"); return

    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    st.caption("처음 1회 동기화가 필요합니다. (업체명/커스텀 ID/담당자)")
    repo_exists = os.path.exists(ACCOUNTS_XLSX)
    st.caption(f"기본 경로: `{ACCOUNTS_XLSX}` {'✅' if repo_exists else '❌ (파일 없음)'}")
    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"], accept_multiple_files=False)

    colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
    with colA: do_sync = st.button("🔁 동기화 실행", use_container_width=True)
    with colB:
        if st.button("🧹 캐시 비우기", use_container_width=True):
            st.cache_data.clear(); st.cache_resource.clear()
            st.session_state.pop("_table_cols_cache", None); st.session_state.pop("_table_names_cache", None)
            st.success("캐시를 비웠습니다."); st.rerun()
    with colC: st.caption("필터/조회가 이상하거나 최신일이 안 바뀌면 캐시 비우기 후 재시도")

    if do_sync:
        try:
            df_src = None
            if up is not None: df_src = pd.read_excel(up)
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: meta {res.get('meta', 0)}건")
            st.session_state["meta_ver"] = int(time.time()); st.cache_data.clear(); st.rerun()
        except Exception as e: st.error(f"동기화 실패: {e}")

    st.divider()
    st.markdown("### 🔎 현재 dim_account_meta 상태")
    try:
        dfm = get_meta(engine)
        st.write(f"- 건수: **{len(dfm)}**")
        if dfm is None or dfm.empty: st.warning("dim_account_meta가 비어있습니다. 위에서 accounts.xlsx 동기화를 먼저 해주세요.")
        else: st_dataframe_safe(dfm.head(50), use_container_width=True, height=360)
    except Exception as e: st.error(f"meta 조회 실패: {e}")

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

    with st.sidebar:
        st.markdown("### 📍 메뉴")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if not meta_ready: st.warning("처음 1회: accounts.xlsx 동기화가 필요합니다.")

        nav_items = ["요약(한눈에)", "예산/잔액", "캠페인", "키워드", "소재", "설정/연결"]
        if not meta_ready: nav_items = ["설정/연결"]
        if not meta_ready: st.session_state["nav_page"] = "설정/연결"

        nav = st.radio("menu", nav_items, key="nav_page", label_visibility="collapsed")
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    st.markdown(f"<div class='nv-h1'>{nav}</div>", unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    f = None
    if nav != "설정/연결":
        if not meta_ready:
            st.error("dim_account_meta가 비어있습니다. 좌측 메뉴의 '설정/연결'에서 accounts.xlsx 동기화를 먼저 해주세요.")
            return
        dim_campaign = load_dim_campaign(engine)
        type_opts = get_campaign_type_options(dim_campaign)
        f = build_filters(meta, type_opts, engine)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    if nav == "요약(한눈에)": page_overview(meta, engine, f)
    elif nav == "예산/잔액": page_budget(meta, engine, f)
    elif nav == "캠페인": page_perf_campaign(meta, engine, f)
    elif nav == "키워드": page_perf_keyword(meta, engine, f)
    elif nav == "소재": page_perf_ad(meta, engine, f)
    else: page_settings(engine)

if __name__ == "__main__":
    main()
