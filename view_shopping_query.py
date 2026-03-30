# -*- coding: utf-8 -*-
"""view_shopping_query.py - Shopping Search Term (Query Text) performance view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import io
from typing import Dict
from datetime import date

from data import query_shopping_search_terms
from page_helpers import _perf_common_merge_meta


def _safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _build_action_labels(view: pd.DataFrame) -> pd.DataFrame:
    out = view.copy()
    numeric_cols = [
        "구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액",
        "위시리스트수", "위시리스트 매출액", "총 전환수", "총 전환매출"
    ]
    for c in numeric_cols:
        if c in out.columns:
            out[c] = _safe_num(out[c])

    pos_purchase = out.loc[out["구매완료수"] > 0, "구매완료수"] if "구매완료수" in out.columns else pd.Series(dtype=float)
    pos_sales = out.loc[out["구매완료 매출"] > 0, "구매완료 매출"] if "구매완료 매출" in out.columns else pd.Series(dtype=float)
    purchase_thr = max(float(pos_purchase.median()) if not pos_purchase.empty else 1, 1)
    sales_thr = max(float(pos_sales.median()) if not pos_sales.empty else 1, 1)

    out["구매기여율(%)"] = np.where(out["총 전환수"] > 0, (out["구매완료수"] / out["총 전환수"]) * 100, 0.0)
    out["장바구니기여율(%)"] = np.where(out["총 전환수"] > 0, (out["장바구니수"] / out["총 전환수"]) * 100, 0.0)
    out["위시리스트기여율(%)"] = np.where(out["총 전환수"] > 0, (out["위시리스트수"] / out["총 전환수"]) * 100, 0.0)

    conditions = [
        (out["구매완료수"] >= purchase_thr) & (out["구매완료 매출"] >= sales_thr),
        (out["구매완료수"] >= 1),
        (out["구매완료수"] == 0) & (out["장바구니수"] >= 1),
        (out["구매완료수"] == 0) & (out["장바구니수"] == 0) & (out["위시리스트수"] >= 1),
    ]
    choices = ["확대 후보", "유지", "관찰", "저의도 의심"]
    out["액션 라벨"] = np.select(conditions, choices, default="유지")

    reasons = []
    for _, row in out.iterrows():
        purchase = float(row.get("구매완료수", 0) or 0)
        cart = float(row.get("장바구니수", 0) or 0)
        wish = float(row.get("위시리스트수", 0) or 0)
        sales = float(row.get("구매완료 매출", 0) or 0)
        label = str(row.get("액션 라벨", "유지"))
        if label == "확대 후보":
            reasons.append(f"구매 {purchase:,.0f}건 · 매출 {sales:,.0f}원")
        elif label == "유지":
            reasons.append(f"구매 {purchase:,.0f}건 발생")
        elif label == "관찰":
            reasons.append(f"장바구니 {cart:,.0f}건 · 구매 0건")
        elif label == "저의도 의심":
            reasons.append(f"위시리스트 {wish:,.0f}건 · 구매/장바구니 없음")
        else:
            reasons.append("추가 확인 필요")
    out["추천 사유"] = reasons
    return out


def _render_summary_cards(view: pd.DataFrame) -> None:
    if view.empty:
        return
    total_terms = int(view["실제 검색어"].nunique()) if "실제 검색어" in view.columns else int(len(view))
    scale_terms = int((view["액션 라벨"] == "확대 후보").sum()) if "액션 라벨" in view.columns else 0
    observe_terms = int((view["액션 라벨"] == "관찰").sum()) if "액션 라벨" in view.columns else 0
    purchase_terms = int((view["구매완료수"] > 0).sum()) if "구매완료수" in view.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("검색어 수", f"{total_terms:,}")
    c2.metric("확대 후보", f"{scale_terms:,}")
    c3.metric("관찰 필요", f"{observe_terms:,}")
    c4.metric("구매 발생 검색어", f"{purchase_terms:,}")


def page_perf_shopping_query(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    st.markdown("<div class='nv-sec-title'>쇼핑 검색어 상세 분석 (Query Text)</div>", unsafe_allow_html=True)
    st.caption("고객이 네이버 쇼핑에서 실제로 검색한 단어 기준의 전환 성과를 분석합니다. 비용/클릭 지표 없이 퍼널 전환 데이터 중심으로 해석합니다.")

    cids = tuple(f.get("selected_customer_ids", []))
    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)

    if has_pre_patch_cur:
        st.info("3월 11일 이전 데이터는 네이버 퍼널 분리 패치 이전이므로 통합 전환 지표만 존재할 수 있습니다.")

    with st.spinner("쇼핑 검색어 데이터를 불러오는 중입니다..."):
        df = query_shopping_search_terms(engine, f["start"], f["end"], cids)
        if df.empty:
            st.warning("해당 기간에 수집된 쇼핑 검색어 전환 데이터가 없습니다.")
            return

        df = _perf_common_merge_meta(df, meta)
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자",
            "campaign_name": "캠페인", "adgroup_name": "광고그룹", "query_text": "실제 검색어",
            "purchase_conv": "구매완료수", "purchase_sales": "구매완료 매출",
            "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
            "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액",
            "total_conv": "총 전환수", "total_sales": "총 전환매출"
        }).copy()

        for c in ["구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액", "위시리스트수", "위시리스트 매출액", "총 전환수", "총 전환매출"]:
            if c in view.columns:
                view[c] = _safe_num(view[c])

        view = _build_action_labels(view)

    _render_summary_cards(view)
    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    with st.container(border=True):
        st.markdown("<div class='nv-card-title'>쇼핑 검색어 액션 필터</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        camps = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
        sel_camp = c1.selectbox("캠페인 필터", camps, key="sq_camp_filter")
        if sel_camp != "전체":
            view = view[view["캠페인"] == sel_camp]

        grps = ["전체"] + sorted([str(x) for x in view["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in view.columns else ["전체"]
        sel_grp = c2.selectbox("광고그룹 필터", grps, key="sq_grp_filter")
        if sel_grp != "전체":
            view = view[view["광고그룹"] == sel_grp]

        c3, c4, c5 = st.columns([1.2, 1, 1])
        labels = ["전체"] + [x for x in ["확대 후보", "유지", "관찰", "저의도 의심"] if x in set(view["액션 라벨"].dropna())]
        sel_label = c3.selectbox("액션 라벨", labels, key="sq_action_label_filter")
        if sel_label != "전체":
            view = view[view["액션 라벨"] == sel_label]

        zero_purchase_only = c4.checkbox("구매 0건만", value=False, key="sq_zero_purchase_only")
        if zero_purchase_only:
            view = view[view["구매완료수"] <= 0]

        cart_only = c5.checkbox("장바구니 발생만", value=False, key="sq_cart_only")
        if cart_only:
            view = view[view["장바구니수"] > 0]

        c6, c7, c8 = st.columns([1.5, 1, 1])
        term_kw = c6.text_input("검색어 포함", value="", key="sq_term_kw", placeholder="예: 원목, 거실, 수납")
        if term_kw.strip():
            view = view[view["실제 검색어"].astype(str).str.contains(term_kw.strip(), case=False, na=False)]

        min_sales = c7.number_input("최소 구매매출", min_value=0, value=0, step=10000, key="sq_min_purchase_sales")
        if min_sales > 0:
            view = view[view["구매완료 매출"] >= float(min_sales)]

        min_total_conv = c8.number_input("최소 총전환수", min_value=0.0, value=0.0, step=1.0, key="sq_min_total_conv")
        if min_total_conv > 0:
            view = view[view["총 전환수"] >= float(min_total_conv)]

    if view.empty:
        st.warning("현재 필터 조건에 맞는 쇼핑 검색어 데이터가 없습니다.")
        return

    fmt = {
        "구매완료수": "{:,.0f}", "구매완료 매출": "{:,.0f}원",
        "장바구니수": "{:,.0f}", "장바구니 매출액": "{:,.0f}원",
        "위시리스트수": "{:,.0f}", "위시리스트 매출액": "{:,.0f}원",
        "총 전환수": "{:,.0f}", "총 전환매출": "{:,.0f}원",
        "구매기여율(%)": "{:,.1f}%", "장바구니기여율(%)": "{:,.1f}%", "위시리스트기여율(%)": "{:,.1f}%",
    }

    display_cols = [
        "업체명", "캠페인", "광고그룹", "실제 검색어",
        "액션 라벨", "추천 사유",
        "구매완료수", "구매완료 매출", "장바구니수", "위시리스트수",
        "총 전환수", "총 전환매출", "구매기여율(%)", "장바구니기여율(%)"
    ]

    sort_col = "구매완료 매출" if (view["구매완료 매출"] > 0).any() else "총 전환매출"
    disp = view[[c for c in display_cols if c in view.columns]].sort_values(sort_col, ascending=False).head(500).copy()

    st.markdown("<div class='nv-card-title' style='margin-top:20px;'>검색어별 퍼널 성과 · 액션센터</div>", unsafe_allow_html=True)
    st.dataframe(disp.style.format(fmt), use_container_width=True, height=640, hide_index=True)

    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        disp.to_excel(writer, sheet_name='쇼핑_검색어_성과', index=False)
    st.download_button(
        label="검색어 리포트 다운로드 (Excel)",
        data=excel_buffer.getvalue(),
        file_name=f"쇼핑_검색어_리포트_{f['start']}_{f['end']}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
