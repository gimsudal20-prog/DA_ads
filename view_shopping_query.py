# -*- coding: utf-8 -*-
"""view_shopping_query.py - Shopping Search Term performance view (dashboard-styled)."""

from __future__ import annotations
import io
from datetime import date
from typing import Dict

import numpy as np
import pandas as pd
import streamlit as st

from data import query_shopping_search_terms
from page_helpers import _perf_common_merge_meta, period_compare_range
from ui import render_big_table, ui_metric_or_stmetric


FMT_DICT = {
    "구매완료수": "{:,.0f}",
    "구매완료 매출": "{:,.0f}원",
    "장바구니수": "{:,.0f}",
    "장바구니 매출액": "{:,.0f}원",
    "위시리스트수": "{:,.0f}",
    "위시리스트 매출액": "{:,.0f}원",
    "총 전환수": "{:,.0f}",
    "총 전환매출": "{:,.0f}원",
    "구매기여율(%)": "{:,.1f}%",
    "장바구니기여율(%)": "{:,.1f}%",
    "구매완료수 증감": "{:+.1f}%",
    "구매완료 매출 증감": "{:+.1f}%",
    "총 전환수 증감": "{:+.1f}%",
    "총 전환매출 증감": "{:+.1f}%",
}


def _empty_notice(message: str):
    st.info(message)


def _to_num(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
    return out


def _pct_change(cur, base):
    cur = pd.to_numeric(cur, errors="coerce").fillna(0)
    base = pd.to_numeric(base, errors="coerce").fillna(0)
    diff = cur - base
    safe_base = np.where(base == 0, 1, base)
    pct = np.where(base == 0, np.where(cur > 0, 100.0, 0.0), (diff / safe_base) * 100.0)
    return pct, diff


def _merge_compare(cur: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ["customer_id", "campaign_name", "adgroup_name", "query_text"] if c in cur.columns and c in prev.columns]
    if not keys:
        return cur.copy()

    c = cur.copy()
    p = prev.copy()
    for k in keys:
        c[k] = c[k].astype(str)
        p[k] = p[k].astype(str)

    val_cols = [
        "purchase_conv", "purchase_sales", "cart_conv", "cart_sales",
        "wishlist_conv", "wishlist_sales", "total_conv", "total_sales",
    ]
    p = _to_num(p, val_cols)
    p = p[keys + [x for x in val_cols if x in p.columns]].copy()
    p = p.rename(columns={x: f"b_{x}" for x in val_cols if x in p.columns})
    out = c.merge(p, on=keys, how="left")
    for x in val_cols:
        bx = f"b_{x}"
        if bx not in out.columns:
            out[bx] = 0
        out[bx] = pd.to_numeric(out[bx], errors="coerce").fillna(0)

    out["구매완료수 증감"], out["구매완료수 차이"] = _pct_change(out.get("purchase_conv", 0), out.get("b_purchase_conv", 0))
    out["구매완료 매출 증감"], out["구매완료 매출 차이"] = _pct_change(out.get("purchase_sales", 0), out.get("b_purchase_sales", 0))
    out["총 전환수 증감"], out["총 전환수 차이"] = _pct_change(out.get("total_conv", 0), out.get("b_total_conv", 0))
    out["총 전환매출 증감"], out["총 전환매출 차이"] = _pct_change(out.get("total_sales", 0), out.get("b_total_sales", 0))

    def _compare_tag(row):
        cur_p = float(row.get("purchase_conv", 0) or 0)
        base_p = float(row.get("b_purchase_conv", 0) or 0)
        cur_sales = float(row.get("purchase_sales", 0) or 0)
        base_sales = float(row.get("b_purchase_sales", 0) or 0)
        sales_delta = float(row.get("구매완료 매출 증감", 0) or 0)
        if base_p == 0 and cur_p > 0:
            return "전환 회복"
        if cur_sales > 0 and sales_delta >= 50:
            return "급상승"
        if base_sales > 0 and cur_sales == 0:
            return "효율 악화"
        if sales_delta <= -50:
            return "하락"
        return "유지"

    out["변화 태그"] = out.apply(_compare_tag, axis=1)
    return out


def _add_funnel_metrics(view: pd.DataFrame) -> pd.DataFrame:
    out = view.copy()
    out = _to_num(out, [
        "구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액",
        "위시리스트수", "위시리스트 매출액", "총 전환수", "총 전환매출",
    ])
    out["구매기여율(%)"] = np.where(out["총 전환수"] > 0, (out["구매완료수"] / out["총 전환수"]) * 100, 0.0)
    out["장바구니기여율(%)"] = np.where(out["총 전환수"] > 0, (out["장바구니수"] / out["총 전환수"]) * 100, 0.0)
    return out


def _add_action_labels(view: pd.DataFrame) -> pd.DataFrame:
    out = view.copy()
    sales_pos = pd.to_numeric(out.get("구매완료 매출", 0), errors="coerce").fillna(0)
    positive_sales = sales_pos[sales_pos > 0]
    sales_cut = float(positive_sales.quantile(0.6)) if not positive_sales.empty else 0.0
    sales_cut = max(sales_cut, 1.0)

    labels = []
    reasons = []
    for _, row in out.iterrows():
        p = float(row.get("구매완료수", 0) or 0)
        s = float(row.get("구매완료 매출", 0) or 0)
        c = float(row.get("장바구니수", 0) or 0)
        w = float(row.get("위시리스트수", 0) or 0)
        t = float(row.get("총 전환수", 0) or 0)
        q = str(row.get("실제 검색어", "") or "")

        if p >= 1 and (s >= sales_cut or t >= 2):
            labels.append("확대 후보")
            reasons.append("구매 발생 + 매출/전환 기여가 높음")
        elif p == 0 and c >= 1:
            labels.append("관찰 필요")
            reasons.append("장바구니 반응은 있으나 구매 전환 미발생")
        elif p == 0 and c == 0 and w >= 1:
            labels.append("저의도 의심")
            reasons.append("위시리스트 중심 반응으로 구매 의도 약함")
        elif p >= 1:
            labels.append("유지")
            reasons.append("구매는 발생하나 확대 판단 전 추가 관찰 필요")
        elif len(q.strip()) <= 2 and t > 0:
            labels.append("관찰 필요")
            reasons.append("짧은 일반 검색어로 의도 확인 필요")
        else:
            labels.append("유지")
            reasons.append("즉시 확대/제외보다 누적 데이터 관찰 권장")

    out["액션 라벨"] = labels
    out["추천 사유"] = reasons
    return out


def _render_top_cards(view: pd.DataFrame, cmp_mode: str):
    q_cnt = int(len(view))
    expand_cnt = int((view["액션 라벨"] == "확대 후보").sum()) if "액션 라벨" in view.columns else 0
    observe_cnt = int((view["액션 라벨"] == "관찰 필요").sum()) if "액션 라벨" in view.columns else 0
    purchase_cnt = int((pd.to_numeric(view.get("구매완료수", 0), errors="coerce").fillna(0) > 0).sum())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        ui_metric_or_stmetric("검색어 수", f"{q_cnt:,}개", "조회 기간 내 실제 검색어")
    with c2:
        ui_metric_or_stmetric("확대 후보", f"{expand_cnt:,}개", "매출/전환 기여가 높은 검색어")
    with c3:
        ui_metric_or_stmetric("관찰 필요", f"{observe_cnt:,}개", "장바구니 반응 중심 검색어")
    with c4:
        ui_metric_or_stmetric("구매 발생", f"{purchase_cnt:,}개", f"{cmp_mode} 기준 증감 태그 포함")


def _render_filter_panel(view: pd.DataFrame) -> pd.DataFrame:
    st.markdown(
        """
        <div class='nv-section nv-section-muted'>
            <div class='nv-section-head'>
                <div>
                    <div class='nv-sec-title'>쇼핑 검색어 필터</div>
                    <div class='nv-sec-sub'>다른 상세 페이지와 동일하게 캠페인/광고그룹/액션 라벨 기준으로 빠르게 좁힐 수 있습니다.</div>
                </div>
            </div>
        """,
        unsafe_allow_html=True,
    )

    filtered = view.copy()
    r1c1, r1c2, r1c3 = st.columns(3)
    camps = ["전체"] + sorted([str(x) for x in filtered["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in filtered.columns else ["전체"]
    sel_camp = r1c1.selectbox("캠페인", camps, key="sq_camp_filter_unified")
    if sel_camp != "전체":
        filtered = filtered[filtered["캠페인"] == sel_camp]

    grps = ["전체"] + sorted([str(x) for x in filtered["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered.columns else ["전체"]
    sel_grp = r1c2.selectbox("광고그룹", grps, key="sq_grp_filter_unified")
    if sel_grp != "전체":
        filtered = filtered[filtered["광고그룹"] == sel_grp]

    labels = ["전체"] + [x for x in ["확대 후보", "유지", "관찰 필요", "저의도 의심"] if x in filtered["액션 라벨"].unique().tolist()]
    sel_label = r1c3.selectbox("액션 라벨", labels, key="sq_label_filter_unified")
    if sel_label != "전체":
        filtered = filtered[filtered["액션 라벨"] == sel_label]

    r2c1, r2c2, r2c3 = st.columns(3)
    only_zero_purchase = r2c1.checkbox("구매 0건만", key="sq_only_zero_purchase_unified")
    only_cart = r2c2.checkbox("장바구니 발생만", key="sq_only_cart_unified")
    q_text = r2c3.text_input("검색어 포함", value="", key="sq_query_contains_unified", placeholder="예: 의자")

    r3c1, r3c2 = st.columns(2)
    min_purchase_sales = r3c1.number_input("최소 구매매출", min_value=0, value=0, step=10000, key="sq_min_purchase_sales_unified")
    min_total_conv = r3c2.number_input("최소 총 전환수", min_value=0, value=0, step=1, key="sq_min_total_conv_unified")

    if only_zero_purchase:
        filtered = filtered[pd.to_numeric(filtered["구매완료수"], errors="coerce").fillna(0) == 0]
    if only_cart:
        filtered = filtered[pd.to_numeric(filtered["장바구니수"], errors="coerce").fillna(0) > 0]
    if q_text.strip():
        filtered = filtered[filtered["실제 검색어"].astype(str).str.contains(q_text.strip(), case=False, na=False)]
    if min_purchase_sales > 0:
        filtered = filtered[pd.to_numeric(filtered["구매완료 매출"], errors="coerce").fillna(0) >= float(min_purchase_sales)]
    if min_total_conv > 0:
        filtered = filtered[pd.to_numeric(filtered["총 전환수"], errors="coerce").fillna(0) >= float(min_total_conv)]

    st.markdown("</div>", unsafe_allow_html=True)
    return filtered


def page_perf_shopping_query(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    st.markdown(
        """
        <div class='nv-section nv-section-muted' style='margin-top:0;'>
            <div class='nv-section-head'>
                <div>
                    <div class='nv-sec-title'>쇼핑 검색어 분석</div>
                    <div class='nv-sec-sub'>다른 성과 분석 페이지와 동일한 카드/섹션/표 스타일로 실제 검색어 기준 퍼널 성과를 확인합니다.</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cids = tuple(f.get("selected_customer_ids", []))
    patch_date = date(2026, 3, 11)
    if f["start"] < patch_date:
        st.info("3월 11일 이전 데이터가 포함되어 있어 퍼널 분리값 일부가 비어 있을 수 있습니다.")

    cmp_mode = st.radio("비교 기준", ["이전 같은 기간 대비", "전주대비", "전일대비"], horizontal=True, key="sq_cmp_mode_unified")
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    with st.spinner("쇼핑 검색어 데이터를 불러오는 중입니다..."):
        df_cur = query_shopping_search_terms(engine, f["start"], f["end"], cids)
        if df_cur.empty:
            _empty_notice("해당 기간에 수집된 쇼핑 검색어 전환 데이터가 없습니다.")
            return
        df_prev = query_shopping_search_terms(engine, b1, b2, cids)
        df_cur = _perf_common_merge_meta(df_cur, meta)
        if not df_prev.empty:
            df_prev = _perf_common_merge_meta(df_prev, meta)
        df = _merge_compare(df_cur, df_prev)

    view = df.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "query_text": "실제 검색어",
        "purchase_conv": "구매완료수",
        "purchase_sales": "구매완료 매출",
        "cart_conv": "장바구니수",
        "cart_sales": "장바구니 매출액",
        "wishlist_conv": "위시리스트수",
        "wishlist_sales": "위시리스트 매출액",
        "total_conv": "총 전환수",
        "total_sales": "총 전환매출",
    }).copy()

    numeric_cols = [
        "구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액",
        "위시리스트수", "위시리스트 매출액", "총 전환수", "총 전환매출",
        "구매완료수 증감", "구매완료 매출 증감", "총 전환수 증감", "총 전환매출 증감",
    ]
    view = _to_num(view, numeric_cols)
    view = _add_funnel_metrics(view)
    view = _add_action_labels(view)

    _render_top_cards(view, cmp_mode)
    filtered = _render_filter_panel(view)

    display_cols = [
        "업체명", "캠페인", "광고그룹", "실제 검색어", "액션 라벨", "변화 태그", "추천 사유",
        "구매완료수", "구매완료 매출", "장바구니수", "위시리스트수", "총 전환수", "총 전환매출",
        "구매기여율(%)", "장바구니기여율(%)", "구매완료수 증감", "구매완료 매출 증감", "총 전환수 증감",
    ]
    disp = filtered[[c for c in display_cols if c in filtered.columns]].sort_values(["구매완료 매출", "총 전환매출"], ascending=False).head(500).copy()

    top_expand = disp[disp.get("액션 라벨", "") == "확대 후보"].head(100).copy() if "액션 라벨" in disp.columns else pd.DataFrame()
    top_observe = disp[disp.get("액션 라벨", "") == "관찰 필요"].head(100).copy() if "액션 라벨" in disp.columns else pd.DataFrame()

    tabs = st.tabs(["전체 검색어", "확대 후보", "관찰 필요"])

    with tabs[0]:
        st.markdown(
            "<div class='nv-section'><div class='nv-section-head'><div><div class='nv-sec-title'>검색어별 퍼널 성과</div><div class='nv-sec-sub'>다른 상세 분석과 동일하게 표 중심으로 상위 500개를 보여줍니다.</div></div></div>",
            unsafe_allow_html=True,
        )
        if disp.empty:
            _empty_notice("조건에 맞는 검색어가 없습니다.")
        else:
            render_big_table(disp.style.format(FMT_DICT), "shopping_query_table_unified", 620)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[1]:
        st.markdown(
            "<div class='nv-section'><div class='nv-section-head'><div><div class='nv-sec-title'>확대 후보</div><div class='nv-sec-sub'>구매 발생 + 매출/전환 기여가 높은 검색어만 모아봅니다.</div></div></div>",
            unsafe_allow_html=True,
        )
        if top_expand.empty:
            _empty_notice("확대 후보 검색어가 없습니다.")
        else:
            render_big_table(top_expand.style.format(FMT_DICT), "shopping_query_expand_unified", 420)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[2]:
        st.markdown(
            "<div class='nv-section'><div class='nv-section-head'><div><div class='nv-sec-title'>관찰 필요</div><div class='nv-sec-sub'>장바구니 반응은 있지만 구매로 이어지지 않은 검색어입니다.</div></div></div>",
            unsafe_allow_html=True,
        )
        if top_observe.empty:
            _empty_notice("관찰 필요 검색어가 없습니다.")
        else:
            render_big_table(top_observe.style.format(FMT_DICT), "shopping_query_observe_unified", 420)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        "<div class='nv-section'><div class='nv-section-head'><div><div class='nv-sec-title'>리포트 다운로드</div><div class='nv-sec-sub'>현재 필터 기준 결과를 엑셀로 내려받습니다.</div></div></div>",
        unsafe_allow_html=True,
    )
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        disp.to_excel(writer, sheet_name="쇼핑_검색어_성과", index=False)
    st.download_button(
        label="검색어 리포트 다운로드 (Excel)",
        data=excel_buffer.getvalue(),
        file_name=f"쇼핑_검색어_리포트_{f['start']}_{f['end']}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)
