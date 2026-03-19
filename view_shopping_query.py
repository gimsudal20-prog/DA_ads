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

def page_perf_shopping_query(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    
    st.markdown("<div class='nv-sec-title'>쇼핑 검색어 상세 분석 (Query Text)</div>", unsafe_allow_html=True)
    st.caption("고객이 네이버 쇼핑에서 **실제로 검색한 단어** 기준의 전환 성과(장바구니/구매)를 분석합니다. (비용/클릭 지표는 포함되지 않습니다.)")

    cids = tuple(f.get("selected_customer_ids", []))
    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)
    
    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터는 네이버의 퍼널 분리 패치 이전이므로 '통합 전환' 지표만 존재할 수 있습니다.")

    with st.spinner("🔄 쇼핑 검색어 데이터를 불러오는 중입니다..."):
        df = query_shopping_search_terms(engine, f["start"], f["end"], cids)
        
        if df.empty:
            st.warning("해당 기간에 수집된 쇼핑 검색어 전환 데이터가 없습니다.")
            return
            
        df = _perf_common_merge_meta(df, meta)
        
        # 컬럼 한글화
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자", 
            "campaign_name": "캠페인", "adgroup_name": "광고그룹", "query_text": "실제 검색어",
            "purchase_conv": "구매완료수", "purchase_sales": "구매완료 매출",
            "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
            "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액",
            "total_conv": "총 전환수", "total_sales": "총 전환매출"
        }).copy()

        # 결측치 0 처리
        for c in ["구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액", "위시리스트수", "위시리스트 매출액", "총 전환수", "총 전환매출"]:
            if c in view.columns:
                view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    # 상단 필터
    col_camp, col_grp = st.columns(2)
    camps = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp = col_camp.selectbox("캠페인 필터", camps, key="sq_camp_filter")
    if sel_camp != "전체": view = view[view["캠페인"] == sel_camp]
    
    grps = ["전체"] + sorted([str(x) for x in view["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in view.columns else ["전체"]
    sel_grp = col_grp.selectbox("광고그룹 필터", grps, key="sq_grp_filter")
    if sel_grp != "전체": view = view[view["광고그룹"] == sel_grp]

    # 포맷 지정 (소수점 1자리 통일)
    fmt = {
        "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원",
        "장바구니수": "{:,.1f}", "장바구니 매출액": "{:,.0f}원",
        "위시리스트수": "{:,.1f}", "위시리스트 매출액": "{:,.0f}원",
        "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원"
    }

    display_cols = [
        "업체명", "캠페인", "광고그룹", "실제 검색어",
        "구매완료수", "구매완료 매출", "장바구니수", "위시리스트수", "총 전환수", "총 전환매출"
    ]
    
    disp = view[[c for c in display_cols if c in view.columns]].sort_values("구매완료 매출", ascending=False).head(500)
    
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>검색어별 퍼널(Funnel) 성과</div>", unsafe_allow_html=True)
    st.dataframe(disp.style.format(fmt), use_container_width=True, height=600, hide_index=True)

    # 엑셀 다운로드
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        disp.to_excel(writer, sheet_name='쇼핑_검색어_성과', index=False)
    st.download_button(
        label="📥 검색어 리포트 다운로드 (Excel)", 
        data=excel_buffer.getvalue(), 
        file_name=f"쇼핑_검색어_리포트_{f['start']}_{f['end']}.xlsx", 
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
