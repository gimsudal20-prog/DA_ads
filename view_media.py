# -*- coding: utf-8 -*-
"""view_media.py - collected media/device analysis (Region tab removed)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data import sql_read, table_exists, get_table_columns, _sql_in_str_list

_DEVICE_ORDER = ["PC", "MO", "기타"]

FAST_COL_CONFIG = {
    "노출수": st.column_config.NumberColumn("노출수", format="%d"),
    "클릭수": st.column_config.NumberColumn("클릭수", format="%d"),
    "광고비": st.column_config.NumberColumn("광고비", format="%d 원"),
    "전환수": st.column_config.NumberColumn("전환수", format="%.1f"),
    "전환매출": st.column_config.NumberColumn("전환매출", format="%d 원"),
    "ROAS(%)": st.column_config.NumberColumn("ROAS(%)", format="%.2f %%"),
    "CPA(원)": st.column_config.NumberColumn("CPA(원)", format="%d 원"),
    "CTR(%)": st.column_config.NumberColumn("CTR(%)", format="%.2f %%"),
}

# =====================================================================
# 네이버 매체 코드 -> 실제 지면명 매핑 딕셔너리
# =====================================================================
NAVER_MEDIA_MAP = {
    "8753": "네이버 통합검색 (PC)",
    "27758": "네이버 통합검색 (모바일)",
    "8754": "네이버 검색탭 (PC)",
    "27759": "네이버 검색탭 (모바일)",
    "8755": "네이버 지식iN (PC)",
    "27760": "네이버 지식iN (모바일)",
    "8756": "네이버 블로그 (PC)",
    "27761": "네이버 블로그 (모바일)",
    "8757": "네이버 카페 (PC)",
    "27762": "네이버 카페 (모바일)",
    "8768": "네이버 쇼핑검색 (PC)",
    "27771": "네이버 쇼핑검색 (모바일)",
    "27772": "네이버 쇼핑 (모바일)",
    "8769": "네이버 쇼핑 (PC)",
}

def _map_media_name(name_or_code: object) -> str:
    """매체 코드(숫자)를 한글 지면명으로 변환. 딕셔너리에 없으면 코드 그대로 반환"""
    s = str(name_or_code or "").strip()
    if not s or s.lower() in {"nan", "none", "전체"}:
        return "전체"
    
    # 딕셔너리에 매핑된 코드가 있다면 변환
    if s in NAVER_MEDIA_MAP:
        return NAVER_MEDIA_MAP[s]
        
    # 만약 순수 숫자(미등록 매체코드)라면 쉽게 추가할 수 있도록 코드 표출
    if s.isdigit():
        return f"알수없는 지면 (코드: {s})"
        
    return s

def _normalize_device_value(v: object) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none", "전체"}:
        return "기타"
    us = s.upper()
    if s in {"모바일", "MOBILE"} or "MOBILE" in us or "APP" in us or "PHONE" in us or s == "MO":
        return "MO"
    if s == "PC" or "DESKTOP" in us or "WEB" in us or "웹" in s:
        return "PC"
    return s

def _type_filter_sql(type_vals: list[str], col_name: str = "campaign_type") -> str:
    if not type_vals:
        return ""
    return f"AND COALESCE(CAST({col_name} AS TEXT), '') IN ({_sql_in_str_list(type_vals)})"

def _expand_campaign_type_values(type_sel: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    mapping = {
        '파워링크': ['파워링크', 'WEB_SITE', 'website', 'web_site'],
        '쇼핑검색': ['쇼핑검색', 'SHOPPING', 'shopping', 'SHOPPING_SEARCH', 'shopping_search'],
        '파워컨텐츠': ['파워컨텐츠', 'POWER_CONTENTS', 'power_contents'],
        '브랜드검색': ['브랜드검색', 'BRAND_SEARCH', 'brand_search'],
        '플레이스': ['플레이스', 'PLACE', 'place'],
    }
    for x in (type_sel or ()): 
        x = str(x).strip()
        if not x:
            continue
        vals = mapping.get(x, [x])
        out.extend(vals)
    seen, dedup = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


def _first_existing(cols: list[str], candidates: list[str]) -> str | None:
    colset = {str(c).lower(): c for c in cols}
    for name in candidates:
        if name.lower() in colset:
            return colset[name.lower()]
    return None


def _safe_text_dim_expr(cols: list[str], candidates: list[str], default: str) -> str:
    col = _first_existing(cols, candidates)
    if not col:
        return f"'{default}'"
    return f"COALESCE(NULLIF(TRIM(CAST({col} AS TEXT)), ''), '{default}')"

def _query_media_region(engine, f) -> pd.DataFrame:
    if not table_exists(engine, 'fact_media_daily'):
        return pd.DataFrame()

    cols = get_table_columns(engine, 'fact_media_daily')

    imp_expr = "COALESCE(imp, 0)" if "imp" in cols else "0"
    clk_expr = "COALESCE(clk, 0)" if "clk" in cols else "0"
    cost_expr = "COALESCE(cost, 0)" if "cost" in cols else "0"

    if "tot_conv" in cols and "tot_sales" in cols:
        conv_expr, sales_expr = "COALESCE(tot_conv, 0)", "COALESCE(tot_sales, 0)"
    elif "purchase_conv" in cols and "purchase_sales" in cols:
        conv_expr, sales_expr = "COALESCE(purchase_conv, 0)", "COALESCE(purchase_sales, 0)"
    elif "conv" in cols and "sales" in cols:
        conv_expr, sales_expr = "COALESCE(conv, 0)", "COALESCE(sales, 0)"
    else:
        conv_expr, sales_expr = "0", "0"

    media_expr = _safe_text_dim_expr(
        cols,
        ["media_name", "placement_name", "media_code", "placement_code", "media_tp", "placement_tp"],
        "전체",
    )
    device_expr = _safe_text_dim_expr(
        cols,
        ["device_name", "device", "device_tp", "device_type", "platform"],
        "기타",
    )

    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    cids = tuple(f.get('selected_customer_ids', []) or ())
    where_cid = f"AND CAST(customer_id AS TEXT) IN ({_sql_in_str_list(list(cids))})" if cids else ''

    cp_col = "campaign_type"
    if "campaign_tp" in cols:
        cp_col = "campaign_tp"
    elif "campaign_type_label" in cols:
        cp_col = "campaign_type_label"

    type_sql = _type_filter_sql(type_vals, cp_col) if cp_col in cols else ''
    params = {'d1': str(f['start']), 'd2': str(f['end'])}

    sql = f"""
        SELECT
            {media_expr} AS "매체이름",
            {device_expr} AS "기기명",
            SUM({imp_expr}) AS "노출수",
            SUM({clk_expr}) AS "클릭수",
            SUM({cost_expr}) AS "광고비",
            SUM({conv_expr}) AS "전환수",
            SUM({sales_expr}) AS "전환매출"
        FROM fact_media_daily
        WHERE dt BETWEEN :d1 AND :d2 {where_cid} {type_sql}
        GROUP BY 1,2
    """
    try:
        df = sql_read(engine, sql, params)
        if not df.empty:
            df['매체이름'] = df['매체이름'].apply(_map_media_name)
        return df
    except Exception:
        return pd.DataFrame()

def _query_device(engine, f) -> pd.DataFrame:
    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    cids = tuple(f.get('selected_customer_ids', []) or ())
    params = {'d1': str(f['start']), 'd2': str(f['end'])}

    if table_exists(engine, 'fact_campaign_device_daily'):
        cols = get_table_columns(engine, 'fact_campaign_device_daily')
        
        imp_expr = "COALESCE(f.imp, 0)" if "imp" in cols else "0"
        clk_expr = "COALESCE(f.clk, 0)" if "clk" in cols else "0"
        cost_expr = "COALESCE(f.cost, 0)" if "cost" in cols else "0"

        if "tot_conv" in cols and "tot_sales" in cols:
            conv_expr, sales_expr = "COALESCE(f.tot_conv, 0)", "COALESCE(f.tot_sales, 0)"
        elif "purchase_conv" in cols and "purchase_sales" in cols:
            conv_expr, sales_expr = "COALESCE(f.purchase_conv, 0)", "COALESCE(f.purchase_sales, 0)"
        elif "conv" in cols and "sales" in cols:
            conv_expr, sales_expr = "COALESCE(f.conv, 0)", "COALESCE(f.sales, 0)"
        else:
            conv_expr, sales_expr = "0", "0"

        where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(list(cids))})" if cids else ''
        join_sql = ''
        type_sql = ''
        if type_vals:
            join_sql = ' LEFT JOIN dim_campaign c ON CAST(f.customer_id AS TEXT) = CAST(c.customer_id AS TEXT) AND CAST(f.campaign_id AS TEXT) = CAST(c.campaign_id AS TEXT) '
            type_sql = f"AND (COALESCE(CAST(c.campaign_tp AS TEXT),'') IN ({_sql_in_str_list(type_vals)}) OR (CASE WHEN COALESCE(CAST(c.campaign_tp AS TEXT),'') = 'WEB_SITE' THEN '파워링크' WHEN COALESCE(CAST(c.campaign_tp AS TEXT),'') = 'SHOPPING' THEN '쇼핑검색' WHEN COALESCE(CAST(c.campaign_tp AS TEXT),'') = 'POWER_CONTENTS' THEN '파워컨텐츠' WHEN COALESCE(CAST(c.campaign_tp AS TEXT),'') = 'BRAND_SEARCH' THEN '브랜드검색' WHEN COALESCE(CAST(c.campaign_tp AS TEXT),'') = 'PLACE' THEN '플레이스' ELSE COALESCE(CAST(c.campaign_tp AS TEXT),'') END) IN ({_sql_in_str_list(type_vals)}))"
        
        device_expr = _safe_text_dim_expr(
            cols,
            ['device_name', 'device', 'device_tp', 'device_type', 'platform'],
            '기타',
        )

        sql = f"""
            SELECT
                {device_expr} AS "기기명",
                SUM({imp_expr}) AS "노출수",
                SUM({clk_expr}) AS "클릭수",
                SUM({cost_expr}) AS "광고비",
                SUM({conv_expr}) AS "전환수",
                SUM({sales_expr}) AS "전환매출"
            FROM fact_campaign_device_daily f
            {join_sql}
            WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_sql}
            GROUP BY 1
        """
        try:
            out = sql_read(engine, sql, params)
            if out is not None and not out.empty:
                return out
        except Exception:
            pass

    media_df = _query_media_region(engine, f)
    if media_df.empty:
        return pd.DataFrame()
    return media_df.groupby('기기명', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()

def _calc_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for c in ['노출수', '클릭수', '광고비', '전환수', '전환매출']:
        out[c] = pd.to_numeric(out[c], errors='coerce').fillna(0)
    out['ROAS(%)'] = np.where(out['광고비'] > 0, (out['전환매출'] / out['광고비']) * 100, 0.0)
    out['CPA(원)'] = np.where(out['전환수'] > 0, out['광고비'] / out['전환수'], 0.0)
    out['CTR(%)'] = np.where(out['노출수'] > 0, (out['클릭수'] / out['노출수']) * 100, 0.0)
    return out.sort_values('광고비', ascending=False).reset_index(drop=True)

@st.fragment
def page_media(engine, f):
    if not f.get("ready", False): return

    st.markdown("<div class='nv-sec-title'>매체 / 기기 효율 분석</div>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:13px; color:#6B7280; margin-bottom:16px;'>수집된 성과 테이블 기준으로 조회합니다. no_header 리포트로 수집된 매체 코드(숫자)는 자동으로 한글 지면명으로 변환되어 합산 표출됩니다.</div>", unsafe_allow_html=True)

    with st.spinner("🔄 매체 및 기기 성과 데이터를 집계하고 있습니다..."):
        media_region_df = _query_media_region(engine, f)
        device_df = _query_device(engine, f)

    if (media_region_df is None or media_region_df.empty) and (device_df is None or device_df.empty):
        st.warning('자동 수집된 매체/기기 데이터가 없습니다. 현재 프로젝트 기준으로 기기 데이터부터 자동 반영됩니다.')
        return

    df_media = _calc_metrics(media_region_df.groupby('매체이름', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()) if media_region_df is not None and not media_region_df.empty else pd.DataFrame()
    
    if device_df is not None and not device_df.empty:
        device_df['기기명'] = device_df['기기명'].apply(_normalize_device_value)
        device_df = device_df.groupby('기기명', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()
        device_df['ord'] = device_df['기기명'].map({k: i for i, k in enumerate(_DEVICE_ORDER)}).fillna(99)
        df_device = _calc_metrics(device_df).sort_values(['ord', '광고비'], ascending=[True, False]).drop(columns=['ord'], errors='ignore').reset_index(drop=True)
    else:
        df_device = pd.DataFrame()

    selected_tab = st.pills("분석 탭 선택", ["지면(매체)", "기기", "비용 누수 항목"], default="지면(매체)")

    if selected_tab == "지면(매체)":
        with st.container(border=True):
            st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>조회 기간 내 전체 매체(지면) 효율 리스트</div>", unsafe_allow_html=True)
            if not df_media.empty:
                st.dataframe(df_media, use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
            else:
                st.info('현재 자동 수집 경로에 지면 원천이 없어서 매체 리스트가 비어 있습니다. 기기 데이터 탭을 확인해 주세요.')

    elif selected_tab == "기기":
        with st.container(border=True):
            st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>기기별 성과 리스트</div>", unsafe_allow_html=True)
            if not df_device.empty:
                st.dataframe(df_device, use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
            else:
                st.info('기기 자동 수집 데이터가 없습니다.')

    elif selected_tab == "비용 누수 항목":
        st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown("<div style='font-size:14px;font-weight:700;margin-bottom:12px;'>1만 원 이상 소진 매체 (전환 0건)</div>", unsafe_allow_html=True)
            if not df_media.empty:
                bad_m = df_media[(df_media['전환수'] == 0) & (df_media['광고비'] >= 10000)].sort_values('광고비', ascending=False)
                if not bad_m.empty:
                    st.dataframe(bad_m[['매체이름', '광고비', '클릭수', 'CTR(%)']], use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
                else:
                    st.success('비용 누수 매체가 없습니다!')
            else:
                st.info('매체 수집 데이터가 없어 계산할 수 없습니다.')
