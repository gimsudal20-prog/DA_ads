# -*- coding: utf-8 -*-
"""view_media.py - collected media/device analysis (unified UI/UX)."""

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
    return f"AND COALESCE({col_name}, '') IN ({_sql_in_str_list(type_vals)})"

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

def _query_media_region(engine, f) -> pd.DataFrame:
    if not table_exists(engine, 'fact_media_daily'):
        return pd.DataFrame()
    cols = get_table_columns(engine, 'fact_media_daily')
    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    where_cid = f"AND customer_id::text IN ({_sql_in_str_list(list(tuple(f.get('selected_customer_ids', []) or ())))})" if tuple(f.get('selected_customer_ids', []) or ()) else ''
    type_sql = _type_filter_sql(type_vals) if 'campaign_type' in cols else ''
    params = {'d1': str(f['start']), 'd2': str(f['end'])}
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(media_name), ''), '전체') AS "매체이름",
            COALESCE(NULLIF(TRIM(region_name), ''), '전체') AS "지역명",
            COALESCE(NULLIF(TRIM(device_name), ''), '기타') AS "기기명",
            SUM(COALESCE(imp,0)) AS "노출수",
            SUM(COALESCE(clk,0)) AS "클릭수",
            SUM(COALESCE(cost,0)) AS "광고비",
            SUM(COALESCE(conv,0)) AS "전환수",
            SUM(COALESCE(sales,0)) AS "전환매출"
        FROM fact_media_daily
        WHERE dt BETWEEN :d1 AND :d2 {where_cid} {type_sql}
        GROUP BY 1,2,3
    """
    try:
        return sql_read(engine, sql, params)
    except Exception:
        return pd.DataFrame()

def _query_device(engine, f) -> pd.DataFrame:
    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    cids = tuple(f.get('selected_customer_ids', []) or ())
    params = {'d1': str(f['start']), 'd2': str(f['end'])}

    if table_exists(engine, 'fact_campaign_device_daily'):
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ''
        join_sql = ''
        type_sql = ''
        if type_vals:
            join_sql = ' LEFT JOIN dim_campaign c ON f.customer_id::text = c.customer_id::text AND f.campaign_id::text = c.campaign_id::text '
            type_sql = f"AND (COALESCE(c.campaign_tp::text,'') IN ({_sql_in_str_list(type_vals)}) OR (CASE WHEN COALESCE(c.campaign_tp::text,'') = 'WEB_SITE' THEN '파워링크' WHEN COALESCE(c.campaign_tp::text,'') = 'SHOPPING' THEN '쇼핑검색' WHEN COALESCE(c.campaign_tp::text,'') = 'POWER_CONTENTS' THEN '파워컨텐츠' WHEN COALESCE(c.campaign_tp::text,'') = 'BRAND_SEARCH' THEN '브랜드검색' WHEN COALESCE(c.campaign_tp::text,'') = 'PLACE' THEN '플레이스' ELSE COALESCE(c.campaign_tp::text,'') END) IN ({_sql_in_str_list(type_vals)}))"
        sql = f"""
            SELECT
                COALESCE(NULLIF(TRIM(f.device_name), ''), '기타') AS "기기명",
                SUM(COALESCE(f.imp,0)) AS "노출수",
                SUM(COALESCE(f.clk,0)) AS "클릭수",
                SUM(COALESCE(f.cost,0)) AS "광고비",
                SUM(COALESCE(f.conv,0)) AS "전환수",
                SUM(COALESCE(f.sales,0)) AS "전환매출"
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

    st.markdown("<div class='nv-sec-title'>매체 / 지역 / 기기 효율 분석</div>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:13px; color:#6B7280; margin-bottom:16px;'>수집된 성과 테이블 기준으로 조회합니다. 기기는 자동 수집 데이터를 우선 사용하고, 매체/지역은 적재된 원천 데이터가 있을 때 표시합니다.</div>", unsafe_allow_html=True)

    with st.spinner("🔄 매체 및 기기 성과 데이터를 집계하고 있습니다..."):
        media_region_df = _query_media_region(engine, f)
        device_df = _query_device(engine, f)

    if (media_region_df is None or media_region_df.empty) and (device_df is None or device_df.empty):
        st.warning('자동 수집된 매체/기기 데이터가 없습니다. 현재 프로젝트 기준으로 기기 데이터부터 자동 반영됩니다.')
        return

    df_media = _calc_metrics(media_region_df.groupby('매체이름', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()) if media_region_df is not None and not media_region_df.empty else pd.DataFrame()
    df_region = _calc_metrics(media_region_df.groupby('지역명', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()) if media_region_df is not None and not media_region_df.empty else pd.DataFrame()
    if device_df is not None and not device_df.empty:
        device_df['기기명'] = device_df['기기명'].apply(_normalize_device_value)
        device_df = device_df.groupby('기기명', as_index=False)[['노출수', '클릭수', '광고비', '전환수', '전환매출']].sum()
        device_df['ord'] = device_df['기기명'].map({k: i for i, k in enumerate(_DEVICE_ORDER)}).fillna(99)
        df_device = _calc_metrics(device_df).sort_values(['ord', '광고비'], ascending=[True, False]).drop(columns=['ord'], errors='ignore').reset_index(drop=True)
    else:
        df_device = pd.DataFrame()

    selected_tab = st.pills("분석 탭 선택", ["지면(매체)", "지역", "기기", "비용 누수 항목"], default="지면(매체)")

    if selected_tab == "지면(매체)":
        with st.container(border=True):
            st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>조회 기간 내 전체 매체(지면) 효율 리스트</div>", unsafe_allow_html=True)
            if not df_media.empty:
                st.dataframe(df_media, use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
            else:
                st.info('현재 자동 수집 경로에는 지면 원천이 없어서 매체 리스트가 비어 있습니다. 기기 데이터 탭을 확인해 주세요.')

    elif selected_tab == "지역":
        with st.container(border=True):
            st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>조회 기간 내 지역별 성과 리스트</div>", unsafe_allow_html=True)
            if not df_region.empty:
                df_region_clean = df_region[~df_region['지역명'].isin(['전체', '-', '알수없음'])].copy()
                if not df_region_clean.empty:
                    st.dataframe(df_region_clean, use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
                else:
                    st.info('의미 있는 지역 구분 데이터가 없습니다.')
            else:
                st.info('현재 자동 수집 경로에는 지역 원천이 없습니다.')

    elif selected_tab == "기기":
        with st.container(border=True):
            st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>기기별 성과 리스트</div>", unsafe_allow_html=True)
            if not df_device.empty:
                st.dataframe(df_device, use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
            else:
                st.info('기기 자동 수집 데이터가 없습니다.')

    elif selected_tab == "비용 누수 항목":
        st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
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

        with col2:
            with st.container(border=True):
                st.markdown("<div style='font-size:14px;font-weight:700;margin-bottom:12px;'>1만 원 이상 소진 지역 (전환 0건)</div>", unsafe_allow_html=True)
                if not df_region.empty:
                    bad_r = df_region[(df_region['전환수'] == 0) & (df_region['광고비'] >= 10000) & (~df_region['지역명'].isin(['전체', '-', '알수없음']))].sort_values('광고비', ascending=False)
                    if not bad_r.empty:
                        st.dataframe(bad_r[['지역명', '광고비', '클릭수', 'CTR(%)']], use_container_width=True, hide_index=True, column_config=FAST_COL_CONFIG)
                    else:
                        st.success('비용 누수 지역이 없습니다!')
                else:
                    st.info('지역 수집 데이터가 없어 계산할 수 없습니다.')
