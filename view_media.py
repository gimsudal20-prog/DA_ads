# -*- coding: utf-8 -*-
"""view_media.py - collected media/device analysis (no manual CSV upload)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data import sql_read, table_exists, get_meta, get_table_columns, _sql_in_str_list
from ui import render_big_table


_DEVICE_ORDER = ["PC", "MO", "기타"]


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


def page_media(engine, f):
    st.markdown(
        """
        <div class='nv-section nv-section-muted' style='margin-top:0;'>
            <div class='nv-sec-title'>매체 / 지역 / 기기 효율 분석</div>
            <div class='nv-sec-sub'>수집된 성과 테이블 기준으로 조회합니다. 기기는 자동 수집 데이터를 우선 사용하고, 매체/지역은 적재된 fact_media_daily가 있을 때 표시합니다.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

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

    fmt = {
        '노출수': '{:,.0f}',
        '클릭수': '{:,.0f}',
        '광고비': '{:,.0f}',
        '전환수': '{:,.1f}',
        '전환매출': '{:,.0f}',
        'ROAS(%)': '{:,.2f}%',
        'CPA(원)': '{:,.0f}',
        'CTR(%)': '{:,.2f}%',
    }

    tabs = st.tabs(['지면(매체)', '지역', '기기', '비용 누수 항목'])

    with tabs[0]:
        st.markdown("<div class='nv-section-head'><div><div class='nv-sec-title'>조회 기간 내 전체 매체(지면) 효율 리스트</div><div class='nv-sec-sub'>자동 적재된 fact_media_daily가 있을 때 표시됩니다.</div></div></div>", unsafe_allow_html=True)
        if not df_media.empty:
            render_big_table(df_media.style.format(fmt), 'media_table_main', 600)
        else:
            st.info('현재 자동 수집 경로에는 지면/지역 원천이 없어서 매체 리스트가 비어 있습니다. 기기 데이터는 아래 탭에서 확인할 수 있습니다.')

    with tabs[1]:
        st.markdown("<div class='nv-section-head'><div><div class='nv-sec-title'>조회 기간 내 지역별 성과 리스트</div><div class='nv-sec-sub'>자동 적재된 fact_media_daily가 있을 때 표시됩니다.</div></div></div>", unsafe_allow_html=True)
        if not df_region.empty:
            df_region_clean = df_region[~df_region['지역명'].isin(['전체', '-', '알수없음'])].copy()
            if not df_region_clean.empty:
                render_big_table(df_region_clean.style.format(fmt), 'region_table_main', 600)
            else:
                st.info('지역 구분 데이터가 없습니다.')
        else:
            st.info('현재 자동 수집 경로에는 지역 원천이 없습니다.')

    with tabs[2]:
        st.markdown("<div class='nv-section-head'><div><div class='nv-sec-title'>기기별 성과 리스트</div><div class='nv-sec-sub'>fact_campaign_device_daily 자동 수집 데이터를 우선 사용합니다.</div></div></div>", unsafe_allow_html=True)
        if not df_device.empty:
            render_big_table(df_device.style.format(fmt), 'device_table_main', 520)
        else:
            st.info('기기 자동 수집 데이터가 없습니다.')

    with tabs[3]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<div class='nv-sec-title' style='font-size:14px;'>1만 원 이상 소진 매체 (전환 0건)</div>", unsafe_allow_html=True)
            if not df_media.empty:
                bad_m = df_media[(df_media['전환수'] == 0) & (df_media['광고비'] >= 10000)].sort_values('광고비', ascending=False)
                if not bad_m.empty:
                    render_big_table(
                        bad_m[['매체이름', '광고비', '클릭수', 'CTR(%)']].style.format(fmt),
                        'media_leak_table_main',
                        360,
                    )
                else:
                    st.success('비용 누수 매체가 없습니다!')
            else:
                st.info('매체 자동 수집 데이터가 없어 계산할 수 없습니다.')

        with col2:
            st.markdown("<div class='nv-sec-title' style='font-size:14px;'>1만 원 이상 소진 지역 (전환 0건)</div>", unsafe_allow_html=True)
            if not df_region.empty:
                bad_r = df_region[(df_region['전환수'] == 0) & (df_region['광고비'] >= 10000) & (~df_region['지역명'].isin(['전체', '-', '알수없음']))].sort_values('광고비', ascending=False)
                if not bad_r.empty:
                    render_big_table(
                        bad_r[['지역명', '광고비', '클릭수', 'CTR(%)']].style.format(fmt),
                        'region_leak_table_main',
                        360,
                    )
                else:
                    st.success('비용 누수 지역이 없습니다!')
            else:
                st.info('지역 자동 수집 데이터가 없어 계산할 수 없습니다.')
