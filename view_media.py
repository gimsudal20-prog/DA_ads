# -*- coding: utf-8 -*-
"""view_media.py - collected media/device analysis (minimal UI)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data import sql_read, table_exists, get_table_columns, _sql_in_str_list
from ui import render_big_table

_DEVICE_ORDER = ["PC", "MO", "기타"]


def _inject_minimal_css() -> None:
    st.markdown(
        """
        <style>
        .mini-hero{padding:10px 2px 2px 2px;margin-bottom:6px;}
        .mini-hero-title{font-size:19px;font-weight:800;letter-spacing:-0.02em;color:#111827;margin:0;}
        .mini-hero-sub{font-size:12px;color:#6b7280;margin-top:4px;line-height:1.45;}
        .mini-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:8px 0 14px 0;}
        .mini-card{border:1px solid rgba(17,24,39,.08);border-radius:12px;padding:12px 14px;background:#fff;}
        .mini-label{font-size:11px;color:#6b7280;margin-bottom:6px;}
        .mini-value{font-size:20px;line-height:1.1;font-weight:800;color:#111827;letter-spacing:-0.03em;}
        .mini-desc{font-size:11px;color:#9ca3af;margin-top:5px;}
        .mini-section{margin:10px 0 6px 0;}
        .mini-section-title{font-size:14px;font-weight:800;color:#111827;letter-spacing:-0.02em;}
        .mini-section-sub{font-size:11px;color:#6b7280;margin-top:3px;}
        @media (max-width: 900px){ .mini-grid{grid-template-columns:repeat(2,minmax(0,1fr));} }
        </style>
        """,
        unsafe_allow_html=True,
    )


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


def _render_compact_cards(df_media: pd.DataFrame, df_region: pd.DataFrame, df_device: pd.DataFrame) -> None:
    total_cost = float(pd.to_numeric(df_media.get('광고비', 0), errors='coerce').fillna(0).sum()) if not df_media.empty else 0.0
    total_conv = float(pd.to_numeric(df_media.get('전환수', 0), errors='coerce').fillna(0).sum()) if not df_media.empty else 0.0
    media_cnt = int(df_media['매체이름'].nunique()) if not df_media.empty and '매체이름' in df_media.columns else 0
    device_cnt = int(df_device['기기명'].nunique()) if not df_device.empty and '기기명' in df_device.columns else 0
    region_cnt = int(df_region['지역명'].nunique()) if not df_region.empty and '지역명' in df_region.columns else 0
    roas = 0.0
    if not df_media.empty:
        sales = float(pd.to_numeric(df_media.get('전환매출', 0), errors='coerce').fillna(0).sum())
        roas = (sales / total_cost * 100.0) if total_cost > 0 else 0.0
    st.markdown(
        f"""
        <div class="mini-grid">
          <div class="mini-card"><div class="mini-label">광고비</div><div class="mini-value">{total_cost:,.0f}</div><div class="mini-desc">조회 기간 합계</div></div>
          <div class="mini-card"><div class="mini-label">전환수</div><div class="mini-value">{total_conv:,.0f}</div><div class="mini-desc">매체 기준 합계</div></div>
          <div class="mini-card"><div class="mini-label">ROAS</div><div class="mini-value">{roas:,.1f}%</div><div class="mini-desc">전환매출 / 광고비</div></div>
          <div class="mini-card"><div class="mini-label">분석 범위</div><div class="mini-value">{media_cnt} / {region_cnt} / {device_cnt}</div><div class="mini-desc">매체 · 지역 · 기기</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_section(title: str, sub: str) -> None:
    st.markdown(
        f"<div class='mini-section'><div class='mini-section-title'>{title}</div><div class='mini-section-sub'>{sub}</div></div>",
        unsafe_allow_html=True,
    )


def page_media(engine, f):
    _inject_minimal_css()
    st.markdown(
        """
        <div class='mini-hero'>
            <div class='mini-hero-title'>매체 · 지면 분석</div>
            <div class='mini-hero-sub'>수집된 성과 테이블 기준으로 조회합니다. 과한 카드 대신 핵심 수치와 표 위주로 정리했습니다.</div>
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

    _render_compact_cards(
        media_region_df if media_region_df is not None else pd.DataFrame(),
        media_region_df if media_region_df is not None else pd.DataFrame(),
        device_df if device_df is not None else pd.DataFrame(),
    )

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

    tab_media, tab_region, tab_device, tab_leak = st.tabs(['지면', '지역', '기기', '점검'])

    with tab_media:
        _render_section('지면별 성과', '수집된 지면 데이터를 광고비 기준으로 정렬합니다.')
        if not df_media.empty:
            render_big_table(df_media.style.format(fmt), 'media_table_minimal', 560)
        else:
            st.info('현재 자동 수집 경로에는 지면 원천이 없어 빈 상태입니다.')

    with tab_region:
        _render_section('지역별 성과', '지역 데이터가 있을 때만 노출합니다.')
        if not df_region.empty:
            df_region_clean = df_region[~df_region['지역명'].isin(['전체', '-', '알수없음'])].copy()
            if not df_region_clean.empty:
                render_big_table(df_region_clean.style.format(fmt), 'region_table_minimal', 560)
            else:
                st.info('지역 구분 데이터가 없습니다.')
        else:
            st.info('현재 자동 수집 경로에는 지역 원천이 없습니다.')

    with tab_device:
        _render_section('기기별 성과', '자동 수집된 기기 데이터를 우선 사용합니다.')
        if not df_device.empty:
            render_big_table(df_device.style.format(fmt), 'device_table_minimal', 420)
        else:
            st.info('기기 자동 수집 데이터가 없습니다.')

    with tab_leak:
        _render_section('비효율 점검', '전환 없이 비용만 사용된 항목을 간단하게 확인합니다.')
        col1, col2 = st.columns(2)
        with col1:
            if not df_media.empty:
                bad_m = df_media[(df_media['전환수'] == 0) & (df_media['광고비'] >= 10000)].sort_values('광고비', ascending=False)
                if not bad_m.empty:
                    st.dataframe(bad_m[['매체이름', '광고비', '클릭수', 'CTR(%)']].style.format(fmt), hide_index=True, use_container_width=True)
                else:
                    st.success('비효율 지면이 없습니다.')
            else:
                st.info('매체 데이터가 없어 계산할 수 없습니다.')
        with col2:
            if not df_region.empty:
                bad_r = df_region[(df_region['전환수'] == 0) & (df_region['광고비'] >= 10000) & (~df_region['지역명'].isin(['전체', '-', '알수없음']))].sort_values('광고비', ascending=False)
                if not bad_r.empty:
                    st.dataframe(bad_r[['지역명', '광고비', '클릭수', 'CTR(%)']].style.format(fmt), hide_index=True, use_container_width=True)
                else:
                    st.success('비효율 지역이 없습니다.')
            else:
                st.info('지역 데이터가 없어 계산할 수 없습니다.')
