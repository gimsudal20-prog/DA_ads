# -*- coding: utf-8 -*-
"""view_media.py - collected media/device analysis, aligned with dashboard table style."""

from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data import sql_read, table_exists, get_table_columns, _sql_in_str_list
from ui import render_big_table, render_empty_state

_DEVICE_ORDER = ["PC", "MO", "기타", "전체"]
_OVERALL_VALUES = {"전체", "-", "알수없음", "기타"}
FMT_DICT = {
    "노출수": "{:,.0f}",
    "클릭수": "{:,.0f}",
    "광고비": "{:,.0f}원",
    "전환수": "{:,.1f}",
    "전환매출": "{:,.0f}원",
    "ROAS(%)": "{:,.2f}%",
    "CPA(원)": "{:,.0f}원",
    "CTR(%)": "{:,.2f}%",
}


def _normalize_device_value(v: object) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none", "전체"}:
        return "전체"
    us = s.upper()
    if s in {"모바일", "MOBILE"} or "MOBILE" in us or "APP" in us or "PHONE" in us or s in {"MO", "M"}:
        return "MO"
    if s in {"PC", "P"} or "DESKTOP" in us or "WEB" in us or "웹" in s:
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


def _media_where_sql(f, cols: list[str]) -> tuple[str, dict]:
    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    where_cid = f"AND customer_id::text IN ({_sql_in_str_list(list(tuple(f.get('selected_customer_ids', []) or ())))})" if tuple(f.get('selected_customer_ids', []) or ()) else ''
    type_sql = _type_filter_sql(type_vals) if 'campaign_type' in cols else ''
    params = {'d1': str(f['start']), 'd2': str(f['end'])}
    return f"WHERE dt BETWEEN :d1 AND :d2 {where_cid} {type_sql}", params


def _query_media_raw(engine, f) -> pd.DataFrame:
    if not table_exists(engine, 'fact_media_daily'):
        return pd.DataFrame()
    cols = get_table_columns(engine, 'fact_media_daily')
    where_sql, params = _media_where_sql(f, cols)
    select_cols = [
        "COALESCE(NULLIF(TRIM(media_name), ''), '전체') AS media_name",
        "COALESCE(NULLIF(TRIM(region_name), ''), '전체') AS region_name",
        "COALESCE(NULLIF(TRIM(device_name), ''), '전체') AS device_name",
        "SUM(COALESCE(imp,0)) AS imp",
        "SUM(COALESCE(clk,0)) AS clk",
        "SUM(COALESCE(cost,0)) AS cost",
        "SUM(COALESCE(conv,0)) AS conv",
        "SUM(COALESCE(sales,0)) AS sales",
    ]
    if 'data_source' in cols:
        select_cols.append("COALESCE(NULLIF(TRIM(data_source), ''), '-') AS data_source")
    else:
        select_cols.append("'-' AS data_source")
    if 'source_report' in cols:
        select_cols.append("COALESCE(NULLIF(TRIM(source_report), ''), '-') AS source_report")
    else:
        select_cols.append("'-' AS source_report")

    sql = f"""
        SELECT
            {', '.join(select_cols)}
        FROM fact_media_daily
        {where_sql}
        GROUP BY 1,2,3,9,10
    """
    try:
        df = sql_read(engine, sql, params)
        if df is None:
            return pd.DataFrame()
        return df
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
                COALESCE(NULLIF(TRIM(f.device_name), ''), '전체') AS "기기명",
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

    media_df = _query_media_raw(engine, f)
    if media_df.empty:
        return pd.DataFrame()
    tmp = media_df.copy()
    tmp['기기명'] = tmp['device_name'].apply(_normalize_device_value)
    return tmp.groupby('기기명', as_index=False)[['imp', 'clk', 'cost', 'conv', 'sales']].sum().rename(columns={'imp':'노출수','clk':'클릭수','cost':'광고비','conv':'전환수','sales':'전환매출'})


def _calc_metrics(df: pd.DataFrame, metric_cols: dict[str, str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    cols = metric_cols or {'노출수':'노출수','클릭수':'클릭수','광고비':'광고비','전환수':'전환수','전환매출':'전환매출'}
    for c in cols.values():
        out[c] = pd.to_numeric(out[c], errors='coerce').fillna(0)
    out['ROAS(%)'] = np.where(out[cols['광고비']] > 0, (out[cols['전환매출']] / out[cols['광고비']]) * 100, 0.0)
    out['CPA(원)'] = np.where(out[cols['전환수']] > 0, out[cols['광고비']] / out[cols['전환수']], 0.0)
    out['CTR(%)'] = np.where(out[cols['노출수']] > 0, (out[cols['클릭수']] / out[cols['노출수']]) * 100, 0.0)
    return out.reset_index(drop=True)


def _section(title: str, subtitle: str) -> None:
    st.markdown(
        f"<div class='nv-section'><div class='nv-section-head'><div><div class='nv-sec-title'>{title}</div><div class='nv-sec-sub'>{subtitle}</div></div></div>",
        unsafe_allow_html=True,
    )


def _section_end() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def page_media(engine, f):
    st.markdown(
        """
        <div class='nv-section nv-section-muted' style='margin-top:0;'>
            <div class='nv-sec-title'>매체 / 지면 분석</div>
            <div class='nv-sec-sub'>캠페인·쇼핑검색어 탭과 동일하게 표 중심으로 보여줍니다. 지면 상세가 적재되면 지면코드 기준으로, 없으면 전체/기기 요약을 표시합니다.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    raw_media = _query_media_raw(engine, f)
    device_df = _query_device(engine, f)

    if raw_media.empty and (device_df is None or device_df.empty):
        render_empty_state("자동 수집된 매체/기기 데이터가 없습니다.", height=240)
        return

    if not raw_media.empty:
        raw_media = raw_media.copy()
        raw_media['device_name'] = raw_media['device_name'].apply(_normalize_device_value)
        raw_media.rename(columns={
            'media_name':'매체이름','region_name':'지역명','device_name':'기기명',
            'imp':'노출수','clk':'클릭수','cost':'광고비','conv':'전환수','sales':'전환매출',
            'data_source':'데이터원천','source_report':'리포트원천'
        }, inplace=True)
        detail_media = raw_media[~raw_media['매체이름'].isin(_OVERALL_VALUES)].copy()
        summary_media = raw_media[raw_media['매체이름'].isin(_OVERALL_VALUES)].copy()
        detail_region = raw_media[~raw_media['지역명'].isin(_OVERALL_VALUES)].copy()
    else:
        detail_media = pd.DataFrame()
        summary_media = pd.DataFrame()
        detail_region = pd.DataFrame()

    df_media = pd.DataFrame()
    if not detail_media.empty:
        df_media = detail_media.groupby('매체이름', as_index=False)[['노출수','클릭수','광고비','전환수','전환매출']].sum()
        df_media = _calc_metrics(df_media).sort_values(['광고비','클릭수'], ascending=False).reset_index(drop=True)

    df_media_summary = pd.DataFrame()
    if not summary_media.empty:
        df_media_summary = summary_media.groupby('기기명', as_index=False)[['노출수','클릭수','광고비','전환수','전환매출']].sum()
        df_media_summary = _calc_metrics(df_media_summary).sort_values('광고비', ascending=False).reset_index(drop=True)

    df_region = pd.DataFrame()
    if not detail_region.empty:
        df_region = detail_region.groupby('지역명', as_index=False)[['노출수','클릭수','광고비','전환수','전환매출']].sum()
        df_region = _calc_metrics(df_region).sort_values(['광고비','클릭수'], ascending=False).reset_index(drop=True)

    if device_df is not None and not device_df.empty:
        device_df = device_df.copy()
        device_df['기기명'] = device_df['기기명'].apply(_normalize_device_value)
        device_df = device_df.groupby('기기명', as_index=False)[['노출수','클릭수','광고비','전환수','전환매출']].sum()
        device_df['ord'] = device_df['기기명'].map({k: i for i, k in enumerate(_DEVICE_ORDER)}).fillna(99)
        df_device = _calc_metrics(device_df).sort_values(['ord','광고비'], ascending=[True, False]).drop(columns=['ord'], errors='ignore').reset_index(drop=True)
    else:
        df_device = pd.DataFrame()

    tabs = st.tabs(['지면(매체)', '지역', '기기', '원천 미리보기'])

    with tabs[0]:
        _section('지면(매체) 성과', '캠페인/다른 탭과 동일한 표 스타일로 보여줍니다. 지면 상세가 없으면 전체/기기 요약으로 대체합니다.')
        if not df_media.empty:
            render_big_table(df_media.style.format(FMT_DICT), 'media_table_main_aligned', 620)
        elif not df_media_summary.empty:
            render_empty_state('지면 상세 행이 없어 전체/기기 요약을 표시합니다.', height=220)
            render_big_table(df_media_summary.style.format(FMT_DICT), 'media_table_summary_aligned', 420)
        else:
            render_empty_state('현재 자동 수집 경로에는 지면 데이터가 없습니다.', height=220)
        _section_end()

    with tabs[1]:
        _section('지역 성과', '지역 상세가 적재된 경우만 표로 표시합니다.')
        if not df_region.empty:
            render_big_table(df_region.style.format(FMT_DICT), 'region_table_main_aligned', 620)
        else:
            render_empty_state('지역 상세 데이터가 없습니다.', height=220)
        _section_end()

    with tabs[2]:
        _section('기기 성과', '다른 상세 탭과 동일하게 표 중심으로 표시합니다.')
        if not df_device.empty:
            render_big_table(df_device.style.format(FMT_DICT), 'device_table_main_aligned', 520)
        else:
            render_empty_state('기기 자동 수집 데이터가 없습니다.', height=220)
        _section_end()

    with tabs[3]:
        _section('원천 미리보기', '적재된 원천 행을 그대로 확인합니다. 지면코드/기기코드 확인용입니다.')
        if not raw_media.empty:
            preview_cols = [c for c in ['매체이름','지역명','기기명','노출수','클릭수','광고비','전환수','전환매출','데이터원천','리포트원천'] if c in raw_media.columns]
            preview = raw_media[preview_cols].copy().sort_values(['광고비','클릭수'], ascending=[False, False]).reset_index(drop=True)
            render_big_table(preview.style.format(FMT_DICT), 'media_raw_preview_aligned', 620)
        else:
            render_empty_state('원천 미리보기 데이터가 없습니다.', height=220)
        _section_end()
