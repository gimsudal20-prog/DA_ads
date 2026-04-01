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
    s = str(name_or_code or "").strip()
    if not s or s.lower() in {"nan", "none", "전체"}:
        return "전체"
    
    # ✨ 수정 1: 소수점이 붙어있는 매체 코드 처리 (예: "8753.0" -> "8753")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
        
    if s in NAVER_MEDIA_MAP:
        return NAVER_MEDIA_MAP[s]
    if s.isdigit():
        return f"알수없는 지면 (코드: {s})"
    return s

def _normalize_device_value(v: object) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none", "전체"}:
        return "기타"
    us = s.upper()
    if s in {"모바일", "MOBILE"} or "MOBILE" in us or "APP" in us or "PHONE" in us or s in {"MO", "M"}:
        return "MO"
    if s == "PC" or s == "P" or "DESKTOP" in us or "WEB" in us or "웹" in s:
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

def _pick_first_nonempty(row: pd.Series, candidates: list[str], default: str = "") -> str:
    for c in candidates:
        if c in row.index:
            v = str(row.get(c, "") or "").strip()
            if v and v.lower() not in {"nan", "none"}:
                return v
    return default

def _metric_expr(cols: set[str], *candidates: str) -> str:
    for c in candidates:
        if c in cols:
            return f"COALESCE({c}, 0)"
    return "0"

def _query_media_region(engine, f) -> pd.DataFrame:
    if not table_exists(engine, 'fact_media_daily'):
        return pd.DataFrame()

    cols = set(get_table_columns(engine, 'fact_media_daily'))
    params = {'d1': str(f['start']), 'd2': str(f['end'])}
    type_vals = _expand_campaign_type_values(tuple(f.get('type_sel', []) or []))
    cids = tuple(f.get('selected_customer_ids', []) or ())
    where_cid = f"AND CAST(customer_id AS TEXT) IN ({_sql_in_str_list(list(cids))})" if cids else ''

    # UI/UX는 그대로 두고, 연동부만 안정적으로 raw read -> pandas aggregate 로 변경
    select_cols = [
        'dt', 'customer_id',
        'campaign_type' if 'campaign_type' in cols else None,
        'campaign_tp' if 'campaign_tp' in cols else None,
        'campaign_type_label' if 'campaign_type_label' in cols else None,
        'media_name' if 'media_name' in cols else None,
        'media_code' if 'media_code' in cols else None,
        'media_tp' if 'media_tp' in cols else None,
        'placement_name' if 'placement_name' in cols else None,
        'placement_code' if 'placement_code' in cols else None,
        'placement_tp' if 'placement_tp' in cols else None,
        'region_name' if 'region_name' in cols else None,
        'device_name' if 'device_name' in cols else None,
        'device' if 'device' in cols else None,
        'device_tp' if 'device_tp' in cols else None,
        'device_type' if 'device_type' in cols else None,
        'platform' if 'platform' in cols else None,
        f"{_metric_expr(cols, 'imp')} AS imp",
        f"{_metric_expr(cols, 'clk')} AS clk",
        f"{_metric_expr(cols, 'cost')} AS cost",
        f"{_metric_expr(cols, 'tot_conv', 'purchase_conv', 'conv')} AS conv",
        f"{_metric_expr(cols, 'tot_sales', 'purchase_sales', 'sales')} AS sales",
    ]
    select_cols = [c for c in select_cols if c]
    sql = f"""
        SELECT /* media_connection_fix_raw_v1 */
            {', '.join(select_cols)}
        FROM fact_media_daily
        WHERE dt BETWEEN :d1 AND :d2 {where_cid}
    """
    try:
        raw = sql_read(engine, sql, params)
    except Exception:
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()

    # campaign_type filter를 pandas에서 수행해 no-header 적재 케이스도 놓치지 않게 함
    cp_candidates = [c for c in ['campaign_type', 'campaign_tp', 'campaign_type_label'] if c in raw.columns]
    if type_vals and cp_candidates:
        cp_series = raw[cp_candidates].bfill(axis=1).iloc[:, 0].fillna('').astype(str)
        raw = raw[cp_series.isin(type_vals)]
        if raw.empty:
            return pd.DataFrame()

    def _media_from_row(r: pd.Series) -> str:
        return _pick_first_nonempty(r, ['media_name', 'placement_name', 'media_code', 'placement_code', 'media_tp', 'placement_tp'], '전체')

    def _device_from_row(r: pd.Series) -> str:
        return _pick_first_nonempty(r, ['device_name', 'device', 'device_tp', 'device_type', 'platform'], '기타')

    raw['매체이름'] = raw.apply(_media_from_row, axis=1).apply(_map_media_name)
    raw['기기명'] = raw.apply(_device_from_row, axis=1).apply(_normalize_device_value)

    for c in ['imp', 'clk', 'cost', 'conv', 'sales']:
        raw[c] = pd.to_numeric(raw[c], errors='coerce').fillna(0)

    out = raw.groupby(['매체이름', '기기명'], as_index=False)[['imp', 'clk', 'cost', 'conv', 'sales']].sum()
    out = out.rename(columns={'imp':'노출수','clk':'클릭수','cost':'광고비','conv':'전환수','sales':'전환매출'})
    return out

def _query_device(engine, f) -> pd.DataFrame:
    # ✨ 수정 2: 매체/기기 탭의 총합이 100% 일치하도록 fact_media_daily 단일 원천을 공유하도록 변경
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
