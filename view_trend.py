# -*- coding: utf-8 -*-
"""view_trend.py - 시장 트렌드(DataLab) vs 자사 노출수 비교 분석 탭"""

from __future__ import annotations
import os
import json
import urllib.request
import pandas as pd
import numpy as np
import streamlit as st
from datetime import date, timedelta
from typing import Dict

# ✨ data.py에서 캠페인 이름 가져오는 함수(load_dim_campaign) 추가
from data import sql_read, get_table_columns, table_exists, _sql_in_str_list, load_dim_campaign

# Optional ECharts
try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

def get_datalab_trend(client_id: str, client_secret: str, keyword: str, start_date: date, end_date: date) -> pd.DataFrame:
    """네이버 데이터랩 통합검색어 트렌드 API 호출"""
    if not client_id or not client_secret:
        return pd.DataFrame()

    url = "https://openapi.naver.com/v1/datalab/search"
    s_date = start_date
    e_date = end_date
    if s_date == e_date:
        s_date = s_date - timedelta(days=1)

    body = {
        "startDate": s_date.strftime("%Y-%m-%d"),
        "endDate": e_date.strftime("%Y-%m-%d"),
        "timeUnit": "date",
        "keywordGroups": [
            {"groupName": keyword, "keywords": [keyword]}
        ],
        "device": "",
        "ages": [],
        "gender": ""
    }

    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    req.add_header("Content-Type", "application/json")

    try:
        response = urllib.request.urlopen(req, data=json.dumps(body).encode("utf-8"))
        rescode = response.getcode()
        if rescode == 200:
            response_body = response.read().decode('utf-8')
            data = json.loads(response_body)
            
            if "results" in data and len(data["results"]) > 0:
                trend_data = data["results"][0]["data"]
                df = pd.DataFrame(trend_data)
                df = df.rename(columns={"period": "dt", "ratio": "트렌드지수(%)"})
                df["dt"] = pd.to_datetime(df["dt"])
                return df
    except Exception as e:
        st.error(f"데이터랩 API 호출 오류: {e}")
    
    return pd.DataFrame()

def get_specific_keyword_timeseries(engine, d1: date, d2: date, cids: tuple, target_keyword: str, ad_type: str) -> pd.DataFrame:
    """파워링크(키워드)와 쇼핑검색(소재/상품) 데이터를 업체/캠페인 정보와 함께 가져옴"""
    df_list = []
    search_kw = f"%{target_keyword.replace(' ', '')}%"
    
    where_cid_fk = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    where_cid_fa = f"AND fa.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""

    if ad_type in ["전체", "파워링크"]:
        if table_exists(engine, "fact_keyword_daily") and table_exists(engine, "dim_keyword"):
            fk_cols = get_table_columns(engine, "fact_keyword_daily")
            has_sales = "sales" in fk_cols
            sales_expr = "SUM(COALESCE(fk.sales, 0))" if has_sales else "0::numeric"
            
            dk_cols = get_table_columns(engine, "dim_keyword")
            kw_col = next((c for c in ("keyword_name", "keyword", "rel_keyword", "name") if c in dk_cols), None)
            if kw_col:
                sql = f"""
                SELECT fk.dt::date AS dt, fk.customer_id::text AS customer_id, fk.campaign_id::text AS campaign_id,
                       dk.{kw_col} AS matched_name, '파워링크' AS ad_type,
                       SUM(fk.imp) AS imp, SUM(fk.clk) AS clk, SUM(fk.cost) AS cost, {sales_expr} AS sales
                FROM fact_keyword_daily fk
                JOIN dim_keyword dk ON fk.customer_id::text = dk.customer_id::text AND fk.keyword_id::text = dk.keyword_id::text
                WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid_fk}
                AND REPLACE(dk.{kw_col}, ' ', '') ILIKE :kw
                GROUP BY fk.dt::date, fk.customer_id, fk.campaign_id, dk.{kw_col}
                """
                df_kw = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
                if df_kw is not None and not df_kw.empty: df_list.append(df_kw)

    if ad_type in ["전체", "쇼핑검색"]:
        if table_exists(engine, "fact_ad_daily") and table_exists(engine, "dim_ad"):
            fa_cols = get_table_columns(engine, "fact_ad_daily")
            has_sales = "sales" in fa_cols
            sales_expr = "SUM(COALESCE(fa.sales, 0))" if has_sales else "0::numeric"
            
            da_cols = get_table_columns(engine, "dim_ad")
            ad_col = next((c for c in ("ad_name", "name", "product_name") if c in da_cols), None)
            if ad_col:
                sql = f"""
                SELECT fa.dt::date AS dt, fa.customer_id::text AS customer_id, fa.campaign_id::text AS campaign_id,
                       da.{ad_col} AS matched_name, '쇼핑검색' AS ad_type,
                       SUM(fa.imp) AS imp, SUM(fa.clk) AS clk, SUM(fa.cost) AS cost, {sales_expr} AS sales
                FROM fact_ad_daily fa
                JOIN dim_ad da ON fa.customer_id::text = da.customer_id::text AND fa.ad_id::text = da.ad_id::text
                WHERE fa.dt BETWEEN :d1 AND :d2 {where_cid_fa}
                AND REPLACE(da.{ad_col}, ' ', '') ILIKE :kw
                GROUP BY fa.dt::date, fa.customer_id, fa.campaign_id, da.{ad_col}
                """
                df_ad = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
                if df_ad is not None and not df_ad.empty: df_list.append(df_ad)

    if not df_list:
        return pd.DataFrame()
    
    return pd.concat(df_list, ignore_index=True)

def render_trend_chart(df: pd.DataFrame, keyword: str, ad_type_label: str):
    """ECharts를 이용한 듀얼 축 차트 렌더링"""
    if df.empty or not HAS_ECHARTS:
        st.info("차트를 그릴 데이터가 없거나 ECharts 모듈이 없습니다.")
        return

    x_data = df["dt"].dt.strftime('%m-%d').tolist()
    imp_data = df["노출"].fillna(0).tolist()
    trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()
    
    legend_name = f"내부 노출수 ({ad_type_label})"

    options = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [legend_name, "네이버 시장 트렌드 지수"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": "내부 노출수", "splitLine": {"lineStyle": {"type": "dashed", "color": "#f3f4f6"}}},
            {"type": "value", "name": "트렌드 지수(0~100)", "min": 0, "max": 100, "splitLine": {"show": False}}
        ],
        "series": [
            {
                "name": legend_name, 
                "type": "bar", 
                "data": imp_data, 
                "itemStyle": {"color": "#3B82F6", "borderRadius": [4,4,0,0]}
            },
            {
                "name": "네이버 시장 트렌드 지수", 
                "type": "line", 
                "yAxisIndex": 1, 
                "data": trend_data, 
                "itemStyle": {"color": "#10B981"}, 
                "lineStyle": {"width": 3}, 
                "symbol": "circle", 
                "symbolSize": 8
            }
        ]
    }
    st_echarts(options=options, height="400px")

def page_trend(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    
    st.markdown("## 📈 시장 트렌드 vs 자사 노출 분석 (1:1 매칭)")
    st.caption("네이버 전체 유저의 '실제 검색량 추이'와 **우리 광고(파워링크/쇼핑검색) 노출수**를 1:1로 겹쳐보며 인사이트를 발굴합니다.")
    st.divider()

    client_id = ""
    client_secret = ""
    try:
        client_id = st.secrets.get("NAVER_DATALAB_CLIENT_ID", os.getenv("NAVER_DATALAB_CLIENT_ID", ""))
        client_secret = st.secrets.get("NAVER_DATALAB_CLIENT_SECRET", os.getenv("NAVER_DATALAB_CLIENT_SECRET", ""))
    except Exception:
        client_id = os.getenv("NAVER_DATALAB_CLIENT_ID", "")
        client_secret = os.getenv("NAVER_DATALAB_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        st.warning("⚠️ 네이버 데이터랩 API 키가 설정되지 않았습니다. Streamlit Cloud의 [Settings] -> [Secrets] 에 키를 등록해주세요.")
        return

    cids = tuple(f.get("selected_customer_ids", []))
    d1, d2 = f["start"], f["end"]
    
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        target_keyword = st.text_input("🔍 분석할 키워드 또는 상품명 입력", value="", placeholder="예: 나이키운동화").strip()
    with c2:
        ad_type_sel = st.selectbox("🎯 분석할 광고 영역", ["전체 (파워링크+쇼핑검색)", "파워링크 (키워드 매칭)", "쇼핑검색 (상품명 매칭)"])
    
    ad_type_mapper = {"전체 (파워링크+쇼핑검색)": "전체", "파워링크 (키워드 매칭)": "파워링크", "쇼핑검색 (상품명 매칭)": "쇼핑검색"}
    mapped_type = ad_type_mapper[ad_type_sel]

    if not target_keyword:
        st.info("분석을 원하시는 주력 키워드나 상품명을 위에 입력해 주세요.")
        return

    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 더 길게 잡아주세요.")
        return

    with st.spinner(f"'{target_keyword}' 관련 시장 트렌드와 내부 데이터를 가져오는 중입니다..."):
        try:
            raw_internal = get_specific_keyword_timeseries(engine, d1, d2, cids, target_keyword, mapped_type)
        except Exception as e:
            st.error(f"내부 데이터 조회 중 오류가 발생했습니다: {e}")
            return
        
        trend_df = get_datalab_trend(client_id, client_secret, target_keyword, d1, d2)
        
        if trend_df.empty:
            st.error("네이버 데이터랩에서 데이터를 가져오지 못했습니다. 키워드 검색량이 너무 적거나, API 키 설정을 확인해 주세요.")
            return

        if raw_internal is not None and not raw_internal.empty:
            # 1. 차트용 일자별 집계 데이터 생성
            raw_internal["dt"] = pd.to_datetime(raw_internal["dt"])
            agg_internal = raw_internal.groupby("dt", as_index=False)[["imp", "clk", "cost", "sales"]].sum().sort_values("dt")
            agg_internal = agg_internal.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            merged_df = pd.merge(trend_df, agg_internal, on="dt", how="left")
            merged_df["노출"] = merged_df["노출"].fillna(0)
            merged_df["클릭"] = merged_df["클릭"].fillna(0)
            merged_df["광고비"] = merged_df["광고비"].fillna(0)
            
            st.markdown(f"### 📊 [{target_keyword}] 트렌드 비교 차트 ({mapped_type})")
            render_trend_chart(merged_df, target_keyword, mapped_type)
            
            # 2. ✨ [NEW] 출처(Source) 표시용 데이터 가공 (업체명, 캠페인명 조인)
            st.markdown("### 🗂️ 내부 데이터 출처 (어느 캠페인에서 노출되었을까?)")
            
            source_df = raw_internal.groupby(["customer_id", "campaign_id", "ad_type", "matched_name"], as_index=False)[["imp", "clk", "cost", "sales"]].sum()
            
            # 업체명 매칭
            source_df["customer_id"] = source_df["customer_id"].astype(str)
            meta_copy = meta.copy()
            meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
            source_df = source_df.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            source_df["업체명"] = source_df["account_name"].fillna(source_df["customer_id"])
            
            # 캠페인명 매칭
            try:
                dim_camp = load_dim_campaign(engine)
                dim_camp["customer_id"] = dim_camp["customer_id"].astype(str)
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                camp_col = next((c for c in ["campaign_name", "name"] if c in dim_camp.columns), "campaign_name")
                source_df = source_df.merge(dim_camp[["customer_id", "campaign_id", camp_col]], on=["customer_id", "campaign_id"], how="left")
                source_df["캠페인명"] = source_df[camp_col].fillna(source_df["campaign_id"])
            except Exception:
                source_df["캠페인명"] = source_df["campaign_id"]
                
            # 예쁘게 컬럼 정리 후 출력
            disp_source = source_df[["업체명", "캠페인명", "ad_type", "matched_name", "imp", "clk", "cost"]].sort_values("imp", ascending=False)
            disp_source = disp_source.rename(columns={"ad_type": "광고유형", "matched_name": "매칭된 키워드/상품명", "imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            st.dataframe(
                disp_source.style.format({"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"}),
                use_container_width=True, hide_index=True
            )
            
            st.markdown("#### 📅 일자별 상세 데이터")
            st.dataframe(
                merged_df[["dt", "트렌드지수(%)", "노출", "클릭", "광고비"]].sort_values("dt", ascending=False).style.format({
                    "트렌드지수(%)": "{:.1f}",
                    "노출": "{:,.0f}",
                    "클릭": "{:,.0f}",
                    "광고비": "{:,.0f}"
                }),
                use_container_width=True, hide_index=True
            )
        else:
            st.warning(f"선택하신 기간 동안 우리 계정 내에서 '{target_keyword}' ({mapped_type}) 광고가 노출된 기록이 없습니다.")
