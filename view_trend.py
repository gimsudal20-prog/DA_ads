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
import altair as alt

from data import sql_read, get_table_columns, table_exists, _sql_in_str_list, load_dim_campaign
from page_helpers import _perf_common_merge_meta

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

    client_id = client_id.replace('"', '').replace("'", "").strip()
    client_secret = client_secret.replace('"', '').replace("'", "").strip()

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
        err_msg = str(e)
        if hasattr(e, 'read'):
            try: err_msg += " | " + e.read().decode('utf-8')
            except: pass
        st.error(f"🚨 네이버 데이터랩 API 통신 에러: {err_msg}")
    
    return pd.DataFrame()

def get_specific_keyword_timeseries(engine, d1: date, d2: date, cids: tuple, target_keyword: str, ad_type: str) -> pd.DataFrame:
    """✨ 마케터님이 직접 입력한 키워드를 DB에서 100% 긁어오는 함수 (누락 없음, 에러 방어 완벽)"""
    df_list = []
    # 띄어쓰기 무시하고 검색 (예: '나이키 운동화' 치면 '나이키운동화'도 찾아줌)
    search_kw = f"%{target_keyword.replace(' ', '')}%"
    
    where_cid_fk = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    where_cid_fa = f"AND fa.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""

    # 1. 파워링크 (키워드 매칭)
    if ad_type in ["전체", "파워링크"]:
        if table_exists(engine, "fact_keyword_daily") and table_exists(engine, "dim_keyword"):
            fk_cols = get_table_columns(engine, "fact_keyword_daily")
            dk_cols = get_table_columns(engine, "dim_keyword")
            
            has_sales = "sales" in fk_cols
            sales_expr = "SUM(COALESCE(fk.sales, 0))" if has_sales else "0::numeric"
            camp_expr = "fk.campaign_id" if "campaign_id" in fk_cols else ("dk.campaign_id" if "campaign_id" in dk_cols else "''")
            
            kw_col = next((c for c in ("keyword_name", "keyword", "rel_keyword", "name") if c in dk_cols), None)
            if kw_col:
                sql = f"""
                SELECT fk.dt::date AS dt, fk.customer_id::text AS customer_id, {camp_expr}::text AS campaign_id,
                       dk.{kw_col} AS matched_name, '파워링크' AS ad_type,
                       SUM(fk.imp) AS imp, SUM(fk.clk) AS clk, SUM(fk.cost) AS cost, {sales_expr} AS sales
                FROM fact_keyword_daily fk
                JOIN dim_keyword dk ON fk.customer_id::text = dk.customer_id::text AND fk.keyword_id::text = dk.keyword_id::text
                WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid_fk}
                AND REPLACE(dk.{kw_col}, ' ', '') ILIKE :kw
                GROUP BY fk.dt::date, fk.customer_id, {camp_expr}, dk.{kw_col}
                """
                df_kw = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
                if df_kw is not None and not df_kw.empty: df_list.append(df_kw)

    # 2. 쇼핑검색 (상품명 매칭)
    if ad_type in ["전체", "쇼핑검색"]:
        if table_exists(engine, "fact_ad_daily") and table_exists(engine, "dim_ad"):
            fa_cols = get_table_columns(engine, "fact_ad_daily")
            da_cols = get_table_columns(engine, "dim_ad")
            
            has_sales = "sales" in fa_cols
            sales_expr = "SUM(COALESCE(fa.sales, 0))" if has_sales else "0::numeric"
            camp_expr_ad = "fa.campaign_id" if "campaign_id" in fa_cols else ("da.campaign_id" if "campaign_id" in da_cols else "''")
            
            ad_col = next((c for c in ("ad_name", "name", "product_name") if c in da_cols), None)
            if ad_col:
                sql = f"""
                SELECT fa.dt::date AS dt, fa.customer_id::text AS customer_id, {camp_expr_ad}::text AS campaign_id,
                       da.{ad_col} AS matched_name, '쇼핑검색' AS ad_type,
                       SUM(fa.imp) AS imp, SUM(fa.clk) AS clk, SUM(fa.cost) AS cost, {sales_expr} AS sales
                FROM fact_ad_daily fa
                JOIN dim_ad da ON fa.customer_id::text = da.customer_id::text AND fa.ad_id::text = da.ad_id::text
                WHERE fa.dt BETWEEN :d1 AND :d2 {where_cid_fa}
                AND REPLACE(da.{ad_col}, ' ', '') ILIKE :kw
                GROUP BY fa.dt::date, fa.customer_id, {camp_expr_ad}, da.{ad_col}
                """
                df_ad = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
                if df_ad is not None and not df_ad.empty: df_list.append(df_ad)

    if not df_list:
        return pd.DataFrame()
    return pd.concat(df_list, ignore_index=True)

def render_trend_chart(df: pd.DataFrame, keyword: str, ad_type_label: str):
    if df.empty: return

    legend_name = f"내부 노출수 ({ad_type_label})"

    if HAS_ECHARTS:
        x_data = df["dt"].dt.strftime('%m-%d').tolist()
        imp_data = df["노출"].fillna(0).tolist()
        trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()
        
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
    else:
        base = alt.Chart(df).encode(x=alt.X("dt:T", axis=alt.Axis(title="날짜", format="%m-%d")))
        bar = base.mark_bar(color="#3B82F6", opacity=0.7, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            y=alt.Y("노출:Q", axis=alt.Axis(title="자사 노출수", titleColor="#3B82F6"))
        )
        line = base.mark_line(color="#10B981", strokeWidth=3).encode(
            y=alt.Y("트렌드지수(%):Q", axis=alt.Axis(title="네이버 트렌드 지수(0~100)", titleColor="#10B981"))
        )
        points = line.mark_circle(size=70, color="#10B981")
        
        chart = alt.layer(bar, line + points).resolve_scale(y='independent').properties(height=400).configure_axis(labelFontSize=11, titleFontSize=13).configure_legend(orient='bottom')
        st.altair_chart(chart, use_container_width=True)

def page_trend(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    
    st.markdown("## 📈 시장 트렌드 vs 자사 노출 분석 (직접 입력)")
    st.caption("원하시는 키워드나 상품명을 직접 입력하시면, 관련된 내부 데이터를 1건도 빠짐없이 모두 긁어와 시장 트렌드와 겹쳐서 보여줍니다.")
    st.divider()

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
    
    # ✨ 마케터님 요청대로 다시 "직접 입력(text_input)" 방식으로 복구!
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        target_keyword = st.text_input("🔍 분석할 키워드 또는 상품명 직접 입력", value="", placeholder="예: 나이키운동화").strip()
    with c2:
        ad_type_sel = st.selectbox("🎯 분석할 광고 영역", ["전체 (파워링크+쇼핑검색)", "파워링크 (키워드 매칭)", "쇼핑검색 (상품명 매칭)"])
    
    ad_type_mapper = {"전체 (파워링크+쇼핑검색)": "전체", "파워링크 (키워드 매칭)": "파워링크", "쇼핑검색 (상품명 매칭)": "쇼핑검색"}
    mapped_type = ad_type_mapper[ad_type_sel]

    if not target_keyword:
        st.info("분석을 원하시는 키워드나 상품명을 입력 후 엔터를 쳐주세요.")
        return

    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 늘려주세요.")
        return

    with st.spinner(f"'{target_keyword}' 관련 데이터를 싹 다 긁어오는 중입니다..."):
        try:
            raw_internal = get_specific_keyword_timeseries(engine, d1, d2, cids, target_keyword, mapped_type)
        except Exception as e:
            st.error(f"내부 데이터 조회 중 오류가 발생했습니다: {e}")
            return
        
        trend_df = get_datalab_trend(client_id, client_secret, target_keyword, d1, d2)
        
        if trend_df.empty:
            st.error(f"⚠️ 네이버 데이터랩에서 '{target_keyword}' 트렌드를 가져오지 못했습니다. 검색어가 너무 길거나 검색량이 부족합니다.")
            return

        if raw_internal is not None and not raw_internal.empty:
            # 1. 차트용 집계
            raw_internal["dt"] = pd.to_datetime(raw_internal["dt"])
            agg_internal = raw_internal.groupby("dt", as_index=False)[["imp", "clk", "cost", "sales"]].sum().sort_values("dt")
            agg_internal = agg_internal.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            merged_df = pd.merge(trend_df, agg_internal, on="dt", how="left")
            for c in ["노출", "클릭", "광고비"]:
                merged_df[c] = merged_df[c].fillna(0)
            
            st.markdown(f"### 📊 [{target_keyword}] 트렌드 비교 차트 ({mapped_type})")
            render_trend_chart(merged_df, target_keyword, target_keyword, mapped_type)
            
            # 2. 어떤 캠페인/키워드에서 노출되었는지 출처 추적 표
            st.markdown(f"### 🗂️ 내부 데이터 출처 (어떤 캠페인에서 돈을 썼을까?)")
            
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
                
            disp_source = source_df[["업체명", "캠페인명", "ad_type", "matched_name", "imp", "clk", "cost"]].sort_values("imp", ascending=False)
            disp_source = disp_source.rename(columns={"ad_type": "광고유형", "matched_name": "매칭된 키워드/상품명", "imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            st.dataframe(
                disp_source.style.format({"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"}),
                use_container_width=True, hide_index=True
            )
            
            st.markdown("#### 📅 일자별 상세 데이터")
            st.dataframe(
                merged_df[["dt", "트렌드지수(%)", "노출", "클릭", "광고비"]].sort_values("dt", ascending=False).style.format({
                    "트렌드지수(%)": "{:.1f}", "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"
                }),
                use_container_width=True, hide_index=True
            )
        else:
            st.warning(f"⚠️ 선택하신 기간({d1}~{d2}) 동안 우리 계정 내에서 '{target_keyword}' ({mapped_type}) 광고가 노출된 기록이 없습니다.")
