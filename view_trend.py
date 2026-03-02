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

# ✨ 마케터님 아이디어 적용: 기존에 잘 돌아가는 번들 함수를 활용하여 목록 생성!
from data import sql_read, get_table_columns, _sql_in_str_list, query_keyword_bundle, query_ad_bundle
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
        pass
    
    return pd.DataFrame()

def render_trend_chart(df: pd.DataFrame, internal_name: str, datalab_name: str, ad_type_label: str):
    if df.empty or not HAS_ECHARTS:
        st.info("차트를 그릴 데이터가 없거나 ECharts 모듈이 없습니다.")
        return

    x_data = df["dt"].dt.strftime('%m-%d').tolist()
    imp_data = df["노출"].fillna(0).tolist()
    trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()
    
    legend_name = f"자사 노출수 ({ad_type_label})"

    options = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [legend_name, f"네이버 트렌드 ('{datalab_name}')"], "bottom": 0},
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
                "name": f"네이버 트렌드 ('{datalab_name}')", 
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
    
    st.markdown("## 📈 시장 트렌드 vs 자사 노출 분석")
    st.caption("내부 광고 데이터에서 실제 등록된 키워드나 상품을 선택하고, 해당 검색어의 네이버 시장 트렌드(데이터랩)와 1:1로 겹쳐봅니다.")
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
    
    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 더 길게 잡아주세요.")
        return

    # 1. 어떤 영역을 볼지 먼저 선택
    ad_type_sel = st.radio("🎯 분석할 광고 영역 선택", ["파워링크 (키워드)", "쇼핑검색 (상품명)"], horizontal=True)
    is_powerlink = "파워링크" in ad_type_sel

    # 2. 선택된 영역에 따라 내부 데이터 목록을 불러와서 드롭다운으로 제공
    with st.spinner("내부 광고 목록을 불러오는 중..."):
        if is_powerlink:
            bundle = query_keyword_bundle(engine, d1, d2, list(cids), ["파워링크"], topn_cost=20000)
            name_col = "keyword" if "keyword" in bundle.columns else ("name" if "name" in bundle.columns else None)
        else:
            bundle = query_ad_bundle(engine, d1, d2, list(cids), ["쇼핑검색"], topn_cost=20000)
            name_col = "ad_name" if "ad_name" in bundle.columns else ("name" if "name" in bundle.columns else None)

    if bundle is None or bundle.empty or not name_col:
        st.info("선택하신 기간/계정에 분석할 수 있는 내부 광고 데이터(비용 소진 기록)가 없습니다.")
        return

    # 업체명 등 메타 정보 조인
    bundle = _perf_common_merge_meta(bundle, meta)
    
    # 드롭다운 리스트 고유값 추출
    item_list = bundle[name_col].dropna().astype(str).unique().tolist()
    item_list = sorted([x for x in item_list if x.strip()])
    
    if not item_list:
        st.info("데이터가 충분하지 않습니다.")
        return

    st.markdown("### 🔍 분석 대상 선택")
    c1, c2 = st.columns(2)
    with c1:
        # ✨ 마케터님 아이디어: 사용자가 직접 텍스트를 치는 게 아니라 있는 것 중에서 선택!
        selected_internal = st.selectbox("1️⃣ 자사 키워드/상품명 선택 (내부 데이터)", item_list)
    
    with c2:
        # 쇼핑검색의 경우 상품명이 너무 길면 데이터랩 조회가 안되므로, 핵심 단어로 수정할 수 있게 제공
        st.caption("※ 네이버 데이터랩은 검색어가 길면(상품명 등) 조회가 안 됩니다. 핵심 키워드만 남겨주세요.")
        selected_datalab = st.text_input("2️⃣ 데이터랩 트렌드 검색어 (수정 가능)", value=selected_internal)

    if not selected_internal or not selected_datalab:
        return

    # 3. 내부 데이터 출처 표시 (에러 없이 번들 데이터에서 바로 추출!)
    st.markdown(f"### 🗂️ 내부 데이터 출처: `{selected_internal}`")
    source_df = bundle[bundle[name_col] == selected_internal].copy()
    
    disp_source = source_df.rename(columns={
        "account_name": "업체명",
        "campaign_name": "캠페인명",
        "adgroup_name": "광고그룹명",
        name_col: "등록된 이름",
        "imp": "노출", "clk": "클릭", "cost": "광고비"
    })
    
    cols_to_show = ["업체명", "캠페인명", "광고그룹명", "등록된 이름", "노출", "클릭", "광고비"]
    disp_cols = [c for c in cols_to_show if c in disp_source.columns]
    
    st.dataframe(
        disp_source[disp_cols].sort_values("노출", ascending=False).style.format({"노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"}),
        use_container_width=True, hide_index=True
    )

    # 4. 차트용 일자별 데이터 추출
    id_col = "keyword_id" if is_powerlink else "ad_id"
    table_name = "fact_keyword_daily" if is_powerlink else "fact_ad_daily"
    
    if id_col not in source_df.columns:
        return
        
    target_ids = source_df[id_col].dropna().astype(str).unique().tolist()
    if not target_ids:
        return

    # 안전하고 확실한 SQL: 에러 유발하는 JOIN을 빼고 ID로만 조회
    cols = get_table_columns(engine, table_name)
    sales_expr = "sales" if "sales" in cols else "0::numeric"
    
    ids_sql = _sql_in_str_list(target_ids)
    sql = f"""
    SELECT dt::date AS dt, SUM(imp) AS imp, SUM(clk) AS clk, SUM(cost) AS cost, SUM(COALESCE({sales_expr}, 0)) AS sales
    FROM {table_name}
    WHERE dt BETWEEN :d1 AND :d2
      AND {id_col}::text IN ({ids_sql})
    GROUP BY dt::date
    ORDER BY dt::date
    """
    
    with st.spinner("트렌드 차트를 그리는 중..."):
        daily_internal = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
        trend_df = get_datalab_trend(client_id, client_secret, selected_datalab, d1, d2)

    if trend_df.empty:
        st.error(f"네이버 데이터랩에서 '{selected_datalab}' 키워드 트렌드를 가져오지 못했습니다. 검색량이 너무 적거나 키워드가 너무 깁니다.")
        return

    if daily_internal is not None and not daily_internal.empty:
        daily_internal["dt"] = pd.to_datetime(daily_internal["dt"])
        daily_internal = daily_internal.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})
        
        merged_df = pd.merge(trend_df, daily_internal, on="dt", how="left")
        for c in ["노출", "클릭", "광고비"]:
            merged_df[c] = merged_df[c].fillna(0)
            
        st.markdown(f"### 📊 트렌드 비교 차트")
        render_trend_chart(merged_df, selected_internal, selected_datalab, "파워링크" if is_powerlink else "쇼핑검색")
        
        st.markdown("""
        #### 💡 인사이트 해석 가이드
        * **초록선 상승 + 파란막대 하락:** 🚨 시장은 커지는데 우리만 노출이 떨어집니다. **경쟁사에 순위를 뺏겼으니 입찰가를 올리세요!**
        * **초록선 하락 + 파란막대 하락:** 📉 비수기나 시장 수요 감소입니다. 무리하게 입찰가를 올리지 마세요.
        """)
        
        st.markdown("#### 📅 일자별 상세 데이터")
        st.dataframe(
            merged_df[["dt", "트렌드지수(%)", "노출", "클릭", "광고비"]].sort_values("dt", ascending=False).style.format({
                "트렌드지수(%)": "{:.1f}", "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"
            }),
            use_container_width=True, hide_index=True
        )
