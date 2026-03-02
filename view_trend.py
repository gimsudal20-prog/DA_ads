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

# ✨ data.py에서 필요한 도구들 가져오기
from data import sql_read, get_table_columns, table_exists, _sql_in_str_list
from page_helpers import get_dynamic_cmp_options

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

def get_specific_keyword_timeseries(engine, d1: date, d2: date, cids: tuple, target_keyword: str) -> pd.DataFrame:
    """✨ 입력한 키워드와 '일치하거나 포함하는' 내부 광고 데이터만 핀셋처럼 뽑아오는 함수"""
    if not table_exists(engine, "fact_keyword_daily"): 
        return pd.DataFrame()
    
    fk_cols = get_table_columns(engine, "fact_keyword_daily")
    has_sales = "sales" in fk_cols
    sales_expr = "SUM(COALESCE(fk.sales, 0))" if has_sales else "0::numeric"
    where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    # 검색용 키워드 (띄어쓰기 무시하고 검색)
    search_kw = f"%{target_keyword.replace(' ', '')}%"

    dim_kw_exists = table_exists(engine, "dim_keyword")
    if dim_kw_exists:
        dk_cols = get_table_columns(engine, "dim_keyword")
        kw_col = next((c for c in ("keyword_name", "keyword", "rel_keyword", "rel_keyword_name", "name") if c in dk_cols), None)
        
        if kw_col:
            sql = f"""
            SELECT fk.dt::date AS dt, SUM(fk.imp) AS imp, SUM(fk.clk) AS clk, SUM(fk.cost) AS cost, {sales_expr} AS sales
            FROM fact_keyword_daily fk
            JOIN dim_keyword dk ON fk.customer_id::text = dk.customer_id::text AND fk.keyword_id::text = dk.keyword_id::text
            WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid}
            AND REPLACE(dk.{kw_col}, ' ', '') ILIKE :kw
            GROUP BY fk.dt::date
            ORDER BY fk.dt::date
            """
            df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
            if df is not None and not df.empty:
                return df
    
    # dim_keyword 조인이 실패하거나 컬럼이 없으면 fact_keyword_daily 자체에서 찾기
    kw_col_fk = next((c for c in ("keyword", "keyword_name", "kw", "query", "keyword_text") if c in fk_cols), None)
    if kw_col_fk:
        sql = f"""
        SELECT fk.dt::date AS dt, SUM(fk.imp) AS imp, SUM(fk.clk) AS clk, SUM(fk.cost) AS cost, {sales_expr} AS sales
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2 {where_cid}
        AND REPLACE(fk.{kw_col_fk}, ' ', '') ILIKE :kw
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "kw": search_kw})
        if df is not None and not df.empty:
            return df

    return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "sales"])

def render_trend_chart(df: pd.DataFrame, keyword: str):
    """ECharts를 이용한 듀얼 축 차트 렌더링"""
    if df.empty or not HAS_ECHARTS:
        st.info("차트를 그릴 데이터가 없거나 ECharts 모듈이 없습니다.")
        return

    x_data = df["dt"].dt.strftime('%m-%d').tolist()
    imp_data = df["노출"].fillna(0).tolist()
    trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()

    options = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {"data": [f"'{keyword}' 내부 노출수", "네이버 시장 트렌드 지수"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": "내부 노출수", "splitLine": {"lineStyle": {"type": "dashed", "color": "#f3f4f6"}}},
            {"type": "value", "name": "트렌드 지수(0~100)", "min": 0, "max": 100, "splitLine": {"show": False}}
        ],
        "series": [
            {
                "name": f"'{keyword}' 내부 노출수", 
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
    st.caption("네이버 전체 유저의 '실제 검색량 추이'와 **우리 광고의 특정 키워드 노출수**를 1:1로 겹쳐보며 인사이트를 발굴합니다.")
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
    
    c1, c2 = st.columns([1, 2])
    with c1:
        target_keyword = st.text_input("🔍 분석할 주력 키워드 입력", value="", placeholder="예: 나이키운동화").strip()
    
    if not target_keyword:
        st.info("분석을 원하시는 주력 키워드를 위에 입력해 주세요.")
        return

    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 더 길게 잡아주세요.")
        return

    with st.spinner(f"'{target_keyword}' 관련 시장 트렌드와 내부 데이터를 가져오는 중입니다..."):
        # ✨ 업그레이드: 입력한 키워드와 연관된 내부 데이터만 정확하게 필터링해서 가져옴!
        try:
            internal_ts = get_specific_keyword_timeseries(engine, d1, d2, cids, target_keyword)
        except Exception as e:
            st.error(f"내부 데이터 조회 중 오류가 발생했습니다: {e}")
            return
        
        trend_df = get_datalab_trend(client_id, client_secret, target_keyword, d1, d2)
        
        if trend_df.empty:
            st.error("네이버 데이터랩에서 데이터를 가져오지 못했습니다. 키워드 검색량이 너무 적거나, API 키 설정을 확인해 주세요.")
            return

        if internal_ts is not None and not internal_ts.empty:
            internal_ts["dt"] = pd.to_datetime(internal_ts["dt"])
            # 여기서는 이미 get_specific_keyword_timeseries에서 날짜별로 그룹화해서 넘어옵니다.
            agg_internal = internal_ts.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            merged_df = pd.merge(trend_df, agg_internal, on="dt", how="left")
            merged_df["노출"] = merged_df["노출"].fillna(0)
            merged_df["클릭"] = merged_df["클릭"].fillna(0)
            merged_df["광고비"] = merged_df["광고비"].fillna(0)
            
            st.markdown(f"### 📊 [{target_keyword}] 키워드 1:1 비교 차트")
            render_trend_chart(merged_df, target_keyword)
            
            st.markdown("""
            #### 💡 인사이트 해석 가이드
            * **초록색 선(시장 트렌드) 하락 + 파란색 막대(자사 노출수) 하락:** 시장 수요 감소. 무리하게 입찰가를 올리지 마세요.
            * **초록색 선(시장 트렌드) 유지/상승 + 파란색 막대(자사 노출수) 하락:** 🚨경쟁사 입찰가 상승으로 자사 순위가 밀린 상황! 즉시 입찰가를 방어하세요.
            """)
            
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
            st.warning(f"선택하신 기간 동안 우리 계정 내에서 '{target_keyword}' 키워드로 광고가 노출된 기록이 없습니다.")
