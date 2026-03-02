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

from data import query_keyword_timeseries
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
    
    # 데이터랩 API는 최대 1년치, 시작일과 종료일이 같으면 에러를 뱉으므로 하루 전으로 보정
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
        "legend": {"data": ["자사 노출수", "시장 트렌드 지수"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": "노출수", "splitLine": {"lineStyle": {"type": "dashed", "color": "#f3f4f6"}}},
            {"type": "value", "name": "트렌드 지수(0~100)", "min": 0, "max": 100, "splitLine": {"show": False}}
        ],
        "series": [
            {
                "name": "자사 노출수", 
                "type": "bar", 
                "data": imp_data, 
                "itemStyle": {"color": "#3B82F6", "borderRadius": [4,4,0,0]}
            },
            {
                "name": "시장 트렌드 지수", 
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
    st.caption("네이버 전체 유저의 '실제 검색량 추이(데이터랩)'와 우리 광고의 '노출수'를 겹쳐보며, 성과 하락의 원인이 **'시장 수요 감소'**인지 **'자사 순위 하락'**인지 판별합니다.")
    st.divider()

    # ✨ 수정된 부분: 스트림릿 전용 금고(st.secrets)를 먼저 확인하도록 업그레이드!
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
        target_keyword = st.text_input("🔍 분석할 핵심 키워드 입력", value="", placeholder="예: 무지박스, 나이키운동화").strip()
    
    if not target_keyword:
        st.info("분석을 원하시는 키워드를 위에 입력해 주세요.")
        return

    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 더 길게 잡아주세요 (예: 최근 7일, 이번 달).")
        return

    with st.spinner(f"'{target_keyword}' 키워드의 시장 트렌드와 내부 데이터를 조합 중입니다..."):
        type_sel = tuple(["파워링크", "쇼핑검색"])
        try:
            internal_ts = query_keyword_timeseries(engine, d1, d2, cids, type_sel)
        except Exception as e:
            st.error(f"내부 데이터 조회 중 오류가 발생했습니다: {e}")
            return
        
        trend_df = get_datalab_trend(client_id, client_secret, target_keyword, d1, d2)
        
        if trend_df.empty:
            st.error("네이버 데이터랩에서 데이터를 가져오지 못했습니다. 키워드 검색량이 너무 적거나, API 키 설정을 확인해 주세요.")
            return

        if internal_ts is not None and not internal_ts.empty:
            internal_ts["dt"] = pd.to_datetime(internal_ts["dt"])
            agg_internal = internal_ts.groupby("dt", as_index=False)[["imp", "clk", "cost", "sales"]].sum()
            agg_internal = agg_internal.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})
            
            merged_df = pd.merge(trend_df, agg_internal, on="dt", how="left")
            merged_df["노출"] = merged_df["노출"].fillna(0)
            merged_df["클릭"] = merged_df["클릭"].fillna(0)
            merged_df["광고비"] = merged_df["광고비"].fillna(0)
            
            st.markdown(f"### 📊 [{target_keyword}] 트렌드 비교 차트")
            render_trend_chart(merged_df, target_keyword)
            
            st.markdown("""
            #### 💡 인사이트 해석 가이드
            * **초록색 선 (시장 트렌드) 하락 + 파란색 막대 (자사 노출수) 하락:** 시장 전체의 수요가 줄어들고 있습니다. (비수기 진입 등). 무리하게 입찰가를 올리지 마세요.
            * **초록색 선 (시장 트렌드) 유지/상승 + 파란색 막대 (자사 노출수) 하락:** 시장 수요는 충분한데 우리 광고만 안 보입니다! 경쟁사가 입찰가를 높였을 확률이 매우 높습니다. **즉시 입찰가를 점검하세요.**
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
            st.warning("선택하신 기간/계정에 내부(자사) 광고 데이터가 없습니다.")
