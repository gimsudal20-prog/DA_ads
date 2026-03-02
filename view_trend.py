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
import altair as alt  # ✨ 스트림릿 내장 차트 라이브러리 추가 (무조건 작동 보장)

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

    # ✨ API 키에 섞여 들어간 따옴표(')나 쌍따옴표(") 자동 제거
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
        # ✨ 에러 발생 시 숨기지 않고 명확하게 원인을 화면에 출력
        err_msg = str(e)
        if hasattr(e, 'read'):
            try:
                err_msg += " | " + e.read().decode('utf-8')
            except: pass
        st.error(f"🚨 네이버 데이터랩 API 통신 에러: {err_msg}")
    
    return pd.DataFrame()

def render_trend_chart(df: pd.DataFrame, internal_name: str, datalab_name: str, ad_type_label: str):
    if df.empty:
        return

    legend_name = f"자사 노출수 ({ad_type_label})"

    # 1. ECharts가 설치되어 있으면 고급 차트로 렌더링
    if HAS_ECHARTS:
        x_data = df["dt"].dt.strftime('%m-%d').tolist()
        imp_data = df["노출"].fillna(0).tolist()
        trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()
        
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
    
    # 2. ✨ ECharts 모듈이 없을 때(가장 유력한 원인) 무조건 작동하는 안전한 내장 차트(Altair)로 우회 렌더링!
    else:
        base = alt.Chart(df).encode(x=alt.X("dt:T", axis=alt.Axis(title="날짜", format="%m-%d")))
        
        # 파란색 막대 (노출수)
        bar = base.mark_bar(color="#3B82F6", opacity=0.7, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            y=alt.Y("노출:Q", axis=alt.Axis(title="자사 노출수", titleColor="#3B82F6"))
        )
        
        # 초록색 선 (트렌드지수)
        line = base.mark_line(color="#10B981", strokeWidth=3).encode(
            y=alt.Y("트렌드지수(%):Q", axis=alt.Axis(title="네이버 트렌드 지수(0~100)", titleColor="#10B981"))
        )
        points = line.mark_circle(size=70, color="#10B981")
        
        # 듀얼 축 결합
        chart = alt.layer(bar, line + points).resolve_scale(
            y='independent'
        ).properties(
            height=400
        ).configure_axis(
            labelFontSize=11,
            titleFontSize=13
        ).configure_legend(
            orient='bottom'
        )
        st.altair_chart(chart, use_container_width=True)

def page_trend(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    
    st.markdown("## 📈 시장 트렌드 vs 자사 노출 분석")
    st.caption("내부 광고 데이터에서 실제 등록된 키워드나 상품을 선택하고, 해당 검색어의 네이버 시장 트렌드(데이터랩)와 1:1로 겹쳐봅니다.")
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
    
    if (d2 - d1).days < 2:
        st.warning("트렌드 비교는 최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 더 길게 잡아주세요.")
        return

    ad_type_sel = st.radio("🎯 분석할 광고 영역 선택", ["파워링크 (키워드)", "쇼핑검색 (상품명)"], horizontal=True)
    is_powerlink = "파워링크" in ad_type_sel

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

    bundle = _perf_common_merge_meta(bundle, meta)
    
    item_list = bundle[name_col].dropna().astype(str).unique().tolist()
    item_list = sorted([x for x in item_list if x.strip()])
    
    if not item_list:
        st.info("데이터가 충분하지 않습니다.")
        return

    st.markdown("### 🔍 분석 대상 선택")
    c1, c2 = st.columns(2)
    with c1:
        selected_internal = st.selectbox("1️⃣ 자사 키워드/상품명 선택 (내부 데이터)", item_list)
    
    with c2:
        st.caption("※ 네이버 데이터랩은 검색어가 길면(상품명 등) 조회가 안 됩니다. 핵심 키워드만 남겨주세요.")
        selected_datalab = st.text_input("2️⃣ 데이터랩 트렌드 검색어 (수정 가능)", value=selected_internal)

    if not selected_internal or not selected_datalab:
        return

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

    id_col = "keyword_id" if is_powerlink else "ad_id"
    if id_col not in source_df.columns:
        st.error(f"내부 에러: '{id_col}' 고유 식별 컬럼을 찾을 수 없습니다.")
        return
        
    clean_ids = set()
    for x in source_df[id_col].dropna():
        sx = str(x).strip()
        if sx.endswith(".0"): sx = sx[:-2]
        if sx: clean_ids.add(sx)
        
    target_ids = list(clean_ids)
    if not target_ids:
        st.warning("선택된 검색어에 해당하는 고유 ID가 없습니다.")
        return

    table_name = "fact_keyword_daily" if is_powerlink else "fact_ad_daily"
    cols = get_table_columns(engine, table_name)
    sales_expr = "sales" if "sales" in cols else "0::numeric"
    
    ids_sql = _sql_in_str_list(target_ids)
    cid_where = f"AND customer_id::text IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql = f"""
    SELECT dt::date AS dt, SUM(imp) AS imp, SUM(clk) AS clk, SUM(cost) AS cost, SUM(COALESCE({sales_expr}, 0)) AS sales
    FROM {table_name}
    WHERE dt BETWEEN :d1 AND :d2
      {cid_where}
      AND {id_col}::text IN ({ids_sql})
    GROUP BY dt::date
    ORDER BY dt::date
    """
    
    with st.spinner("트렌드 차트를 그리는 중..."):
        daily_internal = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
        trend_df = get_datalab_trend(client_id, client_secret, selected_datalab, d1, d2)

    if trend_df.empty:
        st.error(f"⚠️ 네이버 데이터랩에서 '{selected_datalab}' 검색어의 트렌드를 가져오지 못했습니다. 검색어가 너무 길거나, 데이터랩 API 오류일 수 있습니다.")
        return

    if daily_internal is None:
        st.error("⚠️ 내부 일자별 데이터를 조회하는 중 오류가 발생했습니다.")
        return
        
    if daily_internal.empty:
        st.warning(f"⚠️ 선택하신 기간({d1}~{d2}) 동안 '{selected_internal}' 검색어가 노출된 일자별 데이터 기록이 없습니다.")
        return

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
