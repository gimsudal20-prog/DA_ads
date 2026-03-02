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

from data import sql_read, get_table_columns, table_exists, _sql_in_str_list

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8-sig')

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

def get_datalab_trend(client_id: str, client_secret: str, keyword: str, start_date: date, end_date: date) -> pd.DataFrame:
    if not client_id or not client_secret: return pd.DataFrame()

    client_id = client_id.replace('"', '').replace("'", "").strip()
    client_secret = client_secret.replace('"', '').replace("'", "").strip()

    url = "https://openapi.naver.com/v1/datalab/search"
    s_date = start_date
    e_date = end_date
    if s_date == e_date: s_date = s_date - timedelta(days=1)

    body = {
        "startDate": s_date.strftime("%Y-%m-%d"),
        "endDate": e_date.strftime("%Y-%m-%d"),
        "timeUnit": "date",
        "keywordGroups": [{"groupName": keyword, "keywords": [keyword]}],
        "device": "", "ages": [], "gender": ""
    }

    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    req.add_header("Content-Type", "application/json")

    try:
        response = urllib.request.urlopen(req, data=json.dumps(body).encode("utf-8"))
        if response.getcode() == 200:
            data = json.loads(response.read().decode('utf-8'))
            if "results" in data and len(data["results"]) > 0:
                df = pd.DataFrame(data["results"][0]["data"])
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

def get_internal_daily_detail(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    df_list = []
    cid_str = _sql_in_str_list(list(cids))
    where_cid = f"AND f.customer_id::text IN ({cid_str})" if cids else ""
    
    has_camp = table_exists(_engine, "dim_campaign")
    has_grp = table_exists(_engine, "dim_adgroup")
    
    if table_exists(_engine, "fact_keyword_daily") and table_exists(_engine, "dim_keyword"):
        f_cols = get_table_columns(_engine, "fact_keyword_daily")
        sales_col = "f.sales" if "sales" in f_cols else "0"
        
        g_join = "LEFT JOIN dim_adgroup g ON dk.customer_id::text = g.customer_id::text AND dk.adgroup_id::text = g.adgroup_id::text" if has_grp else ""
        c_join = "LEFT JOIN dim_campaign c ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text" if (has_grp and has_camp) else ""
        
        c_name = "COALESCE(c.campaign_name, g.campaign_id::text, '알 수 없음')" if (has_grp and has_camp) else "'알 수 없음'"
        g_name = "COALESCE(g.adgroup_name, dk.adgroup_id::text, '알 수 없음')" if has_grp else "dk.adgroup_id::text"

        sql = f"""
        SELECT 
            f.dt::date AS dt,
            {c_name} AS campaign_name,
            {g_name} AS adgroup_name,
            SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost, SUM(COALESCE({sales_col}, 0)) AS sales
        FROM fact_keyword_daily f
        JOIN dim_keyword dk ON f.customer_id::text = dk.customer_id::text AND f.keyword_id::text = dk.keyword_id::text
        {g_join}
        {c_join}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY 1, 2, 3
        """
        try:
            df1 = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
            if df1 is not None and not df1.empty: df_list.append(df1)
        except Exception as e:
            st.error(f"파워링크 데이터 로딩 에러: {e}")

    if table_exists(_engine, "fact_ad_daily") and table_exists(_engine, "dim_ad"):
        f_cols = get_table_columns(_engine, "fact_ad_daily")
        sales_col = "f.sales" if "sales" in f_cols else "0"
        
        g_join = "LEFT JOIN dim_adgroup g ON da.customer_id::text = g.customer_id::text AND da.adgroup_id::text = g.adgroup_id::text" if has_grp else ""
        c_join = "LEFT JOIN dim_campaign c ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text" if (has_grp and has_camp) else ""
        
        c_name = "COALESCE(c.campaign_name, g.campaign_id::text, '알 수 없음')" if (has_grp and has_camp) else "'알 수 없음'"
        g_name = "COALESCE(g.adgroup_name, da.adgroup_id::text, '알 수 없음')" if has_grp else "da.adgroup_id::text"

        sql = f"""
        SELECT 
            f.dt::date AS dt,
            {c_name} AS campaign_name,
            {g_name} AS adgroup_name,
            SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost, SUM(COALESCE({sales_col}, 0)) AS sales
        FROM fact_ad_daily f
        JOIN dim_ad da ON f.customer_id::text = da.customer_id::text AND f.ad_id::text = da.ad_id::text
        {g_join}
        {c_join}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY 1, 2, 3
        """
        try:
            df2 = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
            if df2 is not None and not df2.empty: df_list.append(df2)
        except Exception as e:
            st.error(f"쇼핑검색 데이터 로딩 에러: {e}")

    if df_list:
        res = pd.concat(df_list, ignore_index=True)
        return res.groupby(["dt", "campaign_name", "adgroup_name"], as_index=False)[["imp", "clk", "cost", "sales"]].sum()
    
    return pd.DataFrame()

def render_trend_chart(df: pd.DataFrame, datalab_name: str, ad_type_label: str):
    if df.empty: return
    legend_name = f"내부 선택 그룹 노출수"

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
                {"name": legend_name, "type": "bar", "data": imp_data, "itemStyle": {"color": "#3B82F6", "borderRadius": [4,4,0,0]}},
                {"name": f"네이버 트렌드 ('{datalab_name}')", "type": "line", "yAxisIndex": 1, "data": trend_data, "itemStyle": {"color": "#10B981"}, "lineStyle": {"width": 3}, "symbol": "circle", "symbolSize": 8}
            ]
        }
        st_echarts(options=options, height="400px")
    else:
        base = alt.Chart(df).encode(x=alt.X("dt:T", axis=alt.Axis(title="날짜", format="%m-%d")))
        bar = base.mark_bar(color="#3B82F6", opacity=0.7, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(y=alt.Y("노출:Q", axis=alt.Axis(title="내부 노출수", titleColor="#3B82F6")))
        line = base.mark_line(color="#10B981", strokeWidth=3).encode(y=alt.Y("트렌드지수(%):Q", axis=alt.Axis(title="네이버 트렌드 지수", titleColor="#10B981")))
        points = line.mark_circle(size=70, color="#10B981")
        chart = alt.layer(bar, line + points).resolve_scale(y='independent').properties(height=400).configure_axis(labelFontSize=11, titleFontSize=13).configure_legend(orient='bottom')
        st.altair_chart(chart, use_container_width=True)

def page_trend(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    
    st.markdown("## 📈 시장 트렌드 vs 자사 캠페인/그룹 비교")
    st.caption("궁금한 트렌드 검색어를 입력하고, 비교하고 싶은 내부 캠페인과 그룹을 툭툭 선택하여 지표 추이를 확인하세요.")
    st.divider()

    try:
        client_id = st.secrets.get("NAVER_DATALAB_CLIENT_ID", os.getenv("NAVER_DATALAB_CLIENT_ID", ""))
        client_secret = st.secrets.get("NAVER_DATALAB_CLIENT_SECRET", os.getenv("NAVER_DATALAB_CLIENT_SECRET", ""))
    except Exception:
        client_id = os.getenv("NAVER_DATALAB_CLIENT_ID", "")
        client_secret = os.getenv("NAVER_DATALAB_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        st.warning("⚠️ 네이버 데이터랩 API 키가 설정되지 않았습니다. [Settings] -> [Secrets] 에 키를 등록해주세요.")
        return

    cids = tuple(f.get("selected_customer_ids", []))
    d1, d2 = f["start"], f["end"]
    
    if (d2 - d1).days < 2:
        st.warning("최소 3일 이상의 기간을 선택해야 유의미한 차트가 그려집니다. 좌측 필터에서 기간을 늘려주세요.")
        return

    with st.spinner("내부 캠페인/그룹 목록을 실시간으로 가져오는 중..."):
        df_raw = get_internal_daily_detail(engine, d1, d2, cids)

    if df_raw.empty:
        st.warning("선택하신 기간/계정에 내부 광고 데이터가 없습니다. (왼쪽에서 업체나 기간을 변경해 보세요)")
        return

    with st.container():
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:16px;'>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1.5, 1.5, 1.5])
        
        with c1:
            datalab_kw = st.text_input("🔍 데이터랩 트렌드 검색어", placeholder="예: 나이키운동화").strip()
        
        with c2:
            camps = ["전체"] + sorted([str(x) for x in df_raw["campaign_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
            sel_camp = st.selectbox("🎯 비교할 캠페인 선택", camps)
            
        with c3:
            if sel_camp == "전체":
                grps = ["전체"] + sorted([str(x) for x in df_raw["adgroup_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
            else:
                grps = ["전체"] + sorted([str(x) for x in df_raw[df_raw["campaign_name"] == sel_camp]["adgroup_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
            sel_grp = st.selectbox("🎯 비교할 광고그룹 선택", grps)
        st.markdown("</div>", unsafe_allow_html=True)

    if not datalab_kw:
        st.info("👈 비교할 트렌드 검색어를 입력해주세요.")
        return

    df_filtered = df_raw.copy()
    if sel_camp != "전체": df_filtered = df_filtered[df_filtered["campaign_name"] == sel_camp]
    if sel_grp != "전체": df_filtered = df_filtered[df_filtered["adgroup_name"] == sel_grp]

    if df_filtered.empty:
        st.warning(f"선택하신 캠페인/그룹에 해당하는 기간 내 데이터가 없습니다.")
        return

    df_daily = df_filtered.groupby("dt", as_index=False)[["imp", "clk", "cost", "sales"]].sum()
    df_daily["dt"] = pd.to_datetime(df_daily["dt"])
    df_daily = df_daily.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})

    with st.spinner("네이버 데이터랩 트렌드를 가져오는 중..."):
        trend_df = get_datalab_trend(client_id, client_secret, datalab_kw, d1, d2)

    if trend_df.empty:
        st.error(f"⚠️ 네이버 데이터랩에서 '{datalab_kw}' 트렌드를 가져오지 못했습니다. 검색어가 너무 깁니다.")
        return

    merged_df = pd.merge(trend_df, df_daily, on="dt", how="left").fillna(0)

    st.markdown(f"### 📊 '{datalab_kw}' 트렌드 vs 선택 영역 노출 비교")
    
    st.info("""
    **💡 차트 해석 가이드 (어떻게 읽어야 할까요?)**
    
    차트는 시장 전체의 관심도(초록색 선)와 우리 광고의 노출 성과(파란색 막대)를 동시에 보여줍니다. 
    
    * 📉 **초록선(시장) 하락 + 📉 파란막대(우리) 하락:** 시장 전체의 수요가 줄어드는 비수기입니다. 무리하게 입찰가를 올리지 마세요!
    * 📈 **초록선(시장) 상승/유지 + 📉 파란막대(우리) 하락:** 시장은 여전히 좋은데 우리만 안 보이고 있습니다. 경쟁사에 순위가 밀렸으니 입찰가를 바로 올려서 방어하세요.
    * 📈 **초록선(시장) 상승 + 📈 파란막대(우리) 상승:** 가장 완벽한 성수기입니다. 예산이 중간에 꺼지지 않도록 일일 한도를 넉넉하게 열어두세요.
    """)
    
    render_trend_chart(merged_df, datalab_kw, "전체")
    
    # ✨ 다운로드 버튼 추가
    col1, col2 = st.columns([8, 2])
    with col1: st.markdown("#### 📅 일자별 상세 데이터")
    with col2: st.download_button(label="📥 CSV 다운로드", data=convert_df_to_csv(merged_df), file_name=f'trend_comparison_{datalab_kw}.csv', mime='text/csv', use_container_width=True)
    
    st.dataframe(
        merged_df[["dt", "트렌드지수(%)", "노출", "클릭", "광고비"]].sort_values("dt", ascending=False).style.format({
            "트렌드지수(%)": "{:.1f}", "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}"
        }),
        use_container_width=True, hide_index=True
    )
