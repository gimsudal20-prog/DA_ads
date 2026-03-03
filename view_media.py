# -*- coding: utf-8 -*-
"""view_media.py - Media/Placement performance analysis and CSV ingestion."""

import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import text
from datetime import date

from data import sql_read, table_exists
from ui import render_big_table

def page_media(engine, f):
    st.markdown("<div class='nv-sec-title'>🌐 매체(지면) 효율 분석</div>", unsafe_allow_html=True)
    
    # --- 1. 다차원보고서 CSV 자동 수집/적재 엔진 ---
    with st.expander("📁 다차원보고서(CSV) 자동 적재 엔진 열기", expanded=False):
        st.info("💡 네이버 광고관리자에서 다운로드한 '다차원보고서(RAW_SSA...csv)' 파일을 여기에 끌어다 놓으면 DB에 자동으로 누적 저장됩니다.")
        uploaded_file = st.file_uploader("CSV 파일 선택", type=["csv"])
        
        if uploaded_file is not None:
            if st.button("🚀 데이터 적재 및 분석 시작", type="primary"):
                with st.spinner("데이터를 DB에 적재하는 중입니다..."):
                    try:
                        # CSV 파싱
                        df_csv = pd.read_csv(uploaded_file, skiprows=1)
                        cols_to_sum = ['노출수', '클릭수', '총비용(VAT포함,원)', '전환수', '전환매출액(원)']
                        for c in cols_to_sum:
                            if c in df_csv.columns:
                                df_csv[c] = pd.to_numeric(df_csv[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        
                        df_csv['dt'] = pd.to_datetime(df_csv['일별'].astype(str).str.replace('.', '-'), errors='coerce').dt.date
                        df_csv = df_csv.dropna(subset=['dt', '매체이름'])
                        
                        grp = df_csv.groupby(['dt', '매체이름'])[cols_to_sum].sum().reset_index()
                        
                        rows = []
                        for _, r in grp.iterrows():
                            # 부가세 제외 금액으로 환산하여 DB 저장
                            cost = int(round(float(r['총비용(VAT포함,원)']) / 1.1)) 
                            rows.append({
                                "dt": r['dt'],
                                "media_name": str(r['매체이름']),
                                "imp": int(r['노출수']),
                                "clk": int(r['클릭수']),
                                "cost": cost,
                                "conv": float(r['전환수']),
                                "sales": int(float(r['전환매출액(원)']))
                            })
                        
                        # 테이블 생성 및 데이터 Upsert
                        with engine.begin() as conn:
                            conn.execute(text("""
                                CREATE TABLE IF NOT EXISTS fact_media_daily (
                                    dt DATE,
                                    media_name TEXT,
                                    imp BIGINT,
                                    clk BIGINT,
                                    cost BIGINT,
                                    conv DOUBLE PRECISION,
                                    sales BIGINT DEFAULT 0,
                                    PRIMARY KEY(dt, media_name)
                                )
                            """))
                            
                            if rows:
                                sql = """
                                INSERT INTO fact_media_daily (dt, media_name, imp, clk, cost, conv, sales)
                                VALUES (:dt, :media_name, :imp, :clk, :cost, :conv, :sales)
                                ON CONFLICT (dt, media_name) DO UPDATE SET
                                imp = fact_media_daily.imp + EXCLUDED.imp, 
                                clk = fact_media_daily.clk + EXCLUDED.clk, 
                                cost = fact_media_daily.cost + EXCLUDED.cost, 
                                conv = fact_media_daily.conv + EXCLUDED.conv, 
                                sales = fact_media_daily.sales + EXCLUDED.sales
                                """
                                conn.execute(text(sql), rows)
                                
                        st.success(f"🎉 총 {len(rows)}건의 지면 데이터가 완벽하게 적재되었습니다! 아래 분석 결과를 확인하세요.")
                    except Exception as e:
                        st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")

    # --- 2. DB 데이터 불러오기 및 대시보드 렌더링 ---
    if not table_exists(engine, "fact_media_daily"):
        st.warning("🚨 데이터베이스에 매체(지면) 데이터가 없습니다. 위의 업로드 창을 통해 CSV 파일을 적재해주세요.")
        return
        
    d1, d2 = f["start"], f["end"]
    
    sql = """
    SELECT media_name AS "매체이름", SUM(imp) AS "노출수", SUM(clk) AS "클릭수", SUM(cost) AS "광고비", SUM(conv) AS "전환수", SUM(sales) AS "전환매출"
    FROM fact_media_daily
    WHERE dt BETWEEN :d1 AND :d2
    GROUP BY media_name
    """
    df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
    
    if df.empty:
        st.info(f"{d1} ~ {d2} 기간에 해당하는 지면 데이터가 없습니다.")
        return
        
    for c in ["노출수", "클릭수", "광고비", "전환수", "전환매출"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        
    df["ROAS(%)"] = np.where(df["광고비"] > 0, (df["전환매출"] / df["광고비"]) * 100, 0.0)
    df["CPA(원)"] = np.where(df["전환수"] > 0, df["광고비"] / df["전환수"], 0.0)
    df["CTR(%)"] = np.where(df["노출수"] > 0, (df["클릭수"] / df["노출수"]) * 100, 0.0)
    
    df = df.sort_values("광고비", ascending=False).reset_index(drop=True)
    fmt = {"노출수": "{:,.0f}", "클릭수": "{:,.0f}", "광고비": "{:,.0f}", "전환수": "{:,.1f}", "전환매출": "{:,.0f}", "ROAS(%)": "{:,.2f}%", "CPA(원)": "{:,.0f}", "CTR(%)": "{:,.2f}%"}

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    tab_good, tab_bad, tab_all = st.tabs(["🏆 고효율 지면", "🚨 비용 누수 지면", "📊 전체 지면 성과"])
    
    with tab_good:
        st.markdown("<div style='font-size:15px; font-weight:700; margin-bottom:12px; color:#375FFF;'>ROAS 300% 이상 & 비용 5천 원 이상 사용 매체</div>", unsafe_allow_html=True)
        good = df[(df["ROAS(%)"] >= 300) & (df["광고비"] >= 5000)].sort_values("ROAS(%)", ascending=False)
        if not good.empty:
            render_big_table(good.style.format(fmt), "media_good_table", 400)
        else:
            st.info("조건에 맞는 고효율 지면이 없습니다.")
            
    with tab_bad:
        st.markdown("<div style='font-size:15px; font-weight:700; margin-bottom:12px; color:#FC503D;'>전환 0건 & 비용 1만 원 이상 사용 매체</div>", unsafe_allow_html=True)
        bad = df[(df["전환수"] == 0) & (df["광고비"] >= 10000)].sort_values("광고비", ascending=False)
        if not bad.empty:
            render_big_table(bad.style.format(fmt), "media_bad_table", 400)
        else:
            st.success("비용 누수가 발생하는 지면이 없습니다!")
            
    with tab_all:
        st.markdown("<div style='font-size:15px; font-weight:700; margin-bottom:12px;'>조회 기간 내 전체 지면 성과 (광고비 순)</div>", unsafe_allow_html=True)
        render_big_table(df.style.format(fmt), "media_all_table", 600)
