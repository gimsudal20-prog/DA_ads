# -*- coding: utf-8 -*-
"""view_media.py - Media/Placement performance analysis and CSV ingestion."""

import time
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import text
from datetime import date

from data import sql_read, table_exists, get_meta, _sql_in_str_list
from ui import render_big_table

def page_media(engine, f):
    st.markdown("<div class='nv-sec-title'>🌐 매체(지면) 효율 분석</div>", unsafe_allow_html=True)
    meta = get_meta(engine)
    
    # --- 1. 다차원보고서 CSV 자동 수집/적재 엔진 ---
    with st.expander("📁 다차원보고서(CSV) 자동 적재 엔진 열기", expanded=False):
        st.info("💡 여러 업체의 데이터가 섞이지 않도록, 먼저 [👤담당자 ➡️ 🏢업체]를 순서대로 선택한 후 CSV를 업로드하세요.")
        
        if meta.empty:
            st.warning("먼저 '설정' 메뉴에서 업체를 동기화해주세요.")
            return
            
        c1, c2 = st.columns(2)
        
        with c1:
            managers = ["전체 담당자"] + sorted([str(x) for x in meta["manager"].dropna().unique().tolist() if str(x).strip()])
            sel_manager = st.selectbox("👤 담당자 선택", managers)
            
        df_filtered = meta.copy()
        if sel_manager != "전체 담당자":
            df_filtered = df_filtered[df_filtered["manager"].astype(str) == sel_manager]
            
        with c2:
            if df_filtered.empty:
                st.warning("배정된 업체가 없습니다.")
                sel_label = None
            else:
                opts = df_filtered[["customer_id", "account_name"]].copy()
                opts["label"] = opts["account_name"] + " (" + opts["customer_id"].astype(str) + ")"
                labels = sorted(opts["label"].tolist())
                label_to_cid = dict(zip(opts["label"], opts["customer_id"].astype(str).tolist()))
                sel_label = st.selectbox("🏢 데이터를 적재할 업체 선택", labels)
        
        if sel_label:
            target_cid = label_to_cid[sel_label]
            uploaded_file = st.file_uploader(f"[{sel_label}] 다차원보고서 CSV 파일 업로드", type=["csv"])
            
            if uploaded_file is not None:
                if st.button("🚀 데이터 적재 및 분석 시작", type="primary", use_container_width=True):
                    with st.spinner("데이터를 분석하고 DB에 적재하는 중입니다..."):
                        try:
                            # CSV 파싱
                            df_csv = pd.read_csv(uploaded_file, skiprows=1)
                            
                            # ✨ [FIX] 네이버 리포트 양식 호환성 패치 (총 전환수, 비용 등 이름 자동 보정)
                            df_csv = df_csv.rename(columns={
                                '총 전환수': '전환수',
                                '총 전환매출액(원)': '전환매출액(원)',
                                '비용(VAT포함,원)': '총비용(VAT포함,원)',
                                '총 비용(VAT포함,원)': '총비용(VAT포함,원)'
                            })
                            
                            # 만약 누락된 필수 컬럼이 있다면 0으로 채워서 에러 방지
                            for required_col in ['전환수', '전환매출액(원)', '총비용(VAT포함,원)', '노출수', '클릭수']:
                                if required_col not in df_csv.columns:
                                    df_csv[required_col] = 0
                            
                            cols_to_sum = ['노출수', '클릭수', '총비용(VAT포함,원)', '전환수', '전환매출액(원)']
                            for c in cols_to_sum:
                                df_csv[c] = pd.to_numeric(df_csv[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                            
                            # 일별 날짜 파싱 (yyyy.mm.dd. 형식 제거)
                            df_csv['dt'] = pd.to_datetime(df_csv['일별'].astype(str).str.replace(r'\.$', '', regex=True).str.replace('.', '-'), errors='coerce').dt.date
                            df_csv = df_csv.dropna(subset=['dt', '매체이름'])
                            
                            # 캠페인유형 분류 (CSV에 포함되어 있음)
                            if '캠페인유형' not in df_csv.columns:
                                df_csv['캠페인유형'] = '기타'
                                
                            grp = df_csv.groupby(['dt', '캠페인유형', '매체이름'])[cols_to_sum].sum().reset_index()
                            
                            rows = []
                            for _, r in grp.iterrows():
                                # 부가세 제외 금액으로 환산
                                cost = int(round(float(r['총비용(VAT포함,원)']) / 1.1)) 
                                rows.append({
                                    "dt": r['dt'],
                                    "customer_id": target_cid,
                                    "campaign_type": str(r['캠페인유형']),
                                    "media_name": str(r['매체이름']),
                                    "imp": int(r['노출수']),
                                    "clk": int(r['클릭수']),
                                    "cost": cost,
                                    "conv": float(r['전환수']),
                                    "sales": int(float(r['전환매출액(원)']))
                                })
                            
                            with engine.begin() as conn:
                                res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='fact_media_daily'"))
                                cols = [r[0] for r in res]
                                if cols and 'customer_id' not in cols:
                                    conn.execute(text("DROP TABLE fact_media_daily"))
                                    
                                conn.execute(text("""
                                    CREATE TABLE IF NOT EXISTS fact_media_daily (
                                        dt DATE,
                                        customer_id TEXT,
                                        campaign_type TEXT,
                                        media_name TEXT,
                                        imp BIGINT,
                                        clk BIGINT,
                                        cost BIGINT,
                                        conv DOUBLE PRECISION,
                                        sales BIGINT DEFAULT 0,
                                        PRIMARY KEY(dt, customer_id, campaign_type, media_name)
                                    )
                                """))
                                
                                if rows:
                                    sql = """
                                    INSERT INTO fact_media_daily (dt, customer_id, campaign_type, media_name, imp, clk, cost, conv, sales)
                                    VALUES (:dt, :customer_id, :campaign_type, :media_name, :imp, :clk, :cost, :conv, :sales)
                                    ON CONFLICT (dt, customer_id, campaign_type, media_name) DO UPDATE SET
                                    imp = fact_media_daily.imp + EXCLUDED.imp, 
                                    clk = fact_media_daily.clk + EXCLUDED.clk, 
                                    cost = fact_media_daily.cost + EXCLUDED.cost, 
                                    conv = fact_media_daily.conv + EXCLUDED.conv, 
                                    sales = fact_media_daily.sales + EXCLUDED.sales
                                    """
                                    conn.execute(text(sql), rows)
                            
                            if "_table_names_cache" in st.session_state:
                                del st.session_state["_table_names_cache"]
                                    
                            st.success(f"🎉 '{sel_label}' 업체의 지면 데이터 {len(rows)}건이 완벽하게 분리 적재되었습니다! 1.5초 뒤 화면이 새로고침됩니다.")
                            time.sleep(1.5)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")

    # --- 2. DB 데이터 불러오기 및 대시보드 렌더링 ---
    if not table_exists(engine, "fact_media_daily"):
        st.warning("🚨 데이터베이스에 매체(지면) 데이터가 없습니다. 위의 업로드 창을 통해 CSV 파일을 적재해주세요.")
        return
        
    d1, d2 = f["start"], f["end"]
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql = f"""
    SELECT campaign_type AS "캠페인유형", media_name AS "매체이름", SUM(imp) AS "노출수", SUM(clk) AS "클릭수", SUM(cost) AS "광고비", SUM(conv) AS "전환수", SUM(sales) AS "전환매출"
    FROM fact_media_daily
    WHERE dt BETWEEN :d1 AND :d2 {where_cid}
    GROUP BY campaign_type, media_name
    """
    
    try:
        df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
    except Exception:
        df = pd.DataFrame()
    
    if df.empty:
        try:
            minmax = sql_read(engine, "SELECT MIN(dt) as min_dt, MAX(dt) as max_dt FROM fact_media_daily")
            min_dt = minmax.iloc[0]['min_dt']
            max_dt = minmax.iloc[0]['max_dt']
            st.error(f"⚠️ 선택하신 업체 필터와 조회 기간({d1} ~ {d2}) 에는 지면 데이터가 없습니다.")
            st.info(f"💡 현재 DB에는 **{min_dt} ~ {max_dt}** 기간의 지면 데이터가 적재되어 있습니다. 좌측 필터를 변경하시거나, 해당 업체의 데이터를 새로 올려주세요!")
        except Exception:
            st.info(f"조건에 맞는 지면 데이터가 없습니다.")
        return

    if type_sel:
        df = df[df["캠페인유형"].isin(type_sel)]
        if df.empty:
            st.warning("선택하신 광고 유형 필터에 해당하는 지면 데이터가 없습니다.")
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
