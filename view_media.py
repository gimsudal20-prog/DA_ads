# -*- coding: utf-8 -*-
"""view_media.py - Media & Region performance analysis and CSV ingestion."""

import time
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import text
from datetime import date

from data import sql_read, table_exists, get_meta, _sql_in_str_list
from ui import render_big_table

def page_media(engine, f):
    st.markdown("<div class='nv-sec-title'>🌐 매체 및 지역 효율 분석</div>", unsafe_allow_html=True)
    meta = get_meta(engine)
    
    with st.expander("📁 다차원보고서(CSV) 덮어쓰기 적재 엔진 열기", expanded=False):
        st.info("💡 겹치는 기간의 데이터를 올리면 기존 데이터는 지워지고 덮어씌워집니다.")
        
        if meta.empty:
            st.warning("먼저 '설정' 메뉴에서 업체를 동기화해주세요.")
            return
            
        c1, c2 = st.columns(2)
        
        with c1:
            # ✨ [FIX 4] 엑셀에 manager 컬럼이 없는 경우 터지지 않도록 예외 처리
            if "manager" in meta.columns:
                managers = ["전체 담당자"] + sorted([str(x) for x in meta["manager"].dropna().unique().tolist() if str(x).strip()])
                sel_manager = st.selectbox("👤 담당자 선택", managers)
            else:
                sel_manager = "전체 담당자"
            
        df_filtered = meta.copy()
        if sel_manager != "전체 담당자" and "manager" in df_filtered.columns:
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
                
                sel_label = st.selectbox("🏢 데이터를 덮어씌울 업체 선택", labels, index=None, placeholder="먼저 업체를 선택해주세요")
        
        if sel_label:
            target_cid = label_to_cid[sel_label]
            uploaded_file = st.file_uploader(f"[{sel_label}] 다차원보고서 CSV 파일 업로드", type=["csv"])
            
            if uploaded_file is not None:
                if st.button("🚀 데이터 덮어쓰기 시작", type="primary", width="stretch"):
                    with st.spinner("데이터를 분석하고 기존 기간 데이터를 교체하는 중입니다..."):
                        try:
                            df_csv = pd.read_csv(uploaded_file, skiprows=1)
                            
                            df_csv = df_csv.rename(columns={
                                '총 전환수': '전환수',
                                '총 전환매출액(원)': '전환매출액(원)',
                                '비용(VAT포함,원)': '총비용(VAT포함,원)',
                                '총 비용(VAT포함,원)': '총비용(VAT포함,원)'
                            })
                            
                            for required_col in ['전환수', '전환매출액(원)', '총비용(VAT포함,원)', '노출수', '클릭수']:
                                if required_col not in df_csv.columns:
                                    df_csv[required_col] = 0
                                    
                            cols_to_sum = ['노출수', '클릭수', '총비용(VAT포함,원)', '전환수', '전환매출액(원)']
                            for c in cols_to_sum:
                                df_csv[c] = pd.to_numeric(df_csv[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                            
                            df_csv['dt'] = pd.to_datetime(df_csv['일별'].astype(str).str.replace(r'\.$', '', regex=True).str.replace('.', '-'), errors='coerce').dt.date
                            df_csv = df_csv.dropna(subset=['dt'])
                            
                            if '캠페인유형' not in df_csv.columns: df_csv['캠페인유형'] = '기타'
                            if '매체이름' not in df_csv.columns: df_csv['매체이름'] = '전체'
                            if '지역' not in df_csv.columns: df_csv['지역'] = '전체'
                                
                            grp = df_csv.groupby(['dt', '캠페인유형', '매체이름', '지역'])[cols_to_sum].sum().reset_index()
                            
                            min_dt = grp['dt'].min()
                            max_dt = grp['dt'].max()
                            
                            rows = []
                            for _, r in grp.iterrows():
                                cost = int(round(float(r['총비용(VAT포함,원)']) / 1.1)) 
                                rows.append({
                                    "dt": r['dt'],
                                    "customer_id": target_cid,
                                    "campaign_type": str(r['캠페인유형']),
                                    "media_name": str(r['매체이름']),
                                    "region_name": str(r['지역']),
                                    "imp": int(r['노출수']),
                                    "clk": int(r['클릭수']),
                                    "cost": cost,
                                    "conv": float(r['전환수']),
                                    "sales": int(float(r['전환매출액(원)']))
                                })
                            
                            with engine.begin() as conn:
                                res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='fact_media_daily'"))
                                cols = [r[0] for r in res]
                                if cols and 'region_name' not in cols:
                                    conn.execute(text("DROP TABLE fact_media_daily"))
                                    
                                conn.execute(text("""
                                    CREATE TABLE IF NOT EXISTS fact_media_daily (
                                        dt DATE,
                                        customer_id TEXT,
                                        campaign_type TEXT,
                                        media_name TEXT,
                                        region_name TEXT,
                                        imp BIGINT,
                                        clk BIGINT,
                                        cost BIGINT,
                                        conv DOUBLE PRECISION,
                                        sales BIGINT DEFAULT 0,
                                        PRIMARY KEY(dt, customer_id, campaign_type, media_name, region_name)
                                    )
                                """))
                                
                                conn.execute(text("DELETE FROM fact_media_daily WHERE customer_id = :cid AND dt BETWEEN :min_dt AND :max_dt"), 
                                             {"cid": target_cid, "min_dt": min_dt, "max_dt": max_dt})
                                
                                if rows:
                                    sql = """
                                    INSERT INTO fact_media_daily (dt, customer_id, campaign_type, media_name, region_name, imp, clk, cost, conv, sales)
                                    VALUES (:dt, :customer_id, :campaign_type, :media_name, :region_name, :imp, :clk, :cost, :conv, :sales)
                                    """
                                    conn.execute(text(sql), rows)
                            
                            if "_table_names_cache" in st.session_state:
                                del st.session_state["_table_names_cache"]
                                    
                            st.success(f"🎉 '{sel_label}' 업체의 {min_dt} ~ {max_dt} 기간 데이터가 완벽하게 교체(덮어쓰기) 되었습니다! 1.5초 뒤 새로고침됩니다.")
                            time.sleep(1.5)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")

    if not table_exists(engine, "fact_media_daily"):
        st.warning("🚨 데이터베이스에 매체/지역 데이터가 없습니다. 위의 업로드 창을 통해 CSV 파일을 적재해주세요.")
        return
        
    d1, d2 = f["start"], f["end"]
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    
    where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
    
    sql = f"""
    SELECT campaign_type AS "캠페인유형", media_name AS "매체이름", region_name AS "지역명", SUM(imp) AS "노출수", SUM(clk) AS "클릭수", SUM(cost) AS "광고비", SUM(conv) AS "전환수", SUM(sales) AS "전환매출"
    FROM fact_media_daily
    WHERE dt BETWEEN :d1 AND :d2 {where_cid}
    GROUP BY campaign_type, media_name, region_name
    """
    
    try:
        df_raw = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})
    except Exception:
        df_raw = pd.DataFrame()
    
    if df_raw.empty:
        try:
            minmax = sql_read(engine, "SELECT MIN(dt) as min_dt, MAX(dt) as max_dt FROM fact_media_daily")
            min_dt = minmax.iloc[0]['min_dt']
            max_dt = minmax.iloc[0]['max_dt']
            st.error(f"⚠️ 선택하신 업체/광고유형 및 조회 기간({d1} ~ {d2}) 에는 데이터가 없습니다.")
            st.info(f"💡 현재 DB에는 **{min_dt} ~ {max_dt}** 기간의 다차원 데이터가 적재되어 있습니다. 좌측 필터를 변경해주세요!")
        except Exception:
            st.info(f"조건에 맞는 데이터가 없습니다.")
        return

    if type_sel:
        df_raw = df_raw[df_raw["캠페인유형"].isin(type_sel)]
        if df_raw.empty:
            st.warning("선택하신 광고 유형 필터에 해당하는 지면 데이터가 없습니다.")
            return
            
    for c in ["노출수", "클릭수", "광고비", "전환수", "전환매출"]:
        df_raw[c] = pd.to_numeric(df_raw[c], errors="coerce").fillna(0)

    df_media = df_raw.groupby("매체이름")[["노출수", "클릭수", "광고비", "전환수", "전환매출"]].sum().reset_index()
    df_region = df_raw.groupby("지역명")[["노출수", "클릭수", "광고비", "전환수", "전환매출"]].sum().reset_index()
    
    def calc_metrics(df):
        df["ROAS(%)"] = np.where(df["광고비"] > 0, (df["전환매출"] / df["광고비"]) * 100, 0.0)
        df["CPA(원)"] = np.where(df["전환수"] > 0, df["광고비"] / df["전환수"], 0.0)
        df["CTR(%)"] = np.where(df["노출수"] > 0, (df["클릭수"] / df["노출수"]) * 100, 0.0)
        return df.sort_values("광고비", ascending=False).reset_index(drop=True)

    df_media = calc_metrics(df_media)
    df_region = calc_metrics(df_region)
    
    fmt = {"노출수": "{:,.0f}", "클릭수": "{:,.0f}", "광고비": "{:,.0f}", "전환수": "{:,.1f}", "전환매출": "{:,.0f}", "ROAS(%)": "{:,.2f}%", "CPA(원)": "{:,.0f}", "CTR(%)": "{:,.2f}%"}

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    tab_media, tab_region, tab_bad = st.tabs(["🌐 지면(매체) 성과 분석", "📍 지역 성과 분석", "🚨 비용 누수 항목 (전환 0건)"])
    
    with tab_media:
        st.markdown("<div style='font-size:15px; font-weight:700; margin-bottom:12px; color:#375FFF;'>조회 기간 내 전체 매체(지면) 효율 리스트</div>", unsafe_allow_html=True)
        render_big_table(df_media.style.format(fmt), "media_table_main", 600)
            
    with tab_region:
        st.markdown("<div style='font-size:15px; font-weight:700; margin-bottom:12px; color:#047857;'>조회 기간 내 지역별 성과 리스트</div>", unsafe_allow_html=True)
        df_region_clean = df_region[~df_region["지역명"].isin(["전체", "-", "알수없음"])].copy()
        if not df_region_clean.empty:
            render_big_table(df_region_clean.style.format(fmt), "region_table_main", 600)
        else:
            st.info("올려주신 데이터에 지역 정보가 구분되어 있지 않거나 데이터가 없습니다.")
            
    with tab_bad:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; color:#FC503D;'>❌ 1만 원 이상 소진 매체 (전환 0건)</div>", unsafe_allow_html=True)
            bad_m = df_media[(df_media["전환수"] == 0) & (df_media["광고비"] >= 10000)].sort_values("광고비", ascending=False)
            if not bad_m.empty: st.dataframe(bad_m[["매체이름", "광고비", "클릭수", "CTR(%)"]].style.format(fmt), hide_index=True, width="stretch")
            else: st.success("비용 누수 매체가 없습니다!")
            
        with col2:
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; color:#FC503D;'>❌ 1만 원 이상 소진 지역 (전환 0건)</div>", unsafe_allow_html=True)
            bad_r = df_region[(df_region["전환수"] == 0) & (df_region["광고비"] >= 10000) & (~df_region["지역명"].isin(["전체", "-", "알수없음"]))].sort_values("광고비", ascending=False)
            if not bad_r.empty: st.dataframe(bad_r[["지역명", "광고비", "클릭수", "CTR(%)"]].style.format(fmt), hide_index=True, width="stretch")
            else: st.success("비용 누수 지역이 없습니다!")
