# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view (Target ROAS Feature Fixed)."""

from __future__ import annotations
import time
import pandas as pd
import streamlit as st
from sqlalchemy import text

from data import sql_read, sql_exec, db_ping, seed_from_accounts_xlsx

@st.fragment
def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 및 데이터 관리")
    try: 
        db_ping(engine)
    except Exception as e: 
        st.error(f"DB 연결 실패: {e}")
        return
    
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    st.caption("새로운 광고주가 추가되거나 정보가 변경되었을 때 엑셀 파일을 업로드하여 DB를 최신화하세요.")
    
    with st.container():
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
        up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"])
        colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
        with colA: do_sync = st.button("🔁 동기화 실행", use_container_width=True, type="primary")
        with colB: 
            if st.button("🧹 캐시 비우기", use_container_width=True): 
                st.cache_data.clear()
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: {res.get('meta', 0)}건")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
        except Exception as e: 
            st.error(f"실패: {e}")

    st.divider()

    # ====================================================
    # 🎯 완벽 복구된 캠페인별 목표 ROAS 설정
    # ====================================================
    st.markdown("### 🎯 캠페인별 목표 ROAS 설정")
    st.caption("업체를 선택하고 캠페인별 최소/목표 ROAS를 입력하세요. 요약 지면의 '목표 달성 현황'에 즉시 연동됩니다.")

    try:
        with engine.begin() as conn:
            try: conn.execute(text("ALTER TABLE dim_campaign ADD COLUMN target_roas DOUBLE PRECISION;"))
            except Exception: pass
            try: conn.execute(text("ALTER TABLE dim_campaign ADD COLUMN min_roas DOUBLE PRECISION;"))
            except Exception: pass

        # ⚡ 데이터 타입 충돌 완전 해결: JOIN 시 양쪽을 모두 문자열로 형변환(CAST)
        sql = """
            SELECT 
                c.customer_id, 
                COALESCE(cust.account_name, CAST(c.customer_id AS VARCHAR)) as account_name,
                c.campaign_id, 
                c.campaign_name, 
                c.target_roas, 
                c.min_roas 
            FROM dim_campaign c
            LEFT JOIN dim_customer cust ON CAST(c.customer_id AS VARCHAR) = CAST(cust.customer_id AS VARCHAR)
        """
        camp_df = sql_read(engine, sql)

        if not camp_df.empty:
            camp_df = camp_df.sort_values(['account_name', 'campaign_name']).reset_index(drop=True)
            
            # ⚡ 너무 많은 캠페인이 뜨지 않도록 업체별 필터 추가
            accounts = ["전체"] + list(camp_df['account_name'].dropna().unique())
            sel_acc = st.selectbox("🎯 설정할 업체 선택", accounts)
            
            disp_df = camp_df.copy()
            if sel_acc != "전체":
                disp_df = disp_df[disp_df['account_name'] == sel_acc].reset_index(drop=True)

            edited_df = st.data_editor(
                disp_df,
                hide_index=True,
                use_container_width=True,
                height=400,
                column_config={
                    "customer_id": None, 
                    "campaign_id": None, 
                    "account_name": st.column_config.TextColumn("광고주명", disabled=True),
                    "campaign_name": st.column_config.TextColumn("캠페인명", disabled=True, width="large"),
                    "min_roas": st.column_config.NumberColumn("최소 ROAS(%)", min_value=0, step=10, format="%d"),
                    "target_roas": st.column_config.NumberColumn("목표 ROAS(%)", min_value=0, step=10, format="%d")
                },
                key=f"roas_editor_{sel_acc}"
            )

            if st.button("💾 화면의 목표 ROAS 저장", type="primary"):
                with st.spinner("저장 중입니다..."):
                    with engine.begin() as conn:
                        for _, row in edited_df.iterrows():
                            t_roas = row['target_roas']
                            m_roas = row['min_roas']
                            cid = row['customer_id']
                            campid = row['campaign_id']
                            
                            t_val = float(t_roas) if pd.notna(t_roas) and str(t_roas).strip() != "" else None
                            m_val = float(m_roas) if pd.notna(m_roas) and str(m_roas).strip() != "" else None
                            
                            conn.execute(
                                text("UPDATE dim_campaign SET target_roas = :t, min_roas = :m WHERE customer_id = :cid AND campaign_id = :campid"),
                                {"t": t_val, "m": m_val, "cid": str(cid), "campid": str(campid)}
                            )
                st.success("✅ 선택한 항목의 목표 ROAS가 성공적으로 저장되었습니다!")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
        else:
            st.info("💡 설정할 캠페인 정보가 없습니다. 데이터 동기화를 먼저 진행해주세요.")

    except Exception as e:
        st.error(f"목표 ROAS 설정 로딩 중 오류 발생: {e}")

    st.divider()

    st.markdown("### 🚀 대시보드 속도 최적화 (인덱스 생성)")
    st.caption("대량의 데이터가 추가되어 화면이 느려졌을 때 검색 속도를 복구합니다. (최초 1회 권장)")
    
    if st.button("⚡ 초고속 DB 목차 만들기", type="secondary"):
        with st.spinner("DB 최적화 진행 중..."):
            try:
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_fcd_dt ON fact_campaign_daily(dt);",
                    "CREATE INDEX IF NOT EXISTS idx_fkd_dt ON fact_keyword_daily(dt);",
                    "CREATE INDEX IF NOT EXISTS idx_fad_dt ON fact_ad_daily(dt);",
                    "CREATE INDEX IF NOT EXISTS idx_fcd_cid ON fact_campaign_daily(customer_id);",
                    "CREATE INDEX IF NOT EXISTS idx_fkd_cid ON fact_keyword_daily(customer_id);",
                    "CREATE INDEX IF NOT EXISTS idx_fad_cid ON fact_ad_daily(customer_id);"
                ]
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    for idx in indexes:
                        try: conn.execute(text(idx))
                        except Exception: pass
                st.success("🎉 DB 최적화 완료!")
            except Exception as e:
                st.error(f"오류 발생: {e}")

    st.divider()

    st.markdown("### 🧹 DB 찌꺼기 대청소 (VACUUM ANALYZE)")
    if st.button("♻️ DB 대청소 및 튜닝 실행", type="secondary"):
        with st.spinner("DB 대청소 중..."):
            try:
                tables_to_vacuum = ["fact_keyword_daily", "fact_ad_daily", "fact_campaign_daily", "dim_customer", "dim_campaign", "dim_ad"]
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    for tbl in tables_to_vacuum:
                        try: conn.execute(text(f"VACUUM ANALYZE {tbl};"))
                        except Exception: pass
                st.success("🎉 DB 대청소 완료!")
            except Exception as e:
                st.error(f"청소 중 오류: {e}")

    st.divider()

    st.markdown("### 🛑 Danger Zone (수동 데이터 삭제)")
    with st.container():
        col_del1, col_del2 = st.columns([2, 1])
        with col_del1:
            del_cid = st.text_input("삭제할 커스텀 ID 입력", placeholder="예: 12345678", label_visibility="collapsed")
            confirm_delete = st.checkbox("⚠️ 복구 불가 영구 삭제 동의", key="confirm_delete_chk")
        with col_del2:
            if st.button("🗑️ 영구 삭제 실행", type="primary", use_container_width=True, disabled=not confirm_delete):
                if del_cid.strip() and del_cid.strip().isdigit():
                    try:
                        cid_val = str(del_cid.strip())
                        sql_exec(engine, "DELETE FROM dim_customer WHERE customer_id = :cid", {"cid": int(cid_val)})
                        for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_search_term_daily", "fact_ad_daily", "fact_bizmoney_daily"]:
                            try: sql_exec(engine, f"DELETE FROM {table} WHERE customer_id::text = :cid", {"cid": cid_val})
                            except Exception: pass
                        st.success("✅ 삭제 완료!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"오류: {e}")
