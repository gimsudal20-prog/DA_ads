# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view."""

from __future__ import annotations
import time
import pandas as pd
import streamlit as st
from sqlalchemy import text

from data import *
from ui import *
from page_helpers import *

def page_settings(engine) -> None:
    st.markdown("## 설정 및 데이터 관리")
    try: db_ping(engine); st.success("DB 연결 상태: 정상")
    except Exception as e: st.error(f"DB 연결 실패: {e}"); return
    
    st.markdown("### accounts.xlsx → DB 동기화")
    st.caption("새로운 계정이 추가되거나 정보가 변경되었을 때 엑셀 파일을 업로드하여 DB를 최신화하세요.")
    
    with st.container():
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
        up = st.file_uploader("accounts.xlsx 업로드 (선택)", type=["xlsx"])
        colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
        with colA: do_sync = st.button("동기화 실행", use_container_width=True, type="primary")
        with colB: 
            if st.button("캐시 비우기", use_container_width=True): st.cache_data.clear(); st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"동기화 완료: {res.get('meta', 0)}건"); st.cache_data.clear(); st.rerun()
        except Exception as e: st.error(f"실패: {e}")

    st.divider()

    st.markdown("### 대시보드 속도 최적화 (인덱스 생성)")
    st.caption("대량의 백필 데이터가 추가되어 대시보드가 느려졌을 때, 이 버튼을 눌러 DB 검색 속도를 복구하세요. (최초 1회만 실행)")
    
    if st.button("DB 목차(인덱스) 최적화 실행", type="secondary"):
        with st.spinner("DB 최적화 진행 중... (데이터량에 따라 최대 1~2분 소요될 수 있습니다)"):
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
                st.success("DB 최적화가 완료되었습니다.")
                time.sleep(2)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"최적화 중 오류 발생: {e}")

    st.divider()

    # 캠페인별 목표 ROAS 설정 섹션 (최소/목표 모두 반영)
    st.markdown("### 캠페인별 ROAS 목표 설정")
    st.caption("계정을 선택하고 각 캠페인의 최소 및 목표 ROAS를 설정하세요. 설정된 데이터는 요약 탭에 반영됩니다.")
    
    meta = get_meta(engine)
    if not meta.empty:
        acc_list = meta[["customer_id", "account_name"]].drop_duplicates().to_dict('records')
        acc_opts = {f"{r['account_name']} ({r['customer_id']})": r['customer_id'] for r in acc_list}
        selected_acc = st.selectbox("계정 선택", options=["선택하세요"] + list(acc_opts.keys()))
        
        if selected_acc != "선택하세요":
            cid = acc_opts[selected_acc]
            df_camp = load_dim_campaign(engine)
            df_camp_cid = df_camp[df_camp['customer_id'].astype(str) == str(cid)].copy()
            
            if not df_camp_cid.empty:
                ensure_target_roas_column(engine)
                df_camp = load_dim_campaign(engine) 
                df_camp_cid = df_camp[df_camp['customer_id'].astype(str) == str(cid)].copy()
                
                if "min_roas" not in df_camp_cid.columns: df_camp_cid["min_roas"] = 0.0
                if "target_roas" not in df_camp_cid.columns: df_camp_cid["target_roas"] = 0.0
                    
                edit_df = df_camp_cid[["campaign_id", "campaign_name", "min_roas", "target_roas"]].copy()
                
                edited_df = st.data_editor(
                    edit_df,
                    column_config={
                        "campaign_id": st.column_config.TextColumn("캠페인 ID", disabled=True),
                        "campaign_name": st.column_config.TextColumn("캠페인명", disabled=True),
                        "min_roas": st.column_config.NumberColumn("최소 ROAS (%)", min_value=0.0, step=10.0, format="%.1f"),
                        "target_roas": st.column_config.NumberColumn("목표 ROAS (%)", min_value=0.0, step=10.0, format="%.1f")
                    },
                    hide_index=True,
                    use_container_width=True
                )
                
                if st.button("ROAS 설정 저장", type="primary"):
                    with st.spinner("저장 중..."):
                        for _, row in edited_df.iterrows():
                            update_campaign_target_roas(engine, int(cid), row["campaign_id"], float(row["target_roas"]), float(row["min_roas"]))
                    st.success("ROAS 설정이 저장되었습니다.")
                    time.sleep(1)
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("해당 계정의 캠페인 데이터가 없습니다.")

    st.divider()

    st.markdown("### DB 찌꺼기 대청소 (VACUUM ANALYZE)")
    st.caption("중복 데이터 제거 후 보이지 않는 빈 공간을 압축하여 리소스 경고를 해결하고 DB 성능을 회복시킵니다.")
    
    if st.button("DB 대청소 및 튜닝 실행", type="secondary"):
        with st.spinner("DB 대청소 중... (안전하게 백그라운드에서 진행됩니다)"):
            try:
                tables_to_vacuum = [
                    "fact_keyword_daily", 
                    "fact_ad_daily", 
                    "fact_campaign_daily", 
                    "dim_customer", 
                    "dim_campaign", 
                    "dim_ad"
                ]
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    for tbl in tables_to_vacuum:
                        try:
                            conn.execute(text(f"VACUUM ANALYZE {tbl};"))
                        except Exception as e:
                            pass 
                st.success("DB 대청소 및 튜닝이 완료되었습니다.")
                time.sleep(2)
            except Exception as e:
                st.error(f"청소 중 오류 발생: {e}")

    st.divider()

    st.markdown("### 데이터 영구 삭제 (Danger Zone)")
    st.caption("삭제가 필요한 계정이 있다면 커스텀 ID(숫자)를 입력해 과거 데이터를 포함하여 완전히 소각하세요. 이 작업은 되돌릴 수 없습니다.")
    
    with st.container():
        st.markdown("<div style='background-color:#FEF2F2; padding:20px; border-radius:12px; border:1px solid #FECACA; margin-bottom:16px;'>", unsafe_allow_html=True)
        
        col_del1, col_del2 = st.columns([2, 1])
        with col_del1:
            del_cid = st.text_input("삭제할 커스텀 ID 입력", placeholder="예: 12345678", label_visibility="collapsed")
            confirm_delete = st.checkbox("데이터가 완전히 삭제되며 복구할 수 없음을 확인했습니다.", key="confirm_delete_chk")
            
        with col_del2:
            st.markdown("<div style='height:2px'></div>", unsafe_allow_html=True) 
            if st.button("영구 삭제 실행", type="primary", use_container_width=True, disabled=not confirm_delete):
                if del_cid.strip() and del_cid.strip().isdigit():
                    try:
                        cid_val = str(del_cid.strip())
                        sql_exec(engine, "DELETE FROM dim_customer WHERE customer_id = :cid", {"cid": int(cid_val)})
                        for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_search_term_daily", "fact_ad_daily", "fact_bizmoney_daily"]:
                            try: sql_exec(engine, f"DELETE FROM {table} WHERE customer_id::text = :cid", {"cid": cid_val})
                            except Exception: pass
                                
                        st.success(f"ID '{del_cid}' 업체의 모든 데이터가 영구 삭제되었습니다.")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"삭제 중 오류 발생: {e}")
                else:
                    st.warning("유효한 숫자 형태의 커스텀 ID를 입력해주세요.")
        st.markdown("</div>", unsafe_allow_html=True)
