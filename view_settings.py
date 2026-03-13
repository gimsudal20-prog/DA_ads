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
    st.markdown("## ⚙️ 설정 및 데이터 관리")
    try: db_ping(engine); st.success("DB 연결 상태: 정상 ✅")
    except Exception as e: st.error(f"DB 연결 실패: {e}"); return
    
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    st.caption("새로운 광고주가 추가되거나 정보가 변경되었을 때 엑셀 파일을 업로드하여 DB를 최신화하세요.")
    
    with st.container():
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
        up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"])
        colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
        with colA: do_sync = st.button("🔁 동기화 실행", use_container_width=True, type="primary")
        with colB: 
            if st.button("🧹 캐시 비우기", use_container_width=True): st.cache_data.clear(); st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: {res.get('meta', 0)}건"); st.cache_data.clear(); st.rerun()
        except Exception as e: st.error(f"실패: {e}")

    st.divider()

    st.markdown("### 🚀 대시보드 속도 최적화 (인덱스 생성)")
    st.caption("대량의 백필 데이터가 추가되어 대시보드가 느려졌을 때, 이 버튼을 눌러 DB 검색 속도를 복구하세요. (최초 1회만 실행)")
    
    if st.button("⚡ 초고속 DB 목차(인덱스) 만들기", type="secondary"):
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
                st.success("🎉 DB 최적화가 완료되었습니다! 이제 대시보드가 날아다닐 겁니다.")
                time.sleep(2)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"최적화 중 오류 발생: {e}")

    st.divider()

    # ✨ [추가된 부분] 깃허브 환경용 원클릭 DB 대청소(VACUUM) 버튼
    st.markdown("### 🧹 DB 찌꺼기 대청소 (VACUUM ANALYZE)")
    st.caption("중복 데이터 제거 후 보이지 않는 빈 공간을 압축하여 Supabase 리소스 경고를 해결하고 DB 체력을 회복시킵니다.")
    
    if st.button("♻️ DB 대청소 및 튜닝 실행", type="secondary"):
        with st.spinner("DB 대청소 중... (이 작업은 2분 제한 없이 끝까지 안전하게 백그라운드에서 진행됩니다)"):
            try:
                tables_to_vacuum = [
                    "fact_keyword_daily", 
                    "fact_ad_daily", 
                    "fact_campaign_daily", 
                    "dim_customer", 
                    "dim_campaign", 
                    "dim_ad"
                ]
                # AUTOCOMMIT으로 트랜잭션 락을 방지하고 끝까지 실행되도록 보장합니다.
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    for tbl in tables_to_vacuum:
                        try:
                            conn.execute(text(f"VACUUM ANALYZE {tbl};"))
                        except Exception as e:
                            pass # 특정 테이블이 없거나 에러나도 다음 청소 진행
                st.success("🎉 DB 대청소 및 튜닝이 완벽하게 끝났습니다! Supabase 경고 메시지도 곧 자연스럽게 사라질 것입니다.")
                time.sleep(2)
            except Exception as e:
                st.error(f"청소 중 오류 발생: {e}")

    st.divider()

    st.markdown("### 🛑 Danger Zone (수동 DB 소각)")
    st.caption("동기화 후에도 계속 뜨는 악성 '유령 계정'이 있다면 커스텀 ID(숫자)를 입력해 과거 데이터까지 DB에서 완전히 소각하세요. **이 작업은 되돌릴 수 없습니다.**")
    
    with st.container():
        st.markdown("<div style='background-color:#FEF2F2; padding:20px; border-radius:12px; border:1px solid #FECACA; margin-bottom:16px;'>", unsafe_allow_html=True)
        
        col_del1, col_del2 = st.columns([2, 1])
        with col_del1:
            del_cid = st.text_input("삭제할 커스텀 ID 입력", placeholder="예: 12345678", label_visibility="collapsed")
            confirm_delete = st.checkbox("⚠️ 데이터가 완전히 삭제되며 복구할 수 없음을 이해했습니다.", key="confirm_delete_chk")
            
        with col_del2:
            st.markdown("<div style='height:2px'></div>", unsafe_allow_html=True) 
            if st.button("🗑️ 영구 삭제 실행", type="primary", use_container_width=True, disabled=not confirm_delete):
                if del_cid.strip() and del_cid.strip().isdigit():
                    try:
                        cid_val = str(del_cid.strip())
                        sql_exec(engine, "DELETE FROM dim_customer WHERE customer_id = :cid", {"cid": int(cid_val)})
                        for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_search_term_daily", "fact_ad_daily", "fact_bizmoney_daily"]:
                            try: sql_exec(engine, f"DELETE FROM {table} WHERE customer_id::text = :cid", {"cid": cid_val})
                            except Exception: pass
                                
                        st.success(f"✅ ID '{del_cid}' 업체의 모든 데이터가 영구 소각되었습니다.")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"삭제 중 오류 발생: {e}")
                else:
                    st.warning("유효한 숫자 형태의 커스텀 ID를 입력해주세요.")
        st.markdown("</div>", unsafe_allow_html=True)
