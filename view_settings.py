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
    st.markdown("## 설정 및 시스템 관리")
    try: 
        db_ping(engine)
        st.success("DB 연결 상태: 정상")
    except Exception as e: 
        st.error(f"DB 연결 실패: {e}")
        return
    
    st.markdown("<div style='margin-bottom: 24px;'></div>", unsafe_allow_html=True)
    
    # 탭을 활용하여 기능 분리
    tab_roas, tab_sync, tab_system = st.tabs(["운영 설정 (ROAS)", "데이터 동기화", "시스템 관리"])

    # ==========================================
    # 탭 1: 운영 설정 (목표 ROAS 등 비즈니스 로직)
    # ==========================================
    with tab_roas:
        st.markdown("### 캠페인별 ROAS 목표 설정")
        st.caption("담당자와 업체를 차례로 선택한 뒤 표에 숫자를 입력하면 **자동으로 저장**됩니다.")
        
        meta = get_meta(engine)
        if not meta.empty and 'manager' in meta.columns:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                manager_list = [m for m in meta['manager'].dropna().unique() if str(m).strip()]
                selected_manager = st.selectbox("1. 담당자 선택", options=["선택하세요"] + sorted(manager_list))
                
            selected_acc = "선택하세요"
            selected_type = "전체"
            acc_opts = {}
            df_camp_cid = pd.DataFrame()
            cid = None
            
            with col2:
                if selected_manager != "선택하세요":
                    filtered_meta = meta[meta['manager'] == selected_manager]
                    acc_list = filtered_meta[["customer_id", "account_name"]].drop_duplicates().to_dict('records')
                    acc_opts = {f"{r['account_name']} ({r['customer_id']})": r['customer_id'] for r in acc_list}
                    selected_acc = st.selectbox("2. 업체 선택", options=["선택하세요"] + list(acc_opts.keys()))
                else:
                    st.selectbox("2. 업체 선택", options=["담당자를 먼저 선택하세요"], disabled=True)
                    
            with col3:
                if selected_acc != "선택하세요":
                    cid = acc_opts[selected_acc]
                    df_camp = load_dim_campaign(engine)
                    df_camp_cid = df_camp[df_camp['customer_id'].astype(str) == str(cid)].copy()
                    
                    if not df_camp_cid.empty:
                        cp_col = "campaign_tp" if "campaign_tp" in df_camp_cid.columns else ("campaign_type_label" if "campaign_type_label" in df_camp_cid.columns else "campaign_type")
                        if cp_col in df_camp_cid.columns:
                            mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
                            df_camp_cid['type_kor'] = df_camp_cid[cp_col].apply(lambda x: mapping.get(str(x).upper(), str(x)) if pd.notna(x) else "기타")
                        else:
                            df_camp_cid['type_kor'] = "기타"
                            
                        type_list = [t for t in df_camp_cid['type_kor'].dropna().unique() if str(t).strip()]
                        selected_type = st.selectbox("3. 캠페인 유형 선택", options=["전체"] + sorted(type_list))
                    else:
                        st.selectbox("3. 캠페인 유형 선택", options=["데이터 없음"], disabled=True)
                else:
                    st.selectbox("3. 캠페인 유형 선택", options=["업체를 먼저 선택하세요"], disabled=True)

            # 데이터 에디터 렌더링 및 자동 저장 로직
            if selected_acc != "선택하세요" and not df_camp_cid.empty:
                ensure_target_roas_column(engine)
                
                df_camp_fresh = load_dim_campaign(engine) 
                df_camp_cid_fresh = df_camp_fresh[df_camp_fresh['customer_id'].astype(str) == str(cid)].copy()
                
                if "min_roas" not in df_camp_cid_fresh.columns: df_camp_cid_fresh["min_roas"] = 0.0
                if "target_roas" not in df_camp_cid_fresh.columns: df_camp_cid_fresh["target_roas"] = 0.0
                
                cp_col = "campaign_tp" if "campaign_tp" in df_camp_cid_fresh.columns else ("campaign_type_label" if "campaign_type_label" in df_camp_cid_fresh.columns else "campaign_type")
                if cp_col in df_camp_cid_fresh.columns:
                    mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
                    df_camp_cid_fresh['type_kor'] = df_camp_cid_fresh[cp_col].apply(lambda x: mapping.get(str(x).upper(), str(x)) if pd.notna(x) else "기타")
                else:
                    df_camp_cid_fresh['type_kor'] = "기타"
                    
                if selected_type != "전체":
                    edit_df = df_camp_cid_fresh[df_camp_cid_fresh['type_kor'] == selected_type].copy()
                else:
                    edit_df = df_camp_cid_fresh.copy()
                    
                if not edit_df.empty:
                    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
                    
                    edit_df = edit_df[["campaign_id", "type_kor", "campaign_name", "min_roas", "target_roas"]].copy().reset_index(drop=True)
                    
                    edited_df = st.data_editor(
                        edit_df,
                        column_config={
                            "campaign_id": st.column_config.TextColumn("캠페인 ID", disabled=True),
                            "type_kor": st.column_config.TextColumn("캠페인 유형", disabled=True),
                            "campaign_name": st.column_config.TextColumn("캠페인명", disabled=True),
                            "min_roas": st.column_config.NumberColumn("최소 ROAS (%)", min_value=0.0, step=10.0, format="%.1f"),
                            "target_roas": st.column_config.NumberColumn("목표 ROAS (%)", min_value=0.0, step=10.0, format="%.1f")
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # 변경 감지 및 초고속 자동 저장
                    if not edit_df.equals(edited_df):
                        for idx, row in edited_df.iterrows():
                            if row["min_roas"] != edit_df.loc[idx, "min_roas"] or row["target_roas"] != edit_df.loc[idx, "target_roas"]:
                                update_campaign_target_roas(engine, int(cid), row["campaign_id"], float(row["target_roas"]), float(row["min_roas"]))
                        
                        st.cache_data.clear() 
                        st.toast("변경사항이 자동 저장되었습니다.", icon="✅") 
                else:
                    st.info("선택한 캠페인 유형에 해당하는 데이터가 없습니다.")
        else:
            st.info("업체 및 담당자 정보가 없습니다. '데이터 동기화' 탭에서 accounts.xlsx를 먼저 등록해주세요.")

    # ==========================================
    # 탭 2: 데이터 동기화 (외부 파일 연동)
    # ==========================================
    with tab_sync:
        st.markdown("### accounts.xlsx → DB 동기화")
        st.caption("새로운 계정이 추가되거나 정보가 변경되었을 때 엑셀 파일을 업로드하여 DB를 최신화하세요.")
        
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
        up = st.file_uploader("accounts.xlsx 업로드 (선택)", type=["xlsx"])
        do_sync = st.button("동기화 실행", use_container_width=True, type="primary")
        st.markdown("</div>", unsafe_allow_html=True)

        if do_sync:
            try:
                df_src = pd.read_excel(up) if up else None
                res = seed_from_accounts_xlsx(engine, df=df_src)
                st.success(f"동기화 완료: {res.get('meta', 0)}건")
                st.cache_data.clear()
                time.sleep(0.5)
                st.rerun()
            except Exception as e: 
                st.error(f"동기화 실패: {e}")

    # ==========================================
    # 탭 3: 시스템 관리 (최적화, 캐시, 삭제)
    # ==========================================
    with tab_system:
        # 1. 캐시 관리
        st.markdown("### 대시보드 캐시 초기화")
        st.caption("과거 데이터가 화면에 계속 남아있거나, 최신 데이터가 반영되지 않을 때 사용하세요.")
        if st.button("캐시 비우기 및 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.success("캐시가 초기화되었습니다.")
            time.sleep(0.5)
            st.rerun()
            
        st.divider()

        # 2. 인덱스 최적화
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

        # 3. DB 대청소 (VACUUM)
        st.markdown("### DB 찌꺼기 대청소 (VACUUM ANALYZE)")
        st.caption("중복 데이터 제거 후 보이지 않는 빈 공간을 압축하여 리소스 경고를 해결하고 DB 성능을 회복시킵니다.")
        if st.button("DB 대청소 및 튜닝 실행", type="secondary"):
            with st.spinner("DB 대청소 중... (안전하게 백그라운드에서 진행됩니다)"):
                try:
                    tables_to_vacuum = [
                        "fact_keyword_daily", "fact_ad_daily", "fact_campaign_daily", 
                        "dim_customer", "dim_campaign", "dim_ad"
                    ]
                    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                        for tbl in tables_to_vacuum:
                            try: conn.execute(text(f"VACUUM ANALYZE {tbl};"))
                            except Exception: pass 
                    st.success("DB 대청소 및 튜닝이 완료되었습니다.")
                except Exception as e:
                    st.error(f"청소 중 오류 발생: {e}")

        st.divider()

        # 4. 데이터 영구 삭제
        st.markdown("### 데이터 영구 삭제 (Danger Zone)")
        st.caption("삭제가 필요한 계정이 있다면 커스텀 ID(숫자)를 입력해 과거 데이터를 포함하여 완전히 소각하세요. 이 작업은 되돌릴 수 없습니다.")
        
        st.markdown("<div style='background-color:#FEF2F2; padding:20px; border-radius:12px; border:1px solid #FECACA;'>", unsafe_allow_html=True)
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
