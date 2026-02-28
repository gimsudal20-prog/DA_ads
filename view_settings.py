# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view."""

from __future__ import annotations
import time
import pandas as pd
import streamlit as st

from data import *
from ui import *
from page_helpers import *

def page_settings(engine) -> None:
    st.markdown("## ⚙️ 설정 / 연결")
    try: db_ping(engine); st.success("DB 연결 성공 ✅")
    except Exception as e: st.error(f"DB 연결 실패: {e}"); return
    
    st.markdown("### 📌 accounts.xlsx → DB 동기화")
    up = st.file_uploader("accounts.xlsx 업로드(선택)", type=["xlsx"])
    colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
    with colA: do_sync = st.button("🔁 동기화 실행", use_container_width=True)
    with colB: 
        if st.button("🧹 캐시 비우기", use_container_width=True): st.cache_data.clear(); st.rerun()
    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            res = seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"✅ 동기화 완료: {res.get('meta', 0)}건"); st.cache_data.clear(); st.rerun()
        except Exception as e: st.error(f"실패: {e}")

    st.divider()

    st.markdown("### 🗑️ 강제 삭제 도구 (수동 DB 소각)")
    st.caption("동기화 후에도 계속 뜨는 악성 '유령 계정'이 있다면 커스텀 ID(숫자)를 입력해 과거 데이터까지 DB에서 완전히 소각하세요.")
    
    col_del1, col_del2 = st.columns([2, 1])
    with col_del1:
        del_cid = st.text_input("삭제할 커스텀 ID 입력", placeholder="예: 12345678", label_visibility="collapsed")
    with col_del2:
        if st.button("🗑️ 완전 삭제", type="primary", use_container_width=True):
            if del_cid.strip() and del_cid.strip().isdigit():
                try:
                    cid_val = str(del_cid.strip())
                    sql_exec(engine, "DELETE FROM dim_account_meta WHERE customer_id = :cid", {"cid": int(cid_val)})
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
