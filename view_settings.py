# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view."""

from __future__ import annotations
import json
import time
import pandas as pd
import streamlit as st
from sqlalchemy import text

from data import *
from ui import *
from page_helpers import *

PLATFORMS = ["meta", "danggeun", "google", "kakao", "criteo"]

def _platform_label(p: str) -> str:
    mapping = {
        "meta": "메타",
        "danggeun": "당근",
        "google": "구글",
        "kakao": "카카오",
        "criteo": "크리테오",
    }
    return mapping.get(p, p)

def _customer_options(engine):
    meta = get_meta(engine)
    if meta is None or meta.empty or "customer_id" not in meta.columns:
        return {}
    opts = {}
    for _, row in meta[["customer_id", "account_name"]].drop_duplicates().iterrows():
        cid = str(row.get("customer_id", "")).strip()
        name = str(row.get("account_name", "")).strip()
        label = f"{name} ({cid})" if name else cid
        if cid:
            opts[label] = cid
    return opts

def _render_platform_form(engine, platform: str):
    st.markdown(f"### {_platform_label(platform)} 연결")
    ensure_platform_credentials_table(engine)

    rows = get_platform_credentials(engine, platform)
    customer_opts = _customer_options(engine)
    customer_labels = ["선택 안 함"] + list(customer_opts.keys())

    with st.form(f"platform_form_{platform}", clear_on_submit=False):
        left, right = st.columns(2)
        account_label = left.text_input("연결 이름", key=f"{platform}_account_label")
        selected_customer_label = left.selectbox("내부 계정 연결", customer_labels, key=f"{platform}_customer_sel")
        account_id = right.text_input("광고계정 ID", key=f"{platform}_account_id")
        app_id = left.text_input("App ID / Client ID", key=f"{platform}_app_id")
        app_secret = right.text_input("App Secret / Client Secret", key=f"{platform}_app_secret", type="password")
        access_token = left.text_area("Access Token", key=f"{platform}_access_token", height=110)
        refresh_token = right.text_area("Refresh Token", key=f"{platform}_refresh_token", height=110)
        extra_json_raw = st.text_area("추가 설정 (JSON)", key=f"{platform}_extra_json", value="{}", height=100)
        is_active = st.checkbox("활성화", value=True, key=f"{platform}_is_active")

        submitted = st.form_submit_button("저장", use_container_width=True, type="primary")
        if submitted:
            try:
                customer_id = None if selected_customer_label == "선택 안 함" else customer_opts.get(selected_customer_label)
                try:
                    parsed_extra = json.loads(extra_json_raw) if str(extra_json_raw).strip() else {}
                except Exception:
                    st.error("추가 설정(JSON) 형식이 올바르지 않습니다.")
                    st.stop()

                upsert_platform_credential(
                    engine,
                    {
                        "platform": platform,
                        "account_label": account_label,
                        "customer_id": customer_id,
                        "account_id": account_id,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "app_id": app_id,
                        "app_secret": app_secret,
                        "extra_json": parsed_extra,
                        "is_active": is_active,
                    },
                )
                st.success(f"{_platform_label(platform)} 연결 정보를 저장했습니다.")
                time.sleep(0.3)
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    if rows is None or rows.empty:
        st.info("저장된 연결 정보가 없습니다.")
        return

    display_df = rows.copy()
    if "access_token" in display_df.columns:
        display_df["access_token"] = display_df["access_token"].apply(mask_secret)
    if "refresh_token" in display_df.columns:
        display_df["refresh_token"] = display_df["refresh_token"].apply(mask_secret)
    if "app_secret" in display_df.columns:
        display_df["app_secret"] = display_df["app_secret"].apply(mask_secret)
    if "extra_json" in display_df.columns:
        display_df["extra_json"] = display_df["extra_json"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else str(x))

    show_cols = [c for c in ["id", "account_label", "customer_id", "account_id", "is_active", "updated_at", "access_token", "refresh_token"] if c in display_df.columns]
    st.dataframe(display_df[show_cols], use_container_width=True, hide_index=True)

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    st.markdown("#### 저장된 연결 관리")

    for _, row in rows.iterrows():
        rid = int(row["id"])
        label = f"{row['account_label']} · {str(row.get('account_id', '')).strip() or '계정 ID 없음'}"
        with st.expander(label, expanded=False):
            c1, c2, c3 = st.columns(3)
            is_active = bool(row.get("is_active", True))
            if c1.button("활성화/비활성 전환", key=f"toggle_{platform}_{rid}", use_container_width=True):
                try:
                    toggle_platform_credential(engine, rid, not is_active)
                    st.rerun()
                except Exception as e:
                    st.error(f"상태 변경 실패: {e}")
            if c2.button("삭제", key=f"delete_{platform}_{rid}", use_container_width=True):
                try:
                    delete_platform_credential(engine, rid)
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 실패: {e}")
            c3.caption(f"현재 상태: {'활성' if is_active else '비활성'}")

def page_settings(engine) -> None:
    st.markdown("## 설정 및 시스템 관리")
    try:
        db_ping(engine)
        st.success("DB 연결 상태: 정상")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    st.markdown("<div style='margin-bottom: 24px;'></div>", unsafe_allow_html=True)

    tab_roas, tab_sync, tab_conn, tab_system = st.tabs(["운영 설정 (ROAS)", "데이터 동기화", "매체 연결", "시스템 관리"])

    with tab_roas:
        st.markdown("### 캠페인별 ROAS 목표 설정")
        st.caption("담당자와 업체를 차례로 선택한 뒤 표에 숫자를 입력하면 자동으로 저장됩니다.")

        meta = get_meta(engine)
        if not meta.empty and "manager" in meta.columns:
            col1, col2, col3 = st.columns(3)

            with col1:
                manager_list = [m for m in meta["manager"].dropna().unique() if str(m).strip()]
                selected_manager = st.selectbox("1. 담당자 선택", options=["선택하세요"] + sorted(manager_list))

            selected_acc = "선택하세요"
            selected_type = "전체"
            acc_opts = {}
            df_camp_cid = pd.DataFrame()
            cid = None

            with col2:
                if selected_manager != "선택하세요":
                    filtered_meta = meta[meta["manager"] == selected_manager]
                    acc_list = filtered_meta[["customer_id", "account_name"]].drop_duplicates().to_dict("records")
                    acc_opts = {f"{r['account_name']} ({r['customer_id']})": r["customer_id"] for r in acc_list}
                    selected_acc = st.selectbox("2. 업체 선택", options=["선택하세요"] + list(acc_opts.keys()))
                else:
                    st.selectbox("2. 업체 선택", options=["담당자를 먼저 선택하세요"], disabled=True)

            with col3:
                if selected_acc != "선택하세요":
                    cid = acc_opts[selected_acc]
                    df_camp = load_dim_campaign(engine)
                    df_camp_cid = df_camp[df_camp["customer_id"].astype(str) == str(cid)].copy()

                    if not df_camp_cid.empty:
                        cp_col = "campaign_tp" if "campaign_tp" in df_camp_cid.columns else ("campaign_type_label" if "campaign_type_label" in df_camp_cid.columns else "campaign_type")
                        if cp_col in df_camp_cid.columns:
                            mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
                            df_camp_cid["type_kor"] = df_camp_cid[cp_col].apply(lambda x: mapping.get(str(x).upper(), str(x)) if pd.notna(x) else "기타")
                        else:
                            df_camp_cid["type_kor"] = "기타"

                        type_list = [t for t in df_camp_cid["type_kor"].dropna().unique() if str(t).strip()]
                        selected_type = st.selectbox("3. 캠페인 유형 선택", options=["전체"] + sorted(type_list))
                    else:
                        st.selectbox("3. 캠페인 유형 선택", options=["데이터 없음"], disabled=True)
                else:
                    st.selectbox("3. 캠페인 유형 선택", options=["업체를 먼저 선택하세요"], disabled=True)

            if selected_acc != "선택하세요" and not df_camp_cid.empty:
                ensure_target_roas_column(engine)

                df_camp_fresh = load_dim_campaign(engine)
                df_camp_cid_fresh = df_camp_fresh[df_camp_fresh["customer_id"].astype(str) == str(cid)].copy()

                if "min_roas" not in df_camp_cid_fresh.columns:
                    df_camp_cid_fresh["min_roas"] = 0.0
                if "target_roas" not in df_camp_cid_fresh.columns:
                    df_camp_cid_fresh["target_roas"] = 0.0

                cp_col = "campaign_tp" if "campaign_tp" in df_camp_cid_fresh.columns else ("campaign_type_label" if "campaign_type_label" in df_camp_cid_fresh.columns else "campaign_type")
                if cp_col in df_camp_cid_fresh.columns:
                    mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
                    df_camp_cid_fresh["type_kor"] = df_camp_cid_fresh[cp_col].apply(lambda x: mapping.get(str(x).upper(), str(x)) if pd.notna(x) else "기타")
                else:
                    df_camp_cid_fresh["type_kor"] = "기타"

                edit_df = df_camp_cid_fresh[df_camp_cid_fresh["type_kor"] == selected_type].copy() if selected_type != "전체" else df_camp_cid_fresh.copy()

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
                            "target_roas": st.column_config.NumberColumn("목표 ROAS (%)", min_value=0.0, step=10.0, format="%.1f"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )

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

    with tab_conn:
        st.markdown("### 매체 연결 준비")
        st.caption("메타, 당근, 구글, 카카오, 크리테오 연결 정보를 미리 저장해두는 단계입니다. 아직 실제 수집 호출은 붙이지 않아도 됩니다.")
        tabs = st.tabs([_platform_label(p) for p in PLATFORMS])
        for p, t in zip(PLATFORMS, tabs):
            with t:
                _render_platform_form(engine, p)

    with tab_system:
        st.markdown("### 대시보드 캐시 초기화")
        st.caption("과거 데이터가 화면에 계속 남아있거나, 최신 데이터가 반영되지 않을 때 사용하세요.")
        if st.button("캐시 비우기 및 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.success("캐시가 초기화되었습니다.")
            time.sleep(0.5)
            st.rerun()

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
                        "CREATE INDEX IF NOT EXISTS idx_fad_cid ON fact_ad_daily(customer_id);",
                    ]
                    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                        for idx in indexes:
                            try:
                                conn.execute(text(idx))
                            except Exception:
                                pass
                    st.success("DB 최적화가 완료되었습니다.")
                    time.sleep(2)
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"최적화 중 오류 발생: {e}")

        st.divider()

        st.markdown("### DB 찌꺼기 대청소 (VACUUM ANALYZE)")
        st.caption("중복 데이터 제거 후 보이지 않는 빈 공간을 압축하여 리소스 경고를 해결하고 DB 성능을 회복시킵니다.")
        if st.button("DB 대청소 및 튜닝 실행", type="secondary"):
            with st.spinner("DB 대청소 중... (안전하게 백그라운드에서 진행됩니다)"):
                try:
                    tables_to_vacuum = ["fact_keyword_daily", "fact_ad_daily", "fact_campaign_daily", "dim_customer", "dim_campaign", "dim_ad"]
                    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                        for tbl in tables_to_vacuum:
                            try:
                                conn.execute(text(f"VACUUM ANALYZE {tbl};"))
                            except Exception:
                                pass
                    st.success("DB 대청소 및 튜닝이 완료되었습니다.")
                except Exception as e:
                    st.error(f"청소 중 오류 발생: {e}")
