def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    today = date.today()
    default_end = today - timedelta(days=1)
    default_start = default_end

    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "", "manager": [], "account": [], "type_sel": [],
            "period_mode": "어제", "d1": default_start, "d2": default_end,
            "top_n_keyword": 300, "top_n_ad": 200, "top_n_campaign": 200, "prefetch_warm": True,
        }
    if "filters_expanded" not in st.session_state:
        st.session_state["filters_expanded"] = True
    sv = st.session_state["filters_v8"]

    managers = sorted([x for x in meta["manager"].dropna().unique().tolist() if str(x).strip()]) if "manager" in meta.columns else []
    accounts = sorted([x for x in meta["account_name"].dropna().unique().tolist() if str(x).strip()]) if "account_name" in meta.columns else []

    with st.expander("🔍 조회 기간 및 필터 설정", expanded=st.session_state.get("filters_expanded", True)):
        st.caption("💡 기본 필터에서 빠르게 조회하고, 필요할 때만 고급 필터를 여세요.")

        manager_sel = sv.get("manager", [])

        basic_col1, basic_col2, basic_col3 = st.columns([1.5, 1.8, 1.7], gap="medium")
        period_mode = basic_col1.selectbox(
            "📅 기간 선택",
            ["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"],
            index=["어제", "오늘", "최근 7일", "이번 달", "지난 달", "직접 선택"].index(sv.get("period_mode", "어제")),
            key="f_period_mode"
        )

        if period_mode == "직접 선택":
            d1 = basic_col2.date_input("시작일", sv.get("d1", default_start), key="f_d1")
            d2 = basic_col3.date_input("종료일", sv.get("d2", default_end), key="f_d2")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 달": d2 = date(today.year, today.month, 1) - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            basic_col2.text_input("시작일", str(d1), disabled=True, key="f_d1_ro")
            basic_col3.text_input("종료일", str(d2), disabled=True, key="f_d2_ro")

        if period_mode == "오늘":
            st.warning("⚠️ '오늘' 데이터는 매체/API 수집 지연으로 일부 지표가 덜 집계될 수 있습니다.")

        if period_mode != "직접 선택":
            st.caption(f"📅 선택 기간: {d1} ~ {d2}")

        try:
            basic_filter_container = st.container(border=True)
        except TypeError:
            # 구버전 Streamlit 호환: border 파라미터 미지원
            basic_filter_container = st.container()

        with basic_filter_container:
            st.markdown("**기본 필터**")
            manager_sel = ui_multiselect(st, "담당자 필터", managers, default=sv.get("manager", []), key="f_manager", placeholder="모든 담당자")

            accounts_by_mgr = accounts
            if manager_sel:
                try:
                    dfm = meta.copy()
                    if "manager" in dfm.columns and "account_name" in dfm.columns:
                        dfm = dfm[dfm["manager"].astype(str).isin([str(x) for x in manager_sel])]
                        accounts_by_mgr = sorted([x for x in dfm["account_name"].dropna().unique().tolist() if str(x).strip()])
                except Exception:
                    pass

            prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
            account_sel = ui_multiselect(st, "광고주(계정) 필터", accounts_by_mgr, default=prev_acc, key="f_account", placeholder="전체 계정 합산보기")

            if st.button("✅ 필터 적용", key="btn_apply_filters", use_container_width=True):
                # 사용자가 명시적으로 접지 않는 한 필터 박스는 유지한다.
                st.session_state["filters_expanded"] = True
                st.rerun()

        with st.expander("고급 필터 (검색/유형)", expanded=False):
            q = st.text_input("텍스트 검색", sv.get("q", ""), key="f_q", placeholder="찾고 싶은 키워드나 캠페인 이름을 입력하세요")
            type_sel = ui_multiselect(st, "광고 유형 필터", type_opts, default=sv.get("type_sel", []), key="f_type_sel", placeholder="모든 광고 보기")

    sv.update({"q": q or "", "manager": manager_sel or [], "account": account_sel or [], "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2})
    st.session_state["filters_v8"] = sv

    prev_manager_count = len(st.session_state.get("_prev_manager_sel", []))
    prev_account_count = len(st.session_state.get("_prev_account_sel", []))
    cur_manager_count = len(manager_sel or [])
    cur_account_count = len(account_sel or [])
    st.session_state["_prev_manager_sel"] = manager_sel or []
    st.session_state["_prev_account_sel"] = account_sel or []

    # 담당자/계정 선택 시 필터가 자동으로 접히지 않도록 동작 제거

    cids = resolve_customer_ids(meta, manager_sel, account_sel)

    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "ready": True,
    }
