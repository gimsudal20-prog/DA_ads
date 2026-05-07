            "업체명": st.column_config.TextColumn("업체명", disabled=True),
            "담당자": st.column_config.TextColumn("담당자", disabled=True),
            "월 예산": st.column_config.TextColumn(
                "월 예산(원)", 
                help="더블클릭하여 예산을 바로 수정하세요.",
                required=True
            ),
            f"{end_dt.month}월 사용액": st.column_config.TextColumn(
                f"{end_dt.month}월 사용액", 
                disabled=True
            ),
            f"{prev_m_num}월 사용액": st.column_config.TextColumn(
                f"{prev_m_num}월 사용액", 
                disabled=True
            ),
            "집행률(%)": st.column_config.ProgressColumn(
                "집행률(%)",
                help="월 예산 대비 현재 사용액 비율",
                format="%.1f%%",
                min_value=0,
                max_value=100
            ),
            "상태": st.column_config.TextColumn("상태", disabled=True)
        }
    )


@st.fragment
def render_alert_table(alert_view: pd.DataFrame):
    display_df = _build_alert_display(alert_view)
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>비즈머니 잔액 관리 계정</div>", unsafe_allow_html=True)
    
    if display_df.empty:
        st.info("비즈머니 관리 데이터가 없습니다.")
        return
        
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    st.caption("컬럼명을 클릭하면 오름차순/내림차순 정렬할 수 있습니다. 금액과 잔여일수는 숫자 기준으로 정렬됩니다.")

    table_df = display_df.copy()
    # 숫자 정렬을 유지하기 위해 float/int로 변환 (표시는 Styler에서 처리)
    for col in ["비즈머니 잔액", avg_days_label]:
        if col in table_df.columns:
            table_df[col] = _numeric_series(table_df[col], default=0).round(0).astype("float64")
    if "잔여일수" in table_df.columns:
        table_df["잔여일수"] = pd.to_numeric(table_df["잔여일수"], errors="coerce")

    # 다른 뷰와 동일하게 Styler 객체를 전달하고, 첫 번째 주요 컬럼("업체명")을 pinned 처리
    cfg = {
        "업체명": st.column_config.TextColumn("업체명", pinned=True, width="medium"),
        "소진 위험": st.column_config.TextColumn("소진 위험", width="small"),
        "담당자": st.column_config.TextColumn("담당자"),
        "비즈머니 잔액": st.column_config.NumberColumn("비즈머니 잔액", format="%,.0f 원"),
        avg_days_label: st.column_config.NumberColumn(avg_days_label, format="%,.0f 원"),
        "잔여일수": st.column_config.NumberColumn("잔여일수", format="%,.1f 일"),
        "예상 소진일": st.column_config.TextColumn("예상 소진일"),
    }

    styled_df = _build_budget_table_styler(table_df, avg_days_label)

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=550,
        column_config=cfg,
    )


@st.fragment
def render_budget_kpis(biz_view: pd.DataFrame, end_dt: date):
    total_balance = int(safe_numeric_col(biz_view, "bizmoney_balance").sum())
    total_month_cost = int(safe_numeric_col(biz_view, "current_month_cost").sum())
    monthly_budget_src = biz_view["monthly_budget"] if "monthly_budget" in biz_view.columns else pd.Series([0] * len(biz_view.index))
    avg_cost_src = biz_view["avg_cost"] if "avg_cost" in biz_view.columns else pd.Series([0] * len(biz_view.index))
    total_budget = int(_numeric_series(monthly_budget_src, default=0).sum())
    usage_pct = (total_month_cost / total_budget * 100.0) if total_budget > 0 else 0.0
    avg_cost = int(_numeric_series(avg_cost_src, default=0).sum())

    render_kpi_strip([
        {"label": "총 비즈머니 잔액", "value": format_currency(total_balance), "sub": "현재 잔액", "tone": "neu"},
        {"label": f"{end_dt.month}월 총 사용액", "value": format_currency(total_month_cost), "sub": "월 누적", "tone": "neu"},
        {"label": "월 예산 합계", "value": format_currency(total_budget), "sub": "등록 기준", "tone": "neu"},
        {"label": "예산 집행률", "value": f"{usage_pct:.1f}%", "sub": "전체 페이스", "tone": "neu"},
        {"label": f"최근 {TOPUP_AVG_DAYS}일 평균소진", "value": format_currency(avg_cost), "sub": "일 평균", "tone": "neu"},
        {"label": "관리 계정", "value": f"{len(biz_view.index):,}개", "sub": "현재 필터", "tone": "neu"},
    ])


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)
    
    selected_view = st.radio("보기", ["월 예산 현황", "비즈머니 관리"], horizontal=True, label_visibility="collapsed", key="budget_view_mode")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    fallback_end_dt = f.get("end") or yesterday
    end_dt = _resolve_budget_reference_date(engine, fallback_end_dt)
    end_dt = min(end_dt, yesterday)
    avg_d2 = end_dt
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    prev_month_last_day = month_d1 - timedelta(days=1)
    prev_month_d1 = prev_month_last_day.replace(day=1)
    prev_month_d2 = prev_month_last_day

    _, days_in_month = calendar.monthrange(end_dt.year, end_dt.month)
    current_day = end_dt.day
    target_pacing_rate = current_day / days_in_month

    if selected_view == "월 예산 현황":
        bundle = _cached_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        biz_view = _prepare_biz_view(bundle)
        if biz_view.empty:
            st.info("예산 현황 데이터가 없습니다.")
        else:
            render_budget_kpis(biz_view.copy(), end_dt)

            budget_view = _build_budget_editor_view(biz_view, target_pacing_rate)

            if "local_budget_overrides" in st.session_state and not budget_view.empty:
                for cid, new_val in st.session_state["local_budget_overrides"].items():
                    m_cid = budget_view["customer_id"].astype(str) == str(cid)
                    budget_view.loc[m_cid, "monthly_budget"] = new_val
                    budget_view.loc[m_cid, "monthly_budget_val"] = int(new_val)
                    current_cost = pd.to_numeric(budget_view.loc[m_cid, "current_month_cost_val"], errors="coerce").fillna(0)
                    new_budget_float = float(new_val) if float(new_val) > 0 else 0.0
                    usage_rate = (current_cost / new_budget_float) if new_budget_float > 0 else 0.0
                    budget_view.loc[m_cid, "usage_rate"] = usage_rate
                    budget_view.loc[m_cid, "usage_pct"] = usage_rate * 100.0
                    budget_view.loc[m_cid, "상태"] = np.select(
                        [
                            budget_view.loc[m_cid, "monthly_budget_val"] == 0,
                            budget_view.loc[m_cid, "usage_rate"] >= 1.0,
                            budget_view.loc[m_cid, "usage_rate"] > target_pacing_rate + 0.1,
                            budget_view.loc[m_cid, "usage_rate"] < target_pacing_rate - 0.1,
                        ],
                        ["미설정", "예산 초과", "과속 소진", "과소 소진"],
                        default="적정 페이스",
                    )
                    budget_view.loc[m_cid, "_rank"] = np.select(
                        [
                            budget_view.loc[m_cid, "monthly_budget_val"] == 0,
                            budget_view.loc[m_cid, "usage_rate"] >= 1.0,
                            budget_view.loc[m_cid, "usage_rate"] > target_pacing_rate + 0.1,
                            budget_view.loc[m_cid, "usage_rate"] < target_pacing_rate - 0.1,
                        ],
                        [4, 0, 1, 3],
                        default=2,
                    )
                budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

            status_counts = budget_view["상태"].value_counts().to_dict() if not budget_view.empty and "상태" in budget_view.columns else {}
            render_ops_cards([
                {"title": "즉시 점검", "value": f"{int(status_counts.get('예산 초과', 0)):,}개", "note": "월 예산을 초과한 계정", "tone": "danger"},
                {"title": "과속 소진", "value": f"{int(status_counts.get('과속 소진', 0)):,}개", "note": "권장 페이스보다 빠른 계정", "tone": "warning"},
                {"title": "정상 페이스", "value": f"{int(status_counts.get('적정 페이스', 0)):,}개", "note": "현재 기준 안정 범위", "tone": "success"},
            ])
            render_budget_editor(budget_view, engine, end_dt, target_pacing_rate)
    else:
        alert_avg_d2 = end_dt
        alert_avg_d1 = alert_avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
        alert_bundle = _cached_budget_bundle(engine, cids, yesterday, alert_avg_d1, alert_avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        alert_view = _prepare_alert_view(alert_bundle)
        if alert_view.empty:
            st.info("비즈머니 관리 데이터가 없습니다.")
        else:
            alert_display = _build_alert_display(alert_view)
            urgent_count = int(alert_display["소진 위험"].astype(str).isin(["즉시 충전", "소진 임박"]).sum()) if not alert_display.empty else 0
            safe_count = max(len(alert_display.index) - urgent_count, 0) if not alert_display.empty else 0
            render_ops_cards([
                {"title": "충전 우선순위", "value": f"{urgent_count:,}개", "note": "즉시 또는 3일 내 확인", "tone": "danger" if urgent_count else "success"},
                {"title": "안정 계정", "value": f"{safe_count:,}개", "note": "잔여일수 여유", "tone": "success"},
                {"title": "관리 기준", "value": f"{TOPUP_DAYS_COVER}일", "note": f"최근 {TOPUP_AVG_DAYS}일 평균소진 기준", "tone": "info"},
            ])
            render_alert_table(alert_view)
