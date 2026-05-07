    df["담당자"] = df.get("manager", "미배정").fillna("미배정").replace("", "미배정")
    df["업체명"] = df.get("account_name", df.get("customer_id", "-")).fillna("-").replace("", "-")
    df = df.sort_values(["_risk_rank", "_sort_days", "업체명"], ascending=[True, True, True]).reset_index(drop=True)
    return df[["업체명", "소진 위험", "담당자", "비즈머니 잔액", avg_days_label, "잔여일수", "예상 소진일"]]


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _build_budget_editor_view(biz_view: pd.DataFrame, target_pacing_rate: float) -> pd.DataFrame:
    if biz_view is None or biz_view.empty:
        return pd.DataFrame()
    for col in ["customer_id", "account_name", "manager", "monthly_budget", "prev_month_cost", "current_month_cost"]:
        if col not in biz_view.columns:
            biz_view[col] = "" if col in {"customer_id", "account_name", "manager"} else 0
    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "prev_month_cost", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = safe_numeric_col(budget_view, "monthly_budget").astype(int)
    budget_view["prev_month_cost_val"] = safe_numeric_col(budget_view, "prev_month_cost").astype(int)
    budget_view["current_month_cost_val"] = safe_numeric_col(budget_view, "current_month_cost").astype(int)
    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)
    cond_zero = budget_view["monthly_budget_val"] == 0
    cond_over = budget_view["usage_rate"] >= 1.0
    cond_fast = budget_view["usage_rate"] > target_pacing_rate + 0.1
    cond_slow = budget_view["usage_rate"] < target_pacing_rate - 0.1
    budget_view["상태"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        ["미설정", "예산 초과", "과속 소진", "과소 소진"],
        default="적정 페이스",
    )
    budget_view["_rank"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        [4, 0, 1, 3],
        default=2,
    )
    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)
    return budget_view


def _ensure_budget_input_js_once():
    if st.session_state.get("_budget_input_js_once"):
        return
    st.session_state["_budget_input_js_once"] = True


def _resolve_budget_reference_date(engine, fallback_end_dt: date) -> date:
    latest_dates = get_latest_dates(engine) or {}
    candidates = []
    for key in ["fact_campaign_daily", "fact_adgroup_daily", "fact_keyword_daily", "fact_ad_daily"]:
        dt_val = latest_dates.get(key)
        if pd.notna(dt_val):
            try:
                candidates.append(pd.to_datetime(dt_val).date())
            except Exception:
                pass
    if candidates:
        return max(candidates)
    return fallback_end_dt


def _build_budget_table_styler(df: pd.DataFrame, avg_days_label: str):
    """다른 테이블들(overview, campaign 등)과 동일하게 df.style.format을 사용"""
    fmt_map = {
        "비즈머니 잔액": "{:,.0f}원",
        avg_days_label: "{:,.0f}원",
        "잔여일수": "{:,.1f}일",
    }
    # 포맷 맵에 있는 컬럼만 적용
    fmt_map = {k: v for k, v in fmt_map.items() if k in df.columns}
    styler = df.style.format(fmt_map, na_rep='-')
    if "소진 위험" in df.columns:
        def _risk_style(value):
            value = str(value or "")
            if value == "즉시 충전":
                return "background-color:#FEE2E2;color:#B91C1C;font-weight:800;"
            if value == "소진 임박":
                return "background-color:#FEF3C7;color:#92400E;font-weight:800;"
            if value == "주의":
                return "background-color:#E0F2FE;color:#075985;font-weight:800;"
            if value == "여유":
                return "background-color:#DCFCE7;color:#166534;font-weight:800;"
            return ""
        try:
            styler = styler.map(_risk_style, subset=["소진 위험"])
        except AttributeError:
            styler = styler.applymap(_risk_style, subset=["소진 위험"])
    return styler


@st.fragment
def render_budget_editor(budget_view: pd.DataFrame, engine, end_dt: date, target_pacing_rate: float):
    prev_month_dt = (end_dt.replace(day=1) - timedelta(days=1))
    prev_m_num = prev_month_dt.month
    
    editor_df = budget_view[["customer_id", "account_name", "manager", "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val", "usage_pct", "상태"]].copy()
    
    editor_df["월 예산"] = editor_df["monthly_budget_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{end_dt.month}월 사용액"] = editor_df["current_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{prev_m_num}월 사용액"] = editor_df["prev_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    
    editor_df = editor_df.rename(columns={
        "account_name": "업체명", 
        "manager": "담당자", 
        "usage_pct": "집행률(%)"
    })

    ordered_cols = [
        "customer_id", "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val", 
        "업체명", "담당자", "월 예산", f"{end_dt.month}월 사용액", f"{prev_m_num}월 사용액", "집행률(%)", "상태"
    ]
    editor_df = editor_df[ordered_cols]

    def update_budget_from_table():
        if "budget_table_editor" in st.session_state:
            edits = st.session_state["budget_table_editor"].get("edited_rows", {})
            updated_count = 0
            
            if "local_budget_overrides" not in st.session_state:
                st.session_state["local_budget_overrides"] = {}
                
            for row_idx, col_data in edits.items():
                if "월 예산" in col_data:
                    raw_input = str(col_data["월 예산"]).replace(",", "").replace("원", "").strip()
                    if raw_input.isdigit():
                        new_budget = int(raw_input)
                        cid = str(editor_df.iloc[row_idx]["customer_id"])
                        
                        update_monthly_budget(engine, cid, new_budget)
                        st.session_state["local_budget_overrides"][cid] = new_budget
                        updated_count += 1
            
            if updated_count > 0:
                st.toast("예산이 저장되었습니다.")

    st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:4px;'>{end_dt.strftime('%Y년 %m월')} 예산 집행률 (현재 권장 소진율: <span style='color:#0528F2;'>{target_pacing_rate*100:.0f}%</span>)</div>", unsafe_allow_html=True)
    st.caption("표의 '월 예산(원)' 칸을 더블클릭하여 수정하세요. 권장 소진율 대비 10% 이상 차이가 나면 과속/과소 상태로 진단됩니다.")
    _ensure_budget_input_js_once()

    st.data_editor(
        editor_df,
        key="budget_table_editor",
        on_change=update_budget_from_table,
        hide_index=True,
        use_container_width=True,
        height=550,
        column_config={
            "customer_id": None, 
            "monthly_budget_val": None, 
            "prev_month_cost_val": None,
            "current_month_cost_val": None,
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
