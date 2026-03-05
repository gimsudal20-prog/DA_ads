def render_item_comparison_search(entity_label: str, df_cur: pd.DataFrame, df_base: pd.DataFrame, name_col: str, d1: date, d2: date, b1: date, b2: date):
    import textwrap
    import streamlit as st
    import pandas as pd

    items_cur = set(df_cur[name_col].dropna().astype(str).unique()) if not df_cur.empty and name_col in df_cur.columns else set()
    items_base = set(df_base[name_col].dropna().astype(str).unique()) if not df_base.empty and name_col in df_base.columns else set()

    all_items = sorted([x for x in list(items_cur | items_base) if str(x).strip() != ""])

    if not all_items:
        return

    st.markdown(
        f"<div style='font-size:15px; font-weight:700; margin-top:20px; color:#111;'>🎯 {entity_label} 상세 분석 선택</div>",
        unsafe_allow_html=True,
    )
    query = st.text_input("항목 검색", key=f"search_detail_query_{entity_label}_{name_col}", placeholder="이름 일부를 입력해 빠르게 찾기")
    filtered_items = [item for item in all_items if query.lower() in item.lower()] if query else all_items
    options = ["선택 안 함"] + filtered_items

    if len(options) == 1:
        st.info("검색 결과가 없습니다. 검색어를 바꿔주세요.")
        return

    selected = st.selectbox("분석 항목", options, key=f"search_detail_{entity_label}_{name_col}")
    st.caption(f"현재 선택: {selected}" if selected != "선택 안 함" else "현재 선택: 없음")

    if selected == "선택 안 함":
        st.info("비교 카드를 보려면 분석 항목을 선택하세요.")
        return

    c_df = df_cur[df_cur[name_col] == selected] if not df_cur.empty else pd.DataFrame()
    b_df = df_base[df_base[name_col] == selected] if not df_base.empty else pd.DataFrame()

    def _sum_col(df: pd.DataFrame, candidates: list[str]) -> float:
        if df is None or df.empty:
            return 0.0
        for col in candidates:
            if col in df.columns:
                return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
        return 0.0

    def _cur_val(candidates_kr: list[str], candidates_en: list[str]) -> float:
        return _sum_col(c_df, candidates_kr + candidates_en)

    def _base_val(base_cols: list[str], prev_cols: list[str]) -> float:
        val = _sum_col(b_df, base_cols)
        if val > 0:
            return val
        return _sum_col(c_df, prev_cols)

    c_cost = _cur_val(["광고비"], ["cost"])
    c_sales = _cur_val(["전환매출"], ["sales"])
    c_clk = _cur_val(["클릭", "클릭수"], ["clk"])
    c_imp = _cur_val(["노출", "노출수"], ["imp"])
    c_conv = _cur_val(["전환", "전환수"], ["conv"])
    c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0

    b_cost = _base_val(["광고비", "cost"], ["p_cost"])
    b_sales = _base_val(["전환매출", "sales"], ["p_sales"])
    b_clk = _base_val(["클릭", "클릭수", "clk"], ["p_clk"])
    b_imp = _base_val(["노출", "노출수", "imp"], ["p_imp"])
    b_conv = _base_val(["전환", "전환수", "conv"], ["p_conv"])
    b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0

    def fmt_krw(v):
        return f"{int(v):,}원"

    def fmt_num(v):
        return f"{int(v):,}"

    def fmt_pct(v):
        return f"{v:.1f}%"

    def calc_detail_delta(c, b, is_currency=False, is_pct=False):
        diff = c - b
        if b == 0 and c > 0:
            return "신규"
        if diff == 0:
            return "변동 없음"

        sign = "▲" if diff > 0 else "▼"
        if is_currency:
            abs_val = f"{int(abs(diff)):,}원"
        elif is_pct:
            abs_val = f"{abs(diff):.1f}%p"
        else:
            abs_val = f"{int(abs(diff)):,}" if float(diff).is_integer() else f"{abs(diff):.1f}"

        word = "증가" if diff > 0 else "감소"
        return f"{sign} {abs_val} {word}"

    def calc_delta_rate(c, b):
        if b == 0 and c > 0:
            return "신규"
        if b == 0:
            return "0.0%"
        return f"{((c - b) / b) * 100:+.1f}%"

    def delta_chip_text(delta_text: str):
        if delta_text == "변동 없음":
            return "<span class='delta-chip delta-flat'>변동 없음</span>"
        if delta_text == "신규":
            return "<span class='delta-chip delta-up'>▲ 신규</span>"
        if delta_text.startswith("▲"):
            return f"<span class='delta-chip delta-up'>{delta_text}</span>"
        return f"<span class='delta-chip delta-down'>{delta_text}</span>"

    rows = [
        {
            "label": "광고비",
            "base": fmt_krw(b_cost),
            "curr": fmt_krw(c_cost),
            "delta": calc_detail_delta(c_cost, b_cost, is_currency=True),
            "rate": calc_delta_rate(c_cost, b_cost),
        },
        {
            "label": "전환매출",
            "base": fmt_krw(b_sales),
            "curr": fmt_krw(c_sales),
            "delta": calc_detail_delta(c_sales, b_sales, is_currency=True),
            "rate": calc_delta_rate(c_sales, b_sales),
        },
        {
            "label": "ROAS",
            "base": fmt_pct(b_roas),
            "curr": f"<span style='font-weight:900; color:#dc2626;'>{fmt_pct(c_roas)}</span>",
            "delta": calc_detail_delta(c_roas, b_roas, is_pct=True),
            "rate": calc_delta_rate(c_roas, b_roas),
        },
        {
            "label": "노출수",
            "base": fmt_num(b_imp),
            "curr": fmt_num(c_imp),
            "delta": calc_detail_delta(c_imp, b_imp),
            "rate": calc_delta_rate(c_imp, b_imp),
        },
        {
            "label": "클릭수",
            "base": fmt_num(b_clk),
            "curr": fmt_num(c_clk),
            "delta": calc_detail_delta(c_clk, b_clk),
            "rate": calc_delta_rate(c_clk, b_clk),
        },
        {
            "label": "전환수",
            "base": fmt_num(b_conv),
            "curr": fmt_num(c_conv),
            "delta": calc_detail_delta(c_conv, b_conv),
            "rate": calc_delta_rate(c_conv, b_conv),
        },
    ]

    def _board_rows(items: list[dict], is_right: bool = False) -> str:
        html_rows = ""
        for r in items:
            if is_right:
                html_rows += f"""
                <div class='cmp-row'>
                    <div class='cmp-top'>
                        <span class='cmp-label'>{r['label']}</span>
                        <span class='cmp-value'>{r['curr']}</span>
                    </div>
                    <div class='cmp-sub'>
                        {delta_chip_text(r['delta'])}
                        <span class='rate'>({r['rate']})</span>
                    </div>
                </div>
                """
            else:
                html_rows += f"""
                <div class='cmp-row'>
                    <div class='cmp-top'>
                        <span class='cmp-label'>{r['label']}</span>
                        <span class='cmp-value'>{r['base']}</span>
                    </div>
                </div>
                """
        return html_rows

    left_rows = _board_rows(rows, is_right=False)
    right_rows = _board_rows(rows, is_right=True)

    html = textwrap.dedent(f"""\
    <div class='cmp-wrapper'>
        <div class='cmp-title'>✨ [{selected}] 성과 비교 상세 요약</div>
        <div class='cmp-desc'>비교 기간({b1} ~ {b2}) 대비 선택 기간({d1} ~ {d2})의 주요 지표 변화를 보여줍니다.</div>
        <div class='cmp-boards'>
            <div class='cmp-board left'>
                <div class='cmp-board-head'>⚪ 비교 기간 ({b1} ~ {b2})</div>
                {left_rows}
            </div>
            <div class='cmp-board right'>
                <div class='cmp-board-head'>🔵 선택 기간 ({d1} ~ {d2})</div>
                {right_rows}
            </div>
        </div>
    </div>
    <style>
        .cmp-wrapper {{
            background:#f8fafc;
            border:1px solid #d9e4f2;
            border-radius:14px;
            padding:22px;
            margin-top:10px;
            margin-bottom:24px;
            box-shadow:0 4px 12px rgba(15,23,42,0.06);
        }}
        .cmp-title {{
            font-size:18px;
            font-weight:900;
            color:#0f172a;
            margin-bottom:6px;
            text-align:center;
        }}
        .cmp-desc {{
            text-align:center;
            font-size:13px;
            color:#64748b;
            margin-bottom:14px;
        }}
        .cmp-boards {{
            display:grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap:14px;
        }}
        .cmp-board {{
            border-radius:12px;
            padding:14px 16px;
            border:1px solid #d7deea;
        }}
        .cmp-board.left {{
            background:#ffffff;
        }}
        .cmp-board.right {{
            background:#eff6ff;
        }}
        .cmp-board-head {{
            font-size:14px;
            font-weight:900;
            text-align:center;
            margin-bottom:6px;
            padding-bottom:10px;
            border-bottom:1px dashed #dbe4f0;
        }}
        .cmp-board.left .cmp-board-head {{ color:#64748b; }}
        .cmp-board.right .cmp-board-head {{ color:#1d4ed8; }}

        .cmp-row {{
            border-top:1px dashed #dbe4f0;
            padding:13px 0;
        }}
        .cmp-row:first-child {{
            border-top:none;
        }}
        .cmp-top {{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
        }}
        .cmp-label {{
            font-weight:800;
            font-size:14px;
            color:#334155;
        }}
        .cmp-board.right .cmp-label {{ color:#1e40af; }}
        .cmp-value {{
            color:#0f172a;
            font-weight:900;
            font-size:20px;
            line-height:1.2;
        }}
        .cmp-sub {{
            margin-top:4px;
            display:flex;
            justify-content:flex-end;
            gap:8px;
            align-items:center;
            text-align:right;
            font-size:12px;
            line-height:1.4;
            word-break:keep-all;
        }}
        .delta-chip {{
            font-size:12px;
            font-weight:800;
            border-radius:999px;
            padding:2px 8px;
            display:inline-block;
        }}
        .delta-up {{ background:#fee2e2; color:#dc2626; }}
        .delta-down {{ background:#dbeafe; color:#1d4ed8; }}
        .delta-flat {{ background:#e2e8f0; color:#475569; }}
        .rate {{ color:#64748b; font-weight:700; }}

        @media (max-width: 1024px) {{
            .cmp-boards {{
                grid-template-columns: 1fr;
            }}
            .cmp-value {{
                font-size:18px;
            }}
            .cmp-sub {{
                flex-wrap:wrap;
                justify-content:flex-end;
            }}
        }}
    </style>
    """).strip()
    st.components.v1.html(html, height=700, scrolling=False)
