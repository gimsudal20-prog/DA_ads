# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta


def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"

def _build_periodic_report_text(campaign_type: str, imp: float, clk: float, ctr: float, cost: float, roas: float, sales: float, top_keywords_label: str, top_keywords: str) -> str:
    return "\n".join([
        f"[ {campaign_type} 성과 요약 ]",
        _format_report_line("노출수", f"{int(imp):,}"),
        _format_report_line("클릭수", f"{int(clk):,}"),
        _format_report_line("클릭률", f"{float(ctr):.2f}%"),
        _format_report_line("광고 소진비용", f"{int(cost):,}원"),
        _format_report_line("전환매출", f"{int(sales):,}원"),
        _format_report_line("ROAS", f"{float(roas):.2f}%"),
        _format_report_line(top_keywords_label, top_keywords),
    ])


def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel:
        return "전체 유형"
    if len(type_sel) == 1:
        return type_sel[0]
    return ", ".join(type_sel)


# ✨ max_entries=10 추가
@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_keyword_bundle(_engine, start_dt, end_dt, list(cids), type_sel, topn_cost=0)
    except Exception:
        return pd.DataFrame()


# ✨ max_entries=10 추가
@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=5000)
    except Exception:
        return pd.DataFrame()


# ✨ max_entries=10 추가
@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, trend_d1, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        ts = query_campaign_timeseries(_engine, trend_d1, end_dt, cids, type_sel)
        return ts if ts is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f:
        return

    cids, type_sel = tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
    base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)

    state_sig = f"{f['start']}|{f['end']}|{','.join(map(str, cids))}|{','.join(type_sel)}"
    state_hash = abs(hash(state_sig))
    report_loaded_key = f"overview_report_loaded_{state_hash}"
    alerts_loaded_key = f"overview_alerts_loaded_{state_hash}"
    trend_loaded_key = f"overview_trend_loaded_{state_hash}"

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1:
            account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1:
            account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)
    st.markdown(f"<div class='nv-sec-title'>📊 {account_name} 종합 성과 요약 ({selected_type_label})</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:13px; font-weight:500; color:#474747; margin-bottom:12px;'>비교 기준: <span style='color:#375FFF; font-weight:700;'>{cmp_mode}</span></div>", unsafe_allow_html=True)

    cur = cur_summary
    base = base_summary

    def _delta_pct(key):
        try:
            return pct_change(float(cur.get(key, 0.0) or 0.0), float(base.get(key, 0.0) or 0.0))
        except Exception:
            return None

    def _kpi_html(label, value, delta_text, delta_val, highlight=False, improve_when_up=True):
        delta_num = float(delta_val) if delta_val is not None else 0.0
        is_neutral = abs(delta_num) < 5
        if is_neutral:
            cls_delta = "neu"
            delta_text = f"● 유지 ({delta_num:+.1f}%)"
        else:
            improved = delta_num > 0 if improve_when_up else delta_num < 0
            cls_delta = "pos" if improved else "neg"
            trend_label = "✓ 개선" if improved else "✕ 악화"
            delta_text = f"{trend_label} {pct_to_arrow(delta_num)}"
        cls_hl = " highlight" if highlight else ""
        return f"<div class='kpi{cls_hl}'><div class='k'>{label}</div><div class='v'>{value}</div><div class='d {cls_delta}'>{delta_text}</div></div>"

    kpi_groups_html = f"""
    <div class='kpi-group-container'>
        <div class='kpi-group'>
            <div class='kpi-group-title'>👀 유입 지표</div>
            <div class='kpi-row'>
                {_kpi_html("노출수", format_number_commas(cur.get("imp", 0.0)), f"{pct_to_arrow(_delta_pct('imp'))}", _delta_pct("imp"))}
                {_kpi_html("클릭수", format_number_commas(cur.get("clk", 0.0)), f"{pct_to_arrow(_delta_pct('clk'))}", _delta_pct("clk"))}
                {_kpi_html("CTR", f"{float(cur.get('ctr', 0.0) or 0.0):.2f}%", f"{pct_to_arrow(_delta_pct('ctr'))}", _delta_pct("ctr"))}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>💸 비용 지표</div>
            <div class='kpi-row'>
                {_kpi_html("광고비", format_currency(cur.get("cost", 0.0)), f"{pct_to_arrow(_delta_pct('cost'))}", _delta_pct("cost"), highlight=True, improve_when_up=False)}
                {_kpi_html("CPC", format_currency(cur.get("cpc", 0.0)), f"{pct_to_arrow(_delta_pct('cpc'))}", _delta_pct("cpc"), improve_when_up=False)}
            </div>
        </div>
        <div class='kpi-group'>
            <div class='kpi-group-title'>🎯 성과 지표</div>
            <div class='kpi-row'>
                {_kpi_html("ROAS", f"{float(cur.get('roas', 0.0) or 0.0):.2f}%", f"{pct_to_arrow(_delta_pct('roas'))}", _delta_pct("roas"), highlight=True)}
                {_kpi_html("전환수", format_number_commas(cur.get("conv", 0.0)), f"{pct_to_arrow(_delta_pct('conv'))}", _delta_pct("conv"))}
                {_kpi_html("전환매출", format_currency(cur.get("sales", 0.0)), f"{pct_to_arrow(_delta_pct('sales'))}", _delta_pct("sales"))}
            </div>
        </div>
    </div>
    """
    st.markdown(kpi_groups_html, unsafe_allow_html=True)

    cost_delta = _delta_pct("cost") or 0.0
    conv_delta = _delta_pct("conv") or 0.0
    if abs(cost_delta) < 5 and abs(conv_delta) < 5:
        insight = "광고비/전환 모두 큰 변동이 없어 안정적으로 운영 중입니다."
    elif cost_delta > conv_delta + 10:
        insight = f"광고비 증감({cost_delta:+.1f}%) 대비 전환 증감({conv_delta:+.1f}%)이 낮아 효율 악화 가능성이 있습니다."
    elif conv_delta > cost_delta + 10:
        insight = f"전환 증감({conv_delta:+.1f}%)이 광고비 증감({cost_delta:+.1f}%)보다 커 효율 개선 흐름입니다."
    else:
        insight = f"광고비({cost_delta:+.1f}%)와 전환({conv_delta:+.1f}%)이 유사하게 움직이고 있습니다."
    st.info(f"🧭 KPI 해석: {insight}")


    # ==========================================
    # ✨ 업체별 성과 요약 (신규 추가)
    # ==========================================
    st.markdown("<div class='nv-sec-title' style='margin-top: 32px;'>🏢 업체별 성과 요약</div>", unsafe_allow_html=True)
    with st.spinner("업체별 데이터 집계 중..."):
        cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
        base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)

    if not cur_camp.empty:
        # 업체별 합산
        cur_grp = cur_camp.groupby('customer_id')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
        base_grp = base_camp.groupby('customer_id')[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index() if not base_camp.empty else pd.DataFrame(columns=['customer_id', 'imp', 'clk', 'cost', 'conv', 'sales'])
        
        # 메타데이터 병합하여 업체명 가져오기
        if not meta.empty:
            cur_grp = cur_grp.merge(meta[['customer_id', 'account_name']], on='customer_id', how='left')
            cur_grp['account_name'] = cur_grp['account_name'].fillna(cur_grp['customer_id'].astype(str))
        else:
            cur_grp['account_name'] = cur_grp['customer_id'].astype(str)
            
        merged = cur_grp.merge(base_grp, on='customer_id', how='left', suffixes=('_cur', '_base')).fillna(0)
        
        # 광고비 소진 순으로 기본 정렬
        merged = merged.sort_values('cost_cur', ascending=False)
        
        table_data = []
        for _, row in merged.iterrows():
            c_imp, c_clk, c_cost, c_conv, c_sales = row['imp_cur'], row['clk_cur'], row['cost_cur'], row['conv_cur'], row['sales_cur']
            b_imp, b_clk, b_cost, b_conv, b_sales = row.get('imp_base', 0), row.get('clk_base', 0), row.get('cost_base', 0), row.get('conv_base', 0), row.get('sales_base', 0)
            
            c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
            b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
            
            def safe_pct(c, b):
                if b == 0: return 100.0 if c > 0 else 0.0
                return (c - b) / b * 100.0
                
            table_data.append({
                "업체명": row['account_name'],
                "광고비(비용)": c_cost,
                "비용 증감": safe_pct(c_cost, b_cost),
                "클릭수(유입)": c_clk,
                "유입 증감": safe_pct(c_clk, b_clk),
                "전환수(성과)": c_conv,
                "성과 증감": safe_pct(c_conv, b_conv),
                "ROAS": c_roas,
                "ROAS 증감(p)": c_roas - b_roas
            })
            
        df_display = pd.DataFrame(table_data)
        
        # 증감 색상 서식 적용 함수 (상승: 빨간색, 하락: 파란색)
        def color_delta(val):
            if pd.isna(val) or val == 0: return 'color: #888888;'
            return 'color: #FC503D; font-weight: bold;' if val > 0 else 'color: #375FFF; font-weight: bold;'
            
        styled_df = df_display.style.format({
            "광고비(비용)": "{:,.0f}원",
            "비용 증감": "{:+.1f}%",
            "클릭수(유입)": "{:,.0f}",
            "유입 증감": "{:+.1f}%",
            "전환수(성과)": "{:,.0f}",
            "성과 증감": "{:+.1f}%",
            "ROAS": "{:,.1f}%",
            "ROAS 증감(p)": "{:+.1f}%p"
        })
        
        # Pandas 버전에 따른 map/applymap 호환 처리
        if hasattr(styled_df, 'map'):
            styled_df = styled_df.map(color_delta, subset=['비용 증감', '유입 증감', '성과 증감', 'ROAS 증감(p)'])
        else:
            styled_df = styled_df.applymap(color_delta, subset=['비용 증감', '유입 증감', '성과 증감', 'ROAS 증감(p)'])
            
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    else:
        st.info("해당 기간의 캠페인 데이터가 없습니다.")

    st.markdown("<br>", unsafe_allow_html=True)


    with st.expander("📝 보고서 내보내기", expanded=False):
        report_campaign_type = selected_type_label
        report_cur = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)

        st.session_state[report_loaded_key] = True

        top_keywords_text = "-"
        
        is_shopping = False
        if type_sel and any("쇼핑" in t or "SHOPPING" in str(t).upper() for t in type_sel):
            is_shopping = True
            
        sort_col = "conv" if is_shopping else "clk"
        top_keywords_label = "전환이 많았던 키워드" if is_shopping else "클릭이 많았던 키워드"

        if st.session_state.get(report_loaded_key, False):
            with st.spinner("키워드 집계 중..."):
                kw_bundle = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
            if not kw_bundle.empty and {"keyword", sort_col}.issubset(kw_bundle.columns):
                kw_top = kw_bundle.copy()
                kw_top[sort_col] = pd.to_numeric(kw_top[sort_col], errors="coerce").fillna(0)
                kw_top = kw_top.groupby("keyword", as_index=False)[sort_col].sum().sort_values(sort_col, ascending=False).head(3)
                if not kw_top.empty:
                    top_keywords_text = ", ".join([str(x).strip() for x in kw_top["keyword"].tolist() if str(x).strip()]) or "-"
        
        report_text = _build_periodic_report_text(
            campaign_type=report_campaign_type,
            imp=float(report_cur.get("imp", 0.0) or 0.0),
            clk=float(report_cur.get("clk", 0.0) or 0.0),
            ctr=float(report_cur.get("ctr", 0.0) or 0.0),
            cost=float(report_cur.get("cost", 0.0) or 0.0),
            roas=float(report_cur.get("roas", 0.0) or 0.0),
            sales=float(report_cur.get("sales", 0.0) or 0.0),
            top_keywords_label=top_keywords_label,
            top_keywords=top_keywords_text,
        )
        st.code(report_text, language="text")
        st.download_button(
            "📥 요약 보고서 txt 내보내기",
            data=report_text,
            file_name=f"요약보고서_{f['start']}_{f['end']}.txt",
            mime="text/plain",
            use_container_width=True,
        )

    with st.expander("🚨 계정 내 점검 알림", expanded=False):
        load_alerts = st.button("⚡ 알림 분석 불러오기", key=f"btn_load_alerts_{state_hash}", use_container_width=True)
        if load_alerts:
            st.session_state[alerts_loaded_key] = True

        if not st.session_state.get(alerts_loaded_key, False):
            st.caption("초기 로딩 속도를 위해 알림 분석은 필요할 때만 실행합니다.")
        else:
            with st.spinner("알림 항목 분석 중..."):
                camp_bndl = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)

            alerts = []
            cur_roas = cur_summary.get('roas', 0)
            cur_cost = cur_summary.get('cost', 0)

            if cur_cost > 0 and cur_roas < 100:
                alerts.append(f"⚠️ 수익성 적자: 현재 조회 기간의 평균 ROAS가 {cur_roas:.2f}%로 낮습니다.")
            if base_summary.get('cost', 0) > 0:
                cost_surge = (cur_cost - base_summary['cost']) / base_summary['cost'] * 100
                if cost_surge >= 150:
                    alerts.append(f"🔥 비용 폭증: 이전 기간 대비 전체 광고비 소진율이 {cost_surge:.0f}% 증가했습니다.")

            hippos = pd.DataFrame()
            if not camp_bndl.empty:
                hippos = camp_bndl[(camp_bndl['cost'] >= 50000) & (camp_bndl['conv'] == 0)].sort_values('cost', ascending=False)
                if not hippos.empty:
                    alerts.append(f"💸 비용 누수: 비용 5만 원 이상 소진 중이나 전환이 없는 캠페인이 {len(hippos)}개 발견되었습니다.")

            if alerts:
                for a in alerts:
                    st.markdown(f"- {a}")

                if not hippos.empty:
                    disp_hippos = _perf_common_merge_meta(hippos, meta)
                    disp_hippos = disp_hippos.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명", "cost": "광고비", "clk": "클릭수"})
                    cols_to_show = [c for c in ["업체명", "캠페인명", "광고비", "클릭수"] if c in disp_hippos.columns]
                    df_show = disp_hippos[cols_to_show].copy()
                    for c in ["광고비", "클릭수"]:
                        if c in df_show.columns:
                            df_show[c] = df_show[c].apply(lambda x: format_currency(x) if c == "광고비" else format_number_commas(x))

                    st.markdown("<div style='margin-top: 16px; font-weight: 700; color: #FC503D; font-size: 14px;'>비용 누수 캠페인 목록</div>", unsafe_allow_html=True)
                    st.dataframe(df_show, width="stretch", hide_index=True)
            else:
                st.success("✨ 모니터링 결과: 특이한 이상 징후나 비용 누수가 없습니다. 계정이 건강하게 운영되고 있습니다!")

    st.divider()

    st.markdown("<div class='nv-sec-title'>트렌드 및 요일별 효율 분석</div>", unsafe_allow_html=True)
    with st.expander("📈 트렌드 차트 보기", expanded=False):
        st.session_state[trend_loaded_key] = True

        trend_d1 = min(f["start"], date.today() - timedelta(days=7))
        with st.spinner("트렌드 데이터 집계 중..."):
            ts = _cached_campaign_timeseries(engine, trend_d1, f["end"], cids, type_sel)

        if ts is None or ts.empty:
            st.info("선택한 조건에 대한 트렌드 데이터가 없습니다.")
            return

        tab_trend, tab_dow = st.tabs(["전체 트렌드", "요일별 히트맵"])
        with tab_trend:
            ts = ts.copy()
            ts["roas"] = np.where(
                pd.to_numeric(ts["cost"], errors="coerce").fillna(0) > 0,
                round((pd.to_numeric(ts["sales"], errors="coerce").fillna(0) / pd.to_numeric(ts["cost"], errors="coerce").fillna(0) * 100.0), 1),
                0.0
            )

            trend_metric_options = {
                "광고비 + ROAS": {"col": "cost", "label": "광고비(원)", "mode": "dual"},
                "클릭수": {"col": "clk", "label": "클릭수", "mode": "single"},
                "노출수": {"col": "imp", "label": "노출수", "mode": "single"},
                "전환수": {"col": "conv", "label": "전환수", "mode": "single"},
            }

            selected_trend_metric = st.selectbox(
                "전체트렌드 지표 선택",
                list(trend_metric_options.keys()),
                index=0,
                key="overview_trend_metric_selector"
            )
            selected_cfg = trend_metric_options[selected_trend_metric]
            trend_ts = ts.copy()
            trend_ts[selected_cfg["col"]] = pd.to_numeric(trend_ts[selected_cfg["col"]], errors="coerce").fillna(0)

            if selected_cfg["mode"] == "dual":
                if HAS_ECHARTS:
                    render_echarts_dual_axis("일자별 광고비 및 ROAS", trend_ts, "dt", "cost", "광고비(원)", "roas", "ROAS(%)", height=320)
                else:
                    st.line_chart(trend_ts.set_index("dt")[["cost", "roas"]], height=320)
            else:
                chart_title = f"일자별 {selected_cfg['label']}"
                if HAS_ECHARTS:
                    render_echarts_single_axis(chart_title, trend_ts, "dt", selected_cfg["col"], selected_cfg["label"], height=320)
                else:
                    st.line_chart(trend_ts.set_index("dt")[[selected_cfg["col"]]], height=320)

        with tab_dow:
            ts_dow = ts.copy()
            ts_dow["요일"] = ts_dow["dt"].dt.day_name()
            dow_map = {'Monday': '월', 'Tuesday': '화', 'Wednesday': '수', 'Thursday': '목', 'Friday': '금', 'Saturday': '토', 'Sunday': '일'}
            ts_dow["요일"] = ts_dow["요일"].map(dow_map)

            dow_df = ts_dow.groupby("요일")[["cost", "conv", "sales"]].sum().reset_index()
            dow_df["ROAS(%)"] = np.where(dow_df["cost"] > 0, dow_df["sales"] / dow_df["cost"] * 100, 0)

            cat_dtype = pd.CategoricalDtype(categories=['월', '화', '수', '목', '금', '토', '일'], ordered=True)
            dow_df["요일"] = dow_df["요일"].astype(cat_dtype)
            dow_df = dow_df.sort_values("요일")

            dow_disp = dow_df.rename(columns={"cost": "광고비", "conv": "전환수", "sales": "전환매출"})

            styled_df = dow_disp.style.background_gradient(cmap='Blues', subset=['광고비']).background_gradient(cmap='Purples', subset=['ROAS(%)']).format({
                '광고비': '{:,.0f}', '전환수': '{:,.1f}', '전환매출': '{:,.0f}', 'ROAS(%)': '{:,.2f}%'
            })

            st.dataframe(styled_df, width="stretch", hide_index=True)
