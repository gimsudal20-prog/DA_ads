# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px  # ✨ [NEW] 파이 차트를 그리기 위한 라이브러리 추가
from typing import Dict

from data import query_campaign_bundle, query_keyword_bundle, query_campaign_off_log, load_dim_campaign
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, append_comparison_data, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas


def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view


def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in out.columns:
            out[k] = out[k].astype(str)
    return out


def _keyword_rank_by_keys(kw_bundle: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if kw_bundle is None or kw_bundle.empty or "avg_rank" not in kw_bundle.columns:
        return pd.DataFrame(columns=keys + ["avg_rank"])
    tmp = kw_bundle.copy()
    tmp["imp"] = pd.to_numeric(tmp.get("imp", 0), errors="coerce").fillna(0.0)
    tmp["avg_rank"] = pd.to_numeric(tmp.get("avg_rank", np.nan), errors="coerce")
    tmp["_rank_imp"] = tmp["avg_rank"].fillna(0.0) * tmp["imp"]
    grp = tmp.groupby(keys, as_index=False)[["_rank_imp", "imp"]].sum()
    grp["avg_rank"] = np.where(grp["imp"] > 0, grp["_rank_imp"] / grp["imp"], np.nan)
    return grp[keys + ["avg_rank"]]


# ROAS 조건부 텍스트 컬러링 함수
def highlight_roas_text(val):
    try:
        v = float(str(val).replace("%", "").replace(",", ""))
        if 0 <= v < 100.0:
            return 'color: #EF4444; font-weight: 800;' # 적자: 진한 빨간색
        elif v >= 300.0:
            return 'color: #2563EB; font-weight: 800;' # 고효율: 진한 파란색
    except:
        pass
    return ''


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return
        
    loading_placeholder = st.empty()
    loading_placeholder.info("⏳ 최신 필터 조건에 맞추어 데이터를 실시간으로 집계하고 있습니다. 잠시만 기다려주세요...")

    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
    if bundle is None or bundle.empty:
        loading_placeholder.empty()
        return

    kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)

    df = _perf_common_merge_meta(bundle, meta)
    view = df.rename(columns={
        "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
        "campaign_name": "캠페인", "imp": "노출", "clk": "클릭",
        "cost": "광고비", "conv": "전환", "sales": "전환매출"
    }).copy()
    view = _add_perf_metrics(view)

    if not kw_bundle_cur.empty:
        rank_map_camp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id"])
        if not rank_map_camp.empty:
            key_cols = ["customer_id", "campaign_id"]
            view = _normalize_merge_keys(view, key_cols)
            rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
            view = view.merge(rank_map_camp, on=key_cols, how="left")
    if "avg_rank" in view.columns:
        view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    tab_main, tab_group, tab_cmp, tab_history = st.tabs(["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"])
    
    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "CPA(원)": "{:,.0f}", "전환매출": "{:,.0f}", "전환": "{:,.1f}", "CTR(%)": "{:,.2f}%", "ROAS(%)": "{:,.2f}%"
    }

    with tab_main:
        camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
        sel_camp_main = st.selectbox("캠페인 검색", camps_main, key="camp_name_filter_main")

        disp_main = view.copy()
        if sel_camp_main != "전체":
            disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
        if "평균순위" in disp_main.columns:
            base_cols.append("평균순위")
        
        all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]

        # ---------------------------------------------------------
        # 📊 [NEW] 1단: 유형별 지출 테이블 & 2단: PC/모바일 기기 비중 도넛 차트
        # ---------------------------------------------------------
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:12px;'>📊 캠페인 성과 요약 대시보드</div>", unsafe_allow_html=True)
        
        col_type, col_device = st.columns([1.5, 1]) # 가로 공간을 1.5 : 1 비율로 분할

        with col_type:
            # 기존 유형별 지출 요약 표
            type_grp = disp_main.groupby("캠페인유형").agg({"광고비": "sum", "전환매출": "sum"}).reset_index()
            total_cost = type_grp["광고비"].sum()
            type_grp["지출 비중(%)"] = np.where(total_cost > 0, (type_grp["광고비"] / total_cost) * 100, 0.0)
            type_grp["ROAS(%)"] = np.where(type_grp["광고비"] > 0, (type_grp["전환매출"] / type_grp["광고비"]) * 100, 0.0)
            type_grp = type_grp.sort_values("광고비", ascending=False)
            
            st.dataframe(
                type_grp.style.format({"광고비": "{:,.0f}", "ROAS(%)": "{:,.2f}%"}),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "캠페인유형": st.column_config.TextColumn("캠페인 유형"),
                    "광고비": st.column_config.Column("총 광고비(원)"),
                    "지출 비중(%)": st.column_config.ProgressColumn("지출 비중", format="%.1f%%", min_value=0, max_value=100),
                    "ROAS(%)": st.column_config.Column("평균 ROAS(%)")
                }
            )

        with col_device:
            # ✨ PC/모바일 유입 비중 차트
            st.markdown("<div style='font-size:13px; color:#555; text-align:center; margin-bottom:5px;'>📱 기기별 광고비 지출 비중</div>", unsafe_allow_html=True)
            
            # DB 연동 전, 현재 필터링된 총 광고비를 기준으로 모바일 72%, PC 28%로 가정하여 그리는 UI 데모
            # 추후 DB에서 기기별 데이터가 수집되면 아래 mock_device_df를 실제 데이터프레임으로 교체하시면 됩니다.
            mock_device_df = pd.DataFrame({
                "기기": ["모바일", "PC"],
                "광고비": [total_cost * 0.72, total_cost * 0.28]
            })
            
            fig = px.pie(
                mock_device_df, 
                values="광고비", 
                names="기기", 
                hole=0.55, # 가운데가 뚫린 예쁜 도넛 형태
                color="기기", 
                color_discrete_map={"모바일": "#335CFF", "PC": "#CBD5E1"} # 모바일은 메인 컬러, PC는 회색으로 강조
            )
            
            # 차트 여백 최소화 및 레이아웃 정리
            fig.update_layout(
                margin=dict(t=0, b=0, l=0, r=0), 
                height=180, 
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=0.85)
            )
            fig.update_traces(textposition='inside', textinfo='percent')
            
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        # ---------------------------------------------------------
        # 종합 성과 테이블 + 맞춤형 지표(Metric Schema) UI
        # ---------------------------------------------------------
        col_title, col_settings = st.columns([4, 1])
        with col_title:
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>캠페인 종합 성과 데이터</div>", unsafe_allow_html=True)
        with col_settings:
            st.markdown("<div style='margin-top:15px; text-align:right;'>", unsafe_allow_html=True)
            with st.popover("⚙️ 지표 맞춤 설정", use_container_width=True):
                st.caption("표에 표시할 데이터를 선택하세요.")
                selected_metrics = st.multiselect(
                    "지표 항목", 
                    options=all_metrics_cols, 
                    default=["광고비", "ROAS(%)", "클릭", "전환", "전환매출"], 
                    label_visibility="collapsed"
                )
            st.markdown("</div>", unsafe_allow_html=True)

        st.caption("💡 표에서 상세 분석을 원하는 **캠페인 행을 클릭**해 보세요. (아래에 하위 키워드/소재 상세 데이터가 열립니다)")

        if not selected_metrics:
            selected_metrics = ["광고비"] 

        final_cols = [c for c in base_cols + selected_metrics if c in disp_main.columns]
        
        sort_col = "광고비" if "광고비" in final_cols else (selected_metrics[0] if selected_metrics else final_cols[0])
        disp_main = disp_main[final_cols].sort_values(sort_col, ascending=False).head(top_n)

        active_fmt = {k: v for k, v in fmt.items() if k in selected_metrics}
        
        try:
            styled_main = disp_main.style.format(active_fmt)
            if "ROAS(%)" in selected_metrics:
                styled_main = styled_main.map(highlight_roas_text, subset=["ROAS(%)"])
        except AttributeError:
            styled_main = disp_main.style.format(active_fmt)
            if "ROAS(%)" in selected_metrics:
                styled_main = styled_main.applymap(highlight_roas_text, subset=["ROAS(%)"])

        col_config = {
            "업체명": st.column_config.TextColumn(width="small"),
            "담당자": st.column_config.TextColumn(width="small"),
            "캠페인유형": st.column_config.TextColumn(width="small"),
            "캠페인": st.column_config.TextColumn(width="medium")
        }

        event = st.dataframe(
            styled_main,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            column_config=col_config
        )

        selected_rows = event.selection.rows
        if selected_rows:
            selected_idx = selected_rows[0]
            selected_campaign = disp_main.iloc[selected_idx]["캠페인"]
            
            kw_detail = kw_bundle_cur[kw_bundle_cur["campaign_name"] == selected_campaign].copy()
            
            st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
            with st.container(border=True):
                st.markdown(f"<h5 style='color: #335CFF; margin-bottom: 8px;'>↳ [{selected_campaign}] 하위 그룹/키워드 상세</h5>", unsafe_allow_html=True)
                
                if not kw_detail.empty:
                    kw_view = kw_detail.rename(columns={
                        "adgroup_name": "광고그룹", "keyword": "키워드",
                        "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
                    })
                    kw_view = _add_perf_metrics(kw_view)
                    
                    kw_final_cols = ["광고그룹", "키워드"] + selected_metrics
                    kw_disp = kw_view[[c for c in kw_final_cols if c in kw_view.columns]].sort_values(sort_col, ascending=False).head(100)
                    
                    try:
                        styled_kw = kw_disp.style.format(active_fmt)
                        if "ROAS(%)" in selected_metrics:
                            styled_kw = styled_kw.map(highlight_roas_text, subset=["ROAS(%)"])
                    except AttributeError:
                        styled_kw = kw_disp.style.format(active_fmt)
                        if "ROAS(%)" in selected_metrics:
                            styled_kw = styled_kw.applymap(highlight_roas_text, subset=["ROAS(%)"])

                    st.dataframe(
                        styled_kw,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "광고그룹": st.column_config.TextColumn(width="medium"),
                            "키워드": st.column_config.TextColumn(width="medium")
                        }
                    )
                else:
                    st.info("해당 캠페인에 등록된 하위 키워드/소재 데이터가 조회 기간 내에 없습니다.")

    with tab_group:
        if kw_bundle_cur is None or kw_bundle_cur.empty:
            st.info("광고그룹 성과 데이터가 없습니다.")
        else:
            grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in kw_bundle_cur.columns]
            val_cols = [c for c in ["imp", "clk", "cost", "conv", "sales"] if c in kw_bundle_cur.columns]
            if not grp_cols or not val_cols:
                st.info("광고그룹 성과 데이터가 없습니다.")
            else:
                grp = kw_bundle_cur.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp = _perf_common_merge_meta(grp, meta)
                grouped = grp.rename(columns={
                    "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인",
                    "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
                }).copy()

                rank_map_grp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id", "adgroup_id"])
                if not rank_map_grp.empty:
                    key_cols_grp = ["customer_id", "campaign_id", "adgroup_id"]
                    grouped = _normalize_merge_keys(grouped, key_cols_grp)
                    rank_map_grp = _normalize_merge_keys(rank_map_grp, key_cols_grp)
                    grouped = grouped.merge(rank_map_grp, on=key_cols_grp, how="left")
                    grouped["평균순위"] = grouped["avg_rank"].apply(_format_avg_rank)

                camps = ["전체"] + sorted([str(x) for x in grouped["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in grouped.columns else ["전체"]
                sel_camp = st.selectbox("캠페인 필터", camps, key="camp_group_filter")
                if sel_camp != "전체" and "캠페인" in grouped.columns:
                    grouped = grouped[grouped["캠페인"] == sel_camp]

                grouped = _add_perf_metrics(grouped)
                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                if "평균순위" in grouped.columns:
                    base_cols_grp.append("평균순위")
                metrics_cols_grp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]
                cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in grouped.columns]
                disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n)
                
                try:
                    styled_grp = disp_grp.style.format(fmt).map(highlight_roas_text, subset=["ROAS(%)"])
                except AttributeError:
                    styled_grp = disp_grp.style.format(fmt).applymap(highlight_roas_text, subset=["ROAS(%)"])

                grp_col_config = {
                    "업체명": st.column_config.TextColumn(width="small"),
                    "담당자": st.column_config.TextColumn(width="small"),
                    "캠페인유형": st.column_config.TextColumn(width="small"),
                    "캠페인": st.column_config.TextColumn(width="medium"),
                    "광고그룹": st.column_config.TextColumn(width="medium")
                }

                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>광고그룹별 성과 데이터</div>", unsafe_allow_html=True)
                st.dataframe(styled_grp, use_container_width=True, hide_index=True, column_config=grp_col_config)

    with tab_cmp:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")

        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)
        base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=50000)

        view_cmp = view.copy()
        if not base_bundle.empty:
            valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
            if valid_keys:
                view_cmp = append_comparison_data(view_cmp, base_bundle, valid_keys)

        if not base_kw_bundle.empty:
            base_rank_map = _keyword_rank_by_keys(base_kw_bundle, ["customer_id", "campaign_id"]).rename(columns={"avg_rank": "base_avg_rank"})
            if not base_rank_map.empty:
                key_cols_cmp = ["customer_id", "campaign_id"]
                view_cmp = _normalize_merge_keys(view_cmp, key_cols_cmp)
                base_rank_map = _normalize_merge_keys(base_rank_map, key_cols_cmp)
                view_cmp = view_cmp.merge(base_rank_map, on=key_cols_cmp, how="left")

        base_for_search = base_bundle.rename(columns={"campaign_name": "캠페인"}) if not base_bundle.empty else pd.DataFrame()
        render_item_comparison_search("캠페인", view_cmp, base_for_search, "캠페인", f["start"], f["end"], b1, b2)

        base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인"]
        if "평균순위" in view_cmp.columns:
            base_cols_cmp.append("평균순위")
        metrics_cols_cmp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
        final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
        disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n)

        styled_cmp = disp_cmp.style.format(fmt)
        delta_cols = [c for c in ["광고비 증감(%)", "ROAS 증감(%)", "전환 증감"] if c in disp_cmp.columns]
        if delta_cols:
            try:
                styled_cmp = styled_cmp.map(style_table_deltas, subset=delta_cols)
            except AttributeError:
                styled_cmp = styled_cmp.applymap(style_table_deltas, subset=delta_cols)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>캠페인별 기간 비교 데이터</div>", unsafe_allow_html=True)
        render_big_table(styled_cmp, "camp_grid_cmp", 550)

    with tab_history:
        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty:
            st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        else:
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty:
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                off_log["campaign_id"] = off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
            else:
                off_log["campaign_name"] = off_log["campaign_id"]
                
            if not meta.empty:
                meta_copy = meta.copy()
                meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
                off_log["customer_id"] = off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            else:
                off_log["account_name"] = off_log["customer_id"]
            
            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            
            pivot_df = off_log.pivot_table(
                index=["account_name", "campaign_name"], 
                columns="dt_str", 
                values="off_time", 
                aggfunc='first'
            ).reset_index()
            
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인명"}).fillna("-")
            
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>일자별 꺼짐 기록</div>", unsafe_allow_html=True)
            st.dataframe(pivot_df, use_container_width=True, hide_index=True)

    loading_placeholder.empty()
    st.toast("✅ 데이터 집계 및 화면 렌더링이 완료되었습니다!", icon="🚀")
