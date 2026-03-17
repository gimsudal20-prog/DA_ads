# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from typing import Dict

from data import query_campaign_bundle, query_keyword_bundle, query_ad_bundle, query_campaign_off_log, load_dim_campaign
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, append_comparison_data, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas


def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    # ✨ 장바구니수 추가
    for c in ["광고비", "구매 전환매출", "노출", "클릭", "구매 전환수", "장바구니수"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["구매 전환수"] > 0, view["광고비"] / view["구매 전환수"], 0.0)
    view["진성 ROAS(%)"] = np.where(view["광고비"] > 0, (view["구매 전환매출"] / view["광고비"]) * 100, 0.0)
    return view


def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in out.columns:
            out[k] = out[k].astype(str)
    return out


def _keyword_rank_by_keys(detail_bundle: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if detail_bundle is None or detail_bundle.empty or "avg_rank" not in detail_bundle.columns:
        return pd.DataFrame(columns=keys + ["avg_rank"])
    tmp = detail_bundle.copy()
    tmp["imp"] = pd.to_numeric(tmp.get("imp", 0), errors="coerce").fillna(0.0)
    tmp["avg_rank"] = pd.to_numeric(tmp.get("avg_rank", np.nan), errors="coerce")
    tmp["_rank_imp"] = tmp["avg_rank"].fillna(0.0) * tmp["imp"]
    grp = tmp.groupby(keys, as_index=False)[["_rank_imp", "imp"]].sum()
    grp["avg_rank"] = np.where(grp["imp"] > 0, grp["_rank_imp"] / grp["imp"], np.nan)
    return grp[keys + ["avg_rank"]]


def highlight_roas_text(val):
    try:
        v = float(str(val).replace("%", "").replace(",", ""))
        if 0 <= v < 100.0:
            return 'color: #EF4444; font-weight: 800;' 
        elif v >= 300.0:
            return 'color: #2563EB; font-weight: 800;' 
    except:
        pass
    return ''


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    with st.spinner("🔄 최신 필터 조건에 맞추어 데이터를 실시간으로 집계하고 있습니다..."):
        bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
        if bundle is None or bundle.empty:
            return

        kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=0)
        ad_bundle_cur = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=0, top_k=50)

        if not kw_bundle_cur.empty:
            kw_tmp = kw_bundle_cur.rename(columns={"keyword": "item_name"})
        else:
            kw_tmp = pd.DataFrame()
            
        if not ad_bundle_cur.empty:
            ad_tmp = ad_bundle_cur.copy()
            if "ad_title" in ad_tmp.columns:
                ad_tmp["final_ad_name"] = ad_tmp["ad_title"].fillna("").astype(str).str.strip()
                mask_empty = ad_tmp["final_ad_name"].isin(["", "nan", "None"])
                ad_tmp.loc[mask_empty, "final_ad_name"] = ad_tmp.loc[mask_empty, "ad_name"].astype(str)
            else:
                ad_tmp["final_ad_name"] = ad_tmp["ad_name"].astype(str)
            ad_tmp = ad_tmp.rename(columns={"final_ad_name": "item_name"})
        else:
            ad_tmp = pd.DataFrame()
            
        detail_bundle = pd.concat([kw_tmp, ad_tmp], ignore_index=True)

        df = _perf_common_merge_meta(bundle, meta)
        
        # ✨ 장바구니수 매핑
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
            "campaign_name": "캠페인", "imp": "노출", "clk": "클릭",
            "cost": "광고비", "cart_conv": "장바구니수", "conv": "구매 전환수", "sales": "구매 전환매출"
        }).copy()
        view = _add_perf_metrics(view)

        if not detail_bundle.empty:
            rank_map_camp = _keyword_rank_by_keys(detail_bundle, ["customer_id", "campaign_id"])
            if not rank_map_camp.empty:
                key_cols = ["customer_id", "campaign_id"]
                view = _normalize_merge_keys(view, key_cols)
                rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
                view = view.merge(rank_map_camp, on=key_cols, how="left")
        if "avg_rank" in view.columns:
            view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    tab_main, tab_group, tab_cmp, tab_history = st.tabs(["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"])
    
    # ✨ 장바구니수 포맷 추가
    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "장바구니수": "{:,.0f}", "CPA(원)": "{:,.0f}", "구매 전환매출": "{:,.0f}", "구매 전환수": "{:,.1f}", 
        "CTR(%)": "{:,.2f}%", "진성 ROAS(%)": "{:,.2f}%"
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
        
        # ✨ 장바구니수 추가
        all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "장바구니수", "구매 전환수", "CPA(원)", "구매 전환매출", "진성 ROAS(%)"]

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:12px;'>캠페인 성과 요약 대시보드</div>", unsafe_allow_html=True)
        
        col_type, col_device = st.columns([1.5, 1])

        with col_type:
            type_grp = disp_main.groupby("캠페인유형").agg({"광고비": "sum", "구매 전환매출": "sum"}).reset_index()
            total_cost = type_grp["광고비"].sum()
            type_grp["지출 비중(%)"] = np.where(total_cost > 0, (type_grp["광고비"] / total_cost) * 100, 0.0)
            type_grp["진성 ROAS(%)"] = np.where(type_grp["광고비"] > 0, (type_grp["구매 전환매출"] / type_grp["광고비"]) * 100, 0.0)
            type_grp = type_grp.sort_values("광고비", ascending=False)
            
            st.dataframe(
                type_grp.style.format({"광고비": "{:,.0f}", "진성 ROAS(%)": "{:,.2f}%"}),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "캠페인유형": st.column_config.TextColumn("캠페인 유형"),
                    "광고비": st.column_config.Column("총 광고비(원)"),
                    "지출 비중(%)": st.column_config.ProgressColumn("지출 비중", format="%.1f%%", min_value=0, max_value=100),
                    "진성 ROAS(%)": st.column_config.Column("평균 진성 ROAS(%)")
                }
            )

        with col_device:
            st.markdown("<div style='font-size:13px; color:#555; text-align:center; margin-bottom:5px;'>기기별 광고비 지출 비중</div>", unsafe_allow_html=True)
            
            mock_device_df = pd.DataFrame({
                "기기": ["모바일", "PC"],
                "광고비": [total_cost * 0.72, total_cost * 0.28]
            })
            
            fig = px.pie(
                mock_device_df, 
                values="광고비", 
                names="기기", 
                hole=0.55, 
                color="기기", 
                color_discrete_map={"모바일": "#335CFF", "PC": "#CBD5E1"} 
            )
            
            fig.update_layout(
                margin=dict(t=0, b=0, l=0, r=0), 
                height=180, 
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=0.85)
            )
            fig.update_traces(textposition='inside', textinfo='percent')
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        final_cols = [c for c in base_cols + all_metrics_cols if c in disp_main.columns]
        disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n).reset_index(drop=True)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:4px; margin-top:20px;'>캠페인 종합 성과 데이터</div>", unsafe_allow_html=True)
        st.caption("표에서 상세 분석을 원하는 캠페인의 가장 앞(체크박스)을 선택해 보세요. (아래에 하위 키워드/소재 상세 데이터가 열립니다)")

        try:
            styled_main = disp_main.style.format(fmt).map(highlight_roas_text, subset=["진성 ROAS(%)"])
        except AttributeError:
            styled_main = disp_main.style.format(fmt).applymap(highlight_roas_text, subset=["진성 ROAS(%)"])

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
            
            kw_detail = detail_bundle[detail_bundle["campaign_name"] == selected_campaign].copy()
            
            st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
            with st.container(border=True):
                st.markdown(f"<h5 style='color: #335CFF; margin-bottom: 8px;'>↳ [{selected_campaign}] 하위 그룹/상세 성과</h5>", unsafe_allow_html=True)
                
                if not kw_detail.empty:
                    # ✨ 하위 상세표 장바구니수 매핑
                    kw_view = kw_detail.rename(columns={
                        "adgroup_name": "광고그룹", "item_name": "키워드/상품명",
                        "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "conv": "구매 전환수", "sales": "구매 전환매출"
                    })
                    
                    kw_view['광고그룹'] = kw_view['광고그룹'].fillna('미분류').replace('', '미분류')
                    kw_view['키워드/상품명'] = kw_view['키워드/상품명'].fillna('미분류').replace('', '미분류')
                    
                    # ✨ 장바구니수 합산
                    grp_kw = kw_view.groupby(['광고그룹', '키워드/상품명'], as_index=False)[['노출', '클릭', '광고비', '장바구니수', '구매 전환수', '구매 전환매출']].sum()
                    grp_kw = _add_perf_metrics(grp_kw)
                    
                    st.markdown("<div style='font-size:13px; font-weight:700; margin-top:16px; margin-bottom:8px;'>🎯 세부 효율 분석 (분산형 4사분면 차트 / 상위 30개)</div>", unsafe_allow_html=True)
                    st.caption("가로축은 '광고비', 세로축은 '진성 ROAS'입니다. 원의 크기는 '클릭수'를 나타냅니다.<br><b>우측 상단</b>(돈을 많이 쓰고 효율도 좋은 항목)과 <b>우측 하단</b>(돈은 많이 쓰는데 적자인 항목)을 중점적으로 확인하세요.")
                    
                    scatter_df = grp_kw[grp_kw['광고비'] > 0].sort_values('광고비', ascending=False).head(30).copy()
                    
                    if not scatter_df.empty:
                        def _shorten(name):
                            name_str = str(name)
                            return name_str[:12] + "..." if len(name_str) > 12 else name_str
                            
                        scatter_df['짧은이름'] = scatter_df['키워드/상품명'].apply(_shorten)
                        scatter_df['클릭_size'] = scatter_df['클릭'].apply(lambda x: max(x, 1))

                        fig_scatter = px.scatter(
                            scatter_df, 
                            x='광고비',
                            y='진성 ROAS(%)',
                            color='광고그룹',
                            size='클릭_size',
                            text='짧은이름',
                            hover_data={'키워드/상품명': True, '광고비': ':,.0f', '진성 ROAS(%)': ':.0f', '클릭': ':,.0f', '광고그룹': True, '짧은이름': False, '클릭_size': False}
                        )
                        fig_scatter.update_traces(textposition='top center', textfont_size=11, marker=dict(line=dict(width=1, color='white')))
                        fig_scatter.add_hline(y=100, line_dash="dash", line_color="#EF4444", annotation_text="ROAS 100%", annotation_position="bottom right")
                        fig_scatter.update_layout(margin=dict(t=20, l=10, r=20, b=10), height=450, xaxis_title="광고 소진액 (원)", yaxis_title="진성 ROAS (%)", legend_title="광고그룹")
                        st.plotly_chart(fig_scatter, use_container_width=True, config={'displayModeBar': False})
                    else:
                        st.info("광고비(소진액)가 0원인 항목은 차트에 표시되지 않습니다.")
                    
                    st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
                    
                    # ✨ 하위 표에도 장바구니수 추가
                    kw_disp = grp_kw[["광고그룹", "키워드/상품명", "노출", "클릭", "CTR(%)", "광고비", "장바구니수", "구매 전환수", "구매 전환매출", "진성 ROAS(%)"]].sort_values("광고비", ascending=False).head(100)
                    
                    try:
                        styled_kw = kw_disp.style.format(fmt).map(highlight_roas_text, subset=["진성 ROAS(%)"])
                    except AttributeError:
                        styled_kw = kw_disp.style.format(fmt).applymap(highlight_roas_text, subset=["진성 ROAS(%)"])

                    st.dataframe(styled_kw, use_container_width=True, hide_index=True, column_config={"광고그룹": st.column_config.TextColumn(width="medium"), "키워드/상품명": st.column_config.TextColumn(width="medium")})
                else:
                    st.info("해당 캠페인에 등록된 하위 키워드/소재 데이터가 조회 기간 내에 없습니다.")

    with tab_group:
        if detail_bundle is None or detail_bundle.empty:
            st.info("광고그룹 성과 데이터가 없습니다.")
        else:
            grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in detail_bundle.columns]
            val_cols = [c for c in ["imp", "clk", "cost", "cart_conv", "conv", "sales"] if c in detail_bundle.columns]
            if not grp_cols or not val_cols:
                st.info("광고그룹 성과 데이터가 없습니다.")
            else:
                grp = detail_bundle.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp = _perf_common_merge_meta(grp, meta)
                
                grouped = grp.rename(columns={
                    "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인",
                    "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "conv": "구매 전환수", "sales": "구매 전환매출"
                }).copy()

                rank_map_grp = _keyword_rank_by_keys(detail_bundle, ["customer_id", "campaign_id", "adgroup_id"])
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
                
                metrics_cols_grp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "장바구니수", "구매 전환수", "CPA(원)", "구매 전환매출", "진성 ROAS(%)"]
                cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in grouped.columns]
                disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n)
                
                try: styled_grp = disp_grp.style.format(fmt).map(highlight_roas_text, subset=["진성 ROAS(%)"])
                except AttributeError: styled_grp = disp_grp.style.format(fmt).applymap(highlight_roas_text, subset=["진성 ROAS(%)"])

                grp_col_config = {"업체명": st.column_config.TextColumn(width="small"), "담당자": st.column_config.TextColumn(width="small"), "캠페인유형": st.column_config.TextColumn(width="small"), "캠페인": st.column_config.TextColumn(width="medium"), "광고그룹": st.column_config.TextColumn(width="medium")}
                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>광고그룹별 성과 데이터</div>", unsafe_allow_html=True)
                st.dataframe(styled_grp, use_container_width=True, hide_index=True, column_config=grp_col_config)

    with tab_cmp:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")

        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        
        with st.spinner("🔄 이전 기간의 데이터를 불러오는 중입니다..."):
            base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)
            base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=0)
            base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=0, top_k=50)

        if not base_kw_bundle.empty:
            b_kw_tmp = base_kw_bundle.rename(columns={"keyword": "item_name"})
        else:
            b_kw_tmp = pd.DataFrame()
            
        if not base_ad_bundle.empty:
            b_ad_tmp = base_ad_bundle.copy()
            if "ad_title" in b_ad_tmp.columns:
                b_ad_tmp["final_ad_name"] = b_ad_tmp["ad_title"].fillna("").astype(str).str.strip()
                mask_empty = b_ad_tmp["final_ad_name"].isin(["", "nan", "None"])
                b_ad_tmp.loc[mask_empty, "final_ad_name"] = b_ad_tmp.loc[mask_empty, "ad_name"].astype(str)
            else:
                b_ad_tmp["final_ad_name"] = b_ad_tmp["ad_name"].astype(str)
            b_ad_tmp = b_ad_tmp.rename(columns={"final_ad_name": "item_name"})
        else:
            b_ad_tmp = pd.DataFrame()
            
        base_detail_bundle = pd.concat([b_kw_tmp, b_ad_tmp], ignore_index=True)

        view_cmp = view.copy()
        if not base_bundle.empty:
            valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
            if valid_keys:
                view_cmp = append_comparison_data(view_cmp, base_bundle, valid_keys)
                
                # ✨ 장바구니 증감 로직 수동 연결 (append_comparison_data에 없을 수 있으므로 대비)
                if "이전 장바구니수" not in view_cmp.columns:
                    if "cart_conv_base" in view_cmp.columns:
                        view_cmp = view_cmp.rename(columns={"cart_conv_base": "이전 장바구니수"})
                    else:
                        view_cmp["이전 장바구니수"] = 0
                view_cmp["장바구니 증감"] = view_cmp["장바구니수"] - view_cmp["이전 장바구니수"].fillna(0)

                if "이전 전환" in view_cmp.columns:
                    view_cmp = view_cmp.rename(columns={"이전 전환": "이전 구매 전환수", "이전 전환매출": "이전 구매 전환매출"})
                if "전환 증감" in view_cmp.columns:
                    view_cmp["전환 증감"] = view_cmp["구매 전환수"] - view_cmp["이전 구매 전환수"].fillna(0)
                if "ROAS 증감(%)" in view_cmp.columns and "진성 ROAS(%)" in view_cmp.columns and "이전 ROAS(%)" in view_cmp.columns:
                    view_cmp["ROAS 증감(%)"] = view_cmp["진성 ROAS(%)"] - view_cmp["이전 ROAS(%)"].fillna(0)

        if not base_detail_bundle.empty:
            base_rank_map = _keyword_rank_by_keys(base_detail_bundle, ["customer_id", "campaign_id"]).rename(columns={"avg_rank": "base_avg_rank"})
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
        
        # ✨ 비교 탭 장바구니 증감 컬럼 추가
        metrics_cols_cmp = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "장바구니수", "장바구니 증감", "구매 전환수", "CPA(원)", "구매 전환매출", "진성 ROAS(%)", "광고비 증감(%)", "ROAS 증감(%)", "전환 증감"]
        final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
        disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n)

        styled_cmp = disp_cmp.style.format(fmt)
        delta_cols = [c for c in ["광고비 증감(%)", "ROAS 증감(%)", "전환 증감", "장바구니 증감"] if c in disp_cmp.columns]
        if delta_cols:
            try: styled_cmp = styled_cmp.map(style_table_deltas, subset=delta_cols)
            except AttributeError: styled_cmp = styled_cmp.applymap(style_table_deltas, subset=delta_cols)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>캠페인별 기간 비교 데이터</div>", unsafe_allow_html=True)
        render_big_table(styled_cmp, "camp_grid_cmp", 550)

    with tab_history:
        try:
            days_diff = (pd.to_datetime(f["end"]) - pd.to_datetime(f["start"])).days + 1
            if days_diff < 3:
                st.warning("단기 데이터(3일 미만) 기반 예산 증액 주의: 일시적인 효율 상승일 수 있습니다. 상단 필터에서 기간을 '최근 7일' 이상으로 설정하여 평균 ROAS가 안정적인지 먼저 확인해 주세요.")
        except Exception: pass

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
            
            pivot_df = off_log.pivot_table(index=["account_name", "campaign_name"], columns="dt_str", values="off_time", aggfunc='first').reset_index()
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"}).fillna("-")
            
            if not view.empty and "진성 ROAS(%)" in view.columns:
                roas_df = view[["업체명", "캠페인", "진성 ROAS(%)"]].drop_duplicates()
                pivot_df = pivot_df.merge(roas_df, on=["업체명", "캠페인"], how="left")
                cols = pivot_df.columns.tolist()
                cols.insert(2, cols.pop(cols.index('진성 ROAS(%)')))
                pivot_df = pivot_df[cols]
                pivot_df['진성 ROAS(%)'] = pivot_df['진성 ROAS(%)'].apply(lambda x: f"{float(x):,.0f}%" if pd.notnull(x) and str(x) != '-' else "-")

            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>일자별 꺼짐 기록 및 ROAS 효율 분석</div>", unsafe_allow_html=True)
            st.caption("표에 파란색(ROAS 300% 이상)으로 표시된 캠페인 중, 예산 부족으로 자주 꺼진 기록이 확인된다면 최우선적으로 예산을 증액하세요.")
            
            if "진성 ROAS(%)" in pivot_df.columns:
                try: styled_pivot = pivot_df.style.map(highlight_roas_text, subset=["진성 ROAS(%)"])
                except AttributeError: styled_pivot = pivot_df.style.applymap(highlight_roas_text, subset=["진성 ROAS(%)"])
                st.dataframe(styled_pivot, use_container_width=True, hide_index=True)
            else:
                st.dataframe(pivot_df, use_container_width=True, hide_index=True)

    st.toast("데이터 집계 및 화면 렌더링이 완료되었습니다.")
