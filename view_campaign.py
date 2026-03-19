# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from typing import Dict
from datetime import date

from data import query_campaign_bundle, query_keyword_bundle, query_ad_bundle, query_campaign_off_log, load_dim_campaign
from ui import render_big_table
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta, render_item_comparison_search, style_table_deltas


def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0:
        return "미수집"
    return f"{num:.1f}위"


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "구매완료 매출", "장바구니 매출액", "노출", "클릭", "구매완료수", "장바구니수"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)

    view["총 전환수"] = view.get("tot_conv", view.get("구매완료수", 0) + view.get("장바구니수", 0))
    view["총 전환매출"] = view.get("tot_sales", view.get("구매완료 매출", 0) + view.get("장바구니 매출액", 0))

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["구매 ROAS(%)"] = np.where(view["광고비"] > 0, (view["구매완료 매출"] / view["광고비"]) * 100, 0.0)
    view["통합 ROAS(%)"] = np.where(view["광고비"] > 0, (view["총 전환매출"] / view["광고비"]) * 100, 0.0)
    
    if "장바구니 매출액" in view.columns:
        view["장바구니 ROAS(%)"] = np.where(view["광고비"] > 0, (view["장바구니 매출액"] / view["광고비"]) * 100, 0.0)
    return view


def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    
    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)
            
    val_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_conv', 'tot_sales']
    for c in val_cols:
        if c in base_df.columns:
            base_df[c] = pd.to_numeric(base_df[c], errors='coerce').fillna(0)
            
    agg_dict = {c: 'sum' for c in val_cols if c in base_df.columns}
    if 'avg_rank' in base_df.columns:
        agg_dict['avg_rank'] = 'mean'
        base_df['avg_rank'] = pd.to_numeric(base_df['avg_rank'], errors='coerce')
        
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={c: f"b_{c}" for c in agg_dict.keys()})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()
        
    for c in val_cols:
        bc = f"b_{c}"
        if bc not in merged.columns: merged[bc] = 0
        merged[bc] = pd.to_numeric(merged[bc], errors='coerce').fillna(0)
        
    if 'b_avg_rank' not in merged.columns: merged['b_avg_rank'] = np.nan

    merged['이전 노출'] = merged['b_imp']
    merged['노출 증감'] = merged['노출'] - merged['이전 노출']
    merged['노출 증감(%)'] = np.where(merged['이전 노출'] > 0, (merged['노출 증감'] / merged['이전 노출']) * 100, np.where(merged['노출'] > 0, 100.0, 0.0))

    merged['이전 클릭'] = merged['b_clk']
    merged['클릭 증감'] = merged['클릭'] - merged['이전 클릭']
    merged['클릭 증감(%)'] = np.where(merged['이전 클릭'] > 0, (merged['클릭 증감'] / merged['이전 클릭']) * 100, np.where(merged['클릭'] > 0, 100.0, 0.0))

    merged['이전 광고비'] = merged['b_cost']
    merged['광고비 증감'] = merged['광고비'] - merged['이전 광고비']
    merged['광고비 증감(%)'] = np.where(merged['이전 광고비'] > 0, (merged['광고비 증감'] / merged['이전 광고비']) * 100, np.where(merged['광고비'] > 0, 100.0, 0.0))

    merged['이전 CPC(원)'] = np.where(merged['이전 클릭'] > 0, merged['이전 광고비'] / merged['이전 클릭'], 0.0)
    merged['CPC 증감'] = merged['CPC(원)'] - merged['이전 CPC(원)']
    merged['CPC 증감(%)'] = np.where(merged['이전 CPC(원)'] > 0, (merged['CPC 증감'] / merged['이전 CPC(원)']) * 100, np.where(merged['CPC(원)'] > 0, 100.0, 0.0))

    merged['이전 장바구니수'] = merged['b_cart_conv']
    merged['이전 장바구니 매출액'] = merged['b_cart_sales']
    merged['장바구니 증감'] = merged['장바구니수'] - merged['이전 장바구니수']
    merged['이전 장바구니 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 장바구니 매출액'] / merged['이전 광고비']) * 100, 0.0)
    merged['장바구니ROAS 증감'] = merged['장바구니 ROAS(%)'] - merged['이전 장바구니 ROAS(%)']

    merged['이전 구매완료수'] = merged['b_conv']
    merged['이전 구매완료 매출'] = merged['b_sales']
    merged['구매 증감'] = merged['구매완료수'] - merged['이전 구매완료수']
    merged['이전 구매 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 구매완료 매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['구매 ROAS 증감'] = merged['구매 ROAS(%)'] - merged['이전 구매 ROAS(%)']

    merged['이전 총 전환수'] = merged.get('b_tot_conv', merged['b_conv'] + merged['b_cart_conv'])
    merged['이전 총 전환매출'] = merged.get('b_tot_sales', merged['b_sales'] + merged['b_cart_sales'])
    merged['총 전환 증감'] = merged['총 전환수'] - merged['이전 총 전환수']
    merged['이전 통합 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 총 전환매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['통합 ROAS 증감'] = merged['통합 ROAS(%)'] - merged['이전 통합 ROAS(%)']

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns:
            merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['이전 평균순위'] = merged['b_avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)
        
    return merged


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
        if 0 <= v < 100.0: return 'color: #EF4444; font-weight: 800;' 
        elif v >= 300.0: return 'color: #2563EB; font-weight: 800;' 
    except: pass
    return ''


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    # ✨ 3월 11일 패치 전/후 구분
    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)
    
    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터가 포함되어 있어 '통합 전환' 기준으로 성과가 표시됩니다. (네이버 장바구니 분리 업데이트 이전)")
        funnel_toggle = False
    else:
        funnel_toggle = st.toggle("🔄 장바구니 / 위시리스트 / 통합 성과 함께 보기 (상세 모드)", value=False, key="camp_funnel_toggle")

    with st.spinner("🔄 최신 필터 조건에 맞추어 데이터를 실시간으로 집계하고 있습니다..."):
        bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
        if bundle is None or bundle.empty:
            return

        kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=0)
        ad_bundle_cur = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=0, top_k=50)

        if not kw_bundle_cur.empty: kw_tmp = kw_bundle_cur.rename(columns={"keyword": "item_name"})
        else: kw_tmp = pd.DataFrame()
            
        if not ad_bundle_cur.empty:
            ad_tmp = ad_bundle_cur.copy()
            if "ad_title" in ad_tmp.columns:
                ad_tmp["final_ad_name"] = ad_tmp["ad_title"].fillna("").astype(str).str.strip()
                mask_empty = ad_tmp["final_ad_name"].isin(["", "nan", "None"])
                ad_tmp.loc[mask_empty, "final_ad_name"] = ad_tmp.loc[mask_empty, "ad_name"].astype(str)
            else: ad_tmp["final_ad_name"] = ad_tmp["ad_name"].astype(str)
            ad_tmp = ad_tmp.rename(columns={"final_ad_name": "item_name"})
        else: ad_tmp = pd.DataFrame()
            
        detail_bundle = pd.concat([kw_tmp, ad_tmp], ignore_index=True)

        df = _perf_common_merge_meta(bundle, meta)
        
        view = df.rename(columns={
            "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
            "campaign_name": "캠페인", "imp": "노출", "clk": "클릭", "cost": "광고비", 
            "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액", "conv": "구매완료수", "sales": "구매완료 매출"
        }).copy()
        
        if "장바구니 매출액" not in view.columns: view["장바구니 매출액"] = 0
        if "장바구니수" not in view.columns: view["장바구니수"] = 0
            
        view = _add_perf_metrics(view)

        if not detail_bundle.empty:
            rank_map_camp = _keyword_rank_by_keys(detail_bundle, ["customer_id", "campaign_id"])
            if not rank_map_camp.empty:
                key_cols = ["customer_id", "campaign_id"]
                view = _normalize_merge_keys(view, key_cols)
                rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
                view = view.merge(rank_map_camp, on=key_cols, how="left")
        if "avg_rank" in view.columns: view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    tab_main, tab_group, tab_cmp, tab_history = st.tabs(["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"])
    
    # ✨ 실수형 및 퍼센트를 소수점 첫째자리로 강제 통일
    fmt = {
        "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
        "장바구니수": "{:,.1f}", "장바구니 매출액": "{:,.0f}원", "장바구니 ROAS(%)": "{:,.1f}%",
        "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
        "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%", "CTR(%)": "{:,.1f}%"
    }

    with tab_main:
        camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
        sel_camp_main = st.selectbox("캠페인 검색", camps_main, key="camp_name_filter_main")
        disp_main = view.copy()
        if sel_camp_main != "전체": disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

        base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
        if "평균순위" in disp_main.columns: base_cols.append("평균순위")
        
        # ✨ 패치일에 따른 기본/통합 뷰 로직 적용
        if has_pre_patch_cur:
            all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
            roas_col = "통합 ROAS(%)"
            sales_col = "총 전환매출"
        else:
            if not funnel_toggle:
                all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "구매완료수", "구매완료 매출", "구매 ROAS(%)"]
                roas_col = "구매 ROAS(%)"
                sales_col = "구매완료 매출"
            else:
                all_metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "장바구니수", "장바구니 매출액", "장바구니 ROAS(%)", "구매완료수", "구매완료 매출", "구매 ROAS(%)", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
                roas_col = "구매 ROAS(%)"
                sales_col = "구매완료 매출"

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:12px;'>캠페인 성과 요약 대시보드</div>", unsafe_allow_html=True)
        
        col_type, col_device = st.columns([1.5, 1])
        with col_type:
            type_grp = disp_main.groupby("캠페인유형").agg({"광고비": "sum", sales_col: "sum"}).reset_index()
            total_cost = type_grp["광고비"].sum()
            type_grp["지출 비중(%)"] = np.where(total_cost > 0, (type_grp["광고비"] / total_cost) * 100, 0.0)
            type_grp[roas_col] = np.where(type_grp["광고비"] > 0, (type_grp[sales_col] / type_grp["광고비"]) * 100, 0.0)
            type_grp = type_grp.sort_values("광고비", ascending=False)
            
            st.dataframe(
                type_grp.style.format({"광고비": "{:,.0f}", sales_col: "{:,.0f}원", roas_col: "{:,.1f}%"}),
                use_container_width=True, hide_index=True,
                column_config={
                    "캠페인유형": st.column_config.TextColumn("캠페인 유형"), 
                    "광고비": st.column_config.Column("총 광고비(원)"), 
                    sales_col: st.column_config.Column(f"{sales_col}"),
                    "지출 비중(%)": st.column_config.ProgressColumn("지출 비중", format="%.1f%%", min_value=0, max_value=100), 
                    roas_col: st.column_config.Column(f"평균 {roas_col}")
                }
            )

        with col_device:
            st.markdown("<div style='font-size:13px; color:#555; text-align:center; margin-bottom:5px;'>기기별 광고비 지출 비중</div>", unsafe_allow_html=True)
            mock_device_df = pd.DataFrame({"기기": ["모바일", "PC"], "광고비": [total_cost * 0.72, total_cost * 0.28]})
            fig = px.pie(mock_device_df, values="광고비", names="기기", hole=0.55, color="기기", color_discrete_map={"모바일": "#335CFF", "PC": "#CBD5E1"})
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=180, showlegend=True, legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=0.85))
            fig.update_traces(textposition='inside', textinfo='percent')
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        final_cols = [c for c in base_cols + all_metrics_cols if c in disp_main.columns]
        disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n).reset_index(drop=True)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:4px; margin-top:20px;'>캠페인 종합 성과 데이터</div>", unsafe_allow_html=True)
        try: styled_main = disp_main.style.format(fmt).map(highlight_roas_text, subset=[roas_col])
        except AttributeError: styled_main = disp_main.style.format(fmt).applymap(highlight_roas_text, subset=[roas_col])

        event = st.dataframe(styled_main, use_container_width=True, hide_index=True, selection_mode="single-row", on_select="rerun")

        selected_rows = event.selection.rows
        if selected_rows:
            selected_idx = selected_rows[0]
            selected_campaign = disp_main.iloc[selected_idx]["캠페인"]
            kw_detail = detail_bundle[detail_bundle["campaign_name"] == selected_campaign].copy()
            
            st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
            with st.container(border=True):
                st.markdown(f"<h5 style='color: #335CFF; margin-bottom: 8px;'>↳ [{selected_campaign}] 하위 그룹/상세 성과</h5>", unsafe_allow_html=True)
                
                if not kw_detail.empty:
                    if "cart_sales" not in kw_detail.columns: kw_detail["cart_sales"] = 0
                    if "cart_conv" not in kw_detail.columns: kw_detail["cart_conv"] = 0
                    kw_view = kw_detail.rename(columns={"adgroup_name": "광고그룹", "item_name": "키워드/상품명", "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액", "conv": "구매완료수", "sales": "구매완료 매출"})
                    kw_view['광고그룹'] = kw_view['광고그룹'].fillna('미분류').replace('', '미분류')
                    kw_view['키워드/상품명'] = kw_view['키워드/상품명'].fillna('미분류').replace('', '미분류')
                    
                    grp_kw = kw_view.groupby(['광고그룹', '키워드/상품명'], as_index=False)[['노출', '클릭', '광고비', '장바구니수', '장바구니 매출액', '구매완료수', '구매완료 매출']].sum()
                    grp_kw = _add_perf_metrics(grp_kw)
                    
                    st.markdown("<div style='font-size:13px; font-weight:700; margin-top:16px; margin-bottom:8px;'>🎯 세부 효율 분석 (분산형 4사분면 차트 / 상위 30개)</div>", unsafe_allow_html=True)
                    scatter_df = grp_kw[grp_kw['광고비'] > 0].sort_values('광고비', ascending=False).head(30).copy()
                    if not scatter_df.empty:
                        scatter_df['짧은이름'] = scatter_df['키워드/상품명'].apply(lambda x: str(x)[:12] + "...")
                        scatter_df['클릭_size'] = scatter_df['클릭'].apply(lambda x: max(x, 1))

                        fig_scatter = px.scatter(scatter_df, x='광고비', y=roas_col, color='광고그룹', size='클릭_size', text='짧은이름', hover_data={'키워드/상품명': True, '광고비': ':,.0f', roas_col: ':.1f', '클릭': ':,.0f'})
                        fig_scatter.update_traces(textposition='top center', textfont_size=11, marker=dict(line=dict(width=1, color='white')))
                        fig_scatter.add_hline(y=100, line_dash="dash", line_color="#EF4444")
                        fig_scatter.update_layout(margin=dict(t=20, l=10, r=20, b=10), height=450, xaxis_title="광고 소진액 (원)", yaxis_title=f"{roas_col}", legend_title="광고그룹")
                        st.plotly_chart(fig_scatter, use_container_width=True, config={'displayModeBar': False})
                    
                    st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
                    sub_cols = ["광고그룹", "키워드/상품명", "노출", "클릭", "CTR(%)", "광고비"] + (["장바구니수", "장바구니 매출액", "장바구니 ROAS(%)", "구매완료수", "구매완료 매출", "구매 ROAS(%)"] if funnel_toggle else ["총 전환수", "총 전환매출", "통합 ROAS(%)"])
                    kw_disp = grp_kw[[c for c in sub_cols if c in grp_kw.columns]].sort_values("광고비", ascending=False).head(100)
                    
                    try: styled_kw = kw_disp.style.format(fmt).map(highlight_roas_text, subset=[roas_col])
                    except AttributeError: styled_kw = kw_disp.style.format(fmt).applymap(highlight_roas_text, subset=[roas_col])
                    st.dataframe(styled_kw, use_container_width=True, hide_index=True)
                else: st.info("해당 캠페인에 등록된 하위 키워드/소재 데이터가 없습니다.")

    with tab_group:
        if detail_bundle is None or detail_bundle.empty: st.info("광고그룹 성과 데이터가 없습니다.")
        else:
            grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in detail_bundle.columns]
            val_cols = [c for c in ["imp", "clk", "cost", "cart_conv", "cart_sales", "conv", "sales"] if c in detail_bundle.columns]
            if not grp_cols or not val_cols: st.info("광고그룹 성과 데이터가 없습니다.")
            else:
                grp = detail_bundle.groupby(grp_cols, as_index=False)[val_cols].sum()
                grp = _perf_common_merge_meta(grp, meta)
                grouped = grp.rename(columns={"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액", "conv": "구매완료수", "sales": "구매완료 매출"}).copy()

                grouped = _add_perf_metrics(grouped)
                camps = ["전체"] + sorted([str(x) for x in grouped["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in grouped.columns else ["전체"]
                sel_camp = st.selectbox("캠페인 필터", camps, key="camp_group_filter")
                if sel_camp != "전체": grouped = grouped[grouped["캠페인"] == sel_camp]

                base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
                cols_grp = [c for c in base_cols_grp + all_metrics_cols if c in grouped.columns]
                disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n)
                
                try: styled_grp = disp_grp.style.format(fmt).map(highlight_roas_text, subset=[roas_col])
                except AttributeError: styled_grp = disp_grp.style.format(fmt).applymap(highlight_roas_text, subset=[roas_col])
                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>광고그룹별 성과 데이터</div>", unsafe_allow_html=True)
                st.dataframe(styled_grp, use_container_width=True, hide_index=True)

    with tab_cmp:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")
        b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
        
        with st.spinner("🔄 이전 기간의 데이터를 불러오는 중입니다..."):
            base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=20000)

        view_cmp = view.copy()
        if not base_bundle.empty:
            valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
            if valid_keys:
                view_cmp = _apply_comparison_metrics(view_cmp, base_bundle, valid_keys)

        base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인"]
        
        # ✨ 기간 비교시 패치일 믹스 감지
        has_pre_patch_base = (b1 < patch_date) if b1 else False
        if has_pre_patch_base or has_pre_patch_cur:
            st.warning("⚠️ 비교 기간에 3월 11일 이전(네이버 퍼널 분리 패치 전) 데이터가 포함되어 있습니다. 정확한 비교를 위해 '통합 전환' 기준으로 표시합니다.")
            show_mode = "integrated_only"
        else:
            show_mode = "purchase_default"

        def _combine(r, c_val, c_pct):
            v = r.get(c_val); p = r.get(c_pct)
            if pd.isna(v) or v == 0: return "-"
            v_str = f"{v:+,.0f}" if c_val in ["노출 증감", "클릭 증감", "광고비 증감", "CPC 증감"] else f"{v:+,.1f}"
            return f"{v_str} ({p:+.1f}%)"

        if show_mode == "integrated_only":
            view_cmp["노출 증감/율"] = view_cmp.apply(lambda r: _combine(r, "노출 증감", "노출 증감(%)"), axis=1)
            view_cmp["클릭 증감/율"] = view_cmp.apply(lambda r: _combine(r, "클릭 증감", "클릭 증감(%)"), axis=1)
            view_cmp["광고비 증감/율"] = view_cmp.apply(lambda r: _combine(r, "광고비 증감", "광고비 증감(%)"), axis=1)
            view_cmp["CPC 증감/율"] = view_cmp.apply(lambda r: _combine(r, "CPC 증감", "CPC 증감(%)"), axis=1)
            view_cmp["총 전환 증감 "] = view_cmp["총 전환 증감"].apply(lambda x: f"{x:+.1f}" if pd.notna(x) and x != 0 else "-")
            view_cmp["통합 ROAS 증감 "] = view_cmp["통합 ROAS 증감"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")

            metrics_cols_cmp = ["노출", "노출 증감/율", "클릭", "클릭 증감/율", "광고비", "광고비 증감/율", "CPC(원)", "CPC 증감/율", "총 전환수", "총 전환 증감 ", "총 전환매출", "통합 ROAS(%)", "통합 ROAS 증감 "]
            delta_cols = ["노출 증감/율", "클릭 증감/율", "광고비 증감/율", "CPC 증감/율", "총 전환 증감 ", "통합 ROAS 증감 "]
        else:
            if not funnel_toggle: # Purchase Default
                view_cmp["노출 증감/율"] = view_cmp.apply(lambda r: _combine(r, "노출 증감", "노출 증감(%)"), axis=1)
                view_cmp["클릭 증감/율"] = view_cmp.apply(lambda r: _combine(r, "클릭 증감", "클릭 증감(%)"), axis=1)
                view_cmp["광고비 증감/율"] = view_cmp.apply(lambda r: _combine(r, "광고비 증감", "광고비 증감(%)"), axis=1)
                view_cmp["CPC 증감/율"] = view_cmp.apply(lambda r: _combine(r, "CPC 증감", "CPC 증감(%)"), axis=1)
                view_cmp["구매 증감 "] = view_cmp["구매 증감"].apply(lambda x: f"{x:+.1f}" if pd.notna(x) and x != 0 else "-")
                view_cmp["구매 ROAS 증감 "] = view_cmp["구매 ROAS 증감"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) and x != 0 else "-")

                metrics_cols_cmp = ["노출", "노출 증감/율", "클릭", "클릭 증감/율", "광고비", "광고비 증감/율", "CPC(원)", "CPC 증감/율", "구매완료수", "구매 증감 ", "구매완료 매출", "구매 ROAS(%)", "구매 ROAS 증감 "]
                delta_cols = ["노출 증감/율", "클릭 증감/율", "광고비 증감/율", "CPC 증감/율", "구매 증감 ", "구매 ROAS 증감 "]
            else: # Everything separated
                metrics_cols_cmp = [
                    "이전 노출", "노출", "노출 증감", "노출 증감(%)",
                    "이전 클릭", "클릭", "클릭 증감", "클릭 증감(%)",
                    "이전 광고비", "광고비", "광고비 증감", "광고비 증감(%)",
                    "이전 장바구니수", "장바구니수", "장바구니 증감", "장바구니 증감(%)",
                    "이전 구매완료수", "구매완료수", "구매 증감", 
                    "이전 구매 ROAS(%)", "구매 ROAS(%)", "구매 ROAS 증감",
                    "이전 총 전환수", "총 전환수", "총 전환 증감",
                    "이전 통합 ROAS(%)", "통합 ROAS(%)", "통합 ROAS 증감"
                ]
                delta_cols = ["노출 증감(%)", "노출 증감", "클릭 증감(%)", "클릭 증감", "광고비 증감(%)", "광고비 증감", "장바구니 증감(%)", "장바구니 증감", "구매 증감", "구매 ROAS 증감", "총 전환 증감", "통합 ROAS 증감"]

        final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
        disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n)

        # 콤바인 스트링을 제외한 숫자 포맷팅용 딕셔너리
        fmt_cmp = {
            "노출": "{:,.0f}", "클릭": "{:,.0f}", "광고비": "{:,.0f}", "CPC(원)": "{:,.0f}",
            "장바구니수": "{:,.1f}", "구매완료수": "{:,.1f}", "구매완료 매출": "{:,.0f}원", "구매 ROAS(%)": "{:,.1f}%",
            "총 전환수": "{:,.1f}", "총 전환매출": "{:,.0f}원", "통합 ROAS(%)": "{:,.1f}%",
            "이전 노출": "{:,.0f}", "이전 클릭": "{:,.0f}", "이전 광고비": "{:,.0f}", "이전 CPC(원)": "{:,.0f}",
            "이전 장바구니수": "{:,.1f}", "이전 구매완료수": "{:,.1f}", "이전 총 전환수": "{:,.1f}",
            "노출 증감": "{:+,.0f}", "클릭 증감": "{:+,.0f}", "광고비 증감": "{:+,.0f}",
            "장바구니 증감": "{:+,.1f}", "구매 증감": "{:+,.1f}", "총 전환 증감": "{:+,.1f}",
            "노출 증감(%)": "{:+.1f}%", "클릭 증감(%)": "{:+.1f}%", "광고비 증감(%)": "{:+.1f}%", "장바구니 증감(%)": "{:+.1f}%"
        }

        styled_cmp = disp_cmp.style.format(fmt_cmp)
        if delta_cols:
            target_delta_cols = [c for c in delta_cols if c in disp_cmp.columns]
            if not funnel_toggle or show_mode == "integrated_only":
                try:
                    styled_cmp = styled_cmp.map(style_delta_str, subset=[c for c in target_delta_cols if c not in ["광고비 증감/율", "CPC 증감/율"]])
                    styled_cmp = styled_cmp.map(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in target_delta_cols])
                except AttributeError:
                    styled_cmp = styled_cmp.applymap(style_delta_str, subset=[c for c in target_delta_cols if c not in ["광고비 증감/율", "CPC 증감/율"]])
                    styled_cmp = styled_cmp.applymap(style_delta_str_neg, subset=[c for c in ["광고비 증감/율", "CPC 증감/율"] if c in target_delta_cols])
            else:
                try: styled_cmp = styled_cmp.map(style_table_deltas, subset=target_delta_cols)
                except AttributeError: styled_cmp = styled_cmp.applymap(style_table_deltas, subset=target_delta_cols)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>캠페인별 기간 비교 데이터</div>", unsafe_allow_html=True)
        render_big_table(styled_cmp, "camp_grid_cmp", 550)

    with tab_history:
        st.info("이 지면에서는 상세 퍼널보다 안정적인 광고 운영 여부가 중요합니다.")
        try:
            days_diff = (pd.to_datetime(f["end"]) - pd.to_datetime(f["start"])).days + 1
            if days_diff < 3: st.warning("단기 데이터(3일 미만) 기반 예산 증액 주의: 일시적인 효율 상승일 수 있습니다.")
        except Exception: pass

        off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
        if off_log.empty: st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        else:
            dim_camp = load_dim_campaign(engine)
            if not dim_camp.empty:
                dim_camp["campaign_id"] = dim_camp["campaign_id"].astype(str)
                off_log["campaign_id"] = off_log["campaign_id"].astype(str)
                off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
            else: off_log["campaign_name"] = off_log["campaign_id"]
                
            if not meta.empty:
                meta_copy = meta.copy()
                meta_copy["customer_id"] = meta_copy["customer_id"].astype(str)
                off_log["customer_id"] = off_log["customer_id"].astype(str)
                off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
            else: off_log["account_name"] = off_log["customer_id"]
            
            off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
            pivot_df = off_log.pivot_table(index=["account_name", "campaign_name"], columns="dt_str", values="off_time", aggfunc='first').reset_index()
            pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"}).fillna("-")
            
            if not view.empty and "통합 ROAS(%)" in view.columns:
                roas_df = view[["업체명", "캠페인", "통합 ROAS(%)"]].drop_duplicates()
                pivot_df = pivot_df.merge(roas_df, on=["업체명", "캠페인"], how="left")
                cols = pivot_df.columns.tolist()
                cols.insert(2, cols.pop(cols.index('통합 ROAS(%)')))
                pivot_df = pivot_df[cols]
                pivot_df['통합 ROAS(%)'] = pivot_df['통합 ROAS(%)'].apply(lambda x: f"{float(x):,.1f}%" if pd.notnull(x) and str(x) != '-' else "-")

            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>일자별 꺼짐 기록 및 ROAS 효율 분석</div>", unsafe_allow_html=True)
            
            if "통합 ROAS(%)" in pivot_df.columns:
                try: styled_pivot = pivot_df.style.map(highlight_roas_text, subset=["통합 ROAS(%)"])
                except AttributeError: styled_pivot = pivot_df.style.applymap(highlight_roas_text, subset=["통합 ROAS(%)"])
                st.dataframe(styled_pivot, use_container_width=True, hide_index=True)
            else:
                st.dataframe(pivot_df, use_container_width=True, hide_index=True)

    st.toast("데이터 집계 및 화면 렌더링이 완료되었습니다.")
