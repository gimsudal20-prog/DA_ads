# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict
from datetime import timedelta

from data import query_campaign_bundle, query_keyword_bundle, sql_read
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


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return
    st.markdown("<div class='nv-sec-title'>캠페인 상세 분석</div>", unsafe_allow_html=True)

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))

    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=20000)
    if bundle is None or bundle.empty:
        return

    kw_bundle_cur = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=50000)

    df = _perf_common_merge_meta(bundle, meta)
    view = df.rename(columns={
        "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형",
        "campaign_name": "캠페인", "imp": "노출", "clk": "클릭",
        "cost": "광고비", "conv": "전환", "sales": "전환매출"
    }).copy()
    view = _add_perf_metrics(view)

    # ---------------------------------------------------------
    # ✨ 스파크라인 버그 픽스: 완벽한 데이터 타입 클렌징 및 매핑
    # ---------------------------------------------------------
    try:
        end_date = pd.to_datetime(f["end"]).date()
        start_date = end_date - timedelta(days=6)
        
        # 1. SQL 쿼리를 파라미터 방식으로 변경하여 DB 에러 원천 차단
        sql = """
            SELECT campaign_id, dt, cost, sales 
            FROM fact_campaign_daily 
            WHERE dt BETWEEN :d1 AND :d2
        """
        params = {"d1": str(start_date), "d2": str(end_date)}
        
        if cids:
            cid_str = ",".join(f"'{c}'" for c in cids)
            sql += f" AND customer_id IN ({cid_str})"
            
        trend_df = sql_read(engine, sql, params)
        
        trend_map = {}
        if not trend_df.empty:
            trend_df['dt'] = pd.to_datetime(trend_df['dt'])
            trend_df['cost'] = pd.to_numeric(trend_df['cost'], errors='coerce').fillna(0)
            trend_df['sales'] = pd.to_numeric(trend_df['sales'], errors='coerce').fillna(0)
            
            # 날짜별 합산 후 ROAS 계산
            daily_grp = trend_df.groupby(['campaign_id', 'dt'])[['cost', 'sales']].sum().reset_index()
            daily_grp['roas'] = np.where(daily_grp['cost'] > 0, daily_grp['sales'] / daily_grp['cost'] * 100, 0)
            
            # 피벗팅
            pivot_df = daily_grp.pivot(index='campaign_id', columns='dt', values='roas').fillna(0)
            
            # 7일치 모든 날짜 컬럼을 강제로 생성하여 선이 끊기지 않게 함
            date_range = pd.date_range(start=start_date, end=end_date)
            pivot_df = pivot_df.reindex(columns=date_range, fill_value=0)
            
            # ★ 핵심: 인덱스(캠페인 ID)를 순수한 정수형 문자열로 강제 클렌징 ("1234.0" -> "1234")
            pivot_df.index = pd.to_numeric(pivot_df.index, errors='coerce').fillna(0).astype(int).astype(str)
            
            # 딕셔너리로 변환
            trend_map = {str(k): v.tolist() for k, v in pivot_df.iterrows()}
            
        # ★ 핵심: 원본 View 데이터의 캠페인 ID도 동일하게 클렌징하여 매핑 확률 100% 보장
        view_cid_clean = pd.to_numeric(view["campaign_id"], errors='coerce').fillna(0).astype(int).astype(str)
        view["최근 7일 ROAS"] = view_cid_clean.map(trend_map)
        
        # 매핑 안된 값(NaN)들은 [0]*7 로 초기화
        view["최근 7일 ROAS"] = view["최근 7일 ROAS"].apply(lambda x: x if isinstance(x, list) else [0]*7)
        
    except Exception as e:
        # 치명적 오류 발생 시 화면이 터지지 않도록 방어 코드
        st.error(f"트렌드 데이터 로딩 오류 (무시 가능): {str(e)}")
        view["최근 7일 ROAS"] = [[0]*7] * len(view)
    # ---------------------------------------------------------

    if not kw_bundle_cur.empty:
        rank_map_camp = _keyword_rank_by_keys(kw_bundle_cur, ["customer_id", "campaign_id"])
        if not rank_map_camp.empty:
            key_cols = ["customer_id", "campaign_id"]
            view = _normalize_merge_keys(view, key_cols)
            rank_map_camp = _normalize_merge_keys(rank_map_camp, key_cols)
            view = view.merge(rank_map_camp, on=key_cols, how="left")
    if "avg_rank" in view.columns:
        view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    tab_main, tab_group, tab_cmp = st.tabs(["종합 성과", "그룹 성과", "기간 비교"])
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
        
        metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)", "최근 7일 ROAS"]
        final_cols = [c for c in base_cols + metrics_cols if c in disp_main.columns]
        disp_main = disp_main[final_cols].sort_values("광고비", ascending=False).head(top_n)

        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>캠페인 종합 성과 데이터</div>", unsafe_allow_html=True)
        st.caption("💡 표에서 상세 분석을 원하는 **캠페인 행을 클릭**해 보세요. (아래에 하위 키워드/소재 상세 데이터가 열립니다)")

        # y_max를 지정하지 않아 내부 값에 따라 역동적으로 차트가 움직이도록 설정
        col_config = {
            "업체명": st.column_config.TextColumn(width="small"),
            "담당자": st.column_config.TextColumn(width="small"),
            "캠페인유형": st.column_config.TextColumn(width="small"),
            "캠페인": st.column_config.TextColumn(width="medium"),
            "노출": st.column_config.NumberColumn(format="%d"),
            "클릭": st.column_config.NumberColumn(format="%d"),
            "CTR(%)": st.column_config.NumberColumn(format="%.2f%%"),
            "CPC(원)": st.column_config.NumberColumn(format="%d"),
            "광고비": st.column_config.NumberColumn(format="%d"),
            "전환": st.column_config.NumberColumn(format="%.1f"),
            "CPA(원)": st.column_config.NumberColumn(format="%d"),
            "전환매출": st.column_config.NumberColumn(format="%d"),
            "ROAS(%)": st.column_config.NumberColumn(format="%.2f%%"),
            "최근 7일 ROAS": st.column_config.LineChartColumn("최근 7일 ROAS", y_min=0, width="small")
        }

        event = st.dataframe(
            disp_main,
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
                    kw_disp = kw_view[["광고그룹", "키워드", "노출", "클릭", "CTR(%)", "광고비", "전환", "전환매출", "ROAS(%)"]].sort_values("광고비", ascending=False).head(100)
                    
                    st.dataframe(
                        kw_disp,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "광고그룹": st.column_config.TextColumn(width="medium"),
                            "키워드": st.column_config.TextColumn(width="medium"),
                            "노출": st.column_config.NumberColumn(format="%d"),
                            "클릭": st.column_config.NumberColumn(format="%d"),
                            "CTR(%)": st.column_config.NumberColumn(format="%.2f%%"),
                            "광고비": st.column_config.NumberColumn(format="%d"),
                            "전환": st.column_config.NumberColumn(format="%.1f"),
                            "전환매출": st.column_config.NumberColumn(format="%d"),
                            "ROAS(%)": st.column_config.NumberColumn(format="%.2f%%")
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

                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>광고그룹별 성과 데이터</div>", unsafe_allow_html=True)
                render_big_table(disp_grp.style.format(fmt), "camp_group_grid", 550)

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
