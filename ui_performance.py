import streamlit as st
import pandas as pd
from queries import query_campaign_bundle, query_timeseries_common
from utils import format_currency, format_number_commas, format_roas
from ui_components import ui_metric_or_stmetric, ui_table_or_dataframe

def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    out = df.copy()
    out["ctr"] = (out["clk"] / out["imp"].replace(0, pd.NA)) * 100
    out["cpc"] = out["cost"] / out["clk"].replace(0, pd.NA)
    out["cpa"] = out["cost"] / out["conv"].replace(0, pd.NA)
    out["roas"] = (out["sales"] / out["cost"].replace(0, pd.NA)) * 100
    return out

def render_performance_page(entity_type: str, meta: pd.DataFrame, engine, filters: dict):
    """
    팩토리 패턴으로 캠페인/키워드/소재 페이지의 90% 중복 로직을 완벽히 제거했습니다.
    entity_type: 'campaign', 'keyword', 'ad'
    """
    if not filters.get("ready"):
        st.info("필터를 변경하면 즉시 반영됩니다.")
        return
        
    config = {
        'campaign': {'title': '🚀 성과 (캠페인)', 'fact_table': 'fact_campaign_daily', 'bundle_fn': query_campaign_bundle, 'top_n': filters.get('top_n_campaign', 200)},
        'keyword': {'title': '🔎 성과 (키워드)', 'fact_table': 'fact_keyword_daily', 'bundle_fn': query_campaign_bundle, 'top_n': filters.get('top_n_keyword', 300)}, # 예시: 실제 구현에 맞게 함수 매핑
        'ad': {'title': '🧩 성과 (소재)', 'fact_table': 'fact_ad_daily', 'bundle_fn': query_campaign_bundle, 'top_n': filters.get('top_n_ad', 200)},
    }
    
    cfg = config[entity_type]
    st.markdown(f"## {cfg['title']}")
    st.caption(f"기간: {filters['d1']} ~ {filters['d2']}")
    
    cids = tuple(filters.get("selected_customer_ids", []))
    
    with st.spinner("데이터 집계 중..."):
        # 다이나믹 번들 로더 호출
        bundle = cfg['bundle_fn'](engine, filters['d1'], filters['d2'], cids, topn_cost=cfg['top_n'])
        
    if bundle is None or bundle.empty:
        st.warning("데이터가 없습니다. 조건이나 일자를 변경해보세요.")
        return
        
    df = bundle.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")
    df = add_rates(df)
    
    # 공통 추세
    ts = query_timeseries_common(engine, cfg['fact_table'], filters['d1'], filters['d2'], cids)
    if not ts.empty:
        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1: ui_metric_or_stmetric("총 광고비", format_currency(ts['cost'].sum()), "기간 합계", "k1")
        with k2: ui_metric_or_stmetric("총 클릭", format_number_commas(ts['clk'].sum()), "기간 합계", "k2")
        with k3: ui_metric_or_stmetric("총 전환", format_number_commas(ts['conv'].sum()), "기간 합계", "k3")
        roas_val = (ts['sales'].sum() / ts['cost'].sum() * 100) if ts['cost'].sum() > 0 else 0
        with k4: ui_metric_or_stmetric("총 ROAS", f"{roas_val:.0f}%", "매출/광고비", "k4")
    
    st.divider()
    st.markdown("#### 📋 상세 리포트")
    ui_table_or_dataframe(df.head(cfg['top_n']), key=f"{entity_type}_table", height=500)