# page_helpers.py 파일 맨 아래에 추가해 주세요.

def render_item_comparison_search(entity_label: str, df_cur: pd.DataFrame, df_base: pd.DataFrame, name_col: str, d1: date, d2: date, b1: date, b2: date):
    import streamlit as st
    import pandas as pd
    
    st.markdown(f"<div style='font-size:16px; font-weight:700; margin-top:24px; margin-bottom:12px;'>🔍 특정 {entity_label} 상세 성과 비교</div>", unsafe_allow_html=True)
    
    items_cur = set(df_cur[name_col].dropna().astype(str).unique()) if not df_cur.empty and name_col in df_cur.columns else set()
    items_base = set(df_base[name_col].dropna().astype(str).unique()) if not df_base.empty and name_col in df_base.columns else set()
    
    # 공백이나 None 제거 후 정렬
    all_items = sorted([x for x in list(items_cur | items_base) if str(x).strip() != ''])
    
    if not all_items:
        st.info("검색 가능한 데이터가 없습니다.")
        return
        
    selected = st.selectbox(f"분석할 {entity_label}을(를) 검색 및 선택하세요.", ["- 선택 안함 -"] + all_items)
    
    if selected != "- 선택 안함 -":
        c_df = df_cur[df_cur[name_col] == selected] if not df_cur.empty else pd.DataFrame()
        b_df = df_base[df_base[name_col] == selected] if not df_base.empty else pd.DataFrame()
        
        def _get(df, c): return float(df[c].sum()) if not df.empty and c in df.columns else 0.0
        
        c_cost, c_sales = _get(c_df, "cost"), _get(c_df, "sales")
        c_clk, c_imp, c_conv = _get(c_df, "clk"), _get(c_df, "imp"), _get(c_df, "conv")
        c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
        
        b_cost, b_sales = _get(b_df, "cost"), _get(b_df, "sales")
        b_clk, b_imp, b_conv = _get(b_df, "clk"), _get(b_df, "imp"), _get(b_df, "conv")
        b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0
        
        def fmt_krw(v): return f"{int(v):,}원"
        def fmt_num(v): return f"{int(v):,}"
        def fmt_pct(v): return f"{v:.1f}%"
        
        def calc_delta(c, b, reverse=False):
            if b == 0: return "<span style='color:#888;'>비교불가</span>"
            pct = (c - b) / b * 100
            if pct == 0: return "<span style='color:#888;'>변동없음</span>"
            
            is_good = (pct < 0) if reverse else (pct > 0) # 비용은 줄어드는게(reverse) Good
            color = "#FC503D" if not is_good else "#32D74B" # Red=Bad, Green=Good
            sign = "▲" if pct > 0 else "▼"
            return f"<span style='color:{color}; font-weight:700;'>{sign} {abs(pct):.1f}%</span>"
            
        html = f"""
        <div style='background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 12px; padding: 20px; margin-top: 12px; margin-bottom: 24px;'>
            <div style='font-size: 15px; font-weight: 800; color: #111; margin-bottom: 16px; border-bottom: 1px solid #ddd; padding-bottom: 8px;'>
                🎯 [{selected}] 성과 대조표
            </div>
            <div style='display: flex; gap: 20px; justify-content: space-between; flex-wrap: wrap;'>
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #375FFF; margin-bottom: 12px;'>🔵 현재 기간 ({d1} ~ {d2})</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='font-weight:600; float:right;'>{fmt_krw(c_cost)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='font-weight:600; float:right;'>{fmt_krw(c_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='font-weight:600; color:#375FFF; float:right;'>{fmt_pct(c_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='font-weight:600; float:right;'>{fmt_num(c_conv)}</span>
                    </div>
                </div>
                
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #777; margin-bottom: 12px;'>⚪ 비교 기간 ({b1} ~ {b2})</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='font-weight:600; float:right;'>{fmt_krw(b_cost)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='font-weight:600; float:right;'>{fmt_krw(b_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='font-weight:600; float:right;'>{fmt_pct(b_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='font-weight:600; float:right;'>{fmt_num(b_conv)}</span>
                    </div>
                </div>
                
                <div style='flex: 1; min-width: 200px; background-color: #fff; padding: 16px; border-radius: 8px; border: 1px solid #eee; box-shadow: 0 2px 4px rgba(0,0,0,0.02);'>
                    <div style='font-size: 12px; font-weight: 700; color: #111; margin-bottom: 12px;'>📊 증감 (Delta)</div>
                    <div style='font-size: 14px; line-height: 1.8;'>
                        <span style='color:#555;'>광고비:</span> <span style='float:right;'>{calc_delta(c_cost, b_cost, reverse=True)}</span><br>
                        <span style='color:#555;'>전환매출:</span> <span style='float:right;'>{calc_delta(c_sales, b_sales)}</span><br>
                        <span style='color:#555;'>ROAS:</span> <span style='float:right;'>{calc_delta(c_roas, b_roas)}</span><hr style='margin:8px 0; border:0; border-top:1px dashed #eee;'>
                        <span style='color:#555;'>노출수:</span> <span style='float:right;'>{calc_delta(c_imp, b_imp)}</span><br>
                        <span style='color:#555;'>클릭수:</span> <span style='float:right;'>{calc_delta(c_clk, b_clk)}</span><br>
                        <span style='color:#555;'>전환수:</span> <span style='float:right;'>{calc_delta(c_conv, b_conv)}</span>
                    </div>
                </div>
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
