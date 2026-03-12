def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    """비교 대상 데이터를 머지하여 상세한 이전수치, 증감, 증감률을 계산합니다."""
    if view_df.empty: return view_df
    
    # [수정된 부분] Merge 시 ValueError(타입 불일치) 방지를 위해 merge_keys의 데이터 타입을 문자열로 통일
    for k in merge_keys:
        if k in view_df.columns:
            view_df[k] = view_df[k].astype(str).str.replace(r'\.0$', '', regex=True)
        if k in base_df.columns:
            base_df[k] = base_df[k].astype(str).str.replace(r'\.0$', '', regex=True)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
    if 'avg_rank' in base_df.columns:
        agg_dict['avg_rank'] = 'mean'
        
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales', 'avg_rank': 'b_avg_rank'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = merged[c].fillna(0)
        
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

    merged['이전 전환'] = merged['b_conv']
    merged['전환 증감'] = merged['전환'] - merged['이전 전환']
    
    merged['이전 전환매출'] = merged['b_sales']
    merged['이전 ROAS(%)'] = np.where(merged['이전 광고비'] > 0, (merged['이전 전환매출'] / merged['이전 광고비']) * 100, 0.0)
    merged['ROAS 증감(%)'] = merged['ROAS(%)'] - merged['이전 ROAS(%)']

    if "avg_rank" in merged.columns:
        merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['이전 평균순위'] = merged['b_avg_rank'].apply(_format_avg_rank)
        # 평균순위는 값이 작아지는 것이 개선이므로 변동 폭 계산(현재 순위 - 이전 순위)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)
        
    return merged
