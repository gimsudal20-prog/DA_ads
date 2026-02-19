# -----------------------------
# Rates
# -----------------------------
def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    out["ctr"] = (out["clk"] / out["imp"].replace({0: pd.NA})) * 100
    out["cpc"] = out["cost"] / out["clk"].replace({0: pd.NA})
    out["cpa"] = out["cost"] / out["conv"].replace({0: pd.NA})
    out["roas"] = (out["sales"] / out["cost"].replace({0: pd.NA})) * 100

    return out

# ==========================================
# 👇 여기부터 새로 추가할 함수들입니다.
# ==========================================
def add_summary_row(df: pd.DataFrame, label_col: str, type_col: str) -> pd.DataFrame:
    """데이터프레임 최상단에 총합계(Summary) 행을 추가합니다."""
    if df is None or df.empty:
        return df
    
    # 총계 계산
    s_imp = pd.to_numeric(df.get('imp', 0), errors='coerce').fillna(0).sum()
    s_clk = pd.to_numeric(df.get('clk', 0), errors='coerce').fillna(0).sum()
    s_cost = pd.to_numeric(df.get('cost', 0), errors='coerce').fillna(0).sum()
    s_conv = pd.to_numeric(df.get('conv', 0), errors='coerce').fillna(0).sum()
    s_sales = pd.to_numeric(df.get('sales', 0), errors='coerce').fillna(0).sum()

    # 비율 지표 재계산 (단순 합산 X)
    s_ctr = (s_clk / s_imp * 100) if s_imp > 0 else 0
    s_cpc = (s_cost / s_clk) if s_clk > 0 else 0
    s_cpa = (s_cost / s_conv) if s_conv > 0 else 0
    s_roas = (s_sales / s_cost * 100) if s_cost > 0 else 0

    # 캠페인 유형에 따른 라벨링 (예: [파워링크] 총 4개 종합)
    count = len(df)
    types = df.get(type_col, pd.Series(dtype=str)).dropna().unique()
    types = [t for t in types if str(t).strip() and t != '기타']
    if len(types) == 1:
        prefix = f"[{types[0]}] 총 {format_number_commas(count)}개 종합"
    else:
        prefix = f"[전체] 총 {format_number_commas(count)}개 종합"

    # 요약 행 딕셔너리 생성
    summary = {c: "" for c in df.columns}
    summary['imp'] = s_imp
    summary['clk'] = s_clk
    summary['cost'] = s_cost
    summary['conv'] = s_conv
    summary['sales'] = s_sales
    summary['ctr'] = s_ctr
    summary['cpc'] = s_cpc
    summary['cpa'] = s_cpa
    summary['roas'] = s_roas
    summary[label_col] = prefix
    summary['_is_summary'] = True  # 스타일링을 위한 플래그

    sum_df = pd.DataFrame([summary])
    out = pd.concat([sum_df, df], ignore_index=True)
    out['_is_summary'] = out['_is_summary'].fillna(False)
    return out

def style_summary(row):
    """요약 행(_is_summary=True)에만 회색 배경과 굵은 글씨를 적용합니다."""
    if row.get('_is_summary', False):
        return ['background-color: #f1f5f9; font-weight: bold; color: #0f172a;'] * len(row)
    return [''] * len(row)
# ==========================================
