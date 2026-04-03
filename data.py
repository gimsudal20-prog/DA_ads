@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_customer"):
        return pd.DataFrame()
        
    # ✨ 수정: SELECT * 대신 꼭 필요한 컬럼만 지정
    cols = get_table_columns(_engine, "dim_customer")
    target_cols = [c for c in ["customer_id", "account_name", "manager", "monthly_budget"] if c in cols]
    select_str = ", ".join(target_cols) if target_cols else "*"
    
    df = sql_read(_engine, f"SELECT {select_str} FROM dim_customer")
    if not df.empty:
        rename_map = {}
        for c in df.columns:
            c_clean = str(c).replace(" ", "").lower()
            if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id"]:
                rename_map[c] = "customer_id"
            elif c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]:
                rename_map[c] = "account_name"
            elif c_clean in ["담당자", "manager"]:
                rename_map[c] = "manager"
        df = df.rename(columns=rename_map)
    return df

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()
        
    # ✨ 수정: SELECT * 대신 꼭 필요한 컬럼만 지정
    cols = get_table_columns(_engine, "dim_campaign")
    target_cols = [c for c in ["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type", "status", "target_roas", "min_roas"] if c in cols]
    select_str = ", ".join(target_cols) if target_cols else "*"
    
    return sql_read(_engine, f"SELECT {select_str} FROM dim_campaign")

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_campaign_off_log(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_off_log"):
        return pd.DataFrame()
    cids_tuple = tuple(cids) if cids else ()
    where_cid = f"AND customer_id IN ({_sql_in_str_list(cids_tuple)})" if cids_tuple else ""
    
    # ✨ 수정: SELECT * 제거 및 로드 제한 설정
    return sql_read(_engine, f"SELECT customer_id, campaign_id, off_time FROM fact_campaign_off_log WHERE dt BETWEEN :d1 AND :d2 {where_cid} LIMIT 5000", {"d1": str(d1), "d2": str(d2)})
