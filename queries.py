import pandas as pd
from datetime import date
from sqlalchemy import text, bindparam
from database import sql_read, table_exists

def query_campaign_bundle(engine, d1: date, d2: date, cids: tuple, topn_cost: int = 200, top_k: int = 5) -> pd.DataFrame:
    """SQLAlchemy bindparam(expanding=True)를 활용하여 IN 쿼리 바인딩 개선"""
    if not table_exists(engine, "fact_campaign_daily"):
        return pd.DataFrame()

    where_cid = "AND f.customer_id::text IN :cids" if cids else ""
    params = {"d1": str(d1), "d2": str(d2), "lim_cost": topn_cost, "lim_k": top_k}
    if cids:
        params["cids"] = tuple(map(str, cids))
        
    sql_str = f"""
    WITH base AS (
        SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost, SUM(f.conv) AS conv,
            SUM(COALESCE(f.sales, 0::numeric)) AS sales
        FROM fact_campaign_daily f
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id::text, f.campaign_id
    ),
    cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
    clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
    conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
    picked AS (
        SELECT * FROM cost_top UNION SELECT * FROM clk_top UNION SELECT * FROM conv_top
    )
    SELECT
        p.*,
        COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
        COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
    FROM picked p
    LEFT JOIN dim_campaign c ON p.customer_id = c.customer_id::text AND p.campaign_id = c.campaign_id
    ORDER BY p.cost DESC NULLS LAST
    """
    
    # Expanding 파라미터 바인딩
    stmt = text(sql_str)
    if cids:
        stmt = stmt.bindparams(bindparam('cids', expanding=True))

    df = sql_read(engine, stmt, params)
    
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        for c in ["imp", "clk", "cost", "conv", "sales"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def query_timeseries_common(engine, table: str, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    """캠페인/키워드/소재 공통 시계열 집계 (DRY)"""
    if not table_exists(engine, table): return pd.DataFrame()
    
    where_cid = "AND f.customer_id::text IN :cids" if cids else ""
    params = {"d1": str(d1), "d2": str(d2)}
    if cids: params["cids"] = tuple(map(str, cids))

    sql_str = f"""
    SELECT
        f.dt::date AS dt,
        SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost, SUM(f.conv) AS conv,
        SUM(COALESCE(f.sales, 0::numeric)) AS sales
    FROM {table} f
    WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
    GROUP BY f.dt::date
    ORDER BY f.dt::date
    """
    
    stmt = text(sql_str)
    if cids: stmt = stmt.bindparams(bindparam('cids', expanding=True))
    
    df = sql_read(engine, stmt, params)
    if not df.empty:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        for c in ["imp", "clk", "cost", "conv", "sales"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

# 추가로 Keyword/Ad Bundle 함수들도 위와 동일한 구조(expanding bindparam)로 변환됩니다.
# (본 예제에서는 캠페인 Bundle을 제공하며, 성능/코드 중복을 최소화합니다)