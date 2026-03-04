diff --git a/data.py b/data.py
index 7427b0a486841a73b6b801b234096c1d40d04005..d68b714640cfef6579392454f64915cd4d2ec12a 100644
--- a/data.py
+++ b/data.py
@@ -133,64 +133,76 @@ def get_latest_dates(_engine) -> dict:
 # ==========================================
 # 3. Helper Functions (Math & Formatting)
 # ==========================================
 def pct_change(cur: float, base: float) -> float:
     if not base or base == 0: return 100.0 if cur and cur > 0 else 0.0
     return ((cur - base) / base) * 100.0
 
 def pct_to_arrow(val) -> str:
     if val is None or pd.isna(val): return "-"
     if val > 0: return f"▲ {val:.1f}%"
     if val < 0: return f"▼ {abs(val):.1f}%"
     return "-"
 
 def format_currency(val) -> str:
     try: return f"{int(float(val)):,}원"
     except (ValueError, TypeError): return "0원"
 
 def format_number_commas(val) -> str:
     try: return f"{int(float(val)):,}"
     except (ValueError, TypeError): return "0"
 
 # ==========================================
 # 4. Data Aggregation Queries (🚀 인덱스 풀가동 최적화 버전)
 # ==========================================
 @st.cache_data(ttl=600, show_spinner=False)
-def query_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, avg_days: int) -> pd.DataFrame:
+def query_budget_bundle(_engine, cids: tuple, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, avg_days: int) -> pd.DataFrame:
     meta = get_meta(_engine)
     if meta.empty: return pd.DataFrame()
     
     where_cid = f"AND customer_id IN ({_sql_in_str_list(list(cids))})" if cids else ""
     
     sql_avg = f"SELECT customer_id, SUM(cost)/{avg_days}.0 as avg_cost FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
     df_avg = sql_read(_engine, sql_avg, {"d1": str(avg_d1), "d2": str(avg_d2)})
     
     sql_m = f"SELECT customer_id, SUM(cost) as current_month_cost, SUM(sales) as current_month_sales FROM fact_campaign_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
     df_m = sql_read(_engine, sql_m, {"d1": str(month_d1), "d2": str(month_d2)})
     
     if table_exists(_engine, "fact_bizmoney_daily"):
-        df_b = sql_read(_engine, f"SELECT customer_id, MAX(bizmoney_balance) as bizmoney_balance FROM fact_bizmoney_daily WHERE dt = :d1 {where_cid} GROUP BY customer_id", {"d1": str(yesterday)})
+        sql_b = f"""
+            SELECT customer_id, bizmoney_balance
+            FROM (
+                SELECT
+                    customer_id,
+                    bizmoney_balance,
+                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dt DESC) AS rn
+                FROM fact_bizmoney_daily
+                WHERE 1=1 {where_cid}
+            ) t
+            WHERE rn = 1
+        """
+        df_b = sql_read(_engine, sql_b)
     else:
         df_b = pd.DataFrame(columns=["customer_id", "bizmoney_balance"])
         
     df = meta.copy()
     if cids: df = df[df["customer_id"].isin(cids)]
     
     df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype(int)
     if not df_avg.empty: df_avg["customer_id"] = pd.to_numeric(df_avg["customer_id"], errors="coerce").fillna(0).astype(int)
     if not df_m.empty: df_m["customer_id"] = pd.to_numeric(df_m["customer_id"], errors="coerce").fillna(0).astype(int)
     if not df_b.empty: df_b["customer_id"] = pd.to_numeric(df_b["customer_id"], errors="coerce").fillna(0).astype(int)
     
     if not df_avg.empty: df = df.merge(df_avg, on="customer_id", how="left")
     if not df_m.empty: df = df.merge(df_m, on="customer_id", how="left")
     if not df_b.empty: df = df.merge(df_b, on="customer_id", how="left")
     
     for c in ["avg_cost", "current_month_cost", "current_month_sales", "bizmoney_balance", "monthly_budget"]:
         if c not in df.columns: df[c] = 0
         df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
         
     if "manager" not in df.columns: df["manager"] = "담당자 미지정"
     if "account_name" not in df.columns: df["account_name"] = df["customer_id"].astype(str)
     return df
 
 def update_monthly_budget(_engine, cid: int, val: int):
     try:
