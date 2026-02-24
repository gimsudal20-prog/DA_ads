import os
import math
from datetime import date, timedelta, datetime
import pandas as pd
import streamlit as st
from sqlalchemy import text

from utils import init_page, format_currency, format_number_commas
from state import FilterState
from ui_sidebar import render_sidebar
from database import get_engine, sql_read, sql_exec, table_exists, get_meta
from ui_components import ui_metric_or_stmetric, ui_table_or_dataframe

TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

@st.cache_data(show_spinner=False, ttl=180)
def query_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, avg_days: int) -> pd.DataFrame:
    if not (table_exists(_engine, "dim_account_meta") and table_exists(_engine, "fact_campaign_daily") and table_exists(_engine, "fact_bizmoney_daily")):
        return pd.DataFrame()

    params = {
        "y": str(yesterday), "a1": str(avg_d1), "a2": str(avg_d2),
        "m1": str(month_d1), "m2": str(month_d2),
        "min_dt": str(min(yesterday, avg_d1, month_d1)),
        "max_dt": str(max(yesterday, avg_d2, month_d2))
    }
    
    where_cid = ""
    if cids:
        cids_str = ",".join(f"'{c}'" for c in cids)
        where_cid = f"WHERE m.customer_id::text IN ({cids_str})"

    sql = f"""
    WITH meta AS (
      SELECT customer_id::text AS customer_id, account_name, manager, COALESCE(monthly_budget,0) AS monthly_budget
      FROM dim_account_meta m {where_cid}
    ),
    biz AS (
      SELECT DISTINCT ON (customer_id::text) customer_id::text AS customer_id, bizmoney_balance, dt AS last_update
      FROM fact_bizmoney_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
      ORDER BY customer_id::text, dt DESC
    ),
    camp AS (
      SELECT customer_id::text AS customer_id,
        SUM(cost) FILTER (WHERE dt = :y) AS y_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :a1 AND :a2) AS avg_sum_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :m1 AND :m2) AS month_cost
      FROM fact_campaign_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta) AND dt BETWEEN :min_dt AND :max_dt
      GROUP BY customer_id::text
    )
    SELECT meta.customer_id, meta.account_name, meta.manager, meta.monthly_budget,
      COALESCE(biz.bizmoney_balance,0) AS bizmoney_balance, biz.last_update,
      COALESCE(camp.y_cost,0) AS y_cost, COALESCE(camp.avg_sum_cost,0) AS avg_sum_cost,
      COALESCE(camp.month_cost,0) AS current_month_cost
    FROM meta
    LEFT JOIN biz ON meta.customer_id = biz.customer_id
    LEFT JOIN camp ON meta.customer_id = camp.customer_id
    ORDER BY meta.account_name
    """
    df = sql_read(_engine, sql, params)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        for c in ["monthly_budget", "bizmoney_balance", "y_cost", "avg_sum_cost", "current_month_cost"]:
            df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)
        df["avg_cost"] = df["avg_sum_cost"].astype(float) / float(max(avg_days, 1))
    return df

def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
    if table_exists(engine, "dim_account_meta"):
        sql_exec(engine, "UPDATE dim_account_meta SET monthly_budget = :b, updated_at = now() WHERE customer_id = :cid", 
                 {"b": int(monthly_budget), "cid": int(customer_id)})

def render_budget_month_table_with_bars(table_df: pd.DataFrame, height: int = 520):
    if table_df is None or table_df.empty: return
    df = table_df.copy()
    def _bar(pct, status) -> str:
        pv = float(pct) if pd.notna(pct) else 0.0
        width = max(0.0, min(pv, 120.0))
        stt = str(status or "")
        fill = "var(--nv-red)" if "🔴" in stt else "#F59E0B" if "🟡" in stt else "var(--nv-green)" if "🟢" in stt else "rgba(0,0,0,.25)"
        return f"<div class='nv-pbar'><div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{width:.2f}%;background:{fill};'></div></div><div class='nv-pbar-txt'>{pv:.0f}%</div></div>"

    if "집행률(%)" in df.columns:
        df["집행률"] = [_bar(p, s) for p, s in zip(df["집행률(%)"], df.get("상태", ""))]
        df = df.drop(columns=["집행률(%)"])
        cols = list(df.columns)
        if "상태" in cols and "집행률" in cols:
            cols.remove("집행률")
            cols.insert(cols.index("상태"), "집행률")
            df = df[cols]

    html = df.to_html(index=False, escape=False, classes="nv-table")
    import re
    html = re.sub(r"<td>([\d,]+원)</td>", r"<td class='num'>\1</td>", html)
    html = re.sub(r"<td>([\d,]+)</td>", r"<td class='num'>\1</td>", html)
    st.markdown(f"<div class='nv-table-wrap' style='max-height:{height}px'>{html}</div>", unsafe_allow_html=True)


init_page()
engine = get_engine()
meta = get_meta(engine)
render_sidebar(meta, engine)
f = FilterState.get()

if not f.get("ready"):
    st.info("왼쪽 사이드바에서 검색조건을 설정하세요.")
    st.stop()

st.markdown("## 💰 전체 예산 / 잔액 관리")
cids = tuple(f.get("selected_customer_ids", []))
yesterday = date.today() - timedelta(days=1)
end_dt = f.get("end", yesterday)
avg_d2 = end_dt - timedelta(days=1)
avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
month_d1 = end_dt.replace(day=1)
month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
if bundle.empty:
    st.warning("예산/잔액 데이터를 불러올 수 없습니다.")
    st.stop()

biz_view = bundle.copy()
biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")

m = biz_view["avg_cost"].astype(float) > 0
biz_view["days_cover"] = pd.NA
biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)

biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
biz_view["상태"] = "🟢 여유"
biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)
biz_view["전일 소진액"] = biz_view["y_cost"].map(format_currency)
biz_view["D-소진"] = biz_view["days_cover"].map(lambda d: "-" if pd.isna(d) else "99+일" if float(d) > 99 else f"{float(d):.1f}일")
biz_view["확인일자"] = biz_view["last_update"]

total_balance = int(biz_view["bizmoney_balance"].sum())
total_month_cost = int(biz_view["current_month_cost"].sum())
count_low_balance = int(biz_view["상태"].str.contains("충전필요").sum())

st.markdown("### 🔍 전체 계정 요약")
c1, c2, c3 = st.columns(3)
with c1: ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', 'm_total_balance')
with c2: ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", 'm_month_cost')
with c3: ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', 'm_need_topup')

st.divider()
show_only_topup = st.checkbox("충전필요만 보기", value=False)
biz_view["_rank"] = biz_view["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
if show_only_topup:
    biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)]

view_cols = ["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]
display_df = biz_view[view_cols].rename(columns={"account_name": "업체명", "manager": "담당자"})
ui_table_or_dataframe(display_df, key="budget_biz_table", height=400)

st.divider()
st.markdown(f"### 📅 월 예산 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")

budget_view = bundle[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
budget_view["monthly_budget_val"] = budget_view["monthly_budget"].fillna(0).astype(int)
budget_view["current_month_cost_val"] = budget_view["current_month_cost"].fillna(0).astype(int)

m2 = budget_view["monthly_budget_val"] > 0
budget_view["usage_rate"] = 0.0
budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

def _status(rate, budget):
    if budget == 0: return ("⚪ 미설정", 3)
    if rate >= 1.0: return ("🔴 초과", 0)
    if rate >= 0.9: return ("🟡 주의", 1)
    return ("🟢 적정", 2)

tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
budget_view["상태"] = tmp[0]
budget_view["_rank"] = tmp[1].astype(int)

budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)
budget_view_disp = budget_view.copy()
budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].round(1)

table_df = budget_view_disp[["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]].rename(columns={"account_name": "업체명", "manager": "담당자"})

c1, c2 = st.columns([3, 1])
with c1:
    render_budget_month_table_with_bars(table_df, height=520)
with c2:
    st.markdown("""
        <div class="panel" style="line-height:1.85; font-size:14px; background: rgba(235,238,242,0.75);">
          <b>상태 가이드</b><br><br>
          🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
          🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
          🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
          ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
        </div>
        """, unsafe_allow_html=True)

st.markdown("#### ✍️ 월 예산 수정 (선택 → 입력 → 저장)")
opts = budget_view_disp[["customer_id", "account_name"]].copy()
opts["label"] = opts["account_name"].astype(str) + "  (" + opts["customer_id"].astype(str) + ")"
labels = opts["label"].tolist()
label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

with st.form("budget_update_form", clear_on_submit=False):
    sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
    cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == label_to_cid.get(sel, 0), "monthly_budget_val"].iloc[0]) if labels else 0
    new_budget = st.text_input("새 월 예산(원)", value=format_number_commas(cur_budget) if labels else "0")
    submitted = st.form_submit_button("💾 저장", use_container_width=True)

if submitted and labels:
    cid = int(label_to_cid.get(sel, 0))
    import re
    nb = int(re.sub(r"[^\d]", "", new_budget)) if new_budget else 0
    update_monthly_budget(engine, cid, nb)
    st.success("수정 완료! (캐시가 갱신되었습니다)")
    st.cache_data.clear()
    st.rerun()
