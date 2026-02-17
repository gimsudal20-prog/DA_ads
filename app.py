# -*- coding: utf-8 -*-
"""app.py - 네이버 검색광고 통합 대시보드 (v7.3.0)

✅ 이번 버전 핵심
- NameError(page_budget/page_perf_*) 방지: 전체 함수 포함된 단일 파일
- customer_id 타입 혼재(TEXT vs BIGINT)로 인한 "operator does not exist: text = integer" 해결
  * IN 필터를 항상 문자열 리터럴('420332')로 만들어 비교 (TEXT/BIGINT 모두 안전)
  * 조인에서는 customer_id를 ::text로 통일
- 키워드/소재/캠페인 탭 속도 개선
  * DB에서 기간 집계 → cost 기준 TOP N만 뽑고 → 그 다음 DIM 조인
- 모바일 필터 이슈 해결
  * 사이드바 대신 메인 영역(Expander)에서 필터 노출 + "적용" 버튼으로 재조회 제어

"""

import os
import re
import io
import html
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

from sqlalchemy.engine import Engine as SAEngine

# -----------------------------
# Streamlit cache hashing helpers
# -----------------------------
# SQLAlchemy Engine objects are expensive/unstable to hash on Streamlit Cloud.
# We treat the engine as a constant cache key to ensure cache hits across reruns.
CACHE_HASH_FUNCS = {SAEngine: lambda _: "SQLALCHEMY_ENGINE"}

load_dotenv()

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

BUILD_TAG = "v7.3.1 (TOP5 카드 추가)"

# -----------------------------
# Thresholds (Budget)
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

# -----------------------------
# Global CSS (website mode)
# -----------------------------
GLOBAL_UI_CSS = """
<style>
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }
  div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
  div[data-testid="stDecoration"] { display: none; }
  div[data-testid="stStatusWidget"] { visibility: hidden; height: 0px; }
  thead tr th:first-child { display:none }
  tbody th { display:none }

  .badge { display:inline-block; padding:2px 10px; border-radius:999px; font-size:12px; font-weight:700; margin-right:6px; }
  .b-red { background: rgba(239,68,68,0.12); color: rgb(185,28,28); }
  .b-yellow { background: rgba(234,179,8,0.16); color: rgb(161,98,7); }
  .b-green { background: rgba(34,197,94,0.12); color: rgb(21,128,61); }
  .b-gray { background: rgba(148,163,184,0.18); color: rgb(51,65,85); }
</style>

  /* TOP5 카드 */
  .topcard { padding: 10px 12px; border-radius: 14px;
            background: rgba(15, 23, 42, 0.04);
            border: 1px solid rgba(15, 23, 42, 0.08); }
  .topcard-title { font-size: 13px; font-weight: 800; margin-bottom: 6px; letter-spacing:-0.2px; }
  .topcard-sub { font-size: 11px; color: rgba(49,51,63,0.65); margin-top:-4px; margin-bottom:6px; }
  .topcard-list { margin: 0; padding-left: 18px; font-size: 12px; line-height: 1.45; }
  .topcard-list li { display:flex; justify-content:space-between; gap:10px; margin: 0 0 4px 0; }
  .topcard-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width: 72%; }
  .topcard-val { font-variant-numeric: tabular-nums; white-space:nowrap; }
"""

st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)

# -----------------------------
# Download helpers
# -----------------------------

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
    return output.getvalue()


def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    if df is None or df.empty:
        return

    st.markdown(
        """
        <style>
        div[data-testid="stDownloadButton"] button {
            padding: 0.15rem 0.55rem !important;
            font-size: 0.82rem !important;
            line-height: 1.2 !important;
            min-height: 28px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns([1, 8])
    with c1:
        st.download_button(
            "XLSX",
            data=df_to_xlsx_bytes(df, sheet_name=sheet_name),
            file_name=f"{filename_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            use_container_width=True,
        )
    with c2:
        st.caption("다운로드")


# -----------------------------
# DB helpers
# -----------------------------

def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        try:
            db_url = str(st.secrets.get("DATABASE_URL", "")).strip()
        except Exception:
            db_url = ""

    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. (.env env var or Streamlit secrets)")

    if "sslmode=" not in db_url:
        joiner = "&" if "?" in db_url else "?"
        db_url = db_url + f"{joiner}sslmode=require"

    return db_url


@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(get_database_url(), pool_pre_ping=True, future=True)


def sql_read(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def sql_exec(engine, sql: str, params: Optional[dict] = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})



@st.cache_data(ttl=3600, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _cached_table_names(_engine: SAEngine, schema: str = "public") -> set[str]:
    try:
        return set(inspect(_engine).get_table_names(schema=schema))
    except Exception:
        return set()

def table_exists(engine: SAEngine, table: str, schema: str = "public") -> bool:
    return table in _cached_table_names(engine, schema=schema)

@st.cache_data(ttl=3600, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def get_table_columns(_engine: SAEngine, table: str, schema: str = "public") -> list[str]:
    try:
        return [c["name"] for c in inspect(_engine).get_columns(table, schema=schema)]
    except Exception:
        return []

@st.cache_data(ttl=3600, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def get_column_type(_engine: SAEngine, table: str, column: str, schema: str = "public") -> str:
    """Return Postgres data_type string from information_schema (fallback: empty string)."""
    sql = """
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = %(schema)s AND table_name = %(table)s AND column_name = %(column)s
    LIMIT 1
    """
    try:
        df = sql_read(_engine, sql, {"schema": schema, "table": table, "column": column})
        if not df.empty:
            return str(df.iloc[0]["data_type"])
    except Exception:
        pass
    return ""

def _sql_in_str_list(values: List[int]) -> str:
    """TEXT/BIGINT 혼재를 안전하게 처리하려고, 항상 문자열 리터럴로 IN 리스트를 만듭니다."""
    safe = []
    for v in values:
        try:
            safe.append(f"'{int(v)}'")
        except Exception:
            continue
    return ",".join(safe) if safe else "''"


# -----------------------------
# Formatters
# -----------------------------

def format_currency(val) -> str:
    if pd.isna(val) or val == "":
        return "0원"
    try:
        return f"{int(float(val)):,}원"
    except Exception:
        return "0원"


def format_number_commas(val) -> str:
    if pd.isna(val) or val == "":
        return "0"
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return "0"


def format_roas(val) -> str:
    try:
        if pd.isna(val):
            return "-"
        return f"{float(val):.0f}%"
    except Exception:
        return "-"


def finalize_ctr_col(df: pd.DataFrame, col: str = "CTR(%)") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df
    out = df.copy()
    s = pd.to_numeric(out[col], errors="coerce")

    def _fmt(x):
        if pd.isna(x):
            return ""
        if float(x) == 0.0:
            return "0%"
        return f"{float(x):.1f}%"

    out[col] = s.apply(_fmt)
    return out


def parse_currency(val_str) -> int:
    if pd.isna(val_str):
        return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0



# --------------------
# UI helpers (TOP5 cards)
# --------------------
def _truncate_text(s: str, max_len: int = 34) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _fmt_int(val) -> str:
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return "-"


def _fmt_pct1(val) -> str:
    try:
        return f"{float(val):.1f}%"
    except Exception:
        return "-"


def _fmt_pct0(val) -> str:
    try:
        return f"{float(val):.0f}%"
    except Exception:
        return "-"


def render_top5_cards(df: pd.DataFrame, label_col: str, cards: List[Dict], sub: str = "") -> None:
    """Render 5-item ranked lists in small cards."""
    if df is None or df.empty or label_col not in df.columns:
        return

    cols = st.columns(len(cards))
    for i, spec in enumerate(cards):
        title = spec.get("title", "")
        metric_col = spec.get("metric_col", "")
        sort_dir = (spec.get("sort", "desc") or "desc").lower()
        fmt = spec.get("fmt", lambda x: str(x))
        flt = spec.get("filter", None)

        tmp = df.copy()
        if callable(flt):
            try:
                tmp = tmp[flt(tmp)].copy()
            except Exception:
                pass

        if metric_col not in tmp.columns:
            with cols[i]:
                st.markdown(
                    f"<div class='topcard'><div class='topcard-title'>{html.escape(title)}</div><div class='topcard-sub'>데이터 없음</div></div>",
                    unsafe_allow_html=True,
                )
            continue

        tmp[metric_col] = pd.to_numeric(tmp[metric_col], errors="coerce")
        tmp = tmp.dropna(subset=[metric_col]).copy()
        if tmp.empty:
            with cols[i]:
                st.markdown(
                    f"<div class='topcard'><div class='topcard-title'>{html.escape(title)}</div><div class='topcard-sub'>데이터 없음</div></div>",
                    unsafe_allow_html=True,
                )
            continue

        asc = sort_dir == "asc"
        tmp = tmp.sort_values(metric_col, ascending=asc).head(5)

        items = []
        for _, r in tmp.iterrows():
            name = _truncate_text(r.get(label_col, ""), 36)
            val = fmt(r.get(metric_col))
            items.append(
                f"<li><span class='topcard-name'>{html.escape(str(name))}</span><span class='topcard-val'>{html.escape(str(val))}</span></li>"
            )
        items_html = "\n".join(items)

        sub_html = f"<div class='topcard-sub'>{html.escape(sub)}</div>" if sub else ""
        card_html = f"""
<div class='topcard'>
  <div class='topcard-title'>{html.escape(title)}</div>
  {sub_html}
  <ol class='topcard-list'>
    {items_html}
  </ol>
</div>
"""

        with cols[i]:
            st.markdown(card_html, unsafe_allow_html=True)



# -----------------------------
# Campaign type label
# -----------------------------

_CAMPAIGN_TP_LABEL = {
    "web_site": "파워링크",
    "website": "파워링크",
    "power_link": "파워링크",
    "shopping": "쇼핑검색",
    "shopping_search": "쇼핑검색",
    "power_content": "파워콘텐츠",
    "power_contents": "파워콘텐츠",
    "powercontent": "파워콘텐츠",
    "place": "플레이스",
    "place_search": "플레이스",
    "brand_search": "브랜드검색",
    "brandsearch": "브랜드검색",
}

_LABEL_TO_TP_KEYS: Dict[str, List[str]] = {}
for k, v in _CAMPAIGN_TP_LABEL.items():
    _LABEL_TO_TP_KEYS.setdefault(v, []).append(k)


def campaign_tp_to_label(tp: str) -> str:
    t = (tp or "").strip()
    if not t:
        return ""
    key = t.lower()
    return _CAMPAIGN_TP_LABEL.get(key, t)


def label_to_tp_keys(labels: Tuple[str, ...]) -> List[str]:
    keys: List[str] = []
    for lab in labels:
        keys.extend(_LABEL_TO_TP_KEYS.get(str(lab), []))
    # unique
    out = []
    seen = set()
    for x in keys:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def get_campaign_type_options(dim_campaign: pd.DataFrame) -> List[str]:
    if dim_campaign is None or dim_campaign.empty:
        return []

    raw = dim_campaign.get("campaign_tp", pd.Series([], dtype=str))
    present = set()
    for x in raw.dropna().astype(str).tolist():
        lab = campaign_tp_to_label(x)
        lab = str(lab).strip()
        if lab and lab not in ("미분류", "종합", "기타"):
            present.add(lab)

    order = ["파워링크", "쇼핑검색", "파워콘텐츠", "플레이스", "브랜드검색"]
    opts = [x for x in order if x in present]
    extra = sorted([x for x in present if x not in set(order)])
    return opts + extra


# -----------------------------
# Accounts / Meta sync
# -----------------------------

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))


def normalize_accounts_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    def find_col(cands: List[str]) -> Optional[str]:
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                cc = cand.lower().replace(" ", "").replace("_", "")
                if lc == cc:
                    return c
        for c in df.columns:
            lc = c.lower().replace(" ", "").replace("_", "")
            for cand in cands:
                if cand in lc:
                    return c
        return None

    cid_col = find_col(["customer_id", "customerid", "커스텀id", "커스텀 id", "커스텀ID"])
    name_col = find_col(["account_name", "accountname", "업체명", "업체"])
    mgr_col = find_col(["manager", "담당자", "담당"])

    if not cid_col or not name_col:
        raise ValueError(f"accounts.xlsx is missing columns. Available: {list(df.columns)}")

    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(df[cid_col], errors="coerce").astype("Int64")
    out["account_name"] = df[name_col].astype(str).str.strip()
    out["manager"] = df[mgr_col].astype(str).str.strip() if mgr_col else ""
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    out["manager"] = out["manager"].fillna("").astype(str)
    out = out.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)
    return out


def seed_from_accounts_xlsx(engine) -> Dict[str, int]:
    if not os.path.exists(ACCOUNTS_XLSX):
        return {"meta": 0}

    df = pd.read_excel(ACCOUNTS_XLSX)
    acc = normalize_accounts_columns(df)

    sql_exec(
        engine,
        """CREATE TABLE IF NOT EXISTS dim_account_meta (
          customer_id BIGINT PRIMARY KEY,
          account_name TEXT NOT NULL,
          manager TEXT DEFAULT '',
          monthly_budget BIGINT DEFAULT 0,
          updated_at TIMESTAMPTZ DEFAULT now()
        );""",
    )

    upsert_meta = """
    INSERT INTO dim_account_meta (customer_id, account_name, manager, updated_at)
    VALUES (:customer_id, :account_name, :manager, now())
    ON CONFLICT (customer_id) DO UPDATE SET
      account_name = EXCLUDED.account_name,
      manager = EXCLUDED.manager,
      updated_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_meta), acc.to_dict(orient="records"))

    return {"meta": int(len(acc))}


@st.cache_data(ttl=600, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_account_meta"):
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df = sql_read(
        _engine,
        """
        SELECT customer_id, account_name, manager, monthly_budget, updated_at
        FROM dim_account_meta
        ORDER BY account_name
        """,
    )

    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
        df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    return df


def update_monthly_budget(engine, customer_id: int, monthly_budget: int) -> None:
    if not table_exists(engine, "dim_account_meta"):
        return
    sql_exec(
        engine,
        """
        UPDATE dim_account_meta
        SET monthly_budget = :b, updated_at = now()
        WHERE customer_id = :cid
        """,
        {"b": int(monthly_budget), "cid": int(customer_id)},
    )


# -----------------------------
# DIM loaders
# -----------------------------

@st.cache_data(ttl=3600, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])

    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])

    df["campaign_tp"] = df.get("campaign_tp", "").fillna("")
    df["campaign_type_label"] = df["campaign_tp"].astype(str).apply(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"

    return df


# -----------------------------
# Data freshness
# -----------------------------

def render_data_freshness(engine) -> None:
    tables = ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily"]
    latest = {}

    for t in tables:
        if not table_exists(engine, t):
            continue
        try:
            df = sql_read(engine, f"SELECT MAX(dt) AS mx FROM {t}")
            mx = df["mx"].iloc[0] if df is not None and not df.empty else None
            latest[t] = str(mx)[:10] if mx is not None else "-"
        except Exception:
            latest[t] = "-"

    if not latest:
        return

    chips = []
    label_map = {
        "fact_campaign_daily": "캠페인",
        "fact_keyword_daily": "키워드",
        "fact_ad_daily": "소재",
        "fact_bizmoney_daily": "비즈머니",
    }
    for k, v in latest.items():
        chips.append(f"<span class='badge b-gray'>{label_map.get(k,k)} 최신: {v}</span>")

    st.markdown("".join(chips), unsafe_allow_html=True)


# -----------------------------
# Filters (main area)
# -----------------------------

def build_filters(engine: SAEngine, meta: pd.DataFrame, type_opts: List[str]) -> Dict:
    did_apply = False
    today = date.today()
    default_end = today - timedelta(days=1)  # 기본: 어제
    default_start = default_end

    defaults = {
        "q": "",
        "manager": [],
        "account": [],
        "type_sel": tuple(),
        "period_mode": "어제",
        "d1": default_start,
        "d2": default_end,
        "top_n_keyword": 100,
        "top_n_ad": 100,
        "top_n_campaign": 100,
    }

    if "filters_applied" not in st.session_state:
        st.session_state["filters_applied"] = defaults.copy()

    with st.expander("필터", expanded=True):
        c1, c2, c3 = st.columns([2, 2, 2])

        with c1:
            q = st.text_input("업체명 검색", value=st.session_state["filters_applied"].get("q", ""), placeholder="예: 실리콘플러스")
            manager_opts = sorted([x for x in meta.get("manager", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()])
            manager_sel = st.multiselect("담당자", manager_opts, default=st.session_state["filters_applied"].get("manager", []))

        with c2:
            account_opts_all = sorted([x for x in meta.get("account_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()])
            account_sel = st.multiselect("업체", account_opts_all, default=st.session_state["filters_applied"].get("account", []))

            type_sel = tuple(
                st.multiselect(
                    "캠페인 유형",
                    type_opts or [],
                    default=list(st.session_state["filters_applied"].get("type_sel", tuple())),
                )
            )

        with c3:
            period_mode = st.selectbox(
                "기간",
                ["어제", "최근 3일", "최근 7일", "직접 선택"],
                index=["어제", "최근 3일", "최근 7일", "직접 선택"].index(st.session_state["filters_applied"].get("period_mode", "어제")),
            )

            if period_mode == "최근 3일":
                d2 = default_end
                d1 = d2 - timedelta(days=2)
            elif period_mode == "최근 7일":
                d2 = default_end
                d1 = d2 - timedelta(days=6)
            elif period_mode == "직접 선택":
                d1d2 = st.date_input(
                    "기간 선택",
                    value=(
                        st.session_state["filters_applied"].get("d1", default_start),
                        st.session_state["filters_applied"].get("d2", default_end),
                    ),
                )
                if isinstance(d1d2, (list, tuple)) and len(d1d2) == 2:
                    d1, d2 = d1d2[0], d1d2[1]
                else:
                    d1, d2 = default_start, default_end
            else:
                d1, d2 = default_start, default_end

            top_n_keyword = st.slider("키워드 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_keyword", 100)), step=10)
            top_n_ad = st.slider("소재 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_ad", 100)), step=10)
            top_n_campaign = st.slider("캠페인 TOP N", 20, 500, int(st.session_state["filters_applied"].get("top_n_campaign", 100)), step=10)

        apply_btn = st.button("적용", use_container_width=True)

    if apply_btn:
        did_apply = True
        st.session_state["filters_applied"] = {
            "q": q,
            "manager": manager_sel,
            "account": account_sel,
            "type_sel": type_sel,
            "period_mode": period_mode,
            "d1": d1,
            "d2": d2,
            "top_n_keyword": top_n_keyword,
            "top_n_ad": top_n_ad,
            "top_n_campaign": top_n_campaign,
        }

    f = dict(st.session_state.get("filters_applied", defaults))
    f["start"] = f.get("d1", default_start)
    f["end"] = f.get("d2", default_end)

    # selected_customer_ids: 비어있으면 전체(쿼리 필터 생략)
    df = meta.copy()
    if f.get("manager"):
        df = df[df["manager"].isin(f["manager"])]
    if f.get("account"):
        df = df[df["account_name"].isin(f["account"])]
    if f.get("q"):
        q_ = str(f["q"]).strip()
        if q_:
            df = df[df["account_name"].astype(str).str.contains(q_, case=False, na=False)]

    f["selected_customer_ids"] = df["customer_id"].dropna().astype(int).tolist() if len(df) < len(meta) else []

    # 캐시 워밍업: 필터 적용 직후 자주 쓰는 쿼리를 한 번 돌려서
    # 페이지 이동 시(메인→키워드→소재 등) 체감 속도를 1초 안쪽으로 끌어옵니다.
    if did_apply:
        try:
            with st.spinner("캐시 준비 중... (한 번만)"):
                warm_cache(engine, f)
        except Exception:
            pass

    return f


# -----------------------------
# Budget queries
# -----------------------------

@st.cache_data(ttl=180, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_latest_bizmoney(_engine, cids: Tuple[int, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_bizmoney_daily"):
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])

    where = ""
    if cids:
        where = f"WHERE customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    SELECT DISTINCT ON (customer_id::text)
      customer_id::text AS customer_id,
      bizmoney_balance,
      dt AS last_update
    FROM fact_bizmoney_daily
    {where}
    ORDER BY customer_id::text, dt DESC
    """

    df = sql_read(_engine, sql)
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "bizmoney_balance", "last_update"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["bizmoney_balance"] = pd.to_numeric(df.get("bizmoney_balance", 0), errors="coerce").fillna(0).astype("int64")
    return df


@st.cache_data(ttl=180, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_yesterday_cost(_engine, yesterday: date, cids: Tuple[int, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "y_cost"])

    where_cid = ""
    if cids:
        where_cid = f"AND customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    SELECT customer_id::text AS customer_id, SUM(cost) AS y_cost
    FROM fact_campaign_daily
    WHERE dt = :d
    {where_cid}
    GROUP BY customer_id::text
    """

    df = sql_read(_engine, sql, {"d": str(yesterday)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "y_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["y_cost"] = pd.to_numeric(df.get("y_cost", 0), errors="coerce").fillna(0).astype("int64")
    return df


@st.cache_data(ttl=180, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_recent_avg_cost(_engine, d1: date, d2: date, cids: Tuple[int, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    if d2 < d1:
        d1 = d2

    where_cid = ""
    if cids:
        where_cid = f"AND customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    SELECT customer_id::text AS customer_id, SUM(cost) AS sum_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    {where_cid}
    GROUP BY customer_id::text
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["sum_cost"] = pd.to_numeric(df.get("sum_cost", 0), errors="coerce").fillna(0)

    days = max((d2 - d1).days + 1, 1)
    df["avg_cost"] = df["sum_cost"].astype(float) / float(days)
    return df[["customer_id", "avg_cost"]]


@st.cache_data(ttl=180, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_monthly_cost(_engine, target_date: date, cids: Tuple[int, ...]) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    start_dt = target_date.replace(day=1)
    if target_date.month == 12:
        end_dt = date(target_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)

    where_cid = ""
    if cids:
        where_cid = f"AND customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    SELECT customer_id::text AS customer_id, SUM(cost) AS current_month_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    {where_cid}
    GROUP BY customer_id::text
    """

    df = sql_read(_engine, sql, {"d1": str(start_dt), "d2": str(end_dt)})
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "current_month_cost"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["current_month_cost"] = pd.to_numeric(df.get("current_month_cost", 0), errors="coerce").fillna(0).astype("int64")
    return df


# -----------------------------
# Perf queries (TOP N)
# -----------------------------



def warm_cache(engine: SAEngine, f: Dict) -> None:
    """Warm common caches so page transitions feel instant."""
    try:
        _ = load_dim_campaign(engine)
    except Exception:
        pass

    cids = tuple(f.get("selected_customer_ids") or [])
    d1 = f.get("start")
    d2 = f.get("end")
    if d1 is None or d2 is None:
        return

    # Budget page
    try:
        _ = query_latest_bizmoney(engine, cids)
        _ = query_yesterday_cost(engine, str(d2 - timedelta(days=1)), cids)
        d2_avg = d2 - timedelta(days=1)
        d1_avg = max(d1, d2_avg - timedelta(days=2))
        _ = query_recent_avg_cost(engine, str(d1_avg), str(d2_avg), cids)
        month_start = d1.replace(day=1)
        month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        _ = query_monthly_cost(engine, str(month_start), str(month_end), cids)
    except Exception:
        pass

    # Perf pages (TopN)
    type_sel = f.get("type_sel") or "전체"
    topn_kw = int(f.get("top_n_keyword") or 300)
    topn_ad = int(f.get("top_n_ad") or 300)
    topn_cp = int(f.get("top_n_campaign") or 300)
    try:
        _ = query_campaign_topn(engine, str(d1), str(d2), cids, type_sel, topn_cp)
    except Exception:
        pass
    try:
        _ = query_keyword_bundle(engine, str(d1), str(d2), cids, type_sel, topn_kw)
    except Exception:
        pass
    try:
        _ = query_ad_topn(engine, str(d1), str(d2), cids, type_sel, topn_ad)
    except Exception:
        pass

def _fact_has_sales(_engine, fact_table: str) -> bool:
    return "sales" in get_table_columns(_engine, fact_table)


@st.cache_data(ttl=300, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_campaign_topn(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], top_n: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    where_type = ""
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        where_type = f"AND LOWER(COALESCE(c.campaign_tp,'')) IN ({tp_list})"

    sql = f"""
    WITH agg AS (
      SELECT
        f.customer_id::text AS customer_id,
        f.campaign_id,
        SUM(f.imp) AS imp,
        SUM(f.clk) AS clk,
        SUM(f.cost) AS cost,
        SUM(f.conv) AS conv,
        {sales_expr} AS sales
      FROM fact_campaign_daily f
      LEFT JOIN dim_campaign c
        ON f.customer_id::text = c.customer_id::text
       AND f.campaign_id = c.campaign_id
      WHERE f.dt BETWEEN :d1 AND :d2
      {where_cid}
      {where_type}
      GROUP BY f.customer_id::text, f.campaign_id
    )
    SELECT
      a.*,
      COALESCE(NULLIF(c.campaign_name,''), '') AS campaign_name,
      COALESCE(NULLIF(c.campaign_tp,''), '') AS campaign_tp
    FROM (
      SELECT * FROM agg ORDER BY cost DESC LIMIT :lim
    ) a
    LEFT JOIN dim_campaign c
      ON a.customer_id = c.customer_id::text
     AND a.campaign_id = c.campaign_id
    ORDER BY a.cost DESC
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).apply(campaign_tp_to_label)
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]
    return df.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_keyword_topn(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], top_n: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_keyword") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    where_type = ""
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        where_type = f"AND LOWER(COALESCE(c.campaign_tp,'')) IN ({tp_list})"

    sql = f"""
    WITH agg AS (
      SELECT
        f.customer_id::text AS customer_id,
        f.keyword_id,
        SUM(f.imp) AS imp,
        SUM(f.clk) AS clk,
        SUM(f.cost) AS cost,
        SUM(f.conv) AS conv,
        {sales_expr} AS sales
      FROM fact_keyword_daily f
      LEFT JOIN dim_keyword k
        ON f.customer_id::text = k.customer_id::text
       AND f.keyword_id = k.keyword_id
      LEFT JOIN dim_adgroup g
        ON k.customer_id::text = g.customer_id::text
       AND k.adgroup_id = g.adgroup_id
      LEFT JOIN dim_campaign c
        ON g.customer_id::text = c.customer_id::text
       AND g.campaign_id = c.campaign_id
      WHERE f.dt BETWEEN :d1 AND :d2
      {where_cid}
      {where_type}
      GROUP BY f.customer_id::text, f.keyword_id
    )
    SELECT
      a.*,
      COALESCE(NULLIF(k.keyword,''), '') AS keyword,
      COALESCE(NULLIF(g.adgroup_name,''), '') AS adgroup_name,
      COALESCE(NULLIF(c.campaign_name,''), '') AS campaign_name,
      COALESCE(NULLIF(c.campaign_tp,''), '') AS campaign_tp
    FROM (
      SELECT * FROM agg ORDER BY cost DESC LIMIT :lim
    ) a
    LEFT JOIN dim_keyword k
      ON a.customer_id = k.customer_id::text
     AND a.keyword_id = k.keyword_id
    LEFT JOIN dim_adgroup g
      ON k.customer_id::text = g.customer_id::text
     AND k.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id::text = c.customer_id::text
     AND g.campaign_id = c.campaign_id
    ORDER BY a.cost DESC
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).apply(campaign_tp_to_label)
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]
    return df.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_ad_topn(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...], top_n: int) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    where_type = ""
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        where_type = f"AND LOWER(COALESCE(c.campaign_tp,'')) IN ({tp_list})"

    cols = get_table_columns(_engine, "dim_ad")
    ad_text_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"

    sql = f"""
    WITH agg AS (
      SELECT
        f.customer_id::text AS customer_id,
        f.ad_id,
        SUM(f.imp) AS imp,
        SUM(f.clk) AS clk,
        SUM(f.cost) AS cost,
        SUM(f.conv) AS conv,
        {sales_expr} AS sales
      FROM fact_ad_daily f
      LEFT JOIN dim_ad a
        ON f.customer_id::text = a.customer_id::text
       AND f.ad_id = a.ad_id
      LEFT JOIN dim_adgroup g
        ON a.customer_id::text = g.customer_id::text
       AND a.adgroup_id = g.adgroup_id
      LEFT JOIN dim_campaign c
        ON g.customer_id::text = c.customer_id::text
       AND g.campaign_id = c.campaign_id
      WHERE f.dt BETWEEN :d1 AND :d2
      {where_cid}
      {where_type}
      GROUP BY f.customer_id::text, f.ad_id
    )
    SELECT
      a2.*,
      {ad_text_expr} AS ad_name,
      COALESCE(NULLIF(g.adgroup_name,''), '') AS adgroup_name,
      COALESCE(NULLIF(c.campaign_name,''), '') AS campaign_name,
      COALESCE(NULLIF(c.campaign_tp,''), '') AS campaign_tp
    FROM (
      SELECT * FROM agg ORDER BY cost DESC LIMIT :lim
    ) a2
    LEFT JOIN dim_ad a
      ON a2.customer_id = a.customer_id::text
     AND a2.ad_id = a.ad_id
    LEFT JOIN dim_adgroup g
      ON a.customer_id::text = g.customer_id::text
     AND a.adgroup_id = g.adgroup_id
    LEFT JOIN dim_campaign c
      ON g.customer_id::text = c.customer_id::text
     AND g.campaign_id = c.campaign_id
    ORDER BY a2.cost DESC
    """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).apply(campaign_tp_to_label)
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]
    return df.reset_index(drop=True)


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


# -----------------------------
# Pages
# -----------------------------

def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")

    cids = tuple(f.get("selected_customer_ids", []) or [])

    biz = query_latest_bizmoney(engine, cids)
    yesterday = date.today() - timedelta(days=1)
    y_cost_df = query_yesterday_cost(engine, yesterday, cids)

    avg_df = pd.DataFrame(columns=["customer_id", "avg_cost"])
    if TOPUP_AVG_DAYS > 0:
        d2 = (f.get("end") or (date.today() - timedelta(days=1))) - timedelta(days=1)
        d1 = d2 - timedelta(days=TOPUP_AVG_DAYS - 1)
        avg_df = query_recent_avg_cost(engine, d1, d2, cids)

    month_cost_df = query_monthly_cost(engine, f.get("end") or (date.today() - timedelta(days=1)), cids)

    base = meta[["customer_id", "account_name", "manager", "monthly_budget"]].copy()
    if cids:
        base = base[base["customer_id"].isin(list(cids))].copy()

    biz_view = base[["customer_id", "account_name", "manager"]].merge(biz, on="customer_id", how="left")
    biz_view["bizmoney_balance"] = pd.to_numeric(biz_view.get("bizmoney_balance", 0), errors="coerce").fillna(0).astype("int64")
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")

    biz_view = biz_view.merge(y_cost_df, on="customer_id", how="left")
    biz_view["y_cost"] = pd.to_numeric(biz_view.get("y_cost", 0), errors="coerce").fillna(0).astype("int64")

    biz_view = biz_view.merge(avg_df, on="customer_id", how="left")
    biz_view["avg_cost"] = pd.to_numeric(biz_view.get("avg_cost", 0), errors="coerce").fillna(0.0).astype(float)

    biz_view["days_cover"] = pd.NA
    mask = biz_view["avg_cost"] > 0
    biz_view.loc[mask, "days_cover"] = biz_view.loc[mask, "bizmoney_balance"].astype(float) / biz_view.loc[mask, "avg_cost"].astype(float)

    biz_view["threshold"] = (biz_view["avg_cost"] * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].apply(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    biz_view["bizmoney_fmt"] = biz_view["bizmoney_balance"].apply(format_currency)
    biz_view["y_cost_fmt"] = biz_view["y_cost"].apply(format_currency)
    biz_view["avg_cost_fmt"] = biz_view["avg_cost"].apply(format_currency)

    def _fmt_days(d):
        if pd.isna(d) or d is None:
            return "-"
        try:
            dd = float(d)
        except Exception:
            return "-"
        if dd > 99:
            return "99+일"
        return f"{dd:.1f}일"

    biz_view["days_cover_fmt"] = biz_view["days_cover"].apply(_fmt_days)

    total_balance = int(biz_view["bizmoney_balance"].sum())
    total_month_cost = int(month_cost_df.get("current_month_cost", pd.Series([0])).sum()) if month_cost_df is not None else 0
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    c1.metric("총 비즈머니 잔액", format_currency(total_balance))
    c2.metric(f"{(f.get('end') or yesterday).month}월 총 사용액", format_currency(total_month_cost))
    c3.metric("충전 필요 계정", f"{count_low_balance}건", delta_color="inverse")

    st.divider()

    need_topup = count_low_balance
    ok_topup = int(len(biz_view) - need_topup)
    st.markdown(
        f"<span class='badge b-red'>충전필요 {need_topup}건</span>"
        f"<span class='badge b-green'>여유 {ok_topup}건</span>",
        unsafe_allow_html=True,
    )

    show_only_topup = st.checkbox("충전필요만 보기", value=False)

    biz_view["_rank"] = biz_view["상태"].apply(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    st.dataframe(
        biz_view[["account_name", "manager", "bizmoney_fmt", "avg_cost_fmt", "days_cover_fmt", "y_cost_fmt", "상태", "last_update"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "account_name": "업체명",
            "manager": "담당자",
            "bizmoney_fmt": st.column_config.TextColumn("비즈머니 잔액"),
            "avg_cost_fmt": st.column_config.TextColumn(f"최근{TOPUP_AVG_DAYS}일 평균소진"),
            "days_cover_fmt": st.column_config.TextColumn("D-소진"),
            "y_cost_fmt": st.column_config.TextColumn("전일 소진액"),
            "상태": st.column_config.TextColumn("상태"),
            "last_update": st.column_config.TextColumn("확인일자"),
        },
    )

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({(f.get('end') or yesterday).strftime('%Y년 %m월')} 기준)")

    budget_view = base[["customer_id", "account_name", "manager", "monthly_budget"]].merge(month_cost_df, on="customer_id", how="left")
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

    budget_view["usage_rate"] = 0.0
    m = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m, "usage_rate"] = budget_view.loc[m, "current_month_cost_val"] / budget_view.loc[m, "monthly_budget_val"]
    budget_view["usage_pct"] = (budget_view["usage_rate"] * 100.0).fillna(0.0)

    def _status(rate: float, budget: int):
        if budget == 0:
            return ("⚪ 미설정", "미설정", 3)
        if rate >= 1.0:
            return ("🔴 초과", "초과", 0)
        if rate >= 0.9:
            return ("🟡 주의", "주의", 1)
        return ("🟢 적정", "적정", 2)

    tmp = budget_view.apply(lambda r: _status(float(r["usage_rate"]), int(r["monthly_budget_val"])), axis=1, result_type="expand")
    budget_view["status_icon"] = tmp[0]
    budget_view["status_text"] = tmp[1]
    budget_view["_rank"] = tmp[2].astype(int)

    cnt_over = int((budget_view["status_text"] == "초과").sum())
    cnt_warn = int((budget_view["status_text"] == "주의").sum())
    cnt_unset = int((budget_view["status_text"] == "미설정").sum())

    st.markdown(
        f"<span class='badge b-red'>초과 {cnt_over}건</span>"
        f"<span class='badge b-yellow'>주의 {cnt_warn}건</span>"
        f"<span class='badge b-gray'>미설정 {cnt_unset}건</span>",
        unsafe_allow_html=True,
    )

    budget_view = budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)

    budget_view["monthly_budget_edit"] = budget_view["monthly_budget_val"].apply(format_number_commas)
    budget_view["current_month_cost_disp"] = budget_view["current_month_cost_val"].apply(format_number_commas)

    c1, c2 = st.columns([3, 1])
    with c1:
        edited = st.data_editor(
            budget_view[["customer_id", "account_name", "manager", "monthly_budget_edit", "current_month_cost_disp", "usage_pct", "status_icon"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "customer_id": st.column_config.NumberColumn("CID", disabled=True),
                "account_name": st.column_config.TextColumn("업체명", disabled=True),
                "manager": st.column_config.TextColumn("담당자", disabled=True),
                "monthly_budget_edit": st.column_config.TextColumn("월 예산 (원)", help="예: 500,000", max_chars=20),
                "current_month_cost_disp": st.column_config.TextColumn(f"{(f.get('end') or yesterday).month}월 사용액", disabled=True),
                "usage_pct": st.column_config.NumberColumn("집행률(%)", format="%.1f", disabled=True),
                "status_icon": st.column_config.TextColumn("상태", disabled=True),
            },
            key="budget_editor_v7_2_4",
        )

    with c2:
        st.markdown(
            """
            <div style="padding:12px 14px; border-radius:12px; background-color:rgba(2,132,199,0.06); line-height:1.85; font-size:14px;">
              <b>상태 가이드</b><br><br>
              🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
              🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
              🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
              ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if st.button("💾 예산 저장 및 업데이트", type="primary", use_container_width=True):
            orig_budget = budget_view.set_index("customer_id")["monthly_budget_val"].to_dict()
            changed = 0
            for _, r in edited.iterrows():
                cid = int(r.get("customer_id", 0))
                if cid == 0:
                    continue
                new_val = parse_currency(r.get("monthly_budget_edit", "0"))
                if new_val != int(orig_budget.get(cid, 0)):
                    update_monthly_budget(engine, cid, new_val)
                    changed += 1

            if changed:
                st.success(f"{changed}건 수정 완료.")
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("변경 없음.")


def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")
    return out


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 🚀 성과 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_campaign", 100))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    df = query_campaign_topn(engine, f["start"], f["end"], cids, type_sel, top_n)
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    df = _perf_common_merge_meta(df, meta)
    df = add_rates(df)

    # 🏅 성과 TOP5 (현재 로딩된 TopN 기준)
    df_top = df.copy()
    df_top["_label"] = df_top.get("account_name", "").astype(str).str.strip() + " · " + df_top.get("campaign_name", "").astype(str).str.strip()
    render_top5_cards(
        df_top,
        label_col="_label",
        sub="현재 화면 TopN 기준",
        cards=[
            {"title": "광고비 TOP5", "metric_col": "cost", "sort": "desc", "fmt": format_currency},
            {"title": "전환 TOP5", "metric_col": "conv", "sort": "desc", "fmt": _fmt_int},
            {"title": "ROAS TOP5", "metric_col": "roas", "sort": "desc", "fmt": _fmt_pct0,
             "filter": lambda t: pd.to_numeric(t.get("cost"), errors="coerce").fillna(0) > 0},
            {"title": "CPA 최저 TOP5", "metric_col": "cpa", "sort": "asc", "fmt": format_currency,
             "filter": lambda t: pd.to_numeric(t.get("conv"), errors="coerce").fillna(0) > 0},
        ],
    )
    st.divider()

    disp = df.copy()
    disp["cost"] = disp["cost"].apply(format_currency)
    disp["sales"] = disp["sales"].apply(format_currency)
    disp["cpc"] = disp["cpc"].apply(format_currency)
    disp["cpa"] = disp["cpa"].apply(format_currency)
    disp["roas_disp"] = disp["roas"].apply(format_roas)

    disp = disp.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_type": "광고유형",
            "campaign_name": "캠페인",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "sales": "전환매출",
            "roas_disp": "ROAS(%)",
        }
    )

    disp["노출"] = pd.to_numeric(disp["노출"], errors="coerce").fillna(0).astype(int)
    disp["클릭"] = pd.to_numeric(disp["클릭"], errors="coerce").fillna(0).astype(int)
    disp["전환"] = pd.to_numeric(disp["전환"], errors="coerce").fillna(0).astype(int)
    disp["CTR(%)"] = disp["CTR(%)"].astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "담당자", "광고유형", "캠페인", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = disp[cols].copy()

    st.dataframe(view_df, use_container_width=True, hide_index=True)
    render_download_compact(view_df, f"성과_캠페인_TOP{top_n}_{f['start']}_{f['end']}", "campaign", "camp")




@st.cache_data(ttl=300, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def query_keyword_bundle(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 300,
) -> pd.DataFrame:
    """
    ✅ 한 번의 스캔/집계로 아래를 같이 가져옵니다.
    - 광고비 기준 Top N (rn_cost <= topn_cost)
    - 클릭 Top10 (rn_clk <= 10)
    - 전환 Top10 (rn_conv <= 10)

    → 키워드 탭에서 '성과별 TOP10'이 사라졌던 걸 복원하면서도,
      쿼리를 3번/4번 돌리지 않아서 속도 저하를 막습니다.
    """
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0)) AS sales" if "sales" in fk_cols else "0::bigint AS sales"

    # dim_keyword 키워드 컬럼명 호환
    kw_cols = get_table_columns(_engine, "dim_keyword")
    if "keyword" in kw_cols:
        kw_expr = "k.keyword"
    elif "keyword_name" in kw_cols:
        kw_expr = "k.keyword_name"
    else:
        kw_expr = "''::text"

    # IN 절: customer_id가 TEXT/BIGINT 무엇이든 안전하도록 문자열 리터럴로 넣기
    cids = [str(int(x)) for x in (customer_ids or tuple())]
    in_clause = ""
    if cids:
        quoted = ",".join([f"'{c}'" for c in cids])
        in_clause = f" AND fk.customer_id::text IN ({quoted}) "

    type_clause = ""
    if type_sel:
        tquoted = ",".join(["'" + str(t).replace("'", "''") + "'" for t in type_sel])
        type_clause = f" AND campaign_type_label IN ({tquoted}) "

    sql = f"""
    WITH base AS (
        SELECT
            fk.customer_id::text AS customer_id,
            fk.keyword_id::text AS keyword_id,
            SUM(fk.imp) AS imp,
            SUM(fk.clk) AS clk,
            SUM(fk.cost) AS cost,
            SUM(fk.conv) AS conv,
            {sales_expr}
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2
        {in_clause}
        GROUP BY fk.customer_id::text, fk.keyword_id::text
    ),
    joined AS (
        SELECT
            b.*,
            COALESCE(NULLIF(TRIM({kw_expr}),''),'') AS keyword,
            COALESCE(NULLIF(TRIM(g.adgroup_name),''),'') AS adgroup_name,
            COALESCE(NULLIF(TRIM(c.campaign_name),''),'') AS campaign_name,
            CASE
                WHEN lower(trim(c.campaign_tp)) IN ('web_site','website','power_link','powerlink') THEN '파워링크'
                WHEN lower(trim(c.campaign_tp)) IN ('shopping','shopping_search') THEN '쇼핑검색'
                WHEN lower(trim(c.campaign_tp)) IN ('power_content','power_contents','powercontent') THEN '파워콘텐츠'
                WHEN lower(trim(c.campaign_tp)) IN ('place','place_search') THEN '플레이스'
                WHEN lower(trim(c.campaign_tp)) IN ('brand_search','brandsearch') THEN '브랜드검색'
                ELSE COALESCE(NULLIF(trim(c.campaign_tp),''),'기타')
            END AS campaign_type_label
        FROM base b
        LEFT JOIN dim_keyword k
            ON b.customer_id = k.customer_id::text AND b.keyword_id = k.keyword_id::text
        LEFT JOIN dim_adgroup g
            ON k.customer_id::text = g.customer_id::text AND k.adgroup_id::text = g.adgroup_id::text
        LEFT JOIN dim_campaign c
            ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text
        WHERE 1=1
            AND COALESCE(NULLIF(trim(c.campaign_tp),''),'기타') <> 'etc'
            AND COALESCE(NULLIF(trim(c.campaign_tp),''),'기타') <> '기타'
            {type_clause}
    ),
    ranked AS (
        SELECT
            j.*,
            ROW_NUMBER() OVER (ORDER BY j.cost DESC NULLS LAST) AS rn_cost,
            ROW_NUMBER() OVER (ORDER BY j.clk DESC NULLS LAST) AS rn_clk,
            ROW_NUMBER() OVER (ORDER BY j.conv DESC NULLS LAST) AS rn_conv
        FROM joined j
    )
    SELECT *
    FROM ranked
    WHERE rn_cost <= :topn_cost OR rn_clk <= 10 OR rn_conv <= 10
    ORDER BY rn_cost ASC
    """

    params = {"d1": str(d1), "d2": str(d2), "topn_cost": int(topn_cost)}
    return sql_read(_engine, sql, params)


def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    st.markdown("## 키워드 성과")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    # 필터 적용된 고객 리스트(없으면 전체)
    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", []) or [])

    # Top N 설정
    top_n = int(st.number_input("Top N", min_value=50, max_value=3000, value=300, step=50))

    # ✅ 한 번의 쿼리로: TopN(광고비) + 클릭TOP10 + 전환TOP10
    bundle = query_keyword_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return

    # TOP10 분리
    top_cost = bundle[bundle["rn_cost"] <= 10].sort_values("rn_cost")
    top_clk = bundle[bundle["rn_clk"] <= 10].sort_values("rn_clk")
    top_conv = bundle[bundle["rn_conv"] <= 10].sort_values("rn_conv")

    def _fmt_top(df: pd.DataFrame, metric: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["업체명", "키워드", metric])
        x = df.copy()
        x["customer_id"] = pd.to_numeric(x["customer_id"], errors="coerce").astype("Int64")
        x = x.dropna(subset=["customer_id"]).copy()
        x["customer_id"] = x["customer_id"].astype("int64")
        x = x.merge(meta[["customer_id", "account_name"]], on="customer_id", how="left")
        if metric == "광고비":
            x[metric] = x["cost"].apply(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else:
            x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        return x.rename(columns={"account_name": "업체명", "keyword": "키워드"})[["업체명", "키워드", metric]]

    with st.expander("📌 성과별 TOP10 키워드", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP10")
            st.dataframe(_fmt_top(top_cost, "광고비"), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP10")
            st.dataframe(_fmt_top(top_clk, "클릭"), use_container_width=True, hide_index=True)
        with c3:
            st.markdown("#### ✅ 전환 TOP10")
            st.dataframe(_fmt_top(top_conv, "전환"), use_container_width=True, hide_index=True)

    st.divider()

    # Top N 테이블(광고비 기준)
    df = bundle[bundle["rn_cost"] <= top_n].sort_values("rn_cost").copy()

    # 표시용 후처리
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df = add_rates(df)
    df = df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")

    view = df.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_type_label": "캠페인유형",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "keyword": "키워드",
            "imp": "노출",
            "clk": "클릭",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cost": "비용",
            "conv": "전환",
            "cpa": "CPA",
            "sales": "매출",
            "roas": "ROAS(%)",
        }
    )

    view["비용"] = view["비용"].apply(format_currency)
    view["CPC"] = view["CPC"].apply(format_currency)
    view["CPA"] = view["CPA"].apply(format_currency)
    view["매출"] = pd.to_numeric(view.get("매출", 0), errors="coerce").fillna(0).apply(format_currency)
    view["ROAS(%)"] = view["ROAS(%)"].apply(format_roas)
    view = finalize_ctr_col(view, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드", "노출", "클릭", "CTR(%)", "CPC", "비용", "전환", "CPA", "매출", "ROAS(%)"]
    st.dataframe(view[cols], use_container_width=True, hide_index=True)
    render_download_compact(view[cols], f"키워드성과_{f['start']}_{f['end']}", "keyword", "kw")
def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 🧩 성과 (소재)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_ad", 100))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    df = query_ad_topn(engine, f["start"], f["end"], cids, type_sel, top_n)
    if df is None or df.empty:
        st.warning("데이터 없음 (dim_ad/dim_adgroup/dim_campaign 또는 fact_ad_daily 확인)")
        return

    df = _perf_common_merge_meta(df, meta)
    df = add_rates(df)

    # 🏅 성과 TOP5 (현재 로딩된 TopN 기준)
    df_top = df.copy()
    df_top["_label"] = df_top.get("account_name", "").astype(str).str.strip() + " · " + df_top.get("ad_name", "").astype(str).apply(lambda x: _truncate_text(x, 28))
    render_top5_cards(
        df_top,
        label_col="_label",
        sub="현재 화면 TopN 기준",
        cards=[
            {"title": "광고비 TOP5", "metric_col": "cost", "sort": "desc", "fmt": format_currency},
            {"title": "전환 TOP5", "metric_col": "conv", "sort": "desc", "fmt": _fmt_int},
            {"title": "CTR TOP5", "metric_col": "ctr", "sort": "desc", "fmt": _fmt_pct1},
            {"title": "ROAS TOP5", "metric_col": "roas", "sort": "desc", "fmt": _fmt_pct0,
             "filter": lambda t: pd.to_numeric(t.get("cost"), errors="coerce").fillna(0) > 0},
        ],
    )
    st.divider()

    disp = df.copy()
    disp["cost"] = disp["cost"].apply(format_currency)
    disp["sales"] = disp["sales"].apply(format_currency)
    disp["cpc"] = disp["cpc"].apply(format_currency)
    disp["cpa"] = disp["cpa"].apply(format_currency)
    disp["roas_disp"] = disp["roas"].apply(format_roas)

    disp = disp.rename(
        columns={
            "account_name": "업체명",
            "manager": "담당자",
            "campaign_name": "캠페인",
            "adgroup_name": "광고그룹",
            "ad_id": "소재ID",
            "ad_name": "소재내용",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "ctr": "CTR(%)",
            "cpc": "CPC",
            "cpa": "CPA",
            "sales": "전환매출",
            "roas_disp": "ROAS(%)",
        }
    )

    disp["노출"] = pd.to_numeric(disp["노출"], errors="coerce").fillna(0).astype(int)
    disp["클릭"] = pd.to_numeric(disp["클릭"], errors="coerce").fillna(0).astype(int)
    disp["전환"] = pd.to_numeric(disp["전환"], errors="coerce").fillna(0).astype(int)
    disp["CTR(%)"] = disp["CTR(%)"].astype(float)
    disp = finalize_ctr_col(disp, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재ID", "소재내용", "노출", "클릭", "CTR(%)", "CPC", "광고비", "전환", "CPA", "전환매출", "ROAS(%)"]
    view_df = disp[cols].copy()

    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={"소재내용": st.column_config.TextColumn("소재내용", width="large")},
    )
    render_download_compact(view_df, f"성과_소재_TOP{top_n}_{f['start']}_{f['end']}", "ad", "ad")


def page_settings(engine) -> None:
    st.markdown("## 설정 / 연결")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🧹 캐시 비우기", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("캐시를 비웠습니다.")
            st.rerun()
    with c2:
        st.caption("조회가 이상하면 캐시 비우고 다시 실행")

    try:
        sql_read(engine, "SELECT 1 AS ok")
        st.success("DB 연결 성공 ✅")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return

    st.markdown("### accounts.xlsx → DB 동기화")
    if st.button("🔁 동기화 실행", use_container_width=True):
        res = seed_from_accounts_xlsx(engine)
        st.success(f"완료: meta {res.get('meta', 0)}건")
        st.cache_data.clear()
        st.rerun()


# -----------------------------
# Main
# -----------------------------

def main():
    st.title("네이버 검색광고 통합 대시보드")
    st.caption(f"빌드: {BUILD_TAG}")

    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

    render_data_freshness(engine)

    # seed (best effort)
    try:
        seed_from_accounts_xlsx(engine)
    except Exception:
        pass

    meta = get_meta(engine)
    if meta is None or meta.empty:
        st.error("dim_account_meta가 비어있습니다. settings에서 accounts.xlsx 동기화를 먼저 해주세요.")
        return

    dim_campaign = load_dim_campaign(engine)
    type_opts = get_campaign_type_options(dim_campaign)

    f = build_filters(engine, meta, type_opts)

    page = st.selectbox("메뉴", ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정/연결"], index=0)

    st.divider()

    if page == "전체 예산/잔액 관리":
        page_budget(meta, engine, f)
    elif page == "성과(캠페인)":
        page_perf_campaign(meta, engine, f)
    elif page == "성과(키워드)":
        page_perf_keyword(meta, engine, f)
    elif page == "성과(소재)":
        page_perf_ad(meta, engine, f)
    else:
        page_settings(engine)


if __name__ == "__main__":
    main()