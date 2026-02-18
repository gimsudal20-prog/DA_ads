# -*- coding: utf-8 -*-
"""
app.py - 네이버 검색광고 통합 대시보드 (v7.7.1)

✅ 이번 버전 핵심 (승훈 요청 반영)
- 체감 속도 개선(1초 내 목표): 불필요한 자동 동기화 제거 + 쿼리 수 최소화 + 다운로드(xlsx) 생성 캐시
- UI 개선(옵션2): streamlit-shadcn-ui 탭/메트릭카드/테이블 적용 (미설치 시 자동 폴백)
- iOS Safari 프론트 오류( TypeError: ... e[s].sticky ) 회피:
  * Streamlit 내부 DOM을 건드리던 data-testid 기반 CSS 제거
  * st.data_editor 제거(프론트 grid 의존도 낮춤) → 안정적인 폼 기반 예산 업데이트로 변경
- customer_id 타입 혼재(TEXT vs BIGINT) 안전:
  * 모든 fact/dim 조인/필터에서 customer_id::text 로 통일
  * IN 필터는 문자열 리터럴('420332')로 구성 (TEXT/BIGINT 모두 안전)

"""

from __future__ import annotations

import os
import re
import io
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import altair as alt

# Optional UI components (shadcn-ui style)
try:
    import streamlit_shadcn_ui as ui  # pip install streamlit-shadcn-ui
    HAS_SHADCN_UI = True
except Exception:
    ui = None  # type: ignore
    HAS_SHADCN_UI = False
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

# Altair (charts)
try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

def _altair_dashline_theme():
    return {
        "config": {
            "background": "transparent",
            "view": {"stroke": "transparent"},
            "axis": {
                "gridColor": "#EBEEF2",
                "gridOpacity": 1,
                "domain": False,
                "labelColor": "#475569",
                "titleColor": "#0f172a",
                "tickColor": "#CBD5E1",
            },
            "legend": {"labelColor": "#475569", "titleColor": "#0f172a"},
            "range": {"category": ["#0528F2", "#056CF2", "#3D9DF2", "#B4C4D9"]},
        }
    }

try:
    alt.themes.register("dashline", _altair_dashline_theme)
    alt.themes.enable("dashline")
except Exception:
    pass


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="네이버 검색광고 통합 대시보드", page_icon="📊", layout="wide")

BUILD_TAG = "v7.7.1 (Compare DoD/WoW/MoM + Delta Bars + N/A pct / 2026-02-18)"

# -----------------------------
# Thresholds (Budget)
# -----------------------------
TOPUP_STATIC_THRESHOLD = int(os.getenv("TOPUP_STATIC_THRESHOLD", "50000"))
TOPUP_AVG_DAYS = int(os.getenv("TOPUP_AVG_DAYS", "3"))
TOPUP_DAYS_COVER = int(os.getenv("TOPUP_DAYS_COVER", "2"))

# -----------------------------
# Minimal CSS (avoid fragile DOM hooks)
# -----------------------------
GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root{
  --c-blue-900:#0528F2;
  --c-blue-700:#056CF2;
  --c-blue-500:#3D9DF2;
  --c-slate-300:#B4C4D9;
  --c-slate-050:#EBEEF2;
  --text:#0f172a;
  --muted:#475569;
  --radius:18px;
  --shadow: 0 10px 26px rgba(2,6,23,0.06);
  --shadow-sm: 0 6px 16px rgba(2,6,23,0.04);
}

html, body, .stApp{
  font-family: "Pretendard", system-ui, -apple-system, "Segoe UI", "Noto Sans KR", Arial, sans-serif;
  color: var(--text);
  background: #ffffff;
}

/* Clean UI */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* Fix top clipping + nicer width */
.block-container {
  /* iOS Safari safe-area + prevent top clipping */
  padding-top: calc(env(safe-area-inset-top) + 4.2rem) !important;
  padding-bottom: 2.6rem;
  max-width: 1400px;
  overflow: visible !important;
}

h1, h2, h3 {
  letter-spacing: -0.02em;
}
h1 { font-weight: 900; }
h2 { font-weight: 900; }
h3 { font-weight: 800; }

hr {
  border-color: rgba(180,196,217,0.45);
}

/* Hero */
.hero{
  margin-top: 0.8rem;
  border-radius: var(--radius);
  border: 1px solid rgba(180,196,217,0.55);
  background:
    radial-gradient(1200px 320px at 10% 0%, rgba(5,108,242,0.10) 0%, rgba(5,108,242,0.02) 55%, rgba(255,255,255,0) 80%),
    linear-gradient(180deg, rgba(61,157,242,0.06), rgba(255,255,255,1));
  padding: 18px 20px;
  box-shadow: var(--shadow-sm);
}
.kicker{
  display:inline-flex;
  align-items:center;
  gap:8px;
  font-size:12px;
  letter-spacing: .12em;
  text-transform: uppercase;
  font-weight: 900;
  color: var(--c-blue-700);
}
.hero-title{
  margin: 8px 0 2px 0;
  font-size: 34px;
  line-height: 1.15;
  font-weight: 900;
}
.hero-sub{
  margin-top: 8px;
  color: var(--muted);
  font-size: 14px;
}
.hero-meta{
  margin-top: 12px;
  display:flex;
  flex-wrap: wrap;
  gap: 8px;
}

/* Chips */
.badge{
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:6px 12px;
  border-radius:999px;
  font-size:12px;
  font-weight:900;
  border:1px solid rgba(180,196,217,0.55);
  background: rgba(255,255,255,0.8);
  color: var(--text);
}
.b-blue { background: rgba(5,40,242,0.10); color: var(--c-blue-900); border-color: rgba(5,40,242,0.22); }
.b-sky  { background: rgba(61,157,242,0.12); color: var(--c-blue-700); border-color: rgba(61,157,242,0.24); }
.b-gray { background: rgba(180,196,217,0.18); color: var(--text); border-color: rgba(180,196,217,0.52); }
.b-red  { background: rgba(5,40,242,0.10); color: var(--c-blue-900); border-color: rgba(5,40,242,0.22); }
.b-yellow { background: rgba(5,108,242,0.10); color: var(--c-blue-700); border-color: rgba(5,108,242,0.22); }
.b-green  { background: rgba(61,157,242,0.12); color: var(--c-blue-700); border-color: rgba(61,157,242,0.24); }

/* Panels / Cards */
.panel{
  padding:14px 16px;
  border-radius: var(--radius);
  background: #fff;
  border: 1px solid rgba(180,196,217,0.55);
  box-shadow: var(--shadow-sm);
}
.panel-title{
  font-size: 14px;
  font-weight: 900;
  color: var(--muted);
  margin-bottom: 8px;
}

/* KPI cards (fallback) */
.kpi{
  border-radius: 18px;
  border: 1px solid rgba(180,196,217,0.55);
  background: #fff;
  padding: 14px 16px;
  box-shadow: var(--shadow-sm);
}
.kpi .t{ font-size: 13px; color: var(--muted); font-weight: 800; }
.kpi .v{ font-size: 22px; font-weight: 900; margin-top: 6px; }
.kpi .d{ font-size: 12px; color: var(--muted); margin-top: 6px; }

/* Buttons: subtle rounding */
.stButton > button, .stDownloadButton > button{
  border-radius: 14px !important;
}

.stApp{ background:#ffffff; }

html, body { background:#ffffff; }
</style>
"""

st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)


def render_hero() -> None:
    st.markdown(
        f"""
        <div class="hero">
          <div class="kicker">NAVER SEARCH ADS · DASHBOARD</div>
          <div class="hero-title">네이버 검색광고 통합 대시보드</div>
          <div class="hero-sub">예산/잔액과 캠페인·키워드·소재 성과를 한 화면에서 빠르게 확인합니다.</div>
          <div class="hero-meta">
            <span class="badge b-gray">빌드: {BUILD_TAG}</span>
            <span class="badge b-sky">Pretendard</span>
            <span class="badge b-blue">White UI</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



# -----------------------------
# UI helpers (shadcn-ui optional)
# -----------------------------
def ui_badges_or_html(items: List[Tuple[str, str]], key: str) -> None:
    """items: list of (text, variant)."""
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.badges(badge_list=items, class_name="flex gap-2 flex-wrap", key=key)
            return
        except Exception:
            pass
    chips = [f"<span class='badge b-gray'>{t}</span>" for t, _ in items]
    st.markdown("".join(chips), unsafe_allow_html=True)


def ui_metric_or_stmetric(title: str, value: str, desc: str, key: str) -> None:
    """Pretty KPI card. Uses shadcn-ui if installed, otherwise HTML card."""
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.metric_card(title=title, content=value, description=desc, key=key)
            return
        except Exception:
            pass

    st.markdown(
        f"""<div class='kpi'>
            <div class='t'>{title}</div>
            <div class='v'>{value}</div>
            <div class='d'>{desc}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def ui_table_or_dataframe(df: pd.DataFrame, key: str, height: int = 260) -> None:
    """Small tables: shadcn table if available; else st.dataframe."""
    if df is None:
        df = pd.DataFrame()
    if HAS_SHADCN_UI and ui is not None:
        try:
            ui.table(df, maxHeight=height, key=key)
            return
        except Exception:
            pass
    st.dataframe(df, use_container_width=True, hide_index=True, height=height)

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


def table_exists(engine, table: str, schema: str = "public") -> bool:
    try:
        insp = inspect(engine)
        return table in set(insp.get_table_names(schema=schema))
    except Exception:
        return False


def get_table_columns(engine, table: str, schema: str = "public") -> set:
    """Inspector는 느리므로 세션 단위 캐시."""
    cache = st.session_state.setdefault("_table_cols_cache", {})
    key = f"{schema}.{table}"
    if key in cache:
        return cache[key]
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        out = {str(c.get("name", "")).lower() for c in cols}
    except Exception:
        out = set()
    cache[key] = out
    return out


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
# Download helpers (cached)
# -----------------------------
@st.cache_data(show_spinner=False)
def _df_json_to_csv_bytes(df_json: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    return df.to_csv(index=False).encode("utf-8-sig")


@st.cache_data(show_spinner=False)
def _df_json_to_xlsx_bytes(df_json: str, sheet_name: str) -> bytes:
    df = pd.read_json(io.StringIO(df_json), orient="split")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
    return output.getvalue()


def render_download_compact(df: pd.DataFrame, filename_base: str, sheet_name: str, key_prefix: str) -> None:
    """렌더링 속도를 위해 CSV는 기본 제공, XLSX는 캐시된 bytes 사용."""
    if df is None or df.empty:
        return

    df_json = df.to_json(orient="split")

    st.markdown(
        """
        <style>
        .stDownloadButton button {
            padding: 0.15rem 0.55rem !important;
            font-size: 0.82rem !important;
            line-height: 1.2 !important;
            min-height: 28px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns([1, 1, 8])
    with c1:
        st.download_button(
            "CSV",
            data=_df_json_to_csv_bytes(df_json),
            file_name=f"{filename_base}.csv",
            mime="text/csv",
            key=f"{key_prefix}_csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "XLSX",
            data=_df_json_to_xlsx_bytes(df_json, sheet_name),
            file_name=f"{filename_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            use_container_width=True,
        )
    with c3:
        st.caption("다운로드")


# -----------------------------
# Formatters (lightweight)
# -----------------------------
def _safe_int(x, default: int = 0) -> int:
    try:
        if pd.isna(x) or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def format_currency(val) -> str:
    return f"{_safe_int(val):,}원"


def format_number_commas(val) -> str:
    return f"{_safe_int(val):,}"


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

    out[col] = s.map(_fmt)
    return out


def parse_currency(val_str) -> int:
    if pd.isna(val_str):
        return 0
    s = re.sub(r"[^\d]", "", str(val_str))
    return int(s) if s else 0


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
    out = []
    seen = set()
    for x in keys:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df = sql_read(_engine, "SELECT customer_id, campaign_id, campaign_name, campaign_tp FROM dim_campaign")
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "campaign_id", "campaign_name", "campaign_tp"])
    df["campaign_tp"] = df.get("campaign_tp", "").fillna("")
    df["campaign_type_label"] = df["campaign_tp"].astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type_label"].astype(str).str.strip() == "", "campaign_type_label"] = "기타"
    return df


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
# Accounts / Meta
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


def ensure_meta_table(engine) -> None:
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


def seed_from_accounts_xlsx(engine) -> Dict[str, int]:
    """✅ 자동 실행 제거(속도 목적). 설정 페이지에서만 호출."""
    ensure_meta_table(engine)

    if not os.path.exists(ACCOUNTS_XLSX):
        return {"meta": 0}

    df = pd.read_excel(ACCOUNTS_XLSX)
    acc = normalize_accounts_columns(df)

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


@st.cache_data(ttl=600, show_spinner=False)
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

    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "updated_at"])

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["monthly_budget"] = pd.to_numeric(df.get("monthly_budget", 0), errors="coerce").fillna(0).astype("int64")
    df["manager"] = df.get("manager", "").fillna("").astype(str)
    df["account_name"] = df.get("account_name", "").fillna("").astype(str)
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
# Data freshness (single query)
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def query_latest_dates(_engine) -> Dict[str, str]:
    tables = ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily", "fact_bizmoney_daily"]
    parts = []
    for t in tables:
        if table_exists(_engine, t):
            parts.append(f"SELECT '{t}' AS t, MAX(dt) AS mx FROM {t}")
    if not parts:
        return {}
    sql = " UNION ALL ".join(parts)
    df = sql_read(_engine, sql)
    out = {}
    if df is not None and not df.empty:
        for _, r in df.iterrows():
            out[str(r["t"])] = str(r["mx"])[:10] if r["mx"] is not None else "-"
    return out


def render_data_freshness(engine) -> None:
    latest = query_latest_dates(engine)
    if not latest:
        return
    label_map = {
        "fact_campaign_daily": "캠페인",
        "fact_keyword_daily": "키워드",
        "fact_ad_daily": "소재",
        "fact_bizmoney_daily": "비즈머니",
    }
    items = [(f"{label_map.get(k,k)} 최신: {v}", "secondary") for k, v in latest.items()]
    ui_badges_or_html(items, key="freshness_badges")


def build_filters(meta: pd.DataFrame, type_opts: List[str]) -> Dict:
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
        "top_n_keyword": 300,
        "top_n_ad": 200,
        "top_n_campaign": 200,
    }

    if "filters_applied" not in st.session_state:
        st.session_state["filters_applied"] = defaults.copy()
    if "filters_ready" not in st.session_state:
        st.session_state["filters_ready"] = False

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

            top_n_keyword = st.slider("키워드 TOP N", 50, 1000, int(st.session_state["filters_applied"].get("top_n_keyword", 300)), step=50)
            top_n_ad = st.slider("소재 TOP N", 50, 1000, int(st.session_state["filters_applied"].get("top_n_ad", 200)), step=50)
            top_n_campaign = st.slider("캠페인 TOP N", 50, 1000, int(st.session_state["filters_applied"].get("top_n_campaign", 200)), step=50)

        apply_btn = st.button("적용", use_container_width=True)

    if apply_btn:
        st.session_state["filters_ready"] = True
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
    f["ready"] = bool(st.session_state.get("filters_ready", False))

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
    return f


# -----------------------------
# Budget bundle query (single round-trip)
# -----------------------------
@st.cache_data(ttl=180, show_spinner=False)
def query_budget_bundle(
    _engine,
    cids: Tuple[int, ...],
    yesterday: date,
    avg_d1: date,
    avg_d2: date,
    month_d1: date,
    month_d2: date,
    avg_days: int,
) -> pd.DataFrame:
    if not (table_exists(_engine, "dim_account_meta") and table_exists(_engine, "fact_campaign_daily") and table_exists(_engine, "fact_bizmoney_daily")):
        return pd.DataFrame()

    where_cid = ""
    if cids:
        where_cid = f"WHERE m.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    sql = f"""
    WITH meta AS (
      SELECT customer_id::text AS customer_id, account_name, manager, COALESCE(monthly_budget,0) AS monthly_budget
      FROM dim_account_meta m
      {where_cid}
    ),
    biz AS (
      SELECT DISTINCT ON (customer_id::text)
        customer_id::text AS customer_id,
        bizmoney_balance,
        dt AS last_update
      FROM fact_bizmoney_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
      ORDER BY customer_id::text, dt DESC
    ),
    camp AS (
      SELECT
        customer_id::text AS customer_id,
        SUM(cost) FILTER (WHERE dt = :y) AS y_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :a1 AND :a2) AS avg_sum_cost,
        SUM(cost) FILTER (WHERE dt BETWEEN :m1 AND :m2) AS month_cost
      FROM fact_campaign_daily
      WHERE customer_id::text IN (SELECT customer_id FROM meta)
        AND dt BETWEEN :min_dt AND :max_dt
      GROUP BY customer_id::text
    )
    SELECT
      meta.customer_id,
      meta.account_name,
      meta.manager,
      meta.monthly_budget,
      COALESCE(biz.bizmoney_balance,0) AS bizmoney_balance,
      biz.last_update,
      COALESCE(camp.y_cost,0) AS y_cost,
      COALESCE(camp.avg_sum_cost,0) AS avg_sum_cost,
      COALESCE(camp.month_cost,0) AS current_month_cost
    FROM meta
    LEFT JOIN biz ON meta.customer_id = biz.customer_id
    LEFT JOIN camp ON meta.customer_id = camp.customer_id
    ORDER BY meta.account_name
    """

    min_dt = min(yesterday, avg_d1, month_d1)
    max_dt = max(yesterday, avg_d2, month_d2)

    df = sql_read(
        _engine,
        sql,
        {
            "y": str(yesterday),
            "a1": str(avg_d1),
            "a2": str(avg_d2),
            "m1": str(month_d1),
            "m2": str(month_d2),
            "min_dt": str(min_dt),
            "max_dt": str(max_dt),
        },
    )
    if df is None or df.empty:
        return pd.DataFrame()

    # typing
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    for c in ["monthly_budget", "bizmoney_balance", "y_cost", "avg_sum_cost", "current_month_cost"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    df["avg_cost"] = df["avg_sum_cost"].astype(float) / float(max(avg_days, 1))
    return df


# -----------------------------
# Perf queries (TOP N) - single pass (no double join)
# -----------------------------
def _fact_has_sales(engine, fact_table: str) -> bool:
    return "sales" in get_table_columns(engine, fact_table)


@st.cache_data(ttl=600, show_spinner=False)
def query_campaign_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트
    - fact → (필요 시) dim_campaign 타입 필터를 먼저 적용한 뒤 집계
    - 집계 결과에서 cost 기준 TOP N만 뽑고, 그 다음에 이름/타입 조인
    """
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          c.campaign_name,
          c.campaign_tp
        FROM top t
        JOIN c_f c
          ON t.customer_id = c.customer_id
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM top t
        LEFT JOIN dim_campaign c
          ON t.customer_id = c.customer_id::text
         AND t.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")

    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]

    return df.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def query_campaign_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    ✅ 1회 쿼리로 캠페인 탭에 필요한 데이터 동시 확보
    - 광고비 기준 TopN (topn_cost)
    - 클릭 TopK, 전환 TopK (top_k)
    * 비용 TopN만 뽑는 쿼리로는 클릭/전환 TopK가 누락될 수 있어서,
      base 집계 후 (cost/clk/conv) 각각 LIMIT로 뽑아 UNION 합니다.
    """
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT
            customer_id::text AS customer_id,
            campaign_id,
            COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
            COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          JOIN c_f c
            ON f.customer_id::text = c.customer_id
           AND f.campaign_id = c.campaign_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          c.campaign_name,
          c.campaign_tp
        FROM picked p
        JOIN c_f c
          ON p.customer_id = c.customer_id
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.campaign_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_campaign_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.campaign_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM picked p
        LEFT JOIN dim_campaign c
          ON p.customer_id = c.customer_id::text
         AND p.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]

    return df.reset_index(drop=True)


# -----------------------------
# Timeseries Queries (for charts)
# -----------------------------

@st.cache_data(ttl=300, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """캠페인(전체) 일별 추세. (그래프용: row 수 적음)"""
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_campaign_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        JOIN c_f c
          ON f.customer_id::text = c.customer_id
         AND f.campaign_id = c.campaign_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_campaign_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def query_ad_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """소재(전체) 일별 추세."""
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_ad"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        a_f AS (
          SELECT a.customer_id::text AS customer_id, a.ad_id::text AS ad_id
          FROM dim_ad a
          JOIN g_f g ON a.customer_id::text = g.customer_id AND a.adgroup_id::text = g.adgroup_id
        )
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        JOIN a_f a ON f.customer_id::text = a.customer_id AND f.ad_id::text = a.ad_id
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          f.dt::date AS dt,
          SUM(f.imp)  AS imp,
          SUM(f.clk)  AS clk,
          SUM(f.cost) AS cost,
          SUM(f.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_ad_daily f
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY f.dt::date
        ORDER BY f.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def query_keyword_timeseries(_engine, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> pd.DataFrame:
    """키워드(전체) 일별 추세. type_sel 없으면 join 없이 fact만 집계."""
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    if tp_keys and table_exists(_engine, "dim_campaign") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_keyword"):
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id::text AS campaign_id
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        g_f AS (
          SELECT g.customer_id::text AS customer_id, g.adgroup_id::text AS adgroup_id
          FROM dim_adgroup g
          JOIN c_f c ON g.customer_id::text = c.customer_id AND g.campaign_id::text = c.campaign_id
        ),
        k_f AS (
          SELECT k.customer_id::text AS customer_id, k.keyword_id::text AS keyword_id
          FROM dim_keyword k
          JOIN g_f g ON k.customer_id::text = g.customer_id AND k.adgroup_id::text = g.adgroup_id
        )
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        JOIN k_f k ON fk.customer_id::text = k.customer_id AND fk.keyword_id::text = k.keyword_id
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
    else:
        sql = f"""
        SELECT
          fk.dt::date AS dt,
          SUM(fk.imp)  AS imp,
          SUM(fk.clk)  AS clk,
          SUM(fk.cost) AS cost,
          SUM(fk.conv) AS conv,
          {sales_expr} AS sales
        FROM fact_keyword_daily fk
        WHERE fk.dt BETWEEN :d1 AND :d2
          {where_cid}
        GROUP BY fk.dt::date
        ORDER BY fk.dt::date
        """
        df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})

    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "imp", "clk", "cost", "conv", "sales"])

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


# -----------------------------
# Altair Charts (rounded / smooth)
# -----------------------------

def _chart_timeseries(ts: pd.DataFrame, y_col: str, y_title: str, y_format: str = "", height: int = 260):
    if ts is None or ts.empty:
        return None

    d = ts[["dt", y_col]].copy()
    d = d.dropna(subset=["dt"]).copy()
    d["dt"] = pd.to_datetime(d["dt"])
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce").fillna(0)

    base = alt.Chart(d).encode(
        x=alt.X("dt:T", title="", axis=alt.Axis(labelAngle=0, labelOverlap=True)),
        y=alt.Y(f"{y_col}:Q", title=y_title, axis=alt.Axis(format=y_format) if y_format else alt.Axis()),
        tooltip=[
            alt.Tooltip("dt:T", title="날짜"),
            alt.Tooltip(f"{y_col}:Q", title=y_title, format=y_format if y_format else None),
        ],
    )

    line = base.mark_line(interpolate="monotone", strokeWidth=3, color="#0528F2")
    pts = base.mark_circle(size=55, color="#056CF2")
    return (line + pts).properties(height=height)


def _chart_progress_bars(df: pd.DataFrame, label_col: str, value_col: str, value_title: str, top_n: int = 10, height: int = 320):
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        return None

    d = df[[label_col, value_col]].copy()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0)
    d = d.sort_values(value_col, ascending=False).head(int(top_n)).copy()
    d[label_col] = d[label_col].astype(str).str.slice(0, 34)

    maxv = float(d[value_col].max()) if len(d) else 0.0
    d["_max"] = maxv

    y = alt.Y(f"{label_col}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=240))
    bg = alt.Chart(d).mark_bar(cornerRadius=10, height=18, color="#EBEEF2").encode(
        y=y,
        x=alt.X("_max:Q", title=value_title),
    )

    fg = alt.Chart(d).mark_bar(cornerRadius=10, height=18, color="#3D9DF2").encode(
        y=y,
        x=alt.X(f"{value_col}:Q", title=value_title),
        tooltip=[alt.Tooltip(f"{label_col}:N", title="항목"), alt.Tooltip(f"{value_col}:Q", title=value_title, format=",.0f")],
    )

    txt = alt.Chart(d).mark_text(align="left", baseline="middle", dx=8, color="#0f172a", fontSize=12).encode(
        y=y,
        x=alt.X(f"{value_col}:Q"),
        text=alt.Text(f"{value_col}:Q", format=",.0f"),
    )

    return (bg + fg + txt).properties(height=height)


def query_ad_topn(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    top_n: int,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트
    - fact_ad_daily를 먼저 집계 → cost TOP N만 뽑고 → 그 후 dim 조인
    - 캠페인 유형 필터가 있을 때만 dim 경유(scope)를 만들어 fact를 좁혀서 집계
    """
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    cols = get_table_columns(_engine, "dim_ad")
    ad_text_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id,
                 COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
                 COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        ad_scope AS (
          SELECT
            a.customer_id::text AS customer_id,
            a.ad_id,
            a.adgroup_id,
            {ad_text_expr} AS ad_name,
            COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
            c.campaign_name,
            c.campaign_tp
          FROM dim_ad a
          JOIN dim_adgroup g
            ON a.customer_id::text = g.customer_id::text
           AND a.adgroup_id = g.adgroup_id
          JOIN c_f c
            ON g.customer_id::text = c.customer_id
           AND g.campaign_id = c.campaign_id
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          JOIN ad_scope s
            ON f.customer_id::text = s.customer_id
           AND f.ad_id = s.ad_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          s.ad_name,
          s.adgroup_name,
          s.campaign_name,
          s.campaign_tp
        FROM top t
        JOIN ad_scope s
          ON t.customer_id = s.customer_id
         AND t.ad_id = s.ad_id
        ORDER BY t.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        top AS (
          SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim
        )
        SELECT
          t.*,
          {ad_text_expr} AS ad_name,
          COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM top t
        LEFT JOIN dim_ad a
          ON t.customer_id = a.customer_id::text
         AND t.ad_id = a.ad_id
        LEFT JOIN dim_adgroup g
          ON a.customer_id::text = g.customer_id::text
         AND a.adgroup_id = g.adgroup_id
        LEFT JOIN dim_campaign c
          ON g.customer_id::text = c.customer_id::text
         AND g.campaign_id = c.campaign_id
        ORDER BY t.cost DESC NULLS LAST
        """

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), "lim": int(top_n)})
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]

    return df.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def query_ad_bundle(
    _engine,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 200,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    ✅ 1회 쿼리로 소재 탭에 필요한 데이터 동시 확보
    - 광고비 기준 TopN (topn_cost)
    - 클릭 TopK, 전환 TopK (top_k)
    * base 집계 후 (cost/clk/conv) 각각 LIMIT로 뽑아 UNION → 그 다음 DIM 조인
    """
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_ad") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    has_sales = _fact_has_sales(_engine, "fact_ad_daily")
    sales_expr = "SUM(COALESCE(f.sales,0))" if has_sales else "0::numeric"

    where_cid = ""
    if cids:
        where_cid = f"AND f.customer_id::text IN ({_sql_in_str_list(list(cids))})"

    cols = get_table_columns(_engine, "dim_ad")
    ad_text_expr = "COALESCE(NULLIF(a.creative_text,''), NULLIF(a.ad_name,''), '')" if "creative_text" in cols else "COALESCE(a.ad_name,'')"

    tp_keys = label_to_tp_keys(type_sel) if type_sel else []

    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        sql = f"""
        WITH c_f AS (
          SELECT customer_id::text AS customer_id, campaign_id,
                 COALESCE(NULLIF(campaign_name,''),'') AS campaign_name,
                 COALESCE(NULLIF(campaign_tp,''),'')   AS campaign_tp
          FROM dim_campaign
          WHERE LOWER(COALESCE(campaign_tp,'')) IN ({tp_list})
        ),
        ad_scope AS (
          SELECT
            a.customer_id::text AS customer_id,
            a.ad_id,
            a.adgroup_id,
            {ad_text_expr} AS ad_name,
            COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
            c.campaign_name,
            c.campaign_tp
          FROM dim_ad a
          JOIN dim_adgroup g
            ON a.customer_id::text = g.customer_id::text
           AND a.adgroup_id = g.adgroup_id
          JOIN c_f c
            ON g.customer_id::text = c.customer_id
           AND g.campaign_id = c.campaign_id
        ),
        base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          JOIN ad_scope s
            ON f.customer_id::text = s.customer_id
           AND f.ad_id = s.ad_id
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          s.ad_name,
          s.adgroup_name,
          s.campaign_name,
          s.campaign_tp
        FROM picked p
        JOIN ad_scope s
          ON p.customer_id = s.customer_id
         AND p.ad_id = s.ad_id
        ORDER BY p.cost DESC NULLS LAST
        """
    else:
        sql = f"""
        WITH base AS (
          SELECT
            f.customer_id::text AS customer_id,
            f.ad_id,
            SUM(f.imp)  AS imp,
            SUM(f.clk)  AS clk,
            SUM(f.cost) AS cost,
            SUM(f.conv) AS conv,
            {sales_expr} AS sales
          FROM fact_ad_daily f
          WHERE f.dt BETWEEN :d1 AND :d2
            {where_cid}
          GROUP BY f.customer_id::text, f.ad_id
        ),
        cost_top AS (SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :lim_cost),
        clk_top  AS (SELECT * FROM base ORDER BY clk  DESC NULLS LAST LIMIT :lim_k),
        conv_top AS (SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT :lim_k),
        picked AS (
          SELECT * FROM cost_top
          UNION
          SELECT * FROM clk_top
          UNION
          SELECT * FROM conv_top
        )
        SELECT
          p.*,
          {ad_text_expr} AS ad_name,
          COALESCE(NULLIF(g.adgroup_name,''),'') AS adgroup_name,
          COALESCE(NULLIF(c.campaign_name,''),'') AS campaign_name,
          COALESCE(NULLIF(c.campaign_tp,''),'')   AS campaign_tp
        FROM picked p
        LEFT JOIN dim_ad a
          ON p.customer_id = a.customer_id::text
         AND p.ad_id = a.ad_id
        LEFT JOIN dim_adgroup g
          ON a.customer_id::text = g.customer_id::text
         AND a.adgroup_id = g.adgroup_id
        LEFT JOIN dim_campaign c
          ON g.customer_id::text = c.customer_id::text
         AND g.campaign_id = c.campaign_id
        ORDER BY p.cost DESC NULLS LAST
        """

    df = sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "lim_cost": int(topn_cost), "lim_k": int(top_k)},
    )
    if df is None or df.empty:
        return pd.DataFrame()

    for c in ["imp", "clk", "cost", "conv", "sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").fillna(0).astype("int64")
    df["campaign_type"] = df.get("campaign_tp", "").astype(str).map(campaign_tp_to_label)
    df.loc[df["campaign_type"].astype(str).str.strip() == "", "campaign_type"] = "기타"
    df = df[df["campaign_type"].astype(str).str.strip() != "기타"]

    return df.reset_index(drop=True)

def query_keyword_bundle(
    _engine,
    d1: date,
    d2: date,
    customer_ids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    topn_cost: int = 300,
) -> pd.DataFrame:
    """
    ✅ 속도 개선 포인트 (v7.3.1)
    - fact_keyword_daily는 "집계(base)"만 하고,
      cost TOP N / clk TOP10 / conv TOP10만 골라서(dim 조인 포함) 반환
    - dim 조인은 선택된 keyword_id들에 대해서만 수행 → 대폭 가벼움
    """
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()
    if not (table_exists(_engine, "dim_keyword") and table_exists(_engine, "dim_adgroup") and table_exists(_engine, "dim_campaign")):
        return pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_sum = "SUM(COALESCE(fk.sales,0))" if "sales" in fk_cols else "0::numeric"

    # dim_keyword 키워드 컬럼명 호환
    kw_cols = get_table_columns(_engine, "dim_keyword")
    if "keyword" in kw_cols:
        kw_expr = "k.keyword"
    elif "keyword_name" in kw_cols:
        kw_expr = "k.keyword_name"
    else:
        kw_expr = "''::text"

    # cid filter (TEXT/BIGINT 모두 안전)
    cid_clause = ""
    if customer_ids:
        cid_clause = f"AND fk.customer_id::text IN ({_sql_in_str_list(list(customer_ids))})"

    # type filter는 campaign_tp 키로 (더 빠름)
    tp_keys = label_to_tp_keys(type_sel) if type_sel else []
    type_clause = ""
    if tp_keys:
        tp_list = ",".join([f"'{x}'" for x in tp_keys])
        type_clause = f"AND LOWER(COALESCE(c.campaign_tp,'')) IN ({tp_list})"

    sql = f"""
    WITH scope AS (
      SELECT
        k.customer_id::text AS customer_id,
        k.keyword_id::text  AS keyword_id,
        COALESCE(NULLIF(TRIM({kw_expr}),''),'') AS keyword,
        k.adgroup_id::text  AS adgroup_id,
        COALESCE(NULLIF(TRIM(g.adgroup_name),''),'') AS adgroup_name,
        g.campaign_id::text AS campaign_id,
        COALESCE(NULLIF(TRIM(c.campaign_name),''),'') AS campaign_name,
        COALESCE(NULLIF(TRIM(c.campaign_tp),''),'')   AS campaign_tp,
        CASE
          WHEN lower(trim(c.campaign_tp)) IN ('web_site','website','power_link','powerlink') THEN '파워링크'
          WHEN lower(trim(c.campaign_tp)) IN ('shopping','shopping_search') THEN '쇼핑검색'
          WHEN lower(trim(c.campaign_tp)) IN ('power_content','power_contents','powercontent') THEN '파워콘텐츠'
          WHEN lower(trim(c.campaign_tp)) IN ('place','place_search') THEN '플레이스'
          WHEN lower(trim(c.campaign_tp)) IN ('brand_search','brandsearch') THEN '브랜드검색'
          ELSE '기타'
        END AS campaign_type_label
      FROM dim_keyword k
      LEFT JOIN dim_adgroup g
        ON k.customer_id::text = g.customer_id::text
       AND k.adgroup_id::text = g.adgroup_id::text
      LEFT JOIN dim_campaign c
        ON g.customer_id::text = c.customer_id::text
       AND g.campaign_id::text = c.campaign_id::text
      WHERE 1=1
        AND COALESCE(NULLIF(trim(c.campaign_tp),''),'') <> ''
        {type_clause}
    ),
    base AS (
      SELECT
        fk.customer_id::text AS customer_id,
        fk.keyword_id::text  AS keyword_id,
        SUM(fk.imp)  AS imp,
        SUM(fk.clk)  AS clk,
        SUM(fk.cost) AS cost,
        SUM(fk.conv) AS conv,
        {sales_sum}  AS sales
      FROM fact_keyword_daily fk
      JOIN scope s
        ON fk.customer_id::text = s.customer_id
       AND fk.keyword_id::text  = s.keyword_id
      WHERE fk.dt BETWEEN :d1 AND :d2
        {cid_clause}
      GROUP BY fk.customer_id::text, fk.keyword_id::text
    ),
    top_cost0 AS (
      SELECT * FROM base ORDER BY cost DESC NULLS LAST LIMIT :topn_cost
    ),
    top_cost AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY cost DESC NULLS LAST) AS rn_cost
      FROM top_cost0
    ),
    top_clk0 AS (
      SELECT * FROM base ORDER BY clk DESC NULLS LAST LIMIT 10
    ),
    top_clk AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY clk DESC NULLS LAST) AS rn_clk
      FROM top_clk0
    ),
    top_conv0 AS (
      SELECT * FROM base ORDER BY conv DESC NULLS LAST LIMIT 10
    ),
    top_conv AS (
      SELECT
        customer_id, keyword_id,
        ROW_NUMBER() OVER (ORDER BY conv DESC NULLS LAST) AS rn_conv
      FROM top_conv0
    ),
    picked AS (
      SELECT
        customer_id,
        keyword_id,
        MIN(rn_cost) AS rn_cost,
        MIN(rn_clk)  AS rn_clk,
        MIN(rn_conv) AS rn_conv
      FROM (
        SELECT customer_id, keyword_id, rn_cost, NULL::int rn_clk, NULL::int rn_conv FROM top_cost
        UNION ALL
        SELECT customer_id, keyword_id, NULL::int rn_cost, rn_clk, NULL::int rn_conv FROM top_clk
        UNION ALL
        SELECT customer_id, keyword_id, NULL::int rn_cost, NULL::int rn_clk, rn_conv FROM top_conv
      ) u
      GROUP BY customer_id, keyword_id
    )
    SELECT
      p.customer_id,
      p.keyword_id,
      b.imp, b.clk, b.cost, b.conv, b.sales,
      p.rn_cost, p.rn_clk, p.rn_conv,
      s.keyword, s.adgroup_name, s.campaign_name, s.campaign_tp, s.campaign_type_label
    FROM picked p
    JOIN base b
      ON p.customer_id = b.customer_id
     AND p.keyword_id  = b.keyword_id
    LEFT JOIN scope s
      ON b.customer_id = s.customer_id
     AND b.keyword_id  = s.keyword_id
    WHERE COALESCE(s.campaign_type_label,'기타') <> '기타'
    ORDER BY COALESCE(p.rn_cost, 999999), b.cost DESC NULLS LAST
    """

    params = {"d1": str(d1), "d2": str(d2), "topn_cost": int(topn_cost)}
    df = sql_read(_engine, sql, params)
    return df if df is not None else pd.DataFrame()

    fk_cols = get_table_columns(_engine, "fact_keyword_daily")
    sales_expr = "SUM(COALESCE(fk.sales,0)) AS sales" if "sales" in fk_cols else "0::bigint AS sales"

    kw_cols = get_table_columns(_engine, "dim_keyword")
    if "keyword" in kw_cols:
        kw_expr = "k.keyword"
    elif "keyword_name" in kw_cols:
        kw_expr = "k.keyword_name"
    else:
        kw_expr = "''::text"

    in_clause = ""
    if customer_ids:
        in_clause = f" AND fk.customer_id::text IN ({_sql_in_str_list(list(customer_ids))}) "

    # type filter는 alias 참조 문제 때문에 마지막에 적용
    type_filter = ""
    if type_sel:
        tquoted = ",".join(["'" + str(t).replace("'", "''") + "'" for t in type_sel])
        type_filter = f" AND campaign_type_label IN ({tquoted}) "

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
                ELSE '기타'
            END AS campaign_type_label
        FROM base b
        LEFT JOIN dim_keyword k
            ON b.customer_id = k.customer_id::text AND b.keyword_id = k.keyword_id::text
        LEFT JOIN dim_adgroup g
            ON k.customer_id::text = g.customer_id::text AND k.adgroup_id::text = g.adgroup_id::text
        LEFT JOIN dim_campaign c
            ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text
        WHERE 1=1
          AND COALESCE(NULLIF(trim(c.campaign_tp),''),'') <> ''
    ),
    ranked AS (
        SELECT
            j.*,
            ROW_NUMBER() OVER (ORDER BY j.cost DESC NULLS LAST) AS rn_cost,
            ROW_NUMBER() OVER (ORDER BY j.clk DESC NULLS LAST) AS rn_clk,
            ROW_NUMBER() OVER (ORDER BY j.conv DESC NULLS LAST) AS rn_conv
        FROM joined j
        WHERE j.campaign_type_label <> '기타'
        {type_filter}
    )
    SELECT *
    FROM ranked
    WHERE rn_cost <= :topn_cost OR rn_clk <= 10 OR rn_conv <= 10
    ORDER BY rn_cost ASC
    """

    params = {"d1": str(d1), "d2": str(d2), "topn_cost": int(topn_cost)}
    df = sql_read(_engine, sql, params)
    return df if df is not None else pd.DataFrame()


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
# Period comparison (DoD / WoW / MoM)
# -----------------------------

def _last_day_of_month(y: int, m: int) -> int:
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    return (nxt - timedelta(days=1)).day


def _shift_month(d: date, months: int) -> date:
    """Shift month while clamping day (e.g. Mar 31 -> Feb 28/29)."""
    base = (d.year * 12) + (d.month - 1) + int(months)
    y = base // 12
    m = (base % 12) + 1
    day = min(int(d.day), _last_day_of_month(int(y), int(m)))
    return date(int(y), int(m), int(day))


def _period_compare_range(d1: date, d2: date, mode: str) -> Tuple[date, date]:
    mode = str(mode or "").strip()
    if mode == "전일대비":
        return d1 - timedelta(days=1), d2 - timedelta(days=1)
    if mode == "전주대비":
        return d1 - timedelta(days=7), d2 - timedelta(days=7)
    # 전월대비 (default)
    return _shift_month(d1, -1), _shift_month(d2, -1)


def _safe_div(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return float(a) / float(b)
    except Exception:
        return 0.0


def _pct_change(curr: float, prev: float) -> Optional[float]:
    """Percent change. If prev==0 and curr>0 -> None (N/A)."""
    if prev == 0:
        return 0.0 if curr == 0 else None
    return (float(curr) - float(prev)) / float(prev) * 100.0


def _pct_to_str(p: Optional[float]) -> str:
    return "—" if p is None else f"{p:+.1f}%"


@st.cache_data(ttl=300, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: Tuple[int, ...], type_sel: Tuple[str, ...]) -> Dict[str, float]:
    entity = str(entity or "").lower().strip()
    try:
        if entity == "campaign":
            ts = query_campaign_timeseries(_engine, d1, d2, cids, type_sel)
        elif entity == "keyword":
            ts = query_keyword_timeseries(_engine, d1, d2, cids, type_sel)
        else:
            ts = query_ad_timeseries(_engine, d1, d2, cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is None or ts.empty:
        return {"imp": 0.0, "clk": 0.0, "cost": 0.0, "conv": 0.0, "sales": 0.0, "ctr": 0.0, "cpc": 0.0, "cpa": 0.0, "roas": 0.0}

    def _sum(col: str) -> float:
        if col not in ts.columns:
            return 0.0
        return float(pd.to_numeric(ts[col], errors="coerce").fillna(0).sum())

    imp = _sum("imp")
    clk = _sum("clk")
    cost = _sum("cost")
    conv = _sum("conv")
    sales = _sum("sales")
    ctr = _safe_div(clk, imp) * 100.0
    cpc = _safe_div(cost, clk)
    cpa = _safe_div(cost, conv)
    roas = _safe_div(sales, cost) * 100.0

    return {"imp": imp, "clk": clk, "cost": cost, "conv": conv, "sales": sales, "ctr": ctr, "cpc": cpc, "cpa": cpa, "roas": roas}


def _chart_delta_bars(delta_df: pd.DataFrame, height: int = 260):
    if delta_df is None or delta_df.empty:
        return None
    d = delta_df.copy()
    d["raw"] = d["change_pct"]
    d["change_pct"] = pd.to_numeric(d["change_pct"], errors="coerce").fillna(0.0)
    d["label"] = d["raw"].apply(lambda v: "—" if pd.isna(v) else f"{float(v):+.1f}%")

    base = alt.Chart(d).encode(
        y=alt.Y("metric:N", sort=None, title=""),
        x=alt.X("change_pct:Q", title="증감율(%)", axis=alt.Axis(format="+.1f")),
        tooltip=[alt.Tooltip("metric:N", title="지표"), alt.Tooltip("label:N", title="증감율")],
    )

    bars = base.mark_bar(cornerRadius=10).encode(
        color=alt.condition(alt.datum.change_pct >= 0, alt.value("#3D9DF2"), alt.value("#B4C4D9"))
    )

    rule = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(strokeDash=[6, 6]).encode(x="x:Q")

    txt = base.mark_text(align="left", baseline="middle", dx=6).encode(text="label:N")

    ch = (rule + bars + txt).properties(height=height).configure_axis(grid=False).configure_view(strokeWidth=0)
    return ch


def render_period_compare_panel(
    engine,
    entity: str,
    d1: date,
    d2: date,
    cids: Tuple[int, ...],
    type_sel: Tuple[str, ...],
    key_prefix: str,
    expanded: bool = False,
) -> None:
    """Reusable panel: DoD/WoW/MoM comparison + delta bar chart."""
    with st.expander("🔁 전일/전주/전월 비교", expanded=expanded):
        mode = st.radio(
            "비교 기준",
            ["전일대비", "전주대비", "전월대비"],
            horizontal=True,
            index=1,
            key=f"{key_prefix}_cmp_mode",
        )

        b1, b2 = _period_compare_range(d1, d2, mode)

        cur = get_entity_totals(engine, entity, d1, d2, cids, type_sel)
        base = get_entity_totals(engine, entity, b1, b2, cids, type_sel)

        # KPI cards (current + delta)
        c1, c2, c3, c4 = st.columns(4)
        dc = cur["cost"] - base["cost"]
        dclk = cur["clk"] - base["clk"]
        dconv = cur["conv"] - base["conv"]
        droas = cur["roas"] - base["roas"]

        with c1:
            ui_metric_or_stmetric(
                "광고비",
                format_currency(cur["cost"]),
                f"{mode} {b1}~{b2} 대비 {format_currency(dc)} / {_pct_to_str(_pct_change(cur['cost'], base['cost']))}",
                key=f"{key_prefix}_cmp_cost",
            )
        with c2:
            ui_metric_or_stmetric(
                "클릭",
                format_number_commas(cur["clk"]),
                f"{mode} 대비 {format_number_commas(dclk)} / {_pct_to_str(_pct_change(cur['clk'], base['clk']))}",
                key=f"{key_prefix}_cmp_clk",
            )
        with c3:
            ui_metric_or_stmetric(
                "전환",
                format_number_commas(cur["conv"]),
                f"{mode} 대비 {format_number_commas(dconv)} / {_pct_to_str(_pct_change(cur['conv'], base['conv']))}",
                key=f"{key_prefix}_cmp_conv",
            )
        with c4:
            ui_metric_or_stmetric(
                "ROAS(%)",
                format_roas(cur["roas"]),
                f"{mode} 대비 {droas:+.1f}p",
                key=f"{key_prefix}_cmp_roas",
            )

        # Delta bar chart
        delta_df = pd.DataFrame(
            [
                {"metric": "광고비", "change_pct": _pct_change(cur["cost"], base["cost"])},
                {"metric": "클릭", "change_pct": _pct_change(cur["clk"], base["clk"])},
                {"metric": "전환", "change_pct": _pct_change(cur["conv"], base["conv"])},
                {"metric": "매출", "change_pct": _pct_change(cur["sales"], base["sales"])},
                {"metric": "ROAS", "change_pct": _pct_change(cur["roas"], base["roas"])},
            ]
        )
        st.markdown("#### 📊 증감율(%) 막대그래프")
        ch = _chart_delta_bars(delta_df, height=260)
        if ch is not None:
            st.altair_chart(ch, use_container_width=True)

        # Mini table (current vs baseline)
        mini = pd.DataFrame(
            [
                ["광고비", format_currency(cur["cost"]), format_currency(base["cost"]), f"{_pct_to_str(_pct_change(cur['cost'], base['cost']))}"],
                ["클릭", format_number_commas(cur["clk"]), format_number_commas(base["clk"]), f"{_pct_to_str(_pct_change(cur['clk'], base['clk']))}"],
                ["전환", format_number_commas(cur["conv"]), format_number_commas(base["conv"]), f"{_pct_to_str(_pct_change(cur['conv'], base['conv']))}"],
                ["매출", format_currency(cur["sales"]), format_currency(base["sales"]), _pct_to_str(_pct_change(cur["sales"], base["sales"]))],
                ["ROAS(%)", format_roas(cur["roas"]), format_roas(base["roas"]), f"{(cur['roas']-base['roas']):+.1f}p"],
            ],
            columns=["지표", "현재", "비교기간", "증감"],
        )
        ui_table_or_dataframe(mini, key=f"{key_prefix}_cmp_table", height=210)

# -----------------------------
# Pages
# -----------------------------
def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("## 💰 전체 예산 / 잔액 관리")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)

    # 평균소진(최근 TOPUP_AVG_DAYS일) 계산 구간: (end - 1) 기준으로 과거 TOPUP_AVG_DAYS
    end_dt = f.get("end") or yesterday
    avg_d2 = end_dt - timedelta(days=1)
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    # 월 누적 구간
    month_d1 = end_dt.replace(day=1)
    if end_dt.month == 12:
        month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_d2 = date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    bundle = query_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, TOPUP_AVG_DAYS)
    if bundle is None or bundle.empty:
        st.warning("예산/잔액 데이터를 불러올 수 없습니다. (fact_bizmoney_daily/fact_campaign_daily 확인)")
        return

    biz_view = bundle.copy()
    biz_view["last_update"] = pd.to_datetime(biz_view.get("last_update"), errors="coerce").dt.strftime("%y.%m.%d").fillna("-")

    # days_cover & threshold
    biz_view["days_cover"] = pd.NA
    m = biz_view["avg_cost"].astype(float) > 0
    biz_view.loc[m, "days_cover"] = biz_view.loc[m, "bizmoney_balance"].astype(float) / biz_view.loc[m, "avg_cost"].astype(float)

    biz_view["threshold"] = (biz_view["avg_cost"].astype(float) * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))

    biz_view["상태"] = "🟢 여유"
    biz_view.loc[biz_view["bizmoney_balance"].astype(float) < biz_view["threshold"].astype(float), "상태"] = "🔴 충전필요"

    # display columns (small cost)
    biz_view["비즈머니 잔액"] = biz_view["bizmoney_balance"].map(format_currency)
    biz_view[f"최근{TOPUP_AVG_DAYS}일 평균소진"] = biz_view["avg_cost"].map(format_currency)
    biz_view["전일 소진액"] = biz_view["y_cost"].map(format_currency)

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

    biz_view["D-소진"] = biz_view["days_cover"].map(_fmt_days)
    biz_view["확인일자"] = biz_view["last_update"]

    # summary
    total_balance = int(pd.to_numeric(biz_view["bizmoney_balance"], errors="coerce").fillna(0).sum())
    total_month_cost = int(pd.to_numeric(biz_view["current_month_cost"], errors="coerce").fillna(0).sum())
    count_low_balance = int(biz_view["상태"].astype(str).str.contains("충전필요").sum())

    st.markdown("### 🔍 전체 계정 요약")
    c1, c2, c3 = st.columns(3)
    with c1:
        ui_metric_or_stmetric('총 비즈머니 잔액', format_currency(total_balance), '전체 계정 합산', key='m_total_balance')
    with c2:
        ui_metric_or_stmetric(f"{end_dt.month}월 총 사용액", format_currency(total_month_cost), f"{end_dt.strftime('%Y-%m')} 누적", key='m_month_cost')
    with c3:
        ui_metric_or_stmetric('충전 필요 계정', f"{count_low_balance}건", '임계치 미만', key='m_need_topup')

    st.divider()

    need_topup = count_low_balance
    ok_topup = int(len(biz_view) - need_topup)
    st.markdown(
        f"<span class='badge b-red'>충전필요 {need_topup}건</span>"
        f"<span class='badge b-green'>여유 {ok_topup}건</span>",
        unsafe_allow_html=True,
    )

    show_only_topup = st.checkbox("충전필요만 보기", value=False)

    biz_view["_rank"] = biz_view["상태"].map(lambda s: 0 if "충전필요" in str(s) else 1)
    biz_view = biz_view.sort_values(["_rank", "bizmoney_balance", "account_name"]).drop(columns=["_rank"])
    if show_only_topup:
        biz_view = biz_view[biz_view["상태"].str.contains("충전필요", na=False)].copy()

    view_cols = ["account_name", "manager", "비즈머니 잔액", f"최근{TOPUP_AVG_DAYS}일 평균소진", "D-소진", "전일 소진액", "상태", "확인일자"]
    display_df = biz_view[view_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    st.dataframe(display_df, use_container_width=True, hide_index=True)
    render_download_compact(display_df, f"예산_잔액_{f['start']}_{f['end']}", "budget", "budget")

    st.divider()

    st.markdown(f"### 📅 월 예산 관리 ({end_dt.strftime('%Y년 %m월')} 기준)")

    # budget status
    budget_view = biz_view[["customer_id", "account_name", "manager", "monthly_budget", "current_month_cost"]].copy()
    budget_view["monthly_budget_val"] = pd.to_numeric(budget_view.get("monthly_budget", 0), errors="coerce").fillna(0).astype(int)
    budget_view["current_month_cost_val"] = pd.to_numeric(budget_view.get("current_month_cost", 0), errors="coerce").fillna(0).astype(int)

    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = budget_view.loc[m2, "current_month_cost_val"] / budget_view.loc[m2, "monthly_budget_val"]
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
    budget_view["상태"] = tmp[0]
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

    budget_view_disp = budget_view.copy()
    budget_view_disp["월 예산(원)"] = budget_view_disp["monthly_budget_val"].map(format_number_commas)
    budget_view_disp[f"{end_dt.month}월 사용액"] = budget_view_disp["current_month_cost_val"].map(format_number_commas)
    budget_view_disp["집행률(%)"] = budget_view_disp["usage_pct"].map(lambda x: round(float(x), 1) if pd.notna(x) else 0.0)

    disp_cols = ["account_name", "manager", "월 예산(원)", f"{end_dt.month}월 사용액", "집행률(%)", "상태"]
    table_df = budget_view_disp[disp_cols].rename(columns={"account_name": "업체명", "manager": "담당자"}).copy()

    c1, c2 = st.columns([3, 1])
    with c1:
        st.dataframe(table_df, use_container_width=True, hide_index=True)
        render_download_compact(table_df, f"월예산_{f['start']}_{f['end']}", "monthly_budget", "mb")

    with c2:
        st.markdown(
            """
            <div class="panel" style="line-height:1.85; font-size:14px; background: rgba(235,238,242,0.75);">
              <b>상태 가이드</b><br><br>
              🟢 <b>적정</b> : 집행률 <b>90% 미만</b><br>
              🟡 <b>주의</b> : 집행률 <b>90% 이상</b><br>
              🔴 <b>초과</b> : 집행률 <b>100% 이상</b><br>
              ⚪ <b>미설정</b> : 월 예산 <b>0원</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ✅ 안정적인 폼 기반 업데이트 (data_editor 제거)
    st.markdown("#### ✍️ 월 예산 수정 (선택 → 입력 → 저장)")
    opts = budget_view_disp[["customer_id", "account_name"]].copy()
    opts["label"] = opts["account_name"].astype(str) + "  (" + opts["customer_id"].astype(str) + ")"
    labels = opts["label"].tolist()
    label_to_cid = dict(zip(opts["label"], opts["customer_id"].tolist()))

    with st.form("budget_update_form", clear_on_submit=False):
        sel = st.selectbox("업체 선택", labels, index=0 if labels else None, disabled=(len(labels) == 0))
        cur_budget = 0
        if labels:
            cid = int(label_to_cid.get(sel, 0))
            cur_budget = int(budget_view_disp.loc[budget_view_disp["customer_id"] == cid, "monthly_budget_val"].iloc[0])
        new_budget = st.text_input("새 월 예산(원) (예: 500000 또는 500,000)", value=format_number_commas(cur_budget) if labels else "0")
        submitted = st.form_submit_button("💾 저장", use_container_width=True)

    if submitted and labels:
        cid = int(label_to_cid.get(sel, 0))
        nb = parse_currency(new_budget)
        update_monthly_budget(engine, cid, nb)
        st.success("수정 완료. (캐시 갱신)")
        st.cache_data.clear()
        st.rerun()


def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.merge(meta[["customer_id", "account_name", "manager"]], on="customer_id", how="left")


def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터에서 **적용**을 눌러 조회를 시작하세요.")
        return

    st.markdown("## 🚀 성과 (캠페인)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_campaign", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return

    # -----------------------------
    # 📈 Trend (Altair)
    # -----------------------------
    try:
        ts = query_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum())
        total_clk = float(ts["clk"].sum())
        total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_camp_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_camp_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_camp_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_camp_roas")


        render_period_compare_panel(engine, "campaign", f["start"], f["end"], cids, type_sel, key_prefix="camp", expanded=False)


        render_period_compare_panel(engine, "keyword", f["start"], f["end"], cids, type_sel, key_prefix="kw", expanded=False)


        render_period_compare_panel(engine, "ad", f["start"], f["end"], cids, type_sel, key_prefix="ad", expanded=False)

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="camp_trend_metric",
        )

        ts2 = ts.copy()
        ts2 = add_rates(ts2)
        if metric_sel == "광고비":
            ch = _chart_timeseries(ts2, "cost", "광고비(원)", y_format=",.0f", height=260)
        elif metric_sel == "클릭":
            ch = _chart_timeseries(ts2, "clk", "클릭", y_format=",.0f", height=260)
        elif metric_sel == "전환":
            ch = _chart_timeseries(ts2, "conv", "전환", y_format=",.0f", height=260)
        else:
            sales_s = pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) if "sales" in ts2.columns else pd.Series([0.0]*len(ts2))
            ts2["roas"] = (sales_s / ts2["cost"].replace({0: pd.NA})) * 100
            ts2["roas"] = pd.to_numeric(ts2["roas"], errors="coerce").fillna(0)
            ch = _chart_timeseries(ts2, "roas", "ROAS(%)", y_format=",.0f", height=260)

        if ch is not None:
            st.altair_chart(ch, use_container_width=True)

        st.divider()

    df = _perf_common_merge_meta(bundle, meta)
    df = add_rates(df)

    # -----------------
    # TOP5 (비용/클릭/전환)
    # -----------------
    top_cost = df.sort_values("cost", ascending=False).head(5)
    top_clk = df.sort_values("clk", ascending=False).head(5)
    top_conv = df.sort_values("conv", ascending=False).head(5)

    def _fmt_top(dfx: pd.DataFrame, metric: str) -> pd.DataFrame:
        if dfx is None or dfx.empty:
            return pd.DataFrame(columns=["업체명", "광고유형", "캠페인", metric])
        x = dfx.copy()
        x["업체명"] = x.get("account_name", "")
        x["광고유형"] = x.get("campaign_type", "")
        x["캠페인"] = x.get("campaign_name", "")
        if metric == "광고비":
            x[metric] = x.get("cost", 0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else:
            x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)
        return x[["업체명", "광고유형", "캠페인", metric]]

    with st.expander("📌 성과별 TOP5 (캠페인)", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='camp_top5_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='camp_top5_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='camp_top5_conv', height=240)

    
    with st.expander("📊 캠페인 광고비 TOP10 그래프", expanded=False):
        chart_df = bundle[bundle["tag"].isin(["cost", "topn"])].copy() if "tag" in bundle.columns else bundle.copy()
        tmp = chart_df.sort_values("cost", ascending=False).head(10).copy()
        tmp["label"] = tmp.get("campaign_name", "").astype(str)
        ch = _chart_progress_bars(tmp, "label", "cost", "광고비(원)", top_n=10, height=320)
        if ch is not None:
            st.altair_chart(ch, use_container_width=True)
        else:
            st.info("그래프 표시 불가")

    st.divider()
    # -----------------
    # Main table (비용 TOP N)
    # -----------------
    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()

    disp = main_df.copy()
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

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict):
    if not f.get("ready", False):
        st.info("필터에서 **적용**을 눌러 조회를 시작하세요.")
        return

    st.markdown("## 🔎 성과 (키워드)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", []) or [])
    top_n = int(f.get("top_n_keyword", 300))

    bundle = query_keyword_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음")
        return

    # TOP10
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
            x[metric] = pd.to_numeric(x["cost"], errors="coerce").fillna(0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x["clk"], errors="coerce").fillna(0).astype(int).astype(str)
        else:
            x[metric] = pd.to_numeric(x["conv"], errors="coerce").fillna(0).astype(int).astype(str)
        return x.rename(columns={"account_name": "업체명", "keyword": "키워드"})[["업체명", "키워드", metric]]

    with st.expander("📌 성과별 TOP10 키워드", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP10")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='kw_top10_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP10")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='kw_top10_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP10")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='kw_top10_conv', height=240)

    
    with st.expander("📊 키워드 광고비 TOP10 그래프", expanded=False):
        tmp = bundle.sort_values("cost", ascending=False).head(10).copy()
        tmp["label"] = tmp.get("keyword", "").astype(str)
        ch = _chart_progress_bars(tmp, "label", "cost", "광고비(원)", top_n=10, height=320)
        if ch is not None:
            st.altair_chart(ch, use_container_width=True)
        else:
            st.info("그래프 표시 불가")

    st.divider()
    # Top N list (광고비 기준)
    df = bundle[bundle["rn_cost"] <= top_n].sort_values("rn_cost").copy()
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"]).copy()
    df["customer_id"] = df["customer_id"].astype("int64")

    df = add_rates(df)

    # -----------------------------
    # 📈 Trend (Altair)
    # -----------------------------
    try:
        ts = query_keyword_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum())
        total_clk = float(ts["clk"].sum())
        total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_kw_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_kw_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_kw_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_kw_roas")

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="kw_trend_metric",
        )

        ts2 = ts.copy()
        ts2 = add_rates(ts2)
        if metric_sel == "광고비":
            ch = _chart_timeseries(ts2, "cost", "광고비(원)", y_format=",.0f", height=260)
        elif metric_sel == "클릭":
            ch = _chart_timeseries(ts2, "clk", "클릭", y_format=",.0f", height=260)
        elif metric_sel == "전환":
            ch = _chart_timeseries(ts2, "conv", "전환", y_format=",.0f", height=260)
        else:
            sales_s = pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) if "sales" in ts2.columns else pd.Series([0.0]*len(ts2))
            ts2["roas"] = (sales_s / ts2["cost"].replace({0: pd.NA})) * 100
            ts2["roas"] = pd.to_numeric(ts2["roas"], errors="coerce").fillna(0)
            ch = _chart_timeseries(ts2, "roas", "ROAS(%)", y_format=",.0f", height=260)

        if ch is not None:
            st.altair_chart(ch, use_container_width=True)

        st.divider()

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

    view["비용"] = pd.to_numeric(view["비용"], errors="coerce").fillna(0).map(format_currency)
    view["CPC"] = pd.to_numeric(view["CPC"], errors="coerce").fillna(0).map(format_currency)
    view["CPA"] = pd.to_numeric(view["CPA"], errors="coerce").fillna(0).map(format_currency)
    view["매출"] = pd.to_numeric(view.get("매출", 0), errors="coerce").fillna(0).map(format_currency)
    view["ROAS(%)"] = view["ROAS(%)"].map(format_roas)
    view["CTR(%)"] = pd.to_numeric(view["CTR(%)"], errors="coerce").fillna(0).astype(float)
    view = finalize_ctr_col(view, "CTR(%)")

    cols = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹", "키워드", "노출", "클릭", "CTR(%)", "CPC", "비용", "전환", "CPA", "매출", "ROAS(%)"]
    out_df = view[cols].copy()
    out_df["노출"] = pd.to_numeric(out_df["노출"], errors="coerce").fillna(0).astype(int)
    out_df["클릭"] = pd.to_numeric(out_df["클릭"], errors="coerce").fillna(0).astype(int)
    out_df["전환"] = pd.to_numeric(out_df["전환"], errors="coerce").fillna(0).astype(int)

    st.dataframe(out_df, use_container_width=True, hide_index=True)
    render_download_compact(out_df, f"키워드성과_TOP{top_n}_{f['start']}_{f['end']}", "keyword", "kw")


def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        st.info("필터에서 **적용**을 눌러 조회를 시작하세요.")
        return

    st.markdown("## 🧩 성과 (소재)")
    st.caption(f"기간: {f['start']} ~ {f['end']}")

    top_n = int(f.get("top_n_ad", 200))
    cids = tuple(f.get("selected_customer_ids", []) or [])
    type_sel = tuple(f.get("type_sel", tuple()) or tuple())

    bundle = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=top_n, top_k=5)
    if bundle is None or bundle.empty:
        st.warning("데이터 없음 (dim_ad/dim_adgroup/dim_campaign 또는 fact_ad_daily 확인)")
        return

    df = _perf_common_merge_meta(bundle, meta)
    df = add_rates(df)

    # -----------------------------
    # 📈 Trend (Altair)
    # -----------------------------
    try:
        ts = query_ad_timeseries(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        ts = pd.DataFrame()

    if ts is not None and not ts.empty:
        total_cost = float(ts["cost"].sum())
        total_clk = float(ts["clk"].sum())
        total_conv = float(ts["conv"].sum())
        total_sales = float(ts.get("sales", 0).sum()) if "sales" in ts.columns else 0.0
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0

        st.markdown("### 📈 기간 추세")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui_metric_or_stmetric("총 광고비", format_currency(total_cost), "선택 기간 합계", key="kpi_ad_cost")
        with k2:
            ui_metric_or_stmetric("총 클릭", format_number_commas(total_clk), "선택 기간 합계", key="kpi_ad_clk")
        with k3:
            ui_metric_or_stmetric("총 전환", format_number_commas(total_conv), "선택 기간 합계", key="kpi_ad_conv")
        with k4:
            ui_metric_or_stmetric("총 ROAS", f"{total_roas:.0f}%", "매출/광고비", key="kpi_ad_roas")

        metric_sel = st.radio(
            "트렌드 지표",
            ["광고비", "클릭", "전환", "ROAS"],
            horizontal=True,
            index=0,
            key="ad_trend_metric",
        )

        ts2 = ts.copy()
        ts2 = add_rates(ts2)
        if metric_sel == "광고비":
            ch = _chart_timeseries(ts2, "cost", "광고비(원)", y_format=",.0f", height=260)
        elif metric_sel == "클릭":
            ch = _chart_timeseries(ts2, "clk", "클릭", y_format=",.0f", height=260)
        elif metric_sel == "전환":
            ch = _chart_timeseries(ts2, "conv", "전환", y_format=",.0f", height=260)
        else:
            sales_s = pd.to_numeric(ts2["sales"], errors="coerce").fillna(0) if "sales" in ts2.columns else pd.Series([0.0]*len(ts2))
            ts2["roas"] = (sales_s / ts2["cost"].replace({0: pd.NA})) * 100
            ts2["roas"] = pd.to_numeric(ts2["roas"], errors="coerce").fillna(0)
            ch = _chart_timeseries(ts2, "roas", "ROAS(%)", y_format=",.0f", height=260)

        if ch is not None:
            st.altair_chart(ch, use_container_width=True)

        st.divider()


    # -----------------
    # TOP5 (비용/클릭/전환)
    # -----------------
    top_cost = df.sort_values("cost", ascending=False).head(5)
    top_clk = df.sort_values("clk", ascending=False).head(5)
    top_conv = df.sort_values("conv", ascending=False).head(5)

    def _fmt_top(dfx: pd.DataFrame, metric: str) -> pd.DataFrame:
        if dfx is None or dfx.empty:
            return pd.DataFrame(columns=["업체명", "캠페인", "소재내용", metric])
        x = dfx.copy()
        x["업체명"] = x.get("account_name", "")
        x["캠페인"] = x.get("campaign_name", "")
        x["소재내용"] = x.get("ad_name", "")
        if metric == "광고비":
            x[metric] = x.get("cost", 0).map(format_currency)
        elif metric == "클릭":
            x[metric] = pd.to_numeric(x.get("clk", 0), errors="coerce").fillna(0).astype(int)
        else:
            x[metric] = pd.to_numeric(x.get("conv", 0), errors="coerce").fillna(0).astype(int)
        return x[["업체명", "캠페인", "소재내용", metric]]

    with st.expander("📌 성과별 TOP5 (소재)", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 💸 광고비 TOP5")
            ui_table_or_dataframe(_fmt_top(top_cost, "광고비"), key='ad_top5_cost', height=240)
        with c2:
            st.markdown("#### 🖱️ 클릭 TOP5")
            ui_table_or_dataframe(_fmt_top(top_clk, "클릭"), key='ad_top5_clk', height=240)
        with c3:
            st.markdown("#### ✅ 전환 TOP5")
            ui_table_or_dataframe(_fmt_top(top_conv, "전환"), key='ad_top5_conv', height=240)

    
    with st.expander("📊 소재 광고비 TOP10 그래프", expanded=False):
        chart_df = bundle[bundle["tag"].isin(["cost", "topn"])].copy() if "tag" in bundle.columns else bundle.copy()
        tmp = chart_df.sort_values("cost", ascending=False).head(10).copy()
        tmp["label"] = tmp.get("ad_name", "").astype(str)
        ch = _chart_progress_bars(tmp, "label", "cost", "광고비(원)", top_n=10, height=320)
        if ch is not None:
            st.altair_chart(ch, use_container_width=True)
        else:
            st.info("그래프 표시 불가")

    st.divider()
    # -----------------
    # Main table (비용 TOP N)
    # -----------------
    main_df = df.sort_values("cost", ascending=False).head(top_n).copy()

    disp = main_df.copy()
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
    st.markdown("## ⚙️ 설정 / 연결")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🧹 캐시 비우기", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.session_state.pop("_table_cols_cache", None)
            st.session_state.pop("_table_names_cache", None)
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

    st.divider()

    st.markdown("### 🚀 속도 튜닝 (권장 인덱스)")
    st.caption("최초 1회만 실행하면 이후 TOPN/기간 조회가 확 빨라집니다. (권한/정책에 따라 실패할 수 있음)")

    def _create_perf_indexes(_engine) -> List[str]:
        stmts = [
            # FACT (기간+CID+ID) — 조회/집계에 가장 영향 큼
            "CREATE INDEX IF NOT EXISTS idx_f_campaign_dt_cid_txt_camp ON fact_campaign_daily (dt, (customer_id::text), campaign_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_keyword_dt_cid_txt_kw   ON fact_keyword_daily (dt, (customer_id::text), keyword_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_ad_dt_cid_txt_ad        ON fact_ad_daily      (dt, (customer_id::text), ad_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_biz_dt_cid_txt          ON fact_bizmoney_daily(dt, (customer_id::text));",
            # DIM (조인 경로)
            "CREATE INDEX IF NOT EXISTS idx_d_campaign_cid_txt_camp   ON dim_campaign ((customer_id::text), campaign_id, campaign_tp);",
            "CREATE INDEX IF NOT EXISTS idx_d_adgroup_cid_txt_adg     ON dim_adgroup  ((customer_id::text), adgroup_id, campaign_id);",
            "CREATE INDEX IF NOT EXISTS idx_d_keyword_cid_txt_kw      ON dim_keyword  ((customer_id::text), keyword_id, adgroup_id);",
            "CREATE INDEX IF NOT EXISTS idx_d_ad_cid_txt_ad           ON dim_ad       ((customer_id::text), ad_id, adgroup_id);",
        ]
        results: List[str] = []
        with _engine.begin() as conn:
            for s in stmts:
                try:
                    conn.execute(text(s))
                    results.append(f"✅ {s}")
                except Exception as e:
                    results.append(f"⚠️ {s}  -> {e}")
        return results

    if st.button("⚡ 인덱스 생성 실행", use_container_width=True):
        try:
            logs = _create_perf_indexes(engine)
            for line in logs:
                st.write(line)
            st.success("완료! 캐시 비우고 다시 조회해보세요.")
        except Exception as e:
            st.error(f"실패: {e}")

    st.divider()

    st.markdown("### accounts.xlsx → DB 동기화 (수동)")
    st.caption(f"경로: {ACCOUNTS_XLSX}")

    if st.button("🔁 동기화 실행", use_container_width=True):
        try:
            res = seed_from_accounts_xlsx(engine)
            st.success(f"완료: meta {res.get('meta', 0)}건")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"동기화 실패: {e}")


# -----------------------------
# Main
# -----------------------------
def main():
    render_hero()

    try:
        engine = get_engine()
    except Exception as e:
        st.error(str(e))
        return

    render_data_freshness(engine)

    meta = get_meta(engine)
    if meta is None or meta.empty:
        st.error("dim_account_meta가 비어있습니다. 설정/연결에서 accounts.xlsx 동기화를 먼저 해주세요.")
        return

    dim_campaign = load_dim_campaign(engine)
    type_opts = get_campaign_type_options(dim_campaign)

    f = build_filters(meta, type_opts)

    if not f.get('ready', False):
        st.info("필터에서 **적용**을 누르면 조회가 시작됩니다. (초기 로딩 속도 개선)")

    pages = ["전체 예산/잔액 관리", "성과(캠페인)", "성과(키워드)", "성과(소재)", "설정/연결"]
    default_page = st.session_state.get('nav_page', pages[0])
    if default_page not in pages:
        default_page = pages[0]
    if HAS_SHADCN_UI and ui is not None:
        try:
            page = ui.tabs(options=pages, default_value=default_page, key='nav_tabs')
        except Exception:
            page = st.selectbox('메뉴', pages, index=pages.index(default_page), key='nav_select')
    else:
        page = st.selectbox('메뉴', pages, index=pages.index(default_page), key='nav_select')
    st.session_state['nav_page'] = page
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
