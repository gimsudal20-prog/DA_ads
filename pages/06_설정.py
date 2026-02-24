import os
import time
import pandas as pd
import streamlit as st
from sqlalchemy import text
from utils import init_page
from database import get_engine, sql_read, sql_exec, table_exists, get_meta

# 환경 변수 및 파일 경로 설정
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ACCOUNTS_XLSX = os.environ.get("ACCOUNTS_XLSX", os.path.join(APP_DIR, "accounts.xlsx"))

def db_ping(engine, retries=2):
    """데이터베이스 연결 상태를 가볍게 확인합니다."""
    for i in range(retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            if i < retries: 
                time.sleep(0.3)
            else: 
                raise e

def normalize_accounts_columns(df: pd.DataFrame) -> pd.DataFrame:
    """업로드된 엑셀의 컬럼명을 표준 형식으로 변환합니다."""
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    
    def find_col(cands):
        for c in df.columns:
            normalized_c = c.lower().replace(" ", "").replace("_", "")
            if normalized_c in [cand.lower().replace(" ", "").replace("_", "") for cand in cands]:
                return c
        return None

    cid_col = find_col(["customer_id", "customerid", "커스텀id", "커스텀ID"])
    name_col = find_col(["account_name", "accountname", "업체명", "업체"])
    mgr_col = find_col(["manager", "담당자", "담당"])

    if not cid_col or not name_col:
        raise ValueError(f"accounts.xlsx에 필수 컬럼(고객ID, 업체명)이 없습니다. 현재 컬럼: {list(df.columns)}")

    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(df[cid_col], errors="coerce").astype("Int64")
    out["account_name"] = df[name_col].astype(str).str.strip()
    out["manager"] = df[mgr_col].astype(str).str.strip() if mgr_col else ""
    
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")
    return out.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)

def seed_from_accounts_xlsx(engine, df: pd.DataFrame = None):
    """엑셀 데이터를 DB의 dim_account_meta 테이블에 동기화합니다."""
    sql_exec(engine, """
        CREATE TABLE IF NOT EXISTS dim_account_meta (
          customer_id BIGINT PRIMARY KEY,
          account_name TEXT NOT NULL, 
          manager TEXT DEFAULT '',
          monthly_budget BIGINT DEFAULT 0, 
          updated_at TIMESTAMPTZ DEFAULT now()
        );
    """)

    if df is None:
        if not os.path.exists(ACCOUNTS_XLSX): 
            return 0
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
    return len(acc)

# --- 메인 렌더링 영역 ---
init_page()
st.markdown("## ⚙️ 설정 / 연결")

engine = get_engine()

# 1. DB 상태 확인
try:
    db_ping(engine)
    st.success("DB 연결 상태: 정상 ✅")
except Exception as e:
    st.error(f"DB 연결 실패: {e}")
    st.stop()

# 2. 계정 동기화 섹션
st.markdown("### 📌 계정 동기화 (accounts.xlsx → DB)")
st.caption("신규 업체 추가나 담당자 변경 시 동기화가 필요합니다.")

repo_exists = os.path.exists(ACCOUNTS_XLSX)
st.caption(f"기본 파일 경로: `{ACCOUNTS_XLSX}` {'✅ (확인됨)' if repo_exists else '❌ (파일 없음)'}")

up = st.file_uploader("직접 엑셀 업로드 (선택사항)", type=["xlsx"])

c1, c2, c3 = st.columns([1.2, 1.0, 2.2], gap="small")
with c1:
    if st.button("🔁 동기화 실행", use_container_width=True):
        try:
            df_src = pd.read_excel(up) if up else None
            cnt = seed_from_accounts_xlsx(engine, df_src)
            st.success(f"✅ 동기화 완료! ({cnt}개 계정 업데이트됨)")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"동기화 중 오류 발생: {e}")
with c2:
    if st.button("🧹 전체 캐시 비우기", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("캐시를 모두 비웠습니다!")
        st.rerun()
with c3:
    st.caption("데이터가 최신이 아니거나 필터가 이상할 때 클릭하세요.")

st.divider()

# 3. 등록 계정 목록 미리보기
st.markdown("### 🔎 등록된 계정 목록 (dim_account_meta)")
if table_exists(engine, "dim_account_meta"):
    df_meta = sql_read(engine, "SELECT customer_id, account_name, manager, monthly_budget FROM dim_account_meta ORDER BY account_name")
    if not df_meta.empty:
        st.write(f"총 **{len(df_meta)}**개의 계정이 등록되어 있습니다.")
        st.dataframe(df_meta, use_container_width=True, height=300)
    else:
        st.warning("등록된 계정이 없습니다. 동기화를 진행해 주세요.")
else:
    st.warning("계정 테이블이 아직 없습니다. '동기화 실행'을 눌러 테이블을 생성하세요.")

st.divider()

# 4. 성능 튜닝 (인덱스 생성)
with st.expander("⚡ 데이터베이스 인덱스 튜닝 (조회 속도 개선)", expanded=False):
    st.info("데이터 양이 많아져 조회가 느려진 경우 실행하세요. 쿼리 속도가 향상됩니다.")
    if st.button("🚀 인덱스 생성 실행"):
        stmts = [
            "CREATE INDEX IF NOT EXISTS idx_f_campaign_dt_cid ON fact_campaign_daily (dt, (customer_id::text), campaign_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_keyword_dt_cid ON fact_keyword_daily (dt, (customer_id::text), keyword_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_ad_dt_cid ON fact_ad_daily (dt, (customer_id::text), ad_id);",
            "CREATE INDEX IF NOT EXISTS idx_f_biz_dt_cid ON fact_bizmoney_daily(dt, (customer_id::text));"
        ]
        with engine.begin() as conn:
            for s in stmts:
                try:
                    conn.execute(text(s))
                    st.write(f"✅ 완료: `{s.split(' ON ')[0]}`")
                except Exception as e:
                    st.write(f"⚠️ 경고: {e}")
        st.success("인덱스 점검 및 생성이 완료되었습니다.")
