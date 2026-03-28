from sqlalchemy.pool import NullPool # ✨ 상단에 이 임포트를 추가해주세요 (필수)

# ==========================================
# 1. Database Connection (NullPool 적용)
# ==========================================
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return create_engine("sqlite:///:memory:", future=True)
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"

    connect_args = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5
    }

    # ✨ 수정됨: NullPool 적용
    # 커넥션을 쥐고 있지 않고 쿼리가 끝날 때마다 완전히 끊습니다.
    # 밤새 유휴 상태로 인한 끊김, 커넥션 초과(Too many connections) 에러를 원천 차단합니다.
    return create_engine(
        db_url,
        poolclass=NullPool,
        connect_args=connect_args,
        future=True
    )

# ... (중략: db_ping, table_exists, get_table_columns 함수는 그대로 유지) ...

@st.cache_data(ttl=600, max_entries=30, show_spinner=False)
def sql_read(_engine, query: str, params: dict = None) -> pd.DataFrame:
    last_error = None
    for attempt in range(3):
        try:
            with _engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            last_error = e
            time.sleep(1.0) # 재시도 대기
            
    # 3번 모두 실패 시 캐시를 날리고 에러 메세지를 띄웁니다.
    st.cache_resource.clear()
    st.error(f"DB 연결이 지연되고 있습니다. 잠시 후 새로고침(F5) 해주세요. (사유: {last_error})")
    st.stop()

def sql_exec(_engine, query: str, params: dict = None) -> None:
    last_error = None
    for attempt in range(3):
        try:
            with _engine.begin() as conn:
                conn.execute(text(query), params or {})
            return # 성공 시 바로 종료
        except Exception as e:
            last_error = e
            time.sleep(1.0)
            
    st.cache_resource.clear()
    raise RuntimeError(f"쿼리 실행 실패 (사유: {last_error})")
