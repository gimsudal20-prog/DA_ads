@st.cache_data(ttl=600, show_spinner=False)
def get_recent_avg_cost(_engine, d1: date, d2: date, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """
    최근 평균 소진(일 평균).
    
    ✅ 수정 사항 (v7.0.3 Fix):
    - SQL 파라미터 바인딩 오류(ProgrammingError)를 원천 차단하기 위해
      DB 쿼리 단계에서는 '기간(date)'만 필터링합니다.
    - customer_ids 리스트 필터링은 데이터를 가져온 후 Pandas에서 수행합니다.
    """
    # 테이블 존재 여부 확인
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    if d2 < d1:
        d1 = d2

    # 기간 일수 (최소 1일)
    days = max((d2 - d1).days + 1, 1)

    # 1. SQL 실행: 복잡한 리스트 바인딩 없이 날짜로만 조회 (안전성 확보)
    sql = """
    SELECT customer_id, SUM(cost) AS sum_cost
    FROM fact_campaign_daily
    WHERE dt BETWEEN :d1 AND :d2
    GROUP BY customer_id
    """
    params = {"d1": str(d1), "d2": str(d2"}

    try:
        # sql_read 헬퍼 함수 재사용
        tmp = sql_read(_engine, sql, params)
    except Exception as e:
        # 쿼리 실패 시 빈 DF 반환 (앱 중단 방지)
        print(f"Error in get_recent_avg_cost: {e}")
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    if tmp is None or tmp.empty:
        return pd.DataFrame(columns=["customer_id", "avg_cost"])

    # 2. 데이터 타입 정리
    tmp["customer_id"] = pd.to_numeric(tmp["customer_id"], errors="coerce").astype("Int64")
    tmp = tmp.dropna(subset=["customer_id"]).copy()
    tmp["customer_id"] = tmp["customer_id"].astype("int64")

    # 3. Pandas에서 customer_id 필터링 수행
    if customer_ids:
        try:
            # 비교를 위해 입력받은 ID 리스트도 정수형으로 변환
            target_ids = [int(x) for x in customer_ids if str(x).strip() != ""]
            if target_ids:
                tmp = tmp[tmp["customer_id"].isin(target_ids)].copy()
        except Exception:
            # 변환 오류 시 필터링 없이 진행하거나 빈 값 반환 (여기서는 안전하게 진행)
            pass

    # 4. 일 평균 비용 계산
    tmp["avg_cost"] = pd.to_numeric(tmp["sum_cost"], errors="coerce").fillna(0).astype(float) / float(days)
    
    return tmp[["customer_id", "avg_cost"]]
