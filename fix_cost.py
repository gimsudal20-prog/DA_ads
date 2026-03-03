# fix_cost.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# DB 연결
db_url = os.getenv("DATABASE_URL")
if "sslmode=" not in db_url:
    db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
engine = create_engine(db_url)

# 비용을 1.1로 나누고, 그에 맞춰 ROAS를 다시 계산하는 SQL 쿼리
queries = [
    """
    UPDATE fact_campaign_daily 
    SET cost = ROUND(cost / 1.1), 
        roas = CASE WHEN ROUND(cost / 1.1) > 0 THEN (sales / ROUND(cost / 1.1)) * 100 ELSE 0 END;
    """,
    """
    UPDATE fact_keyword_daily 
    SET cost = ROUND(cost / 1.1), 
        roas = CASE WHEN ROUND(cost / 1.1) > 0 THEN (sales / ROUND(cost / 1.1)) * 100 ELSE 0 END;
    """,
    """
    UPDATE fact_ad_daily 
    SET cost = ROUND(cost / 1.1), 
        roas = CASE WHEN ROUND(cost / 1.1) > 0 THEN (sales / ROUND(cost / 1.1)) * 100 ELSE 0 END;
    """
]

print("🛠️ 과거 데이터 부가세 제거 및 ROAS 재계산 중...")
with engine.begin() as conn:
    for q in queries:
        conn.execute(text(q))
        
print("✨ 완벽하게 복구되었습니다! 이제 대시보드를 새로고침 해보세요.")
