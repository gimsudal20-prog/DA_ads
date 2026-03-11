# -*- coding: utf-8 -*-
"""backfill.py - 특정 계정/기간 대상 과거 데이터 수집(백필) 스크립트"""

import subprocess
import time
from datetime import datetime, timedelta

def run_backfill(start_date_str: str, end_date_str: str, customer_id: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        print(f"\n{'='*60}")
        print(f"🔄 [백필 진행 중] 대상 날짜: {date_str} | 대상 계정: {customer_id}")
        print(f"{'='*60}")
        
        # collector.py를 subprocess로 호출하여 해당 날짜의 데이터를 수집
        # 구조(dim) 데이터 수집을 생략해 속도를 높이려면 cmd 배열에 "--skip_dim"을 추가하셔도 됩니다.
        cmd = [
            "python", "collector.py", 
            "--date", date_str, 
            "--customer_id", str(customer_id)
        ]
        
        try:
            # collector.py 실행 및 완료 대기
            result = subprocess.run(cmd)
            
            if result.returncode != 0:
                print(f"⚠️ {date_str} 데이터 수집 중 오류가 발생했습니다. (에러 코드: {result.returncode})")
                # 에러 발생 시 3초 대기 후 다음 날짜로 넘어감
                time.sleep(3)
            
        except KeyboardInterrupt:
            print("\n🛑 사용자에 의해 백필 작업이 강제 중단되었습니다.")
            break
        except Exception as e:
            print(f"❌ 실행 중 예외 발생: {e}")
            
        # 다음 날짜로 +1일 이동
        current_date += timedelta(days=1)

if __name__ == "__main__":
    # 💡 백필을 원하는 시작일, 종료일, 커스텀ID를 지정하세요. (현재 연도에 맞게 수정)
    TARGET_START = "2026-01-01" 
    TARGET_END = "2026-03-10"
    TARGET_CUSTOMER_ID = "406051"
    
    print(f"🚀 지정된 계정({TARGET_CUSTOMER_ID})의 과거 데이터 백필을 시작합니다.")
    print(f"🗓️ 전체 기간: {TARGET_START} ~ {TARGET_END}")
    
    # 실행
    run_backfill(TARGET_START, TARGET_END, TARGET_CUSTOMER_ID)
    
    print("\n✅ 모든 백필 작업이 완료되었습니다!")
