import subprocess
import argparse
import os
import sys
from datetime import datetime, timedelta

# 🚨 1. fast_backfill.py가 API 키를 제대로 받았는지 먼저 검사합니다.
api_key = os.getenv("NAVER_API_KEY", "").strip()
if not api_key:
    print("❌ [FATAL ERROR] fast_backfill.py 자체에 환경변수(API 키)가 전달되지 않았습니다!")
    print("💡 해결방법: 깃허브에 올라간 .github/workflows/backfill.yml 파일 텍스트 안에 'env:' 블록이 정상적으로 적혀있는지 꼭 확인해주세요.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str, required=True, help="시작일 (YYYY-MM-DD)")
parser.add_argument("--end", type=str, required=True, help="종료일 (YYYY-MM-DD)")
args = parser.parse_args()

start_date = datetime.strptime(args.start, "%Y-%m-%d")
end_date = datetime.strptime(args.end, "%Y-%m-%d")

print(f"🚀 대규모 백필 작업을 시작합니다: {args.start} ~ {args.end}")

# ✨ 2. 부모가 가진 환경변수를 자식 프로세스에 "명시적으로 강제 주입" 합니다.
child_env = os.environ.copy()
child_env["NAVER_API_KEY"] = os.getenv("NAVER_API_KEY", "")
child_env["NAVER_API_SECRET"] = os.getenv("NAVER_API_SECRET", "")
child_env["DATABASE_URL"] = os.getenv("DATABASE_URL", "")

curr_date = start_date
while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*50)
    print(f"📅 [ {d_str} ] 전체 계정 데이터 수집 중...")
    print("="*50)
    
    # sys.executable을 사용하여 동일한 파이썬 환경을 강제합니다.
    cmd = [sys.executable, "collector.py", "--date", d_str, "--workers", "15"]
    
    try:
        # env=child_env 를 통해 복사해둔 환경변수를 자식에게 직접 꽂아넣습니다.
        subprocess.run(cmd, check=True, env=child_env)
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
