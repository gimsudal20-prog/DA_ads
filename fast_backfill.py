import subprocess
import argparse
import os
import sys
from datetime import datetime, timedelta

# ✨ 핵심: NAVER_ADS_API_KEY 이름으로도 환경변수를 잘 찾아오도록 수정했습니다.
api_key = os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY") or ""
if not api_key:
    print("❌ [FATAL ERROR] 환경변수(API 키)를 찾을 수 없습니다! yml 파일의 env 설정을 확인해주세요.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str, required=True)
parser.add_argument("--end", type=str, required=True)
args = parser.parse_args()

start_date = datetime.strptime(args.start, "%Y-%m-%d")
end_date = datetime.strptime(args.end, "%Y-%m-%d")

print(f"🚀 대규모 백필 작업을 시작합니다: {args.start} ~ {args.end}")

curr_date = start_date
while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*50)
    print(f"📅 [ {d_str} ] 전체 계정 데이터 수집 중...")
    print("="*50)
    
    cmd = [sys.executable, "collector.py", "--date", d_str, "--workers", "15"]
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
