import subprocess
import argparse
import os
import sys
from datetime import datetime, timedelta

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
    print(f"📅 [ {d_str} ] 대용량 통계 리포트 초고속 수집 진행 ⚡")
    
    # ✨ 우회 로직이 제거되었기 때문에 8명으로 세팅해도 네이버 차단 없이 가장 빠릅니다. (무조건 뼈대수집은 스킵)
    cmd = [sys.executable, "collector.py", "--date", d_str, "--workers", "8", "--skip_dim"]
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
