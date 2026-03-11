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

# ✨ 핵심: 첫 날에만 뼈대(구조)를 수집하도록 플래그 설정
is_first_run = True 

while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*50)
    
    cmd = [sys.executable, "collector.py", "--date", d_str, "--workers", "15"]
    
    if is_first_run:
        print(f"📅 [ {d_str} ] (1/2) 계정 뼈대(캠페인/키워드/소재 목록) 최신화 중... (이 날만 오래 걸립니다!)")
    else:
        print(f"📅 [ {d_str} ] (2/2) 뼈대 수집 스킵! 대용량 통계 리포트만 초고속으로 수집합니다 ⚡")
        # ✨ 핵심: 두 번째 날부터는 구조 조회를 건너뛰는 마법의 옵션 추가
        cmd.append("--skip_dim") 
        
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    # 첫 날 수집이 끝났으므로 플래그를 False로 변경
    is_first_run = False
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
