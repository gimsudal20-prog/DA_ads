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
is_first_run = True  # ✨ 첫 실행 여부를 확인하는 변수 추가

while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*50)
    print(f"📅 [ {d_str} ] 대용량 통계 리포트 수집 진행 ⚡")
    
    # ✨ 핵심 수정: 첫 날짜에는 뼈대(이미지/상품명)를 확실하게 수집하고, 둘째 날부터 스킵합니다.
    cmd = [sys.executable, "collector.py", "--date", d_str, "--workers", "8"]
    
    if not is_first_run:
        cmd.append("--skip_dim")
        print("   ▶ (이후 날짜는 빠른 수집을 위해 뼈대 수집 스킵 옵션 적용)")
    else:
        print("   ▶ [첫 실행] 최신 소재 이미지 및 노출용 상품명 뼈대 동기화 진행 중...")
    
    try:
        subprocess.run(cmd, check=True)
        is_first_run = False  # 성공적으로 끝났으면 다음 반복부터는 무조건 스킵
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
