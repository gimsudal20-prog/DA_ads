import subprocess
import argparse
import os
from datetime import datetime, timedelta

# ✨ 깡통 .env 파일이 깃허브 Secrets를 지우지 못하도록 override=False 로 변경!
try:
    from dotenv import load_dotenv
    load_dotenv(override=False) 
except ImportError:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str, required=True, help="시작일 (YYYY-MM-DD)")
parser.add_argument("--end", type=str, required=True, help="종료일 (YYYY-MM-DD)")
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
    
    cmd = ["python", "collector.py", "--date", d_str, "--workers", "15"]
    
    try:
        # 안전하게 환경변수 복사 후 전달
        env_vars = os.environ.copy()
        subprocess.run(cmd, check=True, env=env_vars)
    except subprocess.CalledProcessError:
        print(f"⚠️ {d_str} 수집 중 일부 오류 발생 (건너뛰고 다음 날짜 진행)")
        
    curr_date += timedelta(days=1)

print("\n🎉 모든 기간의 데이터 수집이 완벽하게 끝났습니다!")
