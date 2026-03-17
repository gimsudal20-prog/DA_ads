# -*- coding: utf-8 -*-
"""fast_backfill.py - 과거 데이터 구매완료 기준 재수집 및 롤링 수집 스크립트"""

import subprocess
import argparse
import os
import sys
from datetime import datetime, timedelta, date

api_key = os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY") or ""
if not api_key:
    print("❌ [FATAL ERROR] 환경변수(API 키)를 찾을 수 없습니다! yml 파일의 env 설정을 확인해주세요.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str, required=False, help="시작일 (YYYY-MM-DD)")
parser.add_argument("--end", type=str, required=False, help="종료일 (YYYY-MM-DD)")
args = parser.parse_args()

# 날짜 인자가 없으면 자동으로 최근 7일(D-7 ~ D-1) 설정
if not args.start or not args.end:
    today = date.today()
    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=7)
    print("ℹ️ 날짜 인자가 없습니다. 자동으로 '최근 7일 순수 구매완료 업데이트 모드'로 작동합니다.")
else:
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

print(f"🚀 [구매완료 필터링 적용] 백필 작업을 시작합니다: {start_date} ~ {end_date}")

curr_date = start_date
is_first_run = True

while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*60)
    print(f"📅 [ {d_str} ] 데이터 수집 및 '구매완료' 성과 매핑 진행 ⚡")
    print("="*60)
    
    # 1. 네이버 검색광고(SA) 수집 실행
    # (collector.py가 CONVERSION 리포트를 가져와서 구매완료만 발라내도록 업데이트됨)
    cmd_sa = [sys.executable, "collector.py", "--date", d_str, "--workers", "8"]
    
    if not is_first_run:
        # 백필 속도를 위해 첫날 이후에는 구조(이미지/이름) 수집은 건너뛰고 '통계'에 집중
        cmd_sa.append("--skip_dim")
        print("   ▶ [검색광고] 통계 데이터 업데이트 중 (뼈대수집 스킵)...")
    else:
        print("   ▶ [검색광고] 최신 구조 및 통계 데이터 동기화 중...")
    
    try:
        # 수집기 실행 (이 과정에서 구매완료 필터링이 자동으로 일어남)
        subprocess.run(cmd_sa, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [검색광고] {d_str} 수집 중 오류 발생 (건너뛰고 다음 날짜 진행)")

    # 2. 쇼핑 확장소재 수집
    print("   ▶ [확장소재] 수집 진행 중...")
    cmd_ext = [sys.executable, "collector_shop_ext.py", "--date", d_str]
    try:
        subprocess.run(cmd_ext, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [확장소재] {d_str} 수집 중 오류 발생")

    # 3. GFA 수집
    print("   ▶ [GFA] 수집 진행 중...")
    cmd_gfa = [sys.executable, "collector_gfa.py", "--date", d_str]
    try:
        subprocess.run(cmd_gfa, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [GFA] {d_str} 수집 중 오류 발생")

    is_first_run = False  
    curr_date += timedelta(days=1)

print("\n" + "★"*30)
print("🎉 지정된 기간의 순수 구매완료 데이터 백업이 모두 완료되었습니다!")
print("★" * 30)
