# -*- coding: utf-8 -*-
"""fast_backfill.py - 과거 데이터 수집 및 매일 새벽 최근 7일치 롤링 수집 스크립트"""

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
# ✨ 수정 1: start와 end를 '필수(required=True)'에서 '선택'으로 변경
parser.add_argument("--start", type=str, required=False, help="시작일 (YYYY-MM-DD)")
parser.add_argument("--end", type=str, required=False, help="종료일 (YYYY-MM-DD)")
args = parser.parse_args()

# ✨ 수정 2: 날짜를 입력하지 않으면 자동으로 '최근 7일(D-7 ~ D-1)'로 설정
if not args.start or not args.end:
    today = date.today()
    end_date = today - timedelta(days=1)   # 어제 (D-1)
    start_date = today - timedelta(days=7) # 7일 전 (D-7)
    print("ℹ️ 날짜 인자가 없습니다. 자동으로 '최근 7일 자동 수집 모드'로 작동합니다.")
else:
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

print(f"🚀 대규모 백필 작업을 시작합니다: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

curr_date = start_date
is_first_run = True

while curr_date <= end_date:
    d_str = curr_date.strftime("%Y-%m-%d")
    print(f"\n" + "="*50)
    print(f"📅 [ {d_str} ] 데이터 수집 진행 ⚡")
    
    # 1. 네이버 검색광고(SA) 수집
    cmd_sa = [sys.executable, "collector.py", "--date", d_str, "--workers", "8"]
    if not is_first_run:
        cmd_sa.append("--skip_dim")
        print("   ▶ [검색광고] 빠른 수집을 위해 뼈대 수집 스킵 옵션 적용")
    else:
        print("   ▶ [검색광고] [첫 실행] 최신 소재 이미지 및 노출용 상품명 뼈대 동기화 진행 중...")
    
    try:
        subprocess.run(cmd_sa, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [검색광고] {d_str} 수집 중 일부 오류 발생")

    # 2. 쇼핑 확장소재 수집 추가
    print("   ▶ [확장소재] 수집 진행 중...")
    cmd_ext = [sys.executable, "collector_shop_ext.py", "--date", d_str]
    try:
        subprocess.run(cmd_ext, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [확장소재] {d_str} 수집 중 오류 발생")

    # 3. GFA 수집 추가
    print("   ▶ [GFA] 수집 진행 중...")
    cmd_gfa = [sys.executable, "collector_gfa.py", "--date", d_str]
    try:
        subprocess.run(cmd_gfa, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [GFA] {d_str} 수집 중 오류 발생")

    is_first_run = False  
    curr_date += timedelta(days=1)

print("\n🎉 지정된 기간의 모든 데이터 수집이 완벽하게 끝났습니다!")
