# -*- coding: utf-8 -*-
"""fast_backfill.py - 날짜 범위 백필 및 선택 계정 대상 수집 실행 스크립트

변경 사항
- --account_name / --account_names 지원
- --workers 지원
- 테스트용 --sa_only, --skip_ext, --skip_gfa 지원
- 로그 문구를 현재 collector 동작(총합 수집 중심)에 맞게 정리
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import List


def run_cmd(cmd: List[str], label: str, day: str) -> None:
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [{label}] {day} 수집 중 오류 발생", flush=True)


def main() -> None:
    api_key = os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY") or ""
    if not api_key:
        print("❌ [FATAL ERROR] 환경변수(API 키)를 찾을 수 없습니다! yml 파일의 env 설정을 확인해주세요.", flush=True)
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, required=False, help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=False, help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=8, help="collector.py 동시 작업 수")
    parser.add_argument("--account_name", type=str, default="", help="정확/부분일치로 테스트할 단일 업체명")
    parser.add_argument("--account_names", type=str, default="", help="쉼표(,)로 구분한 여러 업체명")
    parser.add_argument("--sa_only", action="store_true", help="검색광고(SA)만 수집")
    parser.add_argument("--skip_ext", action="store_true", help="쇼핑 확장소재 수집 스킵")
    parser.add_argument("--skip_gfa", action="store_true", help="GFA 수집 스킵")
    args = parser.parse_args()

    if not args.start or not args.end:
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = today - timedelta(days=7)
        print("ℹ️ 날짜 인자가 없습니다. 자동으로 최근 7일(D-7 ~ D-1) 백필 모드로 작동합니다.", flush=True)
    else:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    if start_date > end_date:
        print("❌ [FATAL ERROR] 시작일이 종료일보다 늦습니다.", flush=True)
        sys.exit(1)

    print(f"🚀 백필 작업 시작: {start_date} ~ {end_date}", flush=True)
    if args.account_name.strip():
        print(f"🎯 단일 업체 필터: {args.account_name.strip()}", flush=True)
    if args.account_names.strip():
        print(f"🎯 다중 업체 필터: {args.account_names.strip()}", flush=True)
    if args.sa_only:
        print("🧪 SA 전용 테스트 모드: 확장소재/GFA는 실행하지 않습니다.", flush=True)

    curr_date = start_date
    is_first_run = True

    while curr_date <= end_date:
        d_str = curr_date.strftime("%Y-%m-%d")
        print("\n" + "=" * 60, flush=True)
        print(f"📅 [ {d_str} ] 데이터 수집 진행", flush=True)
        print("=" * 60, flush=True)

        # 1) 검색광고(SA)
        cmd_sa: List[str] = [
            sys.executable,
            "collector.py",
            "--date",
            d_str,
            "--workers",
            str(args.workers),
        ]

        if not is_first_run:
            cmd_sa.append("--skip_dim")
            print("   ▶ [검색광고] 통계 데이터 업데이트 중 (구조 수집 스킵)...", flush=True)
        else:
            print("   ▶ [검색광고] 최신 구조 및 통계 데이터 동기화 중...", flush=True)

        if args.account_name.strip():
            cmd_sa += ["--account_name", args.account_name.strip()]
        if args.account_names.strip():
            cmd_sa += ["--account_names", args.account_names.strip()]

        run_cmd(cmd_sa, "검색광고", d_str)

        # 2) 확장소재
        if not args.sa_only and not args.skip_ext:
            print("   ▶ [확장소재] 수집 진행 중...", flush=True)
            cmd_ext: List[str] = [sys.executable, "collector_shop_ext.py", "--date", d_str]
            # 확장소재 수집기도 동일 옵션을 지원하는 경우에만 자동 반영되도록 보수적으로 전달
            if args.account_name.strip():
                cmd_ext += ["--account_name", args.account_name.strip()]
            if args.account_names.strip():
                cmd_ext += ["--account_names", args.account_names.strip()]
            run_cmd(cmd_ext, "확장소재", d_str)

        # 3) GFA
        if not args.sa_only and not args.skip_gfa:
            print("   ▶ [GFA] 수집 진행 중...", flush=True)
            cmd_gfa: List[str] = [sys.executable, "collector_gfa.py", "--date", d_str]
            if args.account_name.strip():
                cmd_gfa += ["--account_name", args.account_name.strip()]
            if args.account_names.strip():
                cmd_gfa += ["--account_names", args.account_names.strip()]
            run_cmd(cmd_gfa, "GFA", d_str)

        is_first_run = False
        curr_date += timedelta(days=1)

    print("\n" + "★" * 30, flush=True)
    print("🎉 지정된 기간의 백필 작업이 모두 완료되었습니다!", flush=True)
    print("★" * 30, flush=True)


if __name__ == "__main__":
    main()
