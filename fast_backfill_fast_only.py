# -*- coding: utf-8 -*-
"""fast_backfill.py - 날짜 범위 백필 및 선택 계정 대상 수집 실행 스크립트

지원 기능
- --start / --end 날짜 범위 백필
- --account_name / --account_names 업체 필터
- --workers 동시 작업 수 전달
- --sa_only / --test_sa_only / --skip_ext / --skip_gfa 지원
- 입력값 공백 자동 제거(strip)
- 실행 전 수집 범위/스킵 대상 명확히 로그 출력
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import List


def clean(v: str | None) -> str:
    return (v or "").strip()


def run_cmd(cmd: List[str], label: str, day: str) -> None:
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [{label}] {day} 수집 중 오류 발생", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, required=False, help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=False, help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=8, help="collector.py 동시 작업 수")
    parser.add_argument("--account_name", type=str, default="", help="정확/부분일치로 테스트할 단일 업체명")
    parser.add_argument("--account_names", type=str, default="", help="쉼표(,)로 구분한 여러 업체명")

    # 하위 호환 유지: 기존 --sa_only 그대로 지원
    # 실수 방지용 별칭: --test_sa_only
    parser.add_argument(
        "--sa_only",
        "--test_sa_only",
        dest="sa_only",
        action="store_true",
        help="검색광고(SA)만 수집 (확장소재/GFA 스킵)",
    )
    parser.add_argument("--skip_ext", action="store_true", help="쇼핑 확장소재 수집 스킵")
    parser.add_argument("--skip_gfa", action="store_true", help="GFA 수집 스킵")
    parser.add_argument("--fast", action="store_true", help="빠른 백필 모드: 모든 날짜에서 구조 수집(skip_dim) 스킵")
    args = parser.parse_args()

    args.start = clean(args.start)
    args.end = clean(args.end)
    args.account_name = clean(args.account_name)
    args.account_names = clean(args.account_names)
    return args


def print_run_config(args: argparse.Namespace, start_date: date, end_date: date) -> None:
    print(f"🚀 백필 작업 시작: {start_date} ~ {end_date}", flush=True)

    if args.account_name:
        print(f"🎯 단일 업체 필터: {args.account_name}", flush=True)
    if args.account_names:
        print(f"🎯 다중 업체 필터: {args.account_names}", flush=True)

    print(f"👷 collector 동시 작업 수: {args.workers}", flush=True)

    if args.fast:
        print("⚡ 빠른 백필 모드: 모든 날짜에서 구조 수집(skip_dim) 스킵", flush=True)

    if args.sa_only:
        print("🧪 SA 전용 테스트 모드 활성화: 검색광고만 수집합니다.", flush=True)
        print("⏭️ 확장소재 수집 스킵 사유: --sa_only / --test_sa_only", flush=True)
        print("⏭️ GFA 수집 스킵 사유: --sa_only / --test_sa_only", flush=True)
    else:
        if args.skip_ext:
            print("⏭️ 확장소재 수집 스킵 사유: --skip_ext", flush=True)
        else:
            print("✅ 확장소재 수집 포함", flush=True)

        if args.skip_gfa:
            print("⏭️ GFA 수집 스킵 사유: --skip_gfa", flush=True)
        else:
            print("✅ GFA 수집 포함", flush=True)


def main() -> None:
    api_key = clean(os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY"))
    if not api_key:
        print("❌ [FATAL ERROR] 환경변수(API 키)를 찾을 수 없습니다! yml 파일의 env 설정을 확인해주세요.", flush=True)
        sys.exit(1)

    args = parse_args()

    if not args.start or not args.end:
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = today - timedelta(days=7)
        print("ℹ️ 날짜 인자가 없습니다. 자동으로 최근 7일(D-7 ~ D-1) 백필 모드로 작동합니다.", flush=True)
    else:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
        except ValueError as e:
            print(f"❌ [FATAL ERROR] 날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식으로 입력해주세요. ({e})", flush=True)
            sys.exit(1)

    if start_date > end_date:
        print("❌ [FATAL ERROR] 시작일이 종료일보다 늦습니다.", flush=True)
        sys.exit(1)

    print_run_config(args, start_date, end_date)

    curr_date = start_date
    is_first_run = True

    while curr_date <= end_date:
        d_str = curr_date.strftime("%Y-%m-%d")
        print("\n" + "=" * 60, flush=True)
        print(f"📅 [ {d_str} ] 데이터 수집 진행", flush=True)
        print("=" * 60, flush=True)

        cmd_sa: List[str] = [
            sys.executable,
            "collector.py",
            "--date",
            d_str,
            "--workers",
            str(args.workers),
        ]

        if args.fast:
            cmd_sa.append("--skip_dim")
            print("   ▶ [검색광고] 빠른 백필 모드: 구조 수집 스킵, 통계 데이터 업데이트 중...", flush=True)
        elif not is_first_run:
            cmd_sa.append("--skip_dim")
            print("   ▶ [검색광고] 통계 데이터 업데이트 중 (구조 수집 스킵)...", flush=True)
        else:
            print("   ▶ [검색광고] 최신 구조 및 통계 데이터 동기화 중...", flush=True)

        if args.account_name:
            cmd_sa += ["--account_name", args.account_name]
        if args.account_names:
            cmd_sa += ["--account_names", args.account_names]

        run_cmd(cmd_sa, "검색광고", d_str)

        if args.fast:
        print("⚡ 빠른 백필 모드: 모든 날짜에서 구조 수집(skip_dim) 스킵", flush=True)

    if args.sa_only:
            print("   ⏭️ [확장소재] 이번 실행에서는 건너뜀 (SA 전용 테스트 모드)", flush=True)
            print("   ⏭️ [GFA] 이번 실행에서는 건너뜀 (SA 전용 테스트 모드)", flush=True)
        else:
            if args.skip_ext:
                print("   ⏭️ [확장소재] 스킵 (--skip_ext)", flush=True)
            else:
                print("   ▶ [확장소재] 수집 진행 중...", flush=True)
                cmd_ext: List[str] = [sys.executable, "collector_shop_ext.py", "--date", d_str]
                if args.account_name:
                    cmd_ext += ["--account_name", args.account_name]
                if args.account_names:
                    cmd_ext += ["--account_names", args.account_names]
                run_cmd(cmd_ext, "확장소재", d_str)

            if args.skip_gfa:
                print("   ⏭️ [GFA] 스킵 (--skip_gfa)", flush=True)
            else:
                print("   ▶ [GFA] 수집 진행 중...", flush=True)
                cmd_gfa: List[str] = [sys.executable, "collector_gfa.py", "--date", d_str]
                if args.account_name:
                    cmd_gfa += ["--account_name", args.account_name]
                if args.account_names:
                    cmd_gfa += ["--account_names", args.account_names]
                run_cmd(cmd_gfa, "GFA", d_str)

        is_first_run = False
        curr_date += timedelta(days=1)

    print("\n" + "★" * 30, flush=True)
    print("🎉 지정된 기간의 백필 작업이 모두 완료되었습니다!", flush=True)
    print("★" * 30, flush=True)


if __name__ == "__main__":
    main()
