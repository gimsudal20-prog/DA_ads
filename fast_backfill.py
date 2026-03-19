# -*- coding: utf-8 -*-
"""최근 SA 분리수집 백필 전용 스크립트

목적
- 2026-03-11 이후 purchase/cart/wishlist 분리 수집 구간을 빠르게 재백필
- SA collector만 실행
- GFA suffix 계정은 SA collector 대상에서 제외
- 기본 workers=3
- 기본은 모든 날짜 skip_dim (빠른 재백필)

특징
- collector_backfill_recent_sa.py 호출
- ext/GFA collector는 아예 실행하지 않음
- 이미 적재된 날짜를 다시 돌려도 fact 테이블은 날짜/고객 단위 replace 구조라 누적 중복 적재를 피함
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, date
from typing import List


def clean(v: str | None) -> str:
    return (v or "").strip()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=str, required=True, help="시작일 (YYYY-MM-DD)")
    p.add_argument("--end", type=str, required=True, help="종료일 (YYYY-MM-DD)")
    p.add_argument("--workers", type=int, default=3, help="collector 동시 작업 수 (기본 3)")
    p.add_argument("--account_name", type=str, default="", help="단일 업체명 또는 일부 문자열")
    p.add_argument("--account_names", type=str, default="", help="쉼표(,)로 구분한 여러 업체명")
    p.add_argument("--sync_dim_first_day", action="store_true", help="첫날만 구조 수집, 이후 skip_dim")
    args = p.parse_args()
    args.start = clean(args.start)
    args.end = clean(args.end)
    args.account_name = clean(args.account_name)
    args.account_names = clean(args.account_names)
    return args


def run_cmd(cmd: List[str], label: str, day: str) -> None:
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ [{label}] {day} 수집 중 오류 발생 (exit={e.returncode})", flush=True)


def main() -> None:
    api_key = clean(os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY"))
    if not api_key:
        print("❌ [FATAL ERROR] 환경변수(API 키)를 찾을 수 없습니다.", flush=True)
        sys.exit(1)

    args = parse_args()
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"❌ [FATAL ERROR] 날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식으로 입력해주세요. ({e})", flush=True)
        sys.exit(1)

    if start_date > end_date:
        print("❌ [FATAL ERROR] 시작일이 종료일보다 늦습니다.", flush=True)
        sys.exit(1)

    print(f"🚀 최근 SA 분리수집 빠른 백필 시작: {start_date} ~ {end_date}", flush=True)
    print(f"👷 collector 동시 작업 수: {args.workers}", flush=True)
    print("✅ SA collector만 실행합니다.", flush=True)
    print("🚫 GFA suffix 계정은 수집 대상에서 제외합니다.", flush=True)
    print(
        f"🧱 구조 수집 정책: {'첫날만 구조 수집' if args.sync_dim_first_day else '모든 날짜 skip_dim (빠른 재백필)'}",
        flush=True,
    )
    if args.account_name:
        print(f"🎯 단일 업체 필터: {args.account_name}", flush=True)
    if args.account_names:
        print(f"🎯 다중 업체 필터: {args.account_names}", flush=True)

    curr = start_date
    first = True
    while curr <= end_date:
        d_str = curr.strftime("%Y-%m-%d")
        print("\n" + "=" * 60, flush=True)
        print(f"📅 [ {d_str} ] 데이터 수집 진행", flush=True)
        print("=" * 60, flush=True)

        cmd = [
            sys.executable,
            "collector_backfill_recent_sa.py",
            "--date", d_str,
            "--workers", str(args.workers),
            "--exclude_gfa_accounts",
        ]
        if args.account_name:
            cmd += ["--account_name", args.account_name]
        if args.account_names:
            cmd += ["--account_names", args.account_names]

        use_skip_dim = (not args.sync_dim_first_day) or (not first)
        if use_skip_dim:
            cmd.append("--skip_dim")
            print("   ▶ [검색광고] 빠른 재백필 모드: 구조 수집 스킵, 통계/분리값 업데이트 중...", flush=True)
        else:
            print("   ▶ [검색광고] 첫날 구조 및 통계 동기화 중...", flush=True)

        run_cmd(cmd, "최근 SA 백필", d_str)

        first = False
        curr += timedelta(days=1)

    print("\n" + "★" * 30, flush=True)
    print("🎉 최근 SA 분리수집 빠른 백필이 모두 완료되었습니다!", flush=True)
    print("★" * 30, flush=True)


if __name__ == "__main__":
    main()
