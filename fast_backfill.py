# -*- coding: utf-8 -*-
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
    p = argparse.ArgumentParser(description="날짜 범위별 SA/GFA/확장소재 백필 실행기")
    p.add_argument("--start", required=True, help="시작일 YYYY-MM-DD")
    p.add_argument("--end", required=True, help="종료일 YYYY-MM-DD")
    p.add_argument("--workers", type=int, default=1, help="collector.py workers")
    p.add_argument("--account_name", default="", help="단일 업체명")
    p.add_argument("--account_names", default="", help="여러 업체명 콤마구분")
    p.add_argument("--fast", action="store_true", help="collector.py 빠른 수집 모드")
    p.add_argument(
        "--sync_dim_first_day",
        action="store_true",
        help="첫날만 구조 동기화, 이후 날짜는 --skip_dim",
    )
    p.add_argument("--with_gfa", action="store_true", help="collector_gfa.py도 함께 실행")
    p.add_argument("--with_shop_ext", action="store_true", help="collector_shop_ext.py도 함께 실행")
    p.add_argument("--shopping_only", action="store_true", help="쇼핑검색 캠페인만 수집/백필")
    p.add_argument(
        "--collect_mode",
        default="sa_with_device",
        choices=["sa_only", "device_only", "sa_with_device"],
        help="collector.py 수집 모드",
    )
    p.add_argument(
        "--sa_scope",
        default="full",
        choices=["full", "ad_only", "전체", "소재만"],
        help="collector.py SA 범위",
    )
    p.add_argument(
        "--run_target",
        default="sa_and_shop_ext",
        choices=["sa_only", "shop_ext_only", "sa_and_shop_ext", "SA만", "확장소재만", "SA+확장소재"],
        help="백필 실행 대상",
    )
    p.add_argument(
        "--shop_ext_bucket",
        default="shopping",
        choices=["shopping", "non_shopping", "all", "쇼핑검색", "파워링크외", "전체"],
        help="확장소재 수집 버킷",
    )
    args = p.parse_args()
    args.start = clean(args.start)
    args.end = clean(args.end)
    args.account_name = clean(args.account_name)
    args.account_names = clean(args.account_names)
    return args


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def run_cmd(cmd: List[str], label: str, day: str) -> None:
    print(f"   ▶ [{label}] 수집 진행 중...", flush=True)
    print(f"      실행: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)
    print(f"   ✅ [{label}] {day} 완료", flush=True)


def build_sa_cmd(args: argparse.Namespace, d_str: str, first: bool) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        "collector.py",
        "--date",
        d_str,
        "--workers",
        str(args.workers),
    ]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    cmd += ["--collect_mode", args.collect_mode, "--sa_scope", args.sa_scope]
    if args.shopping_only:
        cmd.append("--shopping_only")

    if args.sync_dim_first_day:
        skip_dim_this_run = not first
    else:
        skip_dim_this_run = True

    if args.fast and skip_dim_this_run:
        cmd.append("--fast")
    elif args.fast and not skip_dim_this_run:
        print("   ℹ️ 첫날 구조 동기화 구간이라 --fast는 붙이지 않습니다.", flush=True)

    if skip_dim_this_run:
        cmd.append("--skip_dim")
    return cmd


def build_shop_ext_cmd(args: argparse.Namespace, d_str: str) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        "collector_shop_ext.py",
        "--date",
        d_str,
        "--ext_bucket",
        args.shop_ext_bucket,
    ]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    return cmd


def build_gfa_cmd(args: argparse.Namespace, d_str: str) -> List[str]:
    cmd: List[str] = [sys.executable, "collector_gfa.py", "--date", d_str]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    return cmd


def main() -> None:
    api_key = clean(os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY"))
    if not api_key:
        print("❌ [FATAL] NAVER_ADS_API_KEY / NAVER_API_KEY 환경변수가 없습니다.", flush=True)
        sys.exit(1)

    args = parse_args()
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        print("❌ [FATAL] 날짜 형식은 YYYY-MM-DD 여야 합니다.", flush=True)
        sys.exit(1)

    if start_date > end_date:
        print("❌ [FATAL] 시작일이 종료일보다 늦습니다.", flush=True)
        sys.exit(1)

    if args.shopping_only:
        if args.fast:
            print("⚠️ 쇼핑검색 백필은 fast 모드를 강제로 해제합니다.", flush=True)
        args.fast = False
        if args.workers != 1:
            print(f"⚠️ 쇼핑검색 백필은 workers=1로 고정합니다. (입력값 {args.workers} → 1)", flush=True)
        args.workers = 1
        args.with_shop_ext = True
        args.shop_ext_bucket = "shopping"

    run_sa = args.run_target in {"sa_only", "sa_and_shop_ext"}
    run_shop_ext = args.run_target in {"shop_ext_only", "sa_and_shop_ext"}
    if args.with_shop_ext:
        run_shop_ext = True

    print(f"🚀 백필 작업 시작: {start_date} ~ {end_date}", flush=True)
    if args.account_name:
        print(f"🎯 단일 업체 필터: {args.account_name}", flush=True)
    if args.account_names:
        print(f"🎯 복수 업체 필터: {args.account_names}", flush=True)
    if args.fast:
        print("🧪 SA 빠른 수집 모드: collector.py 에 --fast 전달", flush=True)
    print(f"🧭 수집 모드: {_label_collect_mode(args.collect_mode)} ({args.collect_mode})", flush=True)
    print(f"🎯 SA 범위: {_label_sa_scope(args.sa_scope)} ({args.sa_scope})", flush=True)
    print(f"🎬 실행 대상: {_label_run_target(args.run_target)} ({args.run_target})", flush=True)
    if args.sync_dim_first_day:
        print("🧱 첫날만 구조 동기화, 이후 날짜는 --skip_dim", flush=True)
    else:
        print("⚡ 전 기간 skip_dim 모드", flush=True)
    if args.fast and args.collect_mode != "sa_only":
        print("⚠️ fast + device 수집 조합입니다. PC/M 문제 분석 시에는 fast=false 를 권장합니다.", flush=True)
    if args.with_gfa:
        print("📺 GFA 수집 포함", flush=True)
    if args.shopping_only:
        print("🛍️ 쇼핑검색 전용 수집", flush=True)
    if run_shop_ext:
        label = {"shopping": "쇼핑검색", "non_shopping": "파워링크외", "all": "전체"}.get(args.shop_ext_bucket, args.shop_ext_bucket)
        print(f"🧩 확장소재 수집 포함 | 버킷: {label}", flush=True)
    print("=" * 60, flush=True)

    first = True
    for d in daterange(start_date, end_date):
        d_str = d.strftime("%Y-%m-%d")
        print(f"\n📅 [ {d_str} ]", flush=True)

        if run_sa:
            cmd_sa = build_sa_cmd(args, d_str, first)
            run_cmd(cmd_sa, "SA", d_str)
        else:
            print("   ⏭️ [SA] run_target 설정으로 스킵합니다.", flush=True)

        if run_shop_ext:
            if os.path.exists("collector_shop_ext.py"):
                cmd_shop_ext = build_shop_ext_cmd(args, d_str)
                run_cmd(cmd_shop_ext, "SHOP_EXT", d_str)
            else:
                print("   ⏭️ [SHOP_EXT] collector_shop_ext.py 파일이 없어 스킵합니다.", flush=True)

        if args.with_gfa:
            if os.path.exists("collector_gfa.py"):
                cmd_gfa = build_gfa_cmd(args, d_str)
                run_cmd(cmd_gfa, "GFA", d_str)
            else:
                print("   ⏭️ [GFA] collector_gfa.py 파일이 없어 스킵합니다.", flush=True)

        first = False

    print("\n" + "★" * 32, flush=True)
    print("🎉 백필 작업 완료", flush=True)
    print("★" * 32, flush=True)


if __name__ == "__main__":
    main()
