# -*- coding: utf-8 -*-
"""backfill.py - 특정 계정/기간 대상 과거 데이터 수집(백필) 스크립트"""

import subprocess
import time
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_single_task(date_str: str, customer_id: str, account_name: str):
    """단일 날짜/계정에 대해 collector.py를 실행하는 함수"""
    cmd = [
        "python", "collector.py", 
        "--date", date_str, 
        "--customer_id", str(customer_id)
    ]
    
    try:
        # 병렬 처리 시 출력이 섞이지 않도록 캡처 모드로 실행
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode != 0:
            error_msg = process.stderr if process.stderr else process.stdout
            return False, f"⚠️ [{date_str} | {account_name}] 수집 중 오류 (코드: {process.returncode})\n{error_msg}"
        return True, f"✅ [{date_str} | {account_name}] 수집 완료"
        
    except Exception as e:
        return False, f"❌ [{date_str} | {account_name}] 실행 중 예외 발생: {e}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="과거 데이터 백필 스크립트 (병렬 처리 지원)")
    parser.add_argument("--start", type=str, required=True, help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--customer_id", type=str, default="", help="특정 계정 ID (생략 시 accounts.xlsx 자동 사용)")
    parser.add_argument("--max_workers", type=int, default=8, help="병렬 처리할 최대 작업 수 (기본값: 8)")
    args = parser.parse_args()

    start_str = args.start
    end_str = args.end
    max_workers = args.max_workers

    print(f"🚀 백필 작업 구간: {start_str} ~ {end_str} (병렬 워커 수: {max_workers})", flush=True)

    targets = []
    if args.customer_id:
        targets.append((args.customer_id, "Target Account"))
    else:
        # 1. 파일 읽기 (오직 accounts.xlsx만 확인)
        try:
            df = pd.read_excel("accounts.xlsx")
            print("📄 'accounts.xlsx' 파일에서 계정 정보를 읽어옵니다.", flush=True)
        except FileNotFoundError:
            print("❌ 'accounts.xlsx' 파일을 찾을 수 없습니다. 프로그램이 실행되는 폴더에 파일이 있는지 확인해주세요.", flush=True)
            sys.exit(1)
        except Exception as e:
            print(f"❌ 'accounts.xlsx' 읽기 오류: {e}", flush=True)
            sys.exit(1)
            
        # 2. 유연한 컬럼명 매핑 (한글 엑셀 헤더 완벽 지원)
        id_col, name_col, mgr_col = None, None, None
        
        for c in df.columns:
            c_clean = str(c).replace(" ", "").lower()
            if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id", "고객 id"]: id_col = c
            if c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]: name_col = c
            if c_clean in ["담당자", "manager"]: mgr_col = c
        
        if not id_col:
            print(f"❌ 파일에서 고객ID 컬럼을 찾을 수 없습니다. (현재 컬럼: {list(df.columns)})", flush=True)
            sys.exit(1)
            
        for _, row in df.iterrows():
            cid = str(row[id_col]).strip()
            if cid and cid.lower() != 'nan':
                acc_name = str(row[name_col]) if name_col else "알수없음"
                mgr_name = str(row[mgr_col]) if mgr_col else "미지정"
                targets.append((cid, f"{mgr_name}_{acc_name}"))

    if not targets:
        print("⚠️ 수집할 계정이 없습니다.", flush=True)
        sys.exit(1)

    print(f"📋 총 {len(targets)}개 타겟 계정 식별 완료.", flush=True)

    # 3. 날짜 목록 생성
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    date_list = []
    curr_date = start_date
    while curr_date <= end_date:
        date_list.append(curr_date.strftime("%Y-%m-%d"))
        curr_date += timedelta(days=1)

    # 4. 전체 작업(Task) 생성: 계정 개수 x 날짜 일수
    tasks = []
    for cid, acc in targets:
        for d in date_list:
            tasks.append((d, cid, acc))

    print(f"🔥 총 {len(tasks)}개의 수집 작업(계정 x 날짜)을 {max_workers}개의 스레드로 병렬 시작합니다...", flush=True)
    
    success_count = 0
    fail_count = 0

    # 5. ThreadPoolExecutor를 이용한 병렬 실행 (속도 최적화 핵심)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 등록
        future_to_task = {
            executor.submit(run_single_task, task[0], task[1], task[2]): task 
            for task in tasks
        }
        
        # 완료되는 순서대로 결과 취합 및 출력
        for future in as_completed(future_to_task):
            task_info = future_to_task[future]
            try:
                success, msg = future.result()
                if success:
                    success_count += 1
                    print(msg, flush=True)
                else:
                    fail_count += 1
                    print(msg, flush=True)
            except Exception as exc:
                fail_count += 1
                print(f"❌ [작업 예외] {task_info}: {exc}", flush=True)

    print(f"\n✅ 모든 백필 작업이 완료되었습니다! (성공: {success_count}, 실패: {fail_count})", flush=True)
