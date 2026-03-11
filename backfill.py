# -*- coding: utf-8 -*-
"""backfill.py - 특정 계정/기간 대상 과거 데이터 수집(백필) 스크립트"""

import subprocess
import time
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta

def run_backfill(start_date_str: str, end_date_str: str, customer_id: str, account_name: str=""):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        print(f"\n{'='*60}", flush=True)
        print(f"🔄 [백필 진행 중] 대상 날짜: {date_str} | 대상 계정: {account_name} ({customer_id})", flush=True)
        print(f"{'='*60}", flush=True)
        
        # ✨ 실시간 로그 출력을 위해 python 뒤에 -u (unbuffered) 옵션 추가
        cmd = [
            "python", "-u", "collector.py", 
            "--date", date_str, 
            "--customer_id", str(customer_id),
            "--workers", "2"
        ]
        
        try:
            # subprocess의 출력을 메인 화면에 실시간으로 연결
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in process.stdout:
                print(line, end='', flush=True)
            process.wait()
            
            if process.returncode != 0:
                print(f"⚠️ {date_str} 수집 중 오류 발생 (에러 코드: {process.returncode})", flush=True)
                time.sleep(3)
                
        except KeyboardInterrupt:
            print("\n🛑 사용자에 의해 백필 작업이 강제 중단되었습니다.", flush=True)
            break
        except Exception as e:
            print(f"❌ 실행 중 예외 발생: {e}", flush=True)
            
        current_date += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="과거 데이터 백필 스크립트")
    parser.add_argument("--start", type=str, required=True, help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--customer_id", type=str, default="", help="특정 계정 ID (생략 시 accounts2.xlsx 사용)")
    args = parser.parse_args()

    start_str = args.start
    end_str = args.end

    print(f"🚀 백필 작업 구간: {start_str} ~ {end_str}", flush=True)

    targets = []
    if args.customer_id:
        targets.append((args.customer_id, "Target Account"))
    else:
        # ✨ accounts2.xlsx 읽기
        try:
            df = pd.read_excel("accounts2.xlsx")
            id_col, name_col, mgr_col = None, None, None
            
            for c in df.columns:
                c_clean = str(c).replace(" ", "").lower()
                if c_clean in ["커스텀id", "customerid", "customer_id", "id"]: id_col = c
                if c_clean in ["업체명", "accountname", "account_name", "name"]: name_col = c
                if c_clean in ["담당자", "manager"]: mgr_col = c
            
            if not id_col:
                print("❌ accounts2.xlsx에 커스텀ID 컬럼을 찾을 수 없습니다.", flush=True)
                sys.exit(1)
                
            for _, row in df.iterrows():
                cid = str(row[id_col]).strip()
                if cid and cid.lower() != 'nan':
                    acc_name = str(row[name_col]) if name_col else "알수없음"
                    mgr_name = str(row[mgr_col]) if mgr_col else "미지정"
                    targets.append((cid, f"{mgr_name}_{acc_name}"))
                    
        except Exception as e:
            print(f"❌ accounts2.xlsx 파일을 읽는 중 오류 발생: {e}", flush=True)
            sys.exit(1)

    if not targets:
        print("⚠️ 수집할 계정이 없습니다.", flush=True)
        sys.exit(1)

    print(f"📋 총 {len(targets)}개 타겟 계정 백필 시작...", flush=True)
    for cid, acc in targets:
        run_backfill(start_str, end_str, cid, acc)
        
    print("\n✅ 모든 백필 작업이 완료되었습니다!", flush=True)
