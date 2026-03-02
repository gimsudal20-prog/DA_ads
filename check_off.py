# -*- coding: utf-8 -*-
"""check_off.py - 매시간 캠페인 꺼짐(EXHAUSTED) 여부만 가볍게 스캔하는 초경량 스크립트"""

import os, time, hmac, base64, hashlib, requests, concurrent.futures
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import psycopg2.extras
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
BASE_URL = "https://api.searchad.naver.com"

def sign(method, path, ts):
    msg = f"{ts}.{method}.{path}".encode("utf-8")
    dig = hmac.new(API_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")

def get_camps(cid):
    path = "/ncc/campaigns"
    ts = str(int(time.time() * 1000))
    headers = {"X-Timestamp": ts, "X-API-KEY": API_KEY, "X-Customer": str(cid), "X-Signature": sign("GET", path, ts)}
    try:
        r = requests.get(BASE_URL + path, headers=headers, timeout=10)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def check_account(engine, cid):
    camps = get_camps(cid)
    if not camps: return
    target_date = date.today()
    off_rows = []
    
    for c in camps:
        status = c.get("status", "")
        reason = c.get("statusReason", "")
        if "EXHAUSTED" in status or "LIMIT" in reason:
            edit_tm = c.get("editTm", "")
            if edit_tm:
                utc_dt = datetime.strptime(edit_tm[:19], "%Y-%m-%dT%H:%M:%S")
                kst_dt = utc_dt + timedelta(hours=9)
                if kst_dt.date() == target_date:
                    off_rows.append({
                        "dt": target_date, "customer_id": str(cid), 
                        "campaign_id": str(c["nccCampaignId"]), "off_time": kst_dt.strftime("%H:%M")
                    })
    
    if off_rows:
        df = pd.DataFrame(off_rows)
        tuples = list(df.itertuples(index=False, name=None))
        sql = 'INSERT INTO fact_campaign_off_log ("dt", "customer_id", "campaign_id", "off_time") VALUES %s ON CONFLICT ("dt", "customer_id", "campaign_id") DO UPDATE SET "off_time"=EXCLUDED."off_time"'
        try:
            conn = engine.raw_connection()
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples)
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ [{cid}] 꺼진 캠페인 {len(off_rows)}개 감지 및 DB 기록 완료!")
        except Exception as e:
            pass

def main():
    print(f"🚀 실시간 캠페인 꺼짐 상태 스캔 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    engine = create_engine(db_url, poolclass=NullPool)
    
    accounts = []
    if os.path.exists("accounts.xlsx"):
        try:
            df = pd.read_excel("accounts.xlsx")
            id_col = None
            for c in df.columns:
                if str(c).replace(" ", "").lower() in ["커스텀id", "customerid", "customer_id", "id"]: id_col = c
            if id_col:
                accounts = df[id_col].dropna().astype(str).unique().tolist()
                print(f"🟢 accounts.xlsx 에서 {len(accounts)}개 업체를 불러왔습니다.")
        except: pass

    if not accounts:
        print("⚠️ 수집할 계정 목록(accounts.xlsx)이 없습니다.")
        return

    # 워커를 5개로 줄여서 DB에 무리가 가지 않게 부드럽게 스캔합니다.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
        [exe.submit(check_account, engine, a) for a in accounts]
        
    print("🎉 순찰 스캔 100% 완료!")

if __name__ == "__main__": 
    main()
