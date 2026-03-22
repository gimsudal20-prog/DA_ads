# -*- coding: utf-8 -*-
"""collect_bizmoney.py - 네이버 비즈머니(잔액) 전용 수집기 (debug)

추가 디버그
- 어떤 계정 마스터 파일을 읽었는지 로그 출력
- SA/GFA 계정 로드 개수 출력
- GFA 계정명 목록 출력
- SA+GFA 공유 그룹 목록 출력
"""
from __future__ import annotations

import os
import sys
import time
import hmac
import base64
import hashlib
import concurrent.futures
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from account_master import load_bizmoney_groups, load_naver_accounts

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()
ACCOUNT_MASTER_FILE = (os.getenv("ACCOUNT_MASTER_FILE") or "account_master.xlsx").strip()
BASE_URL = "https://api.searchad.naver.com"


def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)


if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")


def get_header(method: str, uri: str, customer_id: str) -> Dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    sig = hmac.new(
        API_SECRET.encode("utf-8"),
        f"{timestamp}.{method}.{uri}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": base64.b64encode(sig).decode("utf-8"),
    }


def get_bizmoney(customer_id: str) -> Tuple[Optional[int], Optional[dict]]:
    uri = "/billing/bizmoney"
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL + uri, headers=get_header("GET", uri, customer_id), timeout=20)
            if r.status_code == 403:
                return None, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt)
                continue
            if r.status_code != 200:
                return None, None
            data = r.json()
            total_balance = 0
            if isinstance(data, dict):
                total_balance += int(data.get("bizmoney", 0) or 0)
                total_balance += int(data.get("couponBizmoney", 0) or 0)
                total_balance += int(data.get("prepaidBizmoney", 0) or 0)
                for k, v in data.items():
                    if isinstance(v, (int, float)) and "bizmoney" in str(k).lower() and k not in {"bizmoney", "couponBizmoney", "prepaidBizmoney"}:
                        total_balance += int(v or 0)
            return total_balance, data
        except Exception:
            time.sleep(2 + attempt)
    return None, None


def get_engine() -> Engine:
    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url,
        poolclass=NullPool,
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=300000"},
        future=True,
    )


def upsert_dim_account_meta_bulk(engine: Engine, accounts: List[Dict[str, str]]):
    if not accounts:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_account_meta (
                customer_id TEXT PRIMARY KEY,
                account_name TEXT,
                manager TEXT,
                monthly_budget BIGINT DEFAULT 0,
                platform TEXT,
                naver_media_type TEXT,
                bizmoney_group_key TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        for ddl in [
            "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS platform TEXT",
            "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS naver_media_type TEXT",
            "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS bizmoney_group_key TEXT",
            "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS monthly_budget BIGINT DEFAULT 0",
        ]:
            try:
                conn.execute(text(ddl))
            except Exception:
                pass

    sql = """
        INSERT INTO dim_account_meta (customer_id, account_name, manager, platform, naver_media_type, bizmoney_group_key)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            account_name = EXCLUDED.account_name,
            manager = EXCLUDED.manager,
            platform = EXCLUDED.platform,
            naver_media_type = EXCLUDED.naver_media_type,
            bizmoney_group_key = EXCLUDED.bizmoney_group_key,
            updated_at = NOW()
    """
    tuples = [
        (a["id"], a["name"], a.get("manager", ""), "naver", a.get("media_type", "sa"), a.get("bizmoney_group_key", ""))
        for a in accounts
    ]
    raw_conn, cur = None, None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
        raw_conn.commit()
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if raw_conn:
            try:
                raw_conn.close()
            except Exception:
                pass


def ensure_fact_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
                dt DATE,
                customer_id TEXT,
                bizmoney_balance BIGINT,
                bizmoney_group_key TEXT,
                source_customer_id TEXT,
                is_group_representative BOOLEAN DEFAULT FALSE,
                PRIMARY KEY(dt, customer_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_bizmoney_group_daily (
                dt DATE,
                bizmoney_group_key TEXT,
                representative_customer_id TEXT,
                bizmoney_balance BIGINT,
                PRIMARY KEY(dt, bizmoney_group_key)
            )
        """))
        for ddl in [
            "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS bizmoney_group_key TEXT",
            "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS source_customer_id TEXT",
            "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS is_group_representative BOOLEAN DEFAULT FALSE",
        ]:
            try:
                conn.execute(text(ddl))
            except Exception:
                pass


def upsert_bizmoney_bulk(engine: Engine, account_rows: List[Dict[str, Any]], group_rows: List[Dict[str, Any]]):
    ensure_fact_tables(engine)
    if account_rows:
        df = pd.DataFrame(account_rows).drop_duplicates(subset=["dt", "customer_id"], keep="last")
        sql = """
            INSERT INTO fact_bizmoney_daily (dt, customer_id, bizmoney_balance, bizmoney_group_key, source_customer_id, is_group_representative)
            VALUES %s
            ON CONFLICT (dt, customer_id) DO UPDATE SET
                bizmoney_balance = EXCLUDED.bizmoney_balance,
                bizmoney_group_key = EXCLUDED.bizmoney_group_key,
                source_customer_id = EXCLUDED.source_customer_id,
                is_group_representative = EXCLUDED.is_group_representative
        """
        tuples = list(df.itertuples(index=False, name=None))
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
            raw_conn.commit()
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if raw_conn:
                try:
                    raw_conn.close()
                except Exception:
                    pass

    if group_rows:
        df = pd.DataFrame(group_rows).drop_duplicates(subset=["dt", "bizmoney_group_key"], keep="last")
        sql = """
            INSERT INTO fact_bizmoney_group_daily (dt, bizmoney_group_key, representative_customer_id, bizmoney_balance)
            VALUES %s
            ON CONFLICT (dt, bizmoney_group_key) DO UPDATE SET
                representative_customer_id = EXCLUDED.representative_customer_id,
                bizmoney_balance = EXCLUDED.bizmoney_balance
        """
        tuples = list(df.itertuples(index=False, name=None))
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
            raw_conn.commit()
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if raw_conn:
                try:
                    raw_conn.close()
                except Exception:
                    pass


def main():
    engine = get_engine()

    log(f"📄 ACCOUNT_MASTER_FILE={ACCOUNT_MASTER_FILE} / exists={os.path.exists(ACCOUNT_MASTER_FILE)}")
    try:
        log(f"📁 CWD files: {sorted(os.listdir('.'))[:30]}")
    except Exception:
        pass

    groups = load_bizmoney_groups(file_path=ACCOUNT_MASTER_FILE)
    accounts = load_naver_accounts(file_path=ACCOUNT_MASTER_FILE, include_gfa=True, media_types=["sa", "gfa"])

    gfa_accounts = [a for a in accounts if a.get("media_type") == "gfa"]
    shared_groups = [g for g in groups if g.get("has_gfa") and g.get("has_sa")]

    log(f"📋 master 로드 결과: 계정 {len(accounts)}개 / GFA {len(gfa_accounts)}개 / 그룹 {len(groups)}개 / SA+GFA 공유그룹 {len(shared_groups)}개")
    if gfa_accounts:
        log("🧩 GFA 계정 목록: " + ", ".join(a["name"] for a in gfa_accounts))
    else:
        log("⚠️ GFA 계정이 0개입니다. account_master.xlsx 또는 account_master.py 로더가 반영되지 않았을 가능성이 큽니다.")
    if shared_groups:
        for g in shared_groups:
            log(f"🔗 공유 그룹: {g['bizmoney_group_key']} -> " + ", ".join(m['name'] for m in g['members']))

    if not groups and CUSTOMER_ID:
        groups = [{
            "bizmoney_group_key": "Target Account",
            "representative": {"id": CUSTOMER_ID, "name": "Target Account", "media_type": "sa", "bizmoney_group_key": "Target Account"},
            "members": [{"id": CUSTOMER_ID, "name": "Target Account", "media_type": "sa", "bizmoney_group_key": "Target Account"}],
            "has_gfa": False,
            "has_sa": True,
        }]
        accounts = groups[0]["members"]

    if not groups:
        log("⚠️ 수집할 비즈머니 그룹이 없습니다.")
        return

    upsert_dim_account_meta_bulk(engine, accounts)

    log(f"📋 비즈머니 수집 시작: 그룹 {len(groups)}개 / 계정 {len(accounts)}개")
    today = date.today()
    account_rows: List[Dict[str, Any]] = []
    group_rows: List[Dict[str, Any]] = []

    first_debug_done = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, max(1, len(groups)))) as executor:
        futures = {executor.submit(get_bizmoney, grp["representative"]["id"]): grp for grp in groups}
        for future in concurrent.futures.as_completed(futures):
            grp = futures[future]
            rep = grp["representative"]
            bal, raw_data = future.result()
            key = grp["bizmoney_group_key"]
            if bal is None:
                log(f"🚫 {key}: 조회 실패 (대표 계정 {rep['name']})")
                continue
            if not first_debug_done and raw_data:
                log(f"🔎 [ 네이버 원본 데이터 구조 포착 ] -> {raw_data}")
                first_debug_done = True
            member_names = ", ".join(m["name"] for m in grp["members"])
            if grp.get("has_gfa") and grp.get("has_sa"):
                log(f"✅ {key}: {bal:,}원 (공유 비즈머니: {member_names})")
            else:
                log(f"✅ {key}: {bal:,}원")
            group_rows.append({
                "dt": today,
                "bizmoney_group_key": key,
                "representative_customer_id": rep["id"],
                "bizmoney_balance": bal,
            })
            for member in grp["members"]:
                account_rows.append({
                    "dt": today,
                    "customer_id": member["id"],
                    "bizmoney_balance": bal,
                    "bizmoney_group_key": key,
                    "source_customer_id": rep["id"],
                    "is_group_representative": member["id"] == rep["id"],
                })

    if account_rows or group_rows:
        upsert_bizmoney_bulk(engine, account_rows, group_rows)
        log(f"🎉 비즈머니 수집 완료! 그룹 {len(group_rows)}건 / 계정 {len(account_rows)}건 적재")


if __name__ == "__main__":
    main()
