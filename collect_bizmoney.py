# -*- coding: utf-8 -*-
"""
collect_bizmoney.py - 네이버 검색광고 비즈머니(잔액) 전용 수집기

✅ 수집 대상 계정 기준 (우선순위)
1) (권장) GitHub 레포에 있는 accounts.xlsx 기준
   - 기본 경로: ./accounts.xlsx
   - 환경변수로 경로 변경: ACCOUNTS_FILE
   - 컬럼 자동 인식:
     - customer_id / CUSTOMER_ID / 커스텀 ID / 커스텀ID / ID 등
     - 업체명 / account_name 등 (로그용)
     - (선택) 활성 / is_active / 사용 여부 등이 있으면 활성만 수집
     - (선택) 담당자 컬럼 + env(MANAGER_FILTER)로 담당자 필터 가능

2) DB의 dim_account_meta 테이블 전체(customer_id)
3) DB가 비어있으면 환경변수 CUSTOMER_ID 1개

- 수정사항: JSON 키 값 대소문자 수정 (bizMoney -> bizmoney)
"""

import os
import sys
import time
import hmac
import base64
import hashlib
from datetime import date
from typing import List, Dict, Optional

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# pandas는 accounts.xlsx 읽을 때만 필요 (없으면 DB fallback)
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore


# -----------------------------
# 1) 환경변수 로드
# -----------------------------
load_dotenv()

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()
BASE_URL = "https://api.searchad.naver.com"

# 엑셀 계정 파일(레포에 커밋돼 있어야 함)
ACCOUNTS_FILE = (os.getenv("ACCOUNTS_FILE") or "accounts.xlsx").strip()
MANAGER_FILTER = (os.getenv("MANAGER_FILTER") or "").strip()  # 예: "승훈" 넣으면 해당 담당자만

if not API_KEY or not API_SECRET:
    print("❌ API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
    sys.exit(1)


# -----------------------------
# 2) API 서명 및 헤더 생성
# -----------------------------
def get_header(method: str, uri: str, customer_id: str) -> Dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        f"{timestamp}.{method}.{uri}".encode("utf-8"),
        hashlib.sha256,
    ).digest()

    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": base64.b64encode(signature).decode("utf-8"),
    }


# -----------------------------
# 3) 비즈머니 조회
# -----------------------------
def get_bizmoney(customer_id: str) -> Optional[int]:
    uri = "/billing/bizmoney"
    try:
        r = requests.get(
            BASE_URL + uri,
            headers=get_header("GET", uri, customer_id),
            timeout=20,
        )

        if r.status_code == 200:
            data = r.json()
            balance = int(data.get("bizmoney", 0))
            return balance

        print(f"⚠️ [API Error] {customer_id}: {r.status_code} - {r.text[:200]}")
        return None

    except Exception as e:
        print(f"⚠️ [System Error] {customer_id}: {e}")
        return None


# -----------------------------
# 4) accounts.xlsx -> 계정 목록 파싱
# -----------------------------
def _normalize_col(s: str) -> str:
    return (
        str(s)
        .strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )


def load_accounts_from_xlsx(filepath: str) -> List[Dict[str, str]]:
    """
    return: [{"id": "123", "name": "업체명"}...]
    """
    if pd is None:
        raise RuntimeError("pandas가 설치되어 있지 않아 엑셀을 읽을 수 없습니다.")

    if not os.path.exists(filepath):
        return []

    df = pd.read_excel(filepath)

    # 컬럼 후보 자동 탐지
    cols = { _normalize_col(c): c for c in df.columns }

    id_candidates = [
        "customerid", "customid", "custid",
        "커스텀id", "커스텀아이디", "커스텀아이디id",
        "id", "accountid",
    ]
    name_candidates = ["업체명", "accountname", "name", "계정명", "광고주", "회사명"]
    active_candidates = ["활성", "isactive", "사용여부", "사용", "active", "enabled"]
    manager_candidates = ["담당자", "manager", "owner", "담당"]

    id_col = None
    for k in id_candidates:
        nk = _normalize_col(k)
        if nk in cols:
            id_col = cols[nk]
            break

    if id_col is None:
        raise RuntimeError(f"accounts.xlsx에서 customer_id 컬럼을 찾지 못했습니다. 현재 컬럼: {list(df.columns)}")

    name_col = None
    for k in name_candidates:
        nk = _normalize_col(k)
        if nk in cols:
            name_col = cols[nk]
            break

    active_col = None
    for k in active_candidates:
        nk = _normalize_col(k)
        if nk in cols:
            active_col = cols[nk]
            break

    manager_col = None
    for k in manager_candidates:
        nk = _normalize_col(k)
        if nk in cols:
            manager_col = cols[nk]
            break

    # 활성 필터(있을 때만)
    if active_col is not None:
        # 1/0, True/False, 'Y'/'N', '사용' 등 잡아주기
        s = df[active_col].astype(str).str.strip().str.lower()
        df = df[
            s.isin(["1", "true", "t", "y", "yes", "사용", "활성", "on", "enable", "enabled"])
        ]

    # 담당자 필터(환경변수로 지정했을 때만)
    if MANAGER_FILTER and manager_col is not None:
        df = df[df[manager_col].astype(str).str.strip() == MANAGER_FILTER]

    # customer_id 정리
    cid = (
        df[id_col]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    df = df.assign(_cid=cid)
    df = df[df["_cid"].str.len() > 0]

    # name
    if name_col is not None:
        nm = df[name_col].astype(str).fillna("").str.strip()
    else:
        nm = df["_cid"]

    accounts = []
    seen = set()
    for _cid, _nm in zip(df["_cid"].tolist(), nm.tolist()):
        if _cid in seen:
            continue
        seen.add(_cid)
        accounts.append({"id": str(_cid), "name": str(_nm) if _nm else "Unknown"})
    return accounts


# -----------------------------
# 5) DB dim_account_meta fallback
# -----------------------------
def load_accounts_from_db(engine) -> List[Dict[str, str]]:
    accounts = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT customer_id, account_name FROM dim_account_meta")).fetchall()
            accounts = [{"id": str(r[0]), "name": (r[1] or "Unknown")} for r in rows]
    except Exception:
        accounts = []
    return accounts


# -----------------------------
# 6) 메인
# -----------------------------

# -----------------------------
# 3) DB Upsert (retries for transient SSL drop)
# -----------------------------
def upsert_bizmoney_balance(engine, dt: date, cid: str, bal: int, retries: int = 4) -> None:
    """Upsert one row with retries for transient connection drops."""
    stmt = text(
        """
        INSERT INTO fact_bizmoney_daily (dt, customer_id, bizmoney_balance)
        VALUES (:dt, :cid, :bal)
        ON CONFLICT (dt, customer_id)
        DO UPDATE SET bizmoney_balance = EXCLUDED.bizmoney_balance
        """
    )

    for attempt in range(1, retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(stmt, {"dt": dt, "cid": cid, "bal": bal})
            return
        except OperationalError as e:
            msg = str(e).lower()
            transient = (
                "ssl connection has been closed unexpectedly" in msg
                or "server closed the connection unexpectedly" in msg
                or "connection is closed" in msg
                or "could not receive data from server" in msg
                or "could not send data to server" in msg
                or "terminating connection" in msg
            )
            if (not transient) or (attempt == retries):
                raise
            wait_s = min(2 ** attempt, 10)
            print(f"⚠️ DB 연결 불안정 감지 → 재시도 {attempt}/{retries} (대기 {wait_s}s)")
            try:
                engine.dispose()
            except Exception:
                pass
            time.sleep(wait_s)

def main():
    if not DB_URL:
        print("❌ DATABASE_URL이 없습니다.")
        return

    engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800, pool_timeout=30)

    # 테이블 생성 (없으면)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
                    dt DATE,
                    customer_id TEXT,
                    bizmoney_balance BIGINT,
                    PRIMARY KEY(dt, customer_id)
                )
                """
            )
        )

    accounts: List[Dict[str, str]] = []

    # ✅ 1순위: 엑셀
    try:
        accounts = load_accounts_from_xlsx(ACCOUNTS_FILE)
        if accounts:
            print(f"📌 계정 소스: accounts.xlsx ({ACCOUNTS_FILE})")
    except Exception as e:
        print(f"⚠️ accounts.xlsx 로드 실패 → DB로 fallback: {e}")

    # ✅ 2순위: DB
    if not accounts:
        accounts = load_accounts_from_db(engine)
        if accounts:
            print("📌 계정 소스: dim_account_meta(DB)")

    # ✅ 3순위: 단일 env
    if not accounts and CUSTOMER_ID:
        accounts = [{"id": CUSTOMER_ID, "name": "Target Account"}]
        print("📌 계정 소스: ENV(CUSTOMER_ID)")

    print(f"📋 비즈머니 수집 대상: {len(accounts)}개 계정")

    today = date.today()
    success_count = 0
    failed: List[Dict[str, object]] = []

    for acc in accounts:
        cid = acc["id"]
        name = acc.get("name") or "Unknown"

        balance = get_bizmoney(cid)

        if balance is None:
            print(f"❌ {name}({cid}): 수집 실패")
            continue

        try:
            upsert_bizmoney_balance(engine, today, cid, balance)
        except OperationalError as e:
            print(f"❌ {name}({cid}): DB 저장 실패 ({e.__class__.__name__})")
            failed.append({"id": cid, "name": name, "bal": int(balance)})
            continue

        print(f"✅ {name}({cid}): {balance:,}원 저장 완료")
        success_count += 1


    if failed:
        print(f"🔁 DB 저장 실패 {len(failed)}건 → 연결 재생성 후 재시도합니다.")
        try:
            engine.dispose()
        except Exception:
            pass

        still_failed: List[Dict[str, object]] = []
        for item in failed:
            cid2 = str(item["id"])
            name2 = str(item.get("name") or "Unknown")
            bal2 = int(item.get("bal") or 0)
            try:
                upsert_bizmoney_balance(engine, today, cid2, bal2, retries=6)
                print(f"✅(재시도) {name2}({cid2}): {bal2:,}원 저장 완료")
            except Exception as e:
                print(f"❌(재시도) {name2}({cid2}): 저장 최종 실패 - {e.__class__.__name__}")
                still_failed.append(item)

        if still_failed:
            print(f"❌ 최종 실패 {len(still_failed)}건이 남았습니다. 로그 확인 후 재실행하세요.")
            sys.exit(1)

    print(f"🚀 전체 완료: 성공 {success_count} / 전체 {len(accounts)}")


if __name__ == "__main__":
    main()
