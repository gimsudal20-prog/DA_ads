# -*- coding: utf-8 -*-
"""collector_others.py - External media collector skeleton for Meta/Danggeun/Google/Kakao/Criteo."""

from __future__ import annotations

import os
import json
import argparse
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

PLATFORMS = ["meta", "danggeun", "google", "kakao", "criteo"]

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_engine():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL 이 설정되어 있지 않습니다.")
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(db_url, poolclass=NullPool, future=True)

def ensure_platform_credentials_table(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS platform_credentials (
        id BIGSERIAL PRIMARY KEY,
        platform VARCHAR(30) NOT NULL,
        account_label VARCHAR(120) NOT NULL,
        customer_id BIGINT NULL,
        account_id VARCHAR(120) NULL,
        access_token TEXT NULL,
        refresh_token TEXT NULL,
        app_id VARCHAR(200) NULL,
        app_secret TEXT NULL,
        extra_json JSONB DEFAULT '{}'::jsonb,
        is_active BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

def get_active_credentials(engine, platform: str = "") -> pd.DataFrame:
    ensure_platform_credentials_table(engine)
    query = """
        SELECT *
        FROM platform_credentials
        WHERE is_active = TRUE
          AND (:platform = '' OR platform = :platform)
        ORDER BY platform, updated_at DESC, id DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params={"platform": platform})

def normalize_unified_rows(platform: str, rows: list[dict]) -> list[dict]:
    normalized = []
    for row in rows:
        normalized.append({
            "platform": platform,
            "account_id": str(row.get("account_id", "")).strip(),
            "campaign_id": str(row.get("campaign_id", "")).strip(),
            "adgroup_id": str(row.get("adgroup_id", "")).strip(),
            "ad_id": str(row.get("ad_id", "")).strip(),
            "dt": row.get("dt"),
            "imp": float(row.get("imp", 0) or 0),
            "clk": float(row.get("clk", 0) or 0),
            "cost": float(row.get("cost", 0) or 0),
            "conv": float(row.get("conv", 0) or 0),
            "sales": float(row.get("sales", 0) or 0),
            "extra_metrics_json": json.dumps(row.get("extra_metrics_json", {}), ensure_ascii=False),
        })
    return normalized

def collect_meta_daily(engine, target_date: date, credential_row: dict):
    log(f"[META] not implemented yet | {credential_row.get('account_label')} | {target_date}")

def collect_danggeun_daily(engine, target_date: date, credential_row: dict):
    log(f"[DANGGEUN] not implemented yet | {credential_row.get('account_label')} | {target_date}")

def collect_google_daily(engine, target_date: date, credential_row: dict):
    log(f"[GOOGLE] not implemented yet | {credential_row.get('account_label')} | {target_date}")

def collect_kakao_daily(engine, target_date: date, credential_row: dict):
    log(f"[KAKAO] not implemented yet | {credential_row.get('account_label')} | {target_date}")

def collect_criteo_daily(engine, target_date: date, credential_row: dict):
    log(f"[CRITEO] not implemented yet | {credential_row.get('account_label')} | {target_date}")

def dispatch_collect(engine, platform: str, target_date: date, credential_row: dict):
    if platform == "meta":
        return collect_meta_daily(engine, target_date, credential_row)
    if platform == "danggeun":
        return collect_danggeun_daily(engine, target_date, credential_row)
    if platform == "google":
        return collect_google_daily(engine, target_date, credential_row)
    if platform == "kakao":
        return collect_kakao_daily(engine, target_date, credential_row)
    if platform == "criteo":
        return collect_criteo_daily(engine, target_date, credential_row)
    log(f"[SKIP] unsupported platform: {platform}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--platform", type=str, default="", choices=["", *PLATFORMS])
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else (date.today() - timedelta(days=1))
    engine = get_engine()

    creds = get_active_credentials(engine, args.platform)
    if creds.empty:
        log("활성화된 외부 매체 연결 정보가 없습니다.")
        return

    log("=" * 60)
    log(f"외부 매체 수집기 시작 | target_date={target_date} | rows={len(creds)}")
    log("=" * 60)

    for _, row in creds.iterrows():
        try:
            platform = str(row.get("platform", "")).strip().lower()
            dispatch_collect(engine, platform, target_date, row.to_dict())
        except Exception as e:
            log(f"[ERROR] {row.get('platform')} | {row.get('account_label')} | {e}")

if __name__ == "__main__":
    main()
