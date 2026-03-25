# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from account_master import load_naver_accounts

load_dotenv(override=False)

DB_URL = (os.getenv("DATABASE_URL") or "").strip()
ACCOUNT_MASTER_FILE = (os.getenv("ACCOUNT_MASTER_FILE") or "account_master.xlsx").strip()

# 공식 GFA API는 Bearer Access Token 방식이다.
# 다만 기존 사용자가 GFA_ID / GFA_PW 라는 이름으로 client id/secret 을 저장했을 수 있어 alias 로도 읽는다.
GFA_ACCESS_TOKEN = (os.getenv("GFA_ACCESS_TOKEN") or "").strip()
GFA_REFRESH_TOKEN = (os.getenv("GFA_REFRESH_TOKEN") or "").strip()
GFA_CLIENT_ID = (os.getenv("GFA_CLIENT_ID") or os.getenv("GFA_ID") or "").strip()
GFA_CLIENT_SECRET = (os.getenv("GFA_CLIENT_SECRET") or os.getenv("GFA_PW") or "").strip()
GFA_MANAGER_ACCOUNT_NO = (os.getenv("GFA_MANAGER_ACCOUNT_NO") or "").strip()
GFA_API_VERSION = (os.getenv("GFA_API_VERSION") or "1.0").strip()
TIMEOUT = int((os.getenv("GFA_TIMEOUT") or "60").strip())

TOKEN_URL = "https://nid.naver.com/oauth2.0/token"
OPENAPI_BASE = "https://openapi.naver.com"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"❌ FATAL: {msg}")
    raise SystemExit(code)


def get_engine() -> Engine:
    if not DB_URL:
        die("DATABASE_URL 이 설정되지 않았습니다.")
    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url,
        poolclass=NullPool,
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=300000"},
        future=True,
    )


def ensure_column(engine: Engine, table: str, column: str, datatype: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {table} ADD COLUMN "{column}" {datatype}'))
    except Exception:
        pass


def ensure_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, image_url TEXT, PRIMARY KEY(customer_id, ad_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"))
    for table in ["fact_campaign_daily", "fact_ad_daily"]:
        ensure_column(engine, table, "purchase_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "purchase_sales", "BIGINT")
        ensure_column(engine, table, "purchase_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "cart_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "cart_sales", "BIGINT")
        ensure_column(engine, table, "cart_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "wishlist_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "wishlist_sales", "BIGINT")
        ensure_column(engine, table, "wishlist_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "primary_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "primary_sales", "BIGINT")
        ensure_column(engine, table, "primary_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "split_available", "BOOLEAN")
        ensure_column(engine, table, "data_source", "TEXT")


def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    if update_cols:
        conflict_clause = f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT ({pk_str}) DO NOTHING'
    sql = f"INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        raw_conn.close()


def clear_fact_range(engine: Engine, table: str, customer_id: str, target_dt: date) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table} WHERE customer_id = :cid AND dt = :dt"), {"cid": str(customer_id), "dt": target_dt})


def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, target_dt: date) -> None:
    clear_fact_range(engine, table, customer_id, target_dt)
    if not rows:
        return
    pk = "campaign_id" if table == "fact_campaign_daily" else "ad_id"
    df = pd.DataFrame(rows).drop_duplicates(subset=["dt", "customer_id", pk], keep="last").astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ["dt", "customer_id", pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])
    conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    sql = f"INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        raw_conn.close()


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return default


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _pick_first(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _pick_id(d: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        if k in d:
            iv = _to_int(d.get(k))
            if iv is not None:
                return str(iv)
            sv = _to_str(d.get(k))
            if sv:
                return sv
    return None


def _contains_value(obj: Any, target: str) -> bool:
    t = str(target)
    if isinstance(obj, dict):
        return any(_contains_value(v, t) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_value(v, t) for v in obj)
    return str(obj) == t


def _walk_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_dicts(v)


def _extract_items(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if not isinstance(obj, dict):
        return []
    for key in ["rows", "items", "contents", "content", "data", "list", "result", "results", "campaigns", "adSets", "creatives"]:
        v = obj.get(key)
        if isinstance(v, list) and any(isinstance(x, dict) for x in v):
            return [x for x in v if isinstance(x, dict)]
    for d in _walk_dicts(obj):
        for v in d.values():
            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                return v
    return []


def issue_or_refresh_access_token() -> str:
    if GFA_ACCESS_TOKEN:
        return GFA_ACCESS_TOKEN
    if GFA_REFRESH_TOKEN and GFA_CLIENT_ID and GFA_CLIENT_SECRET:
        params = {
            "grant_type": "refresh_token",
            "client_id": GFA_CLIENT_ID,
            "client_secret": GFA_CLIENT_SECRET,
            "refresh_token": GFA_REFRESH_TOKEN,
        }
        r = requests.get(TOKEN_URL, params=params, timeout=TIMEOUT)
        data = {}
        try:
            data = r.json()
        except Exception:
            pass
        if r.status_code >= 400 or not data.get("access_token"):
            raise RuntimeError(f"GFA access token 갱신 실패: {r.status_code} {data or r.text}")
        return str(data["access_token"])
    die(
        "GFA 인증 정보가 부족합니다. 최소한 GFA_ACCESS_TOKEN 또는 GFA_REFRESH_TOKEN + GFA_CLIENT_ID/GFA_CLIENT_SECRET 이 필요합니다. "
        "참고로 GFA_ID/GFA_PW 만으로는 공식 GFA API 호출이 바로 되지 않습니다."
    )


class GfaClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.session = requests.Session()

    def request_json(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, access_manager_account_no: Optional[str] = None, raise_for_status: bool = True) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if access_manager_account_no:
            headers["AccessManagerAccountNo"] = str(access_manager_account_no)
        url = OPENAPI_BASE + path
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = self.session.request(method.upper(), url, headers=headers, params=params, timeout=TIMEOUT)
                data: Any
                try:
                    data = r.json()
                except Exception:
                    data = r.text
                if r.status_code >= 400:
                    if raise_for_status:
                        raise RuntimeError(f"{r.status_code} {data}")
                    return {"_status": r.status_code, "_data": data}
                return data
            except Exception as e:
                last_err = e
                if attempt < 3:
                    time.sleep(1.5 + attempt)
                    continue
                if raise_for_status:
                    raise
                return {"_status": 0, "_data": str(last_err)}
        if last_err:
            raise last_err
        raise RuntimeError("unexpected request failure")

    def list_ad_accounts(self) -> List[Dict[str, Any]]:
        page = 0
        out: List[Dict[str, Any]] = []
        while True:
            data = self.request_json("GET", f"/v1/ad-api/{GFA_API_VERSION}/adAccounts", params={"page": page, "size": 100}, raise_for_status=False)
            items = _extract_items(data)
            if not items:
                break
            out.extend(items)
            if len(items) < 100:
                break
            page += 1
        return out

    def list_manager_accounts(self) -> List[Dict[str, Any]]:
        page = 0
        out: List[Dict[str, Any]] = []
        while True:
            data = self.request_json("GET", f"/v1/ad-api/{GFA_API_VERSION}/managerAccounts", params={"page": page, "size": 100}, raise_for_status=False)
            items = _extract_items(data)
            if not items:
                break
            out.extend(items)
            if len(items) < 100:
                break
            page += 1
        return out

    def resolve_manager_account_no(self, ad_account_no: str) -> str:
        if GFA_MANAGER_ACCOUNT_NO:
            return GFA_MANAGER_ACCOUNT_NO

        ad_accounts = self.list_ad_accounts()
        for item in ad_accounts:
            acc_no = _pick_id(item, ["adAccountNo", "accountNo", "no", "id"])
            if acc_no == str(ad_account_no):
                mgr = _pick_id(item, ["accessManagerAccountNo", "managerAccountNo", "managerNo", "parentManagerAccountNo"])
                if mgr:
                    return mgr

        managers = self.list_manager_accounts()
        manager_nos: List[str] = []
        for item in managers:
            mgr = _pick_id(item, ["managerAccountNo", "accessManagerAccountNo", "no", "id"])
            if mgr:
                manager_nos.append(mgr)
        manager_nos = list(dict.fromkeys(manager_nos))

        if len(manager_nos) == 1:
            return manager_nos[0]

        for mgr in manager_nos:
            detail = self.request_json(
                "GET",
                f"/v1/ad-api/{GFA_API_VERSION}/managerAccounts/{mgr}",
                access_manager_account_no=mgr,
                raise_for_status=False,
            )
            if _contains_value(detail, str(ad_account_no)):
                return mgr

        die(
            f"광고계정 {ad_account_no} 의 AccessManagerAccountNo 를 자동 판별하지 못했습니다. "
            f"GitHub Secret 에 GFA_MANAGER_ACCOUNT_NO 를 추가해 주세요."
        )

    def paginate(self, path: str, *, params: Optional[Dict[str, Any]] = None, access_manager_account_no: Optional[str] = None) -> List[Dict[str, Any]]:
        params = dict(params or {})
        out: List[Dict[str, Any]] = []
        next_token: Optional[str] = None
        page = 0
        while True:
            if "limit" in params or "next" in params:
                if next_token:
                    params["next"] = next_token
                else:
                    params.setdefault("limit", 1000)
            else:
                params["page"] = page
                params.setdefault("size", 100)
            data = self.request_json("GET", path, params=params, access_manager_account_no=access_manager_account_no)
            items = _extract_items(data)
            out.extend(items)
            if "next" in params or "limit" in params:
                next_token = _to_str(data.get("next")) if isinstance(data, dict) else ""
                if not next_token:
                    break
            else:
                if len(items) < int(params.get("size", 100)):
                    break
                page += 1
        return out


def normalize_campaign_rows(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        campaign_id = _pick_id(it, ["campaignNo", "id", "no"])
        if not campaign_id:
            continue
        name = _to_str(_pick_first(it, ["name", "campaignName", "title"])) or f"campaign_{campaign_id}"
        status = _to_str(_pick_first(it, ["status", "deliveryStatus"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        objective = _to_str(_pick_first(it, ["objective", "campaignObjective"])) or "GFA"
        rows.append({
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "campaign_name": name,
            "campaign_tp": f"gfa_{objective.lower()}",
            "status": status,
        })
    return rows


def normalize_adset_rows(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        adset_id = _pick_id(it, ["adSetNo", "id", "no"])
        if not adset_id:
            continue
        campaign_id = _pick_id(it, ["campaignNo", "parentCampaignNo", "campaignId"])
        name = _to_str(_pick_first(it, ["name", "adSetName", "title"])) or f"adset_{adset_id}"
        status = _to_str(_pick_first(it, ["status", "deliveryStatus"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        rows.append({
            "customer_id": str(customer_id),
            "adgroup_id": adset_id,
            "adgroup_name": name,
            "campaign_id": campaign_id or "",
            "status": status,
        })
    return rows


def normalize_creative_rows(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        creative_id = _pick_id(it, ["creativeNo", "id", "no"])
        if not creative_id:
            continue
        adset_id = _pick_id(it, ["adSetNo", "adgroup_id", "adGroupNo"])
        title = _to_str(_pick_first(it, ["name", "creativeName", "title", "headline"]))
        desc = _to_str(_pick_first(it, ["description", "desc", "subTitle", "body"]))
        status = _to_str(_pick_first(it, ["status", "deliveryStatus"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        pc_url = _to_str(_pick_first(it, ["pcLandingUrl", "landingUrl", "url"]))
        mo_url = _to_str(_pick_first(it, ["mobileLandingUrl", "mobileUrl", "landingUrl"]))
        image_url = _to_str(_pick_first(it, ["imageUrl", "thumbnailUrl"]))
        rows.append({
            "customer_id": str(customer_id),
            "ad_id": creative_id,
            "adgroup_id": adset_id or "",
            "ad_name": title or f"creative_{creative_id}",
            "status": status,
            "ad_title": title,
            "ad_desc": desc,
            "pc_landing_url": pc_url,
            "mobile_landing_url": mo_url,
            "creative_text": " | ".join([x for x in [title, desc] if x]),
            "image_url": image_url,
        })
    return rows


def fetch_dimension_lists(client: GfaClient, customer_id: str, manager_no: str) -> Dict[str, List[Dict[str, Any]]]:
    dims: Dict[str, List[Dict[str, Any]]] = {"campaigns": [], "adsets": [], "creatives": []}

    # docs 상 캠페인 목록 path 가 api/open 버전으로 표기되어 있어 두 경로를 모두 시도
    campaign_paths = [
        f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/campaigns",
        f"/v1/ad-api/api/open/{GFA_API_VERSION}/adAccounts/{customer_id}/campaigns",
    ]
    for p in campaign_paths:
        try:
            items = client.paginate(p, access_manager_account_no=manager_no)
            if items:
                dims["campaigns"] = items
                break
        except Exception:
            continue

    try:
        dims["adsets"] = client.paginate(f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/adSets", access_manager_account_no=manager_no)
    except Exception:
        dims["adsets"] = []

    try:
        dims["creatives"] = client.paginate(f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/creatives", access_manager_account_no=manager_no)
    except Exception:
        dims["creatives"] = []

    return dims


def fetch_past_performance(client: GfaClient, customer_id: str, manager_no: str, target_dt: date, aggregation_type: str) -> List[Dict[str, Any]]:
    params = {
        "startDate": target_dt.isoformat(),
        "endDate": target_dt.isoformat(),
        "timeUnit": "daily",
        "limit": 1000,
    }
    path = f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/performance/past/{aggregation_type}"
    return client.paginate(path, params=params, access_manager_account_no=manager_no)


def build_campaign_fact_rows(customer_id: str, target_dt: date, perf_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in perf_rows:
        campaign_id = _pick_id(r, ["campaignNo", "campaign_id", "id"])
        if not campaign_id:
            continue
        cost = int(round(_to_float(_pick_first(r, ["sales", "cost", "spend"]), 0.0)))
        conv_sales = int(round(_to_float(_pick_first(r, ["convSales", "salesConv"]), 0.0)))
        conv = _to_float(_pick_first(r, ["convCount", "conversions"]), 0.0)
        roas = (conv_sales / cost * 100.0) if cost > 0 else 0.0
        out.append({
            "dt": target_dt,
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "imp": int(_to_float(_pick_first(r, ["impCount", "impressions"]), 0.0)),
            "clk": int(_to_float(_pick_first(r, ["clickCount", "clicks"]), 0.0)),
            "cost": cost,
            "conv": conv,
            "sales": conv_sales,
            "roas": roas,
            "avg_rnk": 0.0,
            "purchase_conv": conv,
            "purchase_sales": conv_sales,
            "purchase_roas": roas,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "cart_roas": 0.0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "wishlist_roas": 0.0,
            "primary_conv": conv,
            "primary_sales": conv_sales,
            "primary_roas": roas,
            "split_available": False,
            "data_source": "gfa_api",
        })
    return out


def build_ad_fact_rows(customer_id: str, target_dt: date, perf_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in perf_rows:
        ad_id = _pick_id(r, ["creativeNo", "ad_id", "id"])
        if not ad_id:
            continue
        cost = int(round(_to_float(_pick_first(r, ["sales", "cost", "spend"]), 0.0)))
        conv_sales = int(round(_to_float(_pick_first(r, ["convSales", "salesConv"]), 0.0)))
        conv = _to_float(_pick_first(r, ["convCount", "conversions"]), 0.0)
        roas = (conv_sales / cost * 100.0) if cost > 0 else 0.0
        out.append({
            "dt": target_dt,
            "customer_id": str(customer_id),
            "ad_id": ad_id,
            "imp": int(_to_float(_pick_first(r, ["impCount", "impressions"]), 0.0)),
            "clk": int(_to_float(_pick_first(r, ["clickCount", "clicks"]), 0.0)),
            "cost": cost,
            "conv": conv,
            "sales": conv_sales,
            "roas": roas,
            "avg_rnk": 0.0,
            "purchase_conv": conv,
            "purchase_sales": conv_sales,
            "purchase_roas": roas,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "cart_roas": 0.0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "wishlist_roas": 0.0,
            "primary_conv": conv,
            "primary_sales": conv_sales,
            "primary_roas": roas,
            "split_available": False,
            "data_source": "gfa_api",
        })
    return out


def upsert_placeholder_dims_from_perf(engine: Engine, customer_id: str, campaign_rows: List[Dict[str, Any]], creative_rows: List[Dict[str, Any]]) -> None:
    camp_dim: List[Dict[str, Any]] = []
    ad_dim: List[Dict[str, Any]] = []
    adgroup_dim_map: Dict[str, Dict[str, Any]] = {}
    for r in campaign_rows:
        cid = _pick_id(r, ["campaignNo"])
        if cid:
            camp_dim.append({
                "customer_id": str(customer_id),
                "campaign_id": cid,
                "campaign_name": f"campaign_{cid}",
                "campaign_tp": "gfa",
                "status": "",
            })
    for r in creative_rows:
        ad_id = _pick_id(r, ["creativeNo"])
        adset_id = _pick_id(r, ["adSetNo"])
        campaign_id = _pick_id(r, ["campaignNo"])
        if adset_id and adset_id not in adgroup_dim_map:
            adgroup_dim_map[adset_id] = {
                "customer_id": str(customer_id),
                "adgroup_id": adset_id,
                "adgroup_name": f"adset_{adset_id}",
                "campaign_id": campaign_id or "",
                "status": "",
            }
        if ad_id:
            ad_dim.append({
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "adgroup_id": adset_id or "",
                "ad_name": f"creative_{ad_id}",
                "status": "",
                "ad_title": "",
                "ad_desc": "",
                "pc_landing_url": "",
                "mobile_landing_url": "",
                "creative_text": "",
                "image_url": "",
            })
    if camp_dim:
        upsert_many(engine, "dim_campaign", camp_dim, ["customer_id", "campaign_id"])
    if adgroup_dim_map:
        upsert_many(engine, "dim_adgroup", list(adgroup_dim_map.values()), ["customer_id", "adgroup_id"])
    if ad_dim:
        upsert_many(engine, "dim_ad", ad_dim, ["customer_id", "ad_id"])


def filter_accounts(accounts: List[Dict[str, str]], account_name: str, account_names: str) -> List[Dict[str, str]]:
    out = accounts
    single = _to_str(account_name)
    multi_raw = _to_str(account_names)
    if single:
        out = [a for a in out if _to_str(a.get("name")) == single]
    if multi_raw:
        wanted = {x.strip() for x in multi_raw.split(",") if x.strip()}
        out = [a for a in out if _to_str(a.get("name")) in wanted]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="NAVER GFA past performance collector")
    parser.add_argument("--date", dest="target_date", default="", help="수집일 (YYYY-MM-DD). 미입력 시 어제")
    parser.add_argument("--account_name", default="", help="단일 업체명")
    parser.add_argument("--account_names", default="", help="여러 업체명 콤마구분")
    args = parser.parse_args()

    if args.target_date:
        target_dt = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    else:
        target_dt = date.today() - timedelta(days=1)

    access_token = issue_or_refresh_access_token()
    client = GfaClient(access_token)
    engine = get_engine()
    ensure_tables(engine)

    accounts = load_naver_accounts(file_path=ACCOUNT_MASTER_FILE, include_gfa=True, media_types=["gfa"])
    accounts = filter_accounts(accounts, args.account_name, args.account_names)
    if not accounts:
        die("account_master 기준 GFA 계정이 없습니다. account_master.xlsx 에 naver_media_type=gfa 계정이 있어야 합니다.")

    log(f"🗂️ GFA 계정 로드: {len(accounts)}개")

    errors: List[str] = []
    for acc in accounts:
        customer_id = _to_str(acc.get("id"))
        account_name = _to_str(acc.get("name")) or customer_id
        if not customer_id:
            continue
        log(f"🚀 [{account_name}] ({customer_id}) 수집 시작 | {target_dt.isoformat()}")
        try:
            manager_no = client.resolve_manager_account_no(customer_id)
            log(f"   ↳ AccessManagerAccountNo = {manager_no}")

            upsert_many(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])

            dims = fetch_dimension_lists(client, customer_id, manager_no)
            dim_campaign_rows = normalize_campaign_rows(customer_id, dims.get("campaigns", []))
            dim_adset_rows = normalize_adset_rows(customer_id, dims.get("adsets", []))
            dim_creative_rows = normalize_creative_rows(customer_id, dims.get("creatives", []))

            if dim_campaign_rows:
                upsert_many(engine, "dim_campaign", dim_campaign_rows, ["customer_id", "campaign_id"])
            if dim_adset_rows:
                upsert_many(engine, "dim_adgroup", dim_adset_rows, ["customer_id", "adgroup_id"])
            if dim_creative_rows:
                upsert_many(engine, "dim_ad", dim_creative_rows, ["customer_id", "ad_id"])

            perf_campaigns = fetch_past_performance(client, customer_id, manager_no, target_dt, "campaigns")
            perf_creatives = fetch_past_performance(client, customer_id, manager_no, target_dt, "creatives")

            if not dim_campaign_rows or not dim_adset_rows or not dim_creative_rows:
                upsert_placeholder_dims_from_perf(engine, customer_id, perf_campaigns, perf_creatives)

            fact_campaign_rows = build_campaign_fact_rows(customer_id, target_dt, perf_campaigns)
            fact_ad_rows = build_ad_fact_rows(customer_id, target_dt, perf_creatives)

            replace_fact_range(engine, "fact_campaign_daily", fact_campaign_rows, customer_id, target_dt)
            replace_fact_range(engine, "fact_ad_daily", fact_ad_rows, customer_id, target_dt)

            log(
                f"✅ [{account_name}] 완료 | campaigns={len(fact_campaign_rows)} rows, creatives={len(fact_ad_rows)} rows"
            )
        except Exception as e:
            msg = f"[{account_name}] {customer_id} 실패: {e}"
            errors.append(msg)
            log(f"❌ {msg}")

    if errors:
        die("; ".join(errors))
    log("🎉 GFA 수집 완료")


if __name__ == "__main__":
    main()
