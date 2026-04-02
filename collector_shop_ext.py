# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 확장소재 수집기
- shopping: 쇼핑검색만
- non_shopping: 쇼핑검색 외(파워링크/플레이스/브랜드검색 등)
- all: 전체
"""

import os
import time
import json
import hmac
import base64
import hashlib
import argparse
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2.extras
from sqlalchemy.pool import NullPool

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
SHOPPING_HINT_KEYS = ["shopping", "catalog", "shop", "쇼핑"]


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


def request_json(method: str, path: str, customer_id: str, params: dict | None = None):
    url = BASE_URL + path
    ts = now_millis()
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sign_path_only(method.upper(), path, ts, API_SECRET),
    }
    for attempt in range(4):
        try:
            r = requests.request(method, url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in [429, 500, 502, 503, 504]:
                time.sleep(2 + attempt)
                continue
            log(f"⚠️ API 응답 오류 | {path} | status={r.status_code} | body={r.text[:300]}")
            return None
        except Exception as e:
            log(f"⚠️ API 요청 예외 | {path} | {e}")
            time.sleep(2 + attempt)
    return None


def get_engine():
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    return create_engine(db_url, poolclass=NullPool, future=True)


def upsert_many(engine, table: str, rows: list, pk_cols: list):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep='last')
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict = (
        f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict}'
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn, cur = None, None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    except Exception as e:
        log(f"⚠️ {table} 저장 중 오류: {e}")
        if raw_conn:
            raw_conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if raw_conn:
            raw_conn.close()


def is_shopping_campaign_obj(camp: dict) -> bool:
    hay = " ".join([
        str(camp.get("campaignTp", "")),
        str(camp.get("campaignType", "")),
        str(camp.get("type", "")),
        str(camp.get("name", "")),
    ]).lower()
    return any(k in hay for k in SHOPPING_HINT_KEYS)


def flatten_extensions(payload) -> list[dict]:
    out = []
    def walk(x):
        if not x:
            return
        if isinstance(x, list):
            for item in x:
                walk(item)
            return
        if isinstance(x, dict):
            if x.get('nccAdExtensionId') or x.get('adExtension') or x.get('extensionType') or x.get('type'):
                out.append(x)
                nested = x.get('adExtension')
                if isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, dict):
                            obj = dict(item)
                            obj.setdefault('nccAdExtensionId', x.get('nccAdExtensionId'))
                            obj.setdefault('extensionType', x.get('extensionType') or x.get('type'))
                            out.append(obj)
                return
            for v in x.values():
                walk(v)
    walk(payload)
    dedup, seen = [], set()
    for item in out:
        key = (str(item.get('nccAdExtensionId') or ''), str(item.get('extensionType') or item.get('type') or ''), json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)[:200])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)
    return dedup


def _iter_string_values(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_string_values(item)
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_string_values(v)


def _first_field(obj, field_names: list[str], url_only: bool | None = None) -> str:
    if isinstance(obj, list):
        for item in obj:
            val = _first_field(item, field_names, url_only=url_only)
            if val:
                return val
        return ''
    if not isinstance(obj, dict):
        return ''
    for name in field_names:
        if name in obj:
            val = obj.get(name)
            if isinstance(val, str) and val.strip():
                if url_only is True and not val.startswith('http'):
                    continue
                if url_only is False and val.startswith('http'):
                    continue
                return val.strip()
            if isinstance(val, (list, dict)):
                nested = _first_field(val, field_names, url_only=url_only)
                if nested:
                    return nested
    for v in obj.values():
        if isinstance(v, (list, dict)):
            nested = _first_field(v, field_names, url_only=url_only)
            if nested:
                return nested
    return ''


def parse_ext_name(ext) -> str:
    ext_info = ext.get('adExtension', {}) if isinstance(ext, dict) else ext
    if isinstance(ext_info, list):
        text_chunks = []
        for item in ext_info:
            if isinstance(item, dict):
                text = _first_field(item, ['promoText', 'addPromoText', 'subLinkName', 'pcText', 'mobileText', 'description', 'title', 'text'], url_only=False)
                if text:
                    text_chunks.append(text)
        ext_type = (ext.get('extensionType') if isinstance(ext, dict) else None) or (ext.get('type') if isinstance(ext, dict) else None) or '확장소재'
        if text_chunks:
            return f"[확장소재] {ext_type} | {' / '.join(text_chunks[:3])}"
        return f"[확장소재] {ext_type} | {str(ext_info)[:150]}"

    if not isinstance(ext_info, dict):
        ext_info = ext if isinstance(ext, dict) else {'text': str(ext)}
    ext_type = ext.get('extensionType') if isinstance(ext, dict) else None
    ext_type = ext_type or (ext.get('type') if isinstance(ext, dict) else None) or ext_info.get('extensionType') or ext_info.get('type') or '확장소재'

    text_val = _first_field(ext_info, ['promoText', 'addPromoText', 'subLinkName', 'pcText', 'mobileText', 'description', 'title', 'text'], url_only=False)
    if not text_val:
        vals = [
            s for s in _iter_string_values(ext_info)
            if s and not str(s).startswith('http') and str(s).strip() not in {'-', ''}
        ]
        text_val = ' / '.join(vals[:3]) if vals else str(ext_info)[:150]
    return f"[확장소재] {ext_type} | {text_val}"


def process_account(engine, customer_id: str, target_date: date, ext_bucket: str = 'shopping'):
    bucket_label = {'shopping': '쇼핑검색', 'non_shopping': '파워링크외', 'all': '전체'}.get(ext_bucket, ext_bucket)
    log(f"--- [ {customer_id} ] {bucket_label} 확장소재 수집 시작 ({target_date}) ---")

    camps = request_json('GET', '/ncc/campaigns', customer_id) or []
    if not camps:
        return

    shop_camps = [c for c in camps if is_shopping_campaign_obj(c)]
    non_shop_camps = [c for c in camps if not is_shopping_campaign_obj(c)]
    if ext_bucket == 'shopping':
        target_camps = shop_camps
    elif ext_bucket == 'non_shopping':
        target_camps = non_shop_camps
    else:
        target_camps = list(camps)

    log(f"   ▶ 대상 캠페인 {len(target_camps)}개 | 쇼핑검색 {len(shop_camps)}개 | 파워링크외 {len(non_shop_camps)}개")

    camp_rows, ag_rows, ad_rows = [], [], []
    target_ad_ids = []

    for c in target_camps:
        cid = c.get('nccCampaignId')
        if not cid:
            continue
        camp_rows.append({
            'customer_id': str(customer_id), 'campaign_id': str(cid),
            'campaign_name': c.get('name'), 'campaign_tp': c.get('campaignTp') or c.get('campaignType') or c.get('type'), 'status': c.get('status')
        })

        camp_exts = flatten_extensions(request_json('GET', '/ncc/ad-extensions', customer_id, {'ownerId': cid}) or [])
        if camp_exts:
            ag_rows.append({
                'customer_id': str(customer_id), 'adgroup_id': f'CAMP_{cid}', 'campaign_id': str(cid),
                'adgroup_name': '[캠페인 공통 소재]', 'status': 'ELIGIBLE'
            })
            for ext in camp_exts:
                ext_id = ext.get('nccAdExtensionId') or ext.get('id')
                if not ext_id:
                    continue
                target_ad_ids.append(str(ext_id))
                ext_info = ext.get('adExtension', {}) if isinstance(ext, dict) else {}
                display_name = parse_ext_name(ext)
                ad_rows.append({
                    'customer_id': str(customer_id), 'ad_id': str(ext_id), 'adgroup_id': f'CAMP_{cid}',
                    'ad_name': display_name, 'status': ext.get('status') if isinstance(ext, dict) else None,
                    'ad_title': display_name, 'ad_desc': display_name,
                    'pc_landing_url': _first_field(ext_info, ['pcLandingUrl'], url_only=True),
                    'mobile_landing_url': _first_field(ext_info, ['mobileLandingUrl'], url_only=True),
                    'creative_text': display_name[:500]
                })

        groups = request_json('GET', '/ncc/adgroups', customer_id, {'nccCampaignId': cid}) or []
        for g in groups:
            gid = g.get('nccAdgroupId')
            if not gid:
                continue
            ag_rows.append({
                'customer_id': str(customer_id), 'adgroup_id': str(gid), 'campaign_id': str(cid),
                'adgroup_name': g.get('name'), 'status': g.get('status')
            })
            extensions = flatten_extensions(request_json('GET', '/ncc/ad-extensions', customer_id, {'ownerId': gid}) or [])
            for ext in extensions:
                ext_id = ext.get('nccAdExtensionId') or ext.get('id')
                if not ext_id:
                    continue
                target_ad_ids.append(str(ext_id))
                ext_info = ext.get('adExtension', {}) if isinstance(ext, dict) else {}
                display_name = parse_ext_name(ext)
                ad_rows.append({
                    'customer_id': str(customer_id), 'ad_id': str(ext_id), 'adgroup_id': str(gid),
                    'ad_name': display_name, 'status': ext.get('status') if isinstance(ext, dict) else None,
                    'ad_title': display_name, 'ad_desc': display_name,
                    'pc_landing_url': _first_field(ext_info, ['pcLandingUrl'], url_only=True),
                    'mobile_landing_url': _first_field(ext_info, ['mobileLandingUrl'], url_only=True),
                    'creative_text': display_name[:500]
                })

    target_ad_ids = list(dict.fromkeys([x for x in target_ad_ids if x]))

    upsert_many(engine, 'dim_campaign', camp_rows, ['customer_id', 'campaign_id'])
    upsert_many(engine, 'dim_adgroup', ag_rows, ['customer_id', 'adgroup_id'])
    upsert_many(engine, 'dim_ad', ad_rows, ['customer_id', 'ad_id'])
    log(f"   ▶ 캠페인({len(camp_rows)}), 광고그룹({len(ag_rows)}), 확장소재({len(ad_rows)}) 매핑 완료")

    if not target_ad_ids:
        log('   ⚠️ 조회된 확장소재 ID가 없습니다.')
        return

    log(f"   ▶ 확장소재 {len(target_ad_ids)}개 실시간 통계 조회 중...")
    d_str = target_date.strftime('%Y-%m-%d')
    fields = json.dumps(['impCnt', 'clkCnt', 'salesAmt', 'ccnt', 'convAmt'], separators=(',', ':'))
    time_range = json.dumps({'since': d_str, 'until': d_str}, separators=(',', ':'))

    raw_stats = []
    for i in range(0, len(target_ad_ids), 50):
        chunk = target_ad_ids[i:i+50]
        params = {'ids': ','.join(chunk), 'fields': fields, 'timeRange': time_range}
        res = request_json('GET', '/stats', customer_id, params=params)
        if res and isinstance(res, dict) and 'data' in res:
            raw_stats.extend(res['data'] or [])

    fact_rows = []
    for r in raw_stats:
        ad_id = r.get('id')
        if not ad_id:
            continue
        cost = int(float(r.get('salesAmt', 0) or 0))
        sales = int(float(r.get('convAmt', 0) or 0))
        fact_rows.append({
            'dt': target_date, 'customer_id': str(customer_id), 'ad_id': str(ad_id),
            'imp': int(r.get('impCnt', 0) or 0), 'clk': int(r.get('clkCnt', 0) or 0),
            'cost': cost, 'conv': float(r.get('ccnt', 0) or 0), 'sales': sales,
            'roas': (sales / cost * 100.0) if cost > 0 else 0.0,
            'avg_rnk': 0.0,
        })

    if fact_rows:
        upsert_many(engine, 'fact_ad_daily', fact_rows, ['dt', 'customer_id', 'ad_id'])
        log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공")
    else:
        log('   ⚠️ 조회된 날짜에 노출/클릭이 발생한 확장소재가 없습니다.')


def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default='')
    parser.add_argument('--account_name', type=str, default='')
    parser.add_argument('--account_names', type=str, default='')
    parser.add_argument('--ext_bucket', type=str, default='shopping', choices=['shopping', 'non_shopping', 'all', '쇼핑검색', '파워링크외', '전체'])
    args = parser.parse_args()

    bucket_map = {'쇼핑검색': 'shopping', '파워링크외': 'non_shopping', '전체': 'all'}
    ext_bucket = bucket_map.get(args.ext_bucket, args.ext_bucket)
    target_date = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else date.today() - timedelta(days=1)

    print('\n' + '='*50, flush=True)
    print(f"🧩 확장소재 수집기 [날짜: {target_date}]", flush=True)
    print('='*50 + '\n', flush=True)
    bucket_label = {'shopping': '쇼핑검색', 'non_shopping': '파워링크외', 'all': '전체'}.get(ext_bucket, ext_bucket)
    log(f"🧩 확장소재 수집 버킷: {bucket_label} ({ext_bucket})")

    accounts = []
    if load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=['sa'])
            accounts = [str(r['id']).strip() for r in rows if str(r.get('id', '')).strip()]
        except Exception as e:
            log(f"⚠️ account_master 로드 실패, dim_account_meta 로 폴백합니다: {e}")

    if not accounts:
        try:
            with engine.connect() as conn:
                accounts = [str(r[0]) for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta WHERE COALESCE(naver_media_type, 'sa') <> 'gfa'"))]
        except Exception:
            pass

    if not accounts:
        cid = os.getenv('CUSTOMER_ID')
        if cid:
            accounts = [cid]

    target_name_tokens = []
    if getattr(args, 'account_name', ''):
        target_name_tokens.append(str(args.account_name).strip())
    if getattr(args, 'account_names', ''):
        target_name_tokens.extend([x.strip() for x in str(args.account_names).split(',') if x.strip()])

    if target_name_tokens and load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=['sa'])
            exact_set = {x for x in target_name_tokens}
            filtered = [r for r in rows if r['name'] in exact_set]
            if not filtered:
                lowered = [x.lower() for x in target_name_tokens]
                filtered = [r for r in rows if any(tok in r['name'].lower() for tok in lowered)]
            if filtered:
                accounts = [str(r['id']).strip() for r in filtered]
                log(f"🎯 업체명 필터 적용: {', '.join(target_name_tokens)} -> {len(accounts)}개")
        except Exception:
            pass

    for acc in accounts:
        process_account(engine, acc, target_date, ext_bucket)


if __name__ == '__main__':
    main()
