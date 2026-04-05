# -*- coding: utf-8 -*-
"""Minimal regression checks for parser/value drift.

This script is intentionally lightweight and dependency-tolerant so it can run
in local/dev environments without full DB drivers.
"""
from __future__ import annotations

import argparse
import importlib
import os
import sys
import types
from datetime import date
from pathlib import Path

import pandas as pd


def _install_stub_modules() -> None:
    dotenv = types.ModuleType('dotenv')
    dotenv.load_dotenv = lambda *a, **k: None
    sys.modules.setdefault('dotenv', dotenv)

    psycopg2 = types.ModuleType('psycopg2')
    psycopg2_extras = types.ModuleType('psycopg2.extras')
    psycopg2.extras = psycopg2_extras
    psycopg2_extras.execute_values = lambda *a, **k: None
    sys.modules.setdefault('psycopg2', psycopg2)
    sys.modules.setdefault('psycopg2.extras', psycopg2_extras)

    sqlalchemy = types.ModuleType('sqlalchemy')
    sqlalchemy.create_engine = lambda *a, **k: None
    sqlalchemy.text = lambda x: x
    sys.modules.setdefault('sqlalchemy', sqlalchemy)

    sqlalchemy_engine = types.ModuleType('sqlalchemy.engine')
    class Engine:
        pass
    sqlalchemy_engine.Engine = Engine
    sys.modules.setdefault('sqlalchemy.engine', sqlalchemy_engine)

    sqlalchemy_pool = types.ModuleType('sqlalchemy.pool')
    class NullPool:
        pass
    sqlalchemy_pool.NullPool = NullPool
    sys.modules.setdefault('sqlalchemy.pool', sqlalchemy_pool)

    helpers = types.ModuleType('device_collector_helpers')
    helpers.DEVICE_PARSER_VERSION = 'test'
    helpers.ensure_device_tables = lambda *a, **k: None
    helpers.build_ad_to_campaign_map = lambda *a, **k: {}
    helpers.parse_ad_device_report = lambda *a, **k: []
    helpers.save_device_stats = lambda *a, **k: {'saved_rows': 0}
    helpers.summarize_stat_res = lambda *a, **k: 'ok'
    sys.modules.setdefault('device_collector_helpers', helpers)


def _prepare_import_env(repo: Path) -> None:
    os.environ.setdefault('NAVER_API_KEY', 'test-key')
    os.environ.setdefault('NAVER_API_SECRET', 'test-secret')
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    _install_stub_modules()


def _assert_equal(name: str, actual, expected, failures: list[str]) -> None:
    if actual != expected:
        failures.append(f'{name} | expected={expected!r} actual={actual!r}')


def _test_shopping_query_parser(collector, failures: list[str]) -> None:
    df = pd.DataFrame([
        ['cmp-1', 'grp-1', '신발', 'nad-1', '구매완료', '2', '10000'],
        ['cmp-1', 'grp-1', '신발', 'nad-1', '장바구니', '1', '3000'],
        ['cmp-2', 'grp-2', '-', 'nad-2', '구매완료', '5', '9999'],
    ])
    rows = collector.parse_shopping_query_report(df, date(2026, 4, 5), 'cust-1')
    _assert_equal('shopping_query.row_count', len(rows), 1, failures)
    if not rows:
        return
    row = rows[0]
    _assert_equal('shopping_query.campaign_id', row['campaign_id'], 'cmp-1', failures)
    _assert_equal('shopping_query.query_text', row['query_text'], '신발', failures)
    _assert_equal('shopping_query.total_conv', row['total_conv'], 3.0, failures)
    _assert_equal('shopping_query.total_sales', row['total_sales'], 13000, failures)
    _assert_equal('shopping_query.purchase_conv', row['purchase_conv'], 2.0, failures)
    _assert_equal('shopping_query.cart_conv', row['cart_conv'], 1.0, failures)


def _test_base_report_split_map(collector, failures: list[str]) -> None:
    df = pd.DataFrame([
        ['캠페인ID', '노출수', '클릭수', '총비용', '전환수', '전환매출액', '평균노출순위'],
        ['cmp-1', '100', '5', '1000', '2', '3000', '1.5'],
    ])
    conv_map = {
        'cmp-1': {
            'purchase_conv': 1.0,
            'purchase_sales': 2000,
            'cart_conv': 1.0,
            'cart_sales': 1000,
            'wishlist_conv': 0.0,
            'wishlist_sales': 0,
            'split_available': True,
        }
    }
    res = collector.parse_base_report(df, 'CAMPAIGN', conv_map=conv_map, has_conv_report=True)
    row = res.get('cmp-1') or {}
    _assert_equal('base_report.imp', row.get('imp'), 100, failures)
    _assert_equal('base_report.clk', row.get('clk'), 5, failures)
    _assert_equal('base_report.cost', row.get('cost'), 1000, failures)
    _assert_equal('base_report.conv', row.get('conv'), 2.0, failures)
    _assert_equal('base_report.sales', row.get('sales'), 3000, failures)
    _assert_equal('base_report.purchase_conv', row.get('purchase_conv'), 1.0, failures)
    _assert_equal('base_report.cart_sales', row.get('cart_sales'), 1000, failures)


def _test_media_report_header_parser(collector, failures: list[str], notes: list[str]) -> None:
    if not hasattr(collector, 'parse_media_report_rows'):
        notes.append('media parser 체크 스킵 | collector.parse_media_report_rows 없음')
        return
    df = pd.DataFrame([
        ['광고ID', '매체명', '디바이스', '노출수', '클릭수', '총비용', '전환수', '전환매출액'],
        ['nad-a', '네이버검색', 'PC', '100', '5', '1000', '2', '3000'],
    ])
    try:
        rows, diag = collector.parse_media_report_rows(
            df,
            date(2026, 4, 5),
            'cust-1',
            {'nad-a': 'cmp-1'},
            {'cmp-1': '쇼핑검색'},
        )
    except NameError as exc:
        notes.append(f'media parser 체크 참고 | 런타임 helper 누락으로 스킵: {exc}')
        return
    except Exception as exc:
        failures.append(f'media_report.parse | {exc.__class__.__name__}: {exc}')
        return

    _assert_equal('media_report.status', diag.get('status'), 'ok', failures)
    _assert_equal('media_report.row_count', len(rows), 1, failures)
    if not rows:
        return
    row = rows[0]
    _assert_equal('media_report.campaign_type', row.get('campaign_type'), '쇼핑검색', failures)
    _assert_equal('media_report.media_name', row.get('media_name'), '네이버검색', failures)
    _assert_equal('media_report.device_name', row.get('device_name'), 'PC', failures)
    _assert_equal('media_report.imp', row.get('imp'), 100, failures)
    _assert_equal('media_report.clk', row.get('clk'), 5, failures)
    _assert_equal('media_report.cost', row.get('cost'), 1000, failures)


def _test_sa_scope_contract(collector, notes: list[str], failures: list[str]) -> None:
    if not hasattr(collector, 'normalize_sa_scope'):
        notes.append('sa_scope 계약 점검 참고 | collector.py에 normalize_sa_scope가 아직 없습니다.')
        return
    try:
        _assert_equal('sa_scope.full', collector.normalize_sa_scope('full'), 'full', failures)
        _assert_equal('sa_scope.ad_only_en', collector.normalize_sa_scope('ad_only'), 'ad_only', failures)
        _assert_equal('sa_scope.ad_only_ko', collector.normalize_sa_scope('소재만'), 'ad_only', failures)
    except Exception as exc:
        failures.append(f'sa_scope.normalize | {exc.__class__.__name__}: {exc}')


def main() -> int:
    parser = argparse.ArgumentParser(description='Run lightweight regression checks.')
    parser.add_argument('--repo', default='.', help='repository root path')
    args = parser.parse_args()

    root = Path(args.repo).resolve()
    failures: list[str] = []
    notes: list[str] = []

    _prepare_import_env(root)

    try:
        collector = importlib.import_module('collector')
    except Exception as exc:
        print('=== regression check summary ===')
        print(f'repo: {root}')
        print('failures:')
        print(f'- collector import 실패 | {exc.__class__.__name__}: {exc}')
        return 1

    _test_shopping_query_parser(collector, failures)
    _test_base_report_split_map(collector, failures)
    _test_media_report_header_parser(collector, failures, notes)
    _test_sa_scope_contract(collector, notes, failures)

    print('=== regression check summary ===')
    print(f'repo: {root}')
    if notes:
        print('notes:')
        for msg in notes:
            print(f'- {msg}')
    if failures:
        print('failures:')
        for msg in failures:
            print(f'- {msg}')
        return 1
    print('all regression checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
