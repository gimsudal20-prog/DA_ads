# -*- coding: utf-8 -*-
"""Minimal regression checks for budget/bizmoney and parser contracts.

This intentionally avoids importing heavy app modules so it can run in light
CI/local environments.
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path


class RegressionFailure(Exception):
    pass


def _read_ast(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding='utf-8'), filename=str(path))


def _function_names(tree: ast.AST) -> set[str]:
    return {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _find_call_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def check_budget_wrapper(root: Path) -> list[str]:
    messages: list[str] = []
    data_path = root / 'data.py'
    view_path = root / 'view_budget.py'
    if not data_path.exists() or not view_path.exists():
        raise RegressionFailure('data.py 또는 view_budget.py 가 없습니다')

    data_tree = _read_ast(data_path)
    view_tree = _read_ast(view_path)

    data_funcs = _function_names(data_tree)
    if 'query_budget_bundle' not in data_funcs:
        raise RegressionFailure('data.py 에 query_budget_bundle 공개 함수가 없습니다')
    messages.append('ok | data.py query_budget_bundle 공개 wrapper 존재')

    calls = _find_call_names(view_tree)
    if 'query_budget_bundle' not in calls:
        raise RegressionFailure('view_budget.py 가 query_budget_bundle 을 호출하지 않습니다')
    messages.append('ok | view_budget.py query_budget_bundle 호출 유지')
    return messages


def check_budget_cache_helpers(root: Path) -> list[str]:
    data_path = root / 'view_budget.py'
    tree = _read_ast(data_path)
    funcs = _function_names(tree)
    required = {
        '_cached_budget_bundle',
        'render_budget_editor',
        'render_alert_table',
    }
    missing = sorted(required - funcs)
    if missing:
        raise RegressionFailure(f'view_budget.py 필수 함수 누락: {", ".join(missing)}')
    return [f'ok | view_budget.py 필수 함수 유지 ({", ".join(sorted(required))})']


def check_sa_scope_contract(root: Path) -> list[str]:
    collector_path = root / 'collector.py'
    if not collector_path.exists():
        return ['note | collector.py 없음: sa_scope 계약 점검 스킵']
    tree = _read_ast(collector_path)
    funcs = _function_names(tree)
    msgs: list[str] = []
    if 'normalize_sa_scope' in funcs and 'label_sa_scope' in funcs and '--sa_scope' in collector_path.read_text(encoding='utf-8'):
        msgs.append('ok | collector.py sa_scope helper/옵션 유지')
    else:
        msgs.append('note | collector.py sa_scope 직접 지원은 현재 기준 미적용')
    return msgs


def main() -> int:
    parser = argparse.ArgumentParser(description='Run minimal regression checks.')
    parser.add_argument('--repo', default='.', help='repository root path')
    args = parser.parse_args()
    root = Path(args.repo).resolve()

    failures: list[str] = []
    notes: list[str] = []

    checks = [
        check_budget_wrapper,
        check_budget_cache_helpers,
        check_sa_scope_contract,
    ]
    for fn in checks:
        try:
            notes.extend(fn(root))
        except RegressionFailure as exc:
            failures.append(f'{fn.__name__} 실패 | {exc}')
        except Exception as exc:  # pragma: no cover - unexpected infra failure
            failures.append(f'{fn.__name__} 예외 | {type(exc).__name__}: {exc}')

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
