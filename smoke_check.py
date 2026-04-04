# -*- coding: utf-8 -*-
"""Lightweight smoke checks for local/GitHub Actions use.

Default checks:
- py_compile on every .py file under the repo
- optional YAML parse for .github/workflows/*.yml when PyYAML is installed

Optional extra checks:
- --with-help: run selected CLI scripts with --help
"""
from __future__ import annotations

import argparse
import ast
import os
import py_compile
import subprocess
import sys
from pathlib import Path


def iter_python_files(root: Path):
    for path in root.rglob("*.py"):
        if any(part in {".venv", "venv", "__pycache__"} for part in path.parts):
            continue
        yield path


def run_py_compile(root: Path) -> list[str]:
    errors: list[str] = []
    for path in iter_python_files(root):
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"py_compile 실패 | {path.relative_to(root)} | {exc.msg}")
    return errors


def run_yaml_parse(root: Path) -> list[str]:
    workflows_dir = root / ".github" / "workflows"
    if not workflows_dir.exists():
        return []

    try:
        import yaml  # type: ignore
    except Exception:
        return ["PyYAML 미설치: workflow YAML 파싱은 건너뜀"]

    messages: list[str] = []
    for path in sorted(workflows_dir.glob("*.yml")):
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            messages.append(f"workflow 파싱 실패 | {path.relative_to(root)} | {exc}")
    return messages


def _module_public_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[-1])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[-1])
    return names


def run_local_import_checks(root: Path) -> list[str]:
    messages: list[str] = []
    module_cache: dict[Path, set[str]] = {}

    for path in iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except Exception as exc:
            messages.append(f"import 체크 스킵 | {path.relative_to(root)} | 파싱 실패: {exc}")
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level != 0 or not node.module or any(alias.name == "*" for alias in node.names):
                continue
            mod_path = root / (node.module.replace(".", os.sep) + ".py")
            if not mod_path.exists():
                continue
            if mod_path not in module_cache:
                module_cache[mod_path] = _module_public_names(mod_path)
            exported = module_cache[mod_path]
            for alias in node.names:
                imported_name = alias.name
                if imported_name not in exported:
                    messages.append(
                        f"import 체크 실패 | {path.relative_to(root)} | from {node.module} import {imported_name}"
                    )
    return messages


def run_help_checks(root: Path) -> list[str]:
    scripts = [
        "collector.py",
        "fast_backfill.py",
    ]
    messages: list[str] = []
    for script_name in scripts:
        script_path = root / script_name
        if not script_path.exists():
            messages.append(f"help 체크 스킵 | {script_name} 없음")
            continue
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            cwd=str(root),
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or proc.stdout).strip().splitlines()
            tail = stderr[-1] if stderr else "출력 없음"
            messages.append(f"help 체크 실패 | {script_name} | {tail}")
    return messages


def main() -> int:
    parser = argparse.ArgumentParser(description="Run lightweight smoke checks.")
    parser.add_argument("--repo", default=".", help="repository root path")
    parser.add_argument("--with-help", action="store_true", help="also run selected scripts with --help")
    args = parser.parse_args()

    root = Path(args.repo).resolve()
    failures: list[str] = []
    notes: list[str] = []

    compile_errors = run_py_compile(root)
    failures.extend(compile_errors)

    yaml_messages = run_yaml_parse(root)
    for msg in yaml_messages:
        if "실패" in msg:
            failures.append(msg)
        else:
            notes.append(msg)

    import_messages = run_local_import_checks(root)
    failures.extend(import_messages)

    if args.with_help:
        help_messages = run_help_checks(root)
        for msg in help_messages:
            failures.append(msg)

    print("=== smoke check summary ===")
    print(f"repo: {root}")
    print(f"py files checked: {sum(1 for _ in iter_python_files(root))}")
    if notes:
        print("notes:")
        for msg in notes:
            print(f"- {msg}")

    if failures:
        print("failures:")
        for msg in failures:
            print(f"- {msg}")
        return 1

    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
