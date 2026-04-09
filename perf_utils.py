# -*- coding: utf-8 -*-
"""perf_utils.py - lightweight runtime profiling helpers for Streamlit pages."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

_TRUTHY = {"1", "true", "yes", "on", "y"}


def _ss():
    try:
        return st.session_state
    except Exception:
        return {}


def perf_enabled() -> bool:
    env_flag = str(os.getenv("DASH_PROFILE", "0") or "0").strip().lower() in _TRUTHY
    try:
        return bool(env_flag or _ss().get("_perf_enabled", False))
    except Exception:
        return env_flag


def set_perf_enabled(enabled: bool) -> None:
    try:
        _ss()["_perf_enabled"] = bool(enabled)
    except Exception:
        pass


def perf_reset(page_name: str = "") -> None:
    if not perf_enabled():
        return
    ss = _ss()
    ss["_perf_log"] = []
    ss["_perf_started_at"] = time.perf_counter()
    ss["_perf_page_name"] = str(page_name or "")


def perf_add(section: str, elapsed_ms: float, rows: Any = None, note: str = "", kind: str = "step") -> None:
    if not perf_enabled():
        return
    ss = _ss()
    logs = list(ss.get("_perf_log", []) or [])
    logs.append({
        "kind": str(kind or "step"),
        "section": str(section or "(unknown)"),
        "ms": round(float(elapsed_ms or 0.0), 2),
        "rows": "-" if rows is None else str(rows),
        "note": str(note or "")[:240],
    })
    if len(logs) > 500:
        logs = logs[-500:]
    ss["_perf_log"] = logs


@contextmanager
def perf_span(section: str, rows: Any = None, note: str = "", kind: str = "step"):
    if not perf_enabled():
        yield
        return
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        perf_add(section, elapsed_ms, rows=rows, note=note, kind=kind)


def render_perf_panel(expanded: bool = False) -> None:
    if not perf_enabled():
        return
    logs = list(_ss().get("_perf_log", []) or [])
    if not logs:
        return

    started = _ss().get("_perf_started_at")
    total_ms = None
    if started is not None:
        try:
            total_ms = round((time.perf_counter() - float(started)) * 1000.0, 2)
        except Exception:
            total_ms = None

    df = pd.DataFrame(logs)
    if df.empty:
        return

    with st.expander("속도 진단", expanded=expanded):
        cols = st.columns(3)
        cols[0].metric("단계 수", f"{len(df):,}")
        cols[1].metric("총 시간", f"{total_ms:,.1f} ms" if total_ms is not None else "-")
        cols[2].metric("페이지", str(_ss().get("_perf_page_name", "-") or "-"))
        st.caption("아래 표는 현재 렌더에서 실행된 단계/쿼리 시간입니다. 느린 항목부터 확인하세요.")

        if "ms" in df.columns:
            slow_df = df.sort_values("ms", ascending=False).reset_index(drop=True)
        else:
            slow_df = df.copy()
        st.markdown("**느린 순 상위 단계**")
        st.dataframe(slow_df.head(30), width="stretch", hide_index=True)

        st.markdown("**전체 단계 로그**")
        st.dataframe(df.reset_index(drop=True), width="stretch", hide_index=True)
