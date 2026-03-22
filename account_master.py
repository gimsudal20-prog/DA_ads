# -*- coding: utf-8 -*-
"""Unified account master loader for Naver/Meta collectors.

지원 포맷
1) 단일 마스터(account_master.xlsx)
   - sheet: 계정마스터
   - 컬럼 예시: 담당자, 업체그룹명, 계정표시명, 사용여부, 플랫폼, 네이버 매체유형,
                네이버 수집ID, 메타 광고계정ID, 메타 픽셀ID, 비즈머니 그룹키, 수집기본값,
                첫날 구조동기화, 비고
2) 기존 레거시(accounts.xlsx)
   - 컬럼 예시: 업체명, 커스텀 ID, 담당자
   - 이름 끝이 ' GFA' 이면 네이버 GFA 로 간주
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd


def _norm(s: object) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip().lower()


def _clean_text(v: object) -> str:
    s = str(v or "").strip()
    return "" if s.lower() == "nan" else s


def _clean_id(v: object) -> str:
    s = _clean_text(v)
    if not s:
        return ""
    # excel float 방지
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def _bool_to_yn(v: object, default: str = "Y") -> str:
    s = _clean_text(v).lower()
    if not s:
        return default
    if s in {"y", "yes", "true", "1", "사용", "사용함"}:
        return "Y"
    if s in {"n", "no", "false", "0", "미사용", "중지"}:
        return "N"
    return default


def _find_existing_file(explicit: Optional[str] = None) -> Optional[str]:
    candidates = [
        explicit,
        os.getenv("ACCOUNT_MASTER_FILE"),
        "account_master.xlsx",
        "accounts.xlsx",
    ]
    for c in candidates:
        if c and Path(c).exists():
            return str(Path(c))
    return None


def _read_master_sheet(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    sheet_name = "계정마스터" if "계정마스터" in xls.sheet_names else xls.sheet_names[0]
    preview = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=15)
    header_row = 0
    for i, row in preview.iterrows():
        vals = {_norm(x) for x in row.tolist() if _clean_text(x)}
        if {"담당자", "업체그룹명", "계정표시명"}.issubset(vals):
            header_row = i
            break
        if {"업체명", "커스텀id"}.issubset(vals):
            header_row = i
            break
    return pd.read_excel(path, sheet_name=sheet_name, header=header_row)


def _canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    # new master aliases
    alias_map = {
        "담당자": "manager",
        "manager": "manager",
        "업체그룹명": "client_group_name",
        "업체그룹": "client_group_name",
        "groupname": "client_group_name",
        "계정표시명": "account_name",
        "업체명": "account_name",
        "accountname": "account_name",
        "계정명": "account_name",
        "사용여부": "use_yn",
        "사용": "use_yn",
        "platform": "platform",
        "플랫폼": "platform",
        "네이버매체유형": "naver_media_type",
        "매체유형": "naver_media_type",
        "navermediatype": "naver_media_type",
        "네이버수집id": "customer_id",
        "커스텀id": "customer_id",
        "customerid": "customer_id",
        "customer_id": "customer_id",
        "id": "customer_id",
        "메타광고계정id": "meta_ad_account_id",
        "메타픽셀id": "meta_pixel_id",
        "비즈머니그룹키": "bizmoney_group_key",
        "수집기본값": "collect_mode",
        "collectmode": "collect_mode",
        "첫날구조동기화": "sync_dim_first_day",
        "비고": "note",
    }
    rename = {}
    for c in df.columns:
        key = alias_map.get(_norm(c))
        if key:
            rename[c] = key
    out = df.rename(columns=rename).copy()

    # legacy accounts.xlsx fallback
    if "account_name" not in out.columns and "업체명" in df.columns:
        out["account_name"] = df["업체명"]
    if "customer_id" not in out.columns:
        for c in df.columns:
            if _norm(c) in {"커스텀id", "customerid", "customer_id", "id"}:
                out["customer_id"] = df[c]
                break
    if "manager" not in out.columns:
        for c in df.columns:
            if _norm(c) in {"담당자", "manager", "owner"}:
                out["manager"] = df[c]
                break

    for col in ["manager", "client_group_name", "account_name", "platform", "naver_media_type", "bizmoney_group_key", "collect_mode", "note"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_clean_text)

    for col in ["customer_id", "meta_ad_account_id", "meta_pixel_id"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_clean_id)

    if "use_yn" not in out.columns:
        out["use_yn"] = "Y"
    out["use_yn"] = out["use_yn"].map(lambda x: _bool_to_yn(x, "Y"))

    if "sync_dim_first_day" not in out.columns:
        out["sync_dim_first_day"] = "N"
    out["sync_dim_first_day"] = out["sync_dim_first_day"].map(lambda x: _bool_to_yn(x, "N"))

    # legacy inference
    if not out["platform"].astype(str).str.strip().any():
        out["platform"] = "naver"
    else:
        out["platform"] = out["platform"].replace({"": "naver"}).fillna("naver")

    if not out["account_name"].astype(str).str.strip().any() and "client_group_name" in out.columns:
        out["account_name"] = out["client_group_name"]
    if not out["client_group_name"].astype(str).str.strip().any():
        out["client_group_name"] = out["account_name"].map(lambda x: re.sub(r"\s+GFA$", "", _clean_text(x), flags=re.I))

    if not out["naver_media_type"].astype(str).str.strip().any():
        out["naver_media_type"] = out["account_name"].map(lambda x: "gfa" if re.search(r"\s+GFA$", _clean_text(x), re.I) else "sa")
    else:
        out["naver_media_type"] = out["naver_media_type"].replace({"": None}).fillna(
            out["account_name"].map(lambda x: "gfa" if re.search(r"\s+GFA$", _clean_text(x), re.I) else "sa")
        )
        out["naver_media_type"] = out["naver_media_type"].map(lambda x: _clean_text(x).lower() or "sa")

    if not out["bizmoney_group_key"].astype(str).str.strip().any():
        out["bizmoney_group_key"] = out["client_group_name"]
    else:
        out["bizmoney_group_key"] = out["bizmoney_group_key"].replace({"": None}).fillna(out["client_group_name"])

    if not out["collect_mode"].astype(str).str.strip().any():
        out["collect_mode"] = out["platform"].map(lambda p: "meta_only" if p == "meta" else "naver_only")

    # remove decorative rows
    if "customer_id" in out.columns:
        has_naver = out["customer_id"].astype(str).str.strip().ne("")
    else:
        has_naver = pd.Series(False, index=out.index)
    has_meta = out.get("meta_ad_account_id", pd.Series("", index=out.index)).astype(str).str.strip().ne("")
    has_name = out["account_name"].astype(str).str.strip().ne("")
    out = out[(has_name) & (has_naver | has_meta)]

    cols = [
        "manager", "client_group_name", "account_name", "use_yn", "platform", "naver_media_type",
        "customer_id", "meta_ad_account_id", "meta_pixel_id", "bizmoney_group_key",
        "collect_mode", "sync_dim_first_day", "note",
    ]
    return out[cols].reset_index(drop=True)


def load_account_master_df(file_path: Optional[str] = None, include_disabled: bool = False) -> pd.DataFrame:
    path = _find_existing_file(file_path)
    if not path:
        return pd.DataFrame(columns=[
            "manager", "client_group_name", "account_name", "use_yn", "platform", "naver_media_type",
            "customer_id", "meta_ad_account_id", "meta_pixel_id", "bizmoney_group_key",
            "collect_mode", "sync_dim_first_day", "note",
        ])
    df = _read_master_sheet(path)
    df = _canonicalize(df)
    if not include_disabled:
        df = df[df["use_yn"] == "Y"].copy()
    return df.reset_index(drop=True)


def load_naver_accounts(file_path: Optional[str] = None, include_gfa: bool = False, media_types: Optional[Iterable[str]] = None) -> List[Dict[str, str]]:
    df = load_account_master_df(file_path=file_path, include_disabled=False)
    if df.empty:
        return []
    df = df[df["platform"].str.lower().eq("naver")].copy()
    if media_types is not None:
        allowed = {str(x).strip().lower() for x in media_types if str(x).strip()}
        df = df[df["naver_media_type"].str.lower().isin(allowed)]
    elif not include_gfa:
        df = df[~df["naver_media_type"].str.lower().eq("gfa")]
    df = df[df["customer_id"].astype(str).str.strip().ne("")].copy()
    seen = set()
    rows = []
    for _, r in df.iterrows():
        cid = str(r["customer_id"]).strip()
        if not cid or cid in seen:
            continue
        rows.append({
            "id": cid,
            "name": str(r["account_name"]).strip(),
            "manager": str(r.get("manager", "")).strip(),
            "group_name": str(r.get("client_group_name", "")).strip(),
            "media_type": str(r.get("naver_media_type", "sa")).strip().lower(),
            "bizmoney_group_key": str(r.get("bizmoney_group_key", "")).strip() or str(r.get("client_group_name", "")).strip(),
            "sync_dim_first_day": str(r.get("sync_dim_first_day", "N")).strip(),
            "note": str(r.get("note", "")).strip(),
        })
        seen.add(cid)
    return rows


def load_meta_accounts(file_path: Optional[str] = None) -> List[Dict[str, str]]:
    df = load_account_master_df(file_path=file_path, include_disabled=False)
    if df.empty:
        return []
    df = df[df["platform"].str.lower().eq("meta")].copy()
    df = df[df["meta_ad_account_id"].astype(str).str.strip().ne("")].copy()
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "id": str(r["meta_ad_account_id"]).strip(),
            "name": str(r["account_name"]).strip(),
            "manager": str(r.get("manager", "")).strip(),
            "group_name": str(r.get("client_group_name", "")).strip(),
            "pixel_id": str(r.get("meta_pixel_id", "")).strip(),
        })
    return rows


def load_bizmoney_groups(file_path: Optional[str] = None) -> List[Dict[str, object]]:
    accounts = load_naver_accounts(file_path=file_path, include_gfa=True, media_types=["sa", "gfa"])
    if not accounts:
        return []
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for acc in accounts:
        key = acc.get("bizmoney_group_key") or acc.get("group_name") or acc["name"]
        grouped.setdefault(key, []).append(acc)
    groups = []
    for key, members in grouped.items():
        members = sorted(members, key=lambda x: (0 if x.get("media_type") == "sa" else 1, x.get("name", "")))
        rep = members[0]
        groups.append({
            "bizmoney_group_key": key,
            "representative": rep,
            "members": members,
            "has_gfa": any(m.get("media_type") == "gfa" for m in members),
            "has_sa": any(m.get("media_type") == "sa" for m in members),
        })
    return groups
