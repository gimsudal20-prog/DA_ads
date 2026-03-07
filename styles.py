# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* ✨ 세련된 모던 인디고 테마로 컬러 팔레트 전면 수정 */
  --nv-bg: #FFFFFF;
  --nv-surface: #F8FAFC;  /* 더 부드러운 슬레이트 톤 배경 */
  --nv-panel: #F1F5F9;
  --nv-line: #E2E8F0;
  --nv-line-strong: #CBD5E1;
  --nv-text: #0F172A;     /* 더 짙고 선명한 텍스트 */
  --nv-muted: #64748B;
  --nv-muted-light: #94A3B8;
  
  --nv-primary: #6366F1;       /* 세련된 인디고 (기본 파란색 대체) */
  --nv-primary-hover: #4F46E5; /* 인디고 호버 색상 */
  --nv-primary-soft: #EEF2FF;  /* 아주 연한 인디고 (배경용) */
  
  --nv-success: #10B981;  /* 에메랄드 그린 */
  --nv-warning: #F59E0B;  /* 앰버 */
  --nv-danger: #EF4444;   /* 로즈 레드 */
  --nv-radius: 12px;
}

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, 'Helvetica Neue', 'Segoe UI', sans-serif !important;
  color: var(--nv-text);
  background: var(--nv-bg);
}

h1, h2, h3, h4, h5, h6 {
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: var(--nv-text);
}

/* =========================================
   🔘 Streamlit 기본 버튼 디자인 전면 커스텀 
   ========================================= */

/* 일반(Secondary) 버튼 */
button[data-testid="baseButton-secondary"] {
    background-color: #FFFFFF !important;
    border: 1px solid var(--nv-line-strong) !important;
    color: var(--nv-text) !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    padding: 4px 16px !important;
    transition: all 0.2s ease-in-out !important;
    box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05) !important;
}

button[data-testid="baseButton-secondary"]:hover {
    border-color: var(--nv-primary) !important;
    color: var(--nv-primary) !important;
    background-color: var(--nv-primary-soft) !important;
    box-shadow: 0 4px 6px -1px rgba(99, 102, 241, 0.1), 0 2px 4px -1px rgba(99, 102, 241, 0.06) !important;
    transform: translateY(-1px);
}

/* 주요(Primary) 버튼 (예: 🚀 데이터 덮어쓰기 시작 버튼) */
button[data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, var(--nv-primary) 0%, var(--nv-primary-hover) 100%) !important;
    border: none !important;
    color: #FFFFFF !important;
    border-radius: 10px !important;
    font-weight: 700 !important;
    padding: 4px 16px !important;
    transition: all 0.2s ease-in-out !important;
    box-shadow: 0 4px 6px -1px rgba(99, 102, 241, 0.3) !important;
}

button[data-testid="baseButton-primary"]:hover {
    box-shadow: 0 10px 15px -3px rgba(99, 102, 241, 0.4), 0 4px 6px -2px rgba(99, 102, 241, 0.2) !important;
    transform: translateY(-2px);
}

/* 토글(Toggle) 스위치 색상 변경 */
[data-testid="stCheckbox"] label span[data-baseweb="checkbox"] {
    background-color: var(--nv-primary) !important;
}

/* =========================================
   📊 대시보드 커스텀 UI 디자인
   ========================================= */

.nv-h1 {
  font-size: 24px;
  font-weight: 800;
  letter-spacing: -0.03em;
  margin: 4px 0 8px;
}

.nv-sec-title {
  font-size: 18px;
  font-weight: 700;
  margin-top: 24px;
  margin-bottom: 8px;
  color: var(--nv-text);
  display: flex;
  align-items: center;
  gap: 8px;
}

.nv-hero {
  background: linear-gradient(180deg, #FFFFFF 0%, var(--nv-surface) 100%);
  border: 1px solid var(--nv-line);
  border-radius: 16px;
  padding: 20px 24px;
  margin-bottom: 24px;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
}

.nv-hero-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 20px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.nv-hero-brand {
  display: flex;
  align-items: center;
  gap: 16px;
}

.nv-hero-title {
  margin: 0;
  font-size: 28px;
  line-height: 1.2;
  font-weight: 800;
}

.nv-hero-sub {
  margin-top: 6px;
  color: var(--nv-muted);
  font-size: 13px;
  font-weight: 600;
}

.nv-fresh-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: var(--nv-primary-soft);
  color: var(--nv-primary-hover);
  border: 1px solid #C7D2FE;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
}

.nv-hero-kpis {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
}

.nv-hero-kpi {
  background: #FFFFFF;
  border: 1px solid var(--nv-line);
  border-radius: 10px;
  padding: 12px;
  box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.02);
}

.nv-hero-kpi .k {
  color: var(--nv-muted);
  font-size: 12px;
  font-weight: 600;
  margin-bottom: 4px;
}

.nv-hero-kpi .v {
  color: var(--nv-text);
  font-size: 18px;
  font-weight: 800;
}

.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: #FFFFFF; border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 16px; box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.02); }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-muted); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }

.kpi { background: var(--nv-surface); border: 1px solid var(--nv-line); padding: 14px; border-radius: 10px; transition: all 0.2s ease; }
.kpi:hover { border-color: #A5B4FC; background: #FFFFFF; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); transform: translateY(-2px); }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 700; margin-top: 6px; display: inline-block; padding: 2px 8px; border-radius: 999px; }
.kpi.highlight { border-color: var(--nv-primary); background: var(--nv-primary-soft); }
.kpi.highlight .v { color: var(--nv-primary-hover); font-size: 22px; }
.kpi .d.pos { background: #D1FAE5; color: var(--nv-success); }
.kpi .d.neg { background: #FEE2E2; color: var(--nv-danger); }
.kpi .d.neu { background: #F1F5F9; color: var(--nv-muted); }

.nv-metric-card {
  background: var(--nv-bg);
  padding: 20px;
  border-radius: 12px;
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
}
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 700; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 4px 10px; border-radius: 999px; }

/* 데이터 테이블 스타일 */
table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; border: 1px solid var(--nv-line); border-radius: 10px; overflow: hidden; }
table.nv-table th { position: sticky; top: 0; z-index: 2; background: var(--nv-surface); padding: 13px 16px; font-weight: 700; color: var(--nv-muted); border-bottom: 1px solid var(--nv-line-strong); }
table.nv-table td { padding: 13px 16px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); transition: all 0.2s ease; }
table.nv-table tr:hover td { background: var(--nv-primary-soft); color: var(--nv-primary-hover); }

/* 프로그레스 바 */
.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

/* 탭(Tabs) 디자인 */
[data-baseweb="tab-list"] { gap: 16px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 600; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: var(--nv-primary) !important; font-weight: 800 !important; border-bottom: 2px solid var(--nv-primary) !important; box-shadow: none !important; }

/* 아코디언(Expander) 디자인 */
[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: 0 1px 2px 0 rgba(0,0,0,0.02) !important; background: var(--nv-bg) !important; }
[data-testid="stExpander"] summary { padding: 16px !important; background-color: var(--nv-surface) !important; border-radius: var(--nv-radius) !important;}
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }

/* =========================================
   좌측 사이드바 디자인
   ========================================= */
[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #F8FAFC 0%, #F1F5F9 100%) !important;
  border-right: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] .block-container { padding-top: 0.9rem !important; }
.nav-sidebar-title { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.nav-sidebar-caption { margin-top: 4px; margin-bottom: 10px; font-size: 12px; color: var(--nv-muted); font-weight: 600; }

/* 사이드바 라디오 버튼(메뉴) 블럭화 */
[data-testid="stSidebar"] [role="radiogroup"] {
  background: #FFFFFF;
  border: 1px solid var(--nv-line);
  border-radius: 12px;
  padding: 8px;
  box-shadow: 0 1px 2px 0 rgba(0,0,0,0.02);
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 10px 12px !important;
  border-radius: 8px !important;
  border: 1px solid transparent;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover {
  background: var(--nv-surface) !important;
  border-color: var(--nv-line);
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: var(--nv-primary) !important;
  border-color: var(--nv-primary-hover) !important;
  box-shadow: 0 2px 4px -1px rgba(99, 102, 241, 0.3) !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
  color: #FFFFFF !important;
  font-weight: 700 !important;
}

/* 드롭다운 리스트박스 커스텀 */
div[role="listbox"] ul li,
[data-baseweb="menu"] [role="option"] {
  border-radius: 6px !important;
  box-shadow: none !important;
}

div[role="listbox"] ul li:hover,
div[role="listbox"] ul li[aria-selected="true"],
[data-baseweb="menu"] [role="option"]:hover,
[data-baseweb="menu"] [role="option"][aria-selected="true"] {
  background-color: var(--nv-primary) !important;
  color: #FFFFFF !important;
}

div[role="listbox"] ul li:hover *,
div[role="listbox"] ul li[aria-selected="true"] *,
[data-baseweb="menu"] [role="option"]:hover *,
[data-baseweb="menu"] [role="option"][aria-selected="true"] * {
  color: #FFFFFF !important;
  font-weight: 700 !important;
}

/* 태그 색상 */
[data-baseweb="tag"] {
  background-color: var(--nv-primary-soft) !important;
  border: 1px solid #C7D2FE !important;
}
[data-baseweb="tag"] * { color: var(--nv-primary-hover) !important; font-weight: 700 !important; }
</style>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
