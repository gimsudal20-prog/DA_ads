# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  --nv-bg: #FFFFFF;
  --nv-surface: #F7F8FA;
  --nv-panel: #F2F4F7;
  --nv-line: #E4E7EC;
  --nv-line-strong: #D0D5DD;
  --nv-text: #101828;
  --nv-muted: #475467;
  --nv-muted-light: #98A2B3;
  --nv-primary: #335CFF;
  --nv-primary-soft: #EEF2FF;
  --nv-success: #17B26A;
  --nv-warning: #F79009;
  --nv-danger: #F04438;
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
  background: linear-gradient(180deg, #FFFFFF 0%, #FCFCFD 100%);
  border: 1px solid var(--nv-line);
  border-radius: 16px;
  padding: 20px 24px;
  margin-bottom: 24px;
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
  color: var(--nv-primary);
  border: 1px solid #C7D7FE;
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
  background: var(--nv-surface);
  border: 1px solid var(--nv-line);
  border-radius: 10px;
  padding: 12px;
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
.kpi-group { flex: 1; min-width: 250px; background: var(--nv-surface); border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 16px; }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-muted); margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }

.kpi { background: var(--nv-bg); border: 1px solid var(--nv-line); padding: 14px; border-radius: 10px; transition: all 0.2s ease; }
.kpi:hover { border-color: #B8C8FE; background: var(--nv-primary-soft); }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 700; margin-top: 6px; display: inline-block; padding: 2px 8px; border-radius: 999px; }
.kpi.highlight { border-color: var(--nv-primary); background: var(--nv-primary-soft); }
.kpi.highlight .v { color: var(--nv-primary); font-size: 22px; }
.kpi .d.pos { background: #EAFBF3; color: var(--nv-success); }
.kpi .d.neg { background: #FFF1F3; color: var(--nv-danger); }
.kpi .d.neu { background: #EEF2F6; color: var(--nv-muted); }

.nv-metric-card {
  background: var(--nv-bg);
  padding: 20px;
  border-radius: 12px;
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
}
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 700; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 4px 10px; border-radius: 999px; }

table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; border: 1px solid var(--nv-line); border-radius: 10px; overflow: hidden; }
table.nv-table th { position: sticky; top: 0; z-index: 2; background: #F9FAFB; padding: 13px 16px; font-weight: 700; color: var(--nv-muted); border-bottom: 1px solid var(--nv-line); }
table.nv-table td { padding: 13px 16px; border-bottom: 1px solid #F2F4F7; vertical-align: middle; color: var(--nv-text); transition: all 0.2s ease; }
table.nv-table tr:hover td { background: var(--nv-primary-soft); color: #1D3DC0; }

.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: #EDF2F7; overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

[data-baseweb="tab-list"] { gap: 16px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 600; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: var(--nv-primary) !important; font-weight: 800 !important; border-bottom: 2px solid var(--nv-primary) !important; box-shadow: none !important; }

[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; background: var(--nv-bg) !important; }
[data-testid="stExpander"] summary { padding: 16px !important; background-color: var(--nv-surface) !important; border-radius: var(--nv-radius) !important;}
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #F8FAFF 0%, #F2F6FF 100%) !important;
  border-right: 1px solid #D7E1FF !important;
}
[data-testid="stSidebar"] .block-container { padding-top: 0.9rem !important; }
.nav-sidebar-title { font-size: 18px; font-weight: 800; color: #1E3A8A; letter-spacing: -0.02em; }
.nav-sidebar-caption { margin-top: 4px; margin-bottom: 10px; font-size: 12px; color: #64748B; font-weight: 600; }
[data-testid="stSidebar"] [role="radiogroup"] {
  background: #FFFFFF;
  border: 1px solid #D8E3FF;
  border-radius: 12px;
  padding: 8px;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 10px 12px !important;
  border-radius: 10px !important;
  border: 1px solid transparent;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover {
  background: #EDF2FF !important;
  border-color: #CBD9FF;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: linear-gradient(90deg, #335CFF 0%, #4F73FF 100%) !important;
  border-color: #3459E6 !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
  color: #FFFFFF !important;
  font-weight: 800 !important;
}

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

[data-baseweb="tag"] {
  background-color: var(--nv-primary) !important;
  border: 1px solid #2748C9 !important;
}
[data-baseweb="tag"] * { color: #FFFFFF !important; }
</style>
"""


def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
