# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* Brand Colors */
  --nv-primary: #2563FF;
  --nv-primary-hover: #1144E6;
  --nv-primary-soft: #EEF4FF;
  --nv-primary-tint: rgba(37, 99, 255, 0.08);

  /* Apple-inspired neutrals */
  --nv-bg: #F5F7FB;
  --nv-surface: rgba(255, 255, 255, 0.86);
  --nv-surface-2: #F8FAFD;
  --nv-surface-3: #EFF3F8;
  --nv-line: rgba(15, 23, 42, 0.08);
  --nv-line-strong: rgba(15, 23, 42, 0.12);
  --nv-muted-light: #9AA4B2;
  --nv-muted: #667085;
  --nv-text: #111827;

  /* Status Colors */
  --nv-success: #2563FF;
  --nv-warning: #D97706;
  --nv-danger: #DC2626;

  /* Geometry */
  --nv-radius: 14px;
  --nv-radius-lg: 22px;
  --nv-shadow-soft: 0 10px 35px rgba(15, 23, 42, 0.05);
  --nv-shadow-hover: 0 18px 45px rgba(15, 23, 42, 0.08);
}

/* =========================================
   Base Layout & Typography
   ========================================= */
footer { display: none !important; }
header[data-testid="stHeader"] { background-color: transparent !important; }
[data-testid="stHeaderActionElements"] { display: none !important; }
[data-testid="stSidebarNav"] { display: none !important; }

div.block-container {
  padding-top: 2rem !important;
  padding-bottom: 4rem !important;
  max-width: 1540px;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
}

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
  color: var(--nv-text);
  background:
    radial-gradient(circle at top left, rgba(37, 99, 255, 0.07), transparent 28%),
    radial-gradient(circle at top right, rgba(255, 255, 255, 0.9), transparent 22%),
    var(--nv-bg);
}

h1, h2, h3, h4, h5, h6 {
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: var(--nv-text);
}

/* =========================================
   Page Components
   ========================================= */
.nv-page-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 28px;
  padding: 24px 28px;
  background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,255,255,0.82));
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  box-shadow: var(--nv-shadow-soft);
  backdrop-filter: blur(14px);
}
.nv-page-head-left { min-width: 0; }
.nv-page-kicker {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  margin-bottom: 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.7);
  border: 1px solid var(--nv-line);
  color: var(--nv-muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.nv-page-desc {
  margin: 8px 0 0 0;
  color: var(--nv-muted);
  font-size: 14px;
  line-height: 1.6;
  max-width: 780px;
}
.nv-h1 {
  font-size: 28px;
  line-height: 1.18;
  font-weight: 800;
  letter-spacing: -0.03em;
  color: var(--nv-text);
  margin: 0 0 6px 0;
}
.nv-page-sub {
  color: var(--nv-muted);
  font-size: 14px;
  line-height: 1.55;
  margin: 0;
}

.nv-section {
  background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(255,255,255,0.84));
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 24px 28px;
  margin-top: 24px;
  box-shadow: var(--nv-shadow-soft);
  backdrop-filter: blur(12px);
}
.nv-section-muted {
  background: var(--nv-surface);
  border: none;
}
.nv-section-head {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:16px;
  margin-bottom:16px;
}
.nv-sec-eyebrow {
  display:inline-flex;
  align-items:center;
  gap:6px;
  margin:0 0 6px 0;
  color: var(--nv-muted-light);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.nv-sec-title {
  font-size: 18px;
  line-height: 1.3;
  font-weight: 750;
  margin: 0;
  color: var(--nv-text);
  display: flex;
  align-items: center;
  gap: 8px;
  letter-spacing: -0.015em;
}
.nv-sec-sub {
  margin: 6px 0 0 0;
  color: var(--nv-muted);
  font-size: 13px;
  line-height: 1.55;
  font-weight: 500;
}
.nv-card-title {
  margin: 0 0 10px 0;
  color: var(--nv-text);
  font-size: 14px;
  line-height: 1.4;
  font-weight: 700;
  letter-spacing: -0.01em;
}
.nv-card-sub {
  margin: 4px 0 0 0;
  color: var(--nv-muted);
  font-size: 12px;
  line-height: 1.5;
  font-weight: 500;
}

/* =========================================
   Metric Cards
   ========================================= */
.nv-metric-card {
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(255,255,255,0.88));
  padding: 22px;
  border-radius: 20px;
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
  transition: all 0.2s ease;
  box-shadow: var(--nv-shadow-soft);
  backdrop-filter: blur(12px);
}
.nv-metric-card:hover {
  border-color: var(--nv-primary);
  box-shadow: var(--nv-shadow-hover);
  transform: translateY(-2px);
}
.nv-metric-card-title {
  color: var(--nv-muted);
  font-size: 13px;
  font-weight: 650;
  margin-bottom: 12px;
  letter-spacing: -0.01em;
}
.nv-metric-card-value {
  color: var(--nv-text);
  font-size: 30px;
  font-weight: 800;
  letter-spacing: -0.03em;
  line-height: 1.08;
}
.nv-metric-card-desc {
  color: var(--nv-primary);
  font-size: 12px;
  font-weight: 700;
  margin-top: 14px;
  background: var(--nv-primary-soft);
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 7px 12px;
  border-radius: 999px;
}


/* Typography alignment for Streamlit markdown headings inside sections */
.element-container h2, .element-container h3, .element-container h4 {
  color: var(--nv-text);
  letter-spacing: -0.02em;
}
.element-container h2 {
  font-size: 22px !important;
  line-height: 1.28 !important;
  margin: 0 0 12px 0 !important;
}
.element-container h3 {
  font-size: 18px !important;
  line-height: 1.35 !important;
  margin: 0 0 10px 0 !important;
}
.element-container h4 {
  font-size: 15px !important;
  line-height: 1.4 !important;
  margin: 0 0 8px 0 !important;
}

/* =========================================
   Streamlit Native Overrides
   ========================================= */
[data-baseweb="tab-list"] {
  gap: 24px;
  padding-bottom: 0px;
  border-bottom: 1px solid var(--nv-line);
}
[data-baseweb="tab"] {
  background: transparent !important;
  border: none !important;
  font-weight: 600;
  padding: 16px 4px !important;
  margin: 0 !important;
  color: var(--nv-muted-light) !important;
  font-size: 15px;
  border-radius: 0 !important;
  transition: color 0.2s ease;
}
[data-baseweb="tab"]:hover { color: var(--nv-text) !important; }
[aria-selected="true"] {
  color: var(--nv-text) !important;
  font-weight: 800 !important;
  border-bottom: 2px solid var(--nv-text) !important;
  box-shadow: none !important;
  background: transparent !important;
}

[data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stExpander"] details,
[data-testid="stMetric"] {
  border-radius: var(--nv-radius-lg) !important;
}

[data-testid="stVerticalBlockBorderWrapper"] {
  border: 1px solid var(--nv-line) !important;
  background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,255,255,0.86)) !important;
  box-shadow: var(--nv-shadow-soft) !important;
}

[data-testid="stSegmentedControl"] {
  background: rgba(255,255,255,0.72);
  border: 1px solid var(--nv-line);
  border-radius: 14px;
  padding: 4px;
}
[data-testid="stSegmentedControl"] [role="radiogroup"] { gap: 4px; }
[data-testid="stSegmentedControl"] label {
  min-height: 36px;
  border-radius: 10px !important;
  padding: 8px 12px !important;
}
[data-testid="stSegmentedControl"] label:has(input:checked) {
  background: #FFFFFF !important;
  border: 1px solid var(--nv-line) !important;
  box-shadow: 0 4px 14px rgba(15, 23, 42, 0.06) !important;
}
[data-testid="stSegmentedControl"] label p {
  font-size: 13px !important;
  font-weight: 700 !important;
}

[data-testid="stMarkdownContainer"] .stDownloadButton button,
[data-testid="stDownloadButton"] button,
button[kind="secondary"] {
  border-radius: 12px !important;
  border: 1px solid var(--nv-line) !important;
  background: rgba(255,255,255,0.86) !important;
  color: var(--nv-text) !important;
  font-weight: 700 !important;
  box-shadow: none !important;
}
[data-testid="stMarkdownContainer"] .stDownloadButton button:hover,
[data-testid="stDownloadButton"] button:hover,
button[kind="secondary"]:hover {
  border-color: var(--nv-line-strong) !important;
  background: #FFFFFF !important;
}

[data-testid="stDataFrame"],
[data-testid="stTable"] {
  border: 1px solid var(--nv-line) !important;
  border-radius: 18px !important;
  overflow: hidden !important;
  background: rgba(255,255,255,0.92) !important;
  box-shadow: var(--nv-shadow-soft) !important;
}
[data-testid="stDataFrame"] [role="grid"],
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
  border-radius: 18px !important;
}
[data-testid="stDataFrame"] thead tr,
[data-testid="stDataFrame"] [role="columnheader"] {
  background: #F7F9FC !important;
}
[data-testid="stDataFrame"] tbody tr:hover {
  background: rgba(37, 99, 255, 0.03) !important;
}

[data-testid="stSidebar"] .stMultiSelect,
[data-testid="stSidebar"] .stSelectbox,
[data-testid="stSidebar"] .stDateInput,
[data-testid="stSidebar"] .stTextInput,
[data-testid="stSidebar"] .stNumberInput {
  margin-bottom: 0.55rem !important;
}

[data-testid="stToggle"] { padding-top: 2px; }
[data-testid="stToggle"] label { font-weight: 600 !important; }

hr, [data-testid="stDivider"] { border-color: var(--nv-line) !important; }

/* =========================================
   Sidebar & Navigation (Minimalist Radio)
   ========================================= */
[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(250,251,255,0.98), rgba(245,247,251,0.95)) !important;
  border-right: 1px solid var(--nv-line) !important;
  backdrop-filter: blur(16px) !important;
}
[data-testid="stSidebar"] .block-container {
  padding-top: 2rem !important;
  padding-left: 1.5rem !important;
  padding-right: 1.5rem !important;
}
.nav-sidebar-title {
  font-size: 12px;
  font-weight: 700;
  color: var(--nv-muted-light);
  margin-bottom: 16px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  padding-left: 8px;
}

[data-testid="stSidebar"] [role="radiogroup"] {
  background: transparent;
  padding: 0;
  gap: 6px;
  display: flex;
  flex-direction: column;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 13px 16px !important;
  margin-bottom: 3px !important;
  border-radius: 14px !important;
  background: transparent !important;
  border: 1px solid transparent !important;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover { 
  background: var(--nv-surface-2) !important; 
}
[data-testid="stSidebar"] [role="radiogroup"] label p {
  color: var(--nv-muted) !important;
  font-weight: 600 !important;
  font-size: 14px !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: var(--nv-bg) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  border: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
  color: var(--nv-primary) !important;
  font-weight: 800 !important;
}

/* =========================================
   Inputs & Controls
   ========================================= */
div[data-baseweb="select"] > div,
[data-testid="stDateInput"] > div,
[data-testid="stNumberInput"] > div,
[data-testid="stTextInput"] > div,
[data-testid="stSelectbox"] > div {
  border-radius: 10px !important;
  border-color: var(--nv-line) !important;
  background: var(--nv-bg) !important;
  box-shadow: none !important;
}
div[data-baseweb="select"] > div:focus-within {
  box-shadow: 0 0 0 1px var(--nv-primary) inset !important;
  border-color: var(--nv-primary) !important;
}

[data-testid="stExpander"] {
  border: 1px solid var(--nv-line) !important;
  border-radius: var(--nv-radius-lg) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  background: var(--nv-bg) !important;
  overflow: hidden;
}
[data-testid="stExpander"] summary {
  padding: 18px 20px !important;
  background-color: transparent !important;
  border-radius: 0 !important;
}
[data-testid="stExpander"] summary p {
  font-weight: 700 !important;
  font-size: 15px !important;
  color: var(--nv-text) !important;
}

[data-testid="baseButton-primary"] {
  background-color: var(--nv-text) !important; /* 모던함을 위해 프라이머리 버튼을 다크톤으로 변경 */
  color: white !important;
  border: none !important;
  border-radius: 10px !important;
  font-weight: 700 !important;
  padding: 8px 16px !important;
  box-shadow: var(--nv-shadow-soft) !important;
}
[data-testid="baseButton-primary"]:hover { 
  background-color: #000000 !important; 
  box-shadow: var(--nv-shadow-hover) !important;
}

.sidebar-info-box {
  background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,255,255,0.8));
  border: 1px solid var(--nv-line);
  border-radius: 16px;
  padding: 16px;
  margin-bottom: 28px;
  box-shadow: var(--nv-shadow-soft);
  backdrop-filter: blur(12px);
}
.sidebar-info-label {
  font-size: 11px;
  color: var(--nv-muted-light);
  font-weight: 700;
  margin-bottom: 6px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.sidebar-info-value {
  font-size: 14px;
  font-weight: 700;
  color: var(--nv-text);
}
.sidebar-info-value span { color: var(--nv-primary); font-weight: 800; }

@media (max-width: 1100px) {
  div.block-container {
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
  }
  .nv-page-head {
    padding: 20px;
    margin-bottom: 24px;
  }
  .nv-h1 {
    font-size: 24px;
  }
  .nv-sec-title {
    font-size: 17px;
  }
}
</style>
"""

JS_CUSTOM_ENHANCEMENTS = """
<script>
(function() {
    const parentDoc = window.parent.document;
    if (parentDoc.getElementById('custom-ux-enhancements')) return;

    const marker = parentDoc.createElement('div');
    marker.id = 'custom-ux-enhancements';
    marker.style.display = 'none';
    parentDoc.body.appendChild(marker);

    parentDoc.addEventListener('click', function(e) {
        let target = e.target;
        let isOptionClicked = false;
        while (target && target !== parentDoc) {
            if (target.getAttribute && target.getAttribute('role') === 'option') { isOptionClicked = true; break; }
            target = target.parentNode;
        }
        if (isOptionClicked) {
            setTimeout(function() {
                const escEvent = new KeyboardEvent('keydown', { key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true });
                if (parentDoc.activeElement) { parentDoc.activeElement.dispatchEvent(escEvent); parentDoc.activeElement.blur(); }
                else { parentDoc.dispatchEvent(escEvent); }
                const popover = parentDoc.querySelector('[data-baseweb="popover"]');
                if(popover && popover.parentNode) parentDoc.body.click();
            }, 50);
        }
    }, true);
})();
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    enable_parent_js = str(st.session_state.get("_enable_parent_js_enhancements", "1")) not in {"0", "false", "False"}
    if not enable_parent_js:
        return
    if st.session_state.get("_parent_js_enhancements_injected"):
        return
    components.html(JS_CUSTOM_ENHANCEMENTS, height=0, width=0)
    st.session_state["_parent_js_enhancements_injected"] = True
