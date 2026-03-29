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
  --nv-primary: #0528F2;
  --nv-primary-hover: #3355FF;
  --nv-primary-soft: #EEF2FF;

  /* Modern Minimalist Grays */
  --nv-bg: #FFFFFF;
  --nv-surface: #F8FAFC; 
  --nv-surface-2: #F1F5F9;
  --nv-line: #E2E8F0;
  --nv-line-strong: #CBD5E1;
  --nv-muted-light: #94A3B8;
  --nv-muted: #64748B;
  --nv-text: #0F172A;

  /* Status Colors */
  --nv-success: #0528F2;
  --nv-warning: #F59E0B;
  --nv-danger: #EF4444;

  /* Geometry */
  --nv-radius: 12px;
  --nv-radius-lg: 16px;
  --nv-shadow-soft: 0 4px 20px rgba(15, 23, 42, 0.03);
  --nv-shadow-hover: 0 10px 30px rgba(15, 23, 42, 0.06);
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
  max-width: 1520px;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
}

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
  color: var(--nv-text);
  background: var(--nv-bg);
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
  margin-bottom: 32px;
  padding: 24px 32px;
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  box-shadow: var(--nv-shadow-soft);
}
.nv-page-head-left { min-width: 0; }
.nv-h1 {
  font-size: 28px;
  line-height: 1.2;
  font-weight: 800;
  letter-spacing: -0.03em;
  color: var(--nv-text);
  margin: 0 0 4px 0;
}
.nv-page-sub {
  color: var(--nv-muted);
  font-size: 14px;
  line-height: 1.5;
  margin: 0;
}

.nv-section {
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 24px 28px;
  margin-top: 24px;
  box-shadow: var(--nv-shadow-soft);
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
  margin-bottom:20px;
}
.nv-sec-title {
  font-size: 17px;
  font-weight: 700;
  margin: 0;
  color: var(--nv-text);
  display: flex;
  align-items: center;
  gap: 8px;
  letter-spacing: -0.01em;
}

/* =========================================
   Metric Cards
   ========================================= */
.nv-metric-card {
  background: var(--nv-bg);
  padding: 24px;
  border-radius: var(--nv-radius-lg);
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
  transition: all 0.2s ease;
  box-shadow: var(--nv-shadow-soft);
}
.nv-metric-card:hover {
  border-color: var(--nv-primary);
  box-shadow: var(--nv-shadow-hover);
  transform: translateY(-2px);
}
.nv-metric-card-title {
  color: var(--nv-muted);
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
}
.nv-metric-card-value {
  color: var(--nv-text);
  font-size: 32px;
  font-weight: 800;
  letter-spacing: -0.02em;
}
.nv-metric-card-desc {
  color: var(--nv-primary);
  font-size: 13px;
  font-weight: 600;
  margin-top: 12px;
  background: var(--nv-primary-soft);
  display: inline-block;
  padding: 6px 12px;
  border-radius: 8px;
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

/* =========================================
   Sidebar & Navigation (Minimalist Radio)
   ========================================= */
[data-testid="stSidebar"] {
  background: var(--nv-surface) !important;
  border-right: 1px solid var(--nv-line) !important;
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
  padding: 12px 16px !important;
  margin-bottom: 2px !important;
  border-radius: 10px !important;
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
   Sidebar Filter Rhythm & Spacing
   ========================================= */
.nv-filter-title {
  font-size: 12px;
  font-weight: 700;
  color: var(--nv-muted);
  margin: 0 0 8px 0;
  line-height: 1.4;
  letter-spacing: -0.01em;
}
.nv-filter-help {
  font-size: 11px;
  color: var(--nv-muted-light);
  margin: 2px 0 0 0;
  line-height: 1.45;
}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] {
  margin-bottom: 6px;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
  margin-bottom: 0;
}
[data-testid="stSidebar"] [data-testid="stSelectbox"],
[data-testid="stSidebar"] [data-testid="stMultiSelect"],
[data-testid="stSidebar"] [data-testid="stDateInput"],
[data-testid="stSidebar"] [data-testid="stTextInput"],
[data-testid="stSidebar"] [data-testid="stNumberInput"] {
  margin-bottom: 8px;
}
[data-testid="stSidebar"] [data-testid="stSelectbox"] label,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] label,
[data-testid="stSidebar"] [data-testid="stDateInput"] label,
[data-testid="stSidebar"] [data-testid="stTextInput"] label,
[data-testid="stSidebar"] [data-testid="stNumberInput"] label {
  font-size: 12px !important;
  font-weight: 600 !important;
  color: var(--nv-muted) !important;
  margin-bottom: 6px !important;
}
[data-testid="stSidebar"] div[data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-testid="stDateInput"] > div,
[data-testid="stSidebar"] [data-testid="stNumberInput"] > div,
[data-testid="stSidebar"] [data-testid="stTextInput"] > div,
[data-testid="stSidebar"] [data-testid="stSelectbox"] > div {
  min-height: 42px !important;
}
[data-testid="stSidebar"] div[data-baseweb="tag"] {
  border-radius: 999px !important;
  min-height: 26px !important;
  padding-inline: 8px !important;
  background: var(--nv-surface-2) !important;
  border: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] div[data-baseweb="tag"] span {
  font-size: 12px !important;
  color: var(--nv-text) !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] {
  margin-top: 2px;
}
[data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
  padding-top: 2px !important;
}
[data-testid="stSidebar"] [data-testid="stExpanderDetails"] > div {
  padding-top: 6px !important;
}
[data-testid="stSidebar"] hr {
  margin: 18px 0 !important;
  border-color: var(--nv-line) !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-primary"],
[data-testid="stSidebar"] [data-testid="baseButton-secondary"] {
  min-height: 42px !important;
}
[data-testid="stSidebar"] [data-testid="stButton"] {
  margin-top: 4px;
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
  background: transparent;
  border: 1px solid var(--nv-line);
  border-radius: 12px;
  padding: 16px;
  margin-bottom: 32px;
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
    components.html(JS_CUSTOM_ENHANCEMENTS, height=0, width=0)
