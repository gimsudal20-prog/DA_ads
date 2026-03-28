# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard (Enterprise Bento UI Applied)."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* STITCH Brand Colors */
  --nv-primary: #4f46e5;       /* 세련된 인디고 블루 */
  --nv-primary-hover: #3525cd;
  --nv-primary-soft: #e5eeff;  /* 아주 연한 블루 (배경용) */

  /* Modern Minimalist Grays & Blues */
  --nv-bg: #f8f9ff;            /* 대시보드 전체 배경 (푸른빛이 도는 밝은 회색) */
  --nv-surface: #ffffff;       /* 카드 컨테이너 배경 (순백색) */
  --nv-surface-2: #f1f5f9;
  --nv-line: #e2e8f0;
  --nv-line-strong: #cbd5e1;
  --nv-muted-light: #94a3b8;
  --nv-muted: #565e74;         /* 서브 텍스트 (고급스러운 슬레이트 그레이) */
  --nv-text: #0b1c30;          /* 메인 텍스트 (완전 검은색이 아닌 딥 네이비) */

  /* Status Colors */
  --nv-success: #10b981;
  --nv-warning: #f59e0b;
  --nv-danger: #ba1a1a;

  /* Geometry (Bento UI) */
  --nv-radius: 16px;
  --nv-radius-lg: 24px;        /* 벤토 박스 스타일의 둥근 모서리 */
  --nv-radius-xl: 32px;
  
  /* Cloud Shadows (소프트 섀도우) */
  --nv-shadow-soft: 0px 20px 40px rgba(11, 28, 48, 0.06);
  --nv-shadow-hover: 0px 30px 60px rgba(11, 28, 48, 0.1);
}

/* =========================================
   Base Layout & Typography
   ========================================= */
footer { display: none !important; }
header[data-testid="stHeader"] { background-color: transparent !important; }
[data-testid="stHeaderActionElements"] { display: none !important; }
[data-testid="stSidebarNav"] { display: none !important; }

/* 앱 전체 배경색 적용 */
.stApp {
    background-color: var(--nv-bg);
}

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
}

h1, h2, h3, h4, h5, h6 {
  font-weight: 800 !important;
  letter-spacing: -0.03em !important;
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
  padding: 32px;
  background: var(--nv-surface);
  border: 1px solid rgba(255, 255, 255, 0.5); /* 유리 질감 느낌의 테두리 */
  border-radius: var(--nv-radius-lg);
  box-shadow: var(--nv-shadow-soft);
}
.nv-page-head-left { min-width: 0; }
.nv-h1 {
  font-size: 32px;
  line-height: 1.2;
  font-weight: 900;
  letter-spacing: -0.04em;
  color: var(--nv-text);
  margin: 0 0 8px 0;
}
.nv-page-sub {
  color: var(--nv-muted);
  font-size: 15px;
  line-height: 1.5;
  margin: 0;
}

.nv-section {
  background: var(--nv-surface);
  border: 1px solid rgba(255, 255, 255, 0.5);
  border-radius: var(--nv-radius-lg);
  padding: 32px;
  margin-top: 32px;
  box-shadow: var(--nv-shadow-soft);
}
.nv-section-muted {
  background: transparent;
  border: none;
  box-shadow: none;
}
.nv-section-head {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:16px;
  margin-bottom:24px;
}
.nv-sec-title {
  font-size: 20px;
  font-weight: 800;
  margin: 0;
  color: var(--nv-text);
  display: flex;
  align-items: center;
  gap: 10px;
  letter-spacing: -0.02em;
}

/* =========================================
   Metric Cards (STITCH Bento Box 스타일)
   ========================================= */
.nv-metric-card {
  background: var(--nv-surface);
  padding: 28px;
  border-radius: var(--nv-radius-lg);
  border: 1px solid rgba(255, 255, 255, 0.6);
  margin-bottom: 16px;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--nv-shadow-soft);
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  position: relative;
  overflow: hidden;
}
.nv-metric-card:hover {
  border-color: var(--nv-primary-soft);
  box-shadow: var(--nv-shadow-hover);
  transform: translateY(-4px);
}
.nv-metric-card-title {
  color: var(--nv-muted);
  font-size: 12px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  margin-bottom: 12px;
  position: relative;
  z-index: 2;
}
.nv-metric-card-value {
  color: var(--nv-text);
  font-size: 40px; /* 크고 강렬하게 */
  font-weight: 900;
  letter-spacing: -0.05em;
  position: relative;
  z-index: 2;
}
.nv-metric-card-desc {
  color: var(--nv-primary);
  font-size: 12px;
  font-weight: 800;
  margin-top: 16px;
  background: var(--nv-primary-soft);
  display: inline-block;
  padding: 6px 14px;
  border-radius: 9999px; /* 알약(Pill) 모양 배지 */
  position: relative;
  z-index: 2;
  width: fit-content;
}

/* =========================================
   Streamlit Native Overrides
   ========================================= */
[data-baseweb="tab-list"] {
  gap: 32px;
  padding-bottom: 0px;
  border-bottom: 2px solid var(--nv-line);
}
[data-baseweb="tab"] {
  background: transparent !important;
  border: none !important;
  font-weight: 700;
  padding: 16px 4px !important;
  margin: 0 !important;
  color: var(--nv-muted-light) !important;
  font-size: 16px;
  border-radius: 0 !important;
  transition: color 0.2s ease;
}
[data-baseweb="tab"]:hover { color: var(--nv-text) !important; }
[aria-selected="true"] {
  color: var(--nv-primary) !important; /* 탭 활성화 시 브랜드 컬러 */
  font-weight: 800 !important;
  border-bottom: 3px solid var(--nv-primary) !important;
  box-shadow: none !important;
  background: transparent !important;
}

/* =========================================
   Sidebar & Navigation (Glassmorphism)
   ========================================= */
[data-testid="stSidebar"] {
  background-color: rgba(250, 251, 255, 0.75) !important;
  backdrop-filter: blur(24px) !important;
  -webkit-backdrop-filter: blur(24px) !important;
  border-right: none !important;
  box-shadow: 4px 0 24px rgba(11, 28, 48, 0.04) !important;
}
[data-testid="stSidebar"] .block-container {
  padding-top: 2.5rem !important;
  padding-left: 1.5rem !important;
  padding-right: 1.5rem !important;
}
.nav-sidebar-title {
  font-size: 11px;
  font-weight: 800;
  color: var(--nv-muted-light);
  margin-bottom: 16px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  padding-left: 12px;
}

[data-testid="stSidebar"] [role="radiogroup"] {
  background: transparent;
  padding: 0;
  gap: 8px;
  display: flex;
  flex-direction: column;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 14px 16px !important;
  margin-bottom: 2px !important;
  border-radius: 12px !important;
  background: transparent !important;
  border: 1px solid transparent !important;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover { 
  background: rgba(255, 255, 255, 0.6) !important; 
  transform: translateX(4px);
}
[data-testid="stSidebar"] [role="radiogroup"] label p {
  color: var(--nv-muted) !important;
  font-weight: 600 !important;
  font-size: 14px !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: var(--nv-surface) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  border: 1px solid rgba(255, 255, 255, 0.8) !important;
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
  border-radius: 12px !important;
  border-color: var(--nv-line) !important;
  background: var(--nv-surface) !important;
  box-shadow: var(--nv-shadow-soft) !important; /* 인풋에도 약한 그림자 */
}
div[data-baseweb="select"] > div:focus-within {
  box-shadow: 0 0 0 2px var(--nv-primary-soft) !important;
  border-color: var(--nv-primary) !important;
}

[data-testid="stExpander"] {
  border: 1px solid rgba(255, 255, 255, 0.6) !important;
  border-radius: var(--nv-radius-lg) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  background: var(--nv-surface) !important;
  overflow: hidden;
}
[data-testid="stExpander"] summary {
  padding: 20px 24px !important;
  background-color: transparent !important;
  border-radius: 0 !important;
}
[data-testid="stExpander"] summary p {
  font-weight: 800 !important;
  font-size: 16px !important;
  color: var(--nv-text) !important;
}

/* 그라데이션 버튼 (엔터프라이즈 느낌) */
[data-testid="baseButton-primary"] {
  background: linear-gradient(135deg, var(--nv-primary) 0%, var(--nv-primary-hover) 100%) !important;
  color: white !important;
  border: none !important;
  border-radius: 12px !important;
  font-weight: 800 !important;
  padding: 10px 24px !important;
  box-shadow: 0 10px 20px rgba(79, 70, 229, 0.2) !important;
  transition: all 0.2s ease !important;
}
[data-testid="baseButton-primary"]:hover { 
  transform: translateY(-2px) !important;
  box-shadow: 0 15px 25px rgba(79, 70, 229, 0.3) !important;
}

/* Secondary Button */
[data-testid="baseButton-secondary"] {
  background: var(--nv-surface) !important;
  color: var(--nv-text) !important;
  border: 1px solid var(--nv-line) !important;
  border-radius: 12px !important;
  font-weight: 700 !important;
  padding: 10px 24px !important;
  box-shadow: var(--nv-shadow-soft) !important;
}
[data-testid="baseButton-secondary"]:hover {
  background: var(--nv-surface-2) !important;
  border-color: var(--nv-line-strong) !important;
}

.sidebar-info-box {
  background: var(--nv-surface);
  border: 1px solid rgba(255,255,255,0.5);
  border-radius: 16px;
  padding: 20px;
  margin-bottom: 32px;
  box-shadow: var(--nv-shadow-soft);
}
.sidebar-info-label {
  font-size: 10px;
  color: var(--nv-muted-light);
  font-weight: 800;
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}
.sidebar-info-value {
  font-size: 15px;
  font-weight: 800;
  color: var(--nv-text);
  line-height: 1.4;
}
.sidebar-info-value span { color: var(--nv-primary); font-weight: 900; }

@media (max-width: 1100px) {
  div.block-container {
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
  }
  .nv-page-head {
    padding: 24px;
    margin-bottom: 24px;
  }
  .nv-h1 {
    font-size: 26px;
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
