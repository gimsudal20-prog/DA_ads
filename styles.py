# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* ✨ Main Theme Colors (Blue) */
  --nv-primary: #0528F2;        /* Main Color (Blue) */
  --nv-primary-hover: #3355FF;  /* Hover Color */
  --nv-primary-soft: #E6E9FF;   /* Soft Background Color */
  
  /* Grayscale */
  --nv-bg: #FFFFFF;
  --nv-surface: #F8F9FB;        /* Gray 100 */
  --nv-line: #DEE2E5;           /* Gray 300 */
  --nv-line-strong: #A8AFB7;    /* Gray 500 */
  --nv-muted-light: #A8AFB7;    /* Gray 500 */
  --nv-muted: #62686F;          /* Gray 700 */
  --nv-text: #19191A;           /* Gray 900 */
  
  /* Status (Positive=Blue, Negative=Red) */
  --nv-success: #0528F2;        /* Positive is now Blue */
  --nv-warning: #F79009;
  --nv-danger: #F04438;         /* Red for Negative/Down */
  
  --nv-radius: 12px;
}

/* ==============================================================
   1. 기본 레이아웃 (✨ 사이드바 절대 보존, 우측 메뉴만 핀셋 제거)
   ============================================================== */
footer { display: none !important; }

/* 헤더 투명화 */
header[data-testid="stHeader"] { background-color: transparent !important; }

/* 🚨 우측 상단 Deploy, 햄버거 메뉴 등 '액션 요소'들만 핀셋으로 제거 (사이드바 버튼 생존) */
[data-testid="stHeaderActionElements"] { display: none !important; }

div.block-container { 
    padding-top: 2rem !important; 
    padding-bottom: 3rem !important; 
    max-width: 1400px;
    padding-left: 2rem;
    padding-right: 2rem;
}

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
  color: var(--nv-text);
  background: var(--nv-bg);
}

h1, h2, h3, h4, h5, h6 { font-weight: 700 !important; letter-spacing: -0.02em !important; color: var(--nv-text); }
.nv-sec-title { font-size: 17px; font-weight: 700; margin-top: 32px; margin-bottom: 16px; color: var(--nv-text); display: flex; align-items: center; gap: 8px; }

/* ==============================================================
   2. 요약 지면 KPI 카드
   ============================================================== */
.nv-metric-card { background: var(--nv-bg); padding: 20px 24px; border-radius: var(--nv-radius); border: 1px solid var(--nv-line); margin-bottom: 16px; transition: all 0.2s ease; box-shadow: 0 1px 3px rgba(25, 25, 26, 0.02); }
.nv-metric-card:hover { border-color: var(--nv-primary); box-shadow: 0 4px 12px rgba(5, 40, 242, 0.08); }
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; }
.nv-metric-card-value { color: var(--nv-text); font-size: 26px; font-weight: 700; letter-spacing: -0.02em; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 600; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 4px 10px; border-radius: 6px; }

.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: var(--nv-bg); border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 20px; box-shadow: 0 1px 3px rgba(25,25,26,0.02); }
.kpi-group-title { font-size: 14px; font-weight: 700; color: var(--nv-text); margin-bottom: 16px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 16px; }

.kpi { display: flex; flex-direction: column; justify-content: flex-end; }
.kpi .k { font-size: 13px; color: var(--nv-muted); font-weight: 500; margin-bottom: 6px; }
.kpi .v { font-size: 20px; font-weight: 700; color: var(--nv-text); letter-spacing: -0.02em; line-height: 1.2;}
.kpi .d { font-size: 12px; font-weight: 600; margin-top: 8px; padding: 4px 6px; border-radius: 4px; display: inline-block;}
.kpi.highlight .v { color: var(--nv-text); } 
.kpi.highlight-positive .v { color: var(--nv-primary); font-size: 22px; }

.kpi .d.pos { background: var(--nv-primary-soft); color: var(--nv-success); }
.kpi .d.neg { background: #FEE4E2; color: var(--nv-danger); }
.kpi .d.neu { background: var(--nv-surface); color: var(--nv-muted-light); }

/* ==============================================================
   3. 테이블 & 탭 (하얀색/밑줄 중심의 깔끔한 구조)
   ============================================================== */
table, div[data-testid="stDataFrame"] table { border-collapse: separate; border-spacing: 0; }
th, div[data-testid="stDataFrame"] th { 
    background-color: var(--nv-bg) !important; 
    color: var(--nv-text) !important; 
    font-weight: 700 !important; 
    font-size: 13px; 
    border-bottom: 2px solid var(--nv-line) !important; 
}
td, div[data-testid="stDataFrame"] td { font-size: 13px; color: var(--nv-text) !important; border-bottom: 1px solid var(--nv-surface) !important; }
tr:hover td, div[data-testid="stDataFrame"] tr:hover td { background-color: var(--nv-surface) !important; cursor: default; }

/* 탭 활성화 시 강조 */
[data-baseweb="tab-list"] { gap: 24px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 500; padding: 12px 4px !important; margin: 0 !important; color: var(--nv-muted) !important; font-size: 15px; border-radius: 0 !important; transition: color 0.2s ease; }
[data-baseweb="tab"]:hover { color: var(--nv-text) !important; }
[aria-selected="true"] { 
    color: var(--nv-text) !important; 
    font-weight: 700 !important; 
    border-bottom: 2px solid var(--nv-text) !important; 
    box-shadow: none !important; 
    background: transparent !important; 
}

/* ==============================================================
   4. 사이드바 (메뉴 선택 박스 흰색 처리)
   ============================================================== */
[data-testid="stSidebar"] { background: var(--nv-surface) !important; border-right: 1px solid var(--nv-line) !important; }
[data-testid="stSidebar"] .block-container { padding-top: 2rem !important; padding-left: 1.5rem !important; padding-right: 1.5rem !important; }
.nav-sidebar-title { font-size: 12px; font-weight: 600; color: var(--nv-muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em; }

[data-testid="stSidebar"] [role="radiogroup"] { background: transparent; padding: 0; gap: 4px; display: flex; flex-direction: column; }
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 10px 14px !important;
  margin-bottom: 2px !important;
  border-radius: 8px !important;
  background: transparent !important;
  border: none !important;
  transition: all 0.15s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover { background: var(--nv-line) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label p { color: var(--nv-muted) !important; font-weight: 500 !important; font-size: 14px !important; }

/* 선택된 메뉴는 하얀색 박스로 띄워서, 파란색 동그라미가 잘 보이게 함 */
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { 
    background: var(--nv-bg) !important; 
    box-shadow: 0 1px 3px rgba(25, 25, 26, 0.06) !important; 
    border: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p { color: var(--nv-text) !important; font-weight: 700 !important; }

/* ==============================================================
   5. 필터 멀티셀렉트 태그 (하얀 바탕)
   ============================================================== */
div[data-baseweb="select"] > div { border-radius: 8px !important; border-color: var(--nv-line) !important; background: var(--nv-bg) !important; }
div[data-baseweb="select"] > div:focus-within { box-shadow: 0 0 0 1px var(--nv-primary) inset !important; border-color: var(--nv-primary) !important; }

span[data-baseweb="tag"] { 
    background-color: var(--nv-bg) !important; 
    color: var(--nv-text) !important; 
    font-weight: 500 !important; 
    border: 1px solid var(--nv-line) !important; 
    border-radius: 4px !important; 
}
span[data-baseweb="tag"] > span[role="button"] { fill: var(--nv-muted-light) !important; color: var(--nv-muted-light) !important; }

/* ==============================================================
   6. 기타 (Expander, 버튼)
   ============================================================== */
[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; background: var(--nv-bg) !important; overflow: hidden; }
[data-testid="stExpander"] summary { padding: 14px 16px !important; background-color: var(--nv-surface) !important; border-radius: 0 !important; }
[data-testid="stExpander"] summary p { font-weight: 600 !important; font-size: 14px !important; color: var(--nv-text) !important; }

[data-testid="baseButton-primary"] { background-color: var(--nv-primary) !important; color: white !important; border: none !important; border-radius: 8px !important; font-weight: 600 !important;}
[data-testid="baseButton-primary"]:hover { background-color: var(--nv-primary-hover) !important; }

.sidebar-info-box { background: var(--nv-bg); border: 1px solid var(--nv-line); border-radius: 8px; padding: 16px; margin-bottom: 32px; }
.sidebar-info-label { font-size: 11px; color: var(--nv-muted); font-weight: 600; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.02em; }
.sidebar-info-value { font-size: 14px; font-weight: 600; color: var(--nv-text); }
.sidebar-info-value span { color: var(--nv-primary); font-weight: 700; }
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
