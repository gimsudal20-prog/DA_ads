# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

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

/* 기본 Streamlit UI 여백 및 요소 숨김 (완벽한 커스텀 앱처럼 보이게) */
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}

div.block-container { 
    padding-top: 2.5rem !important; 
    padding-bottom: 3rem !important; 
    max-width: 1600px; /* 와이드 스크린 대응 */
    padding-left: 2rem;
    padding-right: 2rem;
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
  padding: 6px 12px;
  border-radius: 20px;
  font-size: 12px;
  font-weight: 700;
}

/* Metric Card 애니메이션 및 그림자 추가 (고급스러운 카드 UI) */
.nv-metric-card {
  background: var(--nv-bg);
  padding: 20px;
  border-radius: 12px;
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
  transition: all 0.2s ease-in-out;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.05);
}
.nv-metric-card:hover {
  transform: translateY(-3px);
  box-shadow: 0 6px 16px rgba(16, 24, 40, 0.08);
  border-color: var(--nv-primary-soft);
}

.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 700; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 2px 8px; border-radius: 4px; }

/* 데이터프레임 읽기 편의성을 위한 호버(Hover) 효과 */
div[data-testid="stDataFrame"] table {
    border-collapse: separate;
    border-spacing: 0;
}
div[data-testid="stDataFrame"] tr:hover td {
    background-color: #F0F4FF !important;
    cursor: default;
}

.kpi-group-container {
    display: flex; gap: 16px; margin-top: 12px; margin-bottom: 32px; flex-wrap: wrap;
}
.kpi-group {
    flex: 1; min-width: 280px; background: #FFFFFF; border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 20px;
    box-shadow: 0 1px 2px rgba(16, 24, 40, 0.05); transition: all 0.2s ease-in-out;
}
.kpi-group:hover {
    transform: translateY(-3px);
    box-shadow: 0 6px 16px rgba(16, 24, 40, 0.08);
    border-color: var(--nv-primary-soft);
}
.kpi-group-title {
    font-size: 13px; font-weight: 700; color: var(--nv-muted); margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid var(--nv-line);
}
.kpi-row {
    display: flex; gap: 16px;
}
.kpi {
    flex: 1;
}
.kpi .k { font-size: 12px; color: var(--nv-muted-light); margin-bottom: 4px; font-weight: 600; }
.kpi .v { font-size: 20px; font-weight: 800; color: var(--nv-text); margin-bottom: 4px; letter-spacing: -0.5px;}
.kpi .d { font-size: 12px; font-weight: 700; padding: 2px 6px; border-radius: 4px; display: inline-block; }
.kpi .d.pos { background: #EAF7E9; color: var(--nv-success); }
.kpi .d.neg { background: #FFE6EE; color: var(--nv-danger); }
.kpi .d.neu { background: var(--nv-panel); color: var(--nv-muted); }

.kpi.highlight .v { color: var(--nv-primary); font-size: 22px; }

[data-testid="stSidebar"] {
    background-color: #FAFAFA;
    border-right: 1px solid var(--nv-line);
}
.nav-sidebar-title {
    font-size: 15px; font-weight: 800; padding-bottom: 12px; margin-bottom: 12px;
    border-bottom: 1px solid var(--nv-line-strong); color: var(--nv-text);
}
</style>
"""

# Streamlit Selectbox/Multiselect가 팝업 후 클릭 시 다른 영역을 클릭하지 않아도 자동으로 닫히게 하는 JS 주입
AUTO_CLOSE_JS = """
<script>
const doc = window.parent.document;
const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
            if (node.nodeType === 1 && node.getAttribute('data-baseweb') === 'popover') {
                const listbox = node.querySelector('ul[role="listbox"]');
                if (listbox) {
                    listbox.addEventListener('click', (e) => {
                        const li = e.target.closest('li[role="option"]');
                        if (li) { setTimeout(() => { doc.body.click(); }, 50); }
                    });
                }
            }
        });
    });
});
observer.observe(doc.body, { childList: true, subtree: true });
</script>
"""

# Streamlit 기본 날짜 선택(달력) UI를 한글화하는 스크립트
CALENDAR_I18N_JS = """
<script>
const doc = window.parent.document;
const daysEn = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
const daysKo = ['일', '월', '화', '수', '목', '금', '토'];
const monthsEn = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const monthsKo = ['1월', '2월', '3월', '4월', '5월', '6월', '7월', '8월', '9월', '10월', '11월', '12월'];

function translateCalendar() {
    const popovers = doc.querySelectorAll('[data-baseweb="popover"]');
    popovers.forEach(popover => {
        const cal = popover.querySelector('[data-baseweb="calendar"]');
        if (cal && !cal.dataset.translated) {
            const dayEls = cal.querySelectorAll('div[aria-label^="weekday"]');
            dayEls.forEach(el => {
                const idx = daysEn.indexOf(el.textContent);
                if(idx !== -1) el.textContent = daysKo[idx];
            });

            const monthSelects = cal.querySelectorAll('div[data-baseweb="select"]');
            if (monthSelects.length >= 1) {
                const monthValueEl = monthSelects[0].querySelector('[aria-selected="true"]');
                if (monthValueEl) {
                    const txt = monthValueEl.textContent;
                    const idx = monthsEn.indexOf(txt);
                    if (idx !== -1) monthValueEl.textContent = monthsKo[idx];
                }
            }
            cal.dataset.translated = "true";
        }
    });
}
const observer = new MutationObserver((mutations) => {
    for (let m of mutations) {
        if (m.addedNodes.length > 0) { translateCalendar(); }
    }
});
observer.observe(doc.body, { childList: true, subtree: true });
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    components.html(AUTO_CLOSE_JS, height=0, width=0)
    components.html(CALENDAR_I18N_JS, height=0, width=0)
