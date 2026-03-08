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
  --nv-surface: #F8F9FA;
  --nv-panel: #F4F6FA;
  --nv-line: #E5E6E9;
  --nv-line-strong: #D7DCE5;
  --nv-text: #222222;
  --nv-muted: #666666;
  --nv-muted-light: #999999;
  
  --nv-primary: #4876EF;
  --nv-primary-hover: #3A5EBF;
  --nv-primary-soft: #F0F4FF;
  
  --nv-success: #58B04B; 
  --nv-warning: #FF9839;
  --nv-danger: #FF025D; 
  --nv-radius: 6px;
}

html, body, .stApp {
  font-family: 'Pretendard', -apple-system, sans-serif !important;
  color: var(--nv-text);
}

h1, h2, h3, h4, h5, h6 { font-weight: 700 !important; color: #111111; }

/* =========================================
   🚨 [최종 해결] 드롭다운 & 인풋창 굵은 파란줄 완벽 차단
   ========================================= */
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div {
    border-bottom: 1px solid var(--nv-line-strong) !important;
    border-radius: var(--nv-radius) !important;
}

div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="input"] > div:focus-within {
    border-color: var(--nv-primary) !important;
    border-bottom: 1px solid var(--nv-primary) !important;
    box-shadow: 0 0 0 1px var(--nv-primary) inset !important;
}

div[data-baseweb="select"] > div::before,
div[data-baseweb="select"] > div::after,
div[data-baseweb="input"] > div::before,
div[data-baseweb="input"] > div::after {
    display: none !important;
    content: none !important;
    height: 0 !important;
}

/* =========================================
   🚨 선택된 항목(멀티셀렉트 칩/태그) 선명하게
   ========================================= */
[data-baseweb="tag"] { background-color: var(--nv-primary) !important; border: none !important; border-radius: 4px !important; margin: 2px !important; }
[data-baseweb="tag"] * { color: #FFFFFF !important; font-weight: 600 !important; }
[data-baseweb="tag"] svg { fill: #FFFFFF !important; }

/* =========================================
   📈 요약(Overview) KPI 카드
   ========================================= */
.kpi-group-container { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.kpi-group { flex: 1; min-width: 250px; background: #FFFFFF; border: 1px solid var(--nv-line-strong); border-radius: var(--nv-radius); padding: 16px; box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02); }
.kpi-group-title { font-size: 14px; font-weight: 700; color: #444444; margin-bottom: 12px; display: flex; align-items: center; gap: 6px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px; }

.kpi { background: var(--nv-surface); border: 1px solid var(--nv-line); padding: 14px; border-radius: 4px; transition: border 0.1s ease; }
.kpi:hover { border-color: var(--nv-primary); background: #FFFFFF; }
.kpi .k { font-size: 12px; color: var(--nv-muted); font-weight: 600; margin-bottom: 6px; }
.kpi .v { font-size: 18px; font-weight: 800; color: var(--nv-text); letter-spacing: -0.02em; }
.kpi .d { font-size: 11px; font-weight: 700; margin-top: 6px; display: inline-block; padding: 2px 6px; border-radius: 2px; }
.kpi.highlight { border-color: var(--nv-primary); background: var(--nv-primary-soft); }
.kpi.highlight .v { color: var(--nv-primary); font-size: 20px; }

.kpi .d.pos { background: #EAF7E9; color: var(--nv-success); } 
.kpi .d.neg { background: #FFE6EE; color: var(--nv-danger); } 
.kpi .d.neu { background: #E5E6E9; color: var(--nv-muted); }

/* =========================================
   📊 공통 메트릭 & 테이블 UI
   ========================================= */
.nv-sec-title { font-size: 18px; font-weight: 700; margin-top: 24px; margin-bottom: 8px; color: #111111; display: flex; align-items: center; gap: 8px; }
.nv-metric-card { background: var(--nv-bg); padding: 20px; border-radius: var(--nv-radius); border: 1px solid var(--nv-line); margin-bottom: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; letter-spacing: -0.02em; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }

button[data-testid="baseButton-secondary"] { background-color: #FFFFFF !important; border: 1px solid var(--nv-line-strong) !important; border-radius: var(--nv-radius) !important; font-weight: 600 !important; }
button[data-testid="baseButton-secondary"]:hover { border-color: var(--nv-primary) !important; color: var(--nv-primary) !important; background-color: var(--nv-primary-soft) !important; }
button[data-testid="baseButton-primary"] { background: var(--nv-primary) !important; color: #FFFFFF !important; border-radius: var(--nv-radius) !important; font-weight: 700 !important; border: none !important; }

/* 좌측 사이드바 디자인 */
[data-testid="stSidebar"] { background: var(--nv-surface) !important; border-right: 1px solid var(--nv-line) !important; }
</style>
"""

# 🔥 드롭다운 자동 닫힘 + 영어 텍스트 자동 한글화 통합 스크립트
JS_AUTO_CLOSE_AND_TRANSLATE = """
<script>
(function() {
    const parentDoc = window.parent.document;
    if (parentDoc.getElementById('custom-ux-enhancements-final')) return;
    
    const marker = parentDoc.createElement('div');
    marker.id = 'custom-ux-enhancements-final';
    marker.style.display = 'none';
    parentDoc.body.appendChild(marker);

    // 1. 클릭 시 드롭다운 닫기 기능
    parentDoc.addEventListener('click', function(e) {
        let target = e.target;
        let isOptionClicked = false;
        
        while (target && target !== parentDoc.body) {
            if (target.getAttribute && target.getAttribute('role') === 'option') {
                isOptionClicked = true;
                break;
            }
            target = target.parentNode;
        }
        
        if (isOptionClicked) {
            setTimeout(function() {
                // 앱 배경 강제 클릭 이벤트 발생
                parentDoc.body.click(); 
                if (parentDoc.activeElement) parentDoc.activeElement.blur();
            }, 50);
        }
    }, true);

    // 2. Select all & 달력 영문 한글화 변환 기능
    const monthMap = {
        "January": "1월", "February": "2월", "March": "3월", "April": "4월",
        "May": "5월", "June": "6월", "July": "7월", "August": "8월",
        "September": "9월", "October": "10월", "November": "11월", "December": "12월"
    };

    const translateUI = () => {
        // Select all -> 전체 선택 번역
        const options = parentDoc.querySelectorAll('[role="option"]');
        options.forEach(opt => {
            if (opt.textContent && opt.textContent.includes('Select all')) {
                const walker = parentDoc.createTreeWalker(opt, NodeFilter.SHOW_TEXT, null, false);
                let node;
                while ((node = walker.nextNode())) {
                    if (node.nodeValue.includes('Select all')) {
                        node.nodeValue = node.nodeValue.replace('Select all', '전체 선택');
                    }
                }
            }
        });

        // 달력 월 이름 번역
        const popovers = parentDoc.querySelectorAll('[data-baseweb="calendar"], [data-baseweb="popover"]');
        popovers.forEach(pop => {
            const walker = parentDoc.createTreeWalker(pop, NodeFilter.SHOW_TEXT, null, false);
            let node;
            while ((node = walker.nextNode())) {
                let text = node.nodeValue;
                if (text) {
                    let changed = false;
                    for (const [eng, kor] of Object.entries(monthMap)) {
                        const regex = new RegExp(`\\\\b${eng}\\\\s+(\\\\d{4})\\\\b`);
                        if (regex.test(text)) {
                            text = text.replace(regex, `$1년 ${kor}`);
                            changed = true;
                        } 
                        else if (text.includes(eng)) {
                            text = text.replace(eng, kor);
                            changed = true;
                        }
                    }
                    if (changed) {
                        node.nodeValue = text;
                    }
                }
            }
        });
    };

    // DOM 변화 감지하여 동적으로 생성되는 팝업 번역
    const observer = new MutationObserver((mutations) => {
        let shouldTranslate = false;
        for (let m of mutations) {
            if (m.addedNodes.length > 0) {
                shouldTranslate = true;
                break;
            }
        }
        if (shouldTranslate) translateUI();
    });
    
    observer.observe(parentDoc.body, { childList: true, subtree: true });

})();
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    components.html(JS_AUTO_CLOSE_AND_TRANSLATE, height=0, width=0)

