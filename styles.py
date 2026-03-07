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

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, 'Helvetica Neue', 'Segoe UI', sans-serif !important;
  color: var(--nv-text);
  background: var(--nv-bg);
}

h1, h2, h3, h4, h5, h6 {
  font-weight: 700 !important;
  letter-spacing: -0.03em !important;
  color: #111111;
}

/* =========================================
   🔘 일반 버튼 디자인
   ========================================= */
button[data-testid="baseButton-secondary"] {
    background-color: #FFFFFF !important;
    border: 1px solid var(--nv-line-strong) !important;
    color: var(--nv-text) !important;
    border-radius: var(--nv-radius) !important;
    font-weight: 600 !important;
    padding: 4px 16px !important;
}

button[data-testid="baseButton-secondary"]:hover {
    border-color: var(--nv-primary) !important;
    color: var(--nv-primary) !important;
    background-color: var(--nv-primary-soft) !important;
}

button[data-testid="baseButton-primary"] {
    background: var(--nv-primary) !important;
    border: 1px solid var(--nv-primary) !important;
    color: #FFFFFF !important;
    border-radius: var(--nv-radius) !important;
    font-weight: 700 !important;
    padding: 4px 16px !important;
}

button[data-testid="baseButton-primary"]:hover {
    background: var(--nv-primary-hover) !important;
    border-color: var(--nv-primary-hover) !important;
}

/* =========================================
   🚨 [최종판] 드롭다운 & 인풋 하단 굵은 줄 완전 파괴
   ========================================= */
/* Streamlit BaseWeb 테두리를 담당하는 모든 div 선택 */
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div {
    border-top: 1px solid var(--nv-line-strong) !important;
    border-right: 1px solid var(--nv-line-strong) !important;
    border-left: 1px solid var(--nv-line-strong) !important;
    border-bottom: 1px solid var(--nv-line-strong) !important;
    border-radius: var(--nv-radius) !important;
    box-shadow: none !important;
    background-color: #FFFFFF !important;
}

/* 포커스 및 호버 시 모든 방향에 동일한 1px 파란색 적용 */
div[data-baseweb="select"] > div:hover,
div[data-baseweb="input"] > div:hover,
div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="input"] > div:focus-within {
    border-color: var(--nv-primary) !important;
    box-shadow: 0 0 0 1px var(--nv-primary) inset !important;
}

/* 결정적 원인인 스트림릿의 가상 요소(파란 밑줄) 완전히 숨김 처리 */
div[data-baseweb="select"] > div::before,
div[data-baseweb="select"] > div::after,
div[data-baseweb="input"] > div::before,
div[data-baseweb="input"] > div::after {
    display: none !important;
    content: none !important;
    border: none !important;
    border-bottom: none !important;
    box-shadow: none !important;
    width: 0 !important;
    height: 0 !important;
}

/* =========================================
   🚨 선택된 칩(태그) 디자인
   ========================================= */
[data-baseweb="tag"] {
    background-color: var(--nv-primary) !important;
    border: none !important;
    border-radius: 4px !important;
    margin-top: 3px !important;
    margin-bottom: 3px !important;
}
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
.nv-metric-card { background: var(--nv-bg); padding: 20px; border-radius: var(--nv-radius); border: 1px solid var(--nv-line); margin-bottom: 16px; }
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 700; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 4px 10px; border-radius: 4px; }

table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; border: 1px solid var(--nv-line); border-radius: var(--nv-radius); overflow: hidden; }
table.nv-table th { position: sticky; top: 0; z-index: 2; background: #F8F9FA; padding: 13px 16px; font-weight: 700; color: #444444; border-bottom: 1px solid var(--nv-line-strong); }
table.nv-table td { padding: 13px 16px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); }
table.nv-table tr:hover td { background: var(--nv-primary-soft); color: var(--nv-primary-hover); }

.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

[data-baseweb="tab-list"] { gap: 20px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line-strong); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 600; padding: 14px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: #111111 !important; font-weight: 800 !important; border-bottom: 3px solid var(--nv-primary) !important; box-shadow: none !important; }

/* 좌측 사이드바 디자인 */
[data-testid="stSidebar"] { background: var(--nv-surface) !important; border-right: 1px solid var(--nv-line) !important; }
[data-testid="stSidebar"] .block-container { padding-top: 1rem !important; }
.nav-sidebar-title { font-size: 16px; font-weight: 800; color: #111111; letter-spacing: -0.02em; }
</style>
"""

# Streamlit 환경에서 무조건 드롭다운 창을 닫아버리는 강력한 자바스크립트
JS_AUTO_CLOSE = """
<script>
const initAutoClose = () => {
    try {
        const parentDoc = window.parent.document;
        if (!parentDoc || parentDoc.getElementById('dropdown-fix-v2')) return;
        
        const marker = parentDoc.createElement('div');
        marker.id = 'dropdown-fix-v2';
        parentDoc.body.appendChild(marker);

        parentDoc.addEventListener('click', function(e) {
            // 클릭된 요소가 드롭다운 목록의 아이템(role="option")인지 확인
            let isOption = false;
            let target = e.target;
            while(target && target !== parentDoc) {
                if (target.getAttribute && target.getAttribute('role') === 'option') {
                    isOption = true;
                    break;
                }
                target = target.parentNode;
            }
            
            if (isOption) {
                // 클릭 직후 백그라운드 클릭 이벤트를 쏴서 강제로 팝업을 접히게 함
                setTimeout(() => {
                    const appRoot = parentDoc.querySelector('.stApp');
                    if (appRoot) {
                        appRoot.click(); // 바탕화면 강제 클릭
                    }
                }, 50);
            }
        }, true);
    } catch (err) {}
};
setInterval(initAutoClose, 1000);
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    components.html(JS_AUTO_CLOSE, height=0, width=0)
