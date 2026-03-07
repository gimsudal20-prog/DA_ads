# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* ✨ 사람인(Saramin) 레퍼런스 기반 프로페셔널 비즈니스 팔레트 */
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
  
  --nv-success: #58B04B;  /* 개선/상승 (초록) */
  --nv-warning: #FF9839;
  --nv-danger: #FF025D;   /* 악화/하락 (빨강) */
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
   🔘 버튼 디자인
   ========================================= */
button[data-testid="baseButton-secondary"] {
    background-color: #FFFFFF !important;
    border: 1px solid var(--nv-line-strong) !important;
    color: var(--nv-text) !important;
    border-radius: var(--nv-radius) !important;
    font-weight: 600 !important;
    padding: 4px 16px !important;
    transition: all 0.2s ease-in-out !important;
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
    transition: all 0.2s ease-in-out !important;
    box-shadow: none !important;
}

button[data-testid="baseButton-primary"]:hover {
    background: var(--nv-primary-hover) !important;
    border-color: var(--nv-primary-hover) !important;
}

[data-testid="stCheckbox"] label span[data-baseweb="checkbox"] {
    background-color: var(--nv-primary) !important;
}


/* =========================================
   🚨 [집중 공략] 하단 2px 굵은 파란줄 '완벽' 차단 
   ========================================= */
/* 1. Baseweb 셀렉트박스 & 인풋창 테두리를 1px로 강제 고정 */
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div {
    border-width: 1px !important;
    border-bottom-width: 1px !important; /* 하단 1px 고정 */
    border-style: solid !important;
    border-color: var(--nv-line-strong) !important;
    border-radius: var(--nv-radius) !important;
    box-shadow: none !important;
}

/* 2. 포커스(클릭)되었을 때 하단만 굵어지는 현상 차단 및 전체 1px 테두리 + 은은한 섀도우 */
div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="select"] > div:focus,
div[data-baseweb="select"] > div:active,
div[data-baseweb="input"] > div:focus-within,
div[data-baseweb="input"] > div:focus,
div[data-baseweb="input"] > div:active {
    border-width: 1px !important;
    border-bottom-width: 1px !important; /* 포커스 시에도 무조건 1px 유지 */
    border-color: var(--nv-primary) !important;
    box-shadow: 0 0 0 1px var(--nv-primary) !important; 
    border-radius: var(--nv-radius) !important;
}

/* 3. 호버 시 테두리 색상 변경 */
div[data-baseweb="select"] > div:hover,
div[data-baseweb="input"] > div:hover {
    border-color: var(--nv-primary) !important;
}

/* 4. 스트림릿이 몰래 생성해서 밑줄을 긋는 ::after / ::before 가상 요소 완전히 파괴 */
div[data-baseweb="select"] > div::after,
div[data-baseweb="select"] > div::before,
div[data-baseweb="input"] > div::after,
div[data-baseweb="input"] > div::before {
    content: none !important;
    display: none !important;
    border: none !important;
    height: 0 !important;
}


/* =========================================
   🚨 선택된 항목(멀티셀렉트 칩/태그) 뚜렷하게 변경
   ========================================= */
[data-baseweb="tag"] {
    background-color: var(--nv-primary) !important;
    border: none !important;
    border-radius: 4px !important;
    margin-top: 3px !important;
    margin-bottom: 3px !important;
}

[data-baseweb="tag"] * { 
    color: #FFFFFF !important; 
    font-weight: 600 !important; 
}

[data-baseweb="tag"] svg {
    fill: #FFFFFF !important;
}


/* =========================================
   📈 요약(Overview) KPI 카드 및 기타 레이아웃
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

.nv-sec-title {
  font-size: 18px;
  font-weight: 700;
  margin-top: 24px;
  margin-bottom: 8px;
  color: #111111;
  display: flex;
  align-items: center;
  gap: 8px;
}

.nv-metric-card {
  background: var(--nv-bg);
  padding: 20px;
  border-radius: var(--nv-radius);
  border: 1px solid var(--nv-line);
  margin-bottom: 16px;
  box-shadow: 0 1px 2px rgba(0,0,0,0.03);
}
.nv-metric-card-title { color: var(--nv-muted); font-size: 13px; font-weight: 600; margin-bottom: 8px; letter-spacing: -0.02em; }
.nv-metric-card-value { color: var(--nv-text); font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }
.nv-metric-card-desc { color: var(--nv-primary); font-size: 12px; font-weight: 700; margin-top: 8px; background: var(--nv-primary-soft); display: inline-block; padding: 4px 10px; border-radius: 4px; }

table.nv-table { width: 100%; border-collapse: collapse; background: var(--nv-bg); font-size: 13px; text-align: left; border: 1px solid var(--nv-line); border-radius: var(--nv-radius); overflow: hidden; }
table.nv-table th { position: sticky; top: 0; z-index: 2; background: #F8F9FA; padding: 13px 16px; font-weight: 700; color: #444444; border-bottom: 1px solid var(--nv-line-strong); letter-spacing:-0.02em; }
table.nv-table td { padding: 13px 16px; border-bottom: 1px solid var(--nv-line); vertical-align: middle; color: var(--nv-text); transition: background 0.1s ease; }
table.nv-table tr:hover td { background: var(--nv-primary-soft); color: var(--nv-primary-hover); }

.nv-pbar { display: flex; align-items: center; gap: 10px; min-width: 160px; }
.nv-pbar-bg { position: relative; flex: 1; height: 6px; border-radius: 3px; background: var(--nv-line); overflow: hidden; }
.nv-pbar-fill { position: absolute; left: 0; top: 0; bottom: 0; transition: width 0.5s ease; border-radius: 3px; }
.nv-pbar-txt { min-width: 40px; text-align: right; font-weight: 700; color: var(--nv-text); font-size: 12px; }

[data-baseweb="tab-list"] { gap: 20px; padding-bottom: 0px; border-bottom: 1px solid var(--nv-line-strong); }
[data-baseweb="tab"] { background: transparent !important; border: none !important; font-weight: 600; padding: 14px 4px !important; margin: 0 !important; color: var(--nv-muted-light) !important; font-size: 15px; border-radius: 0 !important; }
[aria-selected="true"] { color: #111111 !important; font-weight: 800 !important; border-bottom: 3px solid var(--nv-primary) !important; box-shadow: none !important; }

[data-testid="stExpander"] { border: 1px solid var(--nv-line) !important; border-radius: var(--nv-radius) !important; box-shadow: none !important; background: var(--nv-bg) !important; }
[data-testid="stExpander"] summary { padding: 16px !important; background-color: var(--nv-surface) !important; border-radius: var(--nv-radius) !important;}
[data-testid="stExpander"] summary p { font-weight: 700 !important; font-size: 14px !important; color: var(--nv-text) !important; }

/* 좌측 사이드바 디자인 */
[data-testid="stSidebar"] { background: var(--nv-surface) !important; border-right: 1px solid var(--nv-line) !important; }
[data-testid="stSidebar"] .block-container { padding-top: 1rem !important; }
.nav-sidebar-title { font-size: 16px; font-weight: 800; color: #111111; letter-spacing: -0.02em; }
.nav-sidebar-caption { margin-top: 4px; margin-bottom: 10px; font-size: 12px; color: var(--nv-muted); font-weight: 600; }

[data-testid="stSidebar"] [role="radiogroup"] { background: #FFFFFF; border: 1px solid var(--nv-line); border-radius: var(--nv-radius); padding: 8px; }
[data-testid="stSidebar"] [role="radiogroup"] label { padding: 10px 12px !important; border-radius: 4px !important; border: 1px solid transparent; transition: background 0.1s ease; }
[data-testid="stSidebar"] [role="radiogroup"] label:hover { background: var(--nv-surface) !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background: var(--nv-primary-soft) !important; border-left: 3px solid var(--nv-primary) !important; border-radius: 0 4px 4px 0 !important; }
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p { color: var(--nv-primary) !important; font-weight: 700 !important; }

div[role="listbox"] ul li, [data-baseweb="menu"] [role="option"] { border-radius: 4px !important; }
div[role="listbox"] ul li:hover, div[role="listbox"] ul li[aria-selected="true"], [data-baseweb="menu"] [role="option"]:hover, [data-baseweb="menu"] [role="option"][aria-selected="true"] { background-color: var(--nv-primary-soft) !important; color: var(--nv-primary) !important; }
div[role="listbox"] ul li:hover *, div[role="listbox"] ul li[aria-selected="true"] *, [data-baseweb="menu"] [role="option"]:hover *, [data-baseweb="menu"] [role="option"][aria-selected="true"] * { color: var(--nv-primary) !important; font-weight: 700 !important; }
</style>
"""

JS_AUTO_CLOSE = """
<script>
// 드롭다운 옵션 클릭 시 자동으로 접히게 만드는 강력한 자바스크립트
const initAutoClose = () => {
    try {
        const parentDoc = window.parent.document;
        // 이미 스크립트가 주입되었는지 확인 (중복 실행 방지)
        if (!parentDoc || parentDoc.getElementById('dropdown-auto-close-fix')) return;
        
        const marker = parentDoc.createElement('div');
        marker.id = 'dropdown-auto-close-fix';
        marker.style.display = 'none';
        parentDoc.body.appendChild(marker);

        // 사용자가 화면을 클릭했을 때 이벤트 감지
        parentDoc.addEventListener('mouseup', function(e) {
            let target = e.target;
            // 클릭한 요소부터 상위로 올라가며 옵션(목록 항목)인지 확인
            while (target && target !== parentDoc) {
                if (target.getAttribute && target.getAttribute('role') === 'option') {
                    // 스트림릿이 클릭을 인지할 수 있도록 0.1초 대기 후 강제 닫기 실행
                    setTimeout(() => {
                        // 1. ESC 키보드 이벤트 발생시켜 드롭다운 박스 닫기
                        const escEvent = new KeyboardEvent('keydown', {
                            key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true
                        });
                        parentDoc.dispatchEvent(escEvent);
                        
                        // 2. 포커스 강제 해제 (선택 후 파란 링이 남아있는 현상 방지)
                        if (parentDoc.activeElement) {
                            parentDoc.activeElement.blur();
                        }
                    }, 100); 
                    break;
                }
                target = target.parentNode;
            }
        }, true);
    } catch (err) {}
};

// Streamlit 환경 특성상 로딩 타이밍을 맞추기 위해 1초마다 스크립트 주입 시도
setInterval(initAutoClose, 1000);
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    # Javascript를 HTML 컴포넌트를 통해 뷰에 숨겨서 렌더링
    components.html(JS_AUTO_CLOSE, height=0, width=0)
