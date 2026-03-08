# ui.py 의 render_hero 함수 내 사이드바 마크다운 부분 수정

def render_hero(latest_dates: dict | None, build_tag: str, dashboard_title: str = "마케팅 통합 대시보드") -> None:
    # ... (생략) ...
    
    # 사이드바 상단 정보창 (margin-bottom을 24px로 변경)
    st.sidebar.markdown(f"""
    <div style='padding: 14px 16px; border-radius: 6px; background: #FFFFFF; border: 1px solid var(--nv-line-strong); margin-bottom: 24px;'>
        <div style='font-size: 12px; color: var(--nv-muted); font-weight: 600; margin-bottom: 6px; display: flex; align-items: center; gap: 4px;'>
            <span style='color:var(--nv-primary)'>■</span> {dashboard_title}
        </div>
        <div style='font-size: 15px; font-weight: 700; color: #111111; letter-spacing: -0.02em;'>
            최근 수집: <span style='color: var(--nv-primary); font-weight: 800;'>{dt_str}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
