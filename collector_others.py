# -*- coding: utf-8 -*-
"""collector_others.py - Fetch data from Meta(Facebook/IG) and other ad platforms."""

import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

# 기존 프로젝트의 DB 연결 및 설정 함수들을 가져옵니다 (필요에 따라 data.py 경로 조정)
from data import get_engine, sql_insert_replace

# 환경변수 로드 (.env 파일에 META_ACCESS_TOKEN 이 있어야 합니다)
load_dotenv()
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")

# API 버전 설정
META_API_VERSION = "v19.0"

def _extract_action_value(actions_list: list, action_type: str) -> float:
    """Meta API의 actions 배열에서 특정 액션(구매 등)의 횟수나 금액을 안전하게 추출합니다."""
    if not isinstance(actions_list, list):
        return 0.0
    for action in actions_list:
        if action.get("action_type") == action_type:
            return float(action.get("value", 0.0))
    return 0.0

def fetch_meta_campaign_daily(act_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """특정 메타 광고 계정의 캠페인별 일별 성과를 수집합니다."""
    if not META_ACCESS_TOKEN:
        print("❌ 오류: META_ACCESS_TOKEN이 설정되지 않았습니다.")
        return pd.DataFrame()

    # act_ 가 안 붙어있으면 붙여줍니다.
    account_id = act_id if str(act_id).startswith("act_") else f"act_{act_id}"
    
    url = f"https://graph.facebook.com/{META_API_VERSION}/{account_id}/insights"
    
    # 수집할 지표들 (노출, 클릭, 지출, 성과 등)
    fields = [
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "actions",          # 전환수 (구매, 장바구니 등)
        "action_values"     # 전환매출
    ]
    
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "campaign",
        "fields": ",".join(fields),
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
        "time_increment": 1, # 1로 설정하면 일자별(daily)로 쪼개서 반환합니다.
        "limit": 1000
    }

    all_data = []
    
    try:
        while True:
            response = requests.get(url, params=params)
            res_json = response.json()
            
            if "error" in res_json:
                print(f"⚠️ API 에러 ({account_id}):", res_json["error"]["message"])
                break
                
            if "data" in res_json:
                all_data.extend(res_json["data"])
                
            # 페이징 처리 (데이터가 많을 경우 다음 페이지 호출)
            paging = res_json.get("paging", {})
            if "next" in paging:
                url = paging["next"]
                params = {} # next URL에는 이미 파라미터가 포함되어 있습니다.
            else:
                break
                
            time.sleep(0.5) # API 속도 제한(Rate Limit) 방지
            
    except Exception as e:
        print(f"⚠️ 요청 중 예외 발생 ({account_id}):", e)

    if not all_data:
        return pd.DataFrame()

    # 수집된 데이터를 기존 대시보드 스키마(fact_media_daily 등)에 맞게 가공
    df_raw = pd.DataFrame(all_data)
    
    df_parsed = pd.DataFrame()
    df_parsed["dt"] = pd.to_datetime(df_raw["date_start"]).dt.date
    # DB에는 숫자형태의 customer_id로 저장하기 위해 'act_' 제거
    df_parsed["customer_id"] = str(account_id).replace("act_", "") 
    df_parsed["campaign_id"] = df_raw["campaign_id"]
    df_parsed["campaign_name"] = df_raw["campaign_name"]
    df_parsed["campaign_type"] = "META" # 대시보드에서 매체 구분을 위해 추가
    
    df_parsed["imp"] = pd.to_numeric(df_raw["impressions"], errors="coerce").fillna(0)
    df_parsed["clk"] = pd.to_numeric(df_raw["clicks"], errors="coerce").fillna(0)
    df_parsed["cost"] = pd.to_numeric(df_raw["spend"], errors="coerce").fillna(0)

    # 전환 지표 파싱 (Meta는 리스트 안에 딕셔너리 형태로 결과를 줍니다)
    # action_type 맵핑: 'purchase'(구매), 'add_to_cart'(장바구니) 등
    df_parsed["conv"] = df_raw.get("actions", []).apply(lambda x: _extract_action_value(x, "purchase"))
    df_parsed["sales"] = df_raw.get("action_values", []).apply(lambda x: _extract_action_value(x, "purchase"))
    
    # 퍼널 데이터 (필요 시 활성화)
    df_parsed["cart_conv"] = df_raw.get("actions", []).apply(lambda x: _extract_action_value(x, "add_to_cart"))
    df_parsed["cart_sales"] = df_raw.get("action_values", []).apply(lambda x: _extract_action_value(x, "add_to_cart"))
    
    return df_parsed

def run_meta_collector(start_date: str, end_date: str):
    """설정된 메타 계정들의 데이터를 수집하고 DB에 적재합니다."""
    print(f"🚀 Meta Ads 수집 시작: {start_date} ~ {end_date}")
    
    # 메타 계정 정보 불러오기 (엑셀 또는 하드코딩)
    try:
        accounts_df = pd.read_excel("accounts_meta.xlsx")
        # customer_id 컬럼에 있는 값들을 리스트로 변환
        act_ids = accounts_df["customer_id"].dropna().astype(str).tolist()
    except FileNotFoundError:
        print("⚠️ 'accounts_meta.xlsx' 파일을 찾을 수 없습니다. 테스트용 계정으로 실행합니다.")
        # 테스트/기본 계정 ID를 입력하세요 (엑셀이 없을 경우 작동)
        act_ids = ["123456789012345"] 

    engine = get_engine() # 기존 data.py의 DB 엔진 가져오기
    total_collected = 0

    for act_id in act_ids:
        print(f"👉 수집 중: Account [{act_id}]...")
        df_meta = fetch_meta_campaign_daily(act_id, start_date, end_date)
        
        if not df_meta.empty:
            # 기존 네이버 캠페인 데이터가 들어가는 테이블(예: fact_campaign_daily)에 밀어넣습니다.
            # sql_insert_replace는 data.py에 있는 기존 적재 함수를 사용합니다.
            # primary key (dt, customer_id, campaign_id) 기준으로 중복 방지 적재
            try:
                sql_insert_replace(
                    engine=engine,
                    table_name="fact_campaign_daily", # 대시보드가 읽는 테이블 이름에 맞추세요
                    df=df_meta,
                    pk_cols=["dt", "customer_id", "campaign_id"]
                )
                print(f"   ✅ 완료: {len(df_meta)}행 적재")
                total_collected += len(df_meta)
            except Exception as e:
                print(f"   ❌ DB 적재 실패: {e}")
        else:
            print("   - 데이터 없음")
            
    print(f"🎉 Meta Ads 수집 완료! 총 {total_collected}행 적재됨.")

if __name__ == "__main__":
    # 이 스크립트를 직접 실행할 때 (어제 날짜 하루치 수집)
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 만약 과거 7일치를 소급하고 싶다면 아래처럼 변경
    # start_dt = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    # run_meta_collector(start_dt, yesterday)
    
    run_meta_collector(yesterday, yesterday)
