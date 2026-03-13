# -*- coding: utf-8 -*-
"""collector_gfa.py - GFA(성과형 디스플레이 광고) 전용 데이터 수집기"""

import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

load_dotenv(override=False)

# ==========================================
# 1. DB 및 설정
# ==========================================
DB_URL = os.getenv("DATABASE_URL", "").strip()
if "sslmode=" not in DB_URL: DB_URL += "&sslmode=require" if "?" in DB_URL else "?sslmode=require"

# 타임아웃 방지용 튼튼한 커넥션
engine = create_engine(DB_URL, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=60)

GFA_API_URL = "https://api.naver.com" # GFA 기본 API 엔드포인트

def get_gfa_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

# ==========================================
# 2. 엑셀 계정 정보 로드
# ==========================================
def load_gfa_accounts():
    file_path = "accounts_gfa.xlsx"
    if not os.path.exists(file_path):
        print(f"⚠️ {file_path} 파일이 없습니다. GFA 수집을 종료합니다.")
        return []
    
    try:
        df = pd.read_excel(file_path)
        # 컬럼명 정규화
        rename_map = {}
        for c in df.columns:
            c_clean = str(c).replace(" ", "").lower()
            if c_clean in ["커스텀id", "customer_id", "id"]: rename_map[c] = "customer_id"
            elif c_clean in ["업체명", "account_name"]: rename_map[c] = "account_name"
            elif c_clean in ["gfa토큰", "gfatoken", "token"]: rename_map[c] = "gfa_token"
            elif c_clean in ["gfa계정id", "adaccountid", "계정id"]: rename_map[c] = "gfa_account_id"
        
        df = df.rename(columns=rename_map)
        
        accounts = []
        for _, row in df.iterrows():
            if pd.notna(row.get('gfa_token')) and pd.notna(row.get('gfa_account_id')):
                accounts.append({
                    "customer_id": int(row['customer_id']),
                    "account_name": str(row['account_name']),
                    "gfa_token": str(row['gfa_token']).strip(),
                    "gfa_account_id": str(row['gfa_account_id']).strip()
                })
        return accounts
    except Exception as e:
        print(f"⚠️ 엑셀 파일 로드 실패: {e}")
        return []

# ==========================================
# 3. GFA 데이터 수집 로직 (캠페인, 소재, 일별 성과)
# ==========================================
def fetch_gfa_stats(account, target_date):
    """
    GFA API를 호출하여 특정 날짜의 성과 데이터를 가져옵니다.
    (대시보드 호환성을 위해 검색광고(SA) 포맷에 맞게 맵핑합니다.)
    """
    headers = get_gfa_headers(account['gfa_token'])
    dt_str = target_date.strftime("%Y-%m-%d")
    
    # [참고] 실제 GFA API 엔드포인트와 파라미터는 네이버 GFA 공식 문서를 따릅니다.
    # 대시보드 연동을 위한 규격화된 샘플 구조입니다.
    stats_url = f"{GFA_API_URL}/glads/api/v1/stats/ad-accounts/{account['gfa_account_id']}"
    
    params = {
        "startDate": dt_str,
        "endDate": dt_str,
        "timeWindow": "DAILY",
        "breakdowns": "CAMPAIGN,AD_GROUP,AD",
        "metrics": "IMPRESSION,CLICK,COST,CONVERSION,CONVERSION_REVENUE"
    }
    
    try:
        # GFA API 호출
        response = requests.get(stats_url, headers=headers, params=params, timeout=30)
        
        # 만약 API 권한이 없거나 설정이 안 된 계정이라면 빈 리스트 반환
        if response.status_code != 200:
            print(f"   [!] {account['account_name']} API 응답 오류 (코드 {response.status_code})")
            return []
            
        data = response.json()
        raw_stats = data.get("data", [])
        
        results = []
        for item in raw_stats:
            # GFA 응답값을 대시보드 DB 규격에 맞게 변환
            results.append({
                "dt": dt_str,
                "customer_id": account['customer_id'],
                "campaign_id": str(item.get("campaignId", "0")),
                "campaign_name": str(item.get("campaignName", "알수없는_캠페인")),
                "campaign_type": "GFA", # ★ 대시보드에서 GFA로 묶어볼 수 있게 지정
                "adgroup_id": str(item.get("adGroupId", "0")),
                "adgroup_name": str(item.get("adGroupName", "알수없는_그룹")),
                "ad_id": str(item.get("adId", "0")),
                "ad_name": str(item.get("adName", "알수없는_소재")),
                "imp": int(item.get("impression", 0)),
                "clk": int(item.get("click", 0)),
                "cost": float(item.get("cost", 0.0)),
                "conv": float(item.get("conversion", 0.0)),
                "sales": float(item.get("conversionRevenue", 0.0)),
                "image_url": str(item.get("imageUrl", "")) # GFA 배너 이미지
            })
        return results
    except Exception as e:
        print(f"   [!] {account['account_name']} 성과 수집 실패: {e}")
        return []

# ==========================================
# 4. DB 저장 (Upsert)
# ==========================================
def upsert_gfa_data(engine, stats_list):
    if not stats_list:
        return
        
    df = pd.DataFrame(stats_list)
    
    # 1. 캠페인 차원 데이터(dim_campaign) 저장
    dim_camp = df[['customer_id', 'campaign_id', 'campaign_name', 'campaign_type']].drop_duplicates()
    for _, row in dim_camp.iterrows():
        sql = """
            INSERT INTO dim_campaign (customer_id, campaign_id, campaign_name, campaign_tp)
            VALUES (:cid, :camp_id, :camp_name, :camp_tp)
            ON CONFLICT (customer_id, campaign_id) 
            DO UPDATE SET campaign_name = EXCLUDED.campaign_name, campaign_tp = EXCLUDED.campaign_tp
        """
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), {"cid": row['customer_id'], "camp_id": row['campaign_id'], "camp_name": row['campaign_name'], "camp_tp": row['campaign_type']})
        except Exception: pass # 이미 존재하거나 에러시 패스

    # 2. 소재 차원 데이터(dim_ad) 저장 (이미지 URL 포함)
    dim_ad = df[['customer_id', 'adgroup_id', 'ad_id', 'ad_name', 'image_url']].drop_duplicates()
    for _, row in dim_ad.iterrows():
        sql = """
            INSERT INTO dim_ad (customer_id, adgroup_id, ad_id, ad_name, image_url)
            VALUES (:cid, :adg_id, :ad_id, :ad_name, :img_url)
            ON CONFLICT (customer_id, ad_id) 
            DO UPDATE SET ad_name = EXCLUDED.ad_name, image_url = EXCLUDED.image_url
        """
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), {"cid": row['customer_id'], "adg_id": row['adgroup_id'], "ad_id": row['ad_id'], "ad_name": row['ad_name'], "img_url": row['image_url']})
        except Exception: pass

    # 3. 일별 캠페인 성과 저장 (fact_campaign_daily)
    camp_daily = df.groupby(['dt', 'customer_id', 'campaign_id'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
    for _, row in camp_daily.iterrows():
        sql = """
            INSERT INTO fact_campaign_daily (dt, customer_id, campaign_id, imp, clk, cost, conv, sales)
            VALUES (:dt, :cid, :camp_id, :imp, :clk, :cost, :conv, :sales)
            ON CONFLICT (dt, customer_id, campaign_id) 
            DO UPDATE SET imp=EXCLUDED.imp, clk=EXCLUDED.clk, cost=EXCLUDED.cost, conv=EXCLUDED.conv, sales=EXCLUDED.sales
        """
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), {"dt": row['dt'], "cid": row['customer_id'], "camp_id": row['campaign_id'], "imp": row['imp'], "clk": row['clk'], "cost": row['cost'], "conv": row['conv'], "sales": row['sales']})
        except Exception: pass

    # 4. 일별 소재 성과 저장 (fact_ad_daily)
    ad_daily = df.groupby(['dt', 'customer_id', 'ad_id'])[['imp', 'clk', 'cost', 'conv', 'sales']].sum().reset_index()
    for _, row in ad_daily.iterrows():
        sql = """
            INSERT INTO fact_ad_daily (dt, customer_id, ad_id, imp, clk, cost, conv, sales)
            VALUES (:dt, :cid, :ad_id, :imp, :clk, :cost, :conv, :sales)
            ON CONFLICT (dt, customer_id, ad_id) 
            DO UPDATE SET imp=EXCLUDED.imp, clk=EXCLUDED.clk, cost=EXCLUDED.cost, conv=EXCLUDED.conv, sales=EXCLUDED.sales
        """
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), {"dt": row['dt'], "cid": row['customer_id'], "ad_id": row['ad_id'], "imp": row['imp'], "clk": row['clk'], "cost": row['cost'], "conv": row['conv'], "sales": row['sales']})
        except Exception: pass

# ==========================================
# 5. 메인 실행 (수집기 작동)
# ==========================================
if __name__ == "__main__":
    print("🚀 GFA 전용 수집기 시작...")
    
    accounts = load_gfa_accounts()
    if not accounts:
        print("🛑 수집할 GFA 계정이 없습니다.")
        exit(0)
        
    print(f"📋 총 {len(accounts)}개의 GFA 계정 수집 대기 중...")
    
    # 최근 3일치 수집 (데이터 누락 및 보정치 반영)
    today = date.today()
    target_dates = [today - timedelta(days=i) for i in range(1, 4)]
    
    total_stats = []
    
    for account in accounts:
        print(f"\n🏢 [{account['account_name']}] 수집 시작")
        
        for tgt_date in target_dates:
            print(f"   👉 {tgt_date.strftime('%Y-%m-%d')} 데이터 가져오는 중...")
            stats = fetch_gfa_stats(account, tgt_date)
            
            if stats:
                total_stats.extend(stats)
                print(f"      ✅ {len(stats)}건 확보")
            else:
                print("      - 데이터 없음")
            
            time.sleep(0.5) # API 호출 제한(Rate Limit) 방지
            
    print(f"\n💾 총 {len(total_stats)}건의 GFA 데이터를 DB에 저장(Upsert)합니다...")
    upsert_gfa_data(engine, total_stats)
    
    print("🎉 GFA 데이터 수집이 안전하게 완료되었습니다!")
