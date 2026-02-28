name: Test Shopping Extension Ads Collector
on:
  workflow_dispatch: 
    inputs:
      start_date:
        description: '시작 날짜 (YYYY-MM-DD 포맷), 미입력시 당해년도 1월 1일'
        required: false
        default: ''
      end_date:
        description: '종료 날짜 (YYYY-MM-DD 포맷), 미입력시 자동으로 어제 날짜'
        required: false
        default: ''
  schedule:
    - cron: '0 1 * * *'  # 매일 오전 10시 (KST) = UTC 01:00

jobs:
  run-collector:
    runs-on: ubuntu-latest
    env:
      TZ: Asia/Seoul
      
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Requirements
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Set Date Range
        id: dates
        run: |
          YEAR=$(TZ=Asia/Seoul date +%Y)
          YESTERDAY=$(TZ=Asia/Seoul date -d 'yesterday' +%Y-%m-%d)

          # 자동 스케줄 실행이면 어제 하루치만
          if [ "${{ github.event_name }}" == "schedule" ]; then
            START_DATE="${YESTERDAY}"
            END_DATE="${YESTERDAY}"
          else
            # 수동 실행 - 입력값 없으면 1월1일 ~ 어제
            if [ -z "${{ github.event.inputs.start_date }}" ]; then
              START_DATE="${YEAR}-01-01"
            else
              START_DATE="${{ github.event.inputs.start_date }}"
            fi

            if [ -z "${{ github.event.inputs.end_date }}" ]; then
              END_DATE="${YESTERDAY}"
            else
              END_DATE="${{ github.event.inputs.end_date }}"
            fi
          fi

          echo "start_date=${START_DATE}" >> $GITHUB_OUTPUT
          echo "end_date=${END_DATE}" >> $GITHUB_OUTPUT
          echo "▶ 수집 기간: ${START_DATE} ~ ${END_DATE}"

      - name: Run Shopping Extension Collector
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          NAVER_ADS_API_KEY: ${{ secrets.NAVER_ADS_API_KEY }}
          NAVER_ADS_SECRET: ${{ secrets.NAVER_ADS_SECRET }}
          CUSTOMER_ID: ${{ secrets.CUSTOMER_ID }}
        run: |
          START="${{ steps.dates.outputs.start_date }}"
          END="${{ steps.dates.outputs.end_date }}"

          CURRENT=$START
          while [[ "$CURRENT" < "$END" || "$CURRENT" == "$END" ]]; do
            echo "▶ 수집 중: $CURRENT"
            python collector_shop_ext.py --date "$CURRENT"
            CURRENT=$(date -d "$CURRENT + 1 day" +%Y-%m-%d)
          done
