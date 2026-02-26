// 설치 시 알람 등록 (매일 오후 12시)
chrome.runtime.onInstalled.addListener(() => {
  setupDailyAlarm();
  
  // 모바일 UA 변조 규칙 (기존 유지)
  const RULE_ID = 1;
  const MOBILE_USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1";

  chrome.declarativeNetRequest.updateDynamicRules({
    removeRuleIds: [RULE_ID],
    addRules: [
      {
        "id": RULE_ID,
        "priority": 1,
        "action": {
          "type": "modifyHeaders",
          "requestHeaders": [
            {
              "header": "User-Agent",
              "operation": "set",
              "value": MOBILE_USER_AGENT
            }
          ]
        },
        "condition": {
          "urlFilter": "m.search.naver.com", 
          "resourceTypes": ["xmlhttprequest", "sub_frame", "main_frame"]
        }
      }
    ]
  });
});

// 알람 설정 함수
function setupDailyAlarm() {
  chrome.alarms.create("dailyCheck", {
    when: getNextNoon(),
    periodInMinutes: 1440 // 24시간마다 반복
  });
}

// 다음 오후 12시 시간 계산
function getNextNoon() {
  const now = new Date();
  const next = new Date();
  next.setHours(12, 0, 0, 0);
  if (next <= now) {
    next.setDate(next.getDate() + 1);
  }
  return next.getTime();
}

// 알람 발생 시 대시보드 열기 (자동 실행 파라미터 포함)
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === "dailyCheck") {
    chrome.tabs.create({ url: 'dashboard.html?auto=true' });
  }
});



