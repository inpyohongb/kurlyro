import os
import requests
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask
import threading
import logging
import pytz

app = Flask(__name__)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_gspread():
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    
    try:
        creds = Credentials.from_service_account_file('scan_key.json', scopes=scope)
        client = gspread.authorize(creds)
        logger.info("Successfully initialized gspread client")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize gspread: {str(e)}")
        raise

def update_spreadsheet(worksheet_name, data):
    try:
        gc = initialize_gspread()
        workbook = gc.open("newdashboard raw")
        worksheet = workbook.worksheet(worksheet_name)
        
        logger.info(f"Updating sheet {worksheet_name} with {len(data)} rows of data")
        
        if not data:
            logger.warning("No data to update")
            return
            
        # 기존 데이터 삭제
        worksheet.batch_clear(["A2:H1000"])
        
        # 새 데이터 업데이트
        worksheet.update('A2', data)
        logger.info(f"Successfully updated {len(data)} rows of data in {worksheet_name}")
            
    except Exception as e:
        logger.error(f"Error in spreadsheet update: {str(e)}")
        raise

def get_data(date):
    try:
        # 로그인, 토큰 저장
        loginurl = "https://api-lms.kurly.com/v1/admin-accounts/login"
        idpw = {
            "loginId": os.environ['KURLY_LOGIN_ID'],
            "password": os.environ['KURLY_PASSWORD']
        }
        
        login_response = requests.post(loginurl, json=idpw)
        login_response.raise_for_status()
        token = login_response.json()['data']['token']
        logger.info("Login successful")
        
        headers = {'authorization': 'Bearer ' + token}
        url = "https://api-lms.kurly.com/v1/commutes/end"

        params = {
            "page": 1,
            "size": 30,
            "cluster": "CC03",
            "center": "GPM1",
            "workPart": "IB",
            "isEndWork": False,
            "isEarlyEndWork": False,
            "isOverWork": False,
            "isEarlyStartWork": False,
            "startDate": date,
            "endDate": date
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()['data']
        
        # 총 페이지 수 확인
        total_pages = data['totalPages']
        result = data['content']
        
        # 2페이지부터 마지막 페이지까지 데이터 수집
        for page in range(2, total_pages + 1):
            time.sleep(3)  # API 호출 간 딜레이
            params['page'] = page
            page_response = requests.get(url, headers=headers, params=params)
            page_response.raise_for_status()
            result.extend(page_response.json()['data']['content'])
            
        logger.info(f"Retrieved {len(result)} records for date {date}")
        return result
        
    except Exception as e:
        logger.error(f"Error getting data: {str(e)}")
        raise

def process_data(response):
    try:
        result = []
        
        for item in response:
            # None 값을 빈 문자열로 처리하는 안전한 변환 함수
            def safe_replace(value):
                if value is None:
                    return ''
                return str(value).replace(' ', '')
            
            # 각 필드에 대해 안전하게 처리
            processed_item = [
                safe_replace(item.get('name')),
                safe_replace(item.get('teamName')),
                safe_replace(item.get('userId')),
                safe_replace(item.get('centerShiftHourType')),
                safe_replace(item.get('startWorkDateTime')),
                safe_replace(item.get('endWorkDateTime')),
                safe_replace(item.get('overWorkMinuteTime')),
                safe_replace(item.get('overWorkStartMinuteTime'))
            ]
            
            result.append(processed_item)
            logger.debug(f"Added record: {processed_item[0]}")  # name 필드 로깅
        
        logger.info(f"Processed {len(result)} records")
        return result
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def run():
    try:
        logger.info("Starting data collection and update process")

        # 한국 시간대 설정
        korean_tz = pytz.timezone('Asia/Seoul')
        
        # 한국 시간 기준으로 오늘, 어제 날짜 계산
        today_korea = datetime.now(korean_tz).date()
        yesterday_korea = today_korea - timedelta(days=1)

        # 날짜를 문자열로 변환
        today_str = today_korea.strftime("%Y-%m-%d")
        yesterday_str = yesterday_korea.strftime("%Y-%m-%d")
        
        # 오늘 날짜 데이터 처리
        today_data = process_data(get_data(today_str))
        if len(today_data) > 0:
            update_spreadsheet("today_kurlyro", today_data)
        
        # 어제 날짜 데이터 처리
        yesterday_data = process_data(get_data(yesterday_str))
        if len(yesterday_data) > 0:
            update_spreadsheet("yesterday_kurlyro", yesterday_data)
        
        logger.info("Completed data update cycle")
        
    except Exception as e:
        logger.error(f"Error in run function: {str(e)}")
        raise

@app.route('/health')
def health_check():
    return 'OK', 200
def run_flask():
    app.run(host='0.0.0.0', port=8000)

if __name__ == "__main__":
    logger.info("Starting application")

    # Flask 서버를 별도 스레드로 실행
    thread = threading.Thread(target=run_flask)
    thread.daemon = True
    thread.start()
    while True:
        try:
            run()
            logger.info("Waiting 300 seconds before next cycle")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            logger.info("Retrying in 300 seconds...")
            time.sleep(300)
