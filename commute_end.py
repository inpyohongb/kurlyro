import os
import requests
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import logging
import pytz

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
        # 로그인 정보 가져오기
        login_id = os.environ.get('KURLY_LOGIN_ID')
        password = os.environ.get('KURLY_PASSWORD')
        
        # 로그인 요청
        loginurl = "https://api-lms.kurly.com/v1/admin-accounts/login"
        login_response = requests.post(loginurl, json={
            "loginId": login_id,
            "password": password
        })
        login_response.raise_for_status()
        
        token = login_response.json()['data']['token']
        logger.info("Login successful")
        
        headers = {'authorization': f'Bearer {token}'}
        url = "https://api-lms.kurly.com/v1/commutes/end"

        # 한 페이지당 더 많은 데이터 요청 (30 -> 100)
        params = {
            "page": 1,
            "size": 100,
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
        
        total_pages = data['totalPages']
        result = data['content']
        
        # 나머지 페이지 데이터 수집
        for page in range(2, total_pages + 1):
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
            processed_item = [
                item.get('name', ''),
                item.get('teamName', ''),
                item.get('userId', ''),
                item.get('centerShiftHourType', ''),
                item.get('startWorkDateTime', ''),
                item.get('endWorkDateTime', ''),
                item.get('overWorkMinuteTime', ''),
                item.get('overWorkStartMinuteTime', '')
            ]
            
            result.append(processed_item)
        
        logger.info(f"Processed {len(result)} records")
        return result
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def main():
    try:
        logger.info("Starting data collection process")
        
        # 한국 시간대 설정
        korean_tz = pytz.timezone('Asia/Seoul')
        today_korea = datetime.now(korean_tz).date()
        yesterday_korea = today_korea - timedelta(days=1)

        # 날짜 문자열 변환
        today_str = today_korea.strftime("%Y-%m-%d")
        yesterday_str = yesterday_korea.strftime("%Y-%m-%d")
        
        # 오늘 데이터 처리
        today_data = process_data(get_data(today_str))
        if len(today_data) > 0:
            update_spreadsheet("today_kurlyro", today_data)
        
        # 어제 데이터 처리
        yesterday_data = process_data(get_data(yesterday_str))
        if len(yesterday_data) > 0:
            update_spreadsheet("yesterday_kurlyro", yesterday_data)
        
        logger.info("Completed data collection and update process")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
