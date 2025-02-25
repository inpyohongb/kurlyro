import os
import requests
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import logging
import pytz
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KurlyDataCollector:
    def __init__(self):
        # 세션 설정 및 재시도 전략
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        
        # 인증 토큰
        self.token = None
        
    def login(self):
        """로그인 및 토큰 획득"""
        login_id = os.environ.get('KURLY_LOGIN_ID')
        password = os.environ.get('KURLY_PASSWORD')
        
        login_response = self.session.post(
            "https://api-lms.kurly.com/v1/admin-accounts/login",
            json={"loginId": login_id, "password": password}
        )
        login_response.raise_for_status()
        self.token = login_response.json()['data']['token']
        self.session.headers.update({'authorization': f'Bearer {self.token}'})

    def get_page_data(self, url, params):
        """단일 페이지 데이터 수집"""
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()['data']['content']
        except Exception as e:
            logger.error(f"Error fetching page {params['page']}: {str(e)}")
            return []

    def get_data(self, date):
        """날짜별 데이터 수집"""
        try:
            if not self.token:
                self.login()
            
            url = "https://api-lms.kurly.com/v1/commutes/end"
            initial_params = {
                "page": 1,
                "size": 30,  # 원래 페이지 크기로 복원
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
            
            # 첫 페이지 요청으로 총 페이지 수 확인
            first_response = self.session.get(url, params=initial_params)
            first_response.raise_for_status()
            data = first_response.json()['data']
            total_pages = data['totalPages']
            result = data['content']

            # 병렬로 나머지 페이지 데이터 수집
            if total_pages > 1:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    future_to_page = {
                        executor.submit(
                            self.get_page_data,
                            url,
                            {**initial_params, "page": page}
                        ): page
                        for page in range(2, total_pages + 1)
                    }
                    
                    for future in as_completed(future_to_page):
                        result.extend(future.result())

            return result

        except Exception as e:
            logger.error(f"Error in get_data: {str(e)}")
            raise

def get_google_credentials():
    """GitHub Secrets에서 JSON을 불러와 Credentials 생성"""
    google_credentials = os.environ.get("GOOGLE_CREDENTIALS_JSON")

    if not google_credentials:
        raise ValueError("Missing GOOGLE_CREDENTIALS_JSON environment variable.")

    try:
        credentials_dict = json.loads(google_credentials)  # JSON 문자열을 딕셔너리로 변환
        
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]

        return Credentials.from_service_account_info(credentials_dict, scopes=scope)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Credential error: {str(e)}")
        raise

def update_spreadsheet(worksheet_name, data):
    """Google Sheets 업데이트 (GitHub Secrets 사용)"""
    try:
        creds = get_google_credentials()
        client = gspread.authorize(creds)

        workbook = client.open("newdashboard raw")
        worksheet = workbook.worksheet(worksheet_name)

        if data:
            worksheet.batch_clear(["A2:H1000"])
            worksheet.update('A2', data)
            print(f"Updated {len(data)} rows in {worksheet_name}")

    except Exception as e:
        print(f"Error in spreadsheet update: {str(e)}")
        raise

def process_data(response):
    """데이터 처리"""
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
        return result
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def main():
    try:
        logger.info("Starting data collection")
        start_time = datetime.now()
        
        collector = KurlyDataCollector()
        
        # 한국 시간 기준 날짜 계산
        korean_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korean_tz)
        today_str = now.strftime("%Y-%m-%d")
        yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 병렬로 오늘/어제 데이터 수집 및 처리
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_date = {
                executor.submit(collector.get_data, today_str): ("today_kurlyro", today_str),
                executor.submit(collector.get_data, yesterday_str): ("yesterday_kurlyro", yesterday_str)
            }
            
            for future in as_completed(future_to_date):
                sheet_name, date = future_to_date[future]
                try:
                    data = future.result()
                    processed_data = process_data(data)
                    if processed_data:
                        update_spreadsheet(sheet_name, processed_data)
                except Exception as e:
                    logger.error(f"Error processing {date}: {str(e)}")

        execution_time = datetime.now() - start_time
        logger.info(f"Completed in {execution_time.total_seconds():.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
