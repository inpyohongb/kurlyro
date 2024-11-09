import requests
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import logging

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
        worksheet.batch_clear(["A2:G1000"])
        
        # 새 데이터 업데이트
        worksheet.update('A2', data)
        logger.info(f"Successfully updated {len(data)} rows of data in {worksheet_name}")
            
    except Exception as e:
        logger.error(f"Error in spreadsheet update: {str(e)}")
        raise

def getdata(date):
    try:
        loginurl = "https://api-lms.kurly.com/v1/admin-accounts/login"
        idpw = {"loginId": "ian980608", "password": "ssh2019!"}
        
        login_response = requests.post(loginurl, json=idpw)
        login_response.raise_for_status()
        token = login_response.json()['data']['token']
        logger.info("Login successful")

        headers = {'authorization': 'Bearer ' + token}
        url = "https://api-lms.kurly.com/v1/commutes/end"

        params = {
            "cluster": "CC03", 
            "center": "GPM1", 
            "dateFrom": date, 
            "dateTo": date
        }

        response = requests.get(url, headers=headers, params=params).json()['data']
        logger.info(f"Retrieved {len(response)} records for date {date}")

        return response
    except Exception as e:
        logger.error(f"Error getting data: {str(e)}")
        raise

def process_data(response):
    try:
        IB_datas = []

        for i in response:
            if i['workPart'] == 'IB':
                emp_type = '상용직' if i['contractType'] == 'CONTRACT' else '일용직'
                IB_datas.append([
                    emp_type,
                    i['userId'].replace(' ', ''),
                    i['name'].replace(' ', ''),
                    i['workProcess'].replace(' ', ''),
                    i['workProcessDetail'].replace(' ', ''),
                    i['checkInTime'].replace(' ', ''),
                    i['checkOutTime']
                ])
                logger.debug(f"Added IB record: {i['name']} - {i['checkInTime']}")

        IB_datas.reverse()
        logger.info(f"Processed {len(IB_datas)} IB records")

        return IB_datas
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def run():
    try:
        logger.info("Starting data collection and update process")
        
        # 오늘 날짜 데이터 처리
        today = datetime.now().strftime("%Y-%m-%d")
        today_data = process_data(getdata(today))
        if len(today_data) > 0:
            update_spreadsheet("today_kurlyro", today_data)
        
        # 어제 날짜 데이터 처리
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_data = process_data(getdata(yesterday))
        if len(yesterday_data) > 0:
            update_spreadsheet("yesterday_kurlyro", yesterday_data)
        
        logger.info("Completed data update cycle")
        
    except Exception as e:
        logger.error(f"Error in run function: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting application")
    while True:
        try:
            run()
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        logger.info("Waiting 300 seconds before next cycle")
        time.sleep(300)