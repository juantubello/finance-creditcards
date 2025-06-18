import os
import gspread
import json
#from  dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
SHEET_NAME = "Gastos"
SHEET_CURRENT_MONTH_EXPENSES = 5
SHEET_HISTORIC_EXPENSES = 1
SHEET_CURRENT_MONTH_INCOME = 6
SHEET_HISTORIC_INCOME = 3

def auth_in_gdrive():

    """
    This function logs into DRIVE API in order to interact, in this case, with our DATABASE
    """
    json_creds = json.loads(os.getenv('GOOGLE_SHEETS_CREDS_JSON'))
    with open('gcreds.json', 'w') as fp:
         json.dump(json_creds, fp)

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'] #Add Endpoint to make it work
    creds = ServiceAccountCredentials.from_json_keyfile_name('gcreds.json', scope)
    client = gspread.authorize(creds)

    return client

def get_current_month_expenses(client):
    sheet = client.open(SHEET_NAME).get_worksheet(SHEET_CURRENT_MONTH_EXPENSES)
    sheet_data = sheet.get_all_records()
    return sheet_data

def get_historic_expenses(client):
    sheet = client.open(SHEET_NAME).get_worksheet(SHEET_HISTORIC_EXPENSES)
    sheet_data = sheet.get_all_records()
    return sheet_data

def get_current_month_income(client):
    sheet = client.open(SHEET_NAME).get_worksheet(SHEET_CURRENT_MONTH_INCOME)
    sheet_data = sheet.get_all_records()
    print(sheet_data)
    return sheet_data

def get_historic_income(client):
    sheet = client.open(SHEET_NAME).get_worksheet(SHEET_HISTORIC_INCOME)
    sheet_data = sheet.get_all_records()
    return sheet_data