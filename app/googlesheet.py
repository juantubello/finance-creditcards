import os
import gspread
import json
#from  dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

SHEET_MES_ACTUAL = 5
SHEET_COMPLETA = 1

def auth_in_gdrive():

    #load_dotenv()

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
    sheet = client.open("Gastos").get_worksheet(SHEET_MES_ACTUAL)
    sheet_data = sheet.get_all_records()
    return sheet_data

def get_historic_expenses(client):
    sheet = client.open("Gastos").get_worksheet(SHEET_COMPLETA)
    sheet_data = sheet.get_all_records()
    return sheet_data

## Logic ##

#client = auth_in_gdrive()
#gs = SHEET_MES_ACTUAL
#gs = SHEET_COMPLETA
#sheet = client.open("Gastos").get_worksheet(gs)
#sheet_data = sheet.get_all_records()
#uuids_from_excel = set(row["UUID"] for row in sheet_data if row["UUID"])
#print(uuids_from_excel)
#uuid_column = sheet.col_values(5)

#prueba = sheet.
#test = sheet.update_cell(rows_count, 2, 15000)
#print(uuid_column)


#db_data = sheet.get_all_records()
#print(db_data)
#db_data = sheet.col_values(1)wqw
