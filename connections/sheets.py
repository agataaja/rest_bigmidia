import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
KEY_FILE = os.path.join(BASE_DIR, 'static\json\certificates-manager-cbw-93a56e120c6a.json')


# Google Sheets API setup
def setup_google_sheet(nome_planilha, work_sheet_name):

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Replace with the path to your downloaded JSON key file
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)

    client = gspread.authorize(creds)

    # Open the sheet by name
    sheet = (client.open(nome_planilha).
             worksheet(work_sheet_name))

    return sheet


def get_data_from_sheet(nome_planilha, work_sheet_number):

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Replace with the path to your downloaded JSON key file
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)

    client = gspread.authorize(creds)

    # Open the sheet by name
    sheet = client.open(nome_planilha).get_worksheet(work_sheet_number)

    data = sheet.get_all_records()

    return data
