import gspread

from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

import requests

import zipfile

import io

from datetime import datetime, timedelta

import os

import json



# 1. Credentials Setup

creds_json = os.environ.get('GCP_CREDENTIALS')

if not creds_json:

    print("CRITICAL: GCP_CREDENTIALS secret missing!")

    exit(1)



creds_dict = json.loads(creds_json)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

client = gspread.authorize(creds)



# ⚠️ अपनी गूगल शीट की ID यहाँ दोबारा डालना न भूलें

spreadsheet_id = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4" 

worksheet = client.open_by_key(spreadsheet_id).worksheet("Top 250 Stocks")वॉल्यूम डेटा टूल



# 2. NSE Data Fetcher

def fetch_bhavcopy_for_date(date_obj):

    date_str = date_obj.strftime("%Y%m%d")

    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"

    

    headers = {

        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',

        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'

    }

    

    try:

        print(f"तारीख {date_str} की फाइल चेक कर रहे हैं...")

        response = requests.get(url, headers=headers, timeout=20)

        

        if response.status_code == 200:

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:

                csv_filename = z.namelist()[0]

                with z.open(csv_filename) as f:

                    df = pd.read_csv(f)

                    

                    # कॉलम के नाम से एक्स्ट्रा स्पेस हटाना

                    df.columns = [c.strip() for c in df.columns]

                    

                    sym_col = next((c for c in ['TckrSymb', 'SYMBOL'] if c in df.columns), None)

                    close_col = next((c for c in ['ClsPric', 'CLOSE'] if c in df.columns), None)



                    series_col = next((c for c in ['SctySrs', 'SERIES'] if c in df.columns), None)

                    

                    # यहाँ हमने TtlTrfVal (NSE का असली नाम) जोड़ दिया है

                    turnover_col = next((c for c in ['TtlTrfVal', 'TtlTrdVal', 'TURNOVER_LACS', 'TURNOVER'] if c in df.columns), None)

                    

                    if not all([sym_col, close_col, turnover_col]):

                        return None

                    

                    if series_col:

                        df = df[df[series_col].astype(str).str.strip() == 'EQ']

                    

                    filter_keywords = 'BEES|ETF|GOLD|LIQUID'

                    df = df[~df[sym_col].astype(str).str.contains(filter_keywords, case=False, na=False)]

                    

                    df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')

                    df = df.dropna(subset=[turnover_col])

                    

                    df_top = df.sort_values(by=turnover_col, ascending=False).head(250)

                    return df_top[[sym_col, turnover_col, close_col]].values.tolist()

        return None

    except Exception as e:

        return None



# 3. Execution Logic

date = datetime.now()

data_to_insert = None

fetched_date_str = ""



for i in range(7): 

    test_date = date - timedelta(days=i)

    if test_date.weekday() >= 5: continue

        

    data_to_insert = fetch_bhavcopy_for_date(test_date)

    

    if data_to_insert:

        fetched_date_str = test_date.strftime('%d-%b-%Y')

        break



# 4. Update Sheet

if data_to_insert:

    try:

        worksheet.batch_clear(['A2:C251'])

        worksheet.update('A2', data_to_insert)

        ist_now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M')

        status_msg = f"Data Date: {fetched_date_str} | Last Update: {ist_now} (IST)"

        worksheet.update('K2', [[status_msg]])

        print(f"SUCCESS: Sheet Updated successfully with Turnover Data for {fetched_date_str}!")

    except Exception as e:

        print(f"Google Sheet Error: {str(e)}")वेब Apps और ऑनलाइन टूल

else:


    print("FAILED: पिछले 7 दिनों में से किसी भी दिन की फाइल नहीं मिली या प्रोसेस नहीं हुई।")
