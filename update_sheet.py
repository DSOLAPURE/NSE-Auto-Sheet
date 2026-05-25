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
    print("ERROR: GCP_CREDENTIALS secret missing!")
    exit(1)

creds_dict = json.loads(creds_json)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# आपकी शीट की ID 
spreadsheet_id = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"

# दोनों शीट्स को कनेक्ट करना
try:
    ws_volume = client.open_by_key(spreadsheet_id).worksheet("Top 250 Stocks")
    ws_turnover = client.open_by_key(spreadsheet_id).worksheet("Top 250 Turnover")
except Exception as e:
    print(f"Sheet Connection Error: {e}")
    exit(1)

# 2. New NSE UDiFF Data Fetcher
def fetch_bhavcopy_for_date(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    print(f"--- तारीख {date_obj.strftime('%d-%m-%Y')} चेक कर रहे हैं ---")
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            print("फाइल मिल गई! अब इसे खोल रहे हैं...")
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pd.read_csv(f)
                    
                    # नए कॉलम के नाम खोजना
                    sym_col = 'TckrSymb' if 'TckrSymb' in df.columns else 'SYMBOL'
                    close_col = 'ClsPric' if 'ClsPric' in df.columns else 'CLOSE'
                    series_col = 'SctySrs' if 'SctySrs' in df.columns else 'SERIES'
                    
                    # 1. वॉल्यूम कॉलम ढूँढना
                    vol_col = 'TtlTradgVol'
                    for c in ['TtlTradgVol', 'TOTTRDQTY', 'TtlTrdQty', 'TotTrdQty']:
                        if c in df.columns:
                            vol_col = c
                            break
                            
                    # 2. टर्नओवर कॉलम ढूँढना (TtlTrfVal)
                    turnover_col = 'TtlTrfVal'
                    for c in ['TtlTrfVal', 'TOTTRDVAL', 'TtlTrdVal', 'TotTrdVal']:
                        if c in df.columns:
                            turnover_col = c
                            break
                    
                    # सिर्फ EQ सीरीज छांटना
                    if series_col in df.columns:
                        df = df[df[series_col].astype(str).str.strip() == 'EQ']
                    
                    # ETF, GOLD, LIQUID हटाना
                    filter_keywords = 'BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ'
                    df = df[~df[sym_col].astype(str).str.contains(filter_keywords, case=False, na=False)]
                    
                    # --- डेटा को दो भागों में बाँटना ---
                    
                    # लिस्ट A: वॉल्यूम के आधार पर टॉप 250
                    df_vol = df.sort_values(by=vol_col, ascending=False).head(250)
                    data_vol = df_vol[[sym_col, vol_col, close_col]].values.tolist()
                    
                    # लिस्ट B: टर्नओवर के आधार पर टॉप 250
                    df_turnover = df.sort_values(by=turnover_col, ascending=False).head(250)
                    data_turnover = df_turnover[[sym_col, turnover_col, close_col]].values.tolist()
                    
                    return data_vol, data_turnover
        else:
            print(f"NSE सर्वर ने {response.status_code} रिस्पॉन्स दिया।")
            return None, None
    except Exception as e:
        print(f"Error: {e}")
        return None, None

# 3. Execution Logic (7 दिन पीछे तक चेक करना)
date = datetime.now()
data_vol_to_insert = None
data_turnover_to_insert = None
fetched_date_str = ""

for i in range(7):
    test_date = date - timedelta(days=i)
    if test_date.weekday() >= 5: # Skip Sat/Sun
        continue
        
    data_vol, data_turnover = fetch_bhavcopy_for_date(test_date)
    if data_vol and data_turnover:
        data_vol_to_insert = data_vol
        data_turnover_to_insert = data_turnover
        fetched_date_str = test_date.strftime('%d-%b-%Y')
        break

# 4. Update Both Sheets
if data_vol_to_insert and data_turnover_to_insert:
    try:
        # A. वॉल्यूम वाली पुरानी शीट अपडेट करें
        ws_volume.batch_clear(['A2:C251'])
        ws_volume.update('A2', data_vol_to_insert)
        
        # B. टर्नओवर वाली नई शीट अपडेट करें
        ws_turnover.batch_clear(['A2:C251'])
        ws_turnover.update('A2', data_turnover_to_insert)
        
        # टाइमस्टैम्प अपडेट करें
        ist_now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M')
        status_msg = f"Data Date: {fetched_date_str} | Last Update: {ist_now} (IST)"
        
        ws_volume.update('K2', [[status_msg]])
        ws_turnover.update('K2', [[status_msg]])
        
        print(f"SUCCESS: दोनों शीट्स (Volume और Turnover) {fetched_date_str} के डेटा से अपडेट हो गई हैं!")
    except Exception as e:
        print(f"Google Sheet अपडेट करने में एरर: {e}")
        exit(1)
else:
    print("FAILED: पिछले 7 दिनों में से किसी भी दिन की फाइल नहीं मिली।")
    exit(1)
