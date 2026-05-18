import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
import io
from datetime import datetime, timedelta
import os
import json

# =====================================
# 1. Credentials Setup
# =====================================

creds_json = os.environ.get('GCP_CREDENTIALS')

if not creds_json:
    print("CRITICAL: GCP_CREDENTIALS secret missing!")
    exit(1)

creds_dict = json.loads(creds_json)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict,
    scope
)

client = gspread.authorize(creds)

# अपनी Google Sheet ID डालें
spreadsheet_id = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"

worksheet = client.open_by_key(
    spreadsheet_id
).worksheet("Top 250 Stocks")


# =====================================
# 2. NSE Data Fetcher
# =====================================

def fetch_bhavcopy_for_date(date_obj):

    date_str = date_obj.strftime("%Y%m%d")

    url = (
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    )

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        ),
        'Accept': '*/*'
    }

    try:

        print(f"Checking file for {date_str}...")

        response = requests.get(
            url,
            headers=headers,
            timeout=20
        )

        if response.status_code != 200:
            print(f"HTTP Error: {response.status_code}")
            return None

        try:
            z = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile:
            print("Invalid ZIP file")
            return None

        csv_filename = z.namelist()[0]

        with z.open(csv_filename) as f:

            df = pd.read_csv(f)

        # Remove extra spaces
        df.columns = [c.strip() for c in df.columns]

        # Dynamic column detection
        sym_col = next(
            (c for c in ['TckrSymb', 'SYMBOL']
             if c in df.columns),
            None
        )

        close_col = next(
            (c for c in ['ClsPric', 'CLOSE']
             if c in df.columns),
            None
        )

        series_col = next(
            (c for c in ['SctySrs', 'SERIES']
             if c in df.columns),
            None
        )

        turnover_col = next(
            (
                c for c in [
                    'TtlTrfVal',
                    'TtlTrdVal',
                    'TURNOVER_LACS',
                    'TURNOVER'
                ]
                if c in df.columns
            ),
            None
        )

        # Required columns check
        if not all([sym_col, close_col, turnover_col]):
            print("Required columns missing")
            return None

        # EQ only
        if series_col:
            df = df[
                df[series_col]
                .astype(str)
                .str.strip()
                == 'EQ'
            ]

        # Remove ETFs
        filter_keywords = 'BEES|ETF|GOLD|LIQUID'

        df = df[
            ~df[sym_col]
            .astype(str)
            .str.contains(
                filter_keywords,
                case=False,
                na=False
            )
        ]

        # Numeric conversion
        df[turnover_col] = pd.to_numeric(
            df[turnover_col],
            errors='coerce'
        )

        df = df.dropna(subset=[turnover_col])

        # Top 250
        df_top = (
            df.sort_values(
                by=turnover_col,
                ascending=False
            )
            .head(250)
        )

        return df_top[
            [sym_col, turnover_col, close_col]
        ].values.tolist()

    except Exception as e:

        print(f"Error: {str(e)}")

        return None


# =====================================
# 3. Execution Logic
# =====================================

date = datetime.now()

data_to_insert = None
fetched_date_str = ""

for i in range(7):

    test_date = date - timedelta(days=i)

    # Skip weekends
    if test_date.weekday() >= 5:
        continue

    data_to_insert = fetch_bhavcopy_for_date(test_date)

    if data_to_insert:

        fetched_date_str = test_date.strftime('%d-%b-%Y')

        break


# =====================================
# 4. Update Google Sheet
# =====================================

if data_to_insert:

    try:

        # Clear old data
        worksheet.batch_clear(['A2:C251'])

        # Update new data
        worksheet.update(
            range_name='A2',
            values=data_to_insert
        )

        ist_now = (
            datetime.utcnow()
            + timedelta(hours=5, minutes=30)
        ).strftime('%d-%b %H:%M')

        status_msg = (
            f"Data Date: {fetched_date_str} | "
            f"Last Update: {ist_now} (IST)"
        )

        worksheet.update(
            range_name='K2',
            values=[[status_msg]]
        )

        print(
            f"SUCCESS: Updated with "
            f"Turnover Data for {fetched_date_str}"
        )

    except Exception as e:

        print(f"Google Sheet Error: {str(e)}")

else:

    print(
        "FAILED: No valid file found "
        "in last 7 days."
    )
