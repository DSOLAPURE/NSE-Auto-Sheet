import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
import io
from datetime import datetime, timedelta
import os
import json

# ==========================================
# 1. GOOGLE SHEETS AUTHENTICATION
# ==========================================

creds_json = os.environ.get("GCP_CREDENTIALS")

if not creds_json:
    print("CRITICAL ERROR: GCP_CREDENTIALS not found!")
    exit(1)

try:
    creds_dict = json.loads(creds_json)
except Exception as e:
    print(f"Invalid GCP_CREDENTIALS JSON: {e}")
    exit(1)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict,
    scope
)

client = gspread.authorize(creds)

# ==========================================
# 2. GOOGLE SHEET SETUP
# ==========================================

# ⚠️ Replace with your actual Google Sheet ID
spreadsheet_id = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"

try:
    worksheet = client.open_by_key(
        spreadsheet_id
    ).worksheet("Top 250 Stocks")

except Exception as e:
    print(f"Google Sheet Connection Error: {e}")
    exit(1)

# ==========================================
# 3. NSE DATA FETCHER
# ==========================================

def fetch_bhavcopy_for_date(date_obj):

    date_str = date_obj.strftime("%Y%m%d")

    url = (
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36"
        ),
        "Accept": "*/*"
    }

    try:

        print(f"Checking NSE file for {date_str}...")

        response = requests.get(
            url,
            headers=headers,
            timeout=20
        )

        if response.status_code != 200:

            print(
                f"HTTP Error {response.status_code} "
                f"for {date_str}"
            )

            return None

        # Validate ZIP
        try:
            z = zipfile.ZipFile(
                io.BytesIO(response.content)
            )

        except zipfile.BadZipFile:

            print(f"Invalid ZIP file for {date_str}")

            return None

        csv_filename = z.namelist()[0]

        with z.open(csv_filename) as f:

            df = pd.read_csv(f)

        # ==========================================
        # CLEAN COLUMN NAMES
        # ==========================================

        df.columns = [c.strip() for c in df.columns]

        # ==========================================
        # DYNAMIC COLUMN DETECTION
        # ==========================================

        sym_col = next(
            (
                c for c in [
                    "TckrSymb",
                    "SYMBOL"
                ]
                if c in df.columns
            ),
            None
        )

        close_col = next(
            (
                c for c in [
                    "ClsPric",
                    "CLOSE"
                ]
                if c in df.columns
            ),
            None
        )

        series_col = next(
            (
                c for c in [
                    "SctySrs",
                    "SERIES"
                ]
                if c in df.columns
            ),
            None
        )

        turnover_col = next(
            (
                c for c in [
                    "TtlTrfVal",
                    "TtlTrdVal",
                    "TURNOVER_LACS",
                    "TURNOVER"
                ]
                if c in df.columns
            ),
            None
        )

        # ==========================================
        # REQUIRED COLUMN CHECK
        # ==========================================

        if not all([sym_col, close_col, turnover_col]):

            print(
                "Required columns missing in file"
            )

            return None

        # ==========================================
        # FILTER EQ SERIES ONLY
        # ==========================================

        if series_col:

            df = df[
                df[series_col]
                .astype(str)
                .str.strip()
                == "EQ"
            ]

        # ==========================================
        # REMOVE ETFs / GOLD / LIQUID
        # ==========================================

        filter_keywords = (
            "BEES|ETF|GOLD|LIQUID|SILVER"
        )

        df = df[
            ~df[sym_col]
            .astype(str)
            .str.contains(
                filter_keywords,
                case=False,
                na=False
            )
        ]

        # ==========================================
        # NUMERIC CONVERSION
        # ==========================================

        df[turnover_col] = pd.to_numeric(
            df[turnover_col],
            errors="coerce"
        )

        df = df.dropna(subset=[turnover_col])

        # ==========================================
        # SORT TOP 250
        # ==========================================

        df_top = (
            df.sort_values(
                by=turnover_col,
                ascending=False
            )
            .head(250)
        )

        # ==========================================
        # RETURN DATA
        # ==========================================

        return df_top[
            [sym_col, turnover_col, close_col]
        ].values.tolist()

    except Exception as e:

        print(
            f"Processing Error for {date_str}: {e}"
        )

        return None

# ==========================================
# 4. FETCH LATEST AVAILABLE DATA
# ==========================================

date = datetime.now()

data_to_insert = None

fetched_date_str = ""

# Check last 7 days
for i in range(7):

    test_date = date - timedelta(days=i)

    # Skip weekends
    if test_date.weekday() >= 5:
        continue

    data_to_insert = fetch_bhavcopy_for_date(
        test_date
    )

    if data_to_insert:

        fetched_date_str = test_date.strftime(
            "%d-%b-%Y"
        )

        break

# ==========================================
# 5. UPDATE GOOGLE SHEET
# ==========================================

if data_to_insert:

    try:

        print("Updating Google Sheet...")

        # Clear old data
        worksheet.batch_clear([
            "A2:C251"
        ])

        # Optional: Update Headers
        worksheet.update(
            values=[
                [
                    "SYMBOL",
                    "TURNOVER",
                    "CLOSE"
                ]
            ],
            range_name="A1:C1"
        )

        # Update main data
        worksheet.update(
            values=data_to_insert,
            range_name="A2:C251"
        )

        # IST Timestamp
        ist_now = (
            datetime.utcnow()
            + timedelta(hours=5, minutes=30)
        ).strftime("%d-%b %H:%M")

        status_msg = (
            f"Data Date: {fetched_date_str} | "
            f"Last Update: {ist_now} (IST)"
        )

        # Update status cell
        worksheet.update(
            values=[[status_msg]],
            range_name="K2"
        )

        print(
            f"SUCCESS: Updated Turnover Data "
            f"for {fetched_date_str}"
        )

    except Exception as e:

        print(
            f"Google Sheet Update Error: {e}"
        )

else:

    print(
        "FAILED: No valid NSE file found "
        "in last 7 days."
    )
