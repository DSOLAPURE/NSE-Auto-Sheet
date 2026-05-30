"""
NSE Bhavcopy → Google Sheets Updater
Fetches top-250 stocks by Volume and Turnover from NSE's UDiFF bhavcopy
and writes them to two separate Google Sheets worksheets.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
import io
import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

# ──────────────────────────────────────────────────────────────
#  CONFIG  — change only here
# ──────────────────────────────────────────────────────────────

SPREADSHEET_ID   = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME     = "Top 250 Stocks"
SHEET_TURNOVER   = "Top 250 Turnover"
STATUS_CELL      = "K2"

TOP_N            = 250
LOOKBACK_DAYS    = 7       # how many calendar days to scan back
REQUEST_TIMEOUT  = 15      # seconds
MAX_RETRIES      = 3
RETRY_DELAY      = 3       # seconds between retries

# Symbols to exclude (ETFs, commodities, liquidity funds)
EXCLUDE_PATTERN  = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

# NSE UDiFF bhavcopy URL template
BHAVCOPY_URL     = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)

# Candidate column names in priority order (NSE changes these occasionally)
COL_CANDIDATES = {
    "symbol":   ["TckrSymb",    "SYMBOL"],
    "close":    ["ClsPric",     "CLOSE"],
    "series":   ["SctySrs",     "SERIES"],
    "volume":   ["TtlTradgVol", "TOTTRDQTY",  "TtlTrdQty", "TotTrdQty"],
    "turnover": ["TtlTrfVal",   "TOTTRDVAL",  "TtlTrdVal", "TotTrdVal"],
}

GSHEETS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    """Return the first candidate column that exists in df, or raise."""
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} found in DataFrame columns: {list(df.columns)}")


def _ist_now() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")


# ──────────────────────────────────────────────────────────────
#  NSE FETCHER
# ──────────────────────────────────────────────────────────────

class BhavcopFetcher:
    """Downloads and parses NSE CM bhavcopy ZIP for a given date."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://www.nseindia.com/",
    }

    def fetch(self, date: datetime) -> Optional[tuple[list, list]]:
        """
        Returns (data_volume, data_turnover) where each is a list of
        [symbol, metric, close] rows, or None if the date has no data.
        """
        url = BHAVCOPY_URL.format(date=date.strftime("%Y%m%d"))
        log.info("Trying %s → %s", date.strftime("%d-%m-%Y"), url)

        raw = self._download(url)
        if raw is None:
            return None

        df = self._read_zip(raw)
        if df is None or df.empty:
            return None

        df = self._clean(df)
        if df.empty:
            log.warning("DataFrame empty after filtering on %s", date.strftime("%d-%m-%Y"))
            return None

        sym_col      = _pick_col(df, COL_CANDIDATES["symbol"])
        close_col    = _pick_col(df, COL_CANDIDATES["close"])
        vol_col      = _pick_col(df, COL_CANDIDATES["volume"])
        turnover_col = _pick_col(df, COL_CANDIDATES["turnover"])

        data_vol = (
            df.sort_values(vol_col, ascending=False)
            .head(TOP_N)[[sym_col, vol_col, close_col]]
            .values.tolist()
        )

        data_to = (
            df.sort_values(turnover_col, ascending=False)
            .head(TOP_N)[[sym_col, turnover_col, close_col]]
            .values.tolist()
        )

        log.info("Fetched %d volume rows and %d turnover rows.", len(data_vol), len(data_to))
        return data_vol, data_to

    # ── private ──────────────────────────────────────────────

    def _download(self, url: str) -> Optional[bytes]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, headers=self.HEADERS, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.content
                log.warning("HTTP %s (attempt %d/%d)", r.status_code, attempt, MAX_RETRIES)
            except requests.RequestException as exc:
                log.warning("Request error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

        return None

    def _read_zip(self, raw: bytes) -> Optional[pd.DataFrame]:
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                csv_name = z.namelist()[0]
                with z.open(csv_name) as f:
                    return pd.read_csv(f, low_memory=False)
        except (zipfile.BadZipFile, KeyError, pd.errors.ParserError) as exc:
            log.error("Failed to parse ZIP: %s", exc)
            return None

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        series_col = _pick_col(df, COL_CANDIDATES["series"])
        sym_col    = _pick_col(df, COL_CANDIDATES["symbol"])

        # Keep EQ series only
        df = df[df[series_col].astype(str).str.strip() == "EQ"].copy()

        # Drop ETFs / commodity / liquidity funds
        mask = df[sym_col].astype(str).str.contains(EXCLUDE_PATTERN, case=False, na=False)
        df = df[~mask]

        return df.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
#  GOOGLE SHEETS WRITER
# ──────────────────────────────────────────────────────────────

class SheetsWriter:
    """Authenticates and writes data to Google Sheets."""

    def __init__(self, creds_json: str) -> None:
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSHEETS_SCOPES)
        self._client = gspread.authorize(creds)

    def open_worksheets(self) -> tuple[gspread.Worksheet, gspread.Worksheet]:
        ss = self._client.open_by_key(SPREADSHEET_ID)
        return ss.worksheet(SHEET_VOLUME), ss.worksheet(SHEET_TURNOVER)

    def write(
        self,
        ws_vol: gspread.Worksheet,
        ws_to:  gspread.Worksheet,
        data_vol: list,
        data_to:  list,
        fetched_date: str,
    ) -> None:
        status = f"Data Date: {fetched_date} | Last Update: {_ist_now()} (IST)"

        for ws, data in [(ws_vol, data_vol), (ws_to, data_to)]:
            end_row = 1 + len(data)
            ws.batch_update(
                [
                    {"range": f"A2:C{end_row}", "values": data},
                    {"range": STATUS_CELL,        "values": [[status]]},
                ],
                value_input_option="USER_ENTERED",
            )
            log.info("Updated '%s' with %d rows.", ws.title, len(data))


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    # ── 1. Load credentials ──────────────────────────────────
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GCP_CREDENTIALS environment variable is not set.")

    # ── 2. Connect to Sheets ─────────────────────────────────
    log.info("Connecting to Google Sheets…")
    writer = SheetsWriter(creds_json)
    ws_volume, ws_turnover = writer.open_worksheets()

    # ── 3. Fetch bhavcopy (most recent trading day) ──────────
    fetcher = BhavcopFetcher()
    today = datetime.utcnow() + timedelta(hours=5, minutes=30)  # use IST date

    result = None
    fetched_date_str = ""

    for days_back in range(LOOKBACK_DAYS + 1):
        candidate = today - timedelta(days=days_back)
        if candidate.weekday() >= 5:          # skip weekends
            continue
        result = fetcher.fetch(candidate)
        if result:
            fetched_date_str = candidate.strftime("%d-%b-%Y")
            break

    if result is None:
        raise RuntimeError(
            f"No bhavcopy data found for the last {LOOKBACK_DAYS} trading days."
        )

    data_vol, data_to = result

    # ── 4. Write to Sheets ───────────────────────────────────
    log.info("Writing to Google Sheets…")
    writer.write(ws_volume, ws_turnover, data_vol, data_to, fetched_date_str)

    log.info(
        "SUCCESS — Both sheets updated with data from %s.", fetched_date_str
    )


if __name__ == "__main__":
    main()
