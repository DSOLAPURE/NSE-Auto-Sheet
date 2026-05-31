"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NSE F&O Auto-Sheet  —  update_sheet.py  (v5 — Final Production Build)     ║
║  GitHub: DSOLAPURE/NSE-Auto-Sheet                                           ║
║                                                                              ║
║  Sheets updated:                                                             ║
║    1. Top 250 Stocks    — top 250 NSE equities by trading volume            ║
║    2. Top 250 Turnover  — top 250 NSE equities by turnover value            ║
║    3. Futures F&O       — 5 indices + all F&O stocks (36 columns)           ║
║    4. Options F&O       — 5 indices + all F&O stocks (40 columns)           ║
║                                                                              ║
║  Data sources:                                                               ║
║    • Equity CMP/Vol   → NSE UDiFF bhavcopy ZIP (daily)                     ║
║    • Index CMP        → NSE ind_close_all_{date}.csv (daily)               ║
║    • Lot sizes        → NSE fo_mktlots.csv (official, free)                ║
║    • Expiry dates     → computed: last Thursday of contract month           ║
║    • ATM premiums     → Black-Scholes approximation (IV ~28%)               ║
║    • Trend signals    → 5-day vs 20-day avg close momentum                 ║
║                                                                              ║
║  Runs: Mon–Fri 06:30 IST (morning) + 16:00 IST (EOD) via GitHub Actions   ║
╚══════════════════════════════════════════════════════════════════════════════╝
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
import math
import time
from datetime import datetime, timedelta, date

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SPREADSHEET_ID  = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME    = "Top 250 Stocks"
SHEET_TURNOVER  = "Top 250 Turnover"
SHEET_FUTURES   = "Futures F&O"
SHEET_OPTIONS   = "Options F&O"
STATUS_CELL     = "K2"           # cell that shows last-updated timestamp
TOP_N           = 250            # rows in volume / turnover sheets
LOOKBACK_DAYS   = 7              # trading days to look back for bhavcopy
REQUEST_TIMEOUT = 25             # seconds per HTTP request
MAX_RETRIES     = 3
RETRY_DELAY     = 5              # seconds between retries
HISTORY_DAYS    = 15             # trading days of history for trend calc
WRITE_CHUNK     = 150            # rows per Sheets API batch write
EXCLUDE_PATTERN = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

# NSE data URLs
BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)
INDEX_CSV_URL = (
    "https://nsearchives.nseindia.com/content/indices/"
    "ind_close_all_{date}.csv"
)
MKTLOTS_URL = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

# Possible column-name variants across NSE CSV format versions
COL_MAP = {
    "symbol":   ["TckrSymb",    "SYMBOL"],
    "close":    ["ClsPric",     "CLOSE"],
    "series":   ["SctySrs",     "SERIES"],
    "volume":   ["TtlTradgVol", "TOTTRDQTY", "TtlTrdQty",  "TotTrdQty"],
    "turnover": ["TtlTrfVal",   "TOTTRDVAL", "TtlTrdVal",  "TotTrdVal"],
}

GSHEETS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — STATIC REFERENCE DATA
# ══════════════════════════════════════════════════════════════════════════════

# 5 Indices — always written first in both F&O sheets
# (sym, display_name, sector_label, futures_margin_pct)
INDEX_META = [
    ("NIFTY",      "Nifty 50",            "Index", 13),
    ("BANKNIFTY",  "Bank Nifty",          "Index", 13),
    ("FINNIFTY",   "Nifty Financial Svc", "Index", 13),
    ("MIDCPNIFTY", "Nifty Midcap Select", "Index", 13),
    ("NIFTYNXT50", "Nifty Next 50",       "Index", 13),
]
INDEX_SYMS = {r[0] for r in INDEX_META}

# NSE index names as they appear in ind_close_all CSV → our symbol
INDEX_NAME_MAP = {
    "Nifty 50":                  "NIFTY",
    "Nifty Bank":                "BANKNIFTY",
    "Nifty Financial Services":  "FINNIFTY",
    "Nifty Fin Services":        "FINNIFTY",
    "Nifty Midcap Select":       "MIDCPNIFTY",
    "Nifty Next 50":             "NIFTYNXT50",
    # Upper-case alternates
    "NIFTY 50":                  "NIFTY",
    "NIFTY BANK":                "BANKNIFTY",
    "NIFTY FINANCIAL SERVICES":  "FINNIFTY",
    "NIFTY MIDCAP SELECT":       "MIDCPNIFTY",
    "NIFTY NEXT 50":             "NIFTYNXT50",
}

# Fallback CMP used only when the NSE index CSV fetch fails entirely
INDEX_FALLBACK_CMP = {
    "NIFTY":      24500.0,
    "BANKNIFTY":  52000.0,
    "FINNIFTY":   23800.0,
    "MIDCPNIFTY": 12400.0,
    "NIFTYNXT50": 67000.0,
}

# Fallback lot sizes (NSE Jan–Mar 2026 revision) — used if fo_mktlots.csv fails
FALLBACK_LOTS = {
    # Indices
    "NIFTY":75,"BANKNIFTY":30,"FINNIFTY":60,"MIDCPNIFTY":120,"NIFTYNXT50":25,
    # Nifty 50 stocks
    "ADANIENT":250,"ADANIPORTS":1250,"APOLLOHOSP":125,"ASIANPAINT":300,
    "AXISBANK":1200,"BAJAJ-AUTO":75,"BAJFINANCE":125,"BAJAJFINSV":500,
    "BEL":3750,"BPCL":1800,"BHARTIARTL":500,"BRITANNIA":125,"CIPLA":650,
    "COALINDIA":1350,"DIVISLAB":200,"DRREDDY":125,"EICHERMOT":175,
    "GRASIM":375,"HCLTECH":700,"HDFCBANK":550,"HDFCLIFE":1100,
    "HEROMOTOCO":300,"HINDALCO":1400,"HINDUNILVR":300,"ICICIBANK":700,
    "INDUSINDBK":1000,"INFY":400,"ITC":1600,"JSWSTEEL":900,
    "KOTAKBANK":400,"LT":300,"LTIM":150,"M&M":700,"MARUTI":100,
    "NESTLEIND":50,"NTPC":2250,"ONGC":1925,"POWERGRID":2300,
    "RELIANCE":250,"SBILIFE":750,"SHRIRAMFIN":500,"SBIN":1500,
    "SUNPHARMA":700,"TCS":175,"TATACONSUM":550,"TATAMOTORS":1400,
    "TATASTEEL":3500,"TECHM":600,"TITAN":225,"TRENT":275,
    "ULTRACEMCO":100,"WIPRO":1500,
    # Other popular F&O stocks
    "AUBANK":500,"AUROPHARMA":500,"DMART":100,"BAJAJHLDNG":50,
    "BALKRISIND":200,"BANDHANBNK":1875,"BANKBARODA":4350,"BERGEPAINT":1100,
    "BHARATFORG":500,"BIOCON":2400,"BSE":250,"CANBK":5000,"CHOLAFIN":750,
    "CUMMINSIND":300,"DABUR":1250,"DEEPAKNTR":375,"DIXON":100,"DLF":1650,
    "ESCORTS":275,"FEDERALBNK":5000,"GAIL":6400,"GODREJCP":500,
    "GODREJPROP":325,"GUJGASLTD":1250,"HAVELLS":500,"HDFCAMC":300,
    "HAL":500,"HINDPETRO":1700,"IDFCFIRSTB":10000,"IEX":3750,
    "INDHOTEL":1500,"IOC":5750,"IRFC":7500,"IGL":1375,"INDIGO":300,
    "IRCTC":875,"IREDA":2000,"JINDALSTEL":875,"JUBLFOOD":1250,
    "KAJARIACER":500,"KEC":750,"LTF":5000,"LTTS":200,"LAURUSLABS":1000,
    "LICI":700,"LUPIN":425,"LODHA":1000,"M&MFIN":3000,"MANAPPURAM":3000,
    "MARICO":1200,"MFSL":700,"MPHASIS":400,"MRF":10,"NAUKRI":150,
    "NAVINFLUOR":100,"NMDC":5750,"OBEROIRLTY":300,"OIL":1750,
    "PAGEIND":15,"PERSISTENT":250,"PETRONET":3000,"PIIND":200,
    "PIDILITIND":250,"POLYCAB":250,"PREMIERENE":500,"PNB":8000,
    "RVNL":2500,"RECLTD":2000,"MOTHERSON":5750,"SBICARD":1000,
    "SIEMENS":275,"SRF":125,"SAIL":7000,"SUNTV":750,"SWIGGY":3000,
    "TATACOMM":500,"TATAELXSI":175,"TATAPOWER":3375,"TORNTPHARM":250,
    "TORNTPOWER":500,"TVSMOTOR":350,"UPL":2000,"VEDL":2750,
    "VOLTAS":500,"WAAREEENER":100,"YESBANK":40000,"ZEEL":3000,"ZOMATO":4750,
}

SECTOR_MAP = {
    "ADANIENT":"Conglomerate","ADANIPORTS":"Infrastructure",
    "APOLLOHOSP":"Healthcare","ASIANPAINT":"FMCG","AXISBANK":"Banking",
    "BAJAJ-AUTO":"Auto","BAJFINANCE":"NBFC","BAJAJFINSV":"Financial Svc",
    "BEL":"Defence","BPCL":"Oil & Gas","BHARTIARTL":"Telecom",
    "BRITANNIA":"FMCG","CIPLA":"Pharma","COALINDIA":"Mining",
    "DIVISLAB":"Pharma","DRREDDY":"Pharma","EICHERMOT":"Auto",
    "GRASIM":"Diversified","HCLTECH":"IT","HDFCBANK":"Banking",
    "HDFCLIFE":"Insurance","HEROMOTOCO":"Auto","HINDALCO":"Metals",
    "HINDUNILVR":"FMCG","ICICIBANK":"Banking","INDUSINDBK":"Banking",
    "INFY":"IT","ITC":"FMCG","JSWSTEEL":"Steel","KOTAKBANK":"Banking",
    "LT":"Infrastructure","LTIM":"IT","M&M":"Auto","MARUTI":"Auto",
    "NESTLEIND":"FMCG","NTPC":"Power","ONGC":"Oil & Gas",
    "POWERGRID":"Power","RELIANCE":"Energy/Retail","SBILIFE":"Insurance",
    "SHRIRAMFIN":"NBFC","SBIN":"Banking","SUNPHARMA":"Pharma",
    "TCS":"IT","TATACONSUM":"FMCG","TATAMOTORS":"Auto","TATASTEEL":"Steel",
    "TECHM":"IT","TITAN":"Consumer","TRENT":"Retail","ULTRACEMCO":"Cement",
    "WIPRO":"IT","AUBANK":"Banking","AUROPHARMA":"Pharma","DMART":"Retail",
    "BAJAJHLDNG":"Holding","BALKRISIND":"Auto Ancillary",
    "BANDHANBNK":"Banking","BANKBARODA":"Banking","BERGEPAINT":"Paints",
    "BHARATFORG":"Auto Ancillary","BIOCON":"Biotech","BSE":"Exchange",
    "CANBK":"Banking","CHOLAFIN":"NBFC","CUMMINSIND":"Engineering",
    "DABUR":"FMCG","DEEPAKNTR":"Chemicals","DIXON":"Electronics",
    "DLF":"Real Estate","ESCORTS":"Auto","FEDERALBNK":"Banking",
    "GAIL":"Gas","GODREJCP":"FMCG","GODREJPROP":"Real Estate",
    "GUJGASLTD":"Gas","HAVELLS":"Electricals","HDFCAMC":"AMC",
    "HAL":"Defence","HINDPETRO":"Oil & Gas","IDFCFIRSTB":"Banking",
    "IEX":"Exchange","INDHOTEL":"Hospitality","IOC":"Oil & Gas",
    "IRFC":"NBFC","IGL":"Gas","INDIGO":"Aviation","IRCTC":"Tourism",
    "IREDA":"NBFC","JINDALSTEL":"Steel","JUBLFOOD":"QSR",
    "KAJARIACER":"Tiles","KEC":"Infrastructure","LTF":"NBFC","LTTS":"IT",
    "LAURUSLABS":"Pharma","LICI":"Insurance","LUPIN":"Pharma",
    "LODHA":"Real Estate","M&MFIN":"NBFC","MANAPPURAM":"NBFC",
    "MARICO":"FMCG","MFSL":"Insurance","MPHASIS":"IT","MRF":"Tyres",
    "NAUKRI":"Internet","NAVINFLUOR":"Chemicals","NMDC":"Mining",
    "OBEROIRLTY":"Real Estate","OIL":"Oil & Gas","PAGEIND":"Textiles",
    "PERSISTENT":"IT","PETRONET":"Gas","PIIND":"Agrochem",
    "PIDILITIND":"Chemicals","POLYCAB":"Electricals","PREMIERENE":"Solar",
    "PNB":"Banking","RVNL":"Infrastructure","RECLTD":"NBFC",
    "MOTHERSON":"Auto Ancillary","SBICARD":"NBFC","SIEMENS":"Engineering",
    "SRF":"Chemicals","SAIL":"Steel","SUNTV":"Media","SWIGGY":"Internet",
    "TATACOMM":"Telecom","TATAELXSI":"IT","TATAPOWER":"Power",
    "TORNTPHARM":"Pharma","TORNTPOWER":"Power","TVSMOTOR":"Auto",
    "UPL":"Agrochem","VEDL":"Metals","VOLTAS":"Consumer Durables",
    "WAAREEENER":"Solar","YESBANK":"Banking","ZEEL":"Media","ZOMATO":"Internet",
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _pick_col(df: pd.DataFrame, candidates: list) -> str:
    """Return first matching column name from candidates list."""
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} found in columns: {list(df.columns)}")


def _ist_now() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime(
        "%d-%b-%Y %H:%M IST"
    )


def _col_letter(n: int) -> str:
    """Convert 1-based column number to Excel-style letter (A, B, …, AA, …)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _last_thursday_of_month(year: int, month: int) -> date:
    """Return the last Thursday of the given month."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    # weekday(): Mon=0 … Thu=3 … Sun=6
    return last_day - timedelta(days=(last_day.weekday() - 3) % 7)


def _expiry_dates() -> tuple:
    """
    Return (near, mid, far) expiry date strings.
    Each is the last Thursday of successive calendar months,
    starting from the first month whose last Thursday >= today.
    """
    today = (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()
    expiries: list = []
    m, y = today.month, today.year
    for _ in range(6):
        exp = _last_thursday_of_month(y, m)
        if exp >= today:
            expiries.append(exp.strftime("%d %b %Y"))
        if len(expiries) == 3:
            break
        m += 1
        if m > 12:
            m, y = 1, y + 1
    while len(expiries) < 3:
        expiries.append("—")
    return tuple(expiries)


def _download_raw(url: str, label: str = "") -> bytes | None:
    """Download URL with retries. Returns bytes or None on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                return resp.content
            log.warning("%s HTTP %s (attempt %d/%d)", label, resp.status_code, attempt, MAX_RETRIES)
        except requests.RequestException as exc:
            log.warning("%s network error attempt %d/%d: %s", label, attempt, MAX_RETRIES, exc)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
    log.error("%s failed after %d attempts — %s", label, MAX_RETRIES, url)
    return None


def _atm_premium(ltp: float, days_to_expiry: int = 25, iv: float = 0.28) -> int:
    """
    Approximate ATM option premium using Black-Scholes:
      premium ≈ 0.4 × IV × √(T/252) × S
    Rounded to nearest ₹5. Minimum ₹5.
    """
    prem = 0.4 * iv * math.sqrt(max(days_to_expiry, 1) / 252) * ltp
    return max(5, int(round(prem / 5) * 5))


def _trend(closes: list) -> str:
    """
    Momentum trend from a close-price series (oldest → newest).
    Compare 5-day avg vs 20-day avg:
      diff > +0.8% → Bullish
      diff < −0.8% → Bearish
      else          → Sideways
    """
    if len(closes) < 3:
        return "Sideways"
    n = len(closes)
    avg5  = sum(closes[-min(5,  n):]) / min(5,  n)
    avg20 = sum(closes[-min(20, n):]) / min(20, n)
    if avg20 == 0:
        return "Sideways"
    diff = (avg5 - avg20) / avg20 * 100
    if diff >  0.8:
        return "Bullish"
    if diff < -0.8:
        return "Bearish"
    return "Sideways"


def _trend_label(t: str) -> str:
    icons = {"Bullish": "🟢", "Bearish": "🔴", "Sideways": "🟡"}
    return f"{icons.get(t, '⚪')} {t}"


def _round_strike(ltp: float) -> int:
    """Round LTP to nearest ATM strike using exchange-standard intervals."""
    if   ltp > 20000: step = 100
    elif ltp >  5000: step = 50
    elif ltp >  1000: step = 20
    elif ltp >   200: step = 10
    else:             step = 5
    return int(round(ltp / step) * step)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — STRATEGY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_engine(
    ltp: float,
    atm_prem: int,
    intra_trend: str,
    swing_trend: str,
    lot: int,
    timeframe: str = "intraday",
) -> dict:
    """
    Selects the best options strategy for the given trend and timeframe.

    Intraday Bullish  → Long Call
    Intraday Bearish  → Long Put
    Intraday Sideways → Short Strangle
    Swing    Bullish  → Bull Call Spread
    Swing    Bearish  → Bear Put Spread
    Swing    Sideways → Iron Condor

    Returns a dict with keys:
      Strategy, CE Entry, PE Entry, CE Target, PE Target,
      Stop Loss, Max Profit/Lot, Max Loss/Lot, Risk:Reward, Rationale
    """
    trend = intra_trend if timeframe == "intraday" else swing_trend
    atm   = _round_strike(ltp)

    if   ltp > 20000: step = 100
    elif ltp >  5000: step = 50
    elif ltp >  1000: step = 20
    elif ltp >   200: step = 10
    else:             step = 5

    otm1 = atm + step
    otm2 = atm + 2 * step
    itm1 = atm - step
    itm2 = atm - 2 * step

    p_atm  = atm_prem
    p_otm1 = max(5, int(p_atm * 0.55 / 5) * 5)
    p_otm2 = max(5, int(p_atm * 0.30 / 5) * 5)

    # ── BULLISH ──────────────────────────────────────────────────────────────
    if trend == "Bullish":
        if timeframe == "intraday":
            sl = max(5, int(p_atm * 0.5))
            return {
                "Strategy":       "Long Call",
                "CE Entry":       f"Buy {atm} CE @ ₹{p_atm}",
                "PE Entry":       "—",
                "CE Target":      f"₹{p_atm * 2}  (2× premium)",
                "PE Target":      "—",
                "Stop Loss":      f"₹{sl}  (50% of premium paid)",
                "Max Profit/Lot": f"₹{p_atm * 2 * lot:,}  (at 2× target)",
                "Max Loss/Lot":   f"₹{p_atm * lot:,}  (premium paid — max risk)",
                "Risk:Reward":    "1:2",
                "Rationale":      (
                    f"Intraday bullish momentum. Buy ATM {atm} CE @ ₹{p_atm}. "
                    f"Target ₹{p_atm * 2}, hard SL at ₹{sl}. "
                    f"Risk limited to premium paid."
                ),
            }
        else:  # swing
            net_debit = p_atm - p_otm1
            max_gain  = (otm1 - atm) - net_debit
            rr = round(max_gain / max(net_debit, 1), 1)
            sl_amt = int(net_debit * 0.4 * lot)
            return {
                "Strategy":       "Bull Call Spread",
                "CE Entry":       f"Buy {atm} CE @ ₹{p_atm}  |  Sell {otm1} CE @ ₹{p_otm1}",
                "PE Entry":       "—",
                "CE Target":      f"Close ≥ ₹{otm1} at expiry  (full profit)",
                "PE Target":      "—",
                "Stop Loss":      f"Exit if MTM loss ≈ ₹{sl_amt:,}  (40% of debit)",
                "Max Profit/Lot": f"₹{max_gain * lot:,}  (at expiry ≥ {otm1})",
                "Max Loss/Lot":   f"₹{net_debit * lot:,}  (net debit paid)",
                "Risk:Reward":    f"1:{rr}",
                "Rationale":      (
                    f"Swing bullish. Buy {atm} CE ₹{p_atm}, sell {otm1} CE ₹{p_otm1}. "
                    f"Net debit ₹{net_debit}/unit. Max profit if price ≥ {otm1} at expiry."
                ),
            }

    # ── BEARISH ──────────────────────────────────────────────────────────────
    elif trend == "Bearish":
        if timeframe == "intraday":
            sl = max(5, int(p_atm * 0.5))
            return {
                "Strategy":       "Long Put",
                "CE Entry":       "—",
                "PE Entry":       f"Buy {atm} PE @ ₹{p_atm}",
                "CE Target":      "—",
                "PE Target":      f"₹{p_atm * 2}  (2× premium)",
                "Stop Loss":      f"₹{sl}  (50% of premium paid)",
                "Max Profit/Lot": f"₹{p_atm * 2 * lot:,}  (at 2× target)",
                "Max Loss/Lot":   f"₹{p_atm * lot:,}  (premium paid — max risk)",
                "Risk:Reward":    "1:2",
                "Rationale":      (
                    f"Intraday bearish momentum. Buy ATM {atm} PE @ ₹{p_atm}. "
                    f"Target ₹{p_atm * 2}, hard SL at ₹{sl}. "
                    f"Risk limited to premium paid."
                ),
            }
        else:  # swing
            net_debit = p_atm - p_otm1
            max_gain  = (atm - itm1) - net_debit
            rr = round(max_gain / max(net_debit, 1), 1)
            sl_amt = int(net_debit * 0.4 * lot)
            return {
                "Strategy":       "Bear Put Spread",
                "CE Entry":       "—",
                "PE Entry":       f"Buy {atm} PE @ ₹{p_atm}  |  Sell {itm1} PE @ ₹{p_otm1}",
                "CE Target":      "—",
                "PE Target":      f"Close ≤ ₹{itm1} at expiry  (full profit)",
                "Stop Loss":      f"Exit if MTM loss ≈ ₹{sl_amt:,}  (40% of debit)",
                "Max Profit/Lot": f"₹{max_gain * lot:,}  (at expiry ≤ {itm1})",
                "Max Loss/Lot":   f"₹{net_debit * lot:,}  (net debit paid)",
                "Risk:Reward":    f"1:{rr}",
                "Rationale":      (
                    f"Swing bearish. Buy {atm} PE ₹{p_atm}, sell {itm1} PE ₹{p_otm1}. "
                    f"Net debit ₹{net_debit}/unit. Max profit if price ≤ {itm1} at expiry."
                ),
            }

    # ── SIDEWAYS ─────────────────────────────────────────────────────────────
    else:
        if timeframe == "intraday":
            credit = p_otm1 * 2
            be_hi  = otm1 + credit
            be_lo  = itm1 - credit
            return {
                "Strategy":       "Short Strangle",
                "CE Entry":       f"Sell {otm1} CE @ ₹{p_otm1}",
                "PE Entry":       f"Sell {itm1} PE @ ₹{p_otm1}",
                "CE Target":      f"CE expires worthless  (stay below ₹{otm1})",
                "PE Target":      f"PE expires worthless  (stay above ₹{itm1})",
                "Stop Loss":      f"Exit both legs if combined loss > ₹{credit * lot:,}  (1× credit)",
                "Max Profit/Lot": f"₹{credit * lot:,}  (full credit if price stays {itm1}–{otm1})",
                "Max Loss/Lot":   "Unlimited beyond BE — mandatory hard SL",
                "Risk:Reward":    "Credit-based; use strict SL",
                "Rationale":      (
                    f"Sideways intraday. Sell {otm1} CE ₹{p_otm1} + {itm1} PE ₹{p_otm1}. "
                    f"Total credit ₹{credit}. Breakevens: ₹{be_lo}–₹{be_hi}. Requires margin."
                ),
            }
        else:  # swing — Iron Condor
            net_credit = max(5, (p_otm1 - p_otm2) * 2)
            wing       = otm1 - atm
            max_loss   = max(1, wing - net_credit)
            rr = round(max_loss / max(net_credit, 1), 1)
            be_hi = otm1 + net_credit
            be_lo = itm1 - net_credit
            return {
                "Strategy":       "Iron Condor",
                "CE Entry":       f"Sell {otm1} CE @ ₹{p_otm1}  |  Buy {otm2} CE @ ₹{p_otm2}",
                "PE Entry":       f"Sell {itm1} PE @ ₹{p_otm1}  |  Buy {itm2} PE @ ₹{p_otm2}",
                "CE Target":      f"Price stays below ₹{otm1}  (upper BE: ₹{be_hi})",
                "PE Target":      f"Price stays above ₹{itm1}  (lower BE: ₹{be_lo})",
                "Stop Loss":      f"Exit breached side if loss > 2× net credit (₹{net_credit * 2 * lot:,})",
                "Max Profit/Lot": f"₹{net_credit * lot:,}  (net credit × lot)",
                "Max Loss/Lot":   f"₹{max_loss * lot:,}  (wing width − net credit)",
                "Risk:Reward":    f"1:{rr}",
                "Rationale":      (
                    f"Sideways swing — Iron Condor. Net credit ₹{net_credit}/unit. "
                    f"Profit zone ₹{be_lo}–₹{be_hi}. Defined max loss ₹{max_loss}/unit."
                ),
            }


def _sv(s: dict, k: str) -> str:
    """Safe value getter for strategy dict."""
    return s.get(k, "—")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

class BhavcopFetcher:
    """Downloads and parses the NSE equity bhavcopy ZIP for a given date."""

    def fetch(self, dt: datetime) -> tuple | None:
        """
        Returns (data_vol, data_to, cmp_map) or None.
          data_vol — list of [symbol, volume, close] sorted by volume desc
          data_to  — list of [symbol, turnover, close] sorted by turnover desc
          cmp_map  — dict {symbol: close_price}
        """
        url = BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("Equity bhavcopy → %s", dt.strftime("%d %b %Y"))
        raw = _download_raw(url, "BhavCopy")
        if raw is None:
            return None
        df = self._unzip(raw)
        if df is None or df.empty:
            return None
        df = self._filter_eq(df)
        if df.empty:
            return None

        sym_c = _pick_col(df, COL_MAP["symbol"])
        cls_c = _pick_col(df, COL_MAP["close"])
        vol_c = _pick_col(df, COL_MAP["volume"])
        tov_c = _pick_col(df, COL_MAP["turnover"])

        for c in (cls_c, vol_c, tov_c):
            df[c] = pd.to_numeric(df[c], errors="coerce")

        data_vol = (
            df.sort_values(vol_c, ascending=False)
            .head(TOP_N)[[sym_c, vol_c, cls_c]]
            .fillna(0).values.tolist()
        )
        data_to = (
            df.sort_values(tov_c, ascending=False)
            .head(TOP_N)[[sym_c, tov_c, cls_c]]
            .fillna(0).values.tolist()
        )
        cmp_map = dict(zip(df[sym_c].astype(str), df[cls_c].fillna(0)))
        log.info("  → %d EQ rows parsed", len(df))
        return data_vol, data_to, cmp_map

    def _unzip(self, raw: bytes) -> pd.DataFrame | None:
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f, low_memory=False)
        except Exception as exc:
            log.error("BhavCopy ZIP error: %s", exc)
            return None

    def _filter_eq(self, df: pd.DataFrame) -> pd.DataFrame:
        ser_c = _pick_col(df, COL_MAP["series"])
        sym_c = _pick_col(df, COL_MAP["symbol"])
        df = df[df[ser_c].astype(str).str.strip() == "EQ"].copy()
        mask = df[sym_c].astype(str).str.contains(
            EXCLUDE_PATTERN, case=False, na=False
        )
        return df[~mask].reset_index(drop=True)


class IndexPriceFetcher:
    """
    Fetches index closing prices from NSE's ind_close_all_{date}.csv.
    Indices are NOT in the equity bhavcopy — they need this separate source.
    Falls back to INDEX_FALLBACK_CMP if the NSE file is unavailable.
    """

    def fetch(self, ist_today: datetime) -> dict:
        """Return {symbol: close} for the 5 tracked indices."""
        for days_back in range(LOOKBACK_DAYS + 1):
            cand = ist_today - timedelta(days=days_back)
            if cand.weekday() >= 5:   # skip Sat/Sun
                continue
            data = self._fetch_one(cand)
            if data:
                log.info(
                    "Index prices from %s: %s",
                    cand.strftime("%d %b %Y"),
                    {k: f"₹{v:,.0f}" for k, v in data.items()},
                )
                return data

        log.warning("Index CSV unavailable — using fallback prices.")
        return dict(INDEX_FALLBACK_CMP)

    def fetch_history(self, ist_today: datetime, days: int = HISTORY_DAYS) -> dict:
        """Return {symbol: [close_oldest, …, close_newest]} for trend calc."""
        history: dict = {}
        found = 0
        for days_back in range(1, days + 15):
            cand = ist_today - timedelta(days=days_back)
            if cand.weekday() >= 5:
                continue
            day_data = self._fetch_one(cand)
            if not day_data:
                continue
            for sym, price in day_data.items():
                history.setdefault(sym, []).append(price)
            found += 1
            if found >= days:
                break
        return {s: list(reversed(v)) for s, v in history.items()}

    def _fetch_one(self, dt: datetime) -> dict:
        url = INDEX_CSV_URL.format(date=dt.strftime("%d%m%Y"))
        raw = _download_raw(url, "IndexCSV")
        if raw is None:
            return {}
        try:
            df = pd.read_csv(io.StringIO(raw.decode("utf-8", errors="replace")))
            df.columns = [c.strip() for c in df.columns]
            # NSE column names: "Index Name", "Closing Index Value"
            name_col  = next(
                (c for c in df.columns if "index" in c.lower() and "name" in c.lower()),
                None,
            )
            close_col = next(
                (c for c in df.columns if "clos" in c.lower()),
                None,
            )
            if not name_col or not close_col:
                log.warning("Index CSV unexpected columns: %s", list(df.columns))
                return {}
            df[close_col] = pd.to_numeric(
                df[close_col].astype(str).str.replace(",", ""), errors="coerce"
            )
            result: dict = {}
            for _, row in df.iterrows():
                raw_name = str(row[name_col]).strip()
                sym = INDEX_NAME_MAP.get(raw_name) or INDEX_NAME_MAP.get(raw_name.title())
                if sym and pd.notna(row[close_col]) and row[close_col] > 0:
                    result[sym] = float(row[close_col])
            # Always fill any missing index with fallback
            for sym, fb in INDEX_FALLBACK_CMP.items():
                if sym not in result:
                    log.debug("Using fallback for %s: ₹%s", sym, fb)
                    result[sym] = fb
            return result
        except Exception as exc:
            log.warning("Index CSV parse error: %s", exc)
            return {}


class LotSizeFetcher:
    """Fetches current F&O lot sizes from NSE's fo_mktlots.csv."""

    def fetch(self) -> dict:
        """Return {symbol: lot_size_int}. Falls back to FALLBACK_LOTS on error."""
        raw = _download_raw(MKTLOTS_URL, "LotSizes")
        if raw is None:
            return {}
        try:
            # The CSV has a title row then a header row — use header=1
            df = pd.read_csv(
                io.StringIO(raw.decode("utf-8", errors="replace")),
                header=1, dtype=str,
            )
            df.columns = [c.strip() for c in df.columns]
            sym_c = df.columns[0]
            lot_c = df.columns[1]   # current month lot size
            df[sym_c] = df[sym_c].str.strip()
            df[lot_c] = pd.to_numeric(
                df[lot_c].str.replace(",", ""), errors="coerce"
            )
            df = df.dropna(subset=[lot_c])
            result = dict(zip(df[sym_c], df[lot_c].astype(int)))
            log.info("Lot sizes: %d symbols loaded from NSE", len(result))
            return result
        except Exception as exc:
            log.warning("LotSizeFetcher error: %s", exc)
            return {}


class EquityHistoryFetcher:
    """Builds a close-price history for equity symbols via successive bhavcopies."""

    def fetch(self, ist_today: datetime, days: int = HISTORY_DAYS) -> dict:
        """Return {symbol: [close_oldest, …, close_newest]}."""
        history: dict = {}
        fetcher = BhavcopFetcher()
        found = 0
        for days_back in range(1, days + 15):
            cand = ist_today - timedelta(days=days_back)
            if cand.weekday() >= 5:
                continue
            res = fetcher.fetch(cand)
            if not res:
                continue
            _, _, cmap = res
            for sym, cls in cmap.items():
                if cls and cls > 0:
                    history.setdefault(sym, []).append(cls)
            found += 1
            if found >= days:
                break
        return {s: list(reversed(v)) for s, v in history.items()}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — SHEET HEADERS  (exact column specification)
# ══════════════════════════════════════════════════════════════════════════════

# OPTIONS F&O — 40 columns
OPT_HEADERS = [
    "Sr.",                                        # 1
    "Company / Index Name",                       # 2
    "NSE Symbol",                                 # 3
    "Sector / Type",                              # 4
    "Lot Size\n(Units)",                          # 5
    "CMP ₹\n(Approx.)",                          # 6
    "Contract\nValue ₹",                          # 7
    "Near-Month\nExpiry",                         # 8
    "Mid-Month\nExpiry",                          # 9
    "Far-Month\nExpiry",                          # 10
    "Approx.\nATM Call ₹\n(Near Expiry)",        # 11
    "Approx.\nATM Put ₹\n(Near Expiry)",         # 12
    "Call Premium\nPaid (1 Lot) ₹",              # 13
    "Put Premium\nPaid (1 Lot) ₹",               # 14
    "Option Seller\nMargin ₹\n(~20% Contract)",  # 15
    "Intraday\nTrend",                            # 16
    "Swing\nTrend",                               # 17
    "Options Strategy\n(Intraday)",               # 18
    "Options Strategy\n(Swing)",                  # 19
    # ── Intraday Signal block ──────────────────────────────────────────
    "Intraday Signal:\nBest Strategy",            # 20
    "Intraday:\nCE Entry",                        # 21
    "Intraday:\nPE Entry",                        # 22
    "Intraday:\nCE Target",                       # 23
    "Intraday:\nPE Target",                       # 24
    "Intraday:\nStop Loss",                       # 25
    "Intraday:\nMax Profit/Lot ₹",               # 26
    "Intraday:\nMax Loss/Lot ₹",                 # 27
    "Intraday:\nRisk:Reward",                     # 28
    "Intraday:\nRationale",                       # 29
    # ── Swing Signal block ────────────────────────────────────────────
    "Swing Signal:\nBest Strategy",               # 30
    "Swing:\nCE Entry",                           # 31
    "Swing:\nPE Entry",                           # 32
    "Swing:\nCE Target",                          # 33
    "Swing:\nPE Target",                          # 34
    "Swing:\nStop Loss",                          # 35
    "Swing:\nMax Profit/Lot ₹",                  # 36
    "Swing:\nMax Loss/Lot ₹",                    # 37
    "Swing:\nRisk:Reward",                        # 38
    "Swing:\nRationale",                          # 39
    "Notes",                                      # 40
]   # total = 40

# FUTURES F&O — 36 columns
FUT_HEADERS = [
    "Sr.",                                        # 1
    "Company / Index Name",                       # 2
    "NSE Symbol",                                 # 3
    "Sector / Type",                              # 4
    "Lot Size\n(Units)",                          # 5
    "CMP ₹\n(Approx.)",                          # 6
    "Contract\nValue ₹",                          # 7
    "Futures\nMargin %",                          # 8
    "Futures\nMargin Req. ₹",                    # 9
    "Near-Month\nExpiry",                         # 10
    "Mid-Month\nExpiry",                          # 11
    "Far-Month\nExpiry",                          # 12
    "Intraday\nTrend",                            # 13
    "Swing\nTrend",                               # 14
    # ── Intraday Signal block ──────────────────────────────────────────
    "Intraday Signal:\nBest Strategy",            # 15
    "Intraday:\nCE Entry",                        # 16
    "Intraday:\nPE Entry",                        # 17
    "Intraday:\nCE Target",                       # 18
    "Intraday:\nPE Target",                       # 19
    "Intraday:\nStop Loss",                       # 20
    "Intraday:\nMax Profit/Lot ₹",               # 21
    "Intraday:\nMax Loss/Lot ₹",                 # 22
    "Intraday:\nRisk:Reward",                     # 23
    "Intraday:\nRationale",                       # 24
    # ── Swing Signal block ────────────────────────────────────────────
    "Swing Signal:\nBest Strategy",               # 25
    "Swing:\nCE Entry",                           # 26
    "Swing:\nPE Entry",                           # 27
    "Swing:\nCE Target",                          # 28
    "Swing:\nPE Target",                          # 29
    "Swing:\nStop Loss",                          # 30
    "Swing:\nMax Profit/Lot ₹",                  # 31
    "Swing:\nMax Loss/Lot ₹",                    # 32
    "Swing:\nRisk:Reward",                        # 33
    "Swing:\nRationale",                          # 34
    "Notes",                                      # 35
    "Last Updated",                               # 36
]   # total = 36


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — ROW BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_opt_row(
    sr: int,
    name: str,
    sym: str,
    sector: str,
    lot: int,
    ltp: float,
    expiries: tuple,
    history: dict,
    note: str = "",
) -> list:
    """Build one Options F&O row — must return exactly 40 items."""
    exp_near, exp_mid, exp_far = expiries
    hist    = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    cval    = round(lot * ltp)
    atm_c   = _atm_premium(ltp)
    atm_p   = _atm_premium(ltp)
    si = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "swing")
    return [
        sr,                         # 1  Sr.
        name,                       # 2  Company / Index Name
        sym,                        # 3  NSE Symbol
        sector,                     # 4  Sector / Type
        lot,                        # 5  Lot Size
        round(ltp, 2),              # 6  CMP
        cval,                       # 7  Contract Value
        exp_near,                   # 8  Near Expiry
        exp_mid,                    # 9  Mid Expiry
        exp_far,                    # 10 Far Expiry
        atm_c,                      # 11 ATM Call premium
        atm_p,                      # 12 ATM Put premium
        atm_c * lot,                # 13 Call cost / lot
        atm_p * lot,                # 14 Put cost / lot
        round(cval * 0.20),         # 15 Seller margin
        _trend_label(intra_t),      # 16 Intraday Trend
        _trend_label(swing_t),      # 17 Swing Trend
        si["Strategy"],             # 18 Options Strategy (Intraday)
        ss["Strategy"],             # 19 Options Strategy (Swing)
        # Intraday signal block (cols 20–29)
        si["Strategy"],             # 20
        _sv(si, "CE Entry"),        # 21
        _sv(si, "PE Entry"),        # 22
        _sv(si, "CE Target"),       # 23
        _sv(si, "PE Target"),       # 24
        _sv(si, "Stop Loss"),       # 25
        _sv(si, "Max Profit/Lot"),  # 26
        _sv(si, "Max Loss/Lot"),    # 27
        _sv(si, "Risk:Reward"),     # 28
        _sv(si, "Rationale"),       # 29
        # Swing signal block (cols 30–39)
        ss["Strategy"],             # 30
        _sv(ss, "CE Entry"),        # 31
        _sv(ss, "PE Entry"),        # 32
        _sv(ss, "CE Target"),       # 33
        _sv(ss, "PE Target"),       # 34
        _sv(ss, "Stop Loss"),       # 35
        _sv(ss, "Max Profit/Lot"),  # 36
        _sv(ss, "Max Loss/Lot"),    # 37
        _sv(ss, "Risk:Reward"),     # 38
        _sv(ss, "Rationale"),       # 39
        note,                       # 40
    ]


def _build_fut_row(
    sr: int,
    name: str,
    sym: str,
    sector: str,
    lot: int,
    ltp: float,
    margin_pct: int,
    expiries: tuple,
    history: dict,
    note: str = "",
) -> list:
    """Build one Futures F&O row — must return exactly 36 items."""
    exp_near, exp_mid, exp_far = expiries
    hist    = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    cval    = round(lot * ltp)
    atm_c   = _atm_premium(ltp)
    si = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "swing")
    return [
        sr,                         # 1  Sr.
        name,                       # 2  Company / Index Name
        sym,                        # 3  NSE Symbol
        sector,                     # 4  Sector / Type
        lot,                        # 5  Lot Size
        round(ltp, 2),              # 6  CMP
        cval,                       # 7  Contract Value
        f"{margin_pct}%",           # 8  Margin %
        round(cval * margin_pct / 100),  # 9 Margin Req ₹
        exp_near,                   # 10 Near Expiry
        exp_mid,                    # 11 Mid Expiry
        exp_far,                    # 12 Far Expiry
        _trend_label(intra_t),      # 13 Intraday Trend
        _trend_label(swing_t),      # 14 Swing Trend
        # Intraday signal block (cols 15–24)
        si["Strategy"],             # 15
        _sv(si, "CE Entry"),        # 16
        _sv(si, "PE Entry"),        # 17
        _sv(si, "CE Target"),       # 18
        _sv(si, "PE Target"),       # 19
        _sv(si, "Stop Loss"),       # 20
        _sv(si, "Max Profit/Lot"),  # 21
        _sv(si, "Max Loss/Lot"),    # 22
        _sv(si, "Risk:Reward"),     # 23
        _sv(si, "Rationale"),       # 24
        # Swing signal block (cols 25–34)
        ss["Strategy"],             # 25
        _sv(ss, "CE Entry"),        # 26
        _sv(ss, "PE Entry"),        # 27
        _sv(ss, "CE Target"),       # 28
        _sv(ss, "PE Target"),       # 29
        _sv(ss, "Stop Loss"),       # 30
        _sv(ss, "Max Profit/Lot"),  # 31
        _sv(ss, "Max Loss/Lot"),    # 32
        _sv(ss, "Risk:Reward"),     # 33
        _sv(ss, "Rationale"),       # 34
        note,                       # 35
        _ist_now(),                 # 36
    ]


def build_all_rows(
    lot_sizes: dict,
    equity_cmp: dict,
    index_cmp: dict,
    equity_hist: dict,
    index_hist: dict,
    expiries: tuple,
) -> tuple:
    """
    Build all Futures and Options rows.
    Order: 5 indices first (fixed order) → stocks alphabetically.
    Returns (futures_rows, options_rows).
    """
    fut_rows: list = []
    opt_rows: list = []
    sr = 1

    # ── INDICES (always first 5 rows) ────────────────────────────────────────
    log.info("Building index rows…")
    for sym, name, sector, margin_pct in INDEX_META:
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = index_cmp.get(sym, 0)

        if lot == 0:
            log.warning("  No lot size for %s — skipping", sym)
            continue
        if ltp == 0:
            ltp = INDEX_FALLBACK_CMP.get(sym, 0)
            log.warning("  No live CMP for %s — using fallback ₹%s", sym, ltp)
        if ltp == 0:
            continue

        # Use index history (preferred); fall back to equity history
        hist = index_hist.get(sym) or equity_hist.get(sym, [])
        hist_dict = {sym: hist}

        fut_rows.append(
            _build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries, hist_dict, "Index Future")
        )
        opt_rows.append(
            _build_opt_row(sr, name, sym, sector, lot, ltp, expiries, hist_dict, "Index Option — Weekly & Monthly expiries available")
        )

        intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
        log.info(
            "  %-12s  CMP=₹%-8s  Lot=%-4d  Trend=%s",
            sym, f"{ltp:,.0f}", lot, intra_t,
        )
        sr += 1

    # ── STOCKS (alphabetical) ─────────────────────────────────────────────────
    log.info("Building stock rows…")
    all_stock_syms = sorted(
        (set(lot_sizes.keys()) | set(FALLBACK_LOTS.keys())) - INDEX_SYMS
    )
    skipped = 0
    for sym in all_stock_syms:
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = equity_cmp.get(sym, 0)
        if lot == 0 or ltp == 0:
            skipped += 1
            continue
        sector = SECTOR_MAP.get(sym, "Equity")
        fut_rows.append(
            _build_fut_row(sr, sym, sym, sector, lot, ltp, 20, expiries, equity_hist)
        )
        opt_rows.append(
            _build_opt_row(sr, sym, sym, sector, lot, ltp, expiries, equity_hist)
        )
        sr += 1

    log.info(
        "Rows built — Futures: %d  Options: %d  (stocks skipped: %d)",
        len(fut_rows), len(opt_rows), skipped,
    )
    return fut_rows, opt_rows


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — GOOGLE SHEETS WRITER
# ══════════════════════════════════════════════════════════════════════════════

class SheetsWriter:
    def __init__(self, creds_json: str) -> None:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(creds_json), GSHEETS_SCOPES
        )
        self._client = gspread.authorize(creds)

    def _get_or_create(self, ss, title: str, cols: int = 50) -> gspread.Worksheet:
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating new sheet tab: '%s'", title)
            return ss.add_worksheet(title=title, rows=600, cols=cols)

    def open_all(self):
        ss = self._client.open_by_key(SPREADSHEET_ID)
        return (
            self._get_or_create(ss, SHEET_VOLUME,   cols=15),
            self._get_or_create(ss, SHEET_TURNOVER, cols=15),
            self._get_or_create(ss, SHEET_FUTURES,  cols=40),
            self._get_or_create(ss, SHEET_OPTIONS,  cols=45),
        )

    def write_vol_turnover(
        self,
        ws_vol,
        ws_to,
        data_vol: list,
        data_to: list,
        fetched_date: str,
    ) -> None:
        status = f"Data: {fetched_date}  |  Updated: {_ist_now()}"
        for ws, data in ((ws_vol, data_vol), (ws_to, data_to)):
            n = len(data)
            ws.batch_update(
                [
                    {"range": f"A2:C{n + 1}", "values": data},
                    {"range": STATUS_CELL,     "values": [[status]]},
                ],
                value_input_option="USER_ENTERED",
            )
            log.info("'%s' → %d rows written", ws.title, n)

    def write_fo_sheet(
        self, ws, headers: list, rows: list, title: str
    ) -> None:
        """Clear the sheet and rewrite header + all data rows in chunks."""
        all_data = [headers] + rows
        n_cols   = len(headers)
        ws.clear()
        time.sleep(1)   # brief pause after clear

        for start in range(0, len(all_data), WRITE_CHUNK):
            end   = min(start + WRITE_CHUNK, len(all_data))
            rng   = f"A{start + 1}:{_col_letter(n_cols)}{end}"
            ws.update(
                range_name=rng,
                values=all_data[start:end],
                value_input_option="USER_ENTERED",
            )
            log.info("  '%s' — wrote rows %d–%d", title, start + 1, end)
            if end < len(all_data):
                time.sleep(1.5)   # stay well under Sheets API rate limit

        log.info("'%s' complete — %d data rows × %d cols", title, len(rows), n_cols)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # 1. Credentials
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError(
            "GCP_CREDENTIALS environment variable is not set. "
            "Add it as a GitHub Actions secret."
        )

    ist_today = datetime.utcnow() + timedelta(hours=5, minutes=30)
    log.info("═" * 60)
    log.info("NSE Auto-Sheet run  —  %s", ist_today.strftime("%d-%b-%Y %H:%M IST"))
    log.info("═" * 60)

    # 2. Connect to Google Sheets
    log.info("Connecting to Google Sheets…")
    writer = SheetsWriter(creds_json)
    ws_vol, ws_to, ws_fut, ws_opt = writer.open_all()

    # 3. Fetch equity bhavcopy (CMP for stocks + vol/turnover top lists)
    log.info("── Equity bhavcopy ──────────────────────────────────────")
    eq_fetcher: BhavcopFetcher = BhavcopFetcher()
    result, fetched_date = None, ""
    for days_back in range(LOOKBACK_DAYS + 1):
        cand = ist_today - timedelta(days=days_back)
        if cand.weekday() >= 5:
            continue
        result = eq_fetcher.fetch(cand)
        if result:
            fetched_date = cand.strftime("%d-%b-%Y")
            break
    if result is None:
        raise RuntimeError(
            f"Could not fetch equity bhavcopy for last {LOOKBACK_DAYS} trading days."
        )
    data_vol, data_to, equity_cmp = result

    # 4. Write Top 250 Volume + Turnover sheets (existing sheets — unchanged)
    log.info("── Writing Top 250 sheets ───────────────────────────────")
    writer.write_vol_turnover(ws_vol, ws_to, data_vol, data_to, fetched_date)

    # 5. Fetch index prices (separate NSE index CSV — not in equity bhavcopy)
    log.info("── Index prices ─────────────────────────────────────────")
    idx_fetcher = IndexPriceFetcher()
    index_cmp   = idx_fetcher.fetch(ist_today)

    # 6. Fetch lot sizes from NSE official CSV
    log.info("── Lot sizes ────────────────────────────────────────────")
    lot_sizes = LotSizeFetcher().fetch()
    if not lot_sizes:
        log.warning("Using fallback lot sizes (NSE CSV unavailable)")
        lot_sizes = FALLBACK_LOTS

    # 7. Fetch 15-day close history for momentum trend calculation
    log.info("── Price history (equity, %d days) ──────────────────────", HISTORY_DAYS)
    equity_hist = EquityHistoryFetcher().fetch(ist_today, days=HISTORY_DAYS)

    log.info("── Price history (indices, %d days) ─────────────────────", HISTORY_DAYS)
    index_hist  = idx_fetcher.fetch_history(ist_today, days=HISTORY_DAYS)

    # 8. Compute expiry dates
    expiries = _expiry_dates()
    log.info("── Expiries  Near: %s  Mid: %s  Far: %s ─────────────────", *expiries)

    # 9. Build all F&O rows
    log.info("── Building F&O rows ────────────────────────────────────")
    fut_rows, opt_rows = build_all_rows(
        lot_sizes, equity_cmp, index_cmp, equity_hist, index_hist, expiries
    )

    # Sanity check — catch header/row count mismatches before writing
    if fut_rows and len(fut_rows[0]) != len(FUT_HEADERS):
        raise ValueError(
            f"Futures row has {len(fut_rows[0])} items but "
            f"FUT_HEADERS has {len(FUT_HEADERS)} — mismatch!"
        )
    if opt_rows and len(opt_rows[0]) != len(OPT_HEADERS):
        raise ValueError(
            f"Options row has {len(opt_rows[0])} items but "
            f"OPT_HEADERS has {len(OPT_HEADERS)} — mismatch!"
        )

    # 10. Write Futures F&O and Options F&O sheets
    log.info("── Writing Futures F&O sheet ────────────────────────────")
    writer.write_fo_sheet(ws_fut, FUT_HEADERS, fut_rows, SHEET_FUTURES)

    log.info("── Writing Options F&O sheet ────────────────────────────")
    writer.write_fo_sheet(ws_opt, OPT_HEADERS, opt_rows, SHEET_OPTIONS)

    log.info("═" * 60)
    log.info(
        "✅  SUCCESS — All 4 sheets updated  |  Equity data: %s  |  %s",
        fetched_date, _ist_now(),
    )
    log.info("═" * 60)


if __name__ == "__main__":
    main()
