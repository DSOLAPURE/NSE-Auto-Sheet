"""
NSE Bhavcopy → Google Sheets Updater  (v2 — Futures & Options edition)
=======================================================================
Sheets written:
  1. Top 250 Stocks    – top 250 by volume  (your existing sheet)
  2. Top 250 Turnover  – top 250 by turnover (your existing sheet)
  3. Futures F&O       – all F&O stocks: lot size, CMP, contract value,
                         margin, expiry, intraday & swing trend
  4. Options F&O       – same stocks: ATM call/put premium estimates,
                         buyer cost per lot, seller margin, strategy

Data sources
  • CMP + volume/turnover  → NSE UDiFF bhavcopy ZIP (same as before)
  • Lot sizes              → NSE fo_mktlots.csv (public, no auth needed)
  • Expiry dates           → computed: last Thursday of near/mid/far month
  • Momentum trend         → simple rule based on 5-day vs 20-day avg
                             close from last 20 trading days of bhavcopy
                             (Bullish / Bearish / Sideways)
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
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  ← change only here
# ─────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID  = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME    = "Top 250 Stocks"
SHEET_TURNOVER  = "Top 250 Turnover"
SHEET_FUTURES   = "Futures F&O"
SHEET_OPTIONS   = "Options F&O"
STATUS_CELL     = "K2"
TOP_N           = 250
LOOKBACK_DAYS   = 7
REQUEST_TIMEOUT = 20
MAX_RETRIES     = 3
RETRY_DELAY     = 4
EXCLUDE_PATTERN = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)
MKTLOTS_URL = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

COL_CANDIDATES = {
    "symbol":   ["TckrSymb",    "SYMBOL"],
    "close":    ["ClsPric",     "CLOSE"],
    "series":   ["SctySrs",     "SERIES"],
    "volume":   ["TtlTradgVol", "TOTTRDQTY", "TtlTrdQty", "TotTrdQty"],
    "turnover": ["TtlTrfVal",   "TOTTRDVAL", "TtlTrdVal", "TotTrdVal"],
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
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _pick_col(df: pd.DataFrame, candidates: list) -> str:
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} found in columns: {list(df.columns)}")


def _ist_now() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%b-%Y %H:%M IST")


def _last_thursday_of_month(year: int, month: int) -> date:
    """Return the last Thursday of the given month (Friday if Thu is holiday — simplified)."""
    # Start from the last day and walk back
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - 3) % 7  # 3 = Thursday
    return last_day - timedelta(days=offset)


def _expiry_dates() -> tuple:
    """Return (near, mid, far) expiry dates as formatted strings."""
    today = (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()
    months = []
    m, y = today.month, today.year
    for _ in range(4):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    # Pick 3 months whose last Thursday is >= today
    expiries = []
    for (yr, mo) in months:
        exp = _last_thursday_of_month(yr, mo)
        if exp >= today:
            expiries.append(exp.strftime("%d %b %Y"))
        if len(expiries) == 3:
            break
    while len(expiries) < 3:
        expiries.append("—")
    return tuple(expiries)


def _approx_atm_premium(ltp: float, days_to_expiry: int = 25, iv: float = 0.28) -> int:
    """Rough Black-Scholes ATM estimate: 0.4 × IV × sqrt(T/252) × S, rounded to ₹5."""
    prem = 0.4 * iv * math.sqrt(max(days_to_expiry, 1) / 252) * ltp
    return max(5, int(round(prem / 5) * 5))


def _trend(close_series: list) -> str:
    """
    Simple momentum: compare 5-day avg vs 20-day avg.
    Bullish if 5d > 20d by >0.5%, Bearish if <-0.5%, else Sideways.
    """
    if len(close_series) < 5:
        return "Sideways"
    avg5  = sum(close_series[-5:])  / 5
    avg20 = sum(close_series[-20:]) / min(20, len(close_series))
    diff  = (avg5 - avg20) / avg20 * 100
    if diff >  0.5:
        return "🟢 Bullish"
    if diff < -0.5:
        return "🔴 Bearish"
    return "🟡 Sideways"


def _options_strategy(trend_str: str) -> str:
    if "Bullish" in trend_str:
        return "Buy Call / Bull Call Spread"
    if "Bearish" in trend_str:
        return "Buy Put / Bear Put Spread"
    return "Sell Strangle / Iron Condor"


# ─────────────────────────────────────────────────────────────────────────────
# BHAVCOPY FETCHER  (unchanged logic from your original)
# ─────────────────────────────────────────────────────────────────────────────
class BhavcopFetcher:
    def fetch(self, dt: datetime) -> Optional[tuple]:
        url = BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("Trying %s → %s", dt.strftime("%d-%m-%Y"), url)
        raw = self._download(url)
        if raw is None:
            return None
        df = self._read_zip(raw)
        if df is None or df.empty:
            return None
        df = self._clean(df)
        if df.empty:
            return None

        sym_col      = _pick_col(df, COL_CANDIDATES["symbol"])
        close_col    = _pick_col(df, COL_CANDIDATES["close"])
        vol_col      = _pick_col(df, COL_CANDIDATES["volume"])
        turnover_col = _pick_col(df, COL_CANDIDATES["turnover"])

        df[close_col]    = pd.to_numeric(df[close_col],    errors="coerce")
        df[vol_col]      = pd.to_numeric(df[vol_col],      errors="coerce")
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors="coerce")

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
        # Return full close price map too (symbol → close)
        cmp_map = dict(zip(df[sym_col].astype(str), df[close_col].fillna(0)))
        log.info("Fetched %d rows from bhavcopy.", len(df))
        return data_vol, data_to, cmp_map

    def _download(self, url: str) -> Optional[bytes]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.content
                log.warning("HTTP %s (attempt %d)", r.status_code, attempt)
            except requests.RequestException as exc:
                log.warning("Request error (attempt %d): %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        return None

    def _read_zip(self, raw: bytes) -> Optional[pd.DataFrame]:
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f, low_memory=False)
        except Exception as exc:
            log.error("ZIP parse error: %s", exc)
            return None

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        series_col = _pick_col(df, COL_CANDIDATES["series"])
        sym_col    = _pick_col(df, COL_CANDIDATES["symbol"])
        df = df[df[series_col].astype(str).str.strip() == "EQ"].copy()
        mask = df[sym_col].astype(str).str.contains(EXCLUDE_PATTERN, case=False, na=False)
        return df[~mask].reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# LOT SIZE FETCHER  (new)
# ─────────────────────────────────────────────────────────────────────────────
class LotSizeFetcher:
    def fetch(self) -> dict:
        """
        Returns dict: symbol (str) → lot_size (int).
        NSE CSV has a header row then rows like:
          UNDERLYING,JAN2026,FEB2026,MAR2026,...
        We take the first numeric month column as current lot size.
        """
        try:
            r = requests.get(MKTLOTS_URL, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                log.warning("fo_mktlots.csv returned HTTP %s", r.status_code)
                return {}
            # The CSV has the first row as a header label, actual headers on row 2
            text = r.text
            df = pd.read_csv(io.StringIO(text), header=1, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            # First col is symbol, second is the current month lot size
            sym_col = df.columns[0]
            lot_col = df.columns[1]
            df[sym_col] = df[sym_col].str.strip()
            df[lot_col] = pd.to_numeric(df[lot_col].str.replace(",", ""), errors="coerce")
            df = df.dropna(subset=[lot_col])
            result = dict(zip(df[sym_col], df[lot_col].astype(int)))
            log.info("Loaded %d lot sizes from fo_mktlots.csv", len(result))
            return result
        except Exception as exc:
            log.warning("LotSizeFetcher error: %s", exc)
            return {}


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-DAY CLOSE HISTORY  (for trend computation)
# ─────────────────────────────────────────────────────────────────────────────
class HistoryFetcher:
    """
    Fetches up to HISTORY_DAYS bhavcopies to build a close-price time series
    per symbol, used for simple momentum trend calculation.
    """
    HISTORY_DAYS = 20  # calendar days to look back (gets ~15 trading days)

    def fetch_history(self, ist_today: datetime) -> dict:
        """Returns {symbol: [close1, close2, ..., closeN]} oldest→newest."""
        history: dict = {}
        fetcher = BhavcopFetcher()
        trading_days_found = 0
        for days_back in range(1, self.HISTORY_DAYS + 1):
            candidate = ist_today - timedelta(days=days_back)
            if candidate.weekday() >= 5:
                continue
            result = fetcher.fetch(candidate)
            if result is None:
                continue
            _, _, cmp_map = result
            for sym, close in cmp_map.items():
                history.setdefault(sym, []).append(close)
            trading_days_found += 1
            if trading_days_found >= 15:
                break
        # Reverse so oldest first
        return {sym: list(reversed(closes)) for sym, closes in history.items()}


# ─────────────────────────────────────────────────────────────────────────────
# F&O SHEET BUILDER  (new)
# ─────────────────────────────────────────────────────────────────────────────
# Fallback lot sizes in case the CSV fetch fails (updated Jan 2026)
FALLBACK_LOT_SIZES = {
    "NIFTY":75,"BANKNIFTY":30,"FINNIFTY":60,"MIDCPNIFTY":120,"NIFTYNXT50":25,
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
    "ULTRACEMCO":100,"WIPRO":1500,"AUBANK":500,"AUROPHARMA":500,
    "DMART":100,"BAJAJHLDNG":50,"BALKRISIND":200,"BANDHANBNK":1875,
    "BANKBARODA":4350,"BERGEPAINT":1100,"BHARATFORG":500,"BIOCON":2400,
    "BSE":250,"CANBK":5000,"CHOLAFIN":750,"CUMMINSIND":300,"DABUR":1250,
    "DEEPAKNTR":375,"DIXON":100,"DLF":1650,"ESCORTS":275,
    "FEDERALBNK":5000,"GAIL":6400,"GODREJCP":500,"GODREJPROP":325,
    "GUJGASLTD":1250,"HAVELLS":500,"HDFCAMC":300,"HAL":500,
    "HINDPETRO":1700,"IDFCFIRSTB":10000,"IEX":3750,"INDHOTEL":1500,
    "IOC":5750,"IRFC":7500,"IGL":1375,"INDIGO":300,"IRCTC":875,
    "IREDA":2000,"JINDALSTEL":875,"JUBLFOOD":1250,"KAJARIACER":500,
    "KEC":750,"LTF":5000,"LTTS":200,"LAURUSLABS":1000,"LICI":700,
    "LUPIN":425,"LODHA":1000,"M&MFIN":3000,"MANAPPURAM":3000,
    "MARICO":1200,"MFSL":700,"MPHASIS":400,"MRF":10,"NAUKRI":150,
    "NAVINFLUOR":100,"NMDC":5750,"OBEROIRLTY":300,"OIL":1750,
    "PAGEIND":15,"PERSISTENT":250,"PETRONET":3000,"PIIND":200,
    "PIDILITIND":250,"POLYCAB":250,"PREMIERENE":500,"PNB":8000,
    "RVNL":2500,"RECLTD":2000,"MOTHERSON":5750,"SBICARD":1000,
    "SIEMENS":275,"SRF":125,"SAIL":7000,"SUNTV":750,"SWIGGY":3000,
    "TATACOMM":500,"TATAELXSI":175,"TATAPOWER":3375,"TORNTPHARM":250,
    "TORNTPOWER":500,"TVSMOTOR":350,"UPL":2000,"VEDL":2750,
    "VOLTAS":500,"WAAREEENER":100,"YESBANK":40000,"ZEEL":3000,
    "ZOMATO":4750,
}

# Sector map for F&O stocks
SECTOR_MAP = {
    "ADANIENT":"Conglomerate","ADANIPORTS":"Infrastructure","APOLLOHOSP":"Healthcare",
    "ASIANPAINT":"FMCG","AXISBANK":"Banking","BAJAJ-AUTO":"Auto",
    "BAJFINANCE":"NBFC","BAJAJFINSV":"Financial Svc","BEL":"Defence",
    "BPCL":"Oil & Gas","BHARTIARTL":"Telecom","BRITANNIA":"FMCG",
    "CIPLA":"Pharma","COALINDIA":"Mining","DIVISLAB":"Pharma",
    "DRREDDY":"Pharma","EICHERMOT":"Auto","GRASIM":"Diversified",
    "HCLTECH":"IT","HDFCBANK":"Banking","HDFCLIFE":"Insurance",
    "HEROMOTOCO":"Auto","HINDALCO":"Metals","HINDUNILVR":"FMCG",
    "ICICIBANK":"Banking","INDUSINDBK":"Banking","INFY":"IT",
    "ITC":"FMCG","JSWSTEEL":"Steel","KOTAKBANK":"Banking",
    "LT":"Infrastructure","LTIM":"IT","M&M":"Auto","MARUTI":"Auto",
    "NESTLEIND":"FMCG","NTPC":"Power","ONGC":"Oil & Gas",
    "POWERGRID":"Power","RELIANCE":"Energy/Retail","SBILIFE":"Insurance",
    "SHRIRAMFIN":"NBFC","SBIN":"Banking","SUNPHARMA":"Pharma",
    "TCS":"IT","TATACONSUM":"FMCG","TATAMOTORS":"Auto","TATASTEEL":"Steel",
    "TECHM":"IT","TITAN":"Consumer","TRENT":"Retail","ULTRACEMCO":"Cement",
    "WIPRO":"IT","AUBANK":"Banking","AUROPHARMA":"Pharma","DMART":"Retail",
    "BAJAJHLDNG":"Holding","BALKRISIND":"Auto Ancillary","BANDHANBNK":"Banking",
    "BANKBARODA":"Banking","BERGEPAINT":"Paints","BHARATFORG":"Auto Ancillary",
    "BIOCON":"Biotech","BSE":"Exchange","CANBK":"Banking","CHOLAFIN":"NBFC",
    "CUMMINSIND":"Engineering","DABUR":"FMCG","DEEPAKNTR":"Chemicals",
    "DIXON":"Electronics","DLF":"Real Estate","ESCORTS":"Auto",
    "FEDERALBNK":"Banking","GAIL":"Gas","GODREJCP":"FMCG",
    "GODREJPROP":"Real Estate","GUJGASLTD":"Gas","HAVELLS":"Electricals",
    "HDFCAMC":"AMC","HAL":"Defence","HINDPETRO":"Oil & Gas",
    "IDFCFIRSTB":"Banking","IEX":"Exchange","INDHOTEL":"Hospitality",
    "IOC":"Oil & Gas","IRFC":"NBFC","IGL":"Gas","INDIGO":"Aviation",
    "IRCTC":"Tourism","IREDA":"NBFC","JINDALSTEL":"Steel",
    "JUBLFOOD":"QSR","KAJARIACER":"Tiles","KEC":"Infrastructure",
    "LTF":"NBFC","LTTS":"IT","LAURUSLABS":"Pharma","LICI":"Insurance",
    "LUPIN":"Pharma","LODHA":"Real Estate","M&MFIN":"NBFC",
    "MANAPPURAM":"NBFC","MARICO":"FMCG","MFSL":"Insurance",
    "MPHASIS":"IT","MRF":"Tyres","NAUKRI":"Internet","NAVINFLUOR":"Chemicals",
    "NMDC":"Mining","OBEROIRLTY":"Real Estate","OIL":"Oil & Gas",
    "PAGEIND":"Textiles","PERSISTENT":"IT","PETRONET":"Gas","PIIND":"Agrochem",
    "PIDILITIND":"Chemicals","POLYCAB":"Electricals","PREMIERENE":"Solar",
    "PNB":"Banking","RVNL":"Infrastructure","RECLTD":"NBFC",
    "MOTHERSON":"Auto Ancillary","SBICARD":"NBFC","SIEMENS":"Engineering",
    "SRF":"Chemicals","SAIL":"Steel","SUNTV":"Media","SWIGGY":"Internet",
    "TATACOMM":"Telecom","TATAELXSI":"IT","TATAPOWER":"Power",
    "TORNTPHARM":"Pharma","TORNTPOWER":"Power","TVSMOTOR":"Auto",
    "UPL":"Agrochem","VEDL":"Metals","VOLTAS":"Consumer Durables",
    "WAAREEENER":"Solar","YESBANK":"Banking","ZEEL":"Media","ZOMATO":"Internet",
}

def build_fo_rows(
    lot_sizes: dict,
    cmp_map: dict,
    history: dict,
    expiries: tuple,
) -> tuple:
    """
    Returns (futures_rows, options_rows).
    Each row is a list of values matching sheet headers.
    """
    exp_near, exp_mid, exp_far = expiries
    futures_rows = []
    options_rows = []
    updated = _ist_now()

    # All F&O symbols = union of lot_size keys (NSE CSV) + fallback
    all_syms = set(lot_sizes.keys()) | set(FALLBACK_LOT_SIZES.keys())
    # Exclude index symbols from stock list (they're written separately)
    index_syms = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

    # ── Index rows first ──────────────────────────────────────────────────
    index_meta = [
        ("NIFTY",      "Nifty 50",            "Index", 13),
        ("BANKNIFTY",  "Bank Nifty",           "Index", 13),
        ("FINNIFTY",   "Nifty Financial Svc",  "Index", 13),
        ("MIDCPNIFTY", "Nifty Midcap Select",  "Index", 13),
        ("NIFTYNXT50", "Nifty Next 50",        "Index", 13),
    ]
    for sym, name, sector, margin_pct in index_meta:
        lot  = lot_sizes.get(sym, FALLBACK_LOT_SIZES.get(sym, 0))
        ltp  = cmp_map.get(sym, 0)
        hist = history.get(sym, [])
        intra_trend = _trend(hist[-5:] if len(hist) >= 5 else hist)
        swing_trend = _trend(hist)
        contract_val = lot * ltp
        margin_req   = round(contract_val * margin_pct / 100)
        atm_prem     = _approx_atm_premium(ltp)
        call_lot_cost = atm_prem * lot
        put_lot_cost  = atm_prem * lot
        seller_margin = round(contract_val * 0.13)

        futures_rows.append([
            name, sym, sector, lot, round(ltp, 2), round(contract_val),
            f"{margin_pct}%", margin_req,
            exp_near, exp_mid, exp_far,
            intra_trend, swing_trend,
            "BUY / LONG" if "Bullish" in intra_trend else "SELL / SHORT" if "Bearish" in intra_trend else "WAIT",
            "BUY / LONG" if "Bullish" in swing_trend else "SELL / SHORT" if "Bearish" in swing_trend else "WAIT",
            updated,
        ])
        options_rows.append([
            name, sym, sector, lot, round(ltp, 2), round(contract_val),
            exp_near, exp_mid, exp_far,
            atm_prem, atm_prem, call_lot_cost, put_lot_cost, seller_margin,
            intra_trend, swing_trend,
            _options_strategy(intra_trend), _options_strategy(swing_trend),
            updated,
        ])

    # ── Stock rows ────────────────────────────────────────────────────────
    stock_syms = sorted(all_syms - index_syms)
    for sym in stock_syms:
        lot  = lot_sizes.get(sym, FALLBACK_LOT_SIZES.get(sym, 0))
        if lot == 0:
            continue
        ltp  = cmp_map.get(sym, 0)
        if ltp == 0:
            continue  # not in today's bhavcopy → skip
        sector = SECTOR_MAP.get(sym, "Equity")
        hist = history.get(sym, [])
        intra_trend = _trend(hist[-5:] if len(hist) >= 5 else hist)
        swing_trend = _trend(hist)
        contract_val  = lot * ltp
        margin_pct    = 20
        margin_req    = round(contract_val * margin_pct / 100)
        atm_prem      = _approx_atm_premium(ltp)
        call_lot_cost = atm_prem * lot
        put_lot_cost  = atm_prem * lot
        seller_margin = round(contract_val * 0.20)

        futures_rows.append([
            sym, sym, sector, lot, round(ltp, 2), round(contract_val),
            f"{margin_pct}%", margin_req,
            exp_near, exp_mid, exp_far,
            intra_trend, swing_trend,
            "BUY / LONG" if "Bullish" in intra_trend else "SELL / SHORT" if "Bearish" in intra_trend else "WAIT",
            "BUY / LONG" if "Bullish" in swing_trend else "SELL / SHORT" if "Bearish" in swing_trend else "WAIT",
            updated,
        ])
        options_rows.append([
            sym, sym, sector, lot, round(ltp, 2), round(contract_val),
            exp_near, exp_mid, exp_far,
            atm_prem, atm_prem, call_lot_cost, put_lot_cost, seller_margin,
            intra_trend, swing_trend,
            _options_strategy(intra_trend), _options_strategy(swing_trend),
            updated,
        ])

    return futures_rows, options_rows


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS WRITER
# ─────────────────────────────────────────────────────────────────────────────
FUT_HEADERS = [
    "Company / Symbol", "NSE Symbol", "Sector", "Lot Size",
    "CMP ₹ (Live)", "Contract Value ₹",
    "Margin %", "Margin Required ₹",
    "Near Expiry", "Mid Expiry", "Far Expiry",
    "Intraday Trend", "Swing Trend",
    "Intraday Signal", "Swing Signal",
    "Last Updated",
]
OPT_HEADERS = [
    "Company / Symbol", "NSE Symbol", "Sector", "Lot Size",
    "CMP ₹ (Live)", "Contract Value ₹",
    "Near Expiry", "Mid Expiry", "Far Expiry",
    "ATM Call Premium ₹", "ATM Put Premium ₹",
    "Call Cost / Lot ₹", "Put Cost / Lot ₹",
    "Seller Margin ₹",
    "Intraday Trend", "Swing Trend",
    "Intraday Strategy", "Swing Strategy",
    "Last Updated",
]


class SheetsWriter:
    def __init__(self, creds_json: str) -> None:
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSHEETS_SCOPES)
        self._client = gspread.authorize(creds)

    def _get_or_create(self, ss, title: str) -> gspread.Worksheet:
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating new sheet: %s", title)
            return ss.add_worksheet(title=title, rows=500, cols=25)

    def open_worksheets(self):
        ss = self._client.open_by_key(SPREADSHEET_ID)
        ws_vol = self._get_or_create(ss, SHEET_VOLUME)
        ws_to  = self._get_or_create(ss, SHEET_TURNOVER)
        ws_fut = self._get_or_create(ss, SHEET_FUTURES)
        ws_opt = self._get_or_create(ss, SHEET_OPTIONS)
        return ws_vol, ws_to, ws_fut, ws_opt

    def write_volume_turnover(self, ws_vol, ws_to, data_vol, data_to, fetched_date):
        status = f"Data Date: {fetched_date} | Updated: {_ist_now()}"
        for ws, data in [(ws_vol, data_vol), (ws_to, data_to)]:
            end_row = 1 + len(data)
            ws.batch_update(
                [
                    {"range": f"A2:C{end_row}", "values": data},
                    {"range": STATUS_CELL,       "values": [[status]]},
                ],
                value_input_option="USER_ENTERED",
            )
            log.info("Updated '%s' with %d rows.", ws.title, len(data))

    def write_fo_sheet(self, ws: gspread.Worksheet, headers: list, rows: list, sheet_name: str):
        """Clear and rewrite an F&O sheet with headers + data rows."""
        total_rows = len(rows) + 1
        all_data = [headers] + rows
        ws.clear()
        # Write in one batch (max 10MB payload; ~300 rows × 20 cols is fine)
        ws.update(range_name=f"A1:{get_col_letter(len(headers))}{total_rows}",
                  values=all_data,
                  value_input_option="USER_ENTERED")
        log.info("Updated '%s' with %d data rows.", sheet_name, len(rows))


def get_col_letter(n: int) -> str:
    """Convert 1-based column number to letter (A, B, ... Z, AA, ...)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # 1. Credentials
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GCP_CREDENTIALS environment variable is not set.")

    # 2. IST today
    ist_today = datetime.utcnow() + timedelta(hours=5, minutes=30)

    # 3. Connect to Sheets
    log.info("Connecting to Google Sheets…")
    writer = SheetsWriter(creds_json)
    ws_vol, ws_to, ws_fut, ws_opt = writer.open_worksheets()

    # 4. Fetch today's bhavcopy (volume + turnover + CMP map)
    fetcher = BhavcopFetcher()
    result, fetched_date_str = None, ""
    for days_back in range(LOOKBACK_DAYS + 1):
        candidate = ist_today - timedelta(days=days_back)
        if candidate.weekday() >= 5:
            continue
        result = fetcher.fetch(candidate)
        if result:
            fetched_date_str = candidate.strftime("%d-%b-%Y")
            break

    if result is None:
        raise RuntimeError(f"No bhavcopy data found for the last {LOOKBACK_DAYS} trading days.")

    data_vol, data_to, cmp_map = result

    # 5. Write volume + turnover sheets (existing logic)
    writer.write_volume_turnover(ws_vol, ws_to, data_vol, data_to, fetched_date_str)

    # 6. Fetch lot sizes
    log.info("Fetching F&O lot sizes from NSE…")
    lot_sizes = LotSizeFetcher().fetch()
    if not lot_sizes:
        log.warning("Lot size fetch failed; using fallback data.")
        lot_sizes = FALLBACK_LOT_SIZES

    # 7. Fetch price history for trend calculation
    log.info("Fetching 15-day price history for momentum trends…")
    history = HistoryFetcher().fetch_history(ist_today)

    # 8. Compute expiry dates
    expiries = _expiry_dates()
    log.info("Expiries: Near=%s  Mid=%s  Far=%s", *expiries)

    # 9. Build F&O rows
    log.info("Building Futures and Options rows…")
    futures_rows, options_rows = build_fo_rows(lot_sizes, cmp_map, history, expiries)

    # 10. Write Futures and Options sheets
    writer.write_fo_sheet(ws_fut, FUT_HEADERS, futures_rows, SHEET_FUTURES)
    writer.write_fo_sheet(ws_opt, OPT_HEADERS, options_rows, SHEET_OPTIONS)

    log.info("✅ SUCCESS — All 4 sheets updated with data from %s.", fetched_date_str)


if __name__ == "__main__":
    main()
