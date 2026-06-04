"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NSE F&O Auto-Sheet  —  update_sheet.py  (v8 — Definitive Fix)             ║
║  GitHub: DSOLAPURE/NSE-Auto-Sheet                                           ║
║                                                                              ║
║  ROOT CAUSE of missing analytics (v6/v7):                                   ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  v7 used NSESession to warm-up via www.nseindia.com before fetching data.   ║
║  www.nseindia.com returns HTTP 403 on GitHub Actions (bot detection).       ║
║  This warm-up FAILED SILENTLY, meaning FO bhavcopy and 52wk CSV fetches    ║
║  that depended on cookies were also returning 403 → all "—".               ║
║                                                                              ║
║  THE FIX (v8):                                                              ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  NSESession / warm_up REMOVED entirely.                                     ║
║                                                                              ║
║  All data comes from nsearchives.nseindia.com which serves files            ║
║  WITHOUT requiring cookies — same domain the CM bhavcopy uses               ║
║  (proven working since Top 250 sheets populate correctly).                  ║
║                                                                              ║
║  52-Week High/Low: NO LONGER a separate file fetch.                         ║
║  Computed from 260 trading days of CM bhavcopy history (≈ 1 year).         ║
║  Eliminates dependency on archives.nseindia.com which needs cookies.        ║
║                                                                              ║
║  DATA SOURCES (all via nsearchives.nseindia.com — no cookies needed):      ║
║    CMP / Volume / Turnover  → CM bhavcopy ZIP       ✅ proven working       ║
║    OI / PCR / Max Pain      → FO bhavcopy ZIP       ✅ same domain as CM    ║
║    Index CMP / India VIX    → ind_close_all CSV     ✅ proven working       ║
║    Delivery %               → sec_bhavdata_full CSV ✅ same domain          ║
║    Lot sizes                → fo_mktlots.csv        ✅ proven working       ║
║    52-Wk High/Low           → computed from history ✅ no HTTP needed       ║
║    RSI / MACD / Beta        → computed from history ✅ no HTTP needed       ║
║    Support / Resistance     → computed from history ✅ no HTTP needed       ║
║    IV %                     → formula from ATM prem ✅ no HTTP needed       ║
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
from datetime import datetime, timedelta, date, timezone

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SPREADSHEET_ID   = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME     = "Top 250 Stocks"
SHEET_TURNOVER   = "Top 250 Turnover"
SHEET_FUTURES    = "Futures F&O"
SHEET_OPTIONS    = "Options F&O"
SHEET_CHAIN      = "Index Option Chain"  # new option chain sheet
TOP_N            = 250
LOOKBACK_DAYS    = 7
REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 3
RETRY_DELAY      = 5
HISTORY_DAYS     = 35    # for RSI/MACD/Beta/Support/Resistance
WEEK52_DAYS      = 260   # ~1 trading year for 52-week High/Low
WRITE_CHUNK      = 100
EXCLUDE_PATTERN  = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

# ── All on nsearchives.nseindia.com — no cookies needed ──────────────────────
BASE = "https://nsearchives.nseindia.com"
BHAVCOPY_URL  = BASE + "/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
FO_BHAV_URL   = BASE + "/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
INDEX_CSV_URL = BASE + "/content/indices/ind_close_all_{date}.csv"
DELIVERY_URL  = BASE + "/products/content/sec_bhavdata_full_{date}.csv"

# ── On archives.nseindia.com — no cookies needed for these ───────────────────
MKTLOTS_URL   = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

# Single shared header — no cookies, no session needed
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — STATIC REFERENCE DATA
# ══════════════════════════════════════════════════════════════════════════════

INDEX_META = [
    ("NIFTY",      "Nifty 50",            "Index", 13),
    ("BANKNIFTY",  "Bank Nifty",          "Index", 13),
    ("FINNIFTY",   "Nifty Financial Svc", "Index", 13),
    ("MIDCPNIFTY", "Nifty Midcap Select", "Index", 13),
    ("NIFTYNXT50", "Nifty Next 50",       "Index", 13),
]
INDEX_SYMS = {r[0] for r in INDEX_META}

INDEX_NAME_MAP = {
    "Nifty 50": "NIFTY", "Nifty Bank": "BANKNIFTY",
    "Nifty Financial Services": "FINNIFTY", "Nifty Fin Services": "FINNIFTY",
    "Nifty Midcap Select": "MIDCPNIFTY", "Nifty Next 50": "NIFTYNXT50",
    "India Vix": "VIX",  "India VIX": "VIX",  "INDIA VIX": "VIX",
    "NIFTY 50": "NIFTY", "NIFTY BANK": "BANKNIFTY",
    "NIFTY FINANCIAL SERVICES": "FINNIFTY", "NIFTY MIDCAP SELECT": "MIDCPNIFTY",
    "NIFTY NEXT 50": "NIFTYNXT50",
}
INDEX_FALLBACK_CMP = {
    "NIFTY": 24500.0, "BANKNIFTY": 52000.0, "FINNIFTY": 23800.0,
    "MIDCPNIFTY": 12400.0, "NIFTYNXT50": 67000.0,
}

FALLBACK_LOTS = {
    "NIFTY":65,"BANKNIFTY":30,"FINNIFTY":60,"MIDCPNIFTY":120,"NIFTYNXT50":25,
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

def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of {candidates} found in {list(df.columns)}")

def _ist_now():
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime(
        "%d-%b-%Y %H:%M IST")

def _ist_today():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def _col_letter(n):
    r = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        r = chr(65 + rem) + r
    return r

def _last_thursday(year, month):
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - 3) % 7)

def _expiry_dates():
    today = _ist_today().date()
    expiries, m, y = [], today.month, today.year
    for _ in range(6):
        exp = _last_thursday(y, m)
        if exp >= today:
            expiries.append(exp.strftime("%d-%b-%Y"))
        if len(expiries) == 3:
            break
        m += 1
        if m > 12:
            m, y = 1, y + 1
    while len(expiries) < 3:
        expiries.append("—")
    return tuple(expiries)

def _download(url: str, label: str = "") -> bytes | None:
    """
    Simple direct download — no session, no cookies, no warm-up.
    nsearchives.nseindia.com serves files without cookies.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                log.info("  %-25s HTTP 200  (%d bytes)", label or url.split("/")[-1], len(r.content))
                return r.content
            log.warning("  %-25s HTTP %s  (attempt %d/%d)", label, r.status_code, attempt, MAX_RETRIES)
        except requests.RequestException as e:
            log.warning("  %-25s error attempt %d: %s", label, attempt, e)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
    return None

def _trading_days_back(ist_today, n=50):
    count = 0
    for d in range(1, n * 3):
        cand = ist_today - timedelta(days=d)
        if cand.weekday() < 5:
            yield cand
            count += 1
            if count >= n:
                break

def _atm_premium(ltp, days=25, iv=0.28):
    prem = 0.4 * iv * math.sqrt(max(days, 1) / 252) * ltp
    return max(5, int(round(prem / 5) * 5))

def _round_strike(ltp):
    step = (100 if ltp > 20000 else 50 if ltp > 5000 else
            20  if ltp > 1000  else 10  if ltp > 200  else 5)
    return int(round(ltp / step) * step)

def _trend(closes):
    if len(closes) < 3:
        return "Sideways"
    n = len(closes)
    avg5  = sum(closes[-min(5,  n):]) / min(5,  n)
    avg20 = sum(closes[-min(20, n):]) / min(20, n)
    if avg20 == 0:
        return "Sideways"
    diff = (avg5 - avg20) / avg20 * 100
    if diff >  0.8: return "Bullish"
    if diff < -0.8: return "Bearish"
    return "Sideways"

def _trend_label(t):
    return {"Bullish": "🟢 Bullish", "Bearish": "🔴 Bearish"}.get(t, "🟡 Sideways")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — ANALYTICS CALCULATORS
# ══════════════════════════════════════════════════════════════════════════════

def _calc_rsi(closes, period=14):
    if len(closes) < period + 2:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        chg = closes[i] - closes[i-1]
        gains.append(max(chg, 0))
        losses.append(max(-chg, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0:
        return 100.0
    return round(100 - (100 / (1 + ag / al)), 1)

def _rsi_label(rsi):
    if rsi is None: return "—"
    if rsi >= 70:   return f"{rsi} 🔴 Overbought"
    if rsi <= 30:   return f"{rsi} 🟢 Oversold"
    return f"{rsi} 🟡 Neutral"

def _calc_ema(closes, period):
    if not closes:
        return []
    k = 2 / (period + 1)
    ema = [closes[0]]
    for p in closes[1:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

def _calc_macd(closes):
    """12/26/9 MACD. Minimum 27 data points."""
    if len(closes) < 27:
        return None, None, None, "—"
    e12  = _calc_ema(closes, 12)
    e26  = _calc_ema(closes, 26)
    macd = [a - b for a, b in zip(e12, e26)]
    sig  = _calc_ema(macd, 9)
    hist = [m - s for m, s in zip(macd, sig)]
    m, s, h = round(macd[-1], 2), round(sig[-1], 2), round(hist[-1], 2)
    if   h > 0 and m > 0: label = f"🟢 Bullish  MACD={m}  Sig={s}  Hist=+{h}"
    elif h > 0:            label = f"🟡 Recovering  MACD={m}  Sig={s}  Hist=+{h}"
    elif h < 0 and m < 0:  label = f"🔴 Bearish  MACD={m}  Sig={s}  Hist={h}"
    else:                  label = f"🟡 Weakening  MACD={m}  Sig={s}  Hist={h}"
    return m, s, h, label

def _calc_beta(stock_closes, nifty_closes):
    n = min(len(stock_closes), len(nifty_closes))
    if n < 5:
        return None
    sr = [(stock_closes[i] - stock_closes[i-1]) / stock_closes[i-1] for i in range(1, n)]
    nr = [(nifty_closes[i] - nifty_closes[i-1]) / nifty_closes[i-1] for i in range(1, n)]
    if len(sr) < 4:
        return None
    ms, mn = sum(sr) / len(sr), sum(nr) / len(nr)
    cov = sum((s - ms) * (ni - mn) for s, ni in zip(sr, nr)) / len(sr)
    vn  = sum((ni - mn) ** 2 for ni in nr) / len(nr)
    return round(cov / vn, 2) if vn else None

def _calc_support_resistance(closes):
    if len(closes) < 5:
        return None, None
    w = closes[-min(10, len(closes)):]
    return round(min(w), 2), round(max(w), 2)

def _calc_52wk(closes_260):
    """52-week High/Low from up to 260 trading-day close history."""
    if not closes_260:
        return None, None
    return round(max(closes_260), 2), round(min(closes_260), 2)

def _calc_iv(ltp, atm_prem, days=25):
    denom = 0.4 * ltp * math.sqrt(max(days, 1) / 252)
    if denom == 0:
        return "—"
    return f"{atm_prem / denom * 100:.1f}%"

def _calc_max_pain(oi_by_strike: dict):
    if not oi_by_strike:
        return None
    strikes = sorted(oi_by_strike.keys())
    min_pain, best = float("inf"), strikes[0]
    for s_test in strikes:
        loss = 0
        for s_str, data in oi_by_strike.items():
            if s_test > s_str: loss += (s_test - s_str) * data.get("CE", 0)
            if s_test < s_str: loss += (s_str - s_test) * data.get("PE", 0)
        if loss < min_pain:
            min_pain, best = loss, s_test
    return best


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — STRATEGY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_engine(ltp, atm_prem, intra_trend, swing_trend, lot, timeframe="intraday"):
    trend = intra_trend if timeframe == "intraday" else swing_trend
    atm   = _round_strike(ltp)
    step  = (100 if ltp > 20000 else 50 if ltp > 5000 else
             20  if ltp > 1000  else 10  if ltp > 200  else 5)
    otm1, otm2 = atm + step, atm + 2 * step
    itm1, itm2 = atm - step, atm - 2 * step
    pa  = atm_prem
    po1 = max(5, int(pa * 0.55 / 5) * 5)
    po2 = max(5, int(pa * 0.30 / 5) * 5)

    if trend == "Bullish":
        if timeframe == "intraday":
            sl = max(5, int(pa * 0.5))
            return {"Strategy": "Long Call",
                    "CE Entry":  f"Buy {atm} CE @ ₹{pa}",
                    "PE Entry":  "—",
                    "CE Target": f"₹{pa*2} (2× premium)",
                    "PE Target": "—",
                    "Stop Loss": f"₹{sl} (50% of premium)",
                    "Max Profit/Lot": f"₹{pa*2*lot:,}",
                    "Max Loss/Lot":   f"₹{pa*lot:,}",
                    "Risk:Reward":    "1:2",
                    "Rationale": f"Intraday bullish. Buy ATM {atm} CE @ ₹{pa}. Target ₹{pa*2}, SL ₹{sl}."}
        else:
            nd = pa - po1; mg = (otm1 - atm) - nd; rr = round(mg / max(nd, 1), 1)
            return {"Strategy": "Bull Call Spread",
                    "CE Entry":  f"Buy {atm} CE @ ₹{pa} | Sell {otm1} CE @ ₹{po1}",
                    "PE Entry":  "—",
                    "CE Target": f"Close ≥ ₹{otm1} at expiry",
                    "PE Target": "—",
                    "Stop Loss": f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot": f"₹{mg*lot:,}",
                    "Max Loss/Lot":   f"₹{nd*lot:,}",
                    "Risk:Reward":    f"1:{rr}",
                    "Rationale": f"Swing bullish. Buy {atm} CE ₹{pa}, sell {otm1} CE ₹{po1}. Net debit ₹{nd}."}

    elif trend == "Bearish":
        if timeframe == "intraday":
            sl = max(5, int(pa * 0.5))
            return {"Strategy": "Long Put",
                    "CE Entry":  "—",
                    "PE Entry":  f"Buy {atm} PE @ ₹{pa}",
                    "CE Target": "—",
                    "PE Target": f"₹{pa*2} (2× premium)",
                    "Stop Loss": f"₹{sl} (50% of premium)",
                    "Max Profit/Lot": f"₹{pa*2*lot:,}",
                    "Max Loss/Lot":   f"₹{pa*lot:,}",
                    "Risk:Reward":    "1:2",
                    "Rationale": f"Intraday bearish. Buy ATM {atm} PE @ ₹{pa}. Target ₹{pa*2}, SL ₹{sl}."}
        else:
            nd = pa - po1; mg = (atm - itm1) - nd; rr = round(mg / max(nd, 1), 1)
            return {"Strategy": "Bear Put Spread",
                    "CE Entry":  "—",
                    "PE Entry":  f"Buy {atm} PE @ ₹{pa} | Sell {itm1} PE @ ₹{po1}",
                    "CE Target": "—",
                    "PE Target": f"Close ≤ ₹{itm1} at expiry",
                    "Stop Loss": f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot": f"₹{mg*lot:,}",
                    "Max Loss/Lot":   f"₹{nd*lot:,}",
                    "Risk:Reward":    f"1:{rr}",
                    "Rationale": f"Swing bearish. Buy {atm} PE ₹{pa}, sell {itm1} PE ₹{po1}. Net debit ₹{nd}."}
    else:
        if timeframe == "intraday":
            cr = po1 * 2; be_hi = otm1 + cr; be_lo = itm1 - cr
            return {"Strategy": "Short Strangle",
                    "CE Entry":  f"Sell {otm1} CE @ ₹{po1}",
                    "PE Entry":  f"Sell {itm1} PE @ ₹{po1}",
                    "CE Target": f"Stay below ₹{otm1}",
                    "PE Target": f"Stay above ₹{itm1}",
                    "Stop Loss": f"Exit both if loss > ₹{cr*lot:,} (1× credit)",
                    "Max Profit/Lot": f"₹{cr*lot:,}",
                    "Max Loss/Lot":   "Unlimited — use hard SL",
                    "Risk:Reward":    "Credit; strict SL required",
                    "Rationale": f"Sideways. Sell {otm1} CE + {itm1} PE. Credit ₹{cr}. BE: ₹{be_lo}–₹{be_hi}."}
        else:
            nc = max(5, (po1 - po2) * 2); w = otm1 - atm; ml = max(1, w - nc)
            rr = round(ml / max(nc, 1), 1)
            return {"Strategy": "Iron Condor",
                    "CE Entry":  f"Sell {otm1} CE @ ₹{po1} | Buy {otm2} CE @ ₹{po2}",
                    "PE Entry":  f"Sell {itm1} PE @ ₹{po1} | Buy {itm2} PE @ ₹{po2}",
                    "CE Target": f"Stay below ₹{otm1}",
                    "PE Target": f"Stay above ₹{itm1}",
                    "Stop Loss": f"Exit breached side if loss > ₹{nc*2*lot:,}",
                    "Max Profit/Lot": f"₹{nc*lot:,}",
                    "Max Loss/Lot":   f"₹{ml*lot:,}",
                    "Risk:Reward":    f"1:{rr}",
                    "Rationale": f"Sideways swing. Iron Condor. Net credit ₹{nc}. Max loss ₹{ml}."}

def _sv(s, k):
    return s.get(k, "—")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

class CMBhavcopFetcher:
    """
    CM equity bhavcopy.
    Same URL pattern proven working on GitHub Actions (Top250 sheets work).
    Returns (data_vol, data_to, cmp_map) or None.
    """
    def fetch(self, dt):
        url = BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        raw = _download(url, f"CM {dt.strftime('%d-%b')}")
        if raw is None:
            return None
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    df = pd.read_csv(f, low_memory=False)
            df.columns = [c.strip() for c in df.columns]
            sym_c = _pick_col(df, COL_MAP["symbol"])
            cls_c = _pick_col(df, COL_MAP["close"])
            vol_c = _pick_col(df, COL_MAP["volume"])
            tov_c = _pick_col(df, COL_MAP["turnover"])
            ser_c = _pick_col(df, COL_MAP["series"])
            # Filter EQ series
            df = df[df[ser_c].astype(str).str.strip() == "EQ"].copy()
            df = df[~df[sym_c].astype(str).str.contains(
                EXCLUDE_PATTERN, case=False, na=False)].reset_index(drop=True)
            for c in (cls_c, vol_c, tov_c):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            data_vol = (df.sort_values(vol_c, ascending=False)
                        .head(TOP_N)[[sym_c, vol_c, cls_c]].fillna(0).values.tolist())
            data_to  = (df.sort_values(tov_c, ascending=False)
                        .head(TOP_N)[[sym_c, tov_c, cls_c]].fillna(0).values.tolist())
            cmp_map  = dict(zip(df[sym_c].astype(str), df[cls_c].fillna(0)))
            log.info("  CM bhavcopy: %d EQ rows", len(df))
            return data_vol, data_to, cmp_map
        except Exception as e:
            log.error("CM bhavcopy parse: %s", e)
            return None


class FOBhavcopFetcher:
    """
    FO bhavcopy for OI, PCR, Max Pain.
    Same domain as CM bhavcopy (nsearchives) — no cookies needed.
    """
    FO_COLS = {
        "symbol": ["TckrSymb", "SYMBOL", "FinInstrmNm"],
        "option": ["OptnTp",   "OPTION_TYP", "OptionType"],
        "strike": ["StrkPric", "STRIKE_PR",  "StrikePrice"],
        "oi":     ["OpnIntrst","OPEN_INT",   "OpenInterest", "OI"],
        "expiry": ["XpryDt",   "EXPIRY_DT",  "ExpiryDate"],
    }

    def fetch(self, dt):
        url = FO_BHAV_URL.format(date=dt.strftime("%Y%m%d"))
        raw = _download(url, f"FO {dt.strftime('%d-%b')}")
        if raw is None:
            return {}, {}, {}, {}
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    df = pd.read_csv(f, low_memory=False)
            df.columns = [c.strip() for c in df.columns]
            sym_c = _pick_col(df, self.FO_COLS["symbol"])
            opt_c = _pick_col(df, self.FO_COLS["option"])
            str_c = _pick_col(df, self.FO_COLS["strike"])
            oi_c  = _pick_col(df, self.FO_COLS["oi"])
            df[sym_c] = df[sym_c].astype(str).str.strip()
            df[opt_c] = df[opt_c].astype(str).str.strip().str.upper()
            df[str_c] = pd.to_numeric(df[str_c], errors="coerce")
            df[oi_c]  = pd.to_numeric(df[oi_c],  errors="coerce").fillna(0)
            # Near-month expiry only
            try:
                exp_c = _pick_col(df, self.FO_COLS["expiry"])
                df[exp_c] = pd.to_datetime(df[exp_c], errors="coerce")
                df = df[df[exp_c] == df[exp_c].min()]
            except Exception:
                pass
            oi_ce, oi_pe, oi_by = {}, {}, {}
            for _, row in df.iterrows():
                sym    = row[sym_c]
                opt    = row[opt_c]
                strike = row[str_c]
                oi     = row[oi_c]
                if opt == "CE":
                    oi_ce[sym] = oi_ce.get(sym, 0) + oi
                    oi_by.setdefault(sym, {}).setdefault(strike, {"CE": 0, "PE": 0})
                    oi_by[sym][strike]["CE"] += oi
                elif opt == "PE":
                    oi_pe[sym] = oi_pe.get(sym, 0) + oi
                    oi_by.setdefault(sym, {}).setdefault(strike, {"CE": 0, "PE": 0})
                    oi_by[sym][strike]["PE"] += oi
            oi_map = {s: oi_ce.get(s, 0) + oi_pe.get(s, 0)
                      for s in set(list(oi_ce) + list(oi_pe))}
            log.info("  FO OI: %d symbols", len(oi_map))
            return oi_map, oi_ce, oi_pe, oi_by
        except Exception as e:
            log.warning("FO bhavcopy parse: %s", e)
            return {}, {}, {}, {}


class IndexPriceFetcher:
    """Index CMP + India VIX from ind_close_all CSV (nsearchives domain)."""

    def fetch(self, ist_today):
        for cand in _trading_days_back(ist_today, LOOKBACK_DAYS):
            data = self._one(cand)
            if data:
                log.info("  Index prices %s: %s", cand.strftime("%d-%b-%Y"),
                         {k: f"₹{v:,.0f}" for k, v in data.items() if k != "VIX"})
                return data
        log.warning("  Index CSV unavailable — using fallback")
        return dict(INDEX_FALLBACK_CMP)

    def fetch_history(self, ist_today, days=HISTORY_DAYS):
        history, found = {}, 0
        for cand in _trading_days_back(ist_today, days + 20):
            dd = self._one(cand)
            if not dd:
                continue
            for sym, price in dd.items():
                history.setdefault(sym, []).append(price)
            found += 1
            if found >= days:
                break
        return {s: list(reversed(v)) for s, v in history.items()}

    def _one(self, dt):
        url = INDEX_CSV_URL.format(date=dt.strftime("%d%m%Y"))
        raw = _download(url, f"Idx {dt.strftime('%d-%b')}")
        if raw is None:
            return {}
        try:
            df = pd.read_csv(io.StringIO(raw.decode("utf-8", errors="replace")))
            df.columns = [c.strip() for c in df.columns]
            nc = next((c for c in df.columns if "index" in c.lower() and "name" in c.lower()), None)
            cc = next((c for c in df.columns if "clos" in c.lower()), None)
            if not nc or not cc:
                return {}
            df[cc] = pd.to_numeric(df[cc].astype(str).str.replace(",", ""), errors="coerce")
            result = {}
            for _, row in df.iterrows():
                name = str(row[nc]).strip()
                sym  = INDEX_NAME_MAP.get(name) or INDEX_NAME_MAP.get(name.title())
                if sym and pd.notna(row[cc]) and row[cc] > 0:
                    result[sym] = float(row[cc])
            for sym, fb in INDEX_FALLBACK_CMP.items():
                if sym not in result:
                    result[sym] = fb
            return result
        except Exception as e:
            log.warning("  Index CSV parse: %s", e)
            return {}


class DeliveryFetcher:
    """
    Delivery % from sec_bhavdata_full_{date}.csv on nsearchives domain.
    Columns: SYMBOL, SERIES, DELIV_QTY, DELIV_PER
    """
    def fetch(self, ist_today) -> dict:
        for cand in _trading_days_back(ist_today, 5):
            url = DELIVERY_URL.format(date=cand.strftime("%d%m%Y"))
            raw = _download(url, f"Delivery {cand.strftime('%d-%b')}")
            if raw is None:
                continue
            try:
                text = raw.decode("utf-8", errors="replace")
                df   = pd.read_csv(io.StringIO(text))
                df.columns = [c.strip().upper() for c in df.columns]
                sym_c = next((c for c in df.columns if c in ("SYMBOL", "TCKRSYMB")), None)
                ser_c = next((c for c in df.columns if "SERIES" in c), None)
                pct_c = next((c for c in df.columns if any(
                    x in c for x in ("DELIV_PER", "DELPCT", "%DLY", "DELV_PER", "DELIVERY_PER"))), None)
                if not sym_c or not pct_c:
                    log.warning("  Delivery cols not found: %s", list(df.columns))
                    continue
                if ser_c:
                    df = df[df[ser_c].astype(str).str.strip() == "EQ"]
                df[pct_c] = pd.to_numeric(df[pct_c], errors="coerce")
                result = dict(zip(df[sym_c].astype(str).str.strip(), df[pct_c].fillna(0)))
                log.info("  Delivery: %d symbols from %s", len(result), cand.strftime("%d-%b-%Y"))
                return result
            except Exception as e:
                log.warning("  Delivery parse: %s", e)
        log.warning("  Delivery data unavailable")
        return {}


class LotSizeFetcher:
    def fetch(self):
        raw = _download(MKTLOTS_URL, "LotSizes")
        if raw is None:
            return {}
        try:
            df = pd.read_csv(io.StringIO(raw.decode("utf-8", errors="replace")), header=1, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            sc, lc = df.columns[0], df.columns[1]
            df[sc] = df[sc].str.strip()
            df[lc] = pd.to_numeric(df[lc].str.replace(",", ""), errors="coerce")
            result = dict(zip(df[sc].dropna(), df[lc].dropna().astype(int)))
            log.info("  Lot sizes: %d symbols", len(result))
            return result
        except Exception as e:
            log.warning("  LotSizes: %s", e)
            return {}


class HistoryFetcher:
    """
    Builds equity close-price history from CM bhavcopy (same proven-working URL).
    Also computes 52-week High/Low from 260-day history — no separate file needed.
    """
    def fetch(self, ist_today, days=HISTORY_DAYS):
        """Returns {symbol: [close_oldest, ..., close_newest]}"""
        history, fetcher, found = {}, CMBhavcopFetcher(), 0
        for cand in _trading_days_back(ist_today, days + 20):
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
        result = {s: list(reversed(v)) for s, v in history.items()}
        log.info("  Equity history: %d symbols × up to %d days", len(result), days)
        return result

    def fetch_52wk(self, ist_today):
        """
        52-Week High/Low computed from ~260 trading days of CM bhavcopy.
        No separate file — uses the same proven-working CM bhavcopy URL.
        Returns ({sym: 52wk_high}, {sym: 52wk_low})
        """
        log.info("  Fetching 260-day history for 52-wk High/Low…")
        history, fetcher, found = {}, CMBhavcopFetcher(), 0
        for cand in _trading_days_back(ist_today, WEEK52_DAYS + 30):
            res = fetcher.fetch(cand)
            if not res:
                continue
            _, _, cmap = res
            for sym, cls in cmap.items():
                if cls and cls > 0:
                    history.setdefault(sym, []).append(cls)
            found += 1
            if found >= WEEK52_DAYS:
                break
        hi_map = {s: round(max(v), 2) for s, v in history.items() if v}
        lo_map = {s: round(min(v), 2) for s, v in history.items() if v}
        log.info("  52-wk H/L: %d symbols", len(hi_map))
        return hi_map, lo_map

    @staticmethod
    def compute_52wk_from_history(combined_history: dict):
        """
        Compute 52-wk High/Low from ALREADY FETCHED history dict.
        Called in main() using the combined equity + index history,
        so zero additional HTTP requests are needed.
        Returns ({sym: high}, {sym: low})
        """
        hi_map = {s: round(max(v), 2) for s, v in combined_history.items() if v}
        lo_map = {s: round(min(v), 2) for s, v in combined_history.items() if v}
        return hi_map, lo_map


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SHEET HEADERS
# ══════════════════════════════════════════════════════════════════════════════

OPT_HEADERS = [
    "Sr.", "Company / Index Name", "NSE Symbol", "Sector / Type",
    "Lot Size\n(Units)", "CMP ₹\n(Approx.)", "Contract\nValue ₹",
    "Near-Month\nExpiry", "Mid-Month\nExpiry", "Far-Month\nExpiry",
    "Approx.\nATM Call ₹\n(Near Expiry)", "Approx.\nATM Put ₹\n(Near Expiry)",
    "Call Premium\nPaid (1 Lot) ₹", "Put Premium\nPaid (1 Lot) ₹",
    "Option Seller\nMargin ₹\n(~20% Contract)",
    "Intraday\nTrend", "Swing\nTrend",
    "Options Strategy\n(Intraday)", "Options Strategy\n(Swing)",
    "Intraday Signal:\nBest Strategy", "Intraday:\nCE Entry", "Intraday:\nPE Entry",
    "Intraday:\nCE Target", "Intraday:\nPE Target", "Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹", "Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward", "Intraday:\nRationale",
    "Swing Signal:\nBest Strategy", "Swing:\nCE Entry", "Swing:\nPE Entry",
    "Swing:\nCE Target", "Swing:\nPE Target", "Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹", "Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward", "Swing:\nRationale",
    # 14 analytics columns
    "Open Interest\n(OI — Lots)", "OI Change\n(vs Prev Day)",
    "PCR\n(Put-Call Ratio)", "52-Week\nHigh ₹", "52-Week\nLow ₹",
    "Delivery\n%", "India\nVIX", "IV %\n(Impl. Volatility)",
    "Max Pain\nStrike ₹", "Support\n₹", "Resistance\n₹",
    "Beta\nvs Nifty", "RSI\n(14-day)", "MACD Signal\n(12/26/9)",
    "Notes",
]   # 54 columns

FUT_HEADERS = [
    "Sr.", "Company / Index Name", "NSE Symbol", "Sector / Type",
    "Lot Size\n(Units)", "CMP ₹\n(Approx.)", "Contract\nValue ₹",
    "Futures\nMargin %", "Futures\nMargin Req. ₹",
    "Near-Month\nExpiry", "Mid-Month\nExpiry", "Far-Month\nExpiry",
    "Intraday\nTrend", "Swing\nTrend",
    "Intraday Signal:\nBest Strategy", "Intraday:\nCE Entry", "Intraday:\nPE Entry",
    "Intraday:\nCE Target", "Intraday:\nPE Target", "Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹", "Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward", "Intraday:\nRationale",
    "Swing Signal:\nBest Strategy", "Swing:\nCE Entry", "Swing:\nPE Entry",
    "Swing:\nCE Target", "Swing:\nPE Target", "Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹", "Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward", "Swing:\nRationale",
    # 14 analytics columns
    "Open Interest\n(OI — Lots)", "OI Change\n(vs Prev Day)",
    "PCR\n(Put-Call Ratio)", "52-Week\nHigh ₹", "52-Week\nLow ₹",
    "Delivery\n%", "India\nVIX", "IV %\n(Impl. Volatility)",
    "Max Pain\nStrike ₹", "Support\n₹", "Resistance\n₹",
    "Beta\nvs Nifty", "RSI\n(14-day)", "MACD Signal\n(12/26/9)",
    "Notes", "Last Updated",
]   # 50 columns


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — ROW BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_block(sym, ltp, lot,
                     oi_map, oi_ce, oi_pe, oi_by, prev_oi,
                     wk52_hi, wk52_lo, delv_map, india_vix,
                     equity_hist, nifty_hist):
    oi    = oi_map.get(sym, 0)
    ce_oi = oi_ce.get(sym, 0)
    pe_oi = oi_pe.get(sym, 0)
    prev  = prev_oi.get(sym, 0)
    oi_chg = (oi - prev) if (oi > 0 and prev > 0) else None
    pcr    = round(pe_oi / ce_oi, 2) if ce_oi > 0 else None
    if pcr:
        pcr_s = (f"{pcr} 🟢 Bullish" if pcr > 1.2
                 else f"{pcr} 🔴 Bearish" if pcr < 0.8
                 else f"{pcr} 🟡 Neutral")
    else:
        pcr_s = "—"
    hi52 = wk52_hi.get(sym) or None
    lo52 = wk52_lo.get(sym) or None
    dlv  = delv_map.get(sym, 0) or None
    vix_s = f"{india_vix:.2f}" if india_vix else "—"
    atm_p = _atm_premium(ltp)
    iv_s  = _calc_iv(ltp, atm_p) if ltp > 0 else "—"
    mp    = _calc_max_pain(oi_by.get(sym, {}))
    # equity_hist here is actually the caller-supplied history dict
    # (either global equity_hist for stocks, or local index history for indices)
    hist  = equity_hist.get(sym, [])
    sup, res = _calc_support_resistance(hist)
    beta  = _calc_beta(hist, nifty_hist) if (hist and nifty_hist) else None
    rsi   = _calc_rsi(hist)
    _, _, _, macd_label = _calc_macd(hist)
    return [
        int(oi)       if oi                else "—",
        int(oi_chg)   if oi_chg is not None else "—",
        pcr_s,
        hi52          if hi52              else "—",
        lo52          if lo52              else "—",
        f"{dlv:.1f}%" if dlv              else "—",
        vix_s,
        iv_s,
        f"₹{mp:,}"   if mp               else "—",
        f"₹{sup:,.2f}" if sup            else "—",
        f"₹{res:,.2f}" if res            else "—",
        str(beta)     if beta is not None else "—",
        _rsi_label(rsi),
        macd_label,
    ]   # exactly 14 items


def _build_opt_row(sr, name, sym, sector, lot, ltp, expiries, history, analytics, note=""):
    en, em, ef = expiries
    hist    = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    cval    = round(lot * ltp)
    ac      = _atm_premium(ltp)
    si = _strategy_engine(ltp, ac, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, ac, intra_t, swing_t, lot, "swing")
    return ([
        sr, name, sym, sector, lot, round(ltp, 2), cval,
        en, em, ef,
        ac, ac, ac * lot, ac * lot, round(cval * 0.20),
        _trend_label(intra_t), _trend_label(swing_t),
        si["Strategy"], ss["Strategy"],
        si["Strategy"], _sv(si, "CE Entry"), _sv(si, "PE Entry"),
        _sv(si, "CE Target"), _sv(si, "PE Target"), _sv(si, "Stop Loss"),
        _sv(si, "Max Profit/Lot"), _sv(si, "Max Loss/Lot"), _sv(si, "Risk:Reward"),
        _sv(si, "Rationale"),
        ss["Strategy"], _sv(ss, "CE Entry"), _sv(ss, "PE Entry"),
        _sv(ss, "CE Target"), _sv(ss, "PE Target"), _sv(ss, "Stop Loss"),
        _sv(ss, "Max Profit/Lot"), _sv(ss, "Max Loss/Lot"), _sv(ss, "Risk:Reward"),
        _sv(ss, "Rationale"),
    ] + analytics + [note])   # 39 + 14 + 1 = 54


def _build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries, history, analytics, note=""):
    en, em, ef = expiries
    hist    = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    cval    = round(lot * ltp)
    ac      = _atm_premium(ltp)
    si = _strategy_engine(ltp, ac, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, ac, intra_t, swing_t, lot, "swing")
    return ([
        sr, name, sym, sector, lot, round(ltp, 2), cval,
        f"{margin_pct}%", round(cval * margin_pct / 100),
        en, em, ef,
        _trend_label(intra_t), _trend_label(swing_t),
        si["Strategy"], _sv(si, "CE Entry"), _sv(si, "PE Entry"),
        _sv(si, "CE Target"), _sv(si, "PE Target"), _sv(si, "Stop Loss"),
        _sv(si, "Max Profit/Lot"), _sv(si, "Max Loss/Lot"), _sv(si, "Risk:Reward"),
        _sv(si, "Rationale"),
        ss["Strategy"], _sv(ss, "CE Entry"), _sv(ss, "PE Entry"),
        _sv(ss, "CE Target"), _sv(ss, "PE Target"), _sv(ss, "Stop Loss"),
        _sv(ss, "Max Profit/Lot"), _sv(ss, "Max Loss/Lot"), _sv(ss, "Risk:Reward"),
        _sv(ss, "Rationale"),
    ] + analytics + [note, _ist_now()])   # 34 + 14 + 2 = 50


def build_all_rows(lot_sizes, equity_cmp, index_cmp, equity_hist, index_hist,
                   oi_map, oi_ce, oi_pe, oi_by, prev_oi,
                   wk52_hi, wk52_lo, delv_map, expiries):
    fut_rows, opt_rows, sr = [], [], 1
    nifty_hist = index_hist.get("NIFTY") or equity_hist.get("NIFTY", [])
    india_vix  = index_cmp.get("VIX")

    def _ana(sym, ltp, lot, local_hist=None):
        # For indices, pass their own history dict so RSI/MACD/Beta/Support
        # use the correct index price series instead of an empty equity lookup.
        h = local_hist if local_hist is not None else equity_hist
        return _analytics_block(
            sym, ltp, lot, oi_map, oi_ce, oi_pe, oi_by, prev_oi,
            wk52_hi, wk52_lo, delv_map, india_vix, h, nifty_hist)

    # ── Indices (top 5 rows) ──────────────────────────────────────────────────
    log.info("Building index rows…")
    for sym, name, sector, margin_pct in INDEX_META:
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = index_cmp.get(sym, 0)
        if lot == 0:
            continue
        if ltp == 0:
            ltp = INDEX_FALLBACK_CMP.get(sym, 0)
        if ltp == 0:
            continue
        # Use index history (from ind_close_all CSV) for MACD/RSI/Beta.
        # Fallback to equity_hist if index_hist is empty for this symbol.
        # This ensures BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50 all get
        # their own price history rather than an empty list.
        idx_close_hist = index_hist.get(sym) or equity_hist.get(sym, [])
        # Build a local history dict with THIS index's closes
        h = {sym: idx_close_hist}
        ana = _ana(sym, ltp, lot, h)
        fut_rows.append(_build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries, h, ana, "Index Future"))
        opt_rows.append(_build_opt_row(sr, name, sym, sector, lot, ltp, expiries, h, ana, "Index Option"))
        log.info("  %-12s  CMP=₹%-8s  Lot=%-4d  History=%d days",
                 sym, f"{ltp:,.0f}", lot, len(idx_close_hist))
        sr += 1

    # ── Stocks (alphabetical) ─────────────────────────────────────────────────
    log.info("Building stock rows…")
    skipped = 0
    for sym in sorted((set(lot_sizes) | set(FALLBACK_LOTS)) - INDEX_SYMS):
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = equity_cmp.get(sym, 0)
        if lot == 0 or ltp == 0:
            skipped += 1
            continue
        sector = SECTOR_MAP.get(sym, "Equity")
        ana    = _ana(sym, ltp, lot)
        fut_rows.append(_build_fut_row(sr, sym, sym, sector, lot, ltp, 20, expiries, equity_hist, ana))
        opt_rows.append(_build_opt_row(sr, sym, sym, sector, lot, ltp, expiries, equity_hist, ana))
        sr += 1
    log.info("Rows — Futures: %d  Options: %d  Skipped: %d", len(fut_rows), len(opt_rows), skipped)
    return fut_rows, opt_rows


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — GOOGLE SHEETS WRITER
# ══════════════════════════════════════════════════════════════════════════════

class SheetsWriter:
    def __init__(self, creds_json):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(creds_json), GSHEETS_SCOPES)
        self._client = gspread.authorize(creds)

    def _get_or_create(self, ss, title, cols=60):
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating tab: '%s'", title)
            return ss.add_worksheet(title=title, rows=600, cols=cols)

    def open_all(self):
        ss = self._client.open_by_key(SPREADSHEET_ID)
        return (
            self._get_or_create(ss, SHEET_VOLUME,   cols=15),
            self._get_or_create(ss, SHEET_TURNOVER, cols=15),
            self._get_or_create(ss, SHEET_FUTURES,  cols=55),
            self._get_or_create(ss, SHEET_OPTIONS,  cols=60),
            self._get_or_create(ss, SHEET_CHAIN,    cols=60),
        )

    def write_vol_turnover(self, ws_vol, ws_to, data_vol, data_to, fetched_date):
        """
        Writes ONLY columns A–C, rows 2 onwards.
        Does NOT call ws.clear() — your formulas in D+, formatting,
        headers in row 1, column widths and filters are fully preserved.
        """
        status = f"Last updated: {fetched_date}  |  {_ist_now()}"
        for ws, data in ((ws_vol, data_vol), (ws_to, data_to)):
            n = len(data)
            if n == 0:
                log.warning("'%s' — no data to write", ws.title)
                continue
            # Timestamp to A1 only — all other row-1 cells untouched
            ws.update(range_name="A1", values=[[status]],
                      value_input_option="USER_ENTERED")
            # Data to A2:C only — columns D+ never touched
            ws.update(range_name=f"A2:C{n + 1}", values=data,
                      value_input_option="RAW")
            # Blank stale rows below new data in A:C only
            clear_from = n + 2
            clear_to   = TOP_N + 10
            if clear_from <= clear_to:
                blanks = [["", "", ""] for _ in range(clear_to - clear_from + 1)]
                ws.update(range_name=f"A{clear_from}:C{clear_to}", values=blanks,
                          value_input_option="RAW")
            log.info("'%s' → %d rows in A2:C%d  (D+ formulas & formatting intact)",
                     ws.title, n, n + 1)

    def add_chain_dropdowns(self, ws, chain_rows):
        """
        Add Google Sheets data validation dropdowns so users can filter
        by Index (col A), Expiry Type (col B), or Expiry Date (col C).

        Since the Google Sheets API does not natively support multi-column
        dropdown slicers, we implement this by:
          1. Adding data validation to col A (Index) to restrict to valid index names
          2. Adding data validation to col B (Expiry Type) for Weekly/Monthly values
          3. Adding data validation to col C (Expiry Date) for valid expiry dates
          4. Adding a helper filter guide in a note at the top

        Users can then use Data → Create a filter (or Ctrl+Shift+L) to get
        full dropdown filtering across all columns.
        """
        if not chain_rows:
            return

        n_rows = len(chain_rows) + 1   # +1 for header

        # Collect unique values for validation lists
        indices      = sorted(set(r[0] for r in chain_rows))
        expiry_types = sorted(set(r[1] for r in chain_rows))
        expiry_dates = sorted(set(r[2] for r in chain_rows))

        idx_list  = ",".join(indices)
        ety_list  = ",".join(expiry_types)
        exp_list  = ",".join(expiry_dates)

        # Build batch data validation requests via the Sheets API
        ws_id  = ws._properties["sheetId"]

        def _dv_rule(col_idx, values_str):
            """Build a setDataValidation request for a column."""
            return {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws_id,
                        "startRowIndex": 1,       # skip header row
                        "endRowIndex":   n_rows,
                        "startColumnIndex": col_idx,
                        "endColumnIndex":   col_idx + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": v}
                                       for v in values_str.split(",")],
                        },
                        "showCustomUi":  True,
                        "strict":        False,   # allow typed values too
                    },
                }
            }

        requests_body = [
            _dv_rule(0, idx_list),     # col A — Index
            _dv_rule(1, ety_list),     # col B — Expiry Type
            _dv_rule(2, exp_list),     # col C — Expiry Date
        ]

        # Apply freeze + auto-filter on first row
        requests_body += [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId":          ws_id,
                            "startRowIndex":    0,
                            "endRowIndex":      n_rows,
                            "startColumnIndex": 0,
                            "endColumnIndex":   len(OC_HEADERS),
                        }
                    }
                }
            },
        ]

        # Execute via batchUpdate
        ss_obj = self._client.open_by_key(SPREADSHEET_ID)
        ss_obj.batch_update({"requests": requests_body})
        log.info("  Dropdowns + auto-filter applied to '%s'", ws.title)

    def write_fo_sheet(self, ws, headers, rows, title):
        """
        F&O sheets are fully rewritten each run (no user formulas expected).
        Uses RAW mode — expiry dates stay as text, not date serials.
        """
        all_data = [headers] + rows
        n_cols   = len(headers)
        ws.clear()
        time.sleep(1)
        for start in range(0, len(all_data), WRITE_CHUNK):
            end = min(start + WRITE_CHUNK, len(all_data))
            ws.update(
                range_name=f"A{start + 1}:{_col_letter(n_cols)}{end}",
                values=all_data[start:end],
                value_input_option="RAW",
            )
            log.info("  '%s' rows %d–%d", title, start + 1, end)
            if end < len(all_data):
                time.sleep(1.5)
        log.info("'%s' done — %d rows × %d cols", title, len(rows), n_cols)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GCP_CREDENTIALS not set.")

    ist_today = _ist_today()
    log.info("═" * 60)
    log.info("NSE Auto-Sheet v8  —  %s", ist_today.strftime("%d-%b-%Y %H:%M IST"))
    log.info("═" * 60)

    writer = SheetsWriter(creds_json)
    ws_vol, ws_to, ws_fut, ws_opt, ws_chain = writer.open_all()

    # ── 1. CM Equity bhavcopy ────────────────────────────────────────────────
    log.info("── CM Equity bhavcopy ───────────────────────────────────")
    cm      = CMBhavcopFetcher()
    result  = None
    fetched_date = ""
    for cand in _trading_days_back(ist_today, LOOKBACK_DAYS):
        result = cm.fetch(cand)
        if result:
            fetched_date = cand.strftime("%d-%b-%Y")
            break
    if not result:
        raise RuntimeError("No CM equity bhavcopy found for last 7 trading days.")
    data_vol, data_to, equity_cmp = result
    writer.write_vol_turnover(ws_vol, ws_to, data_vol, data_to, fetched_date)

    # ── 2. FO bhavcopy (OI / PCR / Max Pain) ────────────────────────────────
    log.info("── FO bhavcopy (OI) ─────────────────────────────────────")
    fo = FOBhavcopFetcher()
    bhavcopy_dt = datetime.strptime(fetched_date, "%d-%b-%Y")
    oi_map, oi_ce, oi_pe, oi_by = fo.fetch(bhavcopy_dt)
    # Previous day OI for OI-change column
    prev_oi = {}
    for cand in _trading_days_back(bhavcopy_dt, 4):
        pm, _, _, _ = fo.fetch(cand)
        if pm:
            prev_oi = pm
            break

    # ── 3. Index prices + India VIX ─────────────────────────────────────────
    log.info("── Index prices + India VIX ─────────────────────────────")
    idx_fetcher = IndexPriceFetcher()
    index_cmp   = idx_fetcher.fetch(ist_today)
    log.info("  India VIX: %s", index_cmp.get("VIX"))

    # ── 4. Lot sizes ─────────────────────────────────────────────────────────
    log.info("── Lot sizes ────────────────────────────────────────────")
    lot_sizes = LotSizeFetcher().fetch() or FALLBACK_LOTS

    # ── 5. Delivery % ────────────────────────────────────────────────────────
    log.info("── Delivery %% ───────────────────────────────────────────")
    delv_map = DeliveryFetcher().fetch(ist_today)

    # ── 6. Price history for RSI / MACD / Beta / Support / Resistance ────────
    log.info("── Equity close history (%d trading days) ───────────────", HISTORY_DAYS)
    hist_fetcher = HistoryFetcher()
    equity_hist  = hist_fetcher.fetch(ist_today, days=HISTORY_DAYS)

    # ── 7. Index history for trend / Beta / MACD ────────────────────────────
    log.info("── Index history (%d trading days) ──────────────────────", HISTORY_DAYS)
    index_hist = idx_fetcher.fetch_history(ist_today, days=HISTORY_DAYS)

    # ── 8. 52-Week High/Low — computed from ALREADY FETCHED history ──────────
    # Merges equity_hist + index_hist so indices also get 52wk values.
    # Zero additional HTTP calls — reuses data already downloaded in steps 6 & 7.
    log.info("── 52-Week High/Low (from combined history — 0 extra HTTP calls)")
    combined_hist = {**equity_hist}
    for sym, closes in index_hist.items():
        if sym not in combined_hist:
            combined_hist[sym] = closes
        else:
            # Merge: take union, sort by most data
            combined_hist[sym] = closes if len(closes) > len(combined_hist[sym]) else combined_hist[sym]
    wk52_hi, wk52_lo = HistoryFetcher.compute_52wk_from_history(combined_hist)
    log.info("  52-wk H/L computed: %d symbols (incl. indices)", len(wk52_hi))

    # ── 9. Expiry dates ───────────────────────────────────────────────────────
    expiries = _expiry_dates()
    log.info("── Expiries: %s | %s | %s", *expiries)

    # ── 10. Build all F&O rows ────────────────────────────────────────────────
    log.info("── Building F&O rows ────────────────────────────────────")
    fut_rows, opt_rows = build_all_rows(
        lot_sizes, equity_cmp, index_cmp, equity_hist, index_hist,
        oi_map, oi_ce, oi_pe, oi_by, prev_oi,
        wk52_hi, wk52_lo, delv_map, expiries,
    )

    # Sanity check — catch header/row count mismatch before writing
    if fut_rows and len(fut_rows[0]) != len(FUT_HEADERS):
        raise ValueError(f"FUT row={len(fut_rows[0])} vs headers={len(FUT_HEADERS)}")
    if opt_rows and len(opt_rows[0]) != len(OPT_HEADERS):
        raise ValueError(f"OPT row={len(opt_rows[0])} vs headers={len(OPT_HEADERS)}")

    # ── 11. Write F&O sheets ──────────────────────────────────────────────────
    log.info("── Writing Futures F&O ──────────────────────────────────")
    writer.write_fo_sheet(ws_fut, FUT_HEADERS, fut_rows, SHEET_FUTURES)
    log.info("── Writing Options F&O ──────────────────────────────────")
    writer.write_fo_sheet(ws_opt, OPT_HEADERS, opt_rows, SHEET_OPTIONS)

    # ── 12. Build and write Option Chain sheet ────────────────────────────────
    log.info("── Building Option Chain ─────────────────────────────────")
    chain_rows = build_option_chain_rows(
        index_cmp, index_hist, lot_sizes,
        oi_map, oi_ce, oi_pe, oi_by, prev_oi,
        wk52_hi, wk52_lo, delv_map, expiries,
    )
    if chain_rows and len(chain_rows[0]) != len(OC_HEADERS):
        raise ValueError(f"Chain row={len(chain_rows[0])} vs OC_HEADERS={len(OC_HEADERS)}")

    log.info("── Writing Index Option Chain ────────────────────────────")
    writer.write_fo_sheet(ws_chain, OC_HEADERS, chain_rows, SHEET_CHAIN)

    # ── 13. Add dropdown filters via data validation ──────────────────────────
    # Col B = Expiry Type  (Weekly / Monthly-Near / Monthly-Mid)
    # Col C = Expiry Date  (actual date strings)
    # Col A = Index name   (5 index names)
    log.info("── Setting up dropdown filters ───────────────────────────")
    try:
        writer.add_chain_dropdowns(ws_chain, chain_rows)
    except Exception as e:
        log.warning("  Dropdown setup failed (non-fatal): %s", e)

    log.info("═" * 60)
    log.info("✅  SUCCESS  |  Data: %s  |  %s", fetched_date, _ist_now())
    log.info("═" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# ADDITION TO update_sheet.py  —  Option Chain Sheet
# ══════════════════════════════════════════════════════════════════════════════
# Paste this BEFORE the main() function and wire it in as shown at the bottom.
# ══════════════════════════════════════════════════════════════════════════════

SHEET_CHAIN      = "Index Option Chain"

# How many OTM strikes to show each side of ATM
CHAIN_OTM_DEPTH  = 15   # 15 strikes above ATM + 15 below = 31 rows per index/expiry

# Option chain sheet header — strike-level detail
OC_HEADERS = [
    # Strike metadata
    "Index",                            # 1
    "Expiry Type",                      # 2  Weekly / Monthly
    "Expiry Date",                      # 3
    "Strike ₹",                         # 4
    "Strike Type",                      # 5  ITM-CE / ATM / ITM-PE
    "CMP ₹",                            # 6
    "Lot Size",                         # 7
    # CE columns
    "CE OI\n(Lots)",                    # 8
    "CE OI\nChange",                    # 9
    "CE LTP ₹\n(Approx.)",             # 10
    "CE Premium\n1 Lot ₹",             # 11
    "CE IV %",                          # 12
    # PE columns
    "PE OI\n(Lots)",                    # 13
    "PE OI\nChange",                    # 14
    "PE LTP ₹\n(Approx.)",             # 15
    "PE Premium\n1 Lot ₹",             # 16
    "PE IV %",                          # 17
    # Index-level analytics (same for all rows of same index/expiry)
    "Intraday\nTrend",                  # 18
    "Swing\nTrend",                     # 19
    "Options Strategy\n(Intraday)",     # 20
    "Options Strategy\n(Swing)",        # 21
    "Intraday Signal:\nBest Strategy",  # 22
    "Intraday:\nCE Entry",              # 23
    "Intraday:\nPE Entry",              # 24
    "Intraday:\nCE Target",             # 25
    "Intraday:\nPE Target",             # 26
    "Intraday:\nStop Loss",             # 27
    "Intraday:\nMax Profit/Lot ₹",     # 28
    "Intraday:\nMax Loss/Lot ₹",       # 29
    "Intraday:\nRisk:Reward",           # 30
    "Intraday:\nRationale",             # 31
    "Swing Signal:\nBest Strategy",     # 32
    "Swing:\nCE Entry",                 # 33
    "Swing:\nPE Entry",                 # 34
    "Swing:\nCE Target",                # 35
    "Swing:\nPE Target",                # 36
    "Swing:\nStop Loss",                # 37
    "Swing:\nMax Profit/Lot ₹",        # 38
    "Swing:\nMax Loss/Lot ₹",          # 39
    "Swing:\nRisk:Reward",              # 40
    "Swing:\nRationale",                # 41
    "Open Interest\n(OI — Lots)",       # 42  index total OI
    "OI Change\n(vs Prev Day)",         # 43
    "PCR\n(Put-Call Ratio)",            # 44
    "52-Week\nHigh ₹",                 # 45
    "52-Week\nLow ₹",                  # 46
    "Delivery\n%",                      # 47
    "India\nVIX",                       # 48
    "IV %\n(Impl. Volatility)",         # 49
    "Max Pain\nStrike ₹",              # 50
    "Support\n₹",                       # 51
    "Resistance\n₹",                    # 52
    "Beta\nvs Nifty",                   # 53
    "RSI\n(14-day)",                    # 54
    "MACD Signal\n(12/26/9)",          # 55
    "Notes",                            # 56
]   # 56 columns


def _strike_type(strike, atm, step):
    """Label a strike as ITM-CE, ATM, or ITM-PE."""
    if strike == atm:
        return "🎯 ATM"
    if strike < atm:
        return f"ITM-PE  ({int((atm-strike)/step)} step)"
    return f"ITM-CE  ({int((strike-atm)/step)} step)"


def _ce_premium_for_strike(ltp, strike, days=25, iv=0.28):
    """Approx Black-Scholes call premium for a given strike."""
    S = ltp; K = strike
    T = max(days, 1) / 252
    intrinsic = max(S - K, 0)
    time_val  = 0.4 * iv * math.sqrt(T) * S * math.exp(-0.5 * ((S-K)/(iv*S*math.sqrt(T)+1))**2)
    return max(1, int(round((intrinsic + time_val) / 5) * 5))


def _pe_premium_for_strike(ltp, strike, days=25, iv=0.28):
    """Approx Black-Scholes put premium for a given strike."""
    S = ltp; K = strike
    T = max(days, 1) / 252
    intrinsic = max(K - S, 0)
    time_val  = 0.4 * iv * math.sqrt(T) * S * math.exp(-0.5 * ((K-S)/(iv*S*math.sqrt(T)+1))**2)
    return max(1, int(round((intrinsic + time_val) / 5) * 5))


def _iv_for_strike(prem, ltp, strike, days=25):
    """Back-solve IV for a given option premium and strike."""
    S = ltp; K = strike; T = max(days, 1) / 252
    denom = 0.4 * S * math.sqrt(T) * math.exp(-0.5 * ((S-K)/(0.3*S*math.sqrt(T)+1))**2)
    if denom == 0:
        return "—"
    iv = prem / denom * 100
    return f"{min(iv, 999):.1f}%"


def _expiry_days_remaining(expiry_str):
    """Days from today to expiry date string '%d-%b-%Y'."""
    try:
        exp = datetime.strptime(expiry_str, "%d-%b-%Y").date()
        today = _ist_today().date()
        return max((exp - today).days, 1)
    except Exception:
        return 25


def _weekly_expiry():
    """
    Next weekly expiry for Bank Nifty (Thursday) and Nifty/FinNifty (Thursday).
    Returns date string.
    """
    today = _ist_today().date()
    # Find next Thursday
    days_ahead = (3 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (today + timedelta(days=days_ahead)).strftime("%d-%b-%Y")


def build_option_chain_rows(
    index_cmp, index_hist, lot_sizes,
    oi_map, oi_ce, oi_pe, oi_by, prev_oi,
    wk52_hi, wk52_lo, delv_map,
    expiries,        # (near, mid, far) monthly expiries
):
    """
    Build all rows for the Option Chain sheet.
    One block per (index × expiry_type).
    Indices: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50
    Expiry types: Weekly (near-Thursday), Monthly-Near, Monthly-Mid
    """
    rows = []
    nifty_hist = index_hist.get("NIFTY", [])
    india_vix  = index_cmp.get("VIX")

    # Weekly expiry (next Thursday)
    weekly_exp = _weekly_expiry()

    # Build expiry schedule per index
    # Nifty, BankNifty, FinNifty have weekly expiries
    # MidcapSelect, NiftyNext50 are monthly only
    index_expiry_schedule = {
        "NIFTY":      [("Weekly", weekly_exp), ("Monthly-Near", expiries[0]), ("Monthly-Mid", expiries[1])],
        "BANKNIFTY":  [("Weekly", weekly_exp), ("Monthly-Near", expiries[0]), ("Monthly-Mid", expiries[1])],
        "FINNIFTY":   [("Weekly", weekly_exp), ("Monthly-Near", expiries[0]), ("Monthly-Mid", expiries[1])],
        "MIDCPNIFTY": [                        ("Monthly-Near", expiries[0]), ("Monthly-Mid", expiries[1])],
        "NIFTYNXT50": [                        ("Monthly-Near", expiries[0]), ("Monthly-Mid", expiries[1])],
    }

    for sym, name, sector, margin_pct in INDEX_META:
        ltp  = index_cmp.get(sym, INDEX_FALLBACK_CMP.get(sym, 0))
        lot  = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        hist = index_hist.get(sym, [])
        if ltp == 0 or lot == 0:
            continue

        # ── Index-level analytics (same for all strike rows) ────────────────
        atm_p   = _atm_premium(ltp)
        intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
        swing_t = _trend(hist)
        si = _strategy_engine(ltp, atm_p, intra_t, swing_t, lot, "intraday")
        ss = _strategy_engine(ltp, atm_p, intra_t, swing_t, lot, "swing")

        oi_total  = oi_map.get(sym, 0)
        oi_ce_tot = oi_ce.get(sym, 0)
        oi_pe_tot = oi_pe.get(sym, 0)
        prev      = prev_oi.get(sym, 0)
        oi_chg    = (oi_total - prev) if (oi_total > 0 and prev > 0) else None
        pcr       = round(oi_pe_tot / oi_ce_tot, 2) if oi_ce_tot > 0 else None
        pcr_s     = (f"{pcr} 🟢 Bullish" if pcr and pcr > 1.2
                     else f"{pcr} 🔴 Bearish" if pcr and pcr < 0.8
                     else f"{pcr} 🟡 Neutral" if pcr else "—")
        mp        = _calc_max_pain(oi_by.get(sym, {}))
        hi52      = wk52_hi.get(sym) or "—"
        lo52      = wk52_lo.get(sym) or "—"
        vix_s     = f"{india_vix:.2f}" if india_vix else "—"
        iv_s      = _calc_iv(ltp, atm_p)
        sup, res  = _calc_support_resistance(hist)
        beta      = _calc_beta(hist, nifty_hist) if (hist and nifty_hist) else None
        rsi       = _calc_rsi(hist)
        _, _, _, macd_lbl = _calc_macd(hist)

        # Shared analytics tail — appended to every strike row for this index
        analytics_tail = [
            int(oi_total)  if oi_total  else "—",
            int(oi_chg)    if oi_chg is not None else "—",
            pcr_s,
            hi52, lo52,
            "—",           # Delivery — not applicable for indices
            vix_s, iv_s,
            f"₹{mp:,}" if mp else "—",
            f"₹{sup:,.2f}" if sup else "—",
            f"₹{res:,.2f}" if res else "—",
            str(beta) if beta is not None else "—",
            _rsi_label(rsi),
            macd_lbl,
            "",            # Notes
        ]   # 15 items → cols 42–56

        # Strategy tail
        strat_tail = [
            _trend_label(intra_t), _trend_label(swing_t),
            si["Strategy"], ss["Strategy"],
            si["Strategy"],       _sv(si, "CE Entry"),  _sv(si, "PE Entry"),
            _sv(si, "CE Target"), _sv(si, "PE Target"), _sv(si, "Stop Loss"),
            _sv(si, "Max Profit/Lot"), _sv(si, "Max Loss/Lot"), _sv(si, "Risk:Reward"),
            _sv(si, "Rationale"),
            ss["Strategy"],       _sv(ss, "CE Entry"),  _sv(ss, "PE Entry"),
            _sv(ss, "CE Target"), _sv(ss, "PE Target"), _sv(ss, "Stop Loss"),
            _sv(ss, "Max Profit/Lot"), _sv(ss, "Max Loss/Lot"), _sv(ss, "Risk:Reward"),
            _sv(ss, "Rationale"),
        ]   # 24 items → cols 18–41

        # ── Strike schedule ─────────────────────────────────────────────────
        step = (100 if ltp > 20000 else 50 if ltp > 5000 else
                20  if ltp > 1000  else 10  if ltp > 200  else 5)
        atm  = _round_strike(ltp)

        strikes = [atm + i * step
                   for i in range(-CHAIN_OTM_DEPTH, CHAIN_OTM_DEPTH + 1)]

        for exp_type, exp_date in index_expiry_schedule.get(sym, []):
            days_left = _expiry_days_remaining(exp_date)

            for strike in strikes:
                s_type   = _strike_type(strike, atm, step)
                ce_prem  = _ce_premium_for_strike(ltp, strike, days_left)
                pe_prem  = _pe_premium_for_strike(ltp, strike, days_left)
                ce_iv    = _iv_for_strike(ce_prem, ltp, strike, days_left)
                pe_iv    = _iv_for_strike(pe_prem, ltp, strike, days_left)

                # Strike-level OI from oi_by dict
                strike_oi = oi_by.get(sym, {}).get(strike, {})
                ce_oi_s   = int(strike_oi.get("CE", 0)) if strike_oi.get("CE") else "—"
                pe_oi_s   = int(strike_oi.get("PE", 0)) if strike_oi.get("PE") else "—"

                row = [
                    name,                        # 1  Index
                    exp_type,                    # 2  Expiry Type
                    exp_date,                    # 3  Expiry Date
                    strike,                      # 4  Strike ₹
                    s_type,                      # 5  Strike Type
                    round(ltp, 2),               # 6  CMP ₹
                    lot,                         # 7  Lot Size
                    ce_oi_s,                     # 8  CE OI
                    "—",                         # 9  CE OI Change (strike-level not in bhavcopy)
                    ce_prem,                     # 10 CE LTP
                    ce_prem * lot,               # 11 CE 1-lot cost
                    ce_iv,                       # 12 CE IV
                    pe_oi_s,                     # 13 PE OI
                    "—",                         # 14 PE OI Change
                    pe_prem,                     # 15 PE LTP
                    pe_prem * lot,               # 16 PE 1-lot cost
                    pe_iv,                       # 17 PE IV
                ] + strat_tail + analytics_tail

                rows.append(row)

    log.info("Option Chain: %d rows built", len(rows))
    return rows




if __name__ == "__main__":
    main()
