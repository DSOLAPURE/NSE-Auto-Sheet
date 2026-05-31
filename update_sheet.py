"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NSE F&O Auto-Sheet  —  update_sheet.py  (v6 — Full Analytics Edition)     ║
║  GitHub: DSOLAPURE/NSE-Auto-Sheet                                           ║
║                                                                              ║
║  Sheets updated:                                                             ║
║    1. Top 250 Stocks    — top 250 NSE equities by trading volume            ║
║    2. Top 250 Turnover  — top 250 NSE equities by turnover value            ║
║    3. Futures F&O       — 5 indices + all F&O stocks (48 columns)           ║
║    4. Options F&O       — 5 indices + all F&O stocks (52 columns)           ║
║                                                                              ║
║  NEW in v6 — 12 analytics columns added to both F&O sheets:                ║
║    • Open Interest (OI)       — from NSE FO bhavcopy ZIP                   ║
║    • OI Change                — vs previous trading day                     ║
║    • PCR (Put-Call Ratio)     — computed from CE+PE OI                     ║
║    • 52-Week High / Low       — from NSE 52wk CSV                          ║
║    • Delivery %               — from NSE CM bhavcopy                       ║
║    • India VIX                — from NSE index CSV                          ║
║    • IV % (Implied Volatility)— back-solved from ATM premium               ║
║    • Max Pain Strike          — from OI distribution                        ║
║    • Support / Resistance     — computed: recent swing lows/highs           ║
║    • Beta vs Nifty            — computed from 20-day returns                ║
║    • RSI (14-day)             — Wilder RSI from close history               ║
║    • MACD Signal              — 12/26/9 EMA MACD                           ║
║                                                                              ║
║  Data sources:                                                               ║
║    • Equity CMP/Vol/Delivery → NSE UDiFF bhavcopy ZIP (daily)              ║
║    • FO OI data              → NSE FO bhavcopy ZIP (daily)                  ║
║    • Index CMP / India VIX   → NSE ind_close_all_{date}.csv                ║
║    • 52-Week High/Low        → NSE 52wk CSV                                ║
║    • Lot sizes               → NSE fo_mktlots.csv                          ║
║    • All indicators          → computed from 20-day price history           ║
║                                                                              ║
║  Runs: Mon–Fri 06:30 IST + 16:00 IST via GitHub Actions                   ║
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
STATUS_CELL     = "K2"
TOP_N           = 250
LOOKBACK_DAYS   = 7
REQUEST_TIMEOUT = 25
MAX_RETRIES     = 3
RETRY_DELAY     = 5
HISTORY_DAYS    = 20          # need 20 days for MACD (26 EMA)
WRITE_CHUNK     = 100         # smaller chunks — more columns now
EXCLUDE_PATTERN = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

# ── NSE URLs ─────────────────────────────────────────────────────────────────
BHAVCOPY_URL   = ("https://nsearchives.nseindia.com/content/cm/"
                  "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip")
FO_BHAV_URL    = ("https://nsearchives.nseindia.com/content/fo/"
                  "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip")
INDEX_CSV_URL  = ("https://nsearchives.nseindia.com/content/indices/"
                  "ind_close_all_{date}.csv")
WEEK52_URL     = "https://archives.nseindia.com/content/equities/52_wk_high_low.csv"
MKTLOTS_URL    = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

COL_MAP = {
    "symbol":   ["TckrSymb",    "SYMBOL"],
    "close":    ["ClsPric",     "CLOSE"],
    "series":   ["SctySrs",     "SERIES"],
    "volume":   ["TtlTradgVol", "TOTTRDQTY", "TtlTrdQty",  "TotTrdQty"],
    "turnover": ["TtlTrfVal",   "TOTTRDVAL", "TtlTrdVal",  "TotTrdVal"],
    "delivery": ["DlvrQty",     "DELQTY",    "DeliveryQty"],
    "delv_pct": ["DlvrPct",     "DELPCT",    "DeliveryPct", "%Dly Qt to Traded Qty"],
}

GSHEETS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

NSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Referer":        "https://www.nseindia.com/",
    "Accept-Language":"en-US,en;q=0.9",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
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
    "Nifty 50":"NIFTY","Nifty Bank":"BANKNIFTY",
    "Nifty Financial Services":"FINNIFTY","Nifty Fin Services":"FINNIFTY",
    "Nifty Midcap Select":"MIDCPNIFTY","Nifty Next 50":"NIFTYNXT50",
    "India Vix":"VIX","India VIX":"VIX",
    "NIFTY 50":"NIFTY","NIFTY BANK":"BANKNIFTY",
    "NIFTY FINANCIAL SERVICES":"FINNIFTY","NIFTY MIDCAP SELECT":"MIDCPNIFTY",
    "NIFTY NEXT 50":"NIFTYNXT50","INDIA VIX":"VIX",
}

INDEX_FALLBACK_CMP = {
    "NIFTY":24500.0,"BANKNIFTY":52000.0,"FINNIFTY":23800.0,
    "MIDCPNIFTY":12400.0,"NIFTYNXT50":67000.0,
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
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} in {list(df.columns)}")

def _ist_now():
    return (datetime.utcnow()+timedelta(hours=5,minutes=30)).strftime("%d-%b-%Y %H:%M IST")

def _col_letter(n):
    r=""
    while n>0:
        n,rem=divmod(n-1,26)
        r=chr(65+rem)+r
    return r

def _last_thursday_of_month(year, month):
    if month==12:
        last=date(year+1,1,1)-timedelta(days=1)
    else:
        last=date(year,month+1,1)-timedelta(days=1)
    return last-timedelta(days=(last.weekday()-3)%7)

def _expiry_dates():
    today=(datetime.utcnow()+timedelta(hours=5,minutes=30)).date()
    expiries,m,y=[],today.month,today.year
    for _ in range(6):
        exp=_last_thursday_of_month(y,m)
        if exp>=today:
            expiries.append(exp.strftime("%d-%b-%Y"))
        if len(expiries)==3: break
        m+=1
        if m>12: m,y=1,y+1
    while len(expiries)<3: expiries.append("—")
    return tuple(expiries)

def _download_raw(url, label=""):
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r=requests.get(url,headers=NSE_HEADERS,timeout=REQUEST_TIMEOUT)
            if r.status_code==200: return r.content
            log.warning("%s HTTP %s (attempt %d/%d)",label,r.status_code,attempt,MAX_RETRIES)
        except requests.RequestException as e:
            log.warning("%s error attempt %d/%d: %s",label,attempt,MAX_RETRIES,e)
        if attempt<MAX_RETRIES: time.sleep(RETRY_DELAY)
    log.error("%s failed — %s",label,url)
    return None

def _atm_premium(ltp, days=25, iv=0.28):
    prem=0.4*iv*math.sqrt(max(days,1)/252)*ltp
    return max(5,int(round(prem/5)*5))

def _trend(closes):
    if len(closes)<3: return "Sideways"
    n=len(closes)
    avg5=sum(closes[-min(5,n):])/min(5,n)
    avg20=sum(closes[-min(20,n):])/min(20,n)
    if avg20==0: return "Sideways"
    diff=(avg5-avg20)/avg20*100
    if diff>0.8:  return "Bullish"
    if diff<-0.8: return "Bearish"
    return "Sideways"

def _trend_label(t):
    return {"Bullish":"🟢 Bullish","Bearish":"🔴 Bearish"}.get(t,"🟡 Sideways")

def _round_strike(ltp):
    if ltp>20000: step=100
    elif ltp>5000: step=50
    elif ltp>1000: step=20
    elif ltp>200:  step=10
    else:          step=5
    return int(round(ltp/step)*step)

def _na(v, fmt=None):
    """Return formatted value or '—' if falsy/zero."""
    if v is None or v!=v:  # NaN check
        return "—"
    if isinstance(v,(int,float)) and v==0:
        return "—"
    if fmt=="pct":   return f"{v:.1f}%"
    if fmt=="2f":    return f"{v:.2f}"
    if fmt=="int":   return int(round(v))
    if fmt=="cr":    return f"₹{v/1e7:.2f} Cr"
    return v


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — ANALYTICS CALCULATORS
# ══════════════════════════════════════════════════════════════════════════════

def _calc_rsi(closes, period=14):
    """Wilder RSI from close price list (oldest→newest). Returns float or None."""
    if len(closes) < period+1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        chg = closes[i] - closes[i-1]
        gains.append(max(chg, 0))
        losses.append(max(-chg, 0))
    # Wilder smoothing: use last `period` values
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return round(100 - (100 / (1 + rs)), 1)

def _rsi_label(rsi):
    if rsi is None: return "—"
    if rsi >= 70:   return f"{rsi} 🔴 Overbought"
    if rsi <= 30:   return f"{rsi} 🟢 Oversold"
    return f"{rsi} 🟡 Neutral"

def _calc_ema(closes, period):
    """Exponential Moving Average. Returns list same length as closes."""
    if not closes: return []
    k = 2 / (period + 1)
    ema = [closes[0]]
    for p in closes[1:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

def _calc_macd(closes):
    """
    Standard 12/26/9 MACD.
    Returns (macd_line, signal_line, histogram, label_str) or (None,None,None,'—').
    """
    if len(closes) < 27:
        return None, None, None, "—"
    ema12 = _calc_ema(closes, 12)
    ema26 = _calc_ema(closes, 26)
    macd_line  = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    signal     = _calc_ema(macd_line, 9)
    histogram  = [m - s for m, s in zip(macd_line, signal)]
    m  = round(macd_line[-1], 2)
    s  = round(signal[-1], 2)
    h  = round(histogram[-1], 2)
    if h > 0 and macd_line[-1] > 0:
        label = f"🟢 Bullish  MACD={m}  Sig={s}  Hist=+{h}"
    elif h > 0:
        label = f"🟡 Recovering  MACD={m}  Sig={s}  Hist=+{h}"
    elif h < 0 and macd_line[-1] < 0:
        label = f"🔴 Bearish  MACD={m}  Sig={s}  Hist={h}"
    else:
        label = f"🟡 Weakening  MACD={m}  Sig={s}  Hist={h}"
    return m, s, h, label

def _calc_beta(stock_closes, nifty_closes):
    """Beta of stock vs Nifty from daily return series."""
    n = min(len(stock_closes), len(nifty_closes))
    if n < 5:
        return None
    s_ret = [(stock_closes[i]-stock_closes[i-1])/stock_closes[i-1]
              for i in range(1, n)]
    n_ret = [(nifty_closes[i]-nifty_closes[i-1])/nifty_closes[i-1]
              for i in range(1, n)]
    if len(s_ret) < 4: return None
    mean_s = sum(s_ret)/len(s_ret)
    mean_n = sum(n_ret)/len(n_ret)
    cov = sum((s-mean_s)*(ni-mean_n) for s,ni in zip(s_ret,n_ret))/len(s_ret)
    var_n = sum((ni-mean_n)**2 for ni in n_ret)/len(n_ret)
    if var_n == 0: return None
    return round(cov/var_n, 2)

def _calc_support_resistance(closes):
    """
    Simple support/resistance from recent price history.
    Support  = lowest low of last 10 sessions.
    Resistance = highest high of last 10 sessions.
    Returns (support, resistance) as rounded floats.
    """
    if len(closes) < 5:
        return None, None
    window = closes[-min(10, len(closes)):]
    support    = round(min(window), 2)
    resistance = round(max(window), 2)
    return support, resistance

def _calc_iv(ltp, atm_prem, days=25):
    """
    Back-solve IV from ATM premium using Black-Scholes approximation:
      IV ≈ premium / (0.4 × S × √(T/252))
    Returns IV as percentage string.
    """
    denom = 0.4 * ltp * math.sqrt(max(days,1)/252)
    if denom == 0: return "—"
    iv = atm_prem / denom * 100
    return f"{iv:.1f}%"

def _calc_max_pain(oi_by_strike):
    """
    Max Pain = strike at which total loss to option buyers is maximum.
    oi_by_strike: dict {strike: {"CE": oi, "PE": oi}}
    Returns max pain strike (int) or None.
    """
    if not oi_by_strike:
        return None
    strikes = sorted(oi_by_strike.keys())
    min_pain, max_pain_strike = float("inf"), strikes[0]
    for s_test in strikes:
        total_loss = 0
        for s_strike, data in oi_by_strike.items():
            # Call holders lose if test strike > strike (calls are ITM)
            if s_test > s_strike:
                total_loss += (s_test - s_strike) * data.get("CE", 0)
            # Put holders lose if test strike < strike (puts are ITM)
            if s_test < s_strike:
                total_loss += (s_strike - s_test) * data.get("PE", 0)
        if total_loss < min_pain:
            min_pain = total_loss
            max_pain_strike = s_test
    return max_pain_strike


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — STRATEGY ENGINE  (unchanged from v5)
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_engine(ltp, atm_prem, intra_trend, swing_trend, lot, timeframe="intraday"):
    trend=intra_trend if timeframe=="intraday" else swing_trend
    atm=_round_strike(ltp)
    if ltp>20000: step=100
    elif ltp>5000: step=50
    elif ltp>1000: step=20
    elif ltp>200: step=10
    else: step=5
    otm1=atm+step; otm2=atm+2*step; itm1=atm-step; itm2=atm-2*step
    p_atm=atm_prem
    p_otm1=max(5,int(p_atm*0.55/5)*5)
    p_otm2=max(5,int(p_atm*0.30/5)*5)

    if trend=="Bullish":
        if timeframe=="intraday":
            sl=max(5,int(p_atm*0.5))
            return {"Strategy":"Long Call","CE Entry":f"Buy {atm} CE @ ₹{p_atm}",
                    "PE Entry":"—","CE Target":f"₹{p_atm*2} (2× premium)","PE Target":"—",
                    "Stop Loss":f"₹{sl} (50% of premium)","Max Profit/Lot":f"₹{p_atm*2*lot:,}",
                    "Max Loss/Lot":f"₹{p_atm*lot:,}","Risk:Reward":"1:2",
                    "Rationale":f"Intraday bullish. Buy ATM {atm} CE @ ₹{p_atm}. Target ₹{p_atm*2}, SL ₹{sl}."}
        else:
            nd=p_atm-p_otm1; mg=(otm1-atm)-nd; rr=round(mg/max(nd,1),1)
            return {"Strategy":"Bull Call Spread",
                    "CE Entry":f"Buy {atm} CE @ ₹{p_atm} | Sell {otm1} CE @ ₹{p_otm1}",
                    "PE Entry":"—","CE Target":f"Close ≥ ₹{otm1} at expiry","PE Target":"—",
                    "Stop Loss":f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot":f"₹{mg*lot:,}","Max Loss/Lot":f"₹{nd*lot:,}",
                    "Risk:Reward":f"1:{rr}",
                    "Rationale":f"Swing bullish. Buy {atm} CE ₹{p_atm}, sell {otm1} CE ₹{p_otm1}. Net debit ₹{nd}."}
    elif trend=="Bearish":
        if timeframe=="intraday":
            sl=max(5,int(p_atm*0.5))
            return {"Strategy":"Long Put","CE Entry":"—",
                    "PE Entry":f"Buy {atm} PE @ ₹{p_atm}","CE Target":"—",
                    "PE Target":f"₹{p_atm*2} (2× premium)","Stop Loss":f"₹{sl} (50% of premium)",
                    "Max Profit/Lot":f"₹{p_atm*2*lot:,}","Max Loss/Lot":f"₹{p_atm*lot:,}",
                    "Risk:Reward":"1:2",
                    "Rationale":f"Intraday bearish. Buy ATM {atm} PE @ ₹{p_atm}. Target ₹{p_atm*2}, SL ₹{sl}."}
        else:
            nd=p_atm-p_otm1; mg=(atm-itm1)-nd; rr=round(mg/max(nd,1),1)
            return {"Strategy":"Bear Put Spread","CE Entry":"—",
                    "PE Entry":f"Buy {atm} PE @ ₹{p_atm} | Sell {itm1} PE @ ₹{p_otm1}",
                    "CE Target":"—","PE Target":f"Close ≤ ₹{itm1} at expiry",
                    "Stop Loss":f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot":f"₹{mg*lot:,}","Max Loss/Lot":f"₹{nd*lot:,}",
                    "Risk:Reward":f"1:{rr}",
                    "Rationale":f"Swing bearish. Buy {atm} PE ₹{p_atm}, sell {itm1} PE ₹{p_otm1}. Net debit ₹{nd}."}
    else:
        if timeframe=="intraday":
            cr=p_otm1*2; be_hi=otm1+cr; be_lo=itm1-cr
            return {"Strategy":"Short Strangle",
                    "CE Entry":f"Sell {otm1} CE @ ₹{p_otm1}",
                    "PE Entry":f"Sell {itm1} PE @ ₹{p_otm1}",
                    "CE Target":f"Stay below ₹{otm1}","PE Target":f"Stay above ₹{itm1}",
                    "Stop Loss":f"Exit both if loss > ₹{cr*lot:,} (1× credit)",
                    "Max Profit/Lot":f"₹{cr*lot:,}","Max Loss/Lot":"Unlimited — use SL",
                    "Risk:Reward":"Credit; strict SL required",
                    "Rationale":f"Sideways. Sell {otm1} CE + {itm1} PE. Credit ₹{cr}. BE: ₹{be_lo}–₹{be_hi}."}
        else:
            nc=max(5,(p_otm1-p_otm2)*2); w=otm1-atm; ml=max(1,w-nc); rr=round(ml/max(nc,1),1)
            return {"Strategy":"Iron Condor",
                    "CE Entry":f"Sell {otm1} CE @ ₹{p_otm1} | Buy {otm2} CE @ ₹{p_otm2}",
                    "PE Entry":f"Sell {itm1} PE @ ₹{p_otm1} | Buy {itm2} PE @ ₹{p_otm2}",
                    "CE Target":f"Stay below ₹{otm1}","PE Target":f"Stay above ₹{itm1}",
                    "Stop Loss":f"Exit breached side if loss > ₹{nc*2*lot:,}",
                    "Max Profit/Lot":f"₹{nc*lot:,}","Max Loss/Lot":f"₹{ml*lot:,}",
                    "Risk:Reward":f"1:{rr}",
                    "Rationale":f"Sideways swing. Iron Condor. Net credit ₹{nc}. Max loss ₹{ml}."}

def _sv(s, k): return s.get(k,"—")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

class BhavcopFetcher:
    """Equity CM bhavcopy — CMP, volume, turnover, delivery%."""

    def fetch(self, dt):
        url=BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("Equity bhavcopy → %s",dt.strftime("%d-%b-%Y"))
        raw=_download_raw(url,"BhavCopy")
        if raw is None: return None
        df=self._unzip(raw)
        if df is None or df.empty: return None
        df=self._filter_eq(df)
        if df.empty: return None

        sym_c=_pick_col(df,COL_MAP["symbol"])
        cls_c=_pick_col(df,COL_MAP["close"])
        vol_c=_pick_col(df,COL_MAP["volume"])
        tov_c=_pick_col(df,COL_MAP["turnover"])
        for c in (cls_c,vol_c,tov_c):
            df[c]=pd.to_numeric(df[c],errors="coerce")

        # Delivery %
        delv_pct_map={}
        try:
            dp_c=_pick_col(df,COL_MAP["delv_pct"])
            df[dp_c]=pd.to_numeric(df[dp_c],errors="coerce")
            delv_pct_map=dict(zip(df[sym_c].astype(str),df[dp_c].fillna(0)))
        except Exception:
            pass

        data_vol=(df.sort_values(vol_c,ascending=False).head(TOP_N)
                  [[sym_c,vol_c,cls_c]].fillna(0).values.tolist())
        data_to=(df.sort_values(tov_c,ascending=False).head(TOP_N)
                 [[sym_c,tov_c,cls_c]].fillna(0).values.tolist())
        cmp_map=dict(zip(df[sym_c].astype(str),df[cls_c].fillna(0)))
        log.info("  → %d EQ rows",len(df))
        return data_vol, data_to, cmp_map, delv_pct_map

    def _unzip(self,raw):
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f,low_memory=False)
        except Exception as e:
            log.error("BhavCopy ZIP: %s",e); return None

    def _filter_eq(self,df):
        ser_c=_pick_col(df,COL_MAP["series"])
        sym_c=_pick_col(df,COL_MAP["symbol"])
        df=df[df[ser_c].astype(str).str.strip()=="EQ"].copy()
        mask=df[sym_c].astype(str).str.contains(EXCLUDE_PATTERN,case=False,na=False)
        return df[~mask].reset_index(drop=True)


class FOBhavcopFetcher:
    """
    NSE FO bhavcopy — Open Interest per symbol and strike.
    Returns:
      oi_map      : {symbol: total_oi}        (CE+PE combined)
      oi_ce_map   : {symbol: ce_oi}
      oi_pe_map   : {symbol: pe_oi}
      oi_by_strike: {symbol: {strike: {"CE":oi,"PE":oi}}}  for max pain
    """

    # FO bhavcopy column candidates
    FO_COL = {
        "symbol": ["TckrSymb","SYMBOL","FinInstrmNm"],
        "option": ["OptnTp","OPTION_TYP","OptionType"],
        "strike": ["StrkPric","STRIKE_PR","StrikePrice"],
        "oi":     ["OpnIntrst","OPEN_INT","OpenInterest","OI"],
        "oi_chg": ["ChngInOpnIntrst","CHG_IN_OI","ChangeInOI"],
        "expiry": ["XpryDt","EXPIRY_DT","ExpiryDate"],
    }

    def fetch(self, dt):
        url=FO_BHAV_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("FO bhavcopy → %s",dt.strftime("%d-%b-%Y"))
        raw=_download_raw(url,"FOBhav")
        if raw is None: return {},{},{},{}
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    df=pd.read_csv(f,low_memory=False)
            df.columns=[c.strip() for c in df.columns]
            sym_c=_pick_col(df,self.FO_COL["symbol"])
            opt_c=_pick_col(df,self.FO_COL["option"])
            str_c=_pick_col(df,self.FO_COL["strike"])
            oi_c =_pick_col(df,self.FO_COL["oi"])
            df[sym_c]=df[sym_c].astype(str).str.strip()
            df[opt_c]=df[opt_c].astype(str).str.strip().str.upper()
            df[str_c]=pd.to_numeric(df[str_c],errors="coerce")
            df[oi_c] =pd.to_numeric(df[oi_c], errors="coerce").fillna(0)

            # Keep only near-month expiry (min expiry date)
            try:
                exp_c=_pick_col(df,self.FO_COL["expiry"])
                df[exp_c]=pd.to_datetime(df[exp_c],errors="coerce")
                min_exp=df[exp_c].min()
                df=df[df[exp_c]==min_exp]
            except Exception:
                pass

            oi_ce_map, oi_pe_map, oi_by_strike = {},{},{}
            for _,row in df.iterrows():
                sym=row[sym_c]; opt=row[opt_c]
                strike=row[str_c]; oi=row[oi_c]
                if opt=="CE":
                    oi_ce_map[sym]=oi_ce_map.get(sym,0)+oi
                    oi_by_strike.setdefault(sym,{}).setdefault(strike,{"CE":0,"PE":0})
                    oi_by_strike[sym][strike]["CE"]+=oi
                elif opt=="PE":
                    oi_pe_map[sym]=oi_pe_map.get(sym,0)+oi
                    oi_by_strike.setdefault(sym,{}).setdefault(strike,{"CE":0,"PE":0})
                    oi_by_strike[sym][strike]["PE"]+=oi

            oi_map={sym:oi_ce_map.get(sym,0)+oi_pe_map.get(sym,0)
                    for sym in set(list(oi_ce_map)+list(oi_pe_map))}
            log.info("  → FO OI loaded: %d symbols",len(oi_map))
            return oi_map, oi_ce_map, oi_pe_map, oi_by_strike
        except Exception as e:
            log.warning("FOBhav parse: %s",e)
            return {},{},{},{}


class IndexPriceFetcher:
    """Index closing prices + India VIX from ind_close_all CSV."""

    def fetch(self, ist_today):
        for days_back in range(LOOKBACK_DAYS+1):
            cand=ist_today-timedelta(days=days_back)
            if cand.weekday()>=5: continue
            data=self._fetch_one(cand)
            if data:
                log.info("Index prices %s: %s",cand.strftime("%d-%b-%Y"),
                         {k:f"₹{v:,.0f}" for k,v in data.items() if k!="VIX"})
                return data
        log.warning("Index CSV unavailable — using fallback prices.")
        return dict(INDEX_FALLBACK_CMP)

    def fetch_history(self, ist_today, days=HISTORY_DAYS):
        history,found={},0
        for days_back in range(1, days+15):
            cand=ist_today-timedelta(days=days_back)
            if cand.weekday()>=5: continue
            dd=self._fetch_one(cand)
            if not dd: continue
            for sym,price in dd.items():
                history.setdefault(sym,[]).append(price)
            found+=1
            if found>=days: break
        return {s:list(reversed(v)) for s,v in history.items()}

    def _fetch_one(self, dt):
        url=INDEX_CSV_URL.format(date=dt.strftime("%d%m%Y"))
        raw=_download_raw(url,"IndexCSV")
        if raw is None: return {}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")))
            df.columns=[c.strip() for c in df.columns]
            name_col=next((c for c in df.columns if "index" in c.lower() and "name" in c.lower()),None)
            close_col=next((c for c in df.columns if "clos" in c.lower()),None)
            if not name_col or not close_col: return {}
            df[close_col]=pd.to_numeric(df[close_col].astype(str).str.replace(",",""),errors="coerce")
            result={}
            for _,row in df.iterrows():
                raw_name=str(row[name_col]).strip()
                sym=INDEX_NAME_MAP.get(raw_name) or INDEX_NAME_MAP.get(raw_name.title())
                if sym and pd.notna(row[close_col]) and row[close_col]>0:
                    result[sym]=float(row[close_col])
            for sym,fb in INDEX_FALLBACK_CMP.items():
                if sym not in result: result[sym]=fb
            return result
        except Exception as e:
            log.warning("IndexCSV parse: %s",e); return {}


class Week52Fetcher:
    """52-week High/Low from NSE CSV."""

    def fetch(self):
        raw=_download_raw(WEEK52_URL,"52wk")
        if raw is None: return {},{}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")))
            df.columns=[c.strip() for c in df.columns]
            sym_c=next((c for c in df.columns if "symbol" in c.lower()),None)
            hi_c =next((c for c in df.columns if "high" in c.lower()),None)
            lo_c =next((c for c in df.columns if "low"  in c.lower()),None)
            if not all([sym_c,hi_c,lo_c]): return {},{}
            df[hi_c]=pd.to_numeric(df[hi_c],errors="coerce")
            df[lo_c]=pd.to_numeric(df[lo_c],errors="coerce")
            hi_map=dict(zip(df[sym_c].astype(str).str.strip(),df[hi_c].fillna(0)))
            lo_map=dict(zip(df[sym_c].astype(str).str.strip(),df[lo_c].fillna(0)))
            log.info("52wk High/Low: %d symbols",len(hi_map))
            return hi_map, lo_map
        except Exception as e:
            log.warning("52wk parse: %s",e); return {},{}


class LotSizeFetcher:
    def fetch(self):
        raw=_download_raw(MKTLOTS_URL,"LotSizes")
        if raw is None: return {}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")),header=1,dtype=str)
            df.columns=[c.strip() for c in df.columns]
            sc,lc=df.columns[0],df.columns[1]
            df[sc]=df[sc].str.strip()
            df[lc]=pd.to_numeric(df[lc].str.replace(",",""),errors="coerce")
            result=dict(zip(df[sc].dropna(),df[lc].dropna().astype(int)))
            log.info("Lot sizes: %d symbols",len(result)); return result
        except Exception as e:
            log.warning("LotSizes: %s",e); return {}


class EquityHistoryFetcher:
    def fetch(self, ist_today, days=HISTORY_DAYS):
        history,fetcher,found={},BhavcopFetcher(),0
        for days_back in range(1,days+15):
            cand=ist_today-timedelta(days=days_back)
            if cand.weekday()>=5: continue
            res=fetcher.fetch(cand)
            if not res: continue
            _,_,cmap,_=res
            for sym,cls in cmap.items():
                if cls and cls>0: history.setdefault(sym,[]).append(cls)
            found+=1
            if found>=days: break
        return {s:list(reversed(v)) for s,v in history.items()}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SHEET HEADERS
# ══════════════════════════════════════════════════════════════════════════════

# ── 12 NEW analytics columns (appended to both sheets before Notes) ──────────
ANALYTICS_HEADERS = [
    "Open Interest\n(OI — Lots)",           # A1
    "OI Change\n(vs Prev Day)",              # A2
    "PCR\n(Put-Call Ratio)",                 # A3
    "52-Week\nHigh ₹",                       # A4
    "52-Week\nLow ₹",                        # A5
    "Delivery\n%",                           # A6
    "India\nVIX",                            # A7
    "IV %\n(Impl. Volatility)",              # A8
    "Max Pain\nStrike ₹",                    # A9
    "Support\n₹",                            # A10
    "Resistance\n₹",                         # A11
    "Beta\nvs Nifty",                        # A12
    "RSI\n(14-day)",                         # A13
    "MACD Signal\n(12/26/9)",               # A14
]   # 14 analytics columns

# OPTIONS F&O — 40 base + 14 analytics + 1 Notes = 55 columns
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
    # ── 14 analytics ─────────────────────────────────────────────────
    "Open Interest\n(OI — Lots)",                # 40
    "OI Change\n(vs Prev Day)",                  # 41
    "PCR\n(Put-Call Ratio)",                     # 42
    "52-Week\nHigh ₹",                           # 43
    "52-Week\nLow ₹",                            # 44
    "Delivery\n%",                               # 45
    "India\nVIX",                                # 46
    "IV %\n(Impl. Volatility)",                  # 47
    "Max Pain\nStrike ₹",                        # 48
    "Support\n₹",                                # 49
    "Resistance\n₹",                             # 50
    "Beta\nvs Nifty",                            # 51
    "RSI\n(14-day)",                             # 52
    "MACD Signal\n(12/26/9)",                   # 53
    "Notes",                                      # 54
]   # total = 54

# FUTURES F&O — 35 base + 14 analytics + 1 Notes + 1 Last Updated = 51 columns
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
    # ── 14 analytics ─────────────────────────────────────────────────
    "Open Interest\n(OI — Lots)",                # 35
    "OI Change\n(vs Prev Day)",                  # 36
    "PCR\n(Put-Call Ratio)",                     # 37
    "52-Week\nHigh ₹",                           # 38
    "52-Week\nLow ₹",                            # 39
    "Delivery\n%",                               # 40
    "India\nVIX",                                # 41
    "IV %\n(Impl. Volatility)",                  # 42
    "Max Pain\nStrike ₹",                        # 43
    "Support\n₹",                                # 44
    "Resistance\n₹",                             # 45
    "Beta\nvs Nifty",                            # 46
    "RSI\n(14-day)",                             # 47
    "MACD Signal\n(12/26/9)",                   # 48
    "Notes",                                      # 49
    "Last Updated",                               # 50
]   # total = 50


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — ROW BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_block(
    sym, ltp, lot, atm_prem,
    oi_map, oi_ce_map, oi_pe_map, oi_by_strike,
    prev_oi_map,
    wk52_hi, wk52_lo,
    delv_pct_map,
    india_vix,
    equity_hist, nifty_hist,
):
    """
    Build the 14-item analytics list for one symbol.
    All values gracefully degrade to '—' if data unavailable.
    """
    # OI
    oi     = oi_map.get(sym, 0)
    oi_ce  = oi_ce_map.get(sym, 0)
    oi_pe  = oi_pe_map.get(sym, 0)
    prev   = prev_oi_map.get(sym, 0)
    oi_chg = (oi - prev) if (oi > 0 and prev > 0) else None

    # PCR
    pcr = round(oi_pe/oi_ce, 2) if oi_ce > 0 else None
    pcr_str = f"{pcr}" if pcr else "—"
    if pcr:
        if pcr > 1.2:   pcr_str = f"{pcr} 🟢 Bullish"
        elif pcr < 0.8: pcr_str = f"{pcr} 🔴 Bearish"
        else:           pcr_str = f"{pcr} 🟡 Neutral"

    # 52-week
    hi52 = wk52_hi.get(sym, 0) or None
    lo52 = wk52_lo.get(sym, 0) or None

    # Delivery %
    dlv = delv_pct_map.get(sym, 0) or None
    dlv_str = f"{dlv:.1f}%" if dlv else "—"

    # India VIX
    vix_str = f"{india_vix:.2f}" if india_vix else "—"

    # IV back-solved
    iv_str = _calc_iv(ltp, atm_prem) if ltp > 0 else "—"

    # Max Pain
    mp = _calc_max_pain(oi_by_strike.get(sym, {}))
    mp_str = f"₹{mp:,}" if mp else "—"

    # Support / Resistance
    hist  = equity_hist.get(sym, [])
    sup, res = _calc_support_resistance(hist)
    sup_str = f"₹{sup:,.2f}" if sup else "—"
    res_str = f"₹{res:,.2f}" if res else "—"

    # Beta
    beta = _calc_beta(hist, nifty_hist) if (hist and nifty_hist) else None
    beta_str = str(beta) if beta is not None else "—"

    # RSI
    rsi = _calc_rsi(hist)
    rsi_str = _rsi_label(rsi)

    # MACD
    _, _, _, macd_label = _calc_macd(hist)

    return [
        int(oi)     if oi else "—",          # OI
        int(oi_chg) if oi_chg is not None else "—",  # OI Change
        pcr_str,                              # PCR
        hi52 if hi52 else "—",               # 52wk High
        lo52 if lo52 else "—",               # 52wk Low
        dlv_str,                              # Delivery %
        vix_str,                              # India VIX
        iv_str,                               # IV %
        mp_str,                               # Max Pain
        sup_str,                              # Support
        res_str,                              # Resistance
        beta_str,                             # Beta
        rsi_str,                              # RSI
        macd_label,                           # MACD
    ]   # 14 items


def _build_opt_row(sr, name, sym, sector, lot, ltp, expiries, history,
                   analytics, note=""):
    """Build one Options F&O row — 54 items."""
    exp_near,exp_mid,exp_far=expiries
    hist   =history.get(sym,[])
    intra_t=_trend(hist[-5:] if len(hist)>=5 else hist)
    swing_t=_trend(hist)
    cval   =round(lot*ltp)
    atm_c  =_atm_premium(ltp)
    atm_p  =_atm_premium(ltp)
    si=_strategy_engine(ltp,atm_c,intra_t,swing_t,lot,"intraday")
    ss=_strategy_engine(ltp,atm_c,intra_t,swing_t,lot,"swing")
    return [
        sr,name,sym,sector,lot,round(ltp,2),cval,
        exp_near,exp_mid,exp_far,
        atm_c,atm_p,atm_c*lot,atm_p*lot,round(cval*0.20),
        _trend_label(intra_t),_trend_label(swing_t),
        si["Strategy"],ss["Strategy"],
        si["Strategy"],_sv(si,"CE Entry"),_sv(si,"PE Entry"),
        _sv(si,"CE Target"),_sv(si,"PE Target"),_sv(si,"Stop Loss"),
        _sv(si,"Max Profit/Lot"),_sv(si,"Max Loss/Lot"),_sv(si,"Risk:Reward"),
        _sv(si,"Rationale"),
        ss["Strategy"],_sv(ss,"CE Entry"),_sv(ss,"PE Entry"),
        _sv(ss,"CE Target"),_sv(ss,"PE Target"),_sv(ss,"Stop Loss"),
        _sv(ss,"Max Profit/Lot"),_sv(ss,"Max Loss/Lot"),_sv(ss,"Risk:Reward"),
        _sv(ss,"Rationale"),
    ] + analytics + [note]   # 39 + 14 + 1 = 54


def _build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries,
                   history, analytics, note=""):
    """Build one Futures F&O row — 50 items."""
    exp_near,exp_mid,exp_far=expiries
    hist   =history.get(sym,[])
    intra_t=_trend(hist[-5:] if len(hist)>=5 else hist)
    swing_t=_trend(hist)
    cval   =round(lot*ltp)
    atm_c  =_atm_premium(ltp)
    si=_strategy_engine(ltp,atm_c,intra_t,swing_t,lot,"intraday")
    ss=_strategy_engine(ltp,atm_c,intra_t,swing_t,lot,"swing")
    return [
        sr,name,sym,sector,lot,round(ltp,2),cval,
        f"{margin_pct}%",round(cval*margin_pct/100),
        exp_near,exp_mid,exp_far,
        _trend_label(intra_t),_trend_label(swing_t),
        si["Strategy"],_sv(si,"CE Entry"),_sv(si,"PE Entry"),
        _sv(si,"CE Target"),_sv(si,"PE Target"),_sv(si,"Stop Loss"),
        _sv(si,"Max Profit/Lot"),_sv(si,"Max Loss/Lot"),_sv(si,"Risk:Reward"),
        _sv(si,"Rationale"),
        ss["Strategy"],_sv(ss,"CE Entry"),_sv(ss,"PE Entry"),
        _sv(ss,"CE Target"),_sv(ss,"PE Target"),_sv(ss,"Stop Loss"),
        _sv(ss,"Max Profit/Lot"),_sv(ss,"Max Loss/Lot"),_sv(ss,"Risk:Reward"),
        _sv(ss,"Rationale"),
    ] + analytics + [note, _ist_now()]   # 34 + 14 + 2 = 50


def build_all_rows(
    lot_sizes, equity_cmp, index_cmp,
    equity_hist, index_hist,
    oi_map, oi_ce_map, oi_pe_map, oi_by_strike,
    prev_oi_map,
    wk52_hi, wk52_lo,
    delv_pct_map,
    expiries,
):
    fut_rows,opt_rows,sr=[],[],1
    nifty_hist=index_hist.get("NIFTY", equity_hist.get("NIFTY",[]))
    india_vix=index_cmp.get("VIX", None)

    def _ana(sym, ltp, lot):
        atm_p=_atm_premium(ltp)
        return _analytics_block(
            sym, ltp, lot, atm_p,
            oi_map, oi_ce_map, oi_pe_map, oi_by_strike,
            prev_oi_map, wk52_hi, wk52_lo, delv_pct_map,
            india_vix, equity_hist, nifty_hist,
        )

    # ── INDICES ──────────────────────────────────────────────────────────────
    log.info("Building index rows…")
    for sym,name,sector,margin_pct in INDEX_META:
        lot=lot_sizes.get(sym,FALLBACK_LOTS.get(sym,0))
        ltp=index_cmp.get(sym,0)
        if lot==0: continue
        if ltp==0: ltp=INDEX_FALLBACK_CMP.get(sym,0)
        if ltp==0: continue
        hist=index_hist.get(sym) or equity_hist.get(sym,[])
        h   ={sym:hist}
        ana =_ana(sym,ltp,lot)
        fut_rows.append(_build_fut_row(sr,name,sym,sector,lot,ltp,margin_pct,expiries,h,ana,"Index Future"))
        opt_rows.append(_build_opt_row(sr,name,sym,sector,lot,ltp,expiries,h,ana,"Index Option"))
        log.info("  %-12s CMP=₹%-8s Lot=%-4d",sym,f"{ltp:,.0f}",lot)
        sr+=1

    # ── STOCKS ───────────────────────────────────────────────────────────────
    log.info("Building stock rows…")
    skipped=0
    for sym in sorted((set(lot_sizes)|set(FALLBACK_LOTS))-INDEX_SYMS):
        lot=lot_sizes.get(sym,FALLBACK_LOTS.get(sym,0))
        ltp=equity_cmp.get(sym,0)
        if lot==0 or ltp==0: skipped+=1; continue
        sector=SECTOR_MAP.get(sym,"Equity")
        ana=_ana(sym,ltp,lot)
        fut_rows.append(_build_fut_row(sr,sym,sym,sector,lot,ltp,20,expiries,equity_hist,ana))
        opt_rows.append(_build_opt_row(sr,sym,sym,sector,lot,ltp,expiries,equity_hist,ana))
        sr+=1

    log.info("Rows — Futures:%d  Options:%d  Skipped:%d",
             len(fut_rows),len(opt_rows),skipped)
    return fut_rows,opt_rows


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — GOOGLE SHEETS WRITER
# ══════════════════════════════════════════════════════════════════════════════

class SheetsWriter:
    def __init__(self, creds_json):
        creds=ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(creds_json),GSHEETS_SCOPES)
        self._client=gspread.authorize(creds)

    def _get_or_create(self, ss, title, cols=60):
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating tab: '%s'",title)
            return ss.add_worksheet(title=title,rows=600,cols=cols)

    def open_all(self):
        ss=self._client.open_by_key(SPREADSHEET_ID)
        return (
            self._get_or_create(ss,SHEET_VOLUME,  cols=15),
            self._get_or_create(ss,SHEET_TURNOVER,cols=15),
            self._get_or_create(ss,SHEET_FUTURES, cols=55),
            self._get_or_create(ss,SHEET_OPTIONS, cols=60),
        )

    def write_vol_turnover(self, ws_vol, ws_to, data_vol, data_to, fetched_date):
        status=f"Data: {fetched_date}  |  Updated: {_ist_now()}"
        for ws,data in ((ws_vol,data_vol),(ws_to,data_to)):
            n=len(data)
            ws.batch_update([{"range":f"A2:C{n+1}","values":data},
                             {"range":STATUS_CELL,"values":[[status]]}],
                            value_input_option="USER_ENTERED")
            log.info("'%s' → %d rows",ws.title,n)

    def write_fo_sheet(self, ws, headers, rows, title):
        all_data=[headers]+rows
        n_cols=len(headers)
        ws.clear()
        time.sleep(1)
        for start in range(0,len(all_data),WRITE_CHUNK):
            end=min(start+WRITE_CHUNK,len(all_data))
            rng=f"A{start+1}:{_col_letter(n_cols)}{end}"
            ws.update(range_name=rng,values=all_data[start:end],
                      value_input_option="RAW")
            log.info("  '%s' rows %d–%d",title,start+1,end)
            if end<len(all_data): time.sleep(1.5)
        log.info("'%s' done — %d rows × %d cols",title,len(rows),n_cols)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    creds_json=os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GCP_CREDENTIALS not set.")

    ist_today=datetime.utcnow()+timedelta(hours=5,minutes=30)
    log.info("═"*60)
    log.info("NSE Auto-Sheet v6  —  %s",ist_today.strftime("%d-%b-%Y %H:%M IST"))
    log.info("═"*60)

    writer=SheetsWriter(creds_json)
    ws_vol,ws_to,ws_fut,ws_opt=writer.open_all()

    # ── Equity bhavcopy (today) ───────────────────────────────────────────────
    log.info("── Equity bhavcopy ──────────────────────────────────────")
    eq_fetcher=BhavcopFetcher()
    result,fetched_date=None,""
    for days_back in range(LOOKBACK_DAYS+1):
        cand=ist_today-timedelta(days=days_back)
        if cand.weekday()>=5: continue
        result=eq_fetcher.fetch(cand)
        if result: fetched_date=cand.strftime("%d-%b-%Y"); break
    if not result:
        raise RuntimeError("No equity bhavcopy found.")
    data_vol,data_to,equity_cmp,delv_pct_map=result
    writer.write_vol_turnover(ws_vol,ws_to,data_vol,data_to,fetched_date)

    # ── FO bhavcopy — today (OI) and yesterday (prev OI for OI change) ───────
    log.info("── FO bhavcopy (OI) ─────────────────────────────────────")
    fo_fetcher=FOBhavcopFetcher()
    oi_map,oi_ce_map,oi_pe_map,oi_by_strike=fo_fetcher.fetch(
        datetime.strptime(fetched_date,"%d-%b-%Y"))

    # Previous day OI for OI-change column
    prev_oi_map={}
    for pb in range(1,5):
        prev_cand=datetime.strptime(fetched_date,"%d-%b-%Y")-timedelta(days=pb)
        if prev_cand.weekday()>=5: continue
        pm,_,_,_=fo_fetcher.fetch(prev_cand)
        if pm: prev_oi_map=pm; break

    # ── Index prices + VIX ───────────────────────────────────────────────────
    log.info("── Index prices + VIX ───────────────────────────────────")
    idx_fetcher=IndexPriceFetcher()
    index_cmp=idx_fetcher.fetch(ist_today)
    india_vix=index_cmp.get("VIX")
    log.info("  India VIX: %s",india_vix)

    # ── Lot sizes ─────────────────────────────────────────────────────────────
    log.info("── Lot sizes ────────────────────────────────────────────")
    lot_sizes=LotSizeFetcher().fetch() or FALLBACK_LOTS

    # ── 52-week High/Low ─────────────────────────────────────────────────────
    log.info("── 52-week High/Low ─────────────────────────────────────")
    wk52_hi,wk52_lo=Week52Fetcher().fetch()

    # ── Price history (equity + index) for indicators ────────────────────────
    log.info("── Price history (%d days) ──────────────────────────────",HISTORY_DAYS)
    equity_hist=EquityHistoryFetcher().fetch(ist_today,days=HISTORY_DAYS)
    index_hist =idx_fetcher.fetch_history(ist_today,days=HISTORY_DAYS)

    # ── Expiry dates ─────────────────────────────────────────────────────────
    expiries=_expiry_dates()
    log.info("── Expiries: %s | %s | %s ──────────────────────────────",*expiries)

    # ── Build rows ────────────────────────────────────────────────────────────
    log.info("── Building F&O rows ────────────────────────────────────")
    fut_rows,opt_rows=build_all_rows(
        lot_sizes,equity_cmp,index_cmp,
        equity_hist,index_hist,
        oi_map,oi_ce_map,oi_pe_map,oi_by_strike,
        prev_oi_map,wk52_hi,wk52_lo,delv_pct_map,
        expiries,
    )

    # Sanity check
    if fut_rows and len(fut_rows[0])!=len(FUT_HEADERS):
        raise ValueError(f"FUT row={len(fut_rows[0])} vs headers={len(FUT_HEADERS)}")
    if opt_rows and len(opt_rows[0])!=len(OPT_HEADERS):
        raise ValueError(f"OPT row={len(opt_rows[0])} vs headers={len(OPT_HEADERS)}")

    # ── Write sheets ──────────────────────────────────────────────────────────
    log.info("── Writing Futures F&O ──────────────────────────────────")
    writer.write_fo_sheet(ws_fut,FUT_HEADERS,fut_rows,SHEET_FUTURES)

    log.info("── Writing Options F&O ──────────────────────────────────")
    writer.write_fo_sheet(ws_opt,OPT_HEADERS,opt_rows,SHEET_OPTIONS)

    log.info("═"*60)
    log.info("✅  SUCCESS  |  Data: %s  |  %s",fetched_date,_ist_now())
    log.info("═"*60)


if __name__ == "__main__":
    main()
