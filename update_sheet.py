"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NSE F&O Auto-Sheet  —  update_sheet.py  (v7 — All Analytics Fixed)        ║
║  GitHub: DSOLAPURE/NSE-Auto-Sheet                                           ║
║                                                                              ║
║  FIXES in v7 (all data columns now populated):                              ║
║                                                                              ║
║  Bug 1 — OI / PCR / Max Pain                                                ║
║    Root cause: NSE blocks requests without a browser session cookie.        ║
║    Fix: NSESession class warms up cookies via homepage + API ping           ║
║         before fetching FO bhavcopy ZIP.                                    ║
║                                                                              ║
║  Bug 2 — Delivery %                                                         ║
║    Root cause: CM bhavcopy (UDiFF format) does NOT contain delivery         ║
║         columns. Delivery data is in a separate NSE file:                   ║
║         sec_bhavdata_full_{date}.csv  (columns: DELIV_QTY, DELIV_PER)      ║
║    Fix: DeliveryFetcher hits the correct URL.                               ║
║                                                                              ║
║  Bug 3 — 52-Week High / Low                                                 ║
║    Root cause: static CSV needs the same session cookie.                    ║
║    Fix: fetched via NSESession.                                             ║
║                                                                              ║
║  Bug 4 — MACD (needs 27+ data points)                                      ║
║    Root cause: HISTORY_DAYS=20 is sometimes too few if some days            ║
║         fail, leaving < 27 closes → MACD returns "—".                      ║
║    Fix: HISTORY_DAYS raised to 35; fetcher requests up to 50 days.         ║
║                                                                              ║
║  Bug 5 — Max Pain empty                                                     ║
║    Root cause: depended on OI data (fixed by Bug 1 fix).                   ║
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

SPREADSHEET_ID  = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME    = "Top 250 Stocks"
SHEET_TURNOVER  = "Top 250 Turnover"
SHEET_FUTURES   = "Futures F&O"
SHEET_OPTIONS   = "Options F&O"
TOP_N           = 250
LOOKBACK_DAYS   = 7
REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3
RETRY_DELAY     = 5
HISTORY_DAYS    = 35          # raised: need 27+ for MACD, buffer for missed days
WRITE_CHUNK     = 100
EXCLUDE_PATTERN = r"BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ"

# ── NSE URLs ─────────────────────────────────────────────────────────────────
BHAVCOPY_URL   = ("https://nsearchives.nseindia.com/content/cm/"
                  "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip")
FO_BHAV_URL    = ("https://nsearchives.nseindia.com/content/fo/"
                  "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip")
INDEX_CSV_URL  = ("https://nsearchives.nseindia.com/content/indices/"
                  "ind_close_all_{date}.csv")
# FIX Bug 2: correct delivery data URL (separate from CM bhavcopy)
DELIVERY_URL   = ("https://archives.nseindia.com/products/content/"
                  "sec_bhavdata_full_{date}.csv")
# FIX Bug 3: 52-week URL (correct, needs session)
WEEK52_URL     = "https://archives.nseindia.com/content/equities/52_wk_high_low.csv"
MKTLOTS_URL    = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

COL_MAP = {
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
    "India Vix":"VIX","India VIX":"VIX","INDIA VIX":"VIX",
    "NIFTY 50":"NIFTY","NIFTY BANK":"BANKNIFTY",
    "NIFTY FINANCIAL SERVICES":"FINNIFTY","NIFTY MIDCAP SELECT":"MIDCPNIFTY",
    "NIFTY NEXT 50":"NIFTYNXT50",
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
# SECTION 3 — NSE SESSION  (FIX for Bug 1, 2, 3)
# ══════════════════════════════════════════════════════════════════════════════

class NSESession:
    """
    Manages a persistent requests.Session with NSE cookies.
    NSE requires a browser-like session (visit homepage first)
    before it will serve bhavcopy ZIPs and static CSVs.
    Without this, all NSE archive URLs return HTTP 403.
    """
    _BASE_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Referer":         "https://www.nseindia.com/",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self._BASE_HEADERS)
        self._warmed = False

    def warm_up(self):
        """Visit NSE homepage + a JSON API endpoint to obtain session cookies."""
        if self._warmed:
            return
        try:
            log.info("NSE session warm-up — fetching homepage…")
            r = self.session.get("https://www.nseindia.com", timeout=REQUEST_TIMEOUT)
            log.info("  Homepage: HTTP %s  cookies: %s", r.status_code, list(self.session.cookies.keys()))
            time.sleep(2)
            # Ping a lightweight API endpoint to solidify the session
            self.session.get("https://www.nseindia.com/api/marketStatus", timeout=REQUEST_TIMEOUT)
            time.sleep(1)
            self._warmed = True
            log.info("  Session warm-up complete.")
        except Exception as exc:
            log.warning("  Session warm-up error (non-fatal): %s", exc)
            self._warmed = True   # proceed anyway

    def get(self, url: str, label: str = "") -> bytes | None:
        """Download URL with retry. Returns bytes or None."""
        self.warm_up()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    log.info("  %s HTTP 200 (%d bytes)", label or url.split("/")[-1], len(r.content))
                    return r.content
                log.warning("  %s HTTP %s (attempt %d/%d)", label, r.status_code, attempt, MAX_RETRIES)
            except requests.RequestException as exc:
                log.warning("  %s error attempt %d: %s", label, attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        log.error("  %s failed after %d attempts.", label, MAX_RETRIES)
        return None


# Global session shared by all fetchers — one warm-up for all
_NSE = NSESession()

def _download(url: str, label: str = "") -> bytes | None:
    return _NSE.get(url, label)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _pick_col(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} in {list(df.columns)}")

def _ist_now():
    return (datetime.now(timezone.utc)+timedelta(hours=5,minutes=30)).strftime("%d-%b-%Y %H:%M IST")

def _ist_today():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def _col_letter(n):
    r=""
    while n>0:
        n,rem=divmod(n-1,26)
        r=chr(65+rem)+r
    return r

def _last_thursday(year, month):
    if month==12:
        last=date(year+1,1,1)-timedelta(days=1)
    else:
        last=date(year,month+1,1)-timedelta(days=1)
    return last-timedelta(days=(last.weekday()-3)%7)

def _expiry_dates():
    today=_ist_today().date()
    expiries,m,y=[],today.month,today.year
    for _ in range(6):
        exp=_last_thursday(y,m)
        if exp>=today:
            expiries.append(exp.strftime("%d-%b-%Y"))
        if len(expiries)==3: break
        m+=1
        if m>12: m,y=1,y+1
    while len(expiries)<3: expiries.append("—")
    return tuple(expiries)

def _atm_premium(ltp, days=25, iv=0.28):
    prem=0.4*iv*math.sqrt(max(days,1)/252)*ltp
    return max(5,int(round(prem/5)*5))

def _trend(closes):
    if len(closes)<3: return "Sideways"
    n=len(closes)
    avg5 =sum(closes[-min(5, n):])/min(5, n)
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

def _trading_days_back(ist_today, n=50):
    """Yield up to n recent trading-day datetime objects (most recent first)."""
    count=0
    for d in range(1, n*2):
        cand=ist_today-timedelta(days=d)
        if cand.weekday()<5:
            yield cand
            count+=1
            if count>=n: break


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — ANALYTICS CALCULATORS
# ══════════════════════════════════════════════════════════════════════════════

def _calc_rsi(closes, period=14):
    if len(closes)<period+2: return None
    gains,losses=[],[]
    for i in range(1,len(closes)):
        chg=closes[i]-closes[i-1]
        gains.append(max(chg,0)); losses.append(max(-chg,0))
    ag=sum(gains[-period:])/period
    al=sum(losses[-period:])/period
    if al==0: return 100.0
    return round(100-(100/(1+ag/al)),1)

def _rsi_label(rsi):
    if rsi is None: return "—"
    if rsi>=70:  return f"{rsi} 🔴 Overbought"
    if rsi<=30:  return f"{rsi} 🟢 Oversold"
    return f"{rsi} 🟡 Neutral"

def _calc_ema(closes, period):
    if not closes: return []
    k=2/(period+1); ema=[closes[0]]
    for p in closes[1:]:
        ema.append(p*k+ema[-1]*(1-k))
    return ema

def _calc_macd(closes):
    """12/26/9 MACD. Needs len(closes) >= 27."""
    if len(closes)<27: return None,None,None,"—"
    e12=_calc_ema(closes,12); e26=_calc_ema(closes,26)
    macd=[a-b for a,b in zip(e12,e26)]
    sig=_calc_ema(macd,9)
    hist=[m-s for m,s in zip(macd,sig)]
    m,s,h=round(macd[-1],2),round(sig[-1],2),round(hist[-1],2)
    if   h>0 and m>0: label=f"🟢 Bullish  MACD={m}  Sig={s}  Hist=+{h}"
    elif h>0:          label=f"🟡 Recovering  MACD={m}  Sig={s}  Hist=+{h}"
    elif h<0 and m<0:  label=f"🔴 Bearish  MACD={m}  Sig={s}  Hist={h}"
    else:              label=f"🟡 Weakening  MACD={m}  Sig={s}  Hist={h}"
    return m,s,h,label

def _calc_beta(stock_closes, nifty_closes):
    n=min(len(stock_closes),len(nifty_closes))
    if n<5: return None
    sr=[(stock_closes[i]-stock_closes[i-1])/stock_closes[i-1] for i in range(1,n)]
    nr=[(nifty_closes[i]-nifty_closes[i-1])/nifty_closes[i-1] for i in range(1,n)]
    if len(sr)<4: return None
    ms,mn=sum(sr)/len(sr),sum(nr)/len(nr)
    cov=sum((s-ms)*(ni-mn) for s,ni in zip(sr,nr))/len(sr)
    vn =sum((ni-mn)**2 for ni in nr)/len(nr)
    return round(cov/vn,2) if vn else None

def _calc_support_resistance(closes):
    if len(closes)<5: return None,None
    w=closes[-min(10,len(closes)):]
    return round(min(w),2),round(max(w),2)

def _calc_iv(ltp, atm_prem, days=25):
    denom=0.4*ltp*math.sqrt(max(days,1)/252)
    if denom==0: return "—"
    return f"{atm_prem/denom*100:.1f}%"

def _calc_max_pain(oi_by_strike):
    if not oi_by_strike: return None
    strikes=sorted(oi_by_strike.keys())
    min_pain,best=float("inf"),strikes[0]
    for s_test in strikes:
        loss=0
        for s_str,data in oi_by_strike.items():
            if s_test>s_str: loss+=(s_test-s_str)*data.get("CE",0)
            if s_test<s_str: loss+=(s_str-s_test)*data.get("PE",0)
        if loss<min_pain: min_pain,best=loss,s_test
    return best


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — STRATEGY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_engine(ltp,atm_prem,intra_trend,swing_trend,lot,timeframe="intraday"):
    trend=intra_trend if timeframe=="intraday" else swing_trend
    atm=_round_strike(ltp)
    step=(100 if ltp>20000 else 50 if ltp>5000 else 20 if ltp>1000 else 10 if ltp>200 else 5)
    otm1=atm+step; otm2=atm+2*step; itm1=atm-step; itm2=atm-2*step
    pa=atm_prem
    po1=max(5,int(pa*0.55/5)*5); po2=max(5,int(pa*0.30/5)*5)
    if trend=="Bullish":
        if timeframe=="intraday":
            sl=max(5,int(pa*0.5))
            return {"Strategy":"Long Call","CE Entry":f"Buy {atm} CE @ ₹{pa}",
                    "PE Entry":"—","CE Target":f"₹{pa*2} (2× premium)","PE Target":"—",
                    "Stop Loss":f"₹{sl} (50% of premium)",
                    "Max Profit/Lot":f"₹{pa*2*lot:,}","Max Loss/Lot":f"₹{pa*lot:,}",
                    "Risk:Reward":"1:2","Rationale":f"Intraday bullish. Buy ATM {atm} CE @ ₹{pa}. Target ₹{pa*2}, SL ₹{sl}."}
        else:
            nd=pa-po1; mg=(otm1-atm)-nd; rr=round(mg/max(nd,1),1)
            return {"Strategy":"Bull Call Spread",
                    "CE Entry":f"Buy {atm} CE @ ₹{pa} | Sell {otm1} CE @ ₹{po1}",
                    "PE Entry":"—","CE Target":f"Close ≥ ₹{otm1} at expiry","PE Target":"—",
                    "Stop Loss":f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot":f"₹{mg*lot:,}","Max Loss/Lot":f"₹{nd*lot:,}",
                    "Risk:Reward":f"1:{rr}","Rationale":f"Swing bullish. Buy {atm} CE ₹{pa}, sell {otm1} CE ₹{po1}. Net debit ₹{nd}."}
    elif trend=="Bearish":
        if timeframe=="intraday":
            sl=max(5,int(pa*0.5))
            return {"Strategy":"Long Put","CE Entry":"—",
                    "PE Entry":f"Buy {atm} PE @ ₹{pa}","CE Target":"—",
                    "PE Target":f"₹{pa*2} (2× premium)","Stop Loss":f"₹{sl} (50% of premium)",
                    "Max Profit/Lot":f"₹{pa*2*lot:,}","Max Loss/Lot":f"₹{pa*lot:,}",
                    "Risk:Reward":"1:2","Rationale":f"Intraday bearish. Buy ATM {atm} PE @ ₹{pa}. Target ₹{pa*2}, SL ₹{sl}."}
        else:
            nd=pa-po1; mg=(atm-itm1)-nd; rr=round(mg/max(nd,1),1)
            return {"Strategy":"Bear Put Spread","CE Entry":"—",
                    "PE Entry":f"Buy {atm} PE @ ₹{pa} | Sell {itm1} PE @ ₹{po1}",
                    "CE Target":"—","PE Target":f"Close ≤ ₹{itm1} at expiry",
                    "Stop Loss":f"Exit if MTM loss ≈ ₹{int(nd*0.4*lot):,} (40% debit)",
                    "Max Profit/Lot":f"₹{mg*lot:,}","Max Loss/Lot":f"₹{nd*lot:,}",
                    "Risk:Reward":f"1:{rr}","Rationale":f"Swing bearish. Buy {atm} PE ₹{pa}, sell {itm1} PE ₹{po1}. Net debit ₹{nd}."}
    else:
        if timeframe=="intraday":
            cr=po1*2; be_hi=otm1+cr; be_lo=itm1-cr
            return {"Strategy":"Short Strangle","CE Entry":f"Sell {otm1} CE @ ₹{po1}",
                    "PE Entry":f"Sell {itm1} PE @ ₹{po1}",
                    "CE Target":f"Stay below ₹{otm1}","PE Target":f"Stay above ₹{itm1}",
                    "Stop Loss":f"Exit both if loss > ₹{cr*lot:,} (1× credit)",
                    "Max Profit/Lot":f"₹{cr*lot:,}","Max Loss/Lot":"Unlimited — use SL",
                    "Risk:Reward":"Credit; strict SL required",
                    "Rationale":f"Sideways. Sell {otm1} CE + {itm1} PE. Credit ₹{cr}. BE: ₹{be_lo}–₹{be_hi}."}
        else:
            nc=max(5,(po1-po2)*2); w=otm1-atm; ml=max(1,w-nc); rr=round(ml/max(nc,1),1)
            return {"Strategy":"Iron Condor",
                    "CE Entry":f"Sell {otm1} CE @ ₹{po1} | Buy {otm2} CE @ ₹{po2}",
                    "PE Entry":f"Sell {itm1} PE @ ₹{po1} | Buy {itm2} PE @ ₹{po2}",
                    "CE Target":f"Stay below ₹{otm1}","PE Target":f"Stay above ₹{itm1}",
                    "Stop Loss":f"Exit breached side if loss > ₹{nc*2*lot:,}",
                    "Max Profit/Lot":f"₹{nc*lot:,}","Max Loss/Lot":f"₹{ml*lot:,}",
                    "Risk:Reward":f"1:{rr}","Rationale":f"Sideways swing. Iron Condor. Net credit ₹{nc}. Max loss ₹{ml}."}

def _sv(s,k): return s.get(k,"—")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

class BhavcopFetcher:
    """Equity CM bhavcopy — CMP, volume, turnover."""
    def fetch(self, dt):
        url=BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("Equity bhavcopy → %s",dt.strftime("%d-%b-%Y"))
        raw=_download(url,"CM-Bhav")
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
        data_vol=(df.sort_values(vol_c,ascending=False).head(TOP_N)
                  [[sym_c,vol_c,cls_c]].fillna(0).values.tolist())
        data_to =(df.sort_values(tov_c,ascending=False).head(TOP_N)
                  [[sym_c,tov_c,cls_c]].fillna(0).values.tolist())
        cmp_map =dict(zip(df[sym_c].astype(str),df[cls_c].fillna(0)))
        log.info("  → %d EQ rows",len(df))
        return data_vol,data_to,cmp_map
    def _unzip(self,raw):
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f,low_memory=False)
        except Exception as e: log.error("CM-Bhav ZIP: %s",e); return None
    def _filter_eq(self,df):
        ser_c=_pick_col(df,COL_MAP["series"]); sym_c=_pick_col(df,COL_MAP["symbol"])
        df=df[df[ser_c].astype(str).str.strip()=="EQ"].copy()
        return df[~df[sym_c].astype(str).str.contains(EXCLUDE_PATTERN,case=False,na=False)].reset_index(drop=True)


class DeliveryFetcher:
    """
    FIX Bug 2: Delivery % is in sec_bhavdata_full_{date}.csv — NOT in CM bhavcopy.
    Columns include: SYMBOL, SERIES, DELIV_QTY, DELIV_PER
    """
    def fetch(self, ist_today) -> dict:
        """Returns {symbol: delivery_pct_float}"""
        for cand in _trading_days_back(ist_today, 5):
            # NSE uses ddmmyyyy format for this file
            url = DELIVERY_URL.format(date=cand.strftime("%d%m%Y"))
            raw = _download(url, f"Delivery-{cand.strftime('%d%b')}")
            if raw is None:
                continue
            try:
                text = raw.decode("utf-8", errors="replace")
                df   = pd.read_csv(io.StringIO(text))
                df.columns = [c.strip().upper() for c in df.columns]
                # Find symbol and delivery % columns
                sym_c  = next((c for c in df.columns if c in ("SYMBOL","TCKRSYMB")), None)
                ser_c  = next((c for c in df.columns if "SERIES" in c), None)
                pct_c  = next((c for c in df.columns if "DELIV_PER" in c or "DELPCT" in c
                               or "%DLY" in c or "DELV_PER" in c), None)
                if not sym_c or not pct_c:
                    log.warning("  Delivery CSV cols not found: %s", list(df.columns))
                    continue
                # Keep EQ series only
                if ser_c:
                    df = df[df[ser_c].astype(str).str.strip()=="EQ"]
                df[pct_c] = pd.to_numeric(df[pct_c], errors="coerce")
                result = dict(zip(df[sym_c].astype(str).str.strip(),
                                  df[pct_c].fillna(0)))
                log.info("  Delivery %%: %d symbols from %s", len(result), cand.strftime("%d-%b-%Y"))
                return result
            except Exception as e:
                log.warning("  Delivery parse error: %s", e)
        log.warning("  Delivery data unavailable — all '—'")
        return {}


class FOBhavcopFetcher:
    """
    FIX Bug 1: FO bhavcopy for OI/PCR/Max Pain.
    Uses NSESession (with cookie warm-up) to bypass 403.
    """
    FO_COL = {
        "symbol": ["TckrSymb","SYMBOL","FinInstrmNm"],
        "option": ["OptnTp","OPTION_TYP","OptionType"],
        "strike": ["StrkPric","STRIKE_PR","StrikePrice"],
        "oi":     ["OpnIntrst","OPEN_INT","OpenInterest","OI"],
        "expiry": ["XpryDt","EXPIRY_DT","ExpiryDate"],
    }
    def fetch(self, dt):
        url=FO_BHAV_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("FO bhavcopy → %s",dt.strftime("%d-%b-%Y"))
        raw=_download(url,"FO-Bhav")
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
            # Keep near-month expiry only
            try:
                exp_c=_pick_col(df,self.FO_COL["expiry"])
                df[exp_c]=pd.to_datetime(df[exp_c],errors="coerce")
                df=df[df[exp_c]==df[exp_c].min()]
            except Exception: pass
            oi_ce,oi_pe,oi_by={},{},{}
            for _,row in df.iterrows():
                sym=row[sym_c]; opt=row[opt_c]; strike=row[str_c]; oi=row[oi_c]
                if opt=="CE":
                    oi_ce[sym]=oi_ce.get(sym,0)+oi
                    oi_by.setdefault(sym,{}).setdefault(strike,{"CE":0,"PE":0})
                    oi_by[sym][strike]["CE"]+=oi
                elif opt=="PE":
                    oi_pe[sym]=oi_pe.get(sym,0)+oi
                    oi_by.setdefault(sym,{}).setdefault(strike,{"CE":0,"PE":0})
                    oi_by[sym][strike]["PE"]+=oi
            oi_map={s:oi_ce.get(s,0)+oi_pe.get(s,0) for s in set(list(oi_ce)+list(oi_pe))}
            log.info("  → FO OI: %d symbols",len(oi_map))
            return oi_map,oi_ce,oi_pe,oi_by
        except Exception as e:
            log.warning("FO-Bhav parse: %s",e); return {},{},{},{}


class IndexPriceFetcher:
    """Index CMP + India VIX from ind_close_all CSV."""
    def fetch(self, ist_today):
        for cand in _trading_days_back(ist_today, LOOKBACK_DAYS):
            data=self._one(cand)
            if data:
                log.info("Index prices %s: %s",cand.strftime("%d-%b-%Y"),
                         {k:f"₹{v:,.0f}" for k,v in data.items() if k!="VIX"})
                return data
        log.warning("Index CSV unavailable — fallback prices")
        return dict(INDEX_FALLBACK_CMP)
    def fetch_history(self, ist_today, days=HISTORY_DAYS):
        history,found={},0
        for cand in _trading_days_back(ist_today, days+15):
            dd=self._one(cand)
            if not dd: continue
            for sym,price in dd.items():
                history.setdefault(sym,[]).append(price)
            found+=1
            if found>=days: break
        return {s:list(reversed(v)) for s,v in history.items()}
    def _one(self, dt):
        url=INDEX_CSV_URL.format(date=dt.strftime("%d%m%Y"))
        raw=_download(url,f"IndexCSV-{dt.strftime('%d%b')}")
        if raw is None: return {}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")))
            df.columns=[c.strip() for c in df.columns]
            nc=next((c for c in df.columns if "index" in c.lower() and "name" in c.lower()),None)
            cc=next((c for c in df.columns if "clos" in c.lower()),None)
            if not nc or not cc: return {}
            df[cc]=pd.to_numeric(df[cc].astype(str).str.replace(",",""),errors="coerce")
            result={}
            for _,row in df.iterrows():
                sym=INDEX_NAME_MAP.get(str(row[nc]).strip()) or INDEX_NAME_MAP.get(str(row[nc]).strip().title())
                if sym and pd.notna(row[cc]) and row[cc]>0:
                    result[sym]=float(row[cc])
            for sym,fb in INDEX_FALLBACK_CMP.items():
                if sym not in result: result[sym]=fb
            return result
        except Exception as e: log.warning("IndexCSV: %s",e); return {}


class Week52Fetcher:
    """FIX Bug 3: 52-week High/Low — uses NSESession cookies."""
    def fetch(self) -> tuple:
        raw=_download(WEEK52_URL,"52wk")
        if raw is None: return {},{}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")))
            df.columns=[c.strip() for c in df.columns]
            sym_c=next((c for c in df.columns if "symbol" in c.lower()),None)
            hi_c =next((c for c in df.columns if "high"   in c.lower()),None)
            lo_c =next((c for c in df.columns if "low"    in c.lower()),None)
            if not all([sym_c,hi_c,lo_c]):
                log.warning("  52wk cols not found: %s",list(df.columns)); return {},{}
            df[hi_c]=pd.to_numeric(df[hi_c],errors="coerce")
            df[lo_c]=pd.to_numeric(df[lo_c],errors="coerce")
            hi=dict(zip(df[sym_c].astype(str).str.strip(),df[hi_c].fillna(0)))
            lo=dict(zip(df[sym_c].astype(str).str.strip(),df[lo_c].fillna(0)))
            log.info("  52wk: %d symbols",len(hi)); return hi,lo
        except Exception as e: log.warning("52wk: %s",e); return {},{}


class LotSizeFetcher:
    def fetch(self):
        raw=_download(MKTLOTS_URL,"LotSizes")
        if raw is None: return {}
        try:
            df=pd.read_csv(io.StringIO(raw.decode("utf-8",errors="replace")),header=1,dtype=str)
            df.columns=[c.strip() for c in df.columns]
            sc,lc=df.columns[0],df.columns[1]
            df[sc]=df[sc].str.strip()
            df[lc]=pd.to_numeric(df[lc].str.replace(",",""),errors="coerce")
            result=dict(zip(df[sc].dropna(),df[lc].dropna().astype(int)))
            log.info("Lot sizes: %d symbols",len(result)); return result
        except Exception as e: log.warning("LotSizes: %s",e); return {}


class EquityHistoryFetcher:
    """FIX Bug 4: HISTORY_DAYS=35, fetch up to 50 calendar days back."""
    def fetch(self, ist_today, days=HISTORY_DAYS):
        history,fetcher,found={},BhavcopFetcher(),0
        for cand in _trading_days_back(ist_today, days+15):
            res=fetcher.fetch(cand)
            if not res: continue
            _,_,cmap=res
            for sym,cls in cmap.items():
                if cls and cls>0: history.setdefault(sym,[]).append(cls)
            found+=1
            if found>=days: break
        result={s:list(reversed(v)) for s,v in history.items()}
        log.info("  Equity history: %d symbols × up to %d days",len(result),days)
        return result


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — SHEET HEADERS
# ══════════════════════════════════════════════════════════════════════════════

OPT_HEADERS = [
    "Sr.","Company / Index Name","NSE Symbol","Sector / Type",
    "Lot Size\n(Units)","CMP ₹\n(Approx.)","Contract\nValue ₹",
    "Near-Month\nExpiry","Mid-Month\nExpiry","Far-Month\nExpiry",
    "Approx.\nATM Call ₹\n(Near Expiry)","Approx.\nATM Put ₹\n(Near Expiry)",
    "Call Premium\nPaid (1 Lot) ₹","Put Premium\nPaid (1 Lot) ₹",
    "Option Seller\nMargin ₹\n(~20% Contract)",
    "Intraday\nTrend","Swing\nTrend",
    "Options Strategy\n(Intraday)","Options Strategy\n(Swing)",
    "Intraday Signal:\nBest Strategy","Intraday:\nCE Entry","Intraday:\nPE Entry",
    "Intraday:\nCE Target","Intraday:\nPE Target","Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹","Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward","Intraday:\nRationale",
    "Swing Signal:\nBest Strategy","Swing:\nCE Entry","Swing:\nPE Entry",
    "Swing:\nCE Target","Swing:\nPE Target","Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹","Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward","Swing:\nRationale",
    # 14 analytics
    "Open Interest\n(OI — Lots)","OI Change\n(vs Prev Day)",
    "PCR\n(Put-Call Ratio)","52-Week\nHigh ₹","52-Week\nLow ₹",
    "Delivery\n%","India\nVIX","IV %\n(Impl. Volatility)",
    "Max Pain\nStrike ₹","Support\n₹","Resistance\n₹",
    "Beta\nvs Nifty","RSI\n(14-day)","MACD Signal\n(12/26/9)",
    "Notes",
]   # 54 columns

FUT_HEADERS = [
    "Sr.","Company / Index Name","NSE Symbol","Sector / Type",
    "Lot Size\n(Units)","CMP ₹\n(Approx.)","Contract\nValue ₹",
    "Futures\nMargin %","Futures\nMargin Req. ₹",
    "Near-Month\nExpiry","Mid-Month\nExpiry","Far-Month\nExpiry",
    "Intraday\nTrend","Swing\nTrend",
    "Intraday Signal:\nBest Strategy","Intraday:\nCE Entry","Intraday:\nPE Entry",
    "Intraday:\nCE Target","Intraday:\nPE Target","Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹","Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward","Intraday:\nRationale",
    "Swing Signal:\nBest Strategy","Swing:\nCE Entry","Swing:\nPE Entry",
    "Swing:\nCE Target","Swing:\nPE Target","Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹","Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward","Swing:\nRationale",
    # 14 analytics
    "Open Interest\n(OI — Lots)","OI Change\n(vs Prev Day)",
    "PCR\n(Put-Call Ratio)","52-Week\nHigh ₹","52-Week\nLow ₹",
    "Delivery\n%","India\nVIX","IV %\n(Impl. Volatility)",
    "Max Pain\nStrike ₹","Support\n₹","Resistance\n₹",
    "Beta\nvs Nifty","RSI\n(14-day)","MACD Signal\n(12/26/9)",
    "Notes","Last Updated",
]   # 50 columns


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — ROW BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_block(sym,ltp,lot,oi_map,oi_ce,oi_pe,oi_by,prev_oi,
                     wk52_hi,wk52_lo,delv_map,india_vix,equity_hist,nifty_hist):
    oi    =oi_map.get(sym,0)
    oi_ce_=oi_ce.get(sym,0)
    oi_pe_=oi_pe.get(sym,0)
    prev  =prev_oi.get(sym,0)
    oi_chg=(oi-prev) if (oi>0 and prev>0) else None
    pcr   =round(oi_pe_/oi_ce_,2) if oi_ce_>0 else None
    if pcr:
        pcr_s=f"{pcr} 🟢 Bullish" if pcr>1.2 else f"{pcr} 🔴 Bearish" if pcr<0.8 else f"{pcr} 🟡 Neutral"
    else: pcr_s="—"
    hi52  =wk52_hi.get(sym) or None
    lo52  =wk52_lo.get(sym) or None
    dlv   =delv_map.get(sym,0) or None
    vix_s =f"{india_vix:.2f}" if india_vix else "—"
    atm_p =_atm_premium(ltp)
    iv_s  =_calc_iv(ltp,atm_p) if ltp>0 else "—"
    mp    =_calc_max_pain(oi_by.get(sym,{}))
    hist  =equity_hist.get(sym,[])
    sup,res=_calc_support_resistance(hist)
    beta  =_calc_beta(hist,nifty_hist) if (hist and nifty_hist) else None
    rsi   =_calc_rsi(hist)
    _,_,_,macd_label=_calc_macd(hist)
    return [
        int(oi)      if oi      else "—",
        int(oi_chg)  if oi_chg is not None else "—",
        pcr_s,
        round(hi52,2) if hi52  else "—",
        round(lo52,2) if lo52  else "—",
        f"{dlv:.1f}%" if dlv   else "—",
        vix_s,
        iv_s,
        f"₹{mp:,}"   if mp     else "—",
        f"₹{sup:,.2f}" if sup  else "—",
        f"₹{res:,.2f}" if res  else "—",
        str(beta)    if beta is not None else "—",
        _rsi_label(rsi),
        macd_label,
    ]   # 14 items

def _build_opt_row(sr,name,sym,sector,lot,ltp,expiries,history,analytics,note=""):
    en,em,ef=expiries; hist=history.get(sym,[])
    it=_trend(hist[-5:] if len(hist)>=5 else hist); st=_trend(hist)
    cval=round(lot*ltp); ac=_atm_premium(ltp); ap=_atm_premium(ltp)
    si=_strategy_engine(ltp,ac,it,st,lot,"intraday")
    ss=_strategy_engine(ltp,ac,it,st,lot,"swing")
    return ([sr,name,sym,sector,lot,round(ltp,2),cval,en,em,ef,
             ac,ap,ac*lot,ap*lot,round(cval*0.20),
             _trend_label(it),_trend_label(st),si["Strategy"],ss["Strategy"],
             si["Strategy"],_sv(si,"CE Entry"),_sv(si,"PE Entry"),
             _sv(si,"CE Target"),_sv(si,"PE Target"),_sv(si,"Stop Loss"),
             _sv(si,"Max Profit/Lot"),_sv(si,"Max Loss/Lot"),_sv(si,"Risk:Reward"),_sv(si,"Rationale"),
             ss["Strategy"],_sv(ss,"CE Entry"),_sv(ss,"PE Entry"),
             _sv(ss,"CE Target"),_sv(ss,"PE Target"),_sv(ss,"Stop Loss"),
             _sv(ss,"Max Profit/Lot"),_sv(ss,"Max Loss/Lot"),_sv(ss,"Risk:Reward"),_sv(ss,"Rationale"),
            ] + analytics + [note])  # 39+14+1 = 54

def _build_fut_row(sr,name,sym,sector,lot,ltp,mp,expiries,history,analytics,note=""):
    en,em,ef=expiries; hist=history.get(sym,[])
    it=_trend(hist[-5:] if len(hist)>=5 else hist); st=_trend(hist)
    cval=round(lot*ltp); ac=_atm_premium(ltp)
    si=_strategy_engine(ltp,ac,it,st,lot,"intraday")
    ss=_strategy_engine(ltp,ac,it,st,lot,"swing")
    return ([sr,name,sym,sector,lot,round(ltp,2),cval,
             f"{mp}%",round(cval*mp/100),en,em,ef,
             _trend_label(it),_trend_label(st),
             si["Strategy"],_sv(si,"CE Entry"),_sv(si,"PE Entry"),
             _sv(si,"CE Target"),_sv(si,"PE Target"),_sv(si,"Stop Loss"),
             _sv(si,"Max Profit/Lot"),_sv(si,"Max Loss/Lot"),_sv(si,"Risk:Reward"),_sv(si,"Rationale"),
             ss["Strategy"],_sv(ss,"CE Entry"),_sv(ss,"PE Entry"),
             _sv(ss,"CE Target"),_sv(ss,"PE Target"),_sv(ss,"Stop Loss"),
             _sv(ss,"Max Profit/Lot"),_sv(ss,"Max Loss/Lot"),_sv(ss,"Risk:Reward"),_sv(ss,"Rationale"),
            ] + analytics + [note,_ist_now()])  # 34+14+2 = 50

def build_all_rows(lot_sizes,equity_cmp,index_cmp,equity_hist,index_hist,
                   oi_map,oi_ce,oi_pe,oi_by,prev_oi,wk52_hi,wk52_lo,
                   delv_map,expiries):
    fut_rows,opt_rows,sr=[],[],1
    nifty_hist=index_hist.get("NIFTY") or equity_hist.get("NIFTY",[])
    india_vix =index_cmp.get("VIX")
    def ana(sym,ltp,lot):
        return _analytics_block(sym,ltp,lot,oi_map,oi_ce,oi_pe,oi_by,prev_oi,
                                wk52_hi,wk52_lo,delv_map,india_vix,equity_hist,nifty_hist)
    # Indices first
    log.info("Building index rows…")
    for sym,name,sector,margin_pct in INDEX_META:
        lot=lot_sizes.get(sym,FALLBACK_LOTS.get(sym,0))
        ltp=index_cmp.get(sym,0)
        if lot==0: continue
        if ltp==0: ltp=INDEX_FALLBACK_CMP.get(sym,0)
        if ltp==0: continue
        hist=index_hist.get(sym) or equity_hist.get(sym,[])
        h={sym:hist}; a=ana(sym,ltp,lot)
        fut_rows.append(_build_fut_row(sr,name,sym,sector,lot,ltp,margin_pct,expiries,h,a,"Index Future"))
        opt_rows.append(_build_opt_row(sr,name,sym,sector,lot,ltp,expiries,h,a,"Index Option"))
        log.info("  %-12s CMP=₹%-8s Lot=%d",sym,f"{ltp:,.0f}",lot)
        sr+=1
    # Stocks
    log.info("Building stock rows…")
    skipped=0
    for sym in sorted((set(lot_sizes)|set(FALLBACK_LOTS))-INDEX_SYMS):
        lot=lot_sizes.get(sym,FALLBACK_LOTS.get(sym,0))
        ltp=equity_cmp.get(sym,0)
        if lot==0 or ltp==0: skipped+=1; continue
        a=ana(sym,ltp,lot)
        fut_rows.append(_build_fut_row(sr,sym,sym,SECTOR_MAP.get(sym,"Equity"),lot,ltp,20,expiries,equity_hist,a))
        opt_rows.append(_build_opt_row(sr,sym,sym,SECTOR_MAP.get(sym,"Equity"),lot,ltp,expiries,equity_hist,a))
        sr+=1
    log.info("Rows — Futures:%d  Options:%d  Skipped:%d",len(fut_rows),len(opt_rows),skipped)
    return fut_rows,opt_rows


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — GOOGLE SHEETS WRITER
# ══════════════════════════════════════════════════════════════════════════════

class SheetsWriter:
    def __init__(self,creds_json):
        creds=ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json),GSHEETS_SCOPES)
        self._client=gspread.authorize(creds)
    def _get_or_create(self,ss,title,cols=60):
        try: return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating tab: '%s'",title)
            return ss.add_worksheet(title=title,rows=600,cols=cols)
    def open_all(self):
        ss=self._client.open_by_key(SPREADSHEET_ID)
        return (self._get_or_create(ss,SHEET_VOLUME,  cols=15),
                self._get_or_create(ss,SHEET_TURNOVER,cols=15),
                self._get_or_create(ss,SHEET_FUTURES, cols=55),
                self._get_or_create(ss,SHEET_OPTIONS, cols=60))

    def write_vol_turnover(self, ws_vol, ws_to, data_vol, data_to, fetched_date):
        """
        Write ONLY columns A–C, rows 2 onwards.
        ─────────────────────────────────────────────────────────────────────
        DELIBERATELY does NOT call ws.clear() so that:
          • Your Google formulas in columns D, E, F, … are preserved
          • All cell formatting / conditional formatting stays intact
          • Column widths, frozen rows, filters are untouched
          • Row 1 headers you have manually set are never overwritten

        Only the NSE data cells (A2:C251) are updated each run.
        The status timestamp is written to A1 of each sheet so you
        can see when data was last refreshed without touching headers.
        ─────────────────────────────────────────────────────────────────────
        """
        status = f"Last updated: {fetched_date}  |  {_ist_now()}"

        datasets = [
            (ws_vol, data_vol, "Symbol", "Volume (Qty)", "Close ₹"),
            (ws_to,  data_to,  "Symbol", "Turnover ₹",  "Close ₹"),
        ]

        for ws, data, col_a, col_b, col_c in datasets:
            n = len(data)
            if n == 0:
                log.warning("'%s' — no data rows to write", ws.title)
                continue

            # ── Step 1: Write status timestamp to A1 only ──────────────
            # Uses USER_ENTERED so the cell is treated as plain text.
            # Does NOT touch B1, C1, D1 or any other header cell.
            ws.update(
                range_name="A1",
                values=[[status]],
                value_input_option="USER_ENTERED",
            )

            # ── Step 2: Overwrite ONLY A2:C(n+1) with fresh NSE data ───
            # RAW mode: prevents Sheets from misinterpreting symbols
            # (e.g. "M&M" or numbers) as formulas or dates.
            # Columns D onwards — where your formulas live — are NEVER touched.
            ws.update(
                range_name=f"A2:C{n + 1}",
                values=data,
                value_input_option="RAW",
            )

            # ── Step 3: Clear stale rows below the new data ─────────────
            # If a previous run had more rows, blank them out so old
            # symbols don't linger. Clears A:C only — formulas in D+ safe.
            # TOP_N is always 250 so max rows = 251 (header + 250 data).
            # We clear up to row 260 as a safe buffer.
            clear_from = n + 2          # first row after new data
            clear_to   = TOP_N + 10    # safe upper bound
            if clear_from <= clear_to:
                blank_rows = [["", "", ""] for _ in range(clear_to - clear_from + 1)]
                ws.update(
                    range_name=f"A{clear_from}:C{clear_to}",
                    values=blank_rows,
                    value_input_option="RAW",
                )

            log.info(
                "'%s' → %d data rows written to A2:C%d  "
                "(formulas in D+ preserved, formatting intact)",
                ws.title, n, n + 1,
            )

    def write_fo_sheet(self,ws,headers,rows,title):
        all_data=[headers]+rows; n_cols=len(headers)
        ws.clear(); time.sleep(1)
        for start in range(0,len(all_data),WRITE_CHUNK):
            end=min(start+WRITE_CHUNK,len(all_data))
            ws.update(range_name=f"A{start+1}:{_col_letter(n_cols)}{end}",
                      values=all_data[start:end],value_input_option="RAW")
            log.info("  '%s' rows %d–%d",title,start+1,end)
            if end<len(all_data): time.sleep(1.5)
        log.info("'%s' done — %d rows × %d cols",title,len(rows),n_cols)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    creds_json=os.environ.get("GCP_CREDENTIALS")
    if not creds_json: raise EnvironmentError("GCP_CREDENTIALS not set.")
    ist_today=_ist_today()
    log.info("═"*60)
    log.info("NSE Auto-Sheet v7  —  %s",ist_today.strftime("%d-%b-%Y %H:%M IST"))
    log.info("═"*60)

    # Warm up NSE session ONCE — all fetchers share it
    _NSE.warm_up()

    writer=SheetsWriter(creds_json)
    ws_vol,ws_to,ws_fut,ws_opt=writer.open_all()

    # ── Equity bhavcopy ───────────────────────────────────────────────────────
    log.info("── Equity bhavcopy ──────────────────────────────────────")
    eq=BhavcopFetcher(); result=None; fetched_date=""
    for cand in _trading_days_back(ist_today,LOOKBACK_DAYS):
        result=eq.fetch(cand)
        if result: fetched_date=cand.strftime("%d-%b-%Y"); break
    if not result: raise RuntimeError("No equity bhavcopy found.")
    data_vol,data_to,equity_cmp=result
    writer.write_vol_turnover(ws_vol,ws_to,data_vol,data_to,fetched_date)

    # ── Delivery % (separate file) ────────────────────────────────────────────
    log.info("── Delivery %% ───────────────────────────────────────────")
    delv_map=DeliveryFetcher().fetch(ist_today)

    # ── FO bhavcopy (OI/PCR/Max Pain) ────────────────────────────────────────
    log.info("── FO bhavcopy (OI) ─────────────────────────────────────")
    fo=FOBhavcopFetcher()
    bhavcopy_dt=datetime.strptime(fetched_date,"%d-%b-%Y")
    oi_map,oi_ce,oi_pe,oi_by=fo.fetch(bhavcopy_dt)
    # Previous day OI for OI-change
    prev_oi={}
    for cand in _trading_days_back(bhavcopy_dt,4):
        pm,_,_,_=fo.fetch(cand)
        if pm: prev_oi=pm; break

    # ── Index prices + VIX ───────────────────────────────────────────────────
    log.info("── Index prices + VIX ───────────────────────────────────")
    idx=IndexPriceFetcher()
    index_cmp=idx.fetch(ist_today)
    log.info("  India VIX: %s",index_cmp.get("VIX"))

    # ── Lot sizes ─────────────────────────────────────────────────────────────
    log.info("── Lot sizes ────────────────────────────────────────────")
    lot_sizes=LotSizeFetcher().fetch() or FALLBACK_LOTS

    # ── 52-Week High/Low ─────────────────────────────────────────────────────
    log.info("── 52-Week High/Low ─────────────────────────────────────")
    wk52_hi,wk52_lo=Week52Fetcher().fetch()

    # ── Price history ────────────────────────────────────────────────────────
    log.info("── Equity history (%d trading days) ─────────────────────",HISTORY_DAYS)
    equity_hist=EquityHistoryFetcher().fetch(ist_today,days=HISTORY_DAYS)
    log.info("── Index history (%d trading days) ──────────────────────",HISTORY_DAYS)
    index_hist =idx.fetch_history(ist_today,days=HISTORY_DAYS)

    # ── Expiries ──────────────────────────────────────────────────────────────
    expiries=_expiry_dates()
    log.info("── Expiries: %s | %s | %s",*expiries)

    # ── Build rows ────────────────────────────────────────────────────────────
    log.info("── Building F&O rows ────────────────────────────────────")
    fut_rows,opt_rows=build_all_rows(
        lot_sizes,equity_cmp,index_cmp,equity_hist,index_hist,
        oi_map,oi_ce,oi_pe,oi_by,prev_oi,wk52_hi,wk52_lo,delv_map,expiries)

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

if __name__=="__main__":
    main()
