"""
NSE Bhavcopy → Google Sheets Updater  (v3 — Advanced Options Strategy Edition)
================================================================================
Sheets written:
  1. Top 250 Stocks    – top 250 by volume
  2. Top 250 Turnover  – top 250 by turnover
  3. Futures F&O       – exact headers + intraday & swing strategy columns
  4. Options F&O       – exact headers + best strategy, CE/PE entry & target
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
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID  = "1RAEu29NQlc6de9Y5E_oME537LMvn1mruVOYRL6EEVM4"
SHEET_VOLUME    = "Top 250 Stocks"
SHEET_TURNOVER  = "Top 250 Turnover"
SHEET_FUTURES   = "Futures F&O"
SHEET_OPTIONS   = "Options F&O"
STATUS_CELL_VOL = "K2"
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _pick_col(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} in {list(df.columns)}")

def _ist_now():
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%b-%Y %H:%M IST")

def _last_thursday(year, month):
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - 3) % 7)

def _expiry_dates():
    today = (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()
    expiries, m, y = [], today.month, today.year
    for _ in range(5):
        exp = _last_thursday(y, m)
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

def _atm_premium(ltp, days=25, iv=0.28):
    prem = 0.4 * iv * math.sqrt(max(days, 1) / 252) * ltp
    return max(5, int(round(prem / 5) * 5))

def _trend(closes):
    if len(closes) < 5:
        return "Sideways"
    avg5  = sum(closes[-5:]) / 5
    avg20 = sum(closes[-min(20, len(closes)):]) / min(20, len(closes))
    diff  = (avg5 - avg20) / avg20 * 100 if avg20 else 0
    if diff >  0.8: return "Bullish"
    if diff < -0.8: return "Bearish"
    return "Sideways"

def _col_letter(n):
    r = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        r = chr(65 + rem) + r
    return r


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def _round_strike(ltp, direction="ce"):
    """Round to nearest 50 (indices) or nearest 5 (stocks), ATM."""
    if ltp > 5000:
        step = 100
    elif ltp > 1000:
        step = 50
    elif ltp > 200:
        step = 10
    else:
        step = 5
    return int(round(ltp / step) * step)

def _strategy_engine(ltp, atm_prem, intra_trend, swing_trend, lot, timeframe="intraday"):
    """
    Selects the BEST options strategy for the given trend and timeframe.
    Returns dict with keys:
      strategy_name, ce_entry, pe_entry, ce_target, pe_target,
      max_profit, max_loss, risk_reward, rationale
    """
    trend = intra_trend if timeframe == "intraday" else swing_trend
    atm   = _round_strike(ltp)
    otm1  = atm + (50 if ltp > 5000 else 20 if ltp > 1000 else 10 if ltp > 200 else 5)
    otm2  = atm + 2 * (50 if ltp > 5000 else 20 if ltp > 1000 else 10 if ltp > 200 else 5)
    itm1  = atm - (50 if ltp > 5000 else 20 if ltp > 1000 else 10 if ltp > 200 else 5)

    prem_atm  = atm_prem
    prem_otm1 = max(5, int(atm_prem * 0.55 / 5) * 5)
    prem_otm2 = max(5, int(atm_prem * 0.30 / 5) * 5)
    prem_itm1 = max(5, int(atm_prem * 1.55 / 5) * 5)

    # ── BULLISH strategies ──────────────────────────────────────────────
    if trend == "Bullish":
        if timeframe == "intraday":
            # Long Call — simple directional for intraday
            name        = "Long Call"
            ce_entry    = f"Buy {atm} CE @ ₹{prem_atm}"
            pe_entry    = "—"
            ce_target   = f"₹{prem_atm * 2} (2× premium)"
            pe_target   = "—"
            stoploss    = f"₹{max(5, int(prem_atm * 0.5))} (50% of premium)"
            max_profit  = f"₹{prem_atm * lot} (unlimited upside)"
            max_loss    = f"₹{prem_atm * lot} (premium paid)"
            rr          = "2:1 (target 2× premium)"
            rationale   = "Strong intraday bullish momentum. ATM call captures move with limited risk = premium paid."
        else:
            # Bull Call Spread — for swing (defined risk)
            name        = "Bull Call Spread"
            ce_entry    = f"Buy {atm} CE @ ₹{prem_atm}  |  Sell {otm1} CE @ ₹{prem_otm1}"
            pe_entry    = "—"
            net_debit   = prem_atm - prem_otm1
            max_gain    = (otm1 - atm) - net_debit
            ce_target   = f"₹{otm1} (upper strike at expiry)"
            pe_target   = "—"
            stoploss    = f"Exit if net debit erodes by 40% (₹{int(net_debit * 0.4)})"
            max_profit  = f"₹{max_gain * lot} per lot"
            max_loss    = f"₹{net_debit * lot} (net debit paid)"
            rr          = f"{round(max_gain / max(net_debit, 1), 1)}:1"
            rationale   = "Bullish swing with capped risk. Sell OTM call to reduce debit cost."

    # ── BEARISH strategies ──────────────────────────────────────────────
    elif trend == "Bearish":
        if timeframe == "intraday":
            # Long Put — simple directional
            name        = "Long Put (Bear Put)"
            ce_entry    = "—"
            pe_entry    = f"Buy {atm} PE @ ₹{prem_atm}"
            ce_target   = "—"
            pe_target   = f"₹{prem_atm * 2} (2× premium)"
            stoploss    = f"₹{max(5, int(prem_atm * 0.5))} (50% of premium)"
            max_profit  = f"₹{prem_atm * lot} (strike - 0 downside)"
            max_loss    = f"₹{prem_atm * lot} (premium paid)"
            rr          = "2:1"
            rationale   = "Strong intraday bearish momentum. ATM put captures downside with defined risk."
        else:
            # Bear Put Spread — swing
            name        = "Bear Put Spread"
            ce_entry    = "—"
            pe_entry    = f"Buy {atm} PE @ ₹{prem_atm}  |  Sell {itm1} PE @ ₹{prem_otm1}"
            net_debit   = prem_atm - prem_otm1
            max_gain    = (atm - itm1) - net_debit
            ce_target   = "—"
            pe_target   = f"₹{itm1} (lower strike at expiry)"
            stoploss    = f"Exit if net debit erodes 40% (₹{int(net_debit * 0.4)})"
            max_profit  = f"₹{max_gain * lot} per lot"
            max_loss    = f"₹{net_debit * lot} (net debit paid)"
            rr          = f"{round(max_gain / max(net_debit, 1), 1)}:1"
            rationale   = "Bearish swing with limited risk. Selling lower strike put reduces debit."

    # ── SIDEWAYS strategies ─────────────────────────────────────────────
    else:
        if timeframe == "intraday":
            # Short Strangle — collect premium in range
            otm_c = otm1
            otm_p = itm1
            prem_c = prem_otm1
            prem_p = prem_otm1
            total_credit = prem_c + prem_p
            name      = "Short Strangle"
            ce_entry  = f"Sell {otm_c} CE @ ₹{prem_c}"
            pe_entry  = f"Sell {otm_p} PE @ ₹{prem_p}"
            ce_target = f"CE expires worthless (stay below ₹{otm_c})"
            pe_target = f"PE expires worthless (stay above ₹{otm_p})"
            stoploss  = f"Exit if loss = 1× total credit (₹{total_credit * lot})"
            max_profit = f"₹{total_credit * lot} (full credit if price stays in range)"
            max_loss   = "Unlimited beyond breakeven strikes"
            rr         = "1:1 (requires margin)"
            rationale  = "Sideways intraday — collect premium from both sides. Needs margin. Best when IV is high."
        else:
            # Iron Condor — swing sideways with 4 legs
            otm_ce2 = otm2
            otm_ce1 = otm1
            otm_pe1 = itm1
            otm_pe2 = atm - 2 * (50 if ltp > 5000 else 20 if ltp > 1000 else 10 if ltp > 200 else 5)
            sell_c  = prem_otm1
            buy_c   = prem_otm2
            sell_p  = prem_otm1
            buy_p   = prem_otm2
            net_credit = (sell_c - buy_c) + (sell_p - buy_p)
            net_credit = max(5, net_credit)
            name     = "Iron Condor"
            ce_entry = f"Sell {otm_ce1} CE @ ₹{sell_c}  |  Buy {otm_ce2} CE @ ₹{buy_c}"
            pe_entry = f"Sell {otm_pe1} PE @ ₹{sell_p}  |  Buy {otm_pe2} PE @ ₹{buy_p}"
            ce_target = f"Price stays below ₹{otm_ce1}"
            pe_target = f"Price stays above ₹{otm_pe1}"
            stoploss  = f"Exit one side if breached; loss > 2× credit"
            max_profit = f"₹{net_credit * lot} (net credit × lot)"
            max_loss   = f"₹{((otm_ce1 - atm) - net_credit) * lot} (wing width - credit)"
            rr         = f"1:{round(((otm_ce1 - atm) - net_credit) / max(net_credit, 1), 1)}"
            rationale  = "Classic 4-leg swing strategy for sideways market. Defined max loss with limited profit."

    return {
        "Strategy":          name,
        "CE Entry":          ce_entry,
        "PE Entry":          pe_entry,
        "CE Target":         ce_target,
        "PE Target":         pe_target,
        "Stop Loss":         stoploss,
        "Max Profit/Lot":    max_profit,
        "Max Loss/Lot":      max_loss,
        "Risk:Reward":       rr,
        "Rationale":         rationale,
    }


# ─────────────────────────────────────────────────────────────────────────────
# BHAVCOPY FETCHER
# ─────────────────────────────────────────────────────────────────────────────
class BhavcopFetcher:
    def fetch(self, dt):
        url = BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
        log.info("Trying %s", dt.strftime("%d-%m-%Y"))
        raw = self._download(url)
        if raw is None: return None
        df = self._read_zip(raw)
        if df is None or df.empty: return None
        df = self._clean(df)
        if df.empty: return None

        sym  = _pick_col(df, COL_CANDIDATES["symbol"])
        cls  = _pick_col(df, COL_CANDIDATES["close"])
        vol  = _pick_col(df, COL_CANDIDATES["volume"])
        tov  = _pick_col(df, COL_CANDIDATES["turnover"])
        for c in [cls, vol, tov]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        data_vol = df.sort_values(vol, ascending=False).head(TOP_N)[[sym, vol, cls]].values.tolist()
        data_to  = df.sort_values(tov, ascending=False).head(TOP_N)[[sym, tov, cls]].values.tolist()
        cmp_map  = dict(zip(df[sym].astype(str), df[cls].fillna(0)))
        log.info("Bhavcopy: %d EQ rows", len(df))
        return data_vol, data_to, cmp_map

    def _download(self, url):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200: return r.content
                log.warning("HTTP %s (attempt %d)", r.status_code, attempt)
            except requests.RequestException as e:
                log.warning("Request error attempt %d: %s", attempt, e)
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
        return None

    def _read_zip(self, raw):
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f, low_memory=False)
        except Exception as e:
            log.error("ZIP parse: %s", e)
            return None

    def _clean(self, df):
        ser = _pick_col(df, COL_CANDIDATES["series"])
        sym = _pick_col(df, COL_CANDIDATES["symbol"])
        df  = df[df[ser].astype(str).str.strip() == "EQ"].copy()
        return df[~df[sym].astype(str).str.contains(EXCLUDE_PATTERN, case=False, na=False)].reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# LOT SIZE FETCHER
# ─────────────────────────────────────────────────────────────────────────────
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

INDEX_META = [
    ("NIFTY",      "Nifty 50",           "Index", 13),
    ("BANKNIFTY",  "Bank Nifty",         "Index", 13),
    ("FINNIFTY",   "Nifty FinSvc",       "Index", 13),
    ("MIDCPNIFTY", "Nifty Midcap Sel",   "Index", 13),
    ("NIFTYNXT50", "Nifty Next 50",      "Index", 13),
]
INDEX_SYMS = {r[0] for r in INDEX_META}


class LotSizeFetcher:
    def fetch(self):
        try:
            r = requests.get(MKTLOTS_URL, headers=NSE_HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                return {}
            df = pd.read_csv(io.StringIO(r.text), header=1, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            s, l = df.columns[0], df.columns[1]
            df[s] = df[s].str.strip()
            df[l] = pd.to_numeric(df[l].str.replace(",", ""), errors="coerce")
            df = df.dropna(subset=[l])
            result = dict(zip(df[s], df[l].astype(int)))
            log.info("Lot sizes loaded: %d", len(result))
            return result
        except Exception as e:
            log.warning("LotSizeFetcher: %s", e)
            return {}


class HistoryFetcher:
    def fetch(self, ist_today):
        history, fetcher, found = {}, BhavcopFetcher(), 0
        for days_back in range(1, 25):
            cand = ist_today - timedelta(days=days_back)
            if cand.weekday() >= 5: continue
            res = fetcher.fetch(cand)
            if not res: continue
            _, _, cmap = res
            for sym, cls in cmap.items():
                history.setdefault(sym, []).append(cls)
            found += 1
            if found >= 15: break
        return {s: list(reversed(v)) for s, v in history.items()}


# ─────────────────────────────────────────────────────────────────────────────
# EXACT SHEET HEADERS (as specified)
# ─────────────────────────────────────────────────────────────────────────────

# ── OPTIONS F&O headers ────────────────────────────────────────────────────
OPT_HEADERS = [
    "Sr.",
    "Company / Index Name",
    "NSE Symbol",
    "Sector / Type",
    "Lot Size\n(Units)",
    "CMP ₹\n(Approx.)",
    "Contract\nValue ₹",
    "Near-Month\nExpiry",
    "Mid-Month\nExpiry",
    "Far-Month\nExpiry",
    "Approx.\nATM Call ₹\n(Near Expiry)",
    "Approx.\nATM Put ₹\n(Near Expiry)",
    "Call Premium\nPaid (1 Lot) ₹",
    "Put Premium\nPaid (1 Lot) ₹",
    "Option Seller\nMargin ₹\n(~20% Contract)",
    "Intraday\nTrend",
    "Swing\nTrend",
    "Options Strategy\n(Intraday)",
    "Options Strategy\n(Swing)",
    # ── Intraday Signal columns ──
    "Intraday Signal:\nBest Strategy",
    "Intraday:\nCE Entry",
    "Intraday:\nPE Entry",
    "Intraday:\nCE Target",
    "Intraday:\nPE Target",
    "Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹",
    "Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward",
    "Intraday:\nRationale",
    # ── Swing Signal columns ──
    "Swing Signal:\nBest Strategy",
    "Swing:\nCE Entry",
    "Swing:\nPE Entry",
    "Swing:\nCE Target",
    "Swing:\nPE Target",
    "Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹",
    "Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward",
    "Swing:\nRationale",
    "Notes",
]

# ── FUTURES F&O headers ────────────────────────────────────────────────────
FUT_HEADERS = [
    "Sr.",
    "Company / Index Name",
    "NSE Symbol",
    "Sector / Type",
    "Lot Size\n(Units)",
    "CMP ₹\n(Approx.)",
    "Contract\nValue ₹",
    "Futures\nMargin %",
    "Futures\nMargin Req. ₹",
    "Near-Month\nExpiry",
    "Mid-Month\nExpiry",
    "Far-Month\nExpiry",
    "Intraday\nTrend",
    "Swing\nTrend",
    # Intraday Signal block
    "Intraday Signal:\nBest Strategy",
    "Intraday:\nCE Entry",
    "Intraday:\nPE Entry",
    "Intraday:\nCE Target",
    "Intraday:\nPE Target",
    "Intraday:\nStop Loss",
    "Intraday:\nMax Profit/Lot ₹",
    "Intraday:\nMax Loss/Lot ₹",
    "Intraday:\nRisk:Reward",
    "Intraday:\nRationale",
    # Swing Signal block
    "Swing Signal:\nBest Strategy",
    "Swing:\nCE Entry",
    "Swing:\nPE Entry",
    "Swing:\nCE Target",
    "Swing:\nPE Target",
    "Swing:\nStop Loss",
    "Swing:\nMax Profit/Lot ₹",
    "Swing:\nMax Loss/Lot ₹",
    "Swing:\nRisk:Reward",
    "Swing:\nRationale",
    "Notes",
    "Last Updated",
]


# ─────────────────────────────────────────────────────────────────────────────
# ROW BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
def _sig(s): return s["Strategy"]
def _sv(s, k): return s.get(k, "—")

def build_opt_row(sr, name, sym, sector, lot, ltp, expiries, history, note=""):
    exp_near, exp_mid, exp_far = expiries
    hist = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    contract_val = round(lot * ltp)
    atm_c = _atm_premium(ltp)
    atm_p = _atm_premium(ltp)
    call_lot = atm_c * lot
    put_lot  = atm_p * lot
    seller_margin = round(contract_val * 0.20)

    si = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "swing")

    i_trend_txt = f"🟢 {intra_t}" if intra_t == "Bullish" else f"🔴 {intra_t}" if intra_t == "Bearish" else f"🟡 {intra_t}"
    s_trend_txt = f"🟢 {swing_t}" if swing_t == "Bullish" else f"🔴 {swing_t}" if swing_t == "Bearish" else f"🟡 {swing_t}"

    intra_strat = si["Strategy"]
    swing_strat = ss["Strategy"]

    return [
        sr, name, sym, sector, lot, round(ltp, 2), contract_val,
        exp_near, exp_mid, exp_far,
        atm_c, atm_p, call_lot, put_lot, seller_margin,
        i_trend_txt, s_trend_txt,
        intra_strat, swing_strat,
        # Intraday signal block
        _sig(si), _sv(si,"CE Entry"), _sv(si,"PE Entry"),
        _sv(si,"CE Target"), _sv(si,"PE Target"), _sv(si,"Stop Loss"),
        _sv(si,"Max Profit/Lot"), _sv(si,"Max Loss/Lot"), _sv(si,"Risk:Reward"),
        _sv(si,"Rationale"),
        # Swing signal block
        _sig(ss), _sv(ss,"CE Entry"), _sv(ss,"PE Entry"),
        _sv(ss,"CE Target"), _sv(ss,"PE Target"), _sv(ss,"Stop Loss"),
        _sv(ss,"Max Profit/Lot"), _sv(ss,"Max Loss/Lot"), _sv(ss,"Risk:Reward"),
        _sv(ss,"Rationale"),
        note,
    ]

def build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries, history, note=""):
    exp_near, exp_mid, exp_far = expiries
    hist = history.get(sym, [])
    intra_t = _trend(hist[-5:] if len(hist) >= 5 else hist)
    swing_t = _trend(hist)
    contract_val = round(lot * ltp)
    margin_req   = round(contract_val * margin_pct / 100)
    atm_c = _atm_premium(ltp)

    si = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "intraday")
    ss = _strategy_engine(ltp, atm_c, intra_t, swing_t, lot, "swing")

    i_trend_txt = f"🟢 {intra_t}" if intra_t == "Bullish" else f"🔴 {intra_t}" if intra_t == "Bearish" else f"🟡 {intra_t}"
    s_trend_txt = f"🟢 {swing_t}" if swing_t == "Bullish" else f"🔴 {swing_t}" if swing_t == "Bearish" else f"🟡 {swing_t}"

    return [
        sr, name, sym, sector, lot, round(ltp, 2), contract_val,
        f"{margin_pct}%", margin_req,
        exp_near, exp_mid, exp_far,
        i_trend_txt, s_trend_txt,
        # Intraday signal
        _sig(si), _sv(si,"CE Entry"), _sv(si,"PE Entry"),
        _sv(si,"CE Target"), _sv(si,"PE Target"), _sv(si,"Stop Loss"),
        _sv(si,"Max Profit/Lot"), _sv(si,"Max Loss/Lot"), _sv(si,"Risk:Reward"),
        _sv(si,"Rationale"),
        # Swing signal
        _sig(ss), _sv(ss,"CE Entry"), _sv(ss,"PE Entry"),
        _sv(ss,"CE Target"), _sv(ss,"PE Target"), _sv(ss,"Stop Loss"),
        _sv(ss,"Max Profit/Lot"), _sv(ss,"Max Loss/Lot"), _sv(ss,"Risk:Reward"),
        _sv(ss,"Rationale"),
        note, _ist_now(),
    ]


def build_all_rows(lot_sizes, cmp_map, history, expiries):
    futures_rows, options_rows = [], []
    sr = 1

    # ── Index rows ──
    for sym, name, sector, margin_pct in INDEX_META:
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = cmp_map.get(sym, 0)
        if lot == 0 or ltp == 0: continue
        futures_rows.append(build_fut_row(sr, name, sym, sector, lot, ltp, margin_pct, expiries, history, "Index Future"))
        options_rows.append(build_opt_row(sr, name, sym, sector, lot, ltp, expiries, history, "Index Option"))
        sr += 1

    # ── Stock rows ──
    all_syms = sorted((set(lot_sizes.keys()) | set(FALLBACK_LOTS.keys())) - INDEX_SYMS)
    for sym in all_syms:
        lot = lot_sizes.get(sym, FALLBACK_LOTS.get(sym, 0))
        ltp = cmp_map.get(sym, 0)
        if lot == 0 or ltp == 0: continue
        name   = sym
        sector = SECTOR_MAP.get(sym, "Equity")
        futures_rows.append(build_fut_row(sr, name, sym, sector, lot, ltp, 20, expiries, history))
        options_rows.append(build_opt_row(sr, name, sym, sector, lot, ltp, expiries, history))
        sr += 1

    return futures_rows, options_rows


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS WRITER
# ─────────────────────────────────────────────────────────────────────────────
class SheetsWriter:
    def __init__(self, creds_json):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(creds_json), GSHEETS_SCOPES)
        self._client = gspread.authorize(creds)

    def _get_or_create(self, ss, title, cols=50):
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            log.info("Creating sheet: %s", title)
            return ss.add_worksheet(title=title, rows=600, cols=cols)

    def open_all(self):
        ss = self._client.open_by_key(SPREADSHEET_ID)
        return (
            self._get_or_create(ss, SHEET_VOLUME,   cols=15),
            self._get_or_create(ss, SHEET_TURNOVER, cols=15),
            self._get_or_create(ss, SHEET_FUTURES,  cols=40),
            self._get_or_create(ss, SHEET_OPTIONS,  cols=45),
        )

    def write_vol_tov(self, ws_vol, ws_to, data_vol, data_to, fetched_date):
        status = f"Data: {fetched_date} | Updated: {_ist_now()}"
        for ws, data in [(ws_vol, data_vol), (ws_to, data_to)]:
            n = len(data)
            ws.batch_update([
                {"range": f"A2:C{n+1}", "values": data},
                {"range": STATUS_CELL_VOL, "values": [[status]]},
            ], value_input_option="USER_ENTERED")
            log.info("'%s' updated: %d rows", ws.title, n)

    def write_sheet(self, ws, headers, rows, title):
        all_data = [headers] + rows
        n_rows   = len(all_data)
        n_cols   = len(headers)
        range_   = f"A1:{_col_letter(n_cols)}{n_rows}"
        ws.clear()
        # Write in chunks of 200 rows to avoid payload limits
        chunk = 200
        for start in range(0, n_rows, chunk):
            end   = min(start + chunk, n_rows)
            chunk_range = f"A{start+1}:{_col_letter(n_cols)}{end}"
            ws.update(range_name=chunk_range,
                      values=all_data[start:end],
                      value_input_option="USER_ENTERED")
            time.sleep(1)   # avoid rate limit
        log.info("'%s' updated: %d data rows, %d cols", title, len(rows), n_cols)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GCP_CREDENTIALS not set")

    ist_today = datetime.utcnow() + timedelta(hours=5, minutes=30)
    log.info("IST: %s", ist_today.strftime("%d-%b-%Y %H:%M"))

    # Connect
    writer = SheetsWriter(creds_json)
    ws_vol, ws_to, ws_fut, ws_opt = writer.open_all()

    # Fetch bhavcopy
    fetcher, result, fetched_date = BhavcopFetcher(), None, ""
    for days_back in range(LOOKBACK_DAYS + 1):
        cand = ist_today - timedelta(days=days_back)
        if cand.weekday() >= 5: continue
        result = fetcher.fetch(cand)
        if result:
            fetched_date = cand.strftime("%d-%b-%Y")
            break
    if not result:
        raise RuntimeError("No bhavcopy data found")
    data_vol, data_to, cmp_map = result

    # Write vol/turnover
    writer.write_vol_tov(ws_vol, ws_to, data_vol, data_to, fetched_date)

    # Lot sizes
    lot_sizes = LotSizeFetcher().fetch() or FALLBACK_LOTS

    # History for trends
    log.info("Fetching 15-day history for momentum…")
    history = HistoryFetcher().fetch(ist_today)

    # Expiries
    expiries = _expiry_dates()
    log.info("Expiries: %s | %s | %s", *expiries)

    # Build rows
    log.info("Building F&O rows…")
    fut_rows, opt_rows = build_all_rows(lot_sizes, cmp_map, history, expiries)

    # Write sheets
    writer.write_sheet(ws_fut, FUT_HEADERS, fut_rows, SHEET_FUTURES)
    writer.write_sheet(ws_opt, OPT_HEADERS, opt_rows, SHEET_OPTIONS)

    log.info("✅ All 4 sheets updated. Data from %s.", fetched_date)


if __name__ == "__main__":
    main()
