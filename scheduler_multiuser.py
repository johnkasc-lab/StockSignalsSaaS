"""
PROJECT 2 — Multi-User SaaS Signal Alerts
Same scanning logic as Project 1, but routes alerts to PAYING USERS
based on their watchlist and subscription status (users.csv).
NO executor/capital management here — this is a pure alert service.

Usage:
    python scheduler_project2.py                # continuous (local)
    python scheduler_project2.py --single-run   # one scan (GitHub Actions)
"""

import sys
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
import schedule
import time
import os

# ── Config ─────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")  # YOUR founder chat
USERS_FILE        = "users.csv"
LOG_FILE          = "saas_signals.csv"          # separate from P1 logs
ALERTED_FILE      = "saas_alerted_today.csv"   # separate from P1 dedup
SCAN_INTERVAL     = 5

# ── Same sectors as Project 1 ──────────────────────────────
SECTORS = {
    "Banking & Finance": [
        "HDFCBANK.NS","ICICIBANK.NS","SBIN.NS","KOTAKBANK.NS","AXISBANK.NS",
        "BAJFINANCE.NS","BAJAJFINSV.NS","INDUSINDBK.NS","AUBANK.NS","FEDERALBNK.NS",
        "IDFCFIRSTB.NS","BANDHANBNK.NS","RBLBANK.NS","PNB.NS","CANBK.NS",
        "BANKBARODA.NS","UNIONBANK.NS","INDIANB.NS","SBILIFE.NS","HDFCLIFE.NS",
        "ICICIPRULI.NS","ICICIGI.NS","MUTHOOTFIN.NS","MANAPPURAM.NS","CHOLAFIN.NS",
        "ABCAPITAL.NS","UJJIVANSFB.NS","EQUITASBNK.NS","ESAFSFB.NS","SURYODAY.NS",
        "UTKARSHBNK.NS","APTUS.NS","HOMEFIRST.NS","AAVAS.NS","CANFINHOME.NS",
        "ANGELONE.NS","IIFL.NS","MOTILALOFS.NS","5PAISA.NS","GEOJITFSL.NS",
    ],
    "Information Technology": [
        "TCS.NS","INFY.NS","WIPRO.NS","HCLTECH.NS","TECHM.NS","MPHASIS.NS",
        "PERSISTENT.NS","COFORGE.NS","KPITTECH.NS","TATAELXSI.NS","OFSS.NS",
        "CYIENT.NS","HFCL.NS","NAUKRI.NS",
    ],
    "Pharma & Healthcare": [
        "SUNPHARMA.NS","DRREDDY.NS","CIPLA.NS","DIVISLAB.NS","APOLLOHOSP.NS",
        "AUROPHARMA.NS","LUPIN.NS","ALKEM.NS","BIOCON.NS","GLAND.NS",
        "LAURUSLABS.NS","GRANULES.NS","NATCOPHARM.NS","IPCALAB.NS","SYNGENE.NS",
        "TORNTPHARM.NS","ZYDUSLIFE.NS","ABBOTINDIA.NS","PFIZER.NS","GLAXO.NS",
        "METROPOLIS.NS","LALPATHLAB.NS","THYROCARE.NS","KRSNAA.NS",
    ],
    "Auto & Auto Ancillary": [
        "MARUTI.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","EICHERMOT.NS","MOTHERSON.NS",
        "BHARATFORG.NS","BALKRISIND.NS","APOLLOTYRE.NS","CEATLTD.NS","ASHOKLEY.NS",
        "ESCORTS.NS","TIINDIA.NS","CRAFTSMAN.NS","SUPRAJIT.NS","MRF.NS",
        "BOSCHLTD.NS","SONACOMS.NS",
    ],
    "Energy & Power": [
        "RELIANCE.NS","ONGC.NS","BPCL.NS","IOC.NS","HINDPETRO.NS","PETRONET.NS",
        "MGL.NS","IGL.NS","TATAPOWER.NS","ADANIGREEN.NS","TORNTPOWER.NS",
        "CESC.NS","NTPC.NS","POWERGRID.NS","NHPC.NS","COALINDIA.NS",
    ],
    "Infra & Defence": [
        "LT.NS","ADANIPORTS.NS","ADANIENT.NS","BHARTIARTL.NS","INDUSTOWER.NS",
        "SIEMENS.NS","ABB.NS","HAVELLS.NS","POLYCAB.NS","BHEL.NS","BEL.NS",
        "HAL.NS","COCHINSHIP.NS","GRSE.NS","BEML.NS","RVNL.NS","IRFC.NS",
        "HUDCO.NS","NBCC.NS","CONCOR.NS","BLUEDART.NS","TCI.NS",
        "CUMMINSIND.NS","THERMAX.NS","GRINDWELL.NS","TIMKEN.NS","SCHAEFFLER.NS",
    ],
    "Real Estate": [
        "DLF.NS","GODREJPROP.NS","PRESTIGE.NS","SOBHA.NS","PHOENIXLTD.NS",
        "BRIGADE.NS","OBEROIRLTY.NS","NESCO.NS",
    ],
    "Consumer & FMCG": [
        "HINDUNILVR.NS","ITC.NS","NESTLEIND.NS","BRITANNIA.NS","DABUR.NS",
        "MARICO.NS","COLPAL.NS","GODREJCP.NS","TATACONSUM.NS","PIDILITIND.NS",
        "BERGEPAINT.NS","ASIANPAINT.NS","PAGEIND.NS","WHIRLPOOL.NS","VOLTAS.NS",
        "TITAN.NS","TRENT.NS","DMART.NS","JUBLFOOD.NS","IRCTC.NS",
        "PVRINOX.NS","NAZARA.NS","ZEEL.NS","PAYTM.NS","NYKAA.NS",
        "INDHOTEL.NS","LEMONTREE.NS","CHALET.NS","TAJGVK.NS",
    ],
    "Metals & Materials": [
        "TATASTEEL.NS","JSWSTEEL.NS","HINDALCO.NS","VEDL.NS","SAIL.NS",
        "NMDC.NS","GRASIM.NS","ULTRACEMCO.NS","AMBUJACEM.NS","SHREECEM.NS",
        "APLAPOLLO.NS","JINDALSTEL.NS","RATNAMANI.NS",
    ],
    "Others": [
        "LICI.NS","RECLTD.NS","PFC.NS","TATAINVEST.NS","3MINDIA.NS",
        "HONAUT.NS","INOXWIND.NS","VIJAYA.NS","BOSCHLTD.NS","MPHASIS.NS",
    ],
}
ALL_STOCKS = [s for sec in SECTORS.values() for s in sec]

# ── IST helpers ────────────────────────────────────────────
def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def ist_str():
    return ist_now().strftime("%Y-%m-%d %H:%M:%S IST")

def is_market_open():
    n = ist_now()
    return n.weekday() < 5 and (
        (n.hour == 9 and n.minute >= 15) or
        (10 <= n.hour <= 14) or
        (n.hour == 15 and n.minute <= 15)
    )

def is_good_trading_window():
    n = ist_now()
    after_open   = not (n.hour == 9 and n.minute < 30)
    before_close = not (n.hour == 15 and n.minute > 0)
    return after_open and before_close

# ── Telegram ───────────────────────────────────────────────
def send_telegram(msg, chat_id):
    if not TELEGRAM_TOKEN or not chat_id:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code != 200:
            print(f"   [Telegram] HTTP {r.status_code} for chat {chat_id}")
    except Exception as e:
        print(f"   [Telegram] Error for chat {chat_id}: {e}")

# ── User management ────────────────────────────────────────
def load_users():
    if not os.path.exists(USERS_FILE):
        return pd.DataFrame()
    return pd.read_csv(USERS_FILE)

def normalize_ticker(t):
    return str(t).strip().upper().replace(".NS", "")

def is_user_active(user_row):
    status = str(user_row["subscription_status"]).lower()
    if status == "active":
        return True
    if status == "trial":
        trial_end = pd.to_datetime(user_row["trial_end_date"]).date()
        return ist_now().date() <= trial_end
    return False

def is_stock_in_watchlist(ticker, watchlist_str):
    watchlist_str = str(watchlist_str).strip()
    if watchlist_str.upper() == "DEFAULT":
        return True
    custom_list = [normalize_ticker(t) for t in watchlist_str.split(",")]
    return normalize_ticker(ticker) in custom_list

def get_recipients(ticker, users_df):
    return [
        str(user["telegram_chat_id"])
        for _, user in users_df.iterrows()
        if is_user_active(user) and is_stock_in_watchlist(ticker, user["watchlist"])
    ]

def send_trial_expiry_warning(users_df):
    """Alert users whose trial expires in 2 days."""
    today = ist_now().date()
    for _, user in users_df.iterrows():
        if str(user["subscription_status"]).lower() == "trial":
            trial_end = pd.to_datetime(user["trial_end_date"]).date()
            days_left = (trial_end - today).days
            if days_left == 2:
                send_telegram(
                    f"Your free trial expires in 2 days ({trial_end}).\n"
                    f"Subscribe to keep receiving signals.\n"
                    f"Reply SUBSCRIBE for payment details.",
                    chat_id=user["telegram_chat_id"]
                )

# ── Nifty trend ────────────────────────────────────────────
def get_nifty_trend():
    try:
        data = yf.download("^NSEI", period="1d", interval="5m",
                           auto_adjust=True, progress=False)
        if data is None or len(data) < 5:
            return "NEUTRAL"
        close = data["Close"].squeeze()
        lookback = min(6, len(close) - 1)
        change_pct = (float(close.iloc[-1]) - float(close.iloc[-lookback])) / float(close.iloc[-lookback]) * 100
        if change_pct > 0.3:  return "UP"
        if change_pct < -0.3: return "DOWN"
        return "NEUTRAL"
    except:
        return "NEUTRAL"

# ── Fetch data ─────────────────────────────────────────────
def fetch_intraday(ticker):
    try:
        data = yf.download(ticker, period="1d", interval="5m",
                           auto_adjust=True, progress=False)
        if data is None or len(data) < 15:
            return None
        data.columns = [c[0] if isinstance(c, tuple) else c for c in data.columns]
        return data
    except:
        return None

def calc_vwap(data):
    tp  = (data["High"].squeeze() + data["Low"].squeeze() + data["Close"].squeeze()) / 3
    vol = data["Volume"].squeeze()
    return (tp * vol).cumsum() / vol.cumsum()

# ── Strategies (same as P1) ────────────────────────────────
def scalp_signal(data):
    close  = data["Close"].squeeze()
    volume = data["Volume"].squeeze()
    vwap   = calc_vwap(data)
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(span=14).mean()
    loss   = (-delta.clip(upper=0)).ewm(span=14).mean()
    rsi    = 100 - 100 / (1 + gain / loss)
    price    = float(close.iloc[-1])
    vwap_val = float(vwap.iloc[-1])
    rsi_val  = float(rsi.iloc[-1])
    vol_avg  = float(volume.rolling(20).mean().iloc[-1])
    vol_now  = float(volume.iloc[-1])
    if price > vwap_val and vol_now > vol_avg * 1.3 and 40 < rsi_val < 65:
        return "BUY", price, round(vwap_val*0.997,2), round(price*1.005,2), rsi_val, "SCALP"
    if price < vwap_val and vol_now > vol_avg * 1.3 and 55 < rsi_val < 75:
        return "SELL", price, round(price*1.003,2), round(price*0.995,2), rsi_val, "SCALP"
    return "HOLD", price, 0, 0, rsi_val, "SCALP"

def momentum_signal(data):
    close  = data["Close"].squeeze()
    volume = data["Volume"].squeeze()
    ema9   = close.ewm(span=9, adjust=False).mean()
    ema21  = close.ewm(span=21, adjust=False).mean()
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(span=14).mean()
    loss   = (-delta.clip(upper=0)).ewm(span=14).mean()
    rsi    = 100 - 100 / (1 + gain / loss)
    price    = float(close.iloc[-1])
    e9n, e9p = float(ema9.iloc[-1]), float(ema9.iloc[-2])
    e21n,e21p= float(ema21.iloc[-1]),float(ema21.iloc[-2])
    rsi_val  = float(rsi.iloc[-1])
    vol_ok   = float(volume.iloc[-1]) > float(volume.rolling(20).mean().iloc[-1]) * 1.2
    if e9n > e21n and e9p <= e21p and vol_ok and rsi_val < 65:
        return "BUY", price, round(e21n*0.993,2), round(price*1.015,2), rsi_val, "MOMENTUM"
    if e9n < e21n and e9p >= e21p and rsi_val > 40:
        return "SELL", price, round(e21n*1.007,2), round(price*0.985,2), rsi_val, "MOMENTUM"
    return "HOLD", price, 0, 0, rsi_val, "MOMENTUM"

def swing_signal(data):
    close = data["Close"].squeeze()
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    sma20  = close.rolling(20).mean()
    std20  = close.rolling(20).std()
    bb_up  = sma20 + 2 * std20
    bb_low = sma20 - 2 * std20
    price    = float(close.iloc[-1])
    mn, mp   = float(macd.iloc[-1]), float(macd.iloc[-2])
    sn, sp   = float(signal.iloc[-1]), float(signal.iloc[-2])
    bbl, bbu = float(bb_low.iloc[-1]), float(bb_up.iloc[-1])
    sma      = float(sma20.iloc[-1])
    if mn > sn and mp <= sp and price < sma and price > bbl:
        return "BUY", price, round(bbl*0.99,2), round(sma*1.025,2), 0, "SWING"
    if mn < sn and mp >= sp and price > sma and price < bbu:
        return "SELL", price, round(bbu*1.01,2), round(sma*0.975,2), 0, "SWING"
    return "HOLD", price, 0, 0, 0, "SWING"

def orb_signal(data):
    try:
        close  = data["Close"].squeeze()
        high   = data["High"].squeeze()
        low    = data["Low"].squeeze()
        volume = data["Volume"].squeeze()
        if len(data) < 6:
            return "HOLD", float(close.iloc[-1]), 0, 0, 0, "ORB"
        orb_high  = float(high.iloc[:3].max())
        orb_low   = float(low.iloc[:3].min())
        orb_range = orb_high - orb_low
        price     = float(close.iloc[-1])
        vol_ok    = float(volume.iloc[-1]) > float(volume.rolling(20).mean().iloc[-1]) * 1.5
        n = ist_now()
        in_window = (n.hour == 9 and n.minute >= 30) or (n.hour == 10) or (n.hour == 11 and n.minute == 0)
        if not in_window:
            return "HOLD", price, 0, 0, 0, "ORB"
        if price > orb_high and vol_ok:
            return "BUY", price, round(orb_high - orb_range*0.5, 2), round(price + orb_range*1.5, 2), 0, "ORB"
        if price < orb_low and vol_ok:
            return "SELL", price, round(orb_low + orb_range*0.5, 2), round(price - orb_range*1.5, 2), 0, "ORB"
        return "HOLD", price, 0, 0, 0, "ORB"
    except:
        return "HOLD", 0, 0, 0, 0, "ORB"

# ── Duplicate prevention (per-user, per-day) ───────────────
def already_alerted(ticker, strategy, chat_id):
    today = ist_now().strftime("%Y-%m-%d")
    if not os.path.exists(ALERTED_FILE):
        return False
    df = pd.read_csv(ALERTED_FILE)
    return len(df[(df["Date"] == today) &
                  (df["Ticker"] == ticker) &
                  (df["Strategy"] == strategy) &
                  (df["ChatId"] == str(chat_id))]) > 0

def mark_alerted(ticker, strategy, chat_id):
    today = ist_now().strftime("%Y-%m-%d")
    row   = pd.DataFrame([{"Date": today, "Ticker": ticker,
                            "Strategy": strategy, "ChatId": str(chat_id)}])
    if os.path.exists(ALERTED_FILE):
        updated = pd.concat([pd.read_csv(ALERTED_FILE), row], ignore_index=True)
    else:
        updated = row
    updated.to_csv(ALERTED_FILE, index=False)

# ── Scan one stock ─────────────────────────────────────────
def scan_stock(ticker, nifty_trend="NEUTRAL"):
    data = fetch_intraday(ticker)
    if data is None:
        return []
    signals = []
    for fn in [scalp_signal, momentum_signal, swing_signal, orb_signal]:
        try:
            sig, price, sl, target, rsi, strat = fn(data)
            if nifty_trend == "DOWN" and sig == "BUY": continue
            if nifty_trend == "UP"   and sig == "SELL": continue
            if sig in ["BUY", "SELL"]:
                rr = round(abs(target-price)/abs(price-sl),2) if sl and target and price!=sl else 0
                signals.append({
                    "Stock"    : ticker.replace(".NS",""),
                    "Strategy" : strat,
                    "Signal"   : sig,
                    "Price"    : f"Rs.{price}",
                    "Stop Loss": f"Rs.{sl}" if sl else "-",
                    "Target"   : f"Rs.{target}" if target else "-",
                    "R:R"      : f"1:{rr}" if rr else "-",
                    "RSI"      : round(rsi, 1) if rsi else "-",
                    "Time"     : ist_str(),
                })
        except: pass
    return signals

def save_to_log(signals):
    if not signals: return
    cols = ["Time","Stock","Strategy","Signal","Price","Stop Loss","Target","R:R","RSI"]
    log  = pd.read_csv(LOG_FILE) if os.path.exists(LOG_FILE) \
           else pd.DataFrame(columns=cols)
    rows = [{c: s.get(c, "-") for c in cols} for s in signals]
    pd.concat([log, pd.DataFrame(rows)], ignore_index=True).to_csv(LOG_FILE, index=False)

# ── Main scan ──────────────────────────────────────────────
def run_full_scan():
    if not is_market_open():
        print(f"[{ist_str()}] Market closed - skipping.")
        return

    if not is_good_trading_window():
        print(f"[{ist_str()}] Outside good trading window - skipping.")
        return

    users_df = load_users()
    if users_df.empty:
        print(f"[{ist_str()}] No users found in {USERS_FILE} - nothing to do.")
        return

    active_users = [u for _, u in users_df.iterrows() if is_user_active(u)]
    print(f"[{ist_str()}] Active users: {len(active_users)} | Scanning {len(ALL_STOCKS)} stocks...")

    # Trial expiry warnings (runs on every scan, deduplicated by already_alerted)
    send_trial_expiry_warning(users_df)

    nifty_trend = get_nifty_trend()
    all_signals = []

    for idx, ticker in enumerate(ALL_STOCKS):
        sigs = scan_stock(ticker, nifty_trend)
        all_signals.extend(sigs)
        time.sleep(0.3)
        if (idx + 1) % 50 == 0:
            print(f"   ...{idx+1}/{len(ALL_STOCKS)}")

    buys  = [s for s in all_signals if s["Signal"] == "BUY"]
    sells = [s for s in all_signals if s["Signal"] == "SELL"]

    # Route to each user
    alert_count = 0
    for s in buys + sells:
        recipients = get_recipients(s["Stock"], users_df)
        for chat_id in recipients:
            if not already_alerted(s["Stock"], s["Strategy"], chat_id):
                mark_alerted(s["Stock"], s["Strategy"], chat_id)
                send_telegram(
                    f"{'BUY' if s['Signal']=='BUY' else 'SELL'} - {s['Stock']}\n"
                    f"Strategy   : {s['Strategy']}\n"
                    f"Price      : {s['Price']}\n"
                    f"Stop Loss  : {s['Stop Loss']}\n"
                    f"Target     : {s['Target']}\n"
                    f"R:R        : {s['R:R']}\n"
                    f"RSI        : {s['RSI']}\n"
                    f"Nifty      : {nifty_trend}\n"
                    f"Time       : {s['Time']}",
                    chat_id=chat_id
                )
                alert_count += 1

    save_to_log(all_signals)

    # Admin summary to YOU only
    send_telegram(
        f"[P2] Scan Done\n"
        f"BUY:{len(buys)} SELL:{len(sells)}\n"
        f"Alerts sent: {alert_count}\n"
        f"Active users: {len(active_users)}\n"
        f"Nifty: {nifty_trend}\n"
        f"{ist_str()}",
        chat_id=ADMIN_CHAT_ID
    )
    print(f"[{ist_str()}] Done - BUY:{len(buys)} SELL:{len(sells)} alerts:{alert_count}")
    print("-" * 60)

# ── Entry point ─────────────────────────────────────────────
SINGLE_RUN = "--single-run" in sys.argv
print("=" * 60)
print("PROJECT 2 - MULTI-USER SAAS SIGNAL ALERTS")
print(f"Mode    : {'SINGLE RUN' if SINGLE_RUN else 'CONTINUOUS'}")
print(f"Users   : {USERS_FILE}")
print(f"Started : {ist_str()}")
print("=" * 60)

if SINGLE_RUN:
    run_full_scan()
    print(f"[{ist_str()}] Single run complete - exiting.")
else:
    send_telegram(f"[P2] Multi-User Scanner started\nEvery {SCAN_INTERVAL} min.\n{ist_str()}", chat_id=ADMIN_CHAT_ID)
    run_full_scan()
    schedule.every(SCAN_INTERVAL).minutes.do(run_full_scan)
    while True:
        schedule.run_pending()
        time.sleep(20)
