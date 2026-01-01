import time
import datetime as dt
import sys
from dataclasses import dataclass

import pytz
import pandas as pd
import yfinance as yf
import requests

# =====================================================
# TIMEZONE / SESSION
# =====================================================
TZ = pytz.timezone("Europe/Amsterdam")

SESSION_START = dt.time(9, 0)     # 09:00 NL
SESSION_END   = dt.time(23, 0)    # 23:00 NL

NY_OPEN = dt.time(15, 30)         # NY "open window" marker in NL time
NY_CONTEXT_MIN = 2                # minutes before/after NY open where we add context

# =====================================================
# MARKET / DATA (rate-limit friendly)
# =====================================================
SYMBOL = "XAUUSD=X"
INTERVAL = "5m"                   # use 5m to avoid yfinance rate limits
LOOKBACK_MINUTES = 240
POLL_SECONDS = 60                 # poll every 60 sec

# =====================================================
# YOUR CURRENT LEVELS (edit anytime)
# =====================================================
@dataclass
class Levels:
    breakout: float = 4326.0
    pullback_low: float = 4325.0
    pullback_high: float = 4326.0
    invalidate_below: float = 4324.0

    sl: float = 4318.0
    tp1: float = 4336.0
    tp2: float = 4345.0

LEVELS = Levels()

# =====================================================
# PROWL
# =====================================================
PROWL_ENABLED = True
PROWL_KEY_FILE = "key.txt"        # put your API key here (single line)
PROWL_APP = "Goud Alert"
PROWL_PRIORITY = 0                # -2..2

# =====================================================
# HELPERS
# =====================================================
def in_session(now: dt.datetime) -> bool:
    return SESSION_START <= now.time() <= SESSION_END

def in_ny_context(now: dt.datetime) -> bool:
    # FIX: make NY time timezone-aware too
    ny_naive = dt.datetime.combine(now.date(), NY_OPEN)
    ny = TZ.localize(ny_naive)
    return abs((now - ny).total_seconds()) <= NY_CONTEXT_MIN * 60

def load_prowl_key() -> str:
    try:
        with open(PROWL_KEY_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""

def send_prowl(message: str, event: str = "Alert"):
    if not PROWL_ENABLED:
        return

    key = load_prowl_key()
    if not key:
        print(f"PROWL: geen key gevonden in {PROWL_KEY_FILE}", flush=True)
        return

    url = "https://api.prowlapp.com/publicapi/add"
    data = {
        "apikey": key,
        "application": PROWL_APP,
        "event": event[:1024],
        "description": message[:10000],
        "priority": str(PROWL_PRIORITY),
    }

    try:
        r = requests.post(url, data=data, timeout=8)
        if r.status_code != 200:
            print(f"PROWL: fout {r.status_code}: {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"PROWL: error {e}", flush=True)

def alert(msg: str, event: str = "Alert"):
    stamp = dt.datetime.now(TZ).strftime("%H:%M:%S")
    out = f"[{stamp}] {msg}"
    print(out, flush=True)
    # terminal bell
    sys.stdout.write("\a")
    sys.stdout.flush()
    send_prowl(out, event=event)

# =====================================================
# DATA
# =====================================================
def fetch_data() -> pd.DataFrame:
    """
    Fetch intraday candles. yfinance 1m is rate-limited; 5m is safer.
    """
    df = yf.download(
        SYMBOL,
        period="1d",
        interval=INTERVAL,
        progress=False,
        threads=False
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # yfinance index usually UTC; convert to Amsterdam
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(TZ)

    cutoff = dt.datetime.now(TZ) - dt.timedelta(minutes=LOOKBACK_MINUTES)
    df = df[df.index >= cutoff].copy()
    df = df.dropna()
    return df

def macd_hist(close: pd.Series) -> pd.Series:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd - signal

# =====================================================
# SIGNAL ENGINE (simple, matches our "wait/pullback/breakout" logic)
# =====================================================
def evaluate(df: pd.DataFrame) -> str | None:
    if df.empty or len(df) < 35:
        return None

    close = df["Close"]
    price = float(close.iloc[-1])
    last_close = float(close.iloc[-1])

    hist = macd_hist(close)
    hist_now = float(hist.iloc[-1])
    hist_prev = float(hist.iloc[-2])

    momentum_up = (hist_now > hist_prev) and (hist_now > 0)

    # 1) Invalidation (no long)
    if last_close < LEVELS.invalidate_below:
        return f"NO TRADE — close {last_close:.1f} < invalidate {LEVELS.invalidate_below:.1f}"

    # 2) Pullback long (price in zone + momentum improving)
    if LEVELS.pullback_low <= price <= LEVELS.pullback_high and momentum_up:
        return (
            f"SIGNAL PULLBACK LONG | entry~{price:.1f} | SL {LEVELS.sl:.1f} | "
            f"TP1 {LEVELS.tp1:.1f} | TP2 {LEVELS.tp2:.1f}"
        )

    # 3) Above breakout but not in pullback zone -> wait for retest
    if last_close > LEVELS.breakout:
        return (
            f"WAIT — boven breakout {LEVELS.breakout:.1f}. "
            f"Wacht op pullback {LEVELS.pullback_low:.1f}-{LEVELS.pullback_high:.1f}."
        )

    # 4) Below breakout -> standby
    return f"STANDBY — onder breakout {LEVELS.breakout:.1f}. Geen long-plan actief."

# =====================================================
# MAIN
# =====================================================
def main():
    last_msg = None
    ny_msg_sent_for_date = None

    alert("Advisor gestart (09:00–23:00 NL, hele dag adviezen + NY-context).")

    while True:
        now = dt.datetime.now(TZ)

        # Outside session: keep quiet
        if not in_session(now):
            time.sleep(60)
            continue

        # NY context: send only once per day (prevents spam)
        if in_ny_context(now) and ny_msg_sent_for_date != now.date():
            alert("NY CONTEXT — verhoogde volatiliteit (fakeouts/spikes). Geen FOMO; wacht op bevestiging.", event="NY")
            ny_msg_sent_for_date = now.date()

        try:
            df = fetch_data()
            msg = evaluate(df)

            # Only alert when message changes (prevents spam)
            if msg and msg != last_msg:
                if msg.startswith("SIGNAL"):
                    alert(msg, event="Signal")
                else:
                    # info messages go to console + (optionally) prowl as normal alert
                    alert(msg, event="Info")
                last_msg = msg

        except Exception as e:
            print(f"Datafout: {e} (wachten…)", flush=True)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
