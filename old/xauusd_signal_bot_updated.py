#!/usr/bin/env python3
"""
gold_bot.py

Bot op basis van jouw 'ticks' tabel (goldprice fetcher).

Features:
- Resample ticks -> candles (1m/5m/15m/1h/1d).
- Indicators opslaan: EMA20/EMA50/RSI14/ATR14/MACD(12,26,9) -> bot_indicators
- Debug per bar opslaan (waarom wel/geen signaal) -> bot_debug
- Signals + positie-state + events:
    NEW_SIGNAL / TP1_HIT / TP2_HIT / SL_HIT / CLOSED
- Prowl berichten alleen bij nieuw signaal en bij TP/SL events.
- Optioneel INIT signaal bij start (geen cross) om gemiste cross te vermijden.
- Optioneel repair/backfill bij start: herberekent indicators/debug + optioneel gaps vullen.
- BELANGRIJK: automatische schema-migratie voor oudere DB's (voorkomt "no column named ...").

Vereiste in jouw bestaande DB:
  ticks(source_ts_ms INTEGER, currency TEXT, xau_price REAL)
"""

import argparse
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests


TIMEFRAMES_S: Dict[str, int] = {
    "1m": 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
}


# ----------------------------
# Models
# ----------------------------

@dataclass
class Bar:
    start_ts_ms: int
    open: float
    high: float
    low: float
    close: float


@dataclass
class Signal:
    direction: str  # "LONG" or "SHORT"
    bar_ts_ms: int  # bar bucket used to decide
    entry_price: float
    atr: float
    sl: float
    tp1: float
    tp2: float
    note: str
    kind: str  # "CROSS" or "INIT"


# ----------------------------
# Utilities
# ----------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def currency_list(csv: str) -> Iterable[str]:
    for c in csv.split(","):
        c = c.strip().upper()
        if c:
            yield c


def bucket_start_ms(ts_ms: int, timeframe_s: int) -> int:
    ts_s = ts_ms // 1000
    start_s = (ts_s // timeframe_s) * timeframe_s
    return start_s * 1000


PIP_SIZE = 0.01  # XAUUSD: 1 pip = 0.01

def _to_pips(price_delta: float) -> int:
    """Convert a price delta to XAUUSD pips (0.01 = 1 pip)."""
    return int(round(price_delta / PIP_SIZE))

def fmt_levels(direction: str, entry: float, sl: float, tp1: float, tp2: float, atr: float | None = None) -> str:
    """Format a trade plan in the same style as the live guidance (console/Prowl)."""
    risk = abs(entry - sl)
    risk_pips = _to_pips(risk) if risk else 0
    tp1_pips = _to_pips(abs(tp1 - entry))
    tp2_pips = _to_pips(abs(tp2 - entry))

    # Trailing suggestion for the runner after TP1:
    # default = ~0.8x ATR, but never too tight.
    trail_pips = None
    if atr is not None and atr > 0:
        trail_pips = int(round(max(0.8 * atr / PIP_SIZE, 120)))

    plan = []
    plan.append(f"SETUP: {direction}")
    plan.append(f"Entry: {entry:.2f}")
    plan.append(f"SL:    {sl:.2f}  ({risk_pips} pips risico)")
    plan.append(f"TP1:   {tp1:.2f}  (+{tp1_pips} pips)")
    plan.append(f"TP2:   {tp2:.2f}  (+{tp2_pips} pips)  = runner/target2")
    plan.append("")
    plan.append("EXECUTIE (praktisch):")
    plan.append("1) Split in 2 posities (bijv. 2x 50%).")
    plan.append("2) TP1: sluit positie #1 volledig.")
    if trail_pips is not None:
        plan.append(f"3) Runner: zet SL naar break-even (entry) zodra TP1 is geraakt, en trail daarna met {trail_pips} pips.")
    else:
        plan.append("3) Runner: zet SL naar break-even (entry) zodra TP1 is geraakt, en trail daarna (bijv. 120–200 pips).")
    plan.append("4) Als prijs terug onder/over MID (BOLL) sluit op momentumbreuk.")
    return "\n".join(plan)
def read_prowl_key(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        k = f.read().strip()
    if not k or len(k) < 20:
        raise ValueError("Prowl key lijkt leeg/ongeldig. Controleer key.txt.")
    return k


def prowl_send(apikey: str, application: str, event: str, description: str, priority: int = 0) -> None:
    url = "https://api.prowlapp.com/publicapi/add"
    data = {
        "apikey": apikey,
        "application": application[:256],
        "event": event[:1024],
        "description": description[:10000],
        "priority": str(int(priority)),
    }
    r = requests.post(url, data=data, timeout=15)
    r.raise_for_status()


# ----------------------------
# Bot schema
# ----------------------------

BOT_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS bot_signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,

  direction TEXT NOT NULL,                   -- 'LONG'/'SHORT'
  signal_kind TEXT NOT NULL DEFAULT 'CROSS', -- 'CROSS'/'INIT'
  signal_bar_ts_ms INTEGER NOT NULL,
  entry_ts_ms INTEGER NOT NULL,
  entry_price REAL NOT NULL,

  atr REAL NOT NULL,
  sl REAL NOT NULL,
  tp1 REAL NOT NULL,
  tp2 REAL NOT NULL,

  note TEXT
);

CREATE INDEX IF NOT EXISTS idx_bot_signals_ct
ON bot_signals(currency, timeframe, signal_bar_ts_ms);

CREATE TABLE IF NOT EXISTS bot_signal_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,             -- NEW_SIGNAL, TP1_HIT, TP2_HIT, SL_HIT, CLOSED
  event_ts_ms INTEGER NOT NULL,
  price REAL,
  note TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(signal_id) REFERENCES bot_signals(id)
);

CREATE INDEX IF NOT EXISTS idx_bot_events_sig
ON bot_signal_events(signal_id, event_ts_ms);

CREATE TABLE IF NOT EXISTS bot_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,

  signal_id INTEGER NOT NULL,
  status TEXT NOT NULL,                 -- 'ACTIVE'/'CLOSED'
  direction TEXT NOT NULL,              -- 'LONG'/'SHORT'

  signal_bar_ts_ms INTEGER NOT NULL,
  entry_ts_ms INTEGER NOT NULL,
  entry_price REAL NOT NULL,

  atr REAL NOT NULL,
  sl REAL NOT NULL,
  tp1 REAL NOT NULL,
  tp2 REAL NOT NULL,

  tp1_hit INTEGER NOT NULL DEFAULT 0,
  tp1_ts_ms INTEGER,
  tp1_price REAL,

  closed_ts_ms INTEGER,
  close_price REAL,
  close_reason TEXT,                    -- 'SL' or 'TP2'
  pnl_pct REAL,

  FOREIGN KEY(signal_id) REFERENCES bot_signals(id)
);

CREATE INDEX IF NOT EXISTS idx_bot_positions_active
ON bot_positions(currency, timeframe, status);

CREATE TABLE IF NOT EXISTS bot_stats (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  total_trades INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (currency, timeframe)
);

CREATE TABLE IF NOT EXISTS bot_last_signal (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  last_signal_bar_ts_ms INTEGER NOT NULL DEFAULT 0,
  last_signal_direction TEXT,
  PRIMARY KEY (currency, timeframe)
);

CREATE TABLE IF NOT EXISTS bot_indicators (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  bar_start_ts_ms INTEGER NOT NULL,

  close REAL NOT NULL,

  ema20 REAL,
  ema50 REAL,
  rsi14 REAL,
  atr14 REAL,

  macd REAL,
  macd_signal REAL,
  macd_hist REAL,

  created_at_utc TEXT NOT NULL,

  PRIMARY KEY(currency, timeframe, bar_start_ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_bot_indicators_ct
ON bot_indicators(currency, timeframe, bar_start_ts_ms);

CREATE TABLE IF NOT EXISTS bot_debug (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  bar_start_ts_ms INTEGER NOT NULL,

  prev_close REAL,
  close REAL,
  ema50_prev REAL,
  ema50 REAL,
  rsi14 REAL,
  atr14 REAL,
  macd REAL,
  macd_signal REAL,
  macd_hist REAL,

  cross_up INTEGER,
  cross_dn INTEGER,
  long_ok INTEGER,
  short_ok INTEGER,

  decision TEXT,
  note TEXT,

  created_at_utc TEXT NOT NULL,

  PRIMARY KEY(currency, timeframe, bar_start_ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_bot_debug_ct
ON bot_debug(currency, timeframe, bar_start_ts_ms);
"""


def init_bot_db(conn: sqlite3.Connection) -> None:
    conn.executescript(BOT_DDL)
    conn.commit()


# ----------------------------
# Schema migration (for old DBs)
# ----------------------------

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set:
    if not table_exists(conn, table):
        return set()
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # row[1] = column name


def ensure_column(conn: sqlite3.Connection, table: str, col_name: str, col_def_sql: str) -> None:
    """
    Add column if missing.
    col_def_sql example: "signal_kind TEXT NOT NULL DEFAULT 'CROSS'"
    """
    if not table_exists(conn, table):
        return
    cols = table_columns(conn, table)
    if col_name in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")
    conn.commit()


def migrate_bot_schema(conn: sqlite3.Connection) -> None:
    # bot_signals: older versions may miss these columns
    ensure_column(conn, "bot_signals", "signal_kind", "signal_kind TEXT NOT NULL DEFAULT 'CROSS'")
    ensure_column(conn, "bot_signals", "note", "note TEXT")

    # bot_signal_events: older versions may miss note
    ensure_column(conn, "bot_signal_events", "note", "note TEXT")

    # bot_indicators: ensure MACD fields exist
    ensure_column(conn, "bot_indicators", "macd", "macd REAL")
    ensure_column(conn, "bot_indicators", "macd_signal", "macd_signal REAL")
    ensure_column(conn, "bot_indicators", "macd_hist", "macd_hist REAL")

    # bot_debug: ensure decision/note exist
    ensure_column(conn, "bot_debug", "decision", "decision TEXT")
    ensure_column(conn, "bot_debug", "note", "note TEXT")


# ----------------------------
# Load + resample
# ----------------------------

def load_ticks(conn: sqlite3.Connection, currency: str, lookback_days: int) -> List[Tuple[int, float]]:
    cutoff_ms = int(time.time() * 1000) - lookback_days * 86400 * 1000
    cur = conn.execute(
        """
        SELECT source_ts_ms, xau_price
        FROM ticks
        WHERE currency = ?
          AND source_ts_ms IS NOT NULL
          AND xau_price IS NOT NULL
          AND source_ts_ms >= ?
        ORDER BY source_ts_ms ASC
        """,
        (currency.upper(), cutoff_ms),
    )
    return [(int(ts), float(px)) for ts, px in cur.fetchall()]


def resample_ticks_to_bars(ticks: List[Tuple[int, float]], timeframe_s: int) -> List[Bar]:
    if not ticks:
        return []

    bars: List[Bar] = []
    cur_bucket = None
    o = h = l = c = None

    for ts_ms, price in ticks:
        b = bucket_start_ms(ts_ms, timeframe_s)
        if cur_bucket is None:
            cur_bucket = b
            o = h = l = c = price
            continue

        if b != cur_bucket:
            bars.append(Bar(start_ts_ms=cur_bucket, open=o, high=h, low=l, close=c))
            cur_bucket = b
            o = h = l = c = price
        else:
            c = price
            h = max(h, price)
            l = min(l, price)

    if cur_bucket is not None:
        bars.append(Bar(start_ts_ms=cur_bucket, open=o, high=h, low=l, close=c))

    return bars


def latest_price(ticks: List[Tuple[int, float]]) -> Optional[Tuple[int, float]]:
    return ticks[-1] if ticks else None


def fill_gaps_flat(bars: List[Bar], timeframe_s: int) -> List[Bar]:
    """Vul ontbrekende candles op met flat candles (O=H=L=C=vorige close)."""
    if len(bars) < 2:
        return bars
    step_ms = timeframe_s * 1000
    out: List[Bar] = [bars[0]]

    for i in range(1, len(bars)):
        prev = out[-1]
        cur = bars[i]
        expected = prev.start_ts_ms + step_ms
        while expected < cur.start_ts_ms:
            px = prev.close
            out.append(Bar(expected, px, px, px, px))
            expected += step_ms
        out.append(cur)

    return out


# ----------------------------
# Indicators
# ----------------------------

def ema(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    k = 2.0 / (period + 1.0)
    sma = sum(values[:period]) / period
    out[period - 1] = sma
    prev = sma
    for i in range(period, len(values)):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < period + 1:
        return out

    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        ch = values[i] - values[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses -= ch

    avg_gain = gains / period
    avg_loss = losses / period
    out[period] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1 + (avg_gain / avg_loss)))

    for i in range(period + 1, len(values)):
        ch = values[i] - values[i - 1]
        gain = ch if ch > 0 else 0.0
        loss = -ch if ch < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1 + rs))
    return out


def atr(bars: List[Bar], period: int = 14) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(bars)
    if len(bars) < period + 1:
        return out

    trs: List[float] = []
    for i in range(1, len(bars)):
        hi = bars[i].high
        lo = bars[i].low
        prev_close = bars[i - 1].close
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(tr)

    if len(trs) < period:
        return out

    first = sum(trs[:period]) / period
    out[period] = first
    prev = first
    for j in range(period, len(trs)):
        prev = (prev * (period - 1) + trs[j]) / period
        out[j + 1] = prev
    return out


def macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    ef = ema(values, fast)
    es = ema(values, slow)

    macd_line: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if ef[i] is None or es[i] is None:
            macd_line[i] = None
        else:
            macd_line[i] = float(ef[i] - es[i])

    filled = [m if m is not None else 0.0 for m in macd_line]
    sig_ema = ema(filled, signal)

    sig_line: List[Optional[float]] = [None] * len(values)
    hist: List[Optional[float]] = [None] * len(values)

    for i in range(len(values)):
        if macd_line[i] is None or sig_ema[i] is None:
            continue
        sig_line[i] = float(sig_ema[i])
        hist[i] = float(macd_line[i] - sig_line[i])

    return macd_line, sig_line, hist


# ----------------------------
# Persistence: indicators/debug (incremental)
# ----------------------------

def get_last_indicator_bar_ts(conn: sqlite3.Connection, currency: str, timeframe: str) -> int:
    cur = conn.execute(
        """
        SELECT COALESCE(MAX(bar_start_ts_ms), 0)
        FROM bot_indicators
        WHERE currency = ? AND timeframe = ?
        """,
        (currency.upper(), timeframe),
    )
    r = cur.fetchone()
    return int(r[0]) if r and r[0] is not None else 0


def upsert_indicators_for_new_bars(
    conn: sqlite3.Connection,
    currency: str,
    timeframe: str,
    bars: List[Bar],
    ema20_vals: List[Optional[float]],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
) -> None:
    last_saved = get_last_indicator_bar_ts(conn, currency, timeframe)
    rows = []
    for i, b in enumerate(bars):
        if b.start_ts_ms <= last_saved:
            continue
        rows.append((
            currency.upper(),
            timeframe,
            int(b.start_ts_ms),
            float(b.close),
            (float(ema20_vals[i]) if ema20_vals[i] is not None else None),
            (float(ema50_vals[i]) if ema50_vals[i] is not None else None),
            (float(rsi14_vals[i]) if rsi14_vals[i] is not None else None),
            (float(atr14_vals[i]) if atr14_vals[i] is not None else None),
            (float(macd_line[i]) if macd_line[i] is not None else None),
            (float(macd_sig[i]) if macd_sig[i] is not None else None),
            (float(macd_hist[i]) if macd_hist[i] is not None else None),
            now_utc_iso(),
        ))
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR REPLACE INTO bot_indicators(
          currency, timeframe, bar_start_ts_ms,
          close, ema20, ema50, rsi14, atr14,
          macd, macd_signal, macd_hist,
          created_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()


def get_last_debug_bar_ts(conn: sqlite3.Connection, currency: str, timeframe: str) -> int:
    cur = conn.execute(
        """
        SELECT COALESCE(MAX(bar_start_ts_ms), 0)
        FROM bot_debug
        WHERE currency = ? AND timeframe = ?
        """,
        (currency.upper(), timeframe),
    )
    r = cur.fetchone()
    return int(r[0]) if r and r[0] is not None else 0


def upsert_debug_for_new_bars(
    conn: sqlite3.Connection,
    currency: str,
    timeframe: str,
    bars: List[Bar],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
) -> None:
    last_saved = get_last_debug_bar_ts(conn, currency, timeframe)
    rows = []

    for i in range(1, len(bars)):
        b = bars[i]
        if b.start_ts_ms <= last_saved:
            continue

        prev_close = bars[i - 1].close
        close = b.close

        e50_prev = ema50_vals[i - 1]
        e50 = ema50_vals[i]
        r = rsi14_vals[i]
        a = atr14_vals[i]
        m = macd_line[i]
        ms = macd_sig[i]
        mh = macd_hist[i]

        cross_up = 0
        cross_dn = 0
        long_ok = 0
        short_ok = 0

        if e50_prev is not None and e50 is not None:
            cross_up = 1 if (prev_close <= e50_prev and close > e50) else 0
            cross_dn = 1 if (prev_close >= e50_prev and close < e50) else 0

        if e50 is not None and r is not None and m is not None and ms is not None and mh is not None:
            long_ok = 1 if (close > e50 and r > 55 and m > ms and mh > 0) else 0
            short_ok = 1 if (close < e50 and r < 45 and m < ms and mh < 0) else 0

        decision = "NONE"
        if cross_up and long_ok:
            decision = "LONG_SIGNAL"
        elif cross_dn and short_ok:
            decision = "SHORT_SIGNAL"

        note = f"cross_up={cross_up} cross_dn={cross_dn} long_ok={long_ok} short_ok={short_ok}"

        rows.append((
            currency.upper(), timeframe, int(b.start_ts_ms),
            float(prev_close), float(close),
            (float(e50_prev) if e50_prev is not None else None),
            (float(e50) if e50 is not None else None),
            (float(r) if r is not None else None),
            (float(a) if a is not None else None),
            (float(m) if m is not None else None),
            (float(ms) if ms is not None else None),
            (float(mh) if mh is not None else None),
            int(cross_up), int(cross_dn),
            int(long_ok), int(short_ok),
            decision, note,
            now_utc_iso(),
        ))

    if not rows:
        return

    conn.executemany(
        """
        INSERT OR REPLACE INTO bot_debug(
          currency, timeframe, bar_start_ts_ms,
          prev_close, close, ema50_prev, ema50, rsi14, atr14, macd, macd_signal, macd_hist,
          cross_up, cross_dn, long_ok, short_ok,
          decision, note, created_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()


# ----------------------------
# Repair/backfill on start (full replace for window)
# ----------------------------

def upsert_indicators_all(
    conn: sqlite3.Connection,
    currency: str,
    timeframe: str,
    bars: List[Bar],
    ema20_vals: List[Optional[float]],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
) -> int:
    rows = []
    for i, b in enumerate(bars):
        rows.append((
            currency.upper(), timeframe, int(b.start_ts_ms),
            float(b.close),
            (float(ema20_vals[i]) if ema20_vals[i] is not None else None),
            (float(ema50_vals[i]) if ema50_vals[i] is not None else None),
            (float(rsi14_vals[i]) if rsi14_vals[i] is not None else None),
            (float(atr14_vals[i]) if atr14_vals[i] is not None else None),
            (float(macd_line[i]) if macd_line[i] is not None else None),
            (float(macd_sig[i]) if macd_sig[i] is not None else None),
            (float(macd_hist[i]) if macd_hist[i] is not None else None),
            now_utc_iso(),
        ))

    conn.executemany(
        """
        INSERT OR REPLACE INTO bot_indicators(
          currency, timeframe, bar_start_ts_ms,
          close, ema20, ema50, rsi14, atr14,
          macd, macd_signal, macd_hist,
          created_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_debug_all(
    conn: sqlite3.Connection,
    currency: str,
    timeframe: str,
    bars: List[Bar],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
) -> int:
    rows = []
    for i in range(1, len(bars)):
        b = bars[i]
        prev_close = bars[i - 1].close
        close = b.close

        e50_prev = ema50_vals[i - 1]
        e50 = ema50_vals[i]
        r = rsi14_vals[i]
        a = atr14_vals[i]
        m = macd_line[i]
        ms = macd_sig[i]
        mh = macd_hist[i]

        cross_up = 0
        cross_dn = 0
        long_ok = 0
        short_ok = 0

        if e50_prev is not None and e50 is not None:
            cross_up = 1 if (prev_close <= e50_prev and close > e50) else 0
            cross_dn = 1 if (prev_close >= e50_prev and close < e50) else 0

        if e50 is not None and r is not None and m is not None and ms is not None and mh is not None:
            long_ok = 1 if (close > e50 and r > 55 and m > ms and mh > 0) else 0
            short_ok = 1 if (close < e50 and r < 45 and m < ms and mh < 0) else 0

        decision = "NONE"
        if cross_up and long_ok:
            decision = "LONG_SIGNAL"
        elif cross_dn and short_ok:
            decision = "SHORT_SIGNAL"

        note = f"cross_up={cross_up} cross_dn={cross_dn} long_ok={long_ok} short_ok={short_ok}"

        rows.append((
            currency.upper(), timeframe, int(b.start_ts_ms),
            float(prev_close), float(close),
            (float(e50_prev) if e50_prev is not None else None),
            (float(e50) if e50 is not None else None),
            (float(r) if r is not None else None),
            (float(a) if a is not None else None),
            (float(m) if m is not None else None),
            (float(ms) if ms is not None else None),
            (float(mh) if mh is not None else None),
            int(cross_up), int(cross_dn),
            int(long_ok), int(short_ok),
            decision, note,
            now_utc_iso(),
        ))

    conn.executemany(
        """
        INSERT OR REPLACE INTO bot_debug(
          currency, timeframe, bar_start_ts_ms,
          prev_close, close, ema50_prev, ema50, rsi14, atr14, macd, macd_signal, macd_hist,
          cross_up, cross_dn, long_ok, short_ok,
          decision, note, created_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def run_repair_on_start(
    conn: sqlite3.Connection,
    currencies_csv: str,
    timeframe: str,
    timeframe_s: int,
    repair_days: int,
    fill_gaps: bool,
) -> None:
    for cur in currency_list(currencies_csv):
        ticks = load_ticks(conn, cur, repair_days)
        if not ticks:
            print(f"[{now_utc_iso()}] REPAIR {cur} {timeframe}: no ticks in last {repair_days} days; skip")
            continue

        bars = resample_ticks_to_bars(ticks, timeframe_s)
        if fill_gaps:
            bars = fill_gaps_flat(bars, timeframe_s)

        if len(bars) < 5:
            print(f"[{now_utc_iso()}] REPAIR {cur} {timeframe}: not enough bars ({len(bars)}); skip")
            continue

        closes = [b.close for b in bars]
        ema20_vals = ema(closes, 20)
        ema50_vals = ema(closes, 50)
        rsi14_vals = rsi(closes, 14)
        atr14_vals = atr(bars, 14)
        macd_line, macd_sig, macd_hist = macd(closes, 12, 26, 9)

        n_i = upsert_indicators_all(
            conn, cur, timeframe, bars,
            ema20_vals, ema50_vals, rsi14_vals, atr14_vals,
            macd_line, macd_sig, macd_hist
        )
        n_d = upsert_debug_all(
            conn, cur, timeframe, bars,
            ema50_vals, rsi14_vals, atr14_vals,
            macd_line, macd_sig, macd_hist
        )

        print(f"[{now_utc_iso()}] REPAIR {cur} {timeframe}: indicators={n_i}, debug={n_d}, fill_gaps={int(fill_gaps)}")


# ----------------------------
# Bot state
# ----------------------------

def insert_signal(conn: sqlite3.Connection, currency: str, timeframe: str, sig: Signal, entry_ts_ms: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO bot_signals(
          created_at_utc, currency, timeframe,
          direction, signal_kind, signal_bar_ts_ms, entry_ts_ms, entry_price,
          atr, sl, tp1, tp2, note
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_utc_iso(),
            currency.upper(),
            timeframe,
            sig.direction,
            sig.kind,
            int(sig.bar_ts_ms),
            int(entry_ts_ms),
            float(sig.entry_price),
            float(sig.atr),
            float(sig.sl),
            float(sig.tp1),
            float(sig.tp2),
            sig.note or "",
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def add_event(conn: sqlite3.Connection, signal_id: int, event_type: str, event_ts_ms: int, price: Optional[float], note: str = "") -> None:
    conn.execute(
        """
        INSERT INTO bot_signal_events(signal_id, event_type, event_ts_ms, price, note, created_at_utc)
        VALUES(?,?,?,?,?,?)
        """,
        (int(signal_id), event_type, int(event_ts_ms), (float(price) if price is not None else None), note, now_utc_iso()),
    )
    conn.commit()


def open_position(conn: sqlite3.Connection, currency: str, timeframe: str, signal_id: int, sig: Signal, entry_ts_ms: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO bot_positions (
          created_at_utc, currency, timeframe,
          signal_id, status, direction,
          signal_bar_ts_ms, entry_ts_ms, entry_price,
          atr, sl, tp1, tp2,
          tp1_hit
        ) VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (
            now_utc_iso(),
            currency.upper(),
            timeframe,
            int(signal_id),
            sig.direction,
            int(sig.bar_ts_ms),
            int(entry_ts_ms),
            float(sig.entry_price),
            float(sig.atr),
            float(sig.sl),
            float(sig.tp1),
            float(sig.tp2),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_active_position(conn: sqlite3.Connection, currency: str, timeframe: str) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT *
        FROM bot_positions
        WHERE currency = ? AND timeframe = ? AND status = 'ACTIVE'
        ORDER BY id DESC
        LIMIT 1
        """,
        (currency.upper(), timeframe),
    )
    row = cur.fetchone()
    conn.row_factory = None
    return row


def get_or_init_stats(conn: sqlite3.Connection, currency: str, timeframe: str) -> Tuple[int, int, int]:
    cur = conn.execute(
        """
        SELECT total_trades, wins, losses
        FROM bot_stats
        WHERE currency = ? AND timeframe = ?
        """,
        (currency.upper(), timeframe),
    )
    r = cur.fetchone()
    if r is None:
        conn.execute(
            "INSERT INTO bot_stats(currency, timeframe, total_trades, wins, losses) VALUES(?,?,0,0,0)",
            (currency.upper(), timeframe),
        )
        conn.commit()
        return (0, 0, 0)
    return (int(r[0]), int(r[1]), int(r[2]))


def update_stats_on_close(conn: sqlite3.Connection, currency: str, timeframe: str, pnl_pct: float) -> Tuple[int, int, int, float]:
    total, wins, losses = get_or_init_stats(conn, currency, timeframe)
    total += 1
    if pnl_pct > 0:
        wins += 1
    else:
        losses += 1
    conn.execute(
        """
        UPDATE bot_stats
        SET total_trades = ?, wins = ?, losses = ?
        WHERE currency = ? AND timeframe = ?
        """,
        (total, wins, losses, currency.upper(), timeframe),
    )
    conn.commit()
    winrate = (wins / total * 100.0) if total else 0.0
    return total, wins, losses, winrate


def mark_tp1(conn: sqlite3.Connection, pos_id: int, new_sl: float, tp1_price: float, tp1_ts_ms: int) -> None:
    conn.execute(
        """
        UPDATE bot_positions
        SET tp1_hit = 1,
            tp1_price = ?,
            tp1_ts_ms = ?,
            sl = ?
        WHERE id = ? AND status = 'ACTIVE'
        """,
        (float(tp1_price), int(tp1_ts_ms), float(new_sl), int(pos_id)),
    )
    conn.commit()


def close_position(conn: sqlite3.Connection, pos_id: int, reason: str, close_price: float, close_ts_ms: int, pnl_pct: float) -> None:
    conn.execute(
        """
        UPDATE bot_positions
        SET status = 'CLOSED',
            closed_ts_ms = ?,
            close_price = ?,
            close_reason = ?,
            pnl_pct = ?
        WHERE id = ? AND status = 'ACTIVE'
        """,
        (int(close_ts_ms), float(close_price), reason, float(pnl_pct), int(pos_id)),
    )
    conn.commit()


def last_signal_guard(conn: sqlite3.Connection, currency: str, timeframe: str) -> Tuple[int, Optional[str]]:
    cur = conn.execute(
        """
        SELECT last_signal_bar_ts_ms, last_signal_direction
        FROM bot_last_signal
        WHERE currency = ? AND timeframe = ?
        """,
        (currency.upper(), timeframe),
    )
    r = cur.fetchone()
    if r is None:
        conn.execute(
            """
            INSERT INTO bot_last_signal(currency, timeframe, last_signal_bar_ts_ms, last_signal_direction)
            VALUES(?,?,0,NULL)
            """,
            (currency.upper(), timeframe),
        )
        conn.commit()
        return 0, None
    return int(r[0]), (r[1] if r[1] is not None else None)


def set_last_signal_guard(conn: sqlite3.Connection, currency: str, timeframe: str, bar_ts_ms: int, direction: str) -> None:
    conn.execute(
        """
        UPDATE bot_last_signal
        SET last_signal_bar_ts_ms = ?, last_signal_direction = ?
        WHERE currency = ? AND timeframe = ?
        """,
        (int(bar_ts_ms), direction, currency.upper(), timeframe),
    )
    conn.commit()


def mark_debug_decision(conn: sqlite3.Connection, currency: str, timeframe: str, bar_ts_ms: int, decision: str, extra_note: str) -> None:
    conn.execute(
        """
        UPDATE bot_debug
        SET decision = ?,
            note = COALESCE(note,'') || ?,
            created_at_utc = ?
        WHERE currency = ? AND timeframe = ? AND bar_start_ts_ms = ?
        """,
        (decision, extra_note, now_utc_iso(), currency.upper(), timeframe, int(bar_ts_ms)),
    )
    conn.commit()


def minmax_since(conn: sqlite3.Connection, currency: str, entry_ts_ms: int) -> Optional[Tuple[float, float, int, int]]:
    cur = conn.execute(
        """
        SELECT MIN(xau_price) AS mn, MAX(xau_price) AS mx
        FROM ticks
        WHERE currency = ?
          AND source_ts_ms >= ?
          AND xau_price IS NOT NULL
        """,
        (currency.upper(), int(entry_ts_ms)),
    )
    r = cur.fetchone()
    if r is None or r[0] is None or r[1] is None:
        return None

    mn = float(r[0])
    mx = float(r[1])

    cur2 = conn.execute(
        """
        SELECT source_ts_ms
        FROM ticks
        WHERE currency = ? AND source_ts_ms >= ? AND xau_price = ?
        ORDER BY source_ts_ms ASC LIMIT 1
        """,
        (currency.upper(), int(entry_ts_ms), mn),
    )
    min_ts = cur2.fetchone()
    min_ts_ms = int(min_ts[0]) if min_ts else int(entry_ts_ms)

    cur3 = conn.execute(
        """
        SELECT source_ts_ms
        FROM ticks
        WHERE currency = ? AND source_ts_ms >= ? AND xau_price = ?
        ORDER BY source_ts_ms ASC LIMIT 1
        """,
        (currency.upper(), int(entry_ts_ms), mx),
    )
    max_ts = cur3.fetchone()
    max_ts_ms = int(max_ts[0]) if max_ts else int(entry_ts_ms)

    return mn, mx, min_ts_ms, max_ts_ms


# ----------------------------
# Signal logic
# ----------------------------

def compute_cross_signal(
    bars: List[Bar],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
    last_tick_price: float,
    min_bars: int,
) -> Optional[Signal]:
    if len(bars) < min_bars:
        return None

    i = len(bars) - 2  # last closed bar
    if i <= 1:
        return None

    prev_close = bars[i - 1].close
    close = bars[i].close

    e50_prev = ema50_vals[i - 1]
    e50 = ema50_vals[i]
    r = rsi14_vals[i]
    a = atr14_vals[i]
    m = macd_line[i]
    ms = macd_sig[i]
    mh = macd_hist[i]

    if e50_prev is None or e50 is None or r is None or a is None or m is None or ms is None or mh is None:
        return None

    cross_up = (prev_close <= e50_prev) and (close > e50)
    cross_dn = (prev_close >= e50_prev) and (close < e50)

    long_ok = (r > 55) and (m > ms) and (mh > 0)
    short_ok = (r < 45) and (m < ms) and (mh < 0)

    entry = float(last_tick_price)

    if cross_up and long_ok:
        sl = entry - 2.0 * a
        tp1 = entry + 2.0 * a
        tp2 = entry + 3.0 * a
        note = f"CROSS LONG: RSI={r:.2f}, MACD={m:.5f}>{ms:.5f}, hist={mh:.5f}"
        return Signal("LONG", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note, "CROSS")

    if cross_dn and short_ok:
        sl = entry + 2.0 * a
        tp1 = entry - 2.0 * a
        tp2 = entry - 3.0 * a
        note = f"CROSS SHORT: RSI={r:.2f}, MACD={m:.5f}<{ms:.5f}, hist={mh:.5f}"
        return Signal("SHORT", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note, "CROSS")

    return None


def compute_init_signal(
    bars: List[Bar],
    ema50_vals: List[Optional[float]],
    rsi14_vals: List[Optional[float]],
    atr14_vals: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
    last_tick_price: float,
    min_bars: int,
) -> Optional[Signal]:
    if len(bars) < min_bars:
        return None

    i = len(bars) - 2
    close = bars[i].close

    e50 = ema50_vals[i]
    r = rsi14_vals[i]
    a = atr14_vals[i]
    m = macd_line[i]
    ms = macd_sig[i]
    mh = macd_hist[i]

    if e50 is None or r is None or a is None or m is None or ms is None or mh is None:
        return None

    entry = float(last_tick_price)

    long_trend_ok = (close > e50) and (r > 55) and (m > ms) and (mh > 0)
    short_trend_ok = (close < e50) and (r < 45) and (m < ms) and (mh < 0)

    if long_trend_ok:
        sl = entry - 2.0 * a
        tp1 = entry + 2.0 * a
        tp2 = entry + 3.0 * a
        note = f"INIT LONG: close>EMA50, RSI={r:.2f}, MACD ok"
        return Signal("LONG", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note, "INIT")

    if short_trend_ok:
        sl = entry + 2.0 * a
        tp1 = entry - 2.0 * a
        tp2 = entry - 3.0 * a
        note = f"INIT SHORT: close<EMA50, RSI={r:.2f}, MACD ok"
        return Signal("SHORT", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note, "INIT")

    return None


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Gold (XAU) bot.")
    p.add_argument("--db", default="goldprice.sqlite", help="SQLite DB pad")
    p.add_argument("--currencies", default="USD,EUR", help="Komma-gescheiden, bijv. USD,EUR")
    p.add_argument("--timeframe", default="1m", choices=sorted(TIMEFRAMES_S.keys()), help="Timeframe")
    p.add_argument("--lookback-days", type=int, default=30, help="Historie voor candles/indicatoren")
    p.add_argument("--max-bars", type=int, default=20000, help="Performance cap (bars)")
    p.add_argument("--interval", type=int, default=20, help="Loop interval seconden")
    p.add_argument("--min-bars", type=int, default=60, help="Minimum bars voordat signalen starten")

    p.add_argument("--init-signal", action="store_true", help="Maak INIT signaal bij start als trendconditie al klopt")

    p.add_argument("--repair-on-start", action="store_true", help="Bij opstart indicators/debug herberekenen")
    p.add_argument("--repair-days", type=int, default=7, help="Aantal dagen terug voor repair/backfill")
    p.add_argument("--repair-fill-gaps", action="store_true", help="Vul ontbrekende candles op met flat candles (repair)")

    p.add_argument("--prowl", action="store_true", help="Stuur meldingen via Prowl (key.txt)")
    p.add_argument("--keyfile", default="key.txt", help="Pad naar Prowl key file")
    p.add_argument("--appname", default="GoldBot", help="Prowl application name")
    args = p.parse_args()

    tf_s = TIMEFRAMES_S[args.timeframe]

    apikey = ""
    if args.prowl:
        apikey = read_prowl_key(args.keyfile)

    conn = sqlite3.connect(args.db)
    try:
        init_bot_db(conn)
        migrate_bot_schema(conn)  # <- zorgt dat signal_kind/note etc. bestaan in oude DB

        if args.repair_on_start:
            run_repair_on_start(
                conn=conn,
                currencies_csv=args.currencies,
                timeframe=args.timeframe,
                timeframe_s=tf_s,
                repair_days=args.repair_days,
                fill_gaps=args.repair_fill_gaps,
            )

        while True:
            for cur in currency_list(args.currencies):
                ticks = load_ticks(conn, cur, args.lookback_days)
                lp = latest_price(ticks)
                if lp is None:
                    continue

                last_ts_ms, last_px = lp
                bars = resample_ticks_to_bars(ticks, tf_s)
                if len(bars) > args.max_bars:
                    bars = bars[-args.max_bars:]

                if not bars:
                    continue

                closes = [b.close for b in bars]
                ema20_vals = ema(closes, 20)
                ema50_vals = ema(closes, 50)
                rsi14_vals = rsi(closes, 14)
                atr14_vals = atr(bars, 14)
                macd_line, macd_sig, macd_hist = macd(closes, 12, 26, 9)

                upsert_indicators_for_new_bars(
                    conn, cur, args.timeframe,
                    bars, ema20_vals, ema50_vals, rsi14_vals, atr14_vals,
                    macd_line, macd_sig, macd_hist
                )
                upsert_debug_for_new_bars(
                    conn, cur, args.timeframe,
                    bars, ema50_vals, rsi14_vals, atr14_vals,
                    macd_line, macd_sig, macd_hist
                )

                if len(bars) < args.min_bars:
                    continue

                active = get_active_position(conn, cur, args.timeframe)

                # ACTIVE positie: TP/SL checks
                if active is not None:
                    pos_id = int(active["id"])
                    signal_id = int(active["signal_id"])
                    direction = str(active["direction"])
                    entry_ts_ms = int(active["entry_ts_ms"])
                    entry_price = float(active["entry_price"])
                    sl = float(active["sl"])
                    tp1 = float(active["tp1"])
                    tp2 = float(active["tp2"])
                    tp1_hit = int(active["tp1_hit"]) == 1

                    mm = minmax_since(conn, cur, entry_ts_ms)
                    if mm is None:
                        continue

                    mn, mx, mn_ts, mx_ts = mm

                    if direction == "LONG":
                        if mn <= sl:
                            close_price = sl
                            close_ts = mn_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "SL_HIT", close_ts, close_price, note="Stop loss geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, cur, args.timeframe, pnl_pct)
                            stats_note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=stats_note)

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} CLOSED: SL\n"
                                    f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                    f"{stats_note}\n\n" + fmt_levels(direction, entry_price, sl, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} SL", msg)

                        elif mx >= tp2:
                            close_price = tp2
                            close_ts = mx_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "TP2_HIT", close_ts, close_price, note="TP2 geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, cur, args.timeframe, pnl_pct)
                            stats_note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=stats_note)

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} CLOSED: TP2\n"
                                    f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                    f"{stats_note}\n\n" + fmt_levels(direction, entry_price, sl, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} TP2", msg)

                        elif (not tp1_hit) and (mx >= tp1):
                            mark_tp1(conn, pos_id, new_sl=entry_price, tp1_price=tp1, tp1_ts_ms=mx_ts)
                            add_event(conn, signal_id, "TP1_HIT", mx_ts, tp1, note="TP1 geraakt; SL naar breakeven")

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} HIT: TP1\n"
                                    f"TP1: {tp1:.4f} | SL -> BE: {entry_price:.4f}\n\n"
                                    + fmt_levels(direction, entry_price, entry_price, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} TP1", msg)

                    else:  # SHORT
                        if mx >= sl:
                            close_price = sl
                            close_ts = mx_ts
                            pnl_pct = (entry_price - close_price) / entry_price
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "SL_HIT", close_ts, close_price, note="Stop loss geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, cur, args.timeframe, pnl_pct)
                            stats_note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=stats_note)

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} CLOSED: SL\n"
                                    f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                    f"{stats_note}\n\n" + fmt_levels(direction, entry_price, sl, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} SL", msg)

                        elif mn <= tp2:
                            close_price = tp2
                            close_ts = mn_ts
                            pnl_pct = (entry_price - close_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "TP2_HIT", close_ts, close_price, note="TP2 geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, cur, args.timeframe, pnl_pct)
                            stats_note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=stats_note)

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} CLOSED: TP2\n"
                                    f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                    f"{stats_note}\n\n" + fmt_levels(direction, entry_price, sl, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} TP2", msg)

                        elif (not tp1_hit) and (mn <= tp1):
                            mark_tp1(conn, pos_id, new_sl=entry_price, tp1_price=tp1, tp1_ts_ms=mn_ts)
                            add_event(conn, signal_id, "TP1_HIT", mn_ts, tp1, note="TP1 geraakt; SL naar breakeven")

                            if args.prowl:
                                msg = (
                                    f"{cur} {args.timeframe} HIT: TP1\n"
                                    f"TP1: {tp1:.4f} | SL -> BE: {entry_price:.4f}\n\n"
                                    + fmt_levels(direction, entry_price, entry_price, tp1, tp2, atr)
                                )
                                prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} TP1", msg)

                    continue  # next currency

                # Geen actieve positie -> signaal zoeken
                sig = compute_cross_signal(
                    bars, ema50_vals, rsi14_vals, atr14_vals,
                    macd_line, macd_sig, macd_hist,
                    last_tick_price=last_px,
                    min_bars=args.min_bars,
                )

                last_bar_ts, _ = last_signal_guard(conn, cur, args.timeframe)

                # INIT bij start (alleen als nog nooit signaal voor deze currency+tf)
                if sig is None and args.init_signal and last_bar_ts == 0:
                    sig = compute_init_signal(
                        bars, ema50_vals, rsi14_vals, atr14_vals,
                        macd_line, macd_sig, macd_hist,
                        last_tick_price=last_px,
                        min_bars=args.min_bars,
                    )
                    if sig is not None:
                        mark_debug_decision(conn, cur, args.timeframe, sig.bar_ts_ms, f"{sig.direction}_INIT", " | init_signal=1")

                if sig is None:
                    continue

                # Guard: CROSS niet herhalen op zelfde bar; INIT alleen bij guard=0
                if sig.kind == "CROSS" and sig.bar_ts_ms <= last_bar_ts:
                    continue

                signal_id = insert_signal(conn, cur, args.timeframe, sig, entry_ts_ms=last_ts_ms)
                add_event(conn, signal_id, "NEW_SIGNAL", last_ts_ms, sig.entry_price, note=sig.note)

                pos_id = open_position(conn, cur, args.timeframe, signal_id, sig, entry_ts_ms=last_ts_ms)
                set_last_signal_guard(conn, cur, args.timeframe, sig.bar_ts_ms, sig.direction)

                if args.prowl:
                    msg = (
                        f"{cur} {args.timeframe} NEW SIGNAL ({sig.kind})\n"
                        f"Time(UTC): {now_utc_iso()}\n"
                        f"SignalID: {signal_id} | PosID: {pos_id}\n\n"
                        + fmt_levels(sig.direction, sig.entry_price, sig.sl, sig.tp1, sig.tp2, getattr(sig, "atr", None))
                        + "\n\n"
                        f"Rationale: {sig.note}\n"
                        "Regel: alleen bericht bij nieuw signaal of als TP/SL geraakt is."
                    )
                    prowl_send(apikey, args.appname, f"Gold {cur} {args.timeframe} NEW {sig.direction} ({sig.kind})", msg)

            time.sleep(max(1, args.interval))

    finally:
        conn.close()


if __name__ == "__main__":
    main()