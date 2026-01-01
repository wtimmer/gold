#!/usr/bin/env python3
import argparse
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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


# ----------------------------
# Helpers
# ----------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def bucket_start_ms(ts_ms: int, timeframe_s: int) -> int:
    ts_s = ts_ms // 1000
    start_s = (ts_s // timeframe_s) * timeframe_s
    return start_s * 1000


def fmt_levels(direction: str, entry: float, sl: float, tp1: float, tp2: float) -> str:
    return (
        f"Dir: {direction}\n"
        f"Entry: {entry:.4f}\n"
        f"SL: {sl:.4f}\n"
        f"TP1: {tp1:.4f}\n"
        f"TP2: {tp2:.4f}"
    )


# ----------------------------
# Bot DB schema (signals + events + indicators + position state + stats + guard)
# ----------------------------

BOT_DDL = """
CREATE TABLE IF NOT EXISTS bot_signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,

  direction TEXT NOT NULL,
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
  status TEXT NOT NULL,                 -- ACTIVE/CLOSED
  direction TEXT NOT NULL,

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
  close_reason TEXT,                    -- SL or TP2
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

-- Indicator snapshot per bar, voor charting.
-- Let op: values zijn gebaseerd op bars die jij aanlevert (resampled uit ticks).
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
"""


def init_bot_db(conn: sqlite3.Connection) -> None:
    conn.executescript(BOT_DDL)
    conn.commit()


# ----------------------------
# Load ticks and resample to bars
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
            bars.append(Bar(cur_bucket, o, h, l, c))
            cur_bucket = b
            o = h = l = c = price
        else:
            c = price
            h = max(h, price)
            l = min(l, price)

    if cur_bucket is not None:
        bars.append(Bar(cur_bucket, o, h, l, c))

    return bars


def latest_price(ticks: List[Tuple[int, float]]) -> Optional[Tuple[int, float]]:
    return ticks[-1] if ticks else None


# ----------------------------
# Indicators (EMA, RSI, ATR, MACD)
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
    """
    Returns (macd_line, signal_line, hist)
    macd_line = EMA(fast) - EMA(slow)
    signal_line = EMA(signal) of macd_line (where available)
    hist = macd_line - signal_line
    """
    ef = ema(values, fast)
    es = ema(values, slow)

    macd_line: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if ef[i] is None or es[i] is None:
            macd_line[i] = None
        else:
            macd_line[i] = float(ef[i] - es[i])

    # Build a series for EMA on macd_line; replace None with 0 in the prefix until first valid,
    # but keep signal_line None until we have enough valid values.
    signal_line: List[Optional[float]] = [None] * len(values)
    hist: List[Optional[float]] = [None] * len(values)

    # Extract macd values where not None, but keep alignment.
    # We'll compute EMA on a "filled" list, but only assign when macd_line exists.
    filled = [m if m is not None else 0.0 for m in macd_line]
    sig_ema = ema(filled, signal)

    for i in range(len(values)):
        if macd_line[i] is None:
            continue
        # Only trust sig_ema after its own warm-up index.
        if sig_ema[i] is None:
            continue
        signal_line[i] = float(sig_ema[i])
        hist[i] = float(macd_line[i] - signal_line[i])

    return macd_line, signal_line, hist


# ----------------------------
# Indicator persistence (for charting)
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
    ema20: List[Optional[float]],
    ema50: List[Optional[float]],
    rsi14: List[Optional[float]],
    atr14: List[Optional[float]],
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
            (float(ema20[i]) if ema20[i] is not None else None),
            (float(ema50[i]) if ema50[i] is not None else None),
            (float(rsi14[i]) if rsi14[i] is not None else None),
            (float(atr14[i]) if atr14[i] is not None else None),
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


# ----------------------------
# Signal logic (EMA50 cross + RSI + MACD confirmation)
# ----------------------------

def compute_signal(
    bars: List[Bar],
    closes: List[float],
    ema50: List[Optional[float]],
    rsi14: List[Optional[float]],
    atr14: List[Optional[float]],
    macd_line: List[Optional[float]],
    macd_sig: List[Optional[float]],
    macd_hist: List[Optional[float]],
    last_tick_price: float,
) -> Optional[Signal]:
    """
    Uses last CLOSED bar (second last bar) to decide.
    Entry price = current last tick price.

    LONG:
      - close crosses above EMA50
      - RSI14 > 55
      - MACD line > signal line AND histogram > 0
    SHORT:
      - close crosses below EMA50
      - RSI14 < 45
      - MACD line < signal line AND histogram < 0
    """
    if len(bars) < 60:
        return None

    i = len(bars) - 2  # last closed bar
    if i <= 1:
        return None

    e50 = ema50[i]
    e50_prev = ema50[i - 1]
    r = rsi14[i]
    a = atr14[i]
    m = macd_line[i]
    ms = macd_sig[i]
    mh = macd_hist[i]

    if e50 is None or e50_prev is None or r is None or a is None or m is None or ms is None or mh is None:
        return None

    prev_close = closes[i - 1]
    close = closes[i]

    cross_up = (prev_close <= e50_prev) and (close > e50)
    cross_dn = (prev_close >= e50_prev) and (close < e50)

    entry = float(last_tick_price)

    if cross_up and r > 55 and (m > ms) and (mh > 0):
        sl = entry - 2.0 * a
        tp1 = entry + 2.0 * a
        tp2 = entry + 3.0 * a
        note = f"EMA50 cross up, RSI14={r:.2f}, MACD={m:.5f}>{ms:.5f}, hist={mh:.5f}"
        return Signal("LONG", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note)

    if cross_dn and r < 45 and (m < ms) and (mh < 0):
        sl = entry + 2.0 * a
        tp1 = entry - 2.0 * a
        tp2 = entry - 3.0 * a
        note = f"EMA50 cross down, RSI14={r:.2f}, MACD={m:.5f}<{ms:.5f}, hist={mh:.5f}"
        return Signal("SHORT", bars[i].start_ts_ms, entry, a, sl, tp1, tp2, note)

    return None


# ----------------------------
# Bot state + stats + events
# ----------------------------

def insert_signal(conn: sqlite3.Connection, currency: str, timeframe: str, sig: Signal, entry_ts_ms: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO bot_signals(
          created_at_utc, currency, timeframe,
          direction, signal_bar_ts_ms, entry_ts_ms, entry_price,
          atr, sl, tp1, tp2, note
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_utc_iso(),
            currency.upper(),
            timeframe,
            sig.direction,
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
# Main loop
# ----------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Gold (XAU) signal bot: MACD + store indicators/signals/events; notify via Prowl.")
    p.add_argument("--db", default="goldprice.sqlite", help="SQLite DB pad")
    p.add_argument("--currency", default="EUR", help="Valuta (EUR/USD/GBP etc.)")
    p.add_argument("--timeframe", default="15m", choices=sorted(TIMEFRAMES_S.keys()), help="Timeframe voor signalen")
    p.add_argument("--lookback-days", type=int, default=60, help="Historie voor candles/indicatoren (1m: hou dit redelijk)")
    p.add_argument("--max-bars", type=int, default=10000, help="Max bars om per loop te gebruiken (performance)")
    p.add_argument("--interval", type=int, default=20, help="Loop interval seconden")
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

        while True:
            ticks = load_ticks(conn, args.currency, args.lookback_days)
            lp = latest_price(ticks)
            if lp is None:
                time.sleep(max(1, args.interval))
                continue

            last_ts_ms, last_px = lp
            bars = resample_ticks_to_bars(ticks, tf_s)

            # Performance cap
            if len(bars) > args.max_bars:
                bars = bars[-args.max_bars:]

            if len(bars) < 60:
                time.sleep(max(1, args.interval))
                continue

            closes = [b.close for b in bars]
            ema20 = ema(closes, 20)
            ema50 = ema(closes, 50)
            rsi14 = rsi(closes, 14)
            atr14 = atr(bars, 14)
            macd_line, macd_sig, macd_hist = macd(closes, 12, 26, 9)

            # 0) Store indicators for new bars (for charting)
            upsert_indicators_for_new_bars(
                conn, args.currency, args.timeframe,
                bars, ema20, ema50, rsi14, atr14,
                macd_line, macd_sig, macd_hist
            )

            active = get_active_position(conn, args.currency, args.timeframe)

            # 1) Active position -> check hits, log events
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

                mm = minmax_since(conn, args.currency, entry_ts_ms)
                if mm is not None:
                    mn, mx, mn_ts, mx_ts = mm

                    if direction == "LONG":
                        if mn <= sl:
                            close_price = sl
                            close_ts = mn_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "SL_HIT", close_ts, close_price, note="Stop loss geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=note)

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: SL\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"{note}\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} SL", msg, priority=0)

                        elif mx >= tp2:
                            close_price = tp2
                            close_ts = mx_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "TP2_HIT", close_ts, close_price, note="TP2 geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=note)

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: TP2\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"{note}\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP2", msg, priority=0)

                        elif (not tp1_hit) and (mx >= tp1):
                            mark_tp1(conn, pos_id, new_sl=entry_price, tp1_price=tp1, tp1_ts_ms=mx_ts)
                            add_event(conn, signal_id, "TP1_HIT", mx_ts, tp1, note="TP1 geraakt; SL naar breakeven")

                            msg = (
                                f"{args.currency} {args.timeframe} HIT: TP1\n"
                                f"TP1: {tp1:.4f} | SL moved to breakeven: {entry_price:.4f}\n\n"
                                + fmt_levels(direction, entry_price, entry_price, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP1", msg, priority=0)

                    else:  # SHORT
                        if mx >= sl:
                            close_price = sl
                            close_ts = mx_ts
                            pnl_pct = (entry_price - close_price) / entry_price
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "SL_HIT", close_ts, close_price, note="Stop loss geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=note)

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: SL\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"{note}\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} SL", msg, priority=0)

                        elif mn <= tp2:
                            close_price = tp2
                            close_ts = mn_ts
                            pnl_pct = (entry_price - close_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            add_event(conn, signal_id, "TP2_HIT", close_ts, close_price, note="TP2 geraakt")
                            total, wins, losses, winrate = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            note = f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%"
                            add_event(conn, signal_id, "CLOSED", close_ts, close_price, note=note)

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: TP2\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"{note}\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP2", msg, priority=0)

                        elif (not tp1_hit) and (mn <= tp1):
                            mark_tp1(conn, pos_id, new_sl=entry_price, tp1_price=tp1, tp1_ts_ms=mn_ts)
                            add_event(conn, signal_id, "TP1_HIT", mn_ts, tp1, note="TP1 geraakt; SL naar breakeven")

                            msg = (
                                f"{args.currency} {args.timeframe} HIT: TP1\n"
                                f"TP1: {tp1:.4f} | SL moved to breakeven: {entry_price:.4f}\n\n"
                                + fmt_levels(direction, entry_price, entry_price, tp1, tp2)
                            )
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP1", msg, priority=0)

                time.sleep(max(1, args.interval))
                continue

            # 2) No active position -> check new signal (with MACD), store, notify once
            sig = compute_signal(
                bars=bars,
                closes=closes,
                ema50=ema50,
                rsi14=rsi14,
                atr14=atr14,
                macd_line=macd_line,
                macd_sig=macd_sig,
                macd_hist=macd_hist,
                last_tick_price=last_px,
            )
            if sig is None:
                time.sleep(max(1, args.interval))
                continue

            last_bar_ts, _ = last_signal_guard(conn, args.currency, args.timeframe)
            if sig.bar_ts_ms <= last_bar_ts:
                time.sleep(max(1, args.interval))
                continue

            signal_id = insert_signal(conn, args.currency, args.timeframe, sig, entry_ts_ms=last_ts_ms)
            add_event(conn, signal_id, "NEW_SIGNAL", last_ts_ms, sig.entry_price, note=sig.note)

            pos_id = open_position(conn, args.currency, args.timeframe, signal_id, sig, entry_ts_ms=last_ts_ms)
            set_last_signal_guard(conn, args.currency, args.timeframe, sig.bar_ts_ms, sig.direction)

            msg = (
                f"{args.currency} {args.timeframe} NEW SIGNAL\n"
                f"Time(UTC): {now_utc_iso()}\n"
                f"SignalID: {signal_id} | PosID: {pos_id}\n\n"
                + fmt_levels(sig.direction, sig.entry_price, sig.sl, sig.tp1, sig.tp2)
                + "\n\n"
                f"Rationale: {sig.note}\n"
                "Regels: bericht alleen bij nieuw signaal of als TP/SL geraakt is."
            )
            if args.prowl:
                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} NEW {sig.direction}", msg, priority=0)

            time.sleep(max(1, args.interval))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
