#!/usr/bin/env python3
"""
Gold (XAU) signal + trade-tracking bot (SQLite -> Prowl)

Gedrag (zoals jij vroeg):
- Stuur alleen een Prowl-bericht bij een NIEUW signaal (met SL, TP1, TP2).
- Als TP1 / TP2 / SL geraakt wordt: stuur direct een Prowl-bericht.
- Na TP2 of SL: positie is gesloten, daarna pas weer wachten op een nieuw signaal.
- Bij het “bereiken” (TP2 of SL) stuur je ook: hoeveel gewonnen/verloren trades + winrate.

Data-bron:
- Leest ticks uit jouw SQLite tabel `ticks` (kolommen: source_ts_ms, currency, xau_price).
- Maakt daar candles van voor 1m/5m/15m/1h/1d (jij kiest met --timeframe).

Strategie (technische duiding; geen persoonlijk beleggingsadvies):
- LONG signaal: close kruist boven EMA50 en RSI14 > 55
- SHORT signaal: close kruist onder EMA50 en RSI14 < 45
- SL/TP niveaus op basis van ATR14:
  LONG: SL = entry - 2*ATR, TP1 = entry + 2*ATR, TP2 = entry + 3*ATR
  SHORT: SL = entry + 2*ATR, TP1 = entry - 2*ATR, TP2 = entry - 3*ATR
- Na TP1: we markeren TP1 geraakt en zetten SL naar breakeven (entry) (optioneel maar praktisch).

Let op:
- “Entry” wordt gezet op de actuele laatste tick-prijs op moment van signaal.
- Hits worden bepaald via min/max tick-prijs sinds entry (dus op tick-niveau).

"""

import argparse
import math
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    bar_ts_ms: int  # the bar close time bucket (start) used to decide signal
    entry_price: float
    atr: float
    sl: float
    tp1: float
    tp2: float


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
    # Prowl public API (XML response). 200 = OK.
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


# ----------------------------
# DB schema for bot state
# ----------------------------

BOT_DDL = """
CREATE TABLE IF NOT EXISTS bot_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,

  status TEXT NOT NULL,                 -- 'ACTIVE' or 'CLOSED'
  direction TEXT NOT NULL,              -- 'LONG' or 'SHORT'

  signal_bar_ts_ms INTEGER NOT NULL,    -- bar bucket used to trigger signal
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
  close_reason TEXT,                    -- 'SL', 'TP2'
  pnl_pct REAL
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
    out: List[Tuple[int, float]] = []
    for ts_ms, price in cur.fetchall():
        out.append((int(ts_ms), float(price)))
    return out


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
    if not ticks:
        return None
    return ticks[-1]


# ----------------------------
# Indicators (EMA, RSI, ATR)
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


# ----------------------------
# Signal logic
# ----------------------------

def compute_signal(
    bars: List[Bar],
    last_tick_price: float,
) -> Optional[Signal]:
    """
    Uses last CLOSED bar (second last bar) to decide.
    Entry price = current last tick price.
    """
    if len(bars) < 60:
        return None

    closes = [b.close for b in bars]
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14 = atr(bars, 14)

    i = len(bars) - 2  # last closed bar
    if i <= 0:
        return None

    e50 = ema50[i]
    e50_prev = ema50[i - 1]
    r = rsi14[i]
    a = atr14[i]

    if e50 is None or e50_prev is None or r is None or a is None:
        return None

    prev_close = bars[i - 1].close
    close = bars[i].close

    cross_up = (prev_close <= e50_prev) and (close > e50)
    cross_dn = (prev_close >= e50_prev) and (close < e50)

    # LONG
    if cross_up and r > 55:
        entry = float(last_tick_price)
        sl = entry - 2.0 * a
        tp1 = entry + 2.0 * a
        tp2 = entry + 3.0 * a
        return Signal(direction="LONG", bar_ts_ms=bars[i].start_ts_ms, entry_price=entry, atr=a, sl=sl, tp1=tp1, tp2=tp2)

    # SHORT
    if cross_dn and r < 45:
        entry = float(last_tick_price)
        sl = entry + 2.0 * a
        tp1 = entry - 2.0 * a
        tp2 = entry - 3.0 * a
        return Signal(direction="SHORT", bar_ts_ms=bars[i].start_ts_ms, entry_price=entry, atr=a, sl=sl, tp1=tp1, tp2=tp2)

    return None


# ----------------------------
# Position state + stats
# ----------------------------

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


def update_stats_on_close(conn: sqlite3.Connection, currency: str, timeframe: str, pnl_pct: float) -> Tuple[int, int, int]:
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
    return total, wins, losses


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


def open_position(conn: sqlite3.Connection, currency: str, timeframe: str, sig: Signal, entry_ts_ms: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO bot_positions (
          created_at_utc, currency, timeframe,
          status, direction,
          signal_bar_ts_ms, entry_ts_ms, entry_price,
          atr, sl, tp1, tp2,
          tp1_hit
        ) VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, 0)
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
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def mark_tp1(conn: sqlite3.Connection, pos_id: int, tp1_price: float, tp1_ts_ms: int, new_sl: float) -> None:
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


# ----------------------------
# Tick-range min/max since entry (hit detection)
# ----------------------------

def minmax_since(conn: sqlite3.Connection, currency: str, entry_ts_ms: int) -> Optional[Tuple[float, float, int, int]]:
    """
    Returns (min_price, max_price, min_ts_ms, max_ts_ms) for ticks >= entry_ts_ms.
    """
    cur = conn.execute(
        """
        SELECT
          MIN(xau_price) AS mn,
          MAX(xau_price) AS mx
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

    # fetch timestamps of first occurrence (for messaging)
    cur2 = conn.execute(
        """
        SELECT source_ts_ms
        FROM ticks
        WHERE currency = ? AND source_ts_ms >= ? AND xau_price = ?
        ORDER BY source_ts_ms ASC
        LIMIT 1
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
        ORDER BY source_ts_ms ASC
        LIMIT 1
        """,
        (currency.upper(), int(entry_ts_ms), mx),
    )
    max_ts = cur3.fetchone()
    max_ts_ms = int(max_ts[0]) if max_ts else int(entry_ts_ms)

    return mn, mx, min_ts_ms, max_ts_ms


# ----------------------------
# Main loop
# ----------------------------

def fmt_levels(direction: str, entry: float, sl: float, tp1: float, tp2: float) -> str:
    return (
        f"Dir: {direction}\n"
        f"Entry: {entry:.4f}\n"
        f"SL: {sl:.4f}\n"
        f"TP1: {tp1:.4f}\n"
        f"TP2: {tp2:.4f}"
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Gold (XAU) signal bot: notify only on new signal / TP / SL via Prowl.")
    p.add_argument("--db", default="goldprice.sqlite", help="SQLite DB pad")
    p.add_argument("--currency", default="EUR", help="Valuta (EUR/USD/GBP etc.)")
    p.add_argument("--timeframe", default="15m", choices=sorted(TIMEFRAMES_S.keys()), help="Timeframe voor signalen")
    p.add_argument("--lookback-days", type=int, default=120, help="Historie voor indicatoren/candles")
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
            # Load ticks and build bars
            ticks = load_ticks(conn, args.currency, args.lookback_days)
            lp = latest_price(ticks)
            if lp is None:
                print(f"[{now_utc_iso()}] No ticks found for {args.currency}.")
                time.sleep(max(1, args.interval))
                continue

            last_ts_ms, last_px = lp
            bars = resample_ticks_to_bars(ticks, tf_s)
            if len(bars) < 60:
                print(f"[{now_utc_iso()}] Not enough bars yet ({len(bars)}). Waiting...")
                time.sleep(max(1, args.interval))
                continue

            active = get_active_position(conn, args.currency, args.timeframe)

            # 1) If position ACTIVE: check SL/TP hits (tick-level min/max since entry)
            if active is not None:
                pos_id = int(active["id"])
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

                    # Determine triggers in a conservative order:
                    # - For LONG: SL if mn <= sl, else TP2 if mx >= tp2, else TP1 if mx >= tp1
                    # - For SHORT: SL if mx >= sl, else TP2 if mn <= tp2, else TP1 if mn <= tp1
                    if direction == "LONG":
                        if mn <= sl:
                            # SL hit
                            close_price = sl
                            close_ts = mn_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            total, wins, losses = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            winrate = (wins / total * 100.0) if total else 0.0

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: SL\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} SL", msg, priority=0)

                        elif mx >= tp2:
                            # TP2 hit (final)
                            close_price = tp2
                            close_ts = mx_ts
                            pnl_pct = (close_price - entry_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            total, wins, losses = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            winrate = (wins / total * 100.0) if total else 0.0

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: TP2\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP2", msg, priority=0)

                        elif (not tp1_hit) and (mx >= tp1):
                            # TP1 hit (partial milestone)
                            # Move SL to breakeven (entry)
                            mark_tp1(conn, pos_id, tp1_price=tp1, tp1_ts_ms=mx_ts, new_sl=entry_price)

                            msg = (
                                f"{args.currency} {args.timeframe} HIT: TP1\n"
                                f"TP1: {tp1:.4f} | SL moved to breakeven: {entry_price:.4f}\n\n"
                                + fmt_levels(direction, entry_price, entry_price, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP1", msg, priority=0)

                    else:  # SHORT
                        if mx >= sl:
                            close_price = sl
                            close_ts = mx_ts
                            pnl_pct = (entry_price - close_price) / entry_price  # short pnl
                            close_position(conn, pos_id, "SL", close_price, close_ts, pnl_pct)

                            total, wins, losses = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            winrate = (wins / total * 100.0) if total else 0.0

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: SL\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} SL", msg, priority=0)

                        elif mn <= tp2:
                            close_price = tp2
                            close_ts = mn_ts
                            pnl_pct = (entry_price - close_price) / entry_price
                            close_position(conn, pos_id, "TP2", close_price, close_ts, pnl_pct)

                            total, wins, losses = update_stats_on_close(conn, args.currency, args.timeframe, pnl_pct)
                            winrate = (wins / total * 100.0) if total else 0.0

                            msg = (
                                f"{args.currency} {args.timeframe} CLOSED: TP2\n"
                                f"Close: {close_price:.4f} | PnL: {pnl_pct*100:.2f}%\n\n"
                                f"Stats: total={total} wins={wins} losses={losses} winrate={winrate:.1f}%\n\n"
                                + fmt_levels(direction, entry_price, sl, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP2", msg, priority=0)

                        elif (not tp1_hit) and (mn <= tp1):
                            mark_tp1(conn, pos_id, tp1_price=tp1, tp1_ts_ms=mn_ts, new_sl=entry_price)

                            msg = (
                                f"{args.currency} {args.timeframe} HIT: TP1\n"
                                f"TP1: {tp1:.4f} | SL moved to breakeven: {entry_price:.4f}\n\n"
                                + fmt_levels(direction, entry_price, entry_price, tp1, tp2)
                            )
                            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
                            if args.prowl:
                                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} TP1", msg, priority=0)

                time.sleep(max(1, args.interval))
                continue

            # 2) If no ACTIVE position: check for a NEW signal (guarded so no repeats)
            sig = compute_signal(bars, last_tick_price=last_px)
            if sig is None:
                # no new signal
                time.sleep(max(1, args.interval))
                continue

            last_bar_ts, last_dir = last_signal_guard(conn, args.currency, args.timeframe)

            # Only treat as "new" if bar_ts_ms strictly greater than last stored
            if sig.bar_ts_ms <= last_bar_ts:
                time.sleep(max(1, args.interval))
                continue

            # Open new position, store guard, notify once
            pos_id = open_position(conn, args.currency, args.timeframe, sig, entry_ts_ms=last_ts_ms)
            set_last_signal_guard(conn, args.currency, args.timeframe, sig.bar_ts_ms, sig.direction)

            msg = (
                f"{args.currency} {args.timeframe} NEW SIGNAL\n"
                f"Time(UTC): {now_utc_iso()}\n"
                f"PosID: {pos_id}\n\n"
                + fmt_levels(sig.direction, sig.entry_price, sig.sl, sig.tp1, sig.tp2)
                + "\n\n"
                "Regels: bericht alleen bij nieuw signaal of als TP/SL geraakt is."
            )

            print(f"[{now_utc_iso()}] {msg.replace(chr(10), ' | ')}")
            if args.prowl:
                prowl_send(apikey, args.appname, f"Gold {args.currency} {args.timeframe} NEW {sig.direction}", msg, priority=0)

            time.sleep(max(1, args.interval))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
