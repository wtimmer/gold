#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XAUUSD Rule Signal Bot (USD-only, no CLI params)

Purpose:
- Reads precomputed OHLC + indicators from goldprice.sqlite
  (uses tables: ohlc, bot_indicators)
- Applies the user's stored discretionary rules as deterministic logic
- Writes trade signals to bot_signals + opens positions in bot_positions
- Tracks outcomes (TP1/TP2/SL) and updates bot_stats
- Sends Prowl notifications for:
    - bot start
    - new signal
    - TP1 hit
    - position closed (win/loss/breakeven)
    - bot stop (graceful exit)

Assumptions:
- Currency fixed: USD
- Symbol fixed: XAUUSD (represented in DB as currency='USD' with XAU price)
- Primary timeframe: 1m
- Indicators are already computed and stored in bot_indicators.
"""

from __future__ import annotations

import os
import sys
import time
import math
import json
import signal
import atexit
import sqlite3
import shutil
import datetime as _dt
from typing import Optional, Tuple, Dict, Any, List

# ----------------------------
# Fixed configuration (NO CLI)
# ----------------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "goldprice.sqlite")
CURRENCY = "USD"
TIMEFRAME = "1m"

# Key level rule (user-defined)
KEY_RESISTANCE = 4347.0

# Polling
SLEEP_SECONDS = 2.0  # continuous loop cadence
LOOKBACK_BARS_FOR_FAILED_BREAKOUT = 12

# Safety / spam control
MIN_SECONDS_BETWEEN_SIGNALS = 60  # don't emit multiple signals too quickly

# Risk / target model (ATR-based; robust across regimes)
TP1_ATR_MULT = 1.0
TP2_ATR_MULT = 2.0
SL_ATR_MULT = 1.0

# "Chop around mid-band" filter thresholds
CHOP_MAX_DISTANCE_TO_MID_ATR = 0.25  # within 0.25*ATR of mid-band is "mid chop"
CHOP_MAX_ABS_MACD_HIST = 0.02        # histogram too small => no momentum expansion

# Prowl
PROWL_APPNAME = "XAUUSD Rule Bot"
PROWL_KEY_FILE = "key.txt"          # optional
PROWL_ENV_VAR = "PROWL_API_KEY"     # optional
PROWL_TIMEOUT = 15


# ----------------------------
# Helpers
# ----------------------------
def utc_now() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"



def open_writable_db(db_path: str) -> sqlite3.Connection:
    """
    If DB is mounted read-only, transparently copy to a writable sibling file and use that instead.
    """
    try:
        conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        # quick write test
        conn.execute("CREATE TABLE IF NOT EXISTS __write_test (id INTEGER);")
        conn.execute("DROP TABLE IF EXISTS __write_test;")
        return conn
    except sqlite3.OperationalError as e:
        try:
            conn.close()
        except Exception:
            pass
        msg = str(e).lower()
        if "readonly" in msg or "read-only" in msg:
            base, ext = os.path.splitext(db_path)
            rw_path = base + "_rw" + (ext if ext else ".sqlite")
            shutil.copy2(db_path, rw_path)
            conn = sqlite3.connect(rw_path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA journal_mode=WAL;")
            print(f"[WARN] Database was read-only. Working copy created: {rw_path}")
            return conn
        raise


def read_prowl_key() -> Optional[str]:
    # 1) env var
    k = os.environ.get(PROWL_ENV_VAR)
    if k and len(k.strip()) >= 20:
        return k.strip()

    # 2) key.txt
    if os.path.exists(PROWL_KEY_FILE):
        try:
            with open(PROWL_KEY_FILE, "r", encoding="utf-8") as f:
                k2 = f.read().strip()
            if k2 and len(k2) >= 20:
                return k2
        except Exception:
            return None
    return None


def prowl_send(apikey: str, application: str, event: str, description: str, priority: int = 0) -> None:
    # Lazy import to keep dependency optional if Prowl not used
    import requests

    url = "https://api.prowlapp.com/publicapi/add"
    data = {
        "apikey": apikey,
        "application": application[:256],
        "event": event[:1024],
        "description": description[:10000],
        "priority": str(int(priority)),
    }
    r = requests.post(url, data=data, timeout=PROWL_TIMEOUT)
    r.raise_for_status()


def pips_between(a: float, b: float, pip_size: float = 0.01) -> int:
    return int(round(abs(a - b) / pip_size))


def fmt_signal(direction: str, entry: float, sl: float, tp1: float, tp2: float, atr: float, note: str) -> str:
    pip_size = 0.01
    sl_pips = pips_between(entry, sl, pip_size)
    tp1_pips = pips_between(entry, tp1, pip_size)
    tp2_pips = pips_between(entry, tp2, pip_size)

    # Runner trailing suggestion in pips (ATR-driven)
    trail_mult = 1.2
    trail_pips = max(10, int(round((atr * trail_mult) / pip_size)))

    lines = []
    lines.append(f"XAUUSD {TIMEFRAME} ADVIES")
    lines.append(f"Richting: {direction}")
    lines.append("")
    lines.append(f"Entry: {entry:.2f}")
    lines.append(f"SL:    {sl:.2f}  ({sl_pips} pips)")
    lines.append(f"TP1:   {tp1:.2f}  (+{tp1_pips} pips)")
    lines.append(f"TP2:   {tp2:.2f}  (+{tp2_pips} pips)")
    lines.append("")
    lines.append("Execution (2-posities):")
    lines.append("A) Positie A: sluit op TP1.")
    lines.append("B) Runner: na TP1 → SL naar BE; daarna trailing.")
    lines.append(f"   Trailing (richtlijn): {trail_pips} pips")
    lines.append("")
    lines.append(f"Note: {note}")
    return "\n".join(lines)


# ----------------------------
# DB schema guard (reuse existing bot tables)
# ----------------------------
BOT_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS bot_last_signal (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  last_signal_bar_ts_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(currency, timeframe)
);

-- bot_signals / bot_positions / bot_stats already exist in your DB,
-- but we guard the core tables defensively:
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
  signal_kind TEXT NOT NULL DEFAULT 'RULES',
  note TEXT
);

CREATE TABLE IF NOT EXISTS bot_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  status TEXT NOT NULL,
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
  close_reason TEXT,
  pnl_pct REAL,
  signal_id INTEGER,
  last_check_ts_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bot_stats (
  currency TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  total_trades INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  breakevens INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(currency, timeframe)
);
"""


def connect_db(path: str) -> sqlite3.Connection:
    # Use a writable DB (creates a working copy if original is read-only)
    try:
        conn = open_writable_db(path)
        conn.executescript(BOT_DDL)
        conn.execute(
            "INSERT OR IGNORE INTO bot_stats(currency,timeframe,total_trades,wins,losses,breakevens) VALUES (?,?,?,?,?,?)",
            (CURRENCY, TIMEFRAME, 0, 0, 0, 0),
        )
        conn.execute(
            "INSERT OR IGNORE INTO bot_last_signal(currency,timeframe,last_signal_bar_ts_ms) VALUES (?,?,?)",
            (CURRENCY, TIMEFRAME, 0),
        )
        return conn
    except sqlite3.OperationalError as e:
        # Fallback: if we hit a read-only error during initialization, force a copied DB.
        msg = str(e).lower()
        if "readonly" in msg or "read-only" in msg:
            base, ext = os.path.splitext(path)
            rw_path = base + "_rw" + (ext if ext else ".sqlite")
            shutil.copy2(path, rw_path)
            conn = sqlite3.connect(rw_path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.executescript(BOT_DDL)
            conn.execute(
                "INSERT OR IGNORE INTO bot_stats(currency,timeframe,total_trades,wins,losses,breakevens) VALUES (?,?,?,?,?,?)",
                (CURRENCY, TIMEFRAME, 0, 0, 0, 0),
            )
            conn.execute(
                "INSERT OR IGNORE INTO bot_last_signal(currency,timeframe,last_signal_bar_ts_ms) VALUES (?,?,?)",
                (CURRENCY, TIMEFRAME, 0),
            )
            print(f"[WARN] DB init required writes; working copy created: {rw_path}")
            return conn
        raise



# ----------------------------
# Data fetch
# ----------------------------
def fetch_latest_indicator_rows(conn: sqlite3.Connection, limit: int = 50) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT bar_start_ts_ms, close, ema20, ema50, rsi14, atr14, macd, macd_signal, macd_hist,
               bb_mid, bb_upper, bb_lower
        FROM bot_indicators
        WHERE currency=? AND timeframe=?
        ORDER BY bar_start_ts_ms DESC
        LIMIT ?
        """,
        (CURRENCY, TIMEFRAME, int(limit)),
    ).fetchall()

    cols = ["ts","close","ema20","ema50","rsi14","atr14","macd","macd_signal","macd_hist","bb_mid","bb_upper","bb_lower"]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        out.append(d)
    out.reverse()  # chronological
    return out


def fetch_ohlc_by_ts(conn: sqlite3.Connection, ts_ms: int) -> Optional[Dict[str, Any]]:
    r = conn.execute(
        """
        SELECT CAST(strftime('%s', period_start_utc) AS INTEGER)*1000 AS period_start_ts_ms, open, high, low, close
        FROM ohlc
        WHERE currency=? AND timeframe=? AND CAST(strftime('%s', period_start_utc) AS INTEGER)*1000=?
        """,
        (CURRENCY, TIMEFRAME, int(ts_ms)),
    ).fetchone()
    if not r:
        return None
    return {"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]}


def fetch_new_ohlc_since(conn: sqlite3.Connection, ts_ms: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT CAST(strftime('%s', period_start_utc) AS INTEGER)*1000 AS period_start_ts_ms, open, high, low, close
        FROM ohlc
        WHERE currency=? AND timeframe=? AND CAST(strftime('%s', period_start_utc) AS INTEGER)*1000>?
        ORDER BY CAST(strftime('%s', period_start_utc) AS INTEGER)*1000 ASC
        """,
        (CURRENCY, TIMEFRAME, int(ts_ms)),
    ).fetchall()
    return [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]


def get_last_processed_ts(conn: sqlite3.Connection) -> int:
    r = conn.execute(
        "SELECT last_signal_bar_ts_ms FROM bot_last_signal WHERE currency=? AND timeframe=?",
        (CURRENCY, TIMEFRAME),
    ).fetchone()
    return int(r[0]) if r else 0


def set_last_processed_ts(conn: sqlite3.Connection, ts_ms: int) -> None:
    conn.execute(
        "UPDATE bot_last_signal SET last_signal_bar_ts_ms=? WHERE currency=? AND timeframe=?",
        (int(ts_ms), CURRENCY, TIMEFRAME),
    )


# ----------------------------
# Rule logic
# ----------------------------
def is_chop_mid(row: Dict[str, Any]) -> bool:
    atr = row.get("atr14") or 0.0
    if atr <= 0:
        return True
    dist = abs((row.get("close") or 0.0) - (row.get("bb_mid") or 0.0))
    macd_hist = row.get("macd_hist")
    if macd_hist is None:
        return True
    return (dist <= CHOP_MAX_DISTANCE_TO_MID_ATR * atr) and (abs(macd_hist) <= CHOP_MAX_ABS_MACD_HIST)


def momentum_expanding(rows: List[Dict[str, Any]], side: str, n: int = 3) -> bool:
    # side: "up" or "down"
    if len(rows) < n + 1:
        return False
    h = [r["macd_hist"] for r in rows[-(n+1):] if r.get("macd_hist") is not None]
    if len(h) < n + 1:
        return False
    # expansion = monotonic move in desired direction
    if side == "up":
        return all(h[i] < h[i+1] for i in range(len(h)-1))
    if side == "down":
        return all(h[i] > h[i+1] for i in range(len(h)-1))
    return False


def detect_failed_breakout(rows: List[Dict[str, Any]]) -> bool:
    """
    Heuristic version of:
    - Price tests upper BB but does NOT get a convincing close above KEY_RESISTANCE,
    - then falls back below mid BB.
    Because bot_indicators doesn't store high/low, we approximate with closes.
    """
    if len(rows) < LOOKBACK_BARS_FOR_FAILED_BREAKOUT:
        return False

    recent = rows[-LOOKBACK_BARS_FOR_FAILED_BREAKOUT:]
    # find any close close-to-upper (upper test), but not above key resistance
    tested_upper = False
    for r in recent:
        atr = r.get("atr14") or 0.0
        if atr <= 0:
            continue
        if r.get("close") is None or r.get("bb_upper") is None:
            continue
        if r["close"] >= (r["bb_upper"] - 0.10 * atr) and r["close"] < KEY_RESISTANCE:
            tested_upper = True
            break

    if not tested_upper:
        return False

    # confirmation: current close below mid-band
    last = rows[-1]
    if last.get("close") is None or last.get("bb_mid") is None:
        return False
    return last["close"] < last["bb_mid"]


def should_long(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    last = rows[-1]
    atr = last.get("atr14") or 0.0
    if atr <= 0:
        return None

    # No-trade: chop around mid without momentum expansion
    if is_chop_mid(last):
        return None

    # Long only if: convincing close above upper band AND above key resistance, momentum expanding
    if (
        last.get("close") is not None
        and last.get("bb_upper") is not None
        and last.get("ema20") is not None
        and last.get("ema50") is not None
        and last["close"] > last["bb_upper"]
        and last["close"] > KEY_RESISTANCE
        and last["ema20"] >= last["ema50"]
        and momentum_expanding(rows, "up", n=2)
    ):
        entry = last["close"]
        sl = entry - (SL_ATR_MULT * atr)
        tp1 = entry + (TP1_ATR_MULT * atr)
        tp2 = entry + (TP2_ATR_MULT * atr)
        return {
            "direction": "LONG",
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "atr": atr,
            "kind": "BREAKOUT_VALID",
            "note": f"Close boven upper BB + boven {KEY_RESISTANCE:.0f} met momentum-expansie.",
            "ts": last["ts"],
        }

    return None


def should_short(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    last = rows[-1]
    atr = last.get("atr14") or 0.0
    if atr <= 0:
        return None

    # No-trade: chop around mid without momentum expansion
    if is_chop_mid(last):
        return None

    # Base bearish bias: below mid-band + bearish momentum
    below_mid = (last.get("close") is not None and last.get("bb_mid") is not None and last["close"] < last["bb_mid"])
    bearish_momo = (last.get("macd_hist") is not None and last["macd_hist"] < 0)

    # Rule: failed breakout => short bias (once below mid-band)
    failed = detect_failed_breakout(rows)

    # Strong bearish impulse + consolidation under mid-band (mid as resistance): short bias
    mid_resistance = below_mid

    if (failed or mid_resistance) and bearish_momo:
        entry = last["close"]
        # SL above upper band / fakeout zone
        bb_upper = last.get("bb_upper")
        if bb_upper is None:
            bb_upper = entry + atr
        sl = max(bb_upper + 0.25 * atr, entry + 0.5 * atr)

        tp1 = entry - (TP1_ATR_MULT * atr)
        tp2 = entry - (TP2_ATR_MULT * atr)

        kind = "FAILED_BREAKOUT_SHORT" if failed else "MID_RESISTANCE_SHORT"
        note = "Mislukte push aan upper BB/Key resistance; close onder mid-band => short-bias." if failed else \
               "Bearish impuls/consolidatie onder mid-band (mid=R) + bearish momentum => short-bias (geen countertrend longs)."

        return {
            "direction": "SHORT",
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "atr": atr,
            "kind": kind,
            "note": note,
            "ts": last["ts"],
        }

    return None


def decide_signal(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # Priority: if both trigger (rare), prefer NO TRADE unless clearly one side dominates.
    s = should_short(rows)
    l = should_long(rows)
    if s and l:
        return None
    return s or l


# ----------------------------
# Signal emission & position tracking
# ----------------------------
def can_emit_signal(conn: sqlite3.Connection, ts_ms: int) -> bool:
    # Avoid spamming: do not emit if a position is still open or if too soon since last signal
    open_cnt = conn.execute(
        "SELECT COUNT(*) FROM bot_positions WHERE currency=? AND timeframe=? AND status='OPEN'",
        (CURRENCY, TIMEFRAME),
    ).fetchone()[0]
    if open_cnt and open_cnt > 0:
        return False

    last_sig_ts = conn.execute(
        "SELECT MAX(signal_bar_ts_ms) FROM bot_signals WHERE currency=? AND timeframe=?",
        (CURRENCY, TIMEFRAME),
    ).fetchone()[0]
    if last_sig_ts is None:
        return True
    return (ts_ms - int(last_sig_ts)) >= (MIN_SECONDS_BETWEEN_SIGNALS * 1000)


def insert_signal_and_open_position(conn: sqlite3.Connection, sig: Dict[str, Any]) -> int:
    now = utc_now()
    direction = sig["direction"]
    ts_ms = int(sig["ts"])
    entry = float(sig["entry"])
    atr = float(sig["atr"])
    sl = float(sig["sl"])
    tp1 = float(sig["tp1"])
    tp2 = float(sig["tp2"])
    kind = sig.get("kind", "RULES")
    note = sig.get("note", "")

    cur = conn.execute(
        """
        INSERT INTO bot_signals(created_at_utc,currency,timeframe,direction,signal_bar_ts_ms,entry_ts_ms,entry_price,atr,sl,tp1,tp2,signal_kind,note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (now, CURRENCY, TIMEFRAME, direction, ts_ms, ts_ms, entry, atr, sl, tp1, tp2, kind, note),
    )
    signal_id = cur.lastrowid

    conn.execute(
        """
        INSERT INTO bot_positions(created_at_utc,currency,timeframe,status,direction,signal_bar_ts_ms,entry_ts_ms,entry_price,atr,sl,tp1,tp2,tp1_hit,signal_id,last_check_ts_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (now, CURRENCY, TIMEFRAME, "OPEN", direction, ts_ms, ts_ms, entry, atr, sl, tp1, tp2, 0, signal_id, ts_ms),
    )
    # update stats total trades
    conn.execute(
        "UPDATE bot_stats SET total_trades=total_trades+1 WHERE currency=? AND timeframe=?",
        (CURRENCY, TIMEFRAME),
    )
    return int(signal_id)


def update_positions_with_candle(conn: sqlite3.Connection, candle: Dict[str, Any], prowl_key: Optional[str]) -> None:
    ts = int(candle["ts"])
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])

    positions = conn.execute(
        """
        SELECT id, direction, entry_price, sl, tp1, tp2, tp1_hit, signal_id
        FROM bot_positions
        WHERE currency=? AND timeframe=? AND status='OPEN'
        """,
        (CURRENCY, TIMEFRAME),
    ).fetchall()

    for (pid, direction, entry, sl, tp1, tp2, tp1_hit, signal_id) in positions:
        direction = str(direction).upper()

        def send(event: str, desc: str, priority: int = 0) -> None:
            if prowl_key:
                try:
                    prowl_send(prowl_key, PROWL_APPNAME, event, desc, priority=priority)
                except Exception:
                    pass

        # LONG logic
        if direction == "LONG":
            # SL hit?
            if low <= sl:
                reason = "SL_HIT" if not tp1_hit else "RUNNER_BE_HIT"
                close_price = sl
                close_position(conn, pid, ts, close_price, reason)
                if not tp1_hit:
                    conn.execute("UPDATE bot_stats SET losses=losses+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                else:
                    conn.execute("UPDATE bot_stats SET breakevens=breakevens+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                send(f"XAUUSD {TIMEFRAME} {reason}", f"Positie gesloten @ {close_price:.2f} (id={pid})", priority=0)
                continue

            # TP1 hit?
            if (not tp1_hit) and high >= tp1:
                conn.execute(
                    "UPDATE bot_positions SET tp1_hit=1,tp1_ts_ms=?,tp1_price=? WHERE id=?",
                    (ts, tp1, pid),
                )
                # move SL to BE for runner
                conn.execute("UPDATE bot_positions SET sl=? WHERE id=?", (entry, pid))
                send(f"XAUUSD {TIMEFRAME} TP1 HIT", f"TP1 @ {tp1:.2f}. Runner SL->BE ({entry:.2f}) (id={pid})", priority=0)

            # TP2 hit?
            if high >= tp2:
                close_position(conn, pid, ts, tp2, "TP2_HIT")
                conn.execute("UPDATE bot_stats SET wins=wins+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                send(f"XAUUSD {TIMEFRAME} TP2 HIT", f"Win. TP2 @ {tp2:.2f} (id={pid})", priority=1)
                continue

        # SHORT logic
        if direction == "SHORT":
            # SL hit?
            if high >= sl:
                reason = "SL_HIT" if not tp1_hit else "RUNNER_BE_HIT"
                close_price = sl
                close_position(conn, pid, ts, close_price, reason)
                if not tp1_hit:
                    conn.execute("UPDATE bot_stats SET losses=losses+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                else:
                    conn.execute("UPDATE bot_stats SET breakevens=breakevens+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                send(f"XAUUSD {TIMEFRAME} {reason}", f"Positie gesloten @ {close_price:.2f} (id={pid})", priority=0)
                continue

            # TP1 hit?
            if (not tp1_hit) and low <= tp1:
                conn.execute(
                    "UPDATE bot_positions SET tp1_hit=1,tp1_ts_ms=?,tp1_price=? WHERE id=?",
                    (ts, tp1, pid),
                )
                # move SL to BE for runner
                conn.execute("UPDATE bot_positions SET sl=? WHERE id=?", (entry, pid))
                send(f"XAUUSD {TIMEFRAME} TP1 HIT", f"TP1 @ {tp1:.2f}. Runner SL->BE ({entry:.2f}) (id={pid})", priority=0)

            # TP2 hit?
            if low <= tp2:
                close_position(conn, pid, ts, tp2, "TP2_HIT")
                conn.execute("UPDATE bot_stats SET wins=wins+1 WHERE currency=? AND timeframe=?", (CURRENCY, TIMEFRAME))
                send(f"XAUUSD {TIMEFRAME} TP2 HIT", f"Win. TP2 @ {tp2:.2f} (id={pid})", priority=1)
                continue


def close_position(conn: sqlite3.Connection, pid: int, ts_ms: int, price: float, reason: str) -> None:
    # pnl_pct as relative to entry
    r = conn.execute("SELECT direction, entry_price FROM bot_positions WHERE id=?", (pid,)).fetchone()
    if not r:
        return
    direction, entry = r
    direction = str(direction).upper()
    entry = float(entry)

    if direction == "LONG":
        pnl_pct = (price - entry) / entry * 100.0
    else:
        pnl_pct = (entry - price) / entry * 100.0

    conn.execute(
        """
        UPDATE bot_positions
        SET status='CLOSED',
            closed_ts_ms=?,
            close_price=?,
            close_reason=?,
            pnl_pct=?
        WHERE id=?
        """,
        (int(ts_ms), float(price), str(reason), float(pnl_pct), int(pid)),
    )


# ----------------------------
# Main loop
# ----------------------------
class BotRuntime:
    def __init__(self):
        self.conn: Optional[sqlite3.Connection] = None
        self.prowl_key: Optional[str] = None
        self._stopping = False

    def stop(self):
        self._stopping = True


RUNTIME = BotRuntime()


def on_exit():
    if RUNTIME.conn:
        try:
            if RUNTIME.prowl_key:
                try:
                    prowl_send(RUNTIME.prowl_key, PROWL_APPNAME, "Bot stopped", f"Stopped at {utc_now()}", priority=0)
                except Exception:
                    pass
            RUNTIME.conn.close()
        except Exception:
            pass


def handle_signal(sig_num, frame):
    RUNTIME.stop()


def main() -> int:
    # DB
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB not found: {DB_PATH}")
        return 2

    conn = connect_db(DB_PATH)
    RUNTIME.conn = conn

    # Prowl
    prowl_key = read_prowl_key()
    RUNTIME.prowl_key = prowl_key

    if prowl_key:
        try:
            prowl_send(prowl_key, PROWL_APPNAME, "Bot started", f"Started at {utc_now()} (USD, {TIMEFRAME})", priority=0)
        except Exception:
            pass

    # Register graceful shutdown
    atexit.register(on_exit)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Loop: process new bars
    last_bar_ts = get_last_processed_ts(conn)
    if last_bar_ts == 0:
        # initialize to latest available bar - 2 bars (avoid partial)
        r = conn.execute(
            "SELECT MAX(CAST(strftime('%s', period_start_utc) AS INTEGER)*1000) FROM ohlc WHERE currency=? AND timeframe=?",
            (CURRENCY, TIMEFRAME),
        ).fetchone()
        if r and r[0]:
            last_bar_ts = int(r[0]) - 2 * 60_000
            set_last_processed_ts(conn, last_bar_ts)

    print(f"[INFO] Running. DB={DB_PATH} currency={CURRENCY} tf={TIMEFRAME} last_ts={last_bar_ts}")

    while not RUNTIME._stopping:
        try:
            new_candles = fetch_new_ohlc_since(conn, last_bar_ts)
            if not new_candles:
                time.sleep(SLEEP_SECONDS)
                continue

            for candle in new_candles:
                ts = int(candle["ts"])

                # Update existing open positions on each new bar
                update_positions_with_candle(conn, candle, prowl_key)

                # Signal evaluation uses indicator rows up to this ts
                # Fetch last N indicators and ensure last row matches current ts
                rows = fetch_latest_indicator_rows(conn, limit=60)
                if not rows:
                    last_bar_ts = ts
                    set_last_processed_ts(conn, last_bar_ts)
                    continue

                # Ensure we are evaluating near the end: pick rows ending at <= ts
                rows2 = [r for r in rows if int(r["ts"]) <= ts]
                if len(rows2) < 15:
                    last_bar_ts = ts
                    set_last_processed_ts(conn, last_bar_ts)
                    continue

                # Avoid duplicate signals
                if can_emit_signal(conn, ts):
                    sig = decide_signal(rows2)
                    if sig and int(sig["ts"]) == ts:
                        signal_id = insert_signal_and_open_position(conn, sig)
                        msg = fmt_signal(sig["direction"], sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["atr"], sig["note"])
                        print(f"\n[SIGNAL] id={signal_id}\n{msg}\n")
                        if prowl_key:
                            try:
                                prowl_send(prowl_key, PROWL_APPNAME,
                                           f"XAUUSD {TIMEFRAME} {sig['direction']} ({sig['kind']})",
                                           msg, priority=1)
                            except Exception:
                                pass

                last_bar_ts = ts
                set_last_processed_ts(conn, last_bar_ts)

            time.sleep(0.25)

        except Exception as e:
            print(f"[WARN] Loop error: {e}")
            time.sleep(SLEEP_SECONDS)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
