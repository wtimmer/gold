#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gold_ohlc_indicators_rebuild.py

Standalone rebuild script for goldprice.sqlite:
- Rebuilds OHLC bars from raw ticks (USD only) for multiple timeframes.
- Computes a broad set of price-based indicators from OHLC.
- Stores indicators per bar in SQLite (JSON payload + key numeric fields).

Design goals (per user request):
- No required CLI parameters (runs with defaults).
- Recomputes everything on each start (idempotent rebuild).
- USD as default / only currency.
"""

import json
import math
import sqlite3
import shutil
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np


# ----------------------------
# Configuration (no parameters)
# ----------------------------
DB_PATH = "goldprice.sqlite"
CURRENCY = "USD"

# Continuous mode
SLEEP_SECONDS = 5          # poll interval
ROLLING_WINDOW_HOURS = 48  # rebuild last N hours when new ticks arrive
INDICATOR_LOOKBACK_BARS = 600  # extra lookback to stabilize indicators

# Keep these aligned with your UI usage. Add/remove as desired.
TIMEFRAMES = [
    ("1m", "1min"),
    ("5m", "5min"),
    ("15m", "15min"),
    ("30m", "30min"),
    ("1h", "1h"),
    ("4h", "4h"),
    ("1d", "1d"),
]


def normalize_pandas_freq(freq: str) -> str:
    """Normalize pandas resample frequency strings for forward compatibility."""
    return freq.replace("H", "h").replace("D", "d")


# Indicator settings (fixed; no runtime params)
SMA_PERIODS = [20, 50, 200]
EMA_PERIODS = [20, 50, 200]

RSI_PERIOD = 14
STOCH_K = 14
STOCH_D = 3

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

BB_PERIOD = 20
BB_STD = 2.0

ATR_PERIOD = 14
ADX_PERIOD = 14

CCI_PERIOD = 20
ROC_PERIOD = 12
MOM_PERIOD = 10
WILLR_PERIOD = 14

DONCHIAN_PERIOD = 20
KELTNER_EMA = 20
KELTNER_ATR = 10
KELTNER_MULT = 1.5

SUPERTREND_ATR = 10
SUPERTREND_MULT = 3.0

ICHIMOKU_TENKAN = 9
ICHIMOKU_KIJUN = 26
ICHIMOKU_SENKOU_B = 52

PSAR_AF_STEP = 0.02
PSAR_AF_MAX = 0.2

TRIX_PERIOD = 15
TSI_R = 25
TSI_S = 13
ULT_OSC = (7, 14, 28)


# ----------------------------
# Helpers
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()



def open_writable_db(db_path: str) -> sqlite3.Connection:
    """
    Some environments mount uploaded DBs as read-only. If we detect write failure,
    we transparently copy to a writable sibling file and continue there.
    """
    try:
        conn = sqlite3.connect(db_path)
        # quick write test
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("CREATE TABLE IF NOT EXISTS __write_test (id INTEGER);")
        conn.execute("DROP TABLE IF EXISTS __write_test;")
        conn.commit()
        return conn
    except sqlite3.OperationalError as e:
        try:
            conn.close()
        except Exception:
            pass
        msg = str(e).lower()
        if "readonly" in msg or "read-only" in msg:
            base = os.path.splitext(db_path)[0]
            rw_path = base + "_rw.sqlite"
            shutil.copy2(db_path, rw_path)
            conn = sqlite3.connect(rw_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.commit()
            print(f"[WARN] Database was read-only. Working copy created: {rw_path}")
            return conn
        raise


def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # Existing tables expected: ticks, ohlc
    # Create ohlc if missing (safe)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlc (
            currency TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            period_start_utc TEXT NOT NULL,
            open_ts_ms INTEGER NOT NULL,
            close_ts_ms INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            PRIMARY KEY (currency, timeframe, period_start_utc)
        )
        """
    )

    # New indicators table (JSON payload is flexible for "as many indicators as possible")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlc_indicators (
            currency TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            period_start_utc TEXT NOT NULL,

            -- convenience fields
            close REAL,
            sma20 REAL, ema20 REAL,
            rsi14 REAL,
            macd REAL, macd_signal REAL, macd_hist REAL,
            bb_mid REAL, bb_upper REAL, bb_lower REAL,
            atr14 REAL,
            adx14 REAL, di_plus REAL, di_minus REAL,

            indicators_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (currency, timeframe, period_start_utc)
        )
        """
    )

    # Helpful index for recent lookups
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_ohlc_ind_tf_time ON ohlc_indicators(timeframe, period_start_utc)"
    )

    conn.commit()


def read_ticks(conn: sqlite3.Connection, currency: str, since_ts_ms: Optional[int] = None) -> pd.DataFrame:
    """Read ticks for a currency. Optionally filter on source_ts_ms >= since_ts_ms."""
    query = """
        SELECT fetched_at_utc, source_ts_ms AS ts_ms, xau_price
        FROM ticks
        WHERE currency = ?
    """
    params = [currency]
    if since_ts_ms is not None:
        query += " AND source_ts_ms >= ?"
        params.append(int(since_ts_ms))
    query += " ORDER BY source_ts_ms ASC"

    df = pd.read_sql_query(query, conn, params=tuple(params))
    if df.empty:
        return df

    df["dt"] = pd.to_datetime(df["fetched_at_utc"], utc=True, errors="coerce")
    # Fallback: if fetched_at_utc missing/unparseable, use ts_ms
    if df["dt"].isna().any():
        df.loc[df["dt"].isna(), "dt"] = pd.to_datetime(df.loc[df["dt"].isna(), "ts_ms"], unit="ms", utc=True)

    df = df.dropna(subset=["dt", "xau_price"]).copy()

    # Fill missing ts_ms from dt if needed
    if "ts_ms" in df.columns:
        if df["ts_ms"].isna().any():
            df.loc[df["ts_ms"].isna(), "ts_ms"] = (df.loc[df["ts_ms"].isna(), "dt"].view("int64") // 1_000_000)
    else:
        df["ts_ms"] = (df["dt"].view("int64") // 1_000_000)

    df = df.set_index("dt").sort_index()
    return df


def rebuild_ohlc(conn: sqlite3.Connection, ticks: pd.DataFrame, currency: str) -> None:
    if ticks.empty:
        return

    cur = conn.cursor()

    # Clear existing OHLC for this currency (full rebuild policy)
    cur.execute("DELETE FROM ohlc WHERE currency = ?", (currency,))
    conn.commit()

    price = ticks["xau_price"].astype(float)

    for tf_label, tf_freq in TIMEFRAMES:
        tf_freq = normalize_pandas_freq(tf_freq)
        # Resample to OHLC
        ohlc = price.resample(tf_freq, label="left", closed="left").ohlc()
        # Remove empty bars
        ohlc = ohlc.dropna()

        if ohlc.empty:
            continue

        # open/close timestamps (ms) from first/last tick in each bucket
        # We compute via groupby on resample bins.
        grp = ticks["ts_ms"].resample(tf_freq, label="left", closed="left")
        open_ts = grp.first().reindex(ohlc.index)
        close_ts = grp.last().reindex(ohlc.index)

        out = pd.DataFrame(
            {
                "currency": currency,
                "timeframe": tf_label,
                "period_start_utc": ohlc.index.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open_ts_ms": open_ts.astype("int64").values,
                "close_ts_ms": close_ts.astype("int64").values,
                "open": ohlc["open"].values,
                "high": ohlc["high"].values,
                "low": ohlc["low"].values,
                "close": ohlc["close"].values,
            }
        )

        rows = list(out.itertuples(index=False, name=None))
        cur.executemany(
            """
            INSERT OR REPLACE INTO ohlc
            (currency, timeframe, period_start_utc, open_ts_ms, close_ts_ms, open, high, low, close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def rebuild_ohlc_window(conn: sqlite3.Connection, ticks: pd.DataFrame, currency: str, window_start_utc: str) -> None:
    """Rebuild OHLC bars for a recent window, leaving older history intact."""
    if ticks.empty:
        return
    cur = conn.cursor()
    # Delete existing bars in window for this currency
    cur.execute(
        "DELETE FROM ohlc WHERE currency=? AND period_start_utc >= ?",
        (currency, window_start_utc),
    )
    conn.commit()

    price = ticks["xau_price"].astype(float)
    for tf_label, tf_freq in TIMEFRAMES:
        ohlc = price.resample(tf_freq, label="left", closed="left").ohlc().dropna()
        if ohlc.empty:
            continue

        grp = ticks["ts_ms"].resample(tf_freq, label="left", closed="left")
        open_ts = grp.first().reindex(ohlc.index)
        close_ts = grp.last().reindex(ohlc.index)

        out = pd.DataFrame(
            {
                "currency": currency,
                "timeframe": tf_label,
                "period_start_utc": ohlc.index.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open_ts_ms": open_ts.astype("int64").values,
                "close_ts_ms": close_ts.astype("int64").values,
                "open": ohlc["open"].values,
                "high": ohlc["high"].values,
                "low": ohlc["low"].values,
                "close": ohlc["close"].values,
            }
        )
        # Keep only rows within the window (in case resample created a bar before window start)
        out = out[out["period_start_utc"] >= window_start_utc]
        if out.empty:
            continue

        rows = list(out.itertuples(index=False, name=None))
        cur.executemany(
            """
            INSERT OR REPLACE INTO ohlc
            (currency, timeframe, period_start_utc, open_ts_ms, close_ts_ms, open, high, low, close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def upsert_indicators_window(conn: sqlite3.Connection, currency: str, window_start_utc: str) -> None:
    """Recompute indicators only for bars within a window (plus lookback for correctness)."""
    cur = conn.cursor()

    # Delete indicator rows in window (we'll re-insert)
    cur.execute(
        "DELETE FROM ohlc_indicators WHERE currency=? AND period_start_utc >= ?",
        (currency, window_start_utc),
    )
    conn.commit()

    for tf_label, _tf_freq in TIMEFRAMES:
        df = pd.read_sql_query(
            """
            SELECT period_start_utc, open, high, low, close
            FROM ohlc
            WHERE currency = ? AND timeframe = ?
            ORDER BY period_start_utc ASC
            """,
            conn,
            params=(currency, tf_label),
        )
        if df.empty:
            continue

        df["dt"] = pd.to_datetime(df["period_start_utc"], utc=True, errors="coerce")
        df = df.dropna(subset=["dt"]).set_index("dt").sort_index()
        df = df[["open", "high", "low", "close"]].astype(float)

        # Lookback slice to stabilize rolling indicators
        if INDICATOR_LOOKBACK_BARS and len(df) > INDICATOR_LOOKBACK_BARS:
            # ensure we keep enough history before window start
            window_dt = pd.to_datetime(window_start_utc, utc=True)
            # find position of window start
            pos = df.index.searchsorted(window_dt)
            start_pos = max(0, pos - INDICATOR_LOOKBACK_BARS)
            df_calc = df.iloc[start_pos:]
        else:
            df_calc = df

        ind = compute_indicators(df_calc)

        indicator_cols = [c for c in ind.columns if c not in ("open", "high", "low", "close")]
        created_at = utc_now_iso()

        rows = []
        for idx, row in ind.iterrows():
            period_start_utc = idx.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
            if period_start_utc < window_start_utc:
                continue

            payload = {k: (None if pd.isna(row[k]) else float(row[k])) for k in indicator_cols}

            rows.append(
                (
                    currency,
                    tf_label,
                    period_start_utc,
                    None if pd.isna(row["close"]) else float(row["close"]),
                    None if pd.isna(row.get("sma20", np.nan)) else float(row["sma20"]),
                    None if pd.isna(row.get("ema20", np.nan)) else float(row["ema20"]),
                    None if pd.isna(row.get(f"rsi{RSI_PERIOD}", np.nan)) else float(row[f"rsi{RSI_PERIOD}"]),
                    None if pd.isna(row.get("macd", np.nan)) else float(row["macd"]),
                    None if pd.isna(row.get("macd_signal", np.nan)) else float(row["macd_signal"]),
                    None if pd.isna(row.get("macd_hist", np.nan)) else float(row["macd_hist"]),
                    None if pd.isna(row.get("bb_mid", np.nan)) else float(row["bb_mid"]),
                    None if pd.isna(row.get("bb_upper", np.nan)) else float(row["bb_upper"]),
                    None if pd.isna(row.get("bb_lower", np.nan)) else float(row["bb_lower"]),
                    None if pd.isna(row.get(f"atr{ATR_PERIOD}", np.nan)) else float(row[f"atr{ATR_PERIOD}"]),
                    None if pd.isna(row.get(f"adx{ADX_PERIOD}", np.nan)) else float(row[f"adx{ADX_PERIOD}"]),
                    None if pd.isna(row.get("di_plus", np.nan)) else float(row["di_plus"]),
                    None if pd.isna(row.get("di_minus", np.nan)) else float(row["di_minus"]),
                    json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                    created_at,
                )
            )

        if not rows:
            continue

        cur.executemany(
            """
            INSERT OR REPLACE INTO ohlc_indicators
            (currency, timeframe, period_start_utc,
             close, sma20, ema20, rsi14, macd, macd_signal, macd_hist,
             bb_mid, bb_upper, bb_lower,
             atr14, adx14, di_plus, di_minus,
             indicators_json, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def wma(series: pd.Series, period: int) -> pd.Series:
    w = np.arange(1, period + 1, dtype=float)
    return series.rolling(period).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)


def rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    tr = true_range(high, low, close)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = true_range(high, low, close)
    atr_s = tr.ewm(alpha=1 / period, adjust=False).mean()

    plus_di = 100 * pd.Series(plus_dm, index=high.index).ewm(alpha=1 / period, adjust=False).mean() / atr_s
    minus_di = 100 * pd.Series(minus_dm, index=high.index).ewm(alpha=1 / period, adjust=False).mean() / atr_s

    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0.0)
    adx_s = dx.ewm(alpha=1 / period, adjust=False).mean()
    return adx_s, plus_di, minus_di


def cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    tp = (high + low + close) / 3.0
    sma_tp = tp.rolling(period).mean()
    mad = tp.rolling(period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    return (tp - sma_tp) / (0.015 * mad.replace(0, np.nan))


def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    hh = high.rolling(period).max()
    ll = low.rolling(period).min()
    return -100 * (hh - close) / (hh - ll).replace(0, np.nan)


def donchian(high: pd.Series, low: pd.Series, period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    upper = high.rolling(period).max()
    lower = low.rolling(period).min()
    mid = (upper + lower) / 2.0
    return upper, mid, lower


def keltner(high: pd.Series, low: pd.Series, close: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    mid = ema(close, KELTNER_EMA)
    rng = atr(high, low, close, KELTNER_ATR)
    upper = mid + KELTNER_MULT * rng
    lower = mid - KELTNER_MULT * rng
    return upper, mid, lower


def supertrend(high: pd.Series, low: pd.Series, close: pd.Series) -> Tuple[pd.Series, pd.Series]:
    # Returns (supertrend line, direction: +1 bull, -1 bear)
    hl2 = (high + low) / 2.0
    atr_s = atr(high, low, close, SUPERTREND_ATR)
    upperband = hl2 + SUPERTREND_MULT * atr_s
    lowerband = hl2 - SUPERTREND_MULT * atr_s

    st = pd.Series(index=close.index, dtype=float)
    direction = pd.Series(index=close.index, dtype=float)

    for i in range(len(close)):
        if i == 0:
            st.iloc[i] = upperband.iloc[i]
            direction.iloc[i] = -1
            continue

        prev_st = st.iloc[i - 1]
        prev_dir = direction.iloc[i - 1]

        # Final bands
        ub = upperband.iloc[i]
        lb = lowerband.iloc[i]

        if close.iloc[i - 1] > upperband.iloc[i - 1]:
            ub = min(ub, upperband.iloc[i - 1])
        if close.iloc[i - 1] < lowerband.iloc[i - 1]:
            lb = max(lb, lowerband.iloc[i - 1])

        if prev_dir == -1:
            if close.iloc[i] > ub:
                direction.iloc[i] = 1
                st.iloc[i] = lb
            else:
                direction.iloc[i] = -1
                st.iloc[i] = ub
        else:
            if close.iloc[i] < lb:
                direction.iloc[i] = -1
                st.iloc[i] = ub
            else:
                direction.iloc[i] = 1
                st.iloc[i] = lb

    return st, direction


def ichimoku(high: pd.Series, low: pd.Series, close: pd.Series) -> Dict[str, pd.Series]:
    tenkan = (high.rolling(ICHIMOKU_TENKAN).max() + low.rolling(ICHIMOKU_TENKAN).min()) / 2.0
    kijun = (high.rolling(ICHIMOKU_KIJUN).max() + low.rolling(ICHIMOKU_KIJUN).min()) / 2.0
    senkou_a = ((tenkan + kijun) / 2.0).shift(ICHIMOKU_KIJUN)
    senkou_b = ((high.rolling(ICHIMOKU_SENKOU_B).max() + low.rolling(ICHIMOKU_SENKOU_B).min()) / 2.0).shift(
        ICHIMOKU_KIJUN
    )
    chikou = close.shift(-ICHIMOKU_KIJUN)
    return {
        "tenkan": tenkan,
        "kijun": kijun,
        "senkou_a": senkou_a,
        "senkou_b": senkou_b,
        "chikou": chikou,
    }


def psar(high: pd.Series, low: pd.Series, af_step: float = PSAR_AF_STEP, af_max: float = PSAR_AF_MAX) -> pd.Series:
    # Basic PSAR implementation
    psar = pd.Series(index=high.index, dtype=float)
    bull = True
    af = af_step
    ep = low.iloc[0]
    psar.iloc[0] = low.iloc[0]

    for i in range(1, len(high)):
        prev_psar = psar.iloc[i - 1]

        if bull:
            psar.iloc[i] = prev_psar + af * (ep - prev_psar)
            psar.iloc[i] = min(psar.iloc[i], low.iloc[i - 1], low.iloc[i])
            if high.iloc[i] > ep:
                ep = high.iloc[i]
                af = min(af + af_step, af_max)
            if low.iloc[i] < psar.iloc[i]:
                bull = False
                psar.iloc[i] = ep
                ep = low.iloc[i]
                af = af_step
        else:
            psar.iloc[i] = prev_psar + af * (ep - prev_psar)
            psar.iloc[i] = max(psar.iloc[i], high.iloc[i - 1], high.iloc[i])
            if low.iloc[i] < ep:
                ep = low.iloc[i]
                af = min(af + af_step, af_max)
            if high.iloc[i] > psar.iloc[i]:
                bull = True
                psar.iloc[i] = ep
                ep = high.iloc[i]
                af = af_step

    return psar


def trix(close: pd.Series, period: int = TRIX_PERIOD) -> pd.Series:
    e1 = ema(close, period)
    e2 = ema(e1, period)
    e3 = ema(e2, period)
    return 100 * e3.pct_change()


def ppo(close: pd.Series, fast: int = MACD_FAST, slow: int = MACD_SLOW) -> pd.Series:
    return 100 * (ema(close, fast) - ema(close, slow)) / ema(close, slow)


def tsi(close: pd.Series, r: int = TSI_R, s: int = TSI_S) -> pd.Series:
    m = close.diff()
    a = m.ewm(span=r, adjust=False).mean()
    b = a.ewm(span=s, adjust=False).mean()
    c = m.abs().ewm(span=r, adjust=False).mean()
    d = c.ewm(span=s, adjust=False).mean()
    return 100 * (b / d.replace(0, np.nan))


def ultimate_osc(high: pd.Series, low: pd.Series, close: pd.Series, p1: int, p2: int, p3: int) -> pd.Series:
    prev_close = close.shift(1)
    bp = close - pd.concat([low, prev_close], axis=1).min(axis=1)
    tr = pd.concat([high, prev_close], axis=1).max(axis=1) - pd.concat([low, prev_close], axis=1).min(axis=1)

    avg1 = bp.rolling(p1).sum() / tr.rolling(p1).sum()
    avg2 = bp.rolling(p2).sum() / tr.rolling(p2).sum()
    avg3 = bp.rolling(p3).sum() / tr.rolling(p3).sum()

    return 100 * (4 * avg1 + 2 * avg2 + avg3) / 7


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects df with columns: open, high, low, close, indexed by period_start_utc datetime (UTC).
    Returns df with added indicator columns + a 'indicators_json' column (dict serialized).
    """
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]

    # SMAs / EMAs
    for p in SMA_PERIODS:
        out[f"sma{p}"] = close.rolling(p).mean()
    for p in EMA_PERIODS:
        out[f"ema{p}"] = ema(close, p)

    out["wma20"] = wma(close, 20)

    # RSI
    out[f"rsi{RSI_PERIOD}"] = rsi(close, RSI_PERIOD)

    # Stochastic
    ll = low.rolling(STOCH_K).min()
    hh = high.rolling(STOCH_K).max()
    out["stoch_k"] = 100 * (close - ll) / (hh - ll).replace(0, np.nan)
    out["stoch_d"] = out["stoch_k"].rolling(STOCH_D).mean()

    # MACD
    macd_line = ema(close, MACD_FAST) - ema(close, MACD_SLOW)
    macd_sig = ema(macd_line, MACD_SIGNAL)
    macd_hist = macd_line - macd_sig
    out["macd"] = macd_line
    out["macd_signal"] = macd_sig
    out["macd_hist"] = macd_hist

    # Bollinger
    bb_mid = close.rolling(BB_PERIOD).mean()
    bb_std = close.rolling(BB_PERIOD).std(ddof=0)
    out["bb_mid"] = bb_mid
    out["bb_upper"] = bb_mid + BB_STD * bb_std
    out["bb_lower"] = bb_mid - BB_STD * bb_std
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]) / bb_mid.replace(0, np.nan)
    out["bb_percent_b"] = (close - out["bb_lower"]) / (out["bb_upper"] - out["bb_lower"]).replace(0, np.nan)

    # ATR
    out[f"atr{ATR_PERIOD}"] = atr(high, low, close, ATR_PERIOD)

    # ADX
    adx_s, plus_di, minus_di = adx(high, low, close, ADX_PERIOD)
    out[f"adx{ADX_PERIOD}"] = adx_s
    out["di_plus"] = plus_di
    out["di_minus"] = minus_di

    # CCI
    out[f"cci{CCI_PERIOD}"] = cci(high, low, close, CCI_PERIOD)

    # ROC & Momentum
    out[f"roc{ROC_PERIOD}"] = 100 * close.pct_change(ROC_PERIOD)
    out[f"mom{MOM_PERIOD}"] = close.diff(MOM_PERIOD)

    # Williams %R
    out[f"willr{WILLR_PERIOD}"] = williams_r(high, low, close, WILLR_PERIOD)

    # Donchian
    dc_u, dc_m, dc_l = donchian(high, low, DONCHIAN_PERIOD)
    out["donchian_upper"] = dc_u
    out["donchian_mid"] = dc_m
    out["donchian_lower"] = dc_l

    # Keltner
    kc_u, kc_m, kc_l = keltner(high, low, close)
    out["keltner_upper"] = kc_u
    out["keltner_mid"] = kc_m
    out["keltner_lower"] = kc_l

    # Supertrend
    st_line, st_dir = supertrend(high, low, close)
    out["supertrend"] = st_line
    out["supertrend_dir"] = st_dir

    # Ichimoku
    ich = ichimoku(high, low, close)
    out["ich_tenkan"] = ich["tenkan"]
    out["ich_kijun"] = ich["kijun"]
    out["ich_senkou_a"] = ich["senkou_a"]
    out["ich_senkou_b"] = ich["senkou_b"]
    out["ich_chikou"] = ich["chikou"]

    # PSAR
    out["psar"] = psar(high, low)

    # TRIX / PPO / TSI / Ultimate Osc
    out["trix"] = trix(close, TRIX_PERIOD)
    out["ppo"] = ppo(close, MACD_FAST, MACD_SLOW)
    out["tsi"] = tsi(close, TSI_R, TSI_S)
    out["ult_osc"] = ultimate_osc(high, low, close, *ULT_OSC)

    return out


def upsert_indicators(conn: sqlite3.Connection, currency: str) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM ohlc_indicators WHERE currency = ?", (currency,))
    conn.commit()

    for tf_label, _tf_freq in TIMEFRAMES:
        df = pd.read_sql_query(
            """
            SELECT period_start_utc, open, high, low, close
            FROM ohlc
            WHERE currency = ? AND timeframe = ?
            ORDER BY period_start_utc ASC
            """,
            conn,
            params=(currency, tf_label),
        )
        if df.empty:
            continue

        df["dt"] = pd.to_datetime(df["period_start_utc"], utc=True, errors="coerce")
        df = df.dropna(subset=["dt"]).set_index("dt").sort_index()
        df = df[["open", "high", "low", "close"]].astype(float)

        ind = compute_indicators(df)

        # Build JSON dict per row (all indicator columns besides OHLC)
        indicator_cols = [c for c in ind.columns if c not in ("open", "high", "low", "close")]
        created_at = utc_now_iso()

        rows = []
        for idx, row in ind.iterrows():
            period_start_utc = idx.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
            payload = {k: (None if pd.isna(row[k]) else float(row[k])) for k in indicator_cols}

            rows.append(
                (
                    currency,
                    tf_label,
                    period_start_utc,
                    None if pd.isna(row["close"]) else float(row["close"]),
                    # convenience fields (best-effort)
                    None if pd.isna(row.get("sma20", np.nan)) else float(row["sma20"]),
                    None if pd.isna(row.get("ema20", np.nan)) else float(row["ema20"]),
                    None if pd.isna(row.get(f"rsi{RSI_PERIOD}", np.nan)) else float(row[f"rsi{RSI_PERIOD}"]),
                    None if pd.isna(row.get("macd", np.nan)) else float(row["macd"]),
                    None if pd.isna(row.get("macd_signal", np.nan)) else float(row["macd_signal"]),
                    None if pd.isna(row.get("macd_hist", np.nan)) else float(row["macd_hist"]),
                    None if pd.isna(row.get("bb_mid", np.nan)) else float(row["bb_mid"]),
                    None if pd.isna(row.get("bb_upper", np.nan)) else float(row["bb_upper"]),
                    None if pd.isna(row.get("bb_lower", np.nan)) else float(row["bb_lower"]),
                    None if pd.isna(row.get(f"atr{ATR_PERIOD}", np.nan)) else float(row[f"atr{ATR_PERIOD}"]),
                    None if pd.isna(row.get(f"adx{ADX_PERIOD}", np.nan)) else float(row[f"adx{ADX_PERIOD}"]),
                    None if pd.isna(row.get("di_plus", np.nan)) else float(row["di_plus"]),
                    None if pd.isna(row.get("di_minus", np.nan)) else float(row["di_minus"]),
                    json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                    created_at,
                )
            )

        cur.executemany(
            """
            INSERT OR REPLACE INTO ohlc_indicators
            (currency, timeframe, period_start_utc,
             close, sma20, ema20, rsi14, macd, macd_signal, macd_hist,
             bb_mid, bb_upper, bb_lower,
             atr14, adx14, di_plus, di_minus,
             indicators_json, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def main() -> None:
    conn = open_writable_db(DB_PATH)
    try:
        ensure_tables(conn)

        # Full rebuild on start
        ticks = read_ticks(conn, CURRENCY)
        if ticks.empty:
            print(f"[WARN] No ticks found for currency={CURRENCY}. Nothing to rebuild.")
            return

        print(f"[INFO] Loaded ticks: {len(ticks):,} rows (currency={CURRENCY}).")
        print("[INFO] Rebuilding OHLC (full) ...")
        rebuild_ohlc(conn, ticks, CURRENCY)

        print("[INFO] Computing & storing indicators (full) ...")
        upsert_indicators(conn, CURRENCY)

        # Track last seen tick timestamp
        last_seen_ts = int(ticks["ts_ms"].max())

        print(f"[OK] Rebuild completed. Entering continuous mode (sleep={SLEEP_SECONDS}s). Press Ctrl+C to stop.")
    finally:
        conn.close()

    # Continuous loop uses fresh connections to avoid long-lived locks
    while True:
        try:
            time.sleep(SLEEP_SECONDS)

            conn2 = open_writable_db(DB_PATH)
            try:
                ensure_tables(conn2)

                cur = conn2.cursor()
                cur.execute(
                    "SELECT MAX(source_ts_ms) FROM ticks WHERE currency = ?",
                    (CURRENCY,),
                )
                row = cur.fetchone()
                max_ts = row[0]
                if max_ts is None:
                    continue
                max_ts = int(max_ts)

                if max_ts <= last_seen_ts:
                    continue

                # Compute rolling window start
                window_ms = int(ROLLING_WINDOW_HOURS * 3600 * 1000)
                cutoff_ms = max_ts - window_ms

                ticks_w = read_ticks(conn2, CURRENCY, since_ts_ms=cutoff_ms)
                if ticks_w.empty:
                    last_seen_ts = max_ts
                    continue

                window_start_dt = ticks_w.index.min()
                window_start_utc = window_start_dt.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

                print(f"[INFO] New ticks detected. Rebuilding window from {window_start_utc} (rows={len(ticks_w):,}) ...")

                rebuild_ohlc_window(conn2, ticks_w, CURRENCY, window_start_utc)
                upsert_indicators_window(conn2, CURRENCY, window_start_utc)

                last_seen_ts = max_ts
                print("[OK] Window update done.")
            finally:
                conn2.close()

        except KeyboardInterrupt:
            print("[INFO] Stopped by user.")
            break
        except Exception as e:
            # keep running on transient failures
            print(f"[ERROR] {e}")
            continue


if __name__ == "__main__":
    main()
