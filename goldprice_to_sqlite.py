#!/usr/bin/env python3
import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import requests


DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS ticks (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc    TEXT    NOT NULL,
  source_ts_ms      INTEGER,
  source_date_text  TEXT,
  currency          TEXT    NOT NULL,

  xau_price         REAL,
  chg_xau           REAL,
  pc_xau            REAL,
  xau_close         REAL,

  raw_json          TEXT    NOT NULL,

  UNIQUE(currency, source_ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_ticks_fetched_at ON ticks(fetched_at_utc);
CREATE INDEX IF NOT EXISTS idx_ticks_currency   ON ticks(currency);

-- OHLC per timeframe & currency (alleen goud / XAU)
CREATE TABLE IF NOT EXISTS ohlc (
  currency          TEXT NOT NULL,
  timeframe         TEXT NOT NULL,              -- '1m','5m','15m','1h','1d'
  period_start_utc  TEXT NOT NULL,              -- bucket start in UTC (ISO)

  open_ts_ms        INTEGER NOT NULL,
  close_ts_ms       INTEGER NOT NULL,

  open             REAL,
  high             REAL,
  low              REAL,
  close            REAL,

  PRIMARY KEY (currency, timeframe, period_start_utc)
);

CREATE INDEX IF NOT EXISTS idx_ohlc_tf_start ON ohlc(timeframe, period_start_utc);
CREATE INDEX IF NOT EXISTS idx_ohlc_curr     ON ohlc(currency);
"""


TIMEFRAMES_S = {
    "1m": 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ms_to_utc_iso_bucket_start(ts_ms: int, bucket_s: int) -> str:
    ts_s = ts_ms // 1000
    start_s = (ts_s // bucket_s) * bucket_s
    dt = datetime.fromtimestamp(start_s, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


def build_url(currency: str) -> str:
    return f"https://data-asg.goldprice.org/dbXRates/{currency}"


def make_cache_buster_id() -> str:
    ms = int(time.time() * 1000)
    return f"{ms}-{time.monotonic_ns()}"


def get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 15) -> Dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; goldprice-to-sqlite/1.0; +https://example.invalid)",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(url, headers=headers, params=params, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def extract_item(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return None
    item = items[0]
    return item if isinstance(item, dict) else None


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def currency_list(csv: str) -> Iterable[str]:
    for c in csv.split(","):
        c = c.strip().upper()
        if c:
            yield c


def last_saved_price(conn: sqlite3.Connection, currency: str) -> Optional[float]:
    row = conn.execute(
        """
        SELECT xau_price
        FROM ticks
        WHERE currency = ?
          AND xau_price IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (currency,),
    ).fetchone()
    return None if row is None else row[0]


def is_new_price(conn: sqlite3.Connection, currency: str, price: Optional[float]) -> bool:
    # Als de API geen prijs geeft: niet opslaan (anders spam je nulls).
    if price is None:
        return False

    prev = last_saved_price(conn, currency)
    # Als er nog niets is opgeslagen, is dit "nieuw".
    if prev is None:
        return True

    # Float-vergelijking; eventueel kan je hier een tolerantie toepassen.
    return price != prev


def insert_tick(conn: sqlite3.Connection, currency: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    item = extract_item(payload)
    if item is None:
        raise ValueError("Onverwacht JSON-formaat: geen items[0] gevonden")

    row = {
        "fetched_at_utc": now_utc_iso(),
        "source_ts_ms": payload.get("ts"),
        "source_date_text": payload.get("date"),
        "currency": item.get("curr") or currency,

        "xau_price": item.get("xauPrice"),
        "chg_xau": item.get("chgXau"),
        "pc_xau": item.get("pcXau"),
        "xau_close": item.get("xauClose"),

        "raw_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    }

    sql = """
    INSERT OR IGNORE INTO ticks (
      fetched_at_utc, source_ts_ms, source_date_text, currency,
      xau_price, chg_xau, pc_xau, xau_close,
      raw_json
    ) VALUES (
      :fetched_at_utc, :source_ts_ms, :source_date_text, :currency,
      :xau_price, :chg_xau, :pc_xau, :xau_close,
      :raw_json
    );
    """
    conn.execute(sql, row)
    conn.commit()
    return row


def upsert_ohlc(
    conn: sqlite3.Connection,
    currency: str,
    timeframe: str,
    period_start_utc: str,
    ts_ms: int,
    price: Optional[float],
) -> None:
    sql = """
    INSERT INTO ohlc (
      currency, timeframe, period_start_utc,
      open_ts_ms, close_ts_ms,
      open, high, low, close
    ) VALUES (
      :currency, :timeframe, :period_start_utc,
      :ts_ms, :ts_ms,
      :p, :p, :p, :p
    )
    ON CONFLICT(currency, timeframe, period_start_utc) DO UPDATE SET
      high = CASE
        WHEN ohlc.high IS NULL THEN excluded.high
        WHEN excluded.high IS NULL THEN ohlc.high
        ELSE max(ohlc.high, excluded.high)
      END,
      low = CASE
        WHEN ohlc.low IS NULL THEN excluded.low
        WHEN excluded.low IS NULL THEN ohlc.low
        ELSE min(ohlc.low, excluded.low)
      END,

      open_ts_ms = min(ohlc.open_ts_ms, excluded.open_ts_ms),
      open = CASE
        WHEN excluded.open_ts_ms < ohlc.open_ts_ms THEN excluded.open
        ELSE ohlc.open
      END,

      close_ts_ms = max(ohlc.close_ts_ms, excluded.close_ts_ms),
      close = CASE
        WHEN excluded.close_ts_ms > ohlc.close_ts_ms THEN excluded.close
        ELSE ohlc.close
      END
    ;
    """
    conn.execute(
        sql,
        {
            "currency": currency,
            "timeframe": timeframe,
            "period_start_utc": period_start_utc,
            "ts_ms": ts_ms,
            "p": price,
        },
    )


def update_all_ohlc(conn: sqlite3.Connection, currency: str, ts_ms: int, price: Optional[float]) -> None:
    for tf, seconds in TIMEFRAMES_S.items():
        start_iso = ms_to_utc_iso_bucket_start(ts_ms, seconds)
        upsert_ohlc(conn, currency, tf, start_iso, ts_ms, price)
    conn.commit()


def main() -> None:
    p = argparse.ArgumentParser(description="Sla live goudprijs op + OHLC (1m/5m/15m/1h/1d) in SQLite (alleen bij nieuwe prijs).")
    p.add_argument("--db", default="goldprice.sqlite", help="Pad naar SQLite databasebestand")
    p.add_argument("--currencies", default="EUR", help="Komma-gescheiden lijst, bijv. EUR,USD,GBP")
    p.add_argument("--interval", type=int, default=20, help="Polling-interval in seconden (advies: 10-60)")
    p.add_argument("--once", action="store_true", help="Voer 1 fetch uit en stop")
    p.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconden")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        init_db(conn)

        while True:
            for cur in currency_list(args.currencies):
                url = build_url(cur)
                cb_id = make_cache_buster_id()
                params = {"id": cb_id}

                try:
                    payload = get_json(url, params=params, timeout_s=args.timeout)

                    item = extract_item(payload)
                    if item is None:
                        raise ValueError("Onverwacht JSON-formaat: geen items[0] gevonden")

                    price = item.get("xauPrice")

                    # Alleen opslaan als er een nieuwe prijs binnenkomt
                    if not is_new_price(conn, cur, price):
                        print(f"[{now_utc_iso()}] SKIP {cur} gold(xauPrice)={price} (no new price, id={cb_id})")
                        continue

                    row = insert_tick(conn, cur, payload)

                    ts_ms = payload.get("ts")
                    if not isinstance(ts_ms, int):
                        ts_ms = int(time.time() * 1000)

                    update_all_ohlc(conn, cur, ts_ms, row.get("xau_price"))

                    print(f"[{now_utc_iso()}] SAVE {cur} gold(xauPrice)={row.get('xau_price')} (ts={ts_ms}, id={cb_id})")

                except Exception as e:
                    print(f"[{now_utc_iso()}] ERROR {cur}: {e}")

            if args.once:
                break
            time.sleep(max(1, args.interval))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
