#!/usr/bin/env bash
# Start Gold bot v2 (sweep & reclaim)
# Usage: ./start_gold_bot_v2.sh
set -euo pipefail

DB="${DB:-goldprice.sqlite}"
TF="${TF:-1m}"
CURRENCIES="${CURRENCIES:-USD}"   # in jouw DB zitten USD/EUR; kies wat je gebruikt
INTERVAL="${INTERVAL:-20}"        # seconds
LOOKBACK_DAYS="${LOOKBACK_DAYS:-30}"

python3 gold_bot_v2.py \
  --db "$DB" \
  --currencies "$CURRENCIES" \
  --timeframe "$TF" \
  --lookback-days "$LOOKBACK_DAYS" \
  --interval "$INTERVAL" \
  --min-bars 120 \
  --sweep-lookback 5 \
  --swing-lookback 5 \
  --sl-atr-buffer 0.5 \
  --rr-tp1 1.0 \
  --rr-tp2 2.0 \
  --trend-filter \
  --prowl
