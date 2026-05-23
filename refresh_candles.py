"""
refresh_candles.py — FTMO candle backfill
Pulls OHLCV + spread from FTMO MT5 terminal via MetaTrader5 Python API.
Upserts into ftmo_candles on Railway Postgres.
Run manually before each FTMO sweep.
No scheduling. No automation. Run when needed.

Usage:
    python refresh_candles.py
"""

import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import MetaTrader5 as mt5
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
MT5_LOGIN    = int(os.getenv("MT5_LOGIN", "0"))
MT5_SERVER   = os.getenv("MT5_SERVER", "FTMO-Demo")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

# MT5 format (no underscore)
INSTRUMENTS = [
    "AUDUSD", "NZDUSD", "USDCHF", "USDJPY", "USDCAD", "EURUSD", "GBPUSD",
    "AUDJPY", "CADJPY", "EURJPY", "GBPJPY", "NZDJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURNZD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD",
    "AUDCAD", "AUDNZD", "NZDCAD", "CADCHF",
]

# Granularities to backfill
GRANULARITIES = {
    "M5":  mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1":  mt5.TIMEFRAME_H1,
}

# MT5 symbol → DB instrument format
MT5_TO_DB = {
    "AUDUSD": "AUD_USD", "NZDUSD": "NZD_USD", "USDCHF": "USD_CHF",
    "USDJPY": "USD_JPY", "USDCAD": "USD_CAD", "EURUSD": "EUR_USD",
    "GBPUSD": "GBP_USD", "AUDJPY": "AUD_JPY", "CADJPY": "CAD_JPY",
    "EURJPY": "EUR_JPY", "GBPJPY": "GBP_JPY", "NZDJPY": "NZD_JPY",
    "CHFJPY": "CHF_JPY", "EURAUD": "EUR_AUD", "EURCAD": "EUR_CAD",
    "EURCHF": "EUR_CHF", "EURGBP": "EUR_GBP", "EURNZD": "EUR_NZD",
    "GBPAUD": "GBP_AUD", "GBPCAD": "GBP_CAD", "GBPCHF": "GBP_CHF",
    "GBPNZD": "GBP_NZD", "AUDCAD": "AUD_CAD", "AUDNZD": "AUD_NZD",
    "NZDCAD": "NZD_CAD", "CADCHF": "CAD_CHF",
}

# How many bars to request per granularity
# MT5 terminal cache depth — request generously, broker caps it
BARS_TO_FETCH = 99_999

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refresh_candles")


# ── MT5 CONNECTION ────────────────────────────────────────────────────────────
def mt5_connect():
    log.info("Connecting to MT5 terminal...")
    ok = mt5.initialize(
        login=MT5_LOGIN,
        server=MT5_SERVER,
        password=MT5_PASSWORD
    )
    if not ok:
        raise RuntimeError(f"mt5.initialize() failed: {mt5.last_error()}")
    acc = mt5.account_info()
    log.info(
        f"MT5 connected | login={acc.login} | "
        f"balance={acc.balance} {acc.currency} | server={acc.server}"
    )


# ── FETCH FROM MT5 ────────────────────────────────────────────────────────────
def fetch_bars(symbol, timeframe_const, granularity_label):
    """
    Pull up to BARS_TO_FETCH bars from MT5 terminal.
    Returns list of tuples: (instrument_db, granularity, time, o, h, l, c, volume, spread_points)
    """
    rates = mt5.copy_rates_from_pos(symbol, timeframe_const, 0, BARS_TO_FETCH)
    if rates is None or len(rates) == 0:
        log.warning(f"  No data returned for {symbol} {granularity_label}")
        return []

    db_instrument = MT5_TO_DB[symbol]
    rows = []
    for r in rates:
        ts = datetime.fromtimestamp(r["time"], tz=timezone.utc)
        rows.append((
            db_instrument,
            granularity_label,
            ts,
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            int(r["tick_volume"]),
            int(r["spread"]),      # real measured spread in points
        ))

    log.info(
        f"  {symbol} {granularity_label}: "
        f"{len(rows):,} bars | "
        f"{rows[0][2].date()} -> {rows[-1][2].date()}"
    )
    return rows


# ── UPSERT TO POSTGRES ────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO ftmo_candles
    (instrument, granularity, time, open, high, low, close, volume, spread_points)
VALUES %s
ON CONFLICT (instrument, granularity, time)
DO UPDATE SET
    open          = EXCLUDED.open,
    high          = EXCLUDED.high,
    low           = EXCLUDED.low,
    close         = EXCLUDED.close,
    volume        = EXCLUDED.volume,
    spread_points = EXCLUDED.spread_points
"""

def upsert_bars(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


def already_loaded(conn, instrument, granularity, min_rows=90000):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ftmo_candles WHERE instrument = %s AND granularity = %s",
            (instrument, granularity)
        )
        count = cur.fetchone()[0]
    return count >= min_rows


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    log.info("refresh_candles starting")
    log.info(f"Instruments: {len(INSTRUMENTS)} | Granularities: {list(GRANULARITIES.keys())}")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set — add to .env or set in shell")

    mt5_connect()

    conn = psycopg2.connect(DATABASE_URL)
    log.info("Postgres connected")

    total_rows = 0

    for symbol in INSTRUMENTS:
        log.info(f"\n{'='*50}")
        log.info(f"Processing {symbol}")
        log.info(f"{'='*50}")

        # Ensure symbol is in Market Watch
        if not mt5.symbol_select(symbol, True):
            log.warning(f"  Could not select {symbol} — skipping")
            continue

        for gran_label, gran_const in GRANULARITIES.items():
            if already_loaded(conn, MT5_TO_DB[symbol], gran_label):
                log.info(f"  {symbol} {gran_label}: already loaded — skipping")
                continue
            rows = fetch_bars(symbol, gran_const, gran_label)
            n = upsert_bars(conn, rows)
            total_rows += n
            log.info(f"  Upserted {n:,} rows")
            time.sleep(0.1)   # polite pause between MT5 calls

    conn.close()
    mt5.shutdown()

    elapsed = time.time() - start
    log.info(f"\nDone — {total_rows:,} total rows upserted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
