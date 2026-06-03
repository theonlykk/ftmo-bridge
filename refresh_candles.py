"""
refresh_candles.py — FTMO candle gap-fill pipeline (ADR-092)
Pulls OHLCV + spread from FTMO MT5 terminal via MetaTrader5 Python API.
Upserts into ftmo_candles on Railway Postgres.
D1 bars normalized to NY close timestamp (21:00/22:00 UTC, DST-aware).
OANDA REST fallback for D1 gaps wider than 3 calendar days.
Run manually or via Windows Task Scheduler (daily 00:05 UTC).

Usage:
    python refresh_candles.py
"""

import os
import time
import logging
import requests
import pytz
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dt_time
from dotenv import load_dotenv

import MetaTrader5 as mt5
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
MT5_LOGIN      = int(os.getenv("MT5_LOGIN", "0"))
MT5_SERVER     = os.getenv("MT5_SERVER", "FTMO-Demo")
MT5_PASSWORD   = os.getenv("MT5_PASSWORD", "")
DATABASE_URL   = os.getenv("DATABASE_URL", "")
OANDA_API_KEY  = os.getenv("OANDA_API_KEY", "")
OANDA_BASE_URL = "https://api-fxtrade.oanda.com/v3"

# MT5 format (no underscore) — all 26 G10 pairs
INSTRUMENTS = [
    "AUDUSD", "NZDUSD", "USDCHF", "USDJPY", "USDCAD", "EURUSD", "GBPUSD",
    "AUDJPY", "CADJPY", "EURJPY", "GBPJPY", "NZDJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURNZD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD",
    "AUDCAD", "AUDNZD", "NZDCAD", "CADCHF",
]

# Fetch priority order — M30 and D1 first, heaviest last
GRANULARITIES = {
    "M30": mt5.TIMEFRAME_M30,
    "D1":  mt5.TIMEFRAME_D1,
    "H1":  mt5.TIMEFRAME_H1,
    "H4":  mt5.TIMEFRAME_H4,
    "M5":  mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
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

# DB instrument → OANDA symbol (for fallback)
DB_TO_OANDA = {v: k for k, v in MT5_TO_DB.items()}

# Default historical start when table is empty for this instrument/granularity
DEFAULT_START = datetime(2022, 1, 1, tzinfo=timezone.utc)

# D1 gap tolerance — gaps wider than this trigger OANDA REST fallback
MAX_D1_GAP_DAYS = 3

# ── LOGGING ───────────────────────────────────────────────────────────────────
LOG_DIR = Path("C:/ftmo-bridge/logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refresh_candles")

log_file = LOG_DIR / f"refresh_candles_{datetime.now().strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
log.addHandler(file_handler)


# ── TIMESTAMP NORMALIZATION ───────────────────────────────────────────────────
def normalize_d1_timestamp(utc_midnight: datetime) -> datetime:
    """
    Convert FTMO D1 00:00 UTC stamp to NY close of the prior FX session.
    FX NY close = 17:00 ET = 21:00 UTC (EDT/summer) or 22:00 UTC (EST/winter).

    Examples:
        2026-06-03 00:00 UTC (summer) → 2026-06-02 21:00 UTC
        2026-01-15 00:00 UTC (winter) → 2026-01-14 22:00 UTC
    """
    et = pytz.timezone("America/New_York")
    bar_date = utc_midnight.date()
    ny_close_naive = datetime.combine(bar_date, dt_time(hour=17))
    ny_close_et = et.localize(ny_close_naive)
    ny_close_utc = ny_close_et.astimezone(timezone.utc) - timedelta(days=1)
    return ny_close_utc


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


# ── GAP DETECTION ─────────────────────────────────────────────────────────────
def get_last_timestamp(conn, instrument, granularity):
    """
    Returns MAX(time) for this instrument/granularity from ftmo_candles.
    Returns None if no rows exist yet for this combo.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(time) FROM ftmo_candles
            WHERE instrument = %s AND granularity = %s
        """, (instrument, granularity))
        result = cur.fetchone()[0]
    return result


# ── MT5 FETCH ─────────────────────────────────────────────────────────────────
def fetch_bars_mt5(symbol, timeframe_const, granularity_label, from_dt):
    """
    Fetch bars from from_dt to now via copy_rates_range.
    D1 bars are timestamp-normalized to NY close (DST-aware).
    Returns list of tuples ready for upsert.
    """
    end_dt = datetime.now(tz=timezone.utc)
    rates = mt5.copy_rates_range(symbol, timeframe_const, from_dt, end_dt)

    if rates is None or len(rates) == 0:
        log.warning(f"  {symbol} {granularity_label}: no bars returned from MT5")
        return []

    db_instrument = MT5_TO_DB[symbol]
    rows = []
    for r in rates:
        ts = datetime.fromtimestamp(r["time"], tz=timezone.utc)
        if granularity_label == "D1":
            ts = normalize_d1_timestamp(ts)
        rows.append((
            db_instrument,
            granularity_label,
            ts,
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            int(r["tick_volume"]),
            int(r["spread"]),
        ))

    log.info(
        f"  {symbol} {granularity_label}: "
        f"{len(rows):,} bars | "
        f"{rows[0][2].date()} -> {rows[-1][2].date()}"
    )
    return rows


# ── OANDA REST FALLBACK ───────────────────────────────────────────────────────
def fetch_d1_oanda_fallback(instrument_db, from_dt, to_dt):
    """
    Fetch D1 bars from OANDA REST for a specific gap window.
    Used when MT5 D1 gap exceeds MAX_D1_GAP_DAYS during workweek.
    Returns rows normalized to 21:00 UTC NY close convention.
    """
    if not OANDA_API_KEY:
        log.error(f"  OANDA fallback: OANDA_API_KEY not set — cannot fill gap")
        return []

    oanda_symbol = DB_TO_OANDA.get(instrument_db)
    if not oanda_symbol:
        log.error(f"  OANDA fallback: no OANDA symbol for {instrument_db}")
        return []

    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    params = {
        "granularity": "D",
        "from": from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to":   to_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price": "M",
    }
    url = f"{OANDA_BASE_URL}/instruments/{oanda_symbol}/candles"

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        candles = resp.json().get("candles", [])
    except Exception as e:
        log.error(f"  OANDA fallback failed for {instrument_db}: {e}")
        return []

    rows = []
    for c in candles:
        if not c.get("complete"):
            continue
        ts = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))
        rows.append((
            instrument_db,
            "D1",
            ts,
            float(c["mid"]["o"]),
            float(c["mid"]["h"]),
            float(c["mid"]["l"]),
            float(c["mid"]["c"]),
            None,
            None,
        ))

    log.info(f"  OANDA fallback {instrument_db} D1: {len(rows)} bars filled")
    return rows


def check_d1_gaps(rows, instrument_db):
    """
    Scan D1 rows for gaps wider than MAX_D1_GAP_DAYS on workdays.
    Returns list of (gap_start, gap_end) tuples requiring OANDA fallback.
    """
    if len(rows) < 2:
        return []

    gaps = []
    timestamps = sorted([r[2] for r in rows])
    for i in range(1, len(timestamps)):
        delta = (timestamps[i] - timestamps[i-1]).days
        if delta > MAX_D1_GAP_DAYS:
            gaps.append((timestamps[i-1], timestamps[i]))
            log.warning(
                f"  D1 gap detected for {instrument_db}: "
                f"{timestamps[i-1].date()} → {timestamps[i].date()} "
                f"({delta} days)"
            )
    return gaps


# ── UPSERT ────────────────────────────────────────────────────────────────────
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
    volume        = COALESCE(EXCLUDED.volume, ftmo_candles.volume),
    spread_points = COALESCE(EXCLUDED.spread_points, ftmo_candles.spread_points)
"""

def upsert_bars(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    log.info("refresh_candles starting (ADR-092)")
    log.info(f"Instruments: {len(INSTRUMENTS)} | Granularities: {list(GRANULARITIES.keys())}")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set — add to .env")

    mt5_connect()
    conn = psycopg2.connect(DATABASE_URL)
    log.info("Postgres connected")

    total_rows = 0

    for symbol in INSTRUMENTS:
        log.info(f"\n{'='*50}")
        log.info(f"Processing {symbol}")
        log.info(f"{'='*50}")

        if not mt5.symbol_select(symbol, True):
            log.warning(f"  Could not select {symbol} — skipping")
            continue

        db_instrument = MT5_TO_DB[symbol]

        for gran_label, gran_const in GRANULARITIES.items():
            try:
                last_ts = get_last_timestamp(conn, db_instrument, gran_label)
                from_dt = last_ts if last_ts is not None else DEFAULT_START
                log.info(f"  {symbol} {gran_label}: fetching from {from_dt.date()}")

                rows = fetch_bars_mt5(symbol, gran_const, gran_label, from_dt)

                # D1 gap detection + OANDA fallback
                if gran_label == "D1" and rows:
                    gaps = check_d1_gaps(rows, db_instrument)
                    for gap_start, gap_end in gaps:
                        fallback_rows = fetch_d1_oanda_fallback(
                            db_instrument, gap_start, gap_end
                        )
                        rows.extend(fallback_rows)

                n = upsert_bars(conn, rows)
                total_rows += n
                log.info(f"  Upserted {n:,} rows")
                time.sleep(0.1)

            except Exception as e:
                log.error(f"  FAILED {symbol} {gran_label}: {e}")
                with open(log_file, "a") as f:
                    f.write(
                        f"{datetime.now(tz=timezone.utc).isoformat()} | "
                        f"FAIL | {symbol} | {gran_label} | {e}\n"
                    )
                continue

    conn.close()
    mt5.shutdown()

    elapsed = time.time() - start
    log.info(f"\nDone — {total_rows:,} total rows upserted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
