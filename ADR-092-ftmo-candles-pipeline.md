# ADR-092 — FTMO Candle Gap-Fill Pipeline

## Problem Statement

`refresh_candles.py` previously used a blunt row-count heuristic (`already_loaded` with `min_rows=90000`) to decide whether to skip fetches. That approach could miss recent bars, ignore true gaps, and provided no visibility into failures.

FTMO sweep and analysis depend on `ftmo_candles` in Railway Postgres. The table did not exist until today's DDL; historical candle data had been maintained manually via OANDA-oriented notebook backfills into `oanda_candles`. FTMO-specific ingestion needed:

- Incremental gap-fill from the last stored bar per instrument/granularity
- D1 bars aligned to FX NY close (21:00/22:00 UTC, DST-aware), not raw MT5 midnight UTC
- Automatic OANDA REST fallback when MT5 D1 history has gaps wider than three calendar days
- Structured logging and per-job failure records for unattended nightly runs

## Architecture Decisions

### Gap-fill via `MAX(time)` + `copy_rates_range()`

- `get_last_timestamp()` reads `MAX(time)` from `ftmo_candles` for each `(instrument, granularity)`.
- If no rows exist, fetch starts at `DEFAULT_START` (`2022-01-01` UTC).
- All MT5 pulls use `mt5.copy_rates_range(symbol, timeframe, from_dt, end_dt)` — never `copy_rates_from_pos()`.

### D1 timestamp normalization (DST-aware)

- `normalize_d1_timestamp()` uses `pytz` (`America/New_York`) to map FTMO D1 00:00 UTC stamps to the prior session's NY close (17:00 ET → 21:00 or 22:00 UTC).
- Applied only when `gran_label == "D1"` inside `fetch_bars_mt5()`.

### OANDA REST fallback for wide D1 gaps

- After MT5 D1 fetch, `check_d1_gaps()` scans sorted bar timestamps; gaps wider than `MAX_D1_GAP_DAYS` (3) trigger `fetch_d1_oanda_fallback()`.
- Fallback uses `OANDA_API_KEY` from `.env`, `DB_TO_OANDA` symbol mapping, and OANDA trade API (`api-fxtrade.oanda.com/v3`).
- Fallback rows may have `volume` and `spread_points` as `None`; upsert uses `COALESCE` to preserve existing MT5 values on conflict.

### Upsert target and granularity order

- Upsert targets `ftmo_candles` only (not `oanda_candles`).
- Fetch order: M30, D1, H1, H4, M5, M15 (M30 and D1 first).

### Logging

- Console + daily file under `C:/ftmo-bridge/logs/refresh_candles_YYYYMMDD.log`.
- Per `(symbol, granularity)` failures append a `FAIL |` line to the same log file; processing continues.

### Scheduling (out of scope for this change)

- Script docstring notes manual run or Windows Task Scheduler (e.g. daily 00:05 UTC). Task Scheduler setup is a manual operator step.

## Negative Space

- No changes under `D:\candlelab\`
- No changes under `D:\oanda-trading\`
- No reads or writes to `oanda_candles`
- No new database tables or DDL (assumes `ftmo_candles` already exists)
- No Task Scheduler configuration in repo
- No modification to notebooks (`Untitled1.ipynb`, etc.)

## Verification Steps

1. **Environment** — `.env` contains `DATABASE_URL`, `MT5_LOGIN`, `MT5_SERVER`, `MT5_PASSWORD`, and `OANDA_API_KEY` (for D1 fallback). No credentials in source.
2. **Dependencies** — `MetaTrader5`, `psycopg2`, `python-dotenv`, `requests`, `pytz` available in the runtime venv.
3. **Static review**
   - `copy_rates_range` present; `copy_rates_from_pos` absent
   - `normalize_d1_timestamp` only on D1 path
   - `DEFAULT_START = datetime(2022, 1, 1, tzinfo=timezone.utc)`
   - `UPSERT_SQL` references `ftmo_candles` with `COALESCE` on `volume` / `spread_points`
4. **Dry run** — With MT5 terminal logged in: `python refresh_candles.py`. Confirm log shows per-symbol fetch-from dates and upsert counts.
5. **Gap-fill** — Pick an instrument/granularity with existing rows; re-run and confirm fetch starts at last `time`, not `2022-01-01`.
6. **D1 normalization** — Query `ftmo_candles` for `granularity = 'D1'`; timestamps should cluster at 21:00 or 22:00 UTC (season-dependent), not 00:00 UTC.
7. **D1 fallback** — If a >3-day D1 gap exists in MT5 data, confirm warning log and OANDA fallback bar count; verify fallback rows do not wipe `volume`/`spread_points` on existing keys.
8. **Failure path** — Simulate failure (e.g. invalid symbol temporarily); confirm `FAIL |` line in `C:/ftmo-bridge/logs/` and script continues other jobs.
