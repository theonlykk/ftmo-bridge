# ADR-075: FTMO Orphan Trade Resolution via ZMQ admin_sweep

## Date
2026-05-27

## Status
Implemented

## Context
FTMO trades can reach `status='CLOSED'` with `result IS NULL` when the
reconciler detects the MT5 position is gone but `recent_deals` does not yet
contain the closing deal (ADR-060). These orphans block accurate P&L reporting
and performance guard calculations indefinitely if the deal never appears in
the heartbeat window.

Manual Jupyter patches were used as a workaround. A durable, automated path
is required that queries MT5 deal history directly for known position tickets.

## Decision
Introduce a new ZMQ message family `admin_sweep` — separate from order
execution and from the `account_state` heartbeat — with two actions:

| Direction | topic | action | Purpose |
|-----------|-------|--------|---------|
| Railway → VPS | `admin_sweep` | `request_closing_deals` | List of MT5 position tickets |
| VPS → Railway | `admin_sweep` | `response_closing_deals` | Closing deal price + profit per ticket |

### VPS bridge (`subscriber.py`)
- Intercept `admin_sweep` messages in the downstream SUB loop **before** order
  routing (`close_position`, `validate`, `execute_order`).
- For each ticket, call `mt5.history_deals_get(position=ticket)` and extract
  the first `DEAL_ENTRY_OUT` deal.
- Publish response upstream via `pub_sock.send_json()`.
- Tickets with no closing deal are omitted — **no fabricated prices**.

### Railway executor (`strategy_executor.py`)
- `_resolve_orphaned_closes()` runs every **900 seconds** from the main poll
  loop (outside the entry dead zone gate).
- Queries `trades` where `status='CLOSED' AND result IS NULL AND source='ftmo'`.
- Fire-and-forget `admin_sweep` request via `_zmq_pub.send_json()`.
- `_zmq_upstream_loop` handles `response_closing_deals` **before** `msg_type`
  dispatch and writes `exit_price`, `oanda_pl`, `result`, `closed_at=NOW()`.
- UPDATE guarded by `AND result IS NULL` — idempotent, no double-resolution.

## Key Rulings (Gemini)
- Do **not** modify `_zmq_reconcile_ftmo_trades()`, `_ftmo_timeout_check()`, or
  `_reconcile_closes()` — orphan sweep is a parallel recovery path.
- Do **not** fabricate exit prices — if MT5 has no closing deal, DB row stays
  unchanged until the next 900s cycle.
- Use `NOW()` for `closed_at` (DB server time), not local clock.
- All failures are non-fatal; orphans persist and retry automatically.

## Failure Modes
| Failure | Behaviour |
|---------|-----------|
| DB connect in sweep | log warning, skip cycle |
| ZMQ send fails | log warning, retry next cycle |
| MT5 history query per ticket | log error, skip ticket |
| Incomplete deal in response | log warning, skip deal |
| DB write in response handler | log error, rollback, no partial commit |

## Consequences
- Orphaned CLOSED trades self-heal without manual intervention
- No schema change required
- No blocking/wait in the executor poll loop
- Bridge admin path bypasses stale-price validation (by design)

## Files Changed
- `D:\FTMO\subscriber.py` — downstream `admin_sweep` interceptor
- `D:\oanda-trading\strategy_executor.py` — sweep, upstream handler, 900s timer
