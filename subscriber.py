"""
subscriber.py — FTMO MT5 Bridge
Receives order payloads from Railway via ZMQ SUB (downstream).
Fires orders via MetaTrader5 Python API.
Publishes trade_confirm / trade_reject / account_state via ZMQ PUB (upstream).
Zero candlelab-core dependencies. Dumb executor.
"""

import base64
import os
import json
import time
import logging
import threading
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

import zmq
import MetaTrader5 as mt5

load_dotenv()

# ── CONFIG FROM ENV ───────────────────────────────────────────────────────────
MT5_LOGIN            = int(os.getenv("MT5_LOGIN", "0"))
MT5_SERVER           = os.getenv("MT5_SERVER", "FTMO-Demo")
MT5_PASSWORD         = os.getenv("MT5_PASSWORD", "")

BRIDGE_PUBLIC_KEY    = os.getenv("BRIDGE_ZMQ_PUBLIC_KEY", "")
BRIDGE_SECRET_KEY    = os.getenv("BRIDGE_ZMQ_SECRET_KEY", "")
RAILWAY_PUBLIC_KEY   = os.getenv("RAILWAY_ZMQ_PUBLIC_KEY", "")

DOWNSTREAM_PORT      = int(os.getenv("ZMQ_DOWNSTREAM_PORT", "5555"))
UPSTREAM_PORT        = int(os.getenv("ZMQ_UPSTREAM_PORT", "5556"))
RAILWAY_HOST         = os.getenv("RAILWAY_HOST", "127.0.0.1")

STALE_THRESHOLD_PIPS = float(os.getenv("STALE_THRESHOLD_PIPS", "5.0"))
ACCOUNT_STATE_INTERVAL = int(os.getenv("ACCOUNT_STATE_INTERVAL", "60"))
HEARTBEAT_TIMEOUT    = int(os.getenv("HEARTBEAT_TIMEOUT", "120"))

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
handler = RotatingFileHandler(
    "logs/bridge.log", maxBytes=5_000_000, backupCount=3
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[handler, logging.StreamHandler()]
)
log = logging.getLogger("bridge")

# ── GLOBALS ───────────────────────────────────────────────────────────────────
upstream_pub = None   # set after socket init, used by account state thread


# ── ZMQ SETUP ─────────────────────────────────────────────────────────────────
def build_sockets(ctx):
    """
    Build and return (sub_sock, pub_sock).
    Downstream: Railway PUB → Bridge SUB (receives order payloads)
    Upstream:   Bridge PUB → Railway SUB (sends confirms/rejects/account state)
    CurveZMQ on both channels.
    """
    # --- Downstream SUB ---
    sub = ctx.socket(zmq.SUB)
    sub.curve_publickey  = base64.b64decode(BRIDGE_PUBLIC_KEY)
    sub.curve_secretkey  = base64.b64decode(BRIDGE_SECRET_KEY)
    sub.curve_server     = True
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.RCVTIMEO, 1000)   # 1s timeout so loop stays responsive
    sub.bind(f"tcp://0.0.0.0:{DOWNSTREAM_PORT}")
    log.info(f"Downstream SUB bound to tcp://0.0.0.0:{DOWNSTREAM_PORT}")

    # --- Upstream PUB ---
    pub = ctx.socket(zmq.PUB)
    pub.curve_publickey  = base64.b64decode(BRIDGE_PUBLIC_KEY)
    pub.curve_secretkey  = base64.b64decode(BRIDGE_SECRET_KEY)
    pub.curve_server     = True
    pub.bind(f"tcp://0.0.0.0:{UPSTREAM_PORT}")
    log.info(f"Upstream PUB bound to tcp://0.0.0.0:{UPSTREAM_PORT}")

    return sub, pub


# ── UPSTREAM PUBLISH HELPERS ──────────────────────────────────────────────────
def publish(pub_sock, payload: dict):
    try:
        pub_sock.send_json(payload)
        log.info(f"PUB out | {payload['msg_type']} | poll_log_id={payload.get('poll_log_id','n/a')}")
    except Exception as e:
        log.error(f"PUB send failed: {e}")


def publish_account_state(pub_sock):
    acc = mt5.account_info()
    if acc is None:
        log.warning("account_info() returned None — MT5 disconnected?")
        publish(pub_sock, {
            "msg_type": "bridge_offline",
            "ts": utcnow()
        })
        return

    # Build open positions snapshot for reconciliation
    positions = mt5.positions_get()
    open_positions = {}
    if positions:
        for p in positions:
            open_positions[p.ticket] = {
                "ticket":     p.ticket,
                "symbol":     p.symbol,
                "volume":     p.volume,
                "price_open": p.price_open,
                "profit":     p.profit,
                "magic":      p.magic,
            }

    # Build recent deals snapshot (last 48 hours) for close reconciliation
    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    from_dt = now_utc - timedelta(hours=48)
    deals = mt5.history_deals_get(from_dt, now_utc)
    recent_deals = {}
    if deals:
        for d in deals:
            if d.entry == 1:  # entry=1 means closing deal
                recent_deals[d.position_id] = {
                    "ticket":      d.ticket,
                    "position_id": d.position_id,
                    "symbol":      d.symbol,
                    "price":       d.price,
                    "profit":      d.profit,
                    "time":        d.time,
                    "magic":       d.magic,
                }

    active_symbols = [s.name for s in mt5.symbols_get() if s.visible]
    live_quotes = {}
    for sym in active_symbols:
        tick = mt5.symbol_info_tick(sym)
        info = mt5.symbol_info(sym)
        if tick is None or info is None:
            continue
        live_quotes[sym] = {
            "bid":        tick.bid,
            "ask":        tick.ask,
            "tick_value": info.trade_tick_value,
            "tick_size":  info.trade_tick_size,
        }

    publish(pub_sock, {
        "msg_type":       "account_state",
        "equity":         acc.equity,
        "balance":        acc.balance,
        "margin_free":    acc.margin_free,
        "open_positions": open_positions,
        "recent_deals":   recent_deals,
        "live_quotes":    live_quotes,
        "ts":             utcnow()
    })


def publish_confirm(pub_sock, payload, result):
    publish(pub_sock, {
        "msg_type":          "trade_confirm",
        "poll_log_id":       payload["poll_log_id"],
        "strategy_id":       payload["strategy_id"],
        "mt5_order_ticket":  result.order,
        "mt5_deal_ticket":   result.deal,
        "mt5_position_id":   result.order,
        "filled_price":      result.price,
        "filled_volume":     result.volume,
        "sl":                payload["sl"],
        "tp":                payload["tp"],
        "mt5_symbol":        payload["mt5_symbol"],
        "instrument":        payload.get("instrument"),
        "direction":         payload["direction"],
        "status":            "filled",
        "ts":                utcnow()
    })


def publish_reject(pub_sock, payload, retcode, description):
    publish(pub_sock, {
        "msg_type":        "trade_reject",
        "poll_log_id":     payload.get("poll_log_id"),
        "strategy_id":     payload.get("strategy_id"),
        "mt5_retcode":     retcode,
        "mt5_retcode_desc": description,
        "ts":              utcnow()
    })


def utcnow():
    return datetime.now(timezone.utc).isoformat()


# ── MT5 INITIALIZATION ────────────────────────────────────────────────────────
def mt5_connect():
    log.info("Connecting to MT5 terminal...")
    ok = mt5.initialize(
        login=MT5_LOGIN,
        server=MT5_SERVER,
        password=MT5_PASSWORD
    )
    if not ok:
        log.error(f"mt5.initialize() failed: {mt5.last_error()}")
        return False
    acc = mt5.account_info()
    log.info(f"MT5 connected | login={acc.login} | balance={acc.balance} {acc.currency} | mode={acc.trade_mode}")
    return True


# ── STALE ORDER CHECK ─────────────────────────────────────────────────────────
# Stale check state constants
TICK_VALID            = "VALID"
TICK_STALE_NO_TICK    = "STALE_NO_TICK"
TICK_STALE_PRICE      = "STALE_PRICE_MOVED"

TICK_AGE_THRESHOLD_SECS = 300  # 5 minutes — older tick treated as no live quote

def check_tick_state(payload) -> str:
    import time as _time
    sym = payload["mt5_symbol"]
    sig_close = payload["sig_close"]
    mt5.symbol_select(sym, True)
    tick = mt5.symbol_info_tick(sym)

    if tick is None:
        log.warning(f"No tick for {sym} — STALE_NO_TICK")
        return TICK_STALE_NO_TICK

    tick_age_secs = _time.time() - tick.time
    if tick_age_secs > TICK_AGE_THRESHOLD_SECS:
        log.warning(f"Tick age {tick_age_secs:.0f}s > {TICK_AGE_THRESHOLD_SECS}s for {sym} — STALE_NO_TICK")
        return TICK_STALE_NO_TICK

    info = mt5.symbol_info(sym)
    pip = info.point * 10
    mid = (tick.bid + tick.ask) / 2
    if mid <= 0:
        log.warning(f"Zero mid price for {sym} — treating as VALID, proceeding")
        return TICK_VALID

    distance_pips = abs(mid - sig_close) / pip
    if distance_pips > STALE_THRESHOLD_PIPS:
        log.warning(
            f"Price moved | {sym} | sig_close={sig_close} "
            f"mid={mid:.5f} | distance={distance_pips:.1f}p > {STALE_THRESHOLD_PIPS}p — STALE_PRICE_MOVED"
        )
        return TICK_STALE_PRICE

    return TICK_VALID


# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def build_request(payload, filling_mode):
    direction = payload["direction"]
    sym = payload["mt5_symbol"]
    tick = mt5.symbol_info_tick(sym)
    price = tick.ask if direction == "long" else tick.bid
    order_type = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL

    return {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       sym,
        "volume":       float(payload["volume"]),
        "type":         order_type,
        "price":        price,
        "sl":           float(payload["sl"]),
        "tp":           float(payload["tp"]),
        "deviation":    10,
        "magic":        int(payload["magic"]),
        "comment":      payload["comment"],
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": filling_mode,
    }


def close_position(pub_sock, payload):
    """Close an existing MT5 position by ticket — used for timeout exits."""
    sym        = payload["mt5_symbol"]
    ticket     = int(payload["mt5_ticket"])
    volume     = float(payload["volume"])
    direction  = payload["direction"].lower()
    reason     = payload.get("reason", "timeout")

    tick = mt5.symbol_info_tick(sym)
    if tick is None:
        log.error(f"close_position: no tick for {sym} | ticket={ticket}")
        publish(pub_sock, {
            "msg_type":   "close_reject",
            "mt5_ticket": ticket,
            "reason":     f"no tick for {sym}",
            "ts":         utcnow(),
        })
        return

    if direction == "long":
        close_type  = mt5.ORDER_TYPE_SELL
        close_price = tick.bid
    else:
        close_type  = mt5.ORDER_TYPE_BUY
        close_price = tick.ask

    close_request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       sym,
        "volume":       volume,
        "type":         close_type,
        "position":     ticket,
        "price":        close_price,
        "deviation":    10,
        "magic":        int(payload.get("magic", 0)),
        "comment":      f"candlelab_{reason}",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    log.info(f"Closing position | {sym} ticket={ticket} dir={direction} vol={volume} price={close_price:.5f}")
    result = mt5.order_send(close_request)

    if result is None:
        err = mt5.last_error()
        log.error(f"close_position: order_send None | {err}")
        publish(pub_sock, {
            "msg_type":   "close_reject",
            "mt5_ticket": ticket,
            "reason":     str(err),
            "ts":         utcnow(),
        })
        return

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"Position closed | ticket={ticket} retcode={result.retcode}")
        publish(pub_sock, {
            "msg_type":     "close_confirm",
            "mt5_ticket":   ticket,
            "filled_price": result.price,
            "volume":       result.volume,
            "reason":       reason,
            "ts":           utcnow(),
        })
    else:
        log.warning(f"close_position failed | retcode={result.retcode} | {result.comment}")
        publish(pub_sock, {
            "msg_type":   "close_reject",
            "mt5_ticket": ticket,
            "reason":     f"retcode={result.retcode} {result.comment}",
            "ts":         utcnow(),
        })


def execute_order(pub_sock, payload):
    sym = payload["mt5_symbol"]
    log.info(
        f"Executing | {sym} {payload['direction']} "
        f"vol={payload['volume']} sl={payload['sl']} tp={payload['tp']} "
        f"poll_log_id={payload['poll_log_id']}"
    )

    # Try IOC first, fall back to FOK on retcode 10030
    for filling_mode, label in [
        (mt5.ORDER_FILLING_IOC, "IOC"),
        (mt5.ORDER_FILLING_FOK, "FOK")
    ]:
        request = build_request(payload, filling_mode)
        result = mt5.order_send(request)

        if result is None:
            err = mt5.last_error()
            log.error(f"order_send() returned None | {err}")
            publish_reject(pub_sock, payload, -1, str(err))
            return

        if result.retcode == mt5.TRADE_RETCODE_DONE:
            # IOC fills often return deal=0 and price=0.0 on initial response
            # Query MT5 history to get real deal ticket and fill price
            real_deal = result.deal
            real_price = result.price
            if real_deal == 0 or real_price == 0.0:
                import time as _time
                _time.sleep(0.2)  # brief wait for MT5 to record the deal
                deals = mt5.history_deals_get(position=result.order)
                if deals:
                    opening_deals = [d for d in deals if d.entry == 0]
                    if opening_deals:
                        best = opening_deals[-1]
                        real_deal = best.ticket
                        real_price = best.price
                        log.info(
                            f"Deal resolved via history | deal={real_deal} "
                            f"price={real_price} order={result.order}"
                        )
                    else:
                        log.warning(
                            f"No opening deal found in history for order={result.order}"
                        )
                else:
                    log.warning(
                        f"history_deals_get returned None for order={result.order}"
                    )

            class _PatchedResult:
                order = result.order
                deal = real_deal
                price = real_price
                volume = result.volume

            log.info(
                f"Filled ({label}) | order={result.order} deal={real_deal} "
                f"price={real_price} vol={result.volume}"
            )
            publish_confirm(pub_sock, payload, _PatchedResult())
            return

        if result.retcode == 10030:
            log.warning(f"Filling mode {label} unsupported (10030) — retrying with next mode")
            continue

        # Any other retcode — reject immediately, no retry
        log.warning(
            f"Order rejected | retcode={result.retcode} | {result.comment}"
        )
        publish_reject(pub_sock, payload, result.retcode, result.comment)
        return

    # Both filling modes exhausted
    log.error("Both IOC and FOK filling modes rejected by broker")
    publish_reject(pub_sock, payload, 10030, "Unsupported filling mode — IOC and FOK both failed")


# ── PAYLOAD VALIDATION ────────────────────────────────────────────────────────
REQUIRED_FIELDS = [
    "poll_log_id", "strategy_id", "mt5_symbol", "direction",
    "volume", "sl", "tp", "sig_close", "magic", "comment"
]

def validate(payload):
    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        log.error(f"Payload missing fields: {missing}")
        return False
    if payload["direction"] not in ("long", "short"):
        log.error(f"Invalid direction: {payload['direction']}")
        return False
    if float(payload["volume"]) <= 0:
        log.error(f"Invalid volume: {payload['volume']}")
        return False
    return True


# ── ACCOUNT STATE THREAD ──────────────────────────────────────────────────────
def account_state_loop(pub_sock, stop_event):
    """Publishes account state every ACCOUNT_STATE_INTERVAL seconds."""
    while not stop_event.is_set():
        publish_account_state(pub_sock)
        stop_event.wait(timeout=ACCOUNT_STATE_INTERVAL)


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    log.info("ftmo-bridge subscriber starting")

    if not mt5_connect():
        log.error("MT5 connection failed — exiting")
        return

    ctx = zmq.Context()
    sub_sock, pub_sock = build_sockets(ctx)

    # Give PUB socket time to bind before Railway connects
    time.sleep(1)

    # Start account state heartbeat thread
    stop_event = threading.Event()
    heartbeat = threading.Thread(
        target=account_state_loop,
        args=(pub_sock, stop_event),
        daemon=True
    )
    heartbeat.start()
    log.info("Account state heartbeat started")
    log.info("Waiting for order payloads...")

    try:
        while True:
            try:
                msg = sub_sock.recv_json()
            except zmq.Again:
                # RCVTIMEO hit — no message, loop continues
                continue
            except Exception as e:
                log.error(f"SUB recv error: {e}")
                time.sleep(1)
                continue

            log.info(f"Received payload: {json.dumps(msg)}")

            # Route 1: Position close — bypass validation and stale price checks
            if msg.get("action") == "close_position":
                close_position(pub_sock, msg)
                continue

            # Route 2: Standard order execution
            if not validate(msg):
                publish_reject(pub_sock, msg, -2, "Payload validation failed")
                continue

            _tick_state = check_tick_state(msg)
            if _tick_state == TICK_STALE_PRICE:
                publish_reject(pub_sock, msg, -3, "Stale order — price moved beyond threshold")
                continue
            if _tick_state == TICK_STALE_NO_TICK:
                for _attempt in range(1, 4):
                    time.sleep(0.5)
                    _tick_state = check_tick_state(msg)
                    log.info(f"Stale retry {_attempt}/3 | {msg.get('mt5_symbol')} | state={_tick_state}")
                    if _tick_state != TICK_STALE_NO_TICK:
                        break
                if _tick_state == TICK_STALE_NO_TICK:
                    publish_reject(pub_sock, msg, -3, "Stale order — no tick after 3 retries")
                    continue
                if _tick_state == TICK_STALE_PRICE:
                    publish_reject(pub_sock, msg, -3, "Stale order — price moved after tick arrived")
                    continue

            execute_order(pub_sock, msg)

    except KeyboardInterrupt:
        log.info("Keyboard interrupt — shutting down")
        sys.exit(1)
    finally:
        stop_event.set()
        sub_sock.close()
        pub_sock.close()
        ctx.term()
        mt5.shutdown()
        log.info("Bridge shutdown complete")

    # Force non-zero exit outside the try/finally block so Task Scheduler restarts it,
    # but unhandled exceptions still print their tracebacks natively.
    sys.exit(1)


if __name__ == "__main__":
    main()
