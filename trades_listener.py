#!/usr/bin/env python3
"""Suscribe a trades por WebSocket. Agrega buy/sell por minuto en delta_buckets
(comportamiento original) y además guarda trades que superen el umbral de
notional en whale_trades."""
import asyncio
import json
import sqlite3
import sys
import time

import websockets

from config import COINS, WS_URL, connect, whale_threshold

MINUTE_MS = 60_000

def round_to_minute(ts_ms: int) -> int:
    return (ts_ms // MINUTE_MS) * MINUTE_MS

def flush_bucket(conn, coin, minute_ms, buy_vol, sell_vol, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            conn.execute(
                """INSERT INTO delta_buckets (coin, minute_ms, buy_vol, sell_vol)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(coin, minute_ms) DO UPDATE SET
                     buy_vol = buy_vol + excluded.buy_vol,
                     sell_vol = sell_vol + excluded.sell_vol""",
                (coin, minute_ms, buy_vol, sell_vol),
            )
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(0.2 * (attempt + 1))
            else:
                raise

WHALE_SQL = """INSERT OR IGNORE INTO whale_trades
    (coin, ts_ms, side, px, sz, notional, buyer, seller, tx_hash, tid)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def flush_whales(conn, whales, max_attempts=5):
    if not whales:
        return 0
    for attempt in range(max_attempts):
        try:
            conn.executemany(WHALE_SQL, whales)
            conn.commit()
            return len(whales)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(0.2 * (attempt + 1))
            else:
                raise
    return 0

async def listen():
    conn = connect()
    pending = {}
    whales_buf = []

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        for coin in COINS:
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": coin},
            }))
        print(f"trades_listener suscrito a {len(COINS)} pares")
        last_flush = time.time()

        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("channel") != "trades":
                continue
            for trade in msg.get("data", []):
                try:
                    coin = trade["coin"]
                    side = trade["side"]
                    sz = float(trade["sz"])
                    px = float(trade["px"])
                    ts_ms = int(trade["time"])
                except (KeyError, TypeError, ValueError):
                    continue
                notional = sz * px
                minute_ms = round_to_minute(ts_ms)

                key = (coin, minute_ms)
                if key not in pending:
                    pending[key] = [0.0, 0.0]
                if side == "B":
                    pending[key][0] += notional
                else:
                    pending[key][1] += notional

                if notional >= whale_threshold(coin):
                    users = trade.get("users") or [None, None]
                    buyer = users[0] if len(users) > 0 else None
                    seller = users[1] if len(users) > 1 else None
                    tid = trade.get("tid")
                    whales_buf.append((
                        coin, ts_ms, side, px, sz, notional,
                        buyer, seller, trade.get("hash"), tid,
                    ))

            if time.time() - last_flush > 10:
                for (coin, minute_ms), (buy_vol, sell_vol) in pending.items():
                    try:
                        flush_bucket(conn, coin, minute_ms, buy_vol, sell_vol)
                    except Exception as e:
                        print(f"[{time.strftime('%H:%M:%S')}] delta flush ERROR: {e}", file=sys.stderr)
                try:
                    n_w = flush_whales(conn, whales_buf)
                except Exception as e:
                    print(f"[{time.strftime('%H:%M:%S')}] whale flush ERROR: {e}", file=sys.stderr)
                    n_w = 0
                print(f"[{time.strftime('%H:%M:%S')}] delta buckets={len(pending)} whales={n_w}")
                pending.clear()
                whales_buf.clear()
                last_flush = time.time()

async def main():
    backoff = 1
    while True:
        try:
            await listen()
            backoff = 1
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR, reconecto en {backoff}s: {e}", file=sys.stderr)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())
