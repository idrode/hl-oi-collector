#!/usr/bin/env python3
"""Backfill con candleSnapshot y mantenimiento en vivo por WebSocket para
1m, 5m, 15m y 1h. Upsert por (coin, interval, t)."""
import asyncio
import json
import sqlite3
import sys
import time

import requests
import websockets

from config import (
    API_URL,
    CANDLE_INTERVALS,
    COINS,
    WS_URL,
    connect,
)

HTTP_TIMEOUT = 15

BACKFILL_WINDOW_MS = {
    "1m": 6 * 3_600_000,       # 6 h
    "5m": 24 * 3_600_000,      # 1 d
    "15m": 3 * 24 * 3_600_000, # 3 d
    "1h": 30 * 24 * 3_600_000, # 30 d
}

UPSERT_SQL = """INSERT INTO candles (coin, interval, t, close_t, o, h, l, c, v, n)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(coin, interval, t) DO UPDATE SET
      close_t=excluded.close_t,
      o=excluded.o, h=excluded.h, l=excluded.l, c=excluded.c,
      v=excluded.v, n=excluded.n"""

def row_from_candle(c):
    return (
        c["s"], c["i"], int(c["t"]), int(c["T"]),
        float(c["o"]), float(c["h"]), float(c["l"]), float(c["c"]),
        float(c["v"]), int(c["n"]),
    )

def upsert(conn, rows, max_attempts=5):
    if not rows:
        return 0
    for attempt in range(max_attempts):
        try:
            conn.executemany(UPSERT_SQL, rows)
            conn.commit()
            return len(rows)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(0.2 * (attempt + 1))
            else:
                raise
    return 0

def backfill(conn):
    now_ms = int(time.time() * 1000)
    total = 0
    for interval in CANDLE_INTERVALS:
        window = BACKFILL_WINDOW_MS[interval]
        start = now_ms - window
        for coin in COINS:
            try:
                r = requests.post(API_URL, timeout=HTTP_TIMEOUT, json={
                    "type": "candleSnapshot",
                    "req": {"coin": coin, "interval": interval,
                            "startTime": start, "endTime": now_ms},
                })
                r.raise_for_status()
                data = r.json() or []
            except Exception as e:
                print(f"[backfill] {coin}/{interval} ERROR: {e}", file=sys.stderr)
                continue
            rows = []
            for c in data:
                try:
                    rows.append(row_from_candle(c))
                except (KeyError, TypeError, ValueError):
                    continue
            total += upsert(conn, rows)
            time.sleep(0.05)
    print(f"[backfill] velas insertadas/actualizadas: {total}")

async def listen(conn):
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        for interval in CANDLE_INTERVALS:
            for coin in COINS:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "candle", "coin": coin, "interval": interval},
                }))
        print(f"candles suscrito a {len(COINS)*len(CANDLE_INTERVALS)} streams")
        pending = []
        last_flush = time.time()
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("channel") != "candle":
                continue
            c = msg.get("data") or {}
            try:
                pending.append(row_from_candle(c))
            except (KeyError, TypeError, ValueError):
                continue
            now = time.time()
            if now - last_flush > 5 or len(pending) >= 200:
                try:
                    n = upsert(conn, pending)
                    if n:
                        print(f"[{time.strftime('%H:%M:%S')}] candles upsert={n}")
                except Exception as e:
                    print(f"[{time.strftime('%H:%M:%S')}] upsert ERROR: {e}", file=sys.stderr)
                pending.clear()
                last_flush = now

async def main():
    conn = connect()
    try:
        backfill(conn)
    except Exception as e:
        print(f"[backfill] fallo global: {e}", file=sys.stderr)
    backoff = 1
    while True:
        try:
            await listen(conn)
            backoff = 1
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] WS ERROR, reconecto en {backoff}s: {e}", file=sys.stderr)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())
