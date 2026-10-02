#!/usr/bin/env python3
"""Suscribe a l2Book para todas las monedas. Agrega en memoria y vuelca
cada BOOK_FLUSH_SECONDS una fila por coin con best bid/ask, spread e imbalance
de profundidad dentro de ±0.5% y ±2% del mid."""
import asyncio
import json
import sqlite3
import sys
import time

import websockets

from config import (
    BOOK_DEPTH_BAND_BIG,
    BOOK_DEPTH_BAND_SMALL,
    BOOK_FLUSH_SECONDS,
    COINS,
    WS_URL,
    connect,
)

INSERT_SQL = """INSERT INTO book_snapshots
    (coin, ts_ms, bid_px, bid_sz, ask_px, ask_sz, mid_px, spread_bps,
     bid_depth_05, ask_depth_05, bid_depth_2, ask_depth_2, imb_05, imb_2)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def compute_row(coin, book):
    bids = book.get("levels", [[], []])[0]
    asks = book.get("levels", [[], []])[1]
    if not bids or not asks:
        return None
    bid_px = float(bids[0]["px"]); bid_sz = float(bids[0]["sz"])
    ask_px = float(asks[0]["px"]); ask_sz = float(asks[0]["sz"])
    mid = (bid_px + ask_px) / 2.0
    if mid <= 0:
        return None
    spread_bps = (ask_px - bid_px) / mid * 10_000.0
    lo_s = mid * (1 - BOOK_DEPTH_BAND_SMALL); hi_s = mid * (1 + BOOK_DEPTH_BAND_SMALL)
    lo_b = mid * (1 - BOOK_DEPTH_BAND_BIG);   hi_b = mid * (1 + BOOK_DEPTH_BAND_BIG)
    bd_s = ad_s = bd_b = ad_b = 0.0
    for lvl in bids:
        try:
            p = float(lvl["px"]); s = float(lvl["sz"])
        except (KeyError, TypeError, ValueError):
            continue
        notional = p * s
        if p >= lo_b:
            bd_b += notional
            if p >= lo_s:
                bd_s += notional
    for lvl in asks:
        try:
            p = float(lvl["px"]); s = float(lvl["sz"])
        except (KeyError, TypeError, ValueError):
            continue
        notional = p * s
        if p <= hi_b:
            ad_b += notional
            if p <= hi_s:
                ad_s += notional
    imb_s = (bd_s - ad_s) / (bd_s + ad_s) if (bd_s + ad_s) > 0 else 0.0
    imb_b = (bd_b - ad_b) / (bd_b + ad_b) if (bd_b + ad_b) > 0 else 0.0
    ts_ms = int(book.get("time") or time.time() * 1000)
    return (coin, ts_ms, bid_px, bid_sz, ask_px, ask_sz, mid, spread_bps,
            bd_s, ad_s, bd_b, ad_b, imb_s, imb_b)

def flush(conn, latest, max_attempts=5):
    rows = []
    for coin, book in latest.items():
        row = compute_row(coin, book)
        if row:
            rows.append(row)
    if not rows:
        return 0
    for attempt in range(max_attempts):
        try:
            conn.executemany(INSERT_SQL, rows)
            conn.commit()
            return len(rows)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(0.2 * (attempt + 1))
            else:
                raise
    return 0

async def listen(conn):
    latest = {}
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        for coin in COINS:
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            }))
        print(f"book_listener suscrito a {len(COINS)} pares (l2Book)")
        last_flush = time.time()
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("channel") != "l2Book":
                continue
            data = msg.get("data") or {}
            coin = data.get("coin")
            if coin:
                latest[coin] = data
            now = time.time()
            if now - last_flush >= BOOK_FLUSH_SECONDS:
                try:
                    n = flush(conn, latest)
                    print(f"[{time.strftime('%H:%M:%S')}] book: {n} filas volcadas")
                except Exception as e:
                    print(f"[{time.strftime('%H:%M:%S')}] flush ERROR: {e}", file=sys.stderr)
                last_flush = now

async def main():
    conn = connect()
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
