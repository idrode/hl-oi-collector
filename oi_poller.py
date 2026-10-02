#!/usr/bin/env python3
"""Poller de Open Interest: cada 5 s saca metaAndAssetCtxs y guarda
snapshots SOLO para las monedas en config.COINS."""
import sqlite3
import sys
import time

import requests

from config import API_URL, COINS_SET, connect

POLL_SECONDS = 5
HTTP_TIMEOUT = 10

def fetch_meta_and_asset_ctxs():
    resp = requests.post(API_URL, json={"type": "metaAndAssetCtxs"}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def insert_snapshots(conn, data):
    meta, asset_ctxs = data[0], data[1]
    universe = meta["universe"]
    ts_ms = int(time.time() * 1000)

    rows = []
    for coin_info, ctx in zip(universe, asset_ctxs):
        coin = coin_info["name"]
        if coin not in COINS_SET:
            continue
        try:
            mark_px = float(ctx["markPx"])
            oi = float(ctx["openInterest"])
            funding = float(ctx.get("funding", 0.0))
        except (KeyError, TypeError, ValueError):
            continue
        rows.append((coin, ts_ms, oi, oi * mark_px, mark_px, funding))

    conn.executemany(
        """INSERT INTO oi_snapshots (coin, ts_ms, oi, oi_notional, mark_px, funding)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)

def insert_with_retry(conn, data, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            return insert_snapshots(conn, data)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                wait = 0.2 * (attempt + 1)
                print(f"[{time.strftime('%H:%M:%S')}] DB bloqueada, reintento {attempt+1} en {wait:.1f}s", file=sys.stderr)
                time.sleep(wait)
            else:
                raise
    return None

def main():
    conn = connect()
    print(f"oi_poller arrancado. {len(COINS_SET)} monedas, poll cada {POLL_SECONDS}s.")
    while True:
        try:
            data = fetch_meta_and_asset_ctxs()
            n = insert_with_retry(conn, data)
            if n is not None:
                print(f"[{time.strftime('%H:%M:%S')}] {n} snapshots guardados")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {e}", file=sys.stderr)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
