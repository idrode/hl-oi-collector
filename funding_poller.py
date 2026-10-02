#!/usr/bin/env python3
"""Guarda funding actual (de metaAndAssetCtxs, reusando esa misma llamada)
y predictedFundings de varios venues. Backfill inicial con fundingHistory."""
import sqlite3
import sys
import time
import requests

from config import (
    API_URL,
    COINS,
    FUNDING_POLL_SECONDS,
    connect,
)

HTTP_TIMEOUT = 15
BACKFILL_DAYS = 7

def post(payload):
    r = requests.post(API_URL, json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def hour_floor_ms(ts_ms: int) -> int:
    return (ts_ms // 3_600_000) * 3_600_000

def insert_batch(conn, rows, max_attempts=5):
    # Upsert: una fila por (coin, venue, ts_ms); si existe se actualizan los
    # valores + ingested_ms para que /health detecte pollers vivos aunque
    # ts_ms esté fijado a la hora en curso.
    sql = ("""INSERT INTO funding_snapshots
              (coin, venue, ts_ms, funding_rate, premium, next_funding_time,
               interval_hours, ingested_ms)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(coin, venue, ts_ms) DO UPDATE SET
                funding_rate=excluded.funding_rate,
                premium=excluded.premium,
                next_funding_time=excluded.next_funding_time,
                interval_hours=excluded.interval_hours,
                ingested_ms=excluded.ingested_ms""")
    for attempt in range(max_attempts):
        try:
            conn.executemany(sql, rows)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(0.2 * (attempt + 1))
            else:
                raise

def backfill(conn):
    start_ms = int((time.time() - BACKFILL_DAYS * 86400) * 1000)
    total = 0
    for coin in COINS:
        try:
            data = post({"type": "fundingHistory", "coin": coin, "startTime": start_ms})
        except Exception as e:
            print(f"[backfill] {coin} ERROR: {e}", file=sys.stderr)
            continue
        rows = []
        for item in data or []:
            try:
                t = int(item["time"])
                rows.append((
                    coin, "HlHist", t,
                    float(item["fundingRate"]),
                    float(item.get("premium", 0.0)) if item.get("premium") is not None else None,
                    None, None, t,
                ))
            except (KeyError, TypeError, ValueError):
                continue
        if rows:
            insert_batch(conn, rows)
            total += len(rows)
        time.sleep(0.1)
    print(f"[backfill] {total} filas de fundingHistory ({BACKFILL_DAYS} días)")

def poll_current(conn):
    data = post({"type": "metaAndAssetCtxs"})
    meta, ctxs = data[0], data[1]
    universe = meta["universe"]
    coins_set = set(COINS)
    now_ms = int(time.time() * 1000)
    hr_ms = hour_floor_ms(now_ms)
    rows = []
    for info, ctx in zip(universe, ctxs):
        coin = info["name"]
        if coin not in coins_set:
            continue
        try:
            fr = float(ctx.get("funding", 0.0))
        except (TypeError, ValueError):
            continue
        rows.append((coin, "Hl", hr_ms, fr, None, None, 1, now_ms))
    if rows:
        insert_batch(conn, rows)
    return len(rows)

def poll_predicted(conn):
    data = post({"type": "predictedFundings"})
    coins_set = set(COINS)
    now_ms = int(time.time() * 1000)
    hr_ms = hour_floor_ms(now_ms)
    rows = []
    for entry in data or []:
        try:
            coin, venues = entry[0], entry[1]
        except (TypeError, IndexError):
            continue
        if coin not in coins_set:
            continue
        for v in venues:
            try:
                venue, info = v[0], v[1]
                if not info:
                    continue
                fr = float(info["fundingRate"])
                nft = int(info["nextFundingTime"])
                ih = info.get("fundingIntervalHours")
                # ts_ms = hora redondeada (dedupe); next_funding_time guarda el
                # campo del venue; ingested_ms refleja la última poll real.
                rows.append((coin, venue, hr_ms, fr, None, nft, ih, now_ms))
            except (KeyError, TypeError, ValueError, IndexError):
                continue
    if rows:
        insert_batch(conn, rows)
    return len(rows)

def main():
    conn = connect()
    print(f"funding_poller arrancado (poll cada {FUNDING_POLL_SECONDS}s)")
    try:
        backfill(conn)
    except Exception as e:
        print(f"[backfill] fallo global: {e}", file=sys.stderr)
    while True:
        try:
            n_cur = poll_current(conn)
            n_pred = poll_predicted(conn)
            print(f"[{time.strftime('%H:%M:%S')}] funding guardado: {n_cur} actuales, {n_pred} predichos")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {e}", file=sys.stderr)
        time.sleep(FUNDING_POLL_SECONDS)

if __name__ == "__main__":
    main()
