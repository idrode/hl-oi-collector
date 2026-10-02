#!/usr/bin/env python3
"""Mantenimiento periódico de la base.

Operaciones:
  - Downsample: filas de oi_snapshots más antiguas que OI_DOWNSAMPLE_AFTER_DAYS
    se agregan a oi_1m (una fila por coin y minuto, último valor del minuto).
  - DELETE por tabla aplicando RETENTION_DAYS / CANDLE_RETENTION_DAYS,
    en lotes de DELETE_BATCH_SIZE para no bloquear ni inflar el WAL.
  - wal_checkpoint(TRUNCATE) tras cada ciclo.
  - VACUUM semanal (controlado por .last_vacuum).

Modos:
  python3 cleanup.py           -> una ejecución y salir.
  python3 cleanup.py --daemon  -> bucle infinito con CLEANUP_INTERVAL_SECONDS.
  python3 cleanup.py --vacuum  -> fuerza VACUUM al final del ciclo.
"""
import os
import sqlite3
import sys
import time

from config import (
    CANDLE_RETENTION_DAYS,
    CLEANUP_INTERVAL_SECONDS,
    DB_PATH,
    DELETE_BATCH_SIZE,
    OI_DOWNSAMPLE_AFTER_DAYS,
    RETENTION_DAYS,
    VACUUM_INTERVAL_SECONDS,
    connect,
)

LAST_VACUUM_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".last_vacuum")

TABLE_TIME_COL = {
    "oi_snapshots": "ts_ms",
    "oi_1m": "minute_ms",
    "delta_buckets": "minute_ms",
    "funding_snapshots": "ts_ms",
    "book_snapshots": "ts_ms",
    "whale_trades": "ts_ms",
}

def db_sizes():
    def sz(p):
        try: return os.path.getsize(p)
        except OSError: return 0
    return sz(DB_PATH), sz(DB_PATH + "-wal")

def human(n):
    for unit in ("B","KB","MB","GB"):
        if n < 1024: return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"

def batch_delete(conn, sql, params=(), batch=DELETE_BATCH_SIZE):
    """Borra en lotes usando WHERE rowid IN (SELECT rowid ... LIMIT N).
    `sql` es la parte 'FROM t WHERE ...'. Devuelve filas totales borradas."""
    total = 0
    while True:
        cur = conn.execute(
            f"DELETE {sql} AND rowid IN (SELECT rowid {sql} LIMIT ?)",
            tuple(params) + tuple(params) + (batch,),
        )
        n = cur.rowcount
        conn.commit()
        total += n
        if n < batch:
            break
    return total

def downsample_oi(conn, now_ms):
    """Mueve oi_snapshots con ts_ms < cutoff a oi_1m (último valor del minuto)."""
    cutoff = now_ms - OI_DOWNSAMPLE_AFTER_DAYS * 86_400_000
    # Inserta/upsert agrupando por minuto.
    inserted = conn.execute(
        """INSERT INTO oi_1m (coin, minute_ms, oi, oi_notional, mark_px, funding)
           SELECT o.coin, (o.ts_ms/60000)*60000 AS m,
                  o.oi, o.oi_notional, o.mark_px, o.funding
           FROM oi_snapshots o
           JOIN (
               SELECT coin, (ts_ms/60000)*60000 AS m, MAX(ts_ms) AS mx
               FROM oi_snapshots
               WHERE ts_ms < ?
               GROUP BY coin, m
           ) t ON o.coin = t.coin
                AND (o.ts_ms/60000)*60000 = t.m
                AND o.ts_ms = t.mx
           WHERE true
           ON CONFLICT(coin, minute_ms) DO UPDATE SET
             oi=excluded.oi, oi_notional=excluded.oi_notional,
             mark_px=excluded.mark_px, funding=excluded.funding""",
        (cutoff,),
    ).rowcount
    conn.commit()
    # Y borra lo original en lotes.
    deleted = batch_delete(conn, "FROM oi_snapshots WHERE ts_ms < ?", (cutoff,))
    return inserted, deleted

def run_once(do_vacuum: bool = False) -> dict:
    t0 = time.time()
    size_db_before, size_wal_before = db_sizes()
    conn = connect(timeout=120)
    now_ms = int(time.time() * 1000)

    deleted = {}
    # 1) Downsample oi
    ins, delo = downsample_oi(conn, now_ms)
    deleted["oi_snapshots"] = delo
    deleted["oi_1m_inserted"] = ins

    # 2) Retención por tabla (sin candles)
    for table, tcol in TABLE_TIME_COL.items():
        days = RETENTION_DAYS.get(table)
        if days is None:
            continue
        cutoff = now_ms - days * 86_400_000
        if table == "oi_snapshots":
            continue  # ya cubierto por downsample
        n = batch_delete(conn, f"FROM {table} WHERE {tcol} < ?", (cutoff,))
        deleted.setdefault(table, 0)
        deleted[table] += n

    # 3) Candles con retención por intervalo
    cand_total = 0
    for interval, days in CANDLE_RETENTION_DAYS.items():
        cutoff = now_ms - days * 86_400_000
        n = batch_delete(conn, "FROM candles WHERE interval = ? AND t < ?", (interval, cutoff))
        cand_total += n
    deleted["candles"] = cand_total

    # 4) Checkpoint WAL
    try:
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
    except Exception as e:
        row = f"ERROR: {e}"

    # 5) VACUUM semanal (o forzado)
    did_vacuum = False
    last_vac = 0.0
    try:
        with open(LAST_VACUUM_FILE) as f:
            last_vac = float(f.read().strip() or 0)
    except (OSError, ValueError):
        last_vac = 0.0
    if do_vacuum or (time.time() - last_vac) > VACUUM_INTERVAL_SECONDS:
        try:
            conn.execute("VACUUM")
            did_vacuum = True
            with open(LAST_VACUUM_FILE, "w") as f:
                f.write(str(time.time()))
        except Exception as e:
            print(f"VACUUM ERROR: {e}", file=sys.stderr)
    conn.close()

    size_db_after, size_wal_after = db_sizes()
    dur = time.time() - t0

    msg = (
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] cleanup {dur:.1f}s "
        f"borrados={deleted} checkpoint={row} vacuum={did_vacuum} "
        f"DB {human(size_db_before)}->{human(size_db_after)} "
        f"WAL {human(size_wal_before)}->{human(size_wal_after)}"
    )
    print(msg)
    return {"deleted": deleted, "vacuum": did_vacuum,
            "db_before": size_db_before, "db_after": size_db_after,
            "wal_before": size_wal_before, "wal_after": size_wal_after}

def main():
    args = set(sys.argv[1:])
    if "--daemon" in args:
        print(f"cleanup daemon arrancado (cada {CLEANUP_INTERVAL_SECONDS}s)")
        while True:
            try:
                run_once(do_vacuum="--vacuum" in args)
            except Exception as e:
                print(f"ERROR en ciclo cleanup: {e}", file=sys.stderr)
            time.sleep(CLEANUP_INTERVAL_SECONDS)
    else:
        run_once(do_vacuum="--vacuum" in args)

if __name__ == "__main__":
    main()
