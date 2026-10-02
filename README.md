# oi — pipeline de datos Hyperliquid

Daemon en Python (Termux / Android ARM64, sin root) que ingesta datos de
Hyperliquid en `hyperT-data/oidata.db` (SQLite WAL) y los sirve por HTTP para
un widget.

## Procesos

Supervisados por `supervisor.sh` (relanzamiento cada 60 s, rotación de logs
> 10 MB, `pgrep -fx` para detectar caídas sin falsos positivos):

| Script | Qué hace | Tablas |
|---|---|---|
| `oi_poller.py` | Snapshot de OI/mark/funding de `metaAndAssetCtxs` cada 5 s. **Filtra `COINS_SET`**: ya no graba monedas fuera de la lista. | `oi_snapshots` |
| `trades_listener.py` | WS `trades`: agrega buy/sell por minuto **y** detecta whales por umbral | `delta_buckets`, `whale_trades` |
| `funding_poller.py` | Backfill `fundingHistory` + loop 60 s con funding actual (de `metaAndAssetCtxs`) y `predictedFundings` por venue | `funding_snapshots` |
| `book_listener.py` | WS `l2Book` por moneda: cada 5 s vuelca 1 fila con best bid/ask, spread_bps e imbalance ±0,5 %/±2 % del mid | `book_snapshots` |
| `candles.py` | Backfill `candleSnapshot` + WS `candle` (upsert) para 1m, 5m, 15m, 1h | `candles` |
| `server.py` | HTTP `:8787` con los endpoints de abajo | — |
| `cleanup.py --daemon` | Retención por tabla, downsample OI, checkpoint WAL y VACUUM semanal. **Corre bajo supervisor cada 6 h.** | `oi_1m` (escritura) |

Config compartida en `config.py` (COINS, intervalos, umbrales, retenciones,
umbrales de aviso para `/health`). Conexión SQLite en `config.connect()`:
`WAL + busy_timeout 30 s + synchronous=NORMAL + wal_autocheckpoint=1000`.

## Endpoints

Base: `http://127.0.0.1:8787`

- `GET /health` — edad de la última escritura por tabla, `db_bytes`,
  `wal_bytes` y `warnings[]`. `status: ok` si todas las tablas frescas y sin
  warnings; `stale` en caso contrario. Warnings:
  - `DB > HEALTH_WARN_DB_BYTES` (1 GB por defecto)
  - `WAL > HEALTH_WARN_WAL_BYTES` (200 MB por defecto)
  - `oi_1m` usa un umbral de frescura amplio porque solo lo escribe el
    cleanup cada 6 h **y** su `MAX(minute_ms)` siempre va rezagado
    `OI_DOWNSAMPLE_AFTER_DAYS` días.
  - `funding_snapshots` usa `ingested_ms` (su `ts_ms` está snap a la hora).
- `GET /snapshot` — último registro por moneda de todas las fuentes en una
  sola respuesta (`oi`, `delta`, `book`, `funding`, `candles`).
- `GET /oi?coin=BTC&since_ms=…` — **auto-pivot**: si `since_ms` cae dentro
  de los últimos `OI_DOWNSAMPLE_AFTER_DAYS` días, lee solo `oi_snapshots`
  (res. 5 s); si es más antiguo, concatena `oi_1m` (hasta la frontera) +
  `oi_snapshots` (desde la frontera). Mismo formato de salida que antes.
- `GET /delta?coin=BTC&since_ms=…`
- `GET /funding?coin=BTC&since_ms=…`
- `GET /book?coin=BTC&since_ms=…`
- `GET /whales?since_ms=…[&coin=BTC]`
- `GET /candles?coin=BTC&interval=1m&since_ms=…`

## Umbrales de whales (USD notional)

Definidos en `config.WHALE_THRESHOLDS_USD`. Default `$25 000`. Overrides:

- BTC: `$250 000`
- ETH: `$150 000`
- SOL / HYPE / BNB / XRP: `$100 000`
- DOGE / LINK / NEAR / ADA / TAO / SUI / AAVE / UNI / ONDO / WLD / PAXG / XMR / ZEC / ENA: `$50 000`

Dedupe por `tid`. Guarda `buyer`/`seller` del campo `users` del trade.

## Retenciones y downsampling

```python
OI_DOWNSAMPLE_AFTER_DAYS = 3   # oi_snapshots → oi_1m al pasar esta edad

RETENTION_DAYS = {
    "oi_snapshots":    3,   # 5 s, cruda
    "oi_1m":          30,   # 1 fila/moneda/minuto (último valor)
    "delta_buckets":  30,
    "funding_snapshots": 90,
    "book_snapshots":  1,
    "whale_trades":   14,
}

CANDLE_RETENTION_DAYS = {"1m": 14, "5m": 60, "15m": 180, "1h": 180}
```

### Cómo trabaja `cleanup.py`

1. **Downsample OI**: filas de `oi_snapshots` con `ts_ms < now - 3d` se
   agregan en `oi_1m` (una por `coin`+minuto, último `ts_ms`) y se borran
   de la cruda (UPSERT para idempotencia).
2. **DELETE por retención** en lotes de `DELETE_BATCH_SIZE` (50 000) usando
   `WHERE … AND rowid IN (SELECT rowid … LIMIT N)` para no bloquear ni
   inflar el WAL.
3. **`wal_checkpoint(TRUNCATE)`** al final de cada ciclo.
4. **VACUUM** semanal controlado por `.last_vacuum`; o forzado con
   `python3 cleanup.py --vacuum`.
5. Loguea `borrados={...} checkpoint=... vacuum=bool DB antes->después
   WAL antes->después`.

Modos:

```bash
python3 cleanup.py            # un ciclo y salir
python3 cleanup.py --daemon   # bucle con CLEANUP_INTERVAL_SECONDS (6 h)
python3 cleanup.py --vacuum   # fuerza VACUUM al final del ciclo
```

El supervisor arranca `cleanup.py --daemon`, así que no hace falta cron.

## Operación

```bash
# arrancar todo
nohup bash supervisor.sh >> supervisor.log 2>&1 & disown

# ver estado
curl -s http://127.0.0.1:8787/health | python3 -m json.tool
pgrep -af "python3 (oi_poller|trades_listener|server|funding_poller|book_listener|candles|cleanup)"

# limpieza manual sin esperar al daemon
python3 cleanup.py
```

## Shrink one-shot: cómo se compactó la base

El estado antiguo: `oidata.db` ≈ 4,6 GB + WAL ≈ 4,6 GB porque el `oi_poller`
guardaba las ~230 monedas del universo (hoy filtradas a 30) y la retención
de OI era 30 d a 5 s. Para recuperar espacio hay un script one-shot:
`migrate_shrink.py`.

Procedimiento (todo parado, nada escribe):

1. `pkill -f supervisor.sh` y luego matar los pollers uno a uno.
2. `python3 migrate_shrink.py` — construye `oidata.db.new`:
   - checkpoint TRUNCATE sobre la base vieja,
   - esquema nuevo (incluye `oi_1m`),
   - `ATTACH DATABASE … AS old` y copia filtrada por `COINS` + retenciones
     nuevas (incluyendo downsample 3 d → 30 d en `oi_1m`),
   - checkpoint y swap atómico (`oidata.db` → `oidata.db.old`;
     `oidata.db.new` → `oidata.db`).
3. Verificar `/health`, borrar `oidata.db.old*` si todo bien.
4. Relanzar `supervisor.sh`.

Resultado medido: **9,1 GB → 228 MB** (DB) con todos los endpoints
sirviendo lo mismo.

## Tamaño en régimen estable (medido ~1 h de pollers)

| Tabla | Filas/día | Retención | Tamaño estimado |
|---|---|---|---|
| `oi_snapshots` | ~444 k | 3 d | ~107 MB |
| `oi_1m` | 43 200 | 30 d | ~104 MB |
| `delta_buckets` | ~41 k | 30 d | ~75 MB |
| `book_snapshots` | ~460 k | 1 d | ~92 MB |
| `funding_snapshots` | ~100 | 90 d | <1 MB |
| `whale_trades` | ~1 k | 14 d | ~3 MB |
| `candles` (todos los intervalos) | ~54 k | 14/60/180/180 d | ~160 MB |
| **Total (incl. índices ×1,4)** | | | **~750 MB – 1 GB** |

Es la frontera del warning `DB>1GB` en `/health`; si se cruza de forma
sostenida, revisar cadencia de `book_listener` o acortar su retención.

## Caveats

- **l2Book trae 20 niveles por lado**. En pares líquidos (BTC/ETH/…) esos
  20 niveles caben enteros dentro del ±0,5 % del mid, por lo que
  `bid_depth_05 == bid_depth_2` muy a menudo.
- **Backup**: un `sqlite3 ".backup …"` sobre la DB en caliente se atasca
  por el WAL creciente mientras hay escritores. Para una copia limpia:
  parar todos los scripts, `wal_checkpoint(TRUNCATE)`, `cp`.
- **No hay backup completo tras el shrink** — solo el esquema en
  `schema_backup.sql`. Fue decisión explícita (espacio en disco).
- Migración de esquema: `migrate.py` es idempotente
  (`CREATE TABLE IF NOT EXISTS`). `apply_schema(conn)` se reusa desde
  `migrate_shrink.py`.
