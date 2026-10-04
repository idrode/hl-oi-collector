-- Esquema de hyperT-data/oidata.db. Volcado con `.schema` (solo estructura,
-- sin datos) desde una conexion mode=ro. NO lo ejecutes contra la base en
-- produccion: el esquema real lo crea migrate.py, que es idempotente y es la
-- unica fuente de verdad (su DDL coincide objeto a objeto con la base viva).
-- Este fichero es documentacion; regeneralo tras cualquier cambio de esquema.
--
-- I1 - INDICES REDUNDANTES (solo anotado, no se toca nada aqui). Tres
-- CREATE INDEX duplican exactamente el indice implicito que SQLite ya crea
-- para un PRIMARY KEY o un UNIQUE compuesto, asi que ocupan espacio y
-- encarecen cada INSERT sin aportar ningun plan de consulta nuevo:
--   idx_candles_coin_int_t  (coin, interval, t)   == sqlite_autoindex_candles_1 (PRIMARY KEY)
--   idx_delta_coin_min      (coin, minute_ms)     == sqlite_autoindex_delta_buckets_1 (UNIQUE)
--   idx_oi1m_coin_min       (coin, minute_ms)     == sqlite_autoindex_oi_1m_1 (PRIMARY KEY)
-- No son redundantes, y deben quedarse: idx_oi_coin_ts, idx_book_coin_ts,
-- idx_whale_coin_ts, idx_funding_ingested y idx_funding_coin_ts (este ultimo
-- NO es prefijo del unico (coin, venue, ts_ms), asi que hace falta).
--
CREATE TABLE oi_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        ts_ms INTEGER NOT NULL,
        oi REAL NOT NULL,
        oi_notional REAL NOT NULL,
        mark_px REAL NOT NULL,
        funding REAL
    );
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE oi_1m (
        coin TEXT NOT NULL,
        minute_ms INTEGER NOT NULL,
        oi REAL NOT NULL,
        oi_notional REAL NOT NULL,
        mark_px REAL NOT NULL,
        funding REAL,
        PRIMARY KEY(coin, minute_ms)
    );
CREATE TABLE delta_buckets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        minute_ms INTEGER NOT NULL,
        buy_vol REAL NOT NULL DEFAULT 0,
        sell_vol REAL NOT NULL DEFAULT 0,
        UNIQUE(coin, minute_ms)
    );
CREATE TABLE funding_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        venue TEXT NOT NULL,
        ts_ms INTEGER NOT NULL,
        funding_rate REAL NOT NULL,
        premium REAL,
        next_funding_time INTEGER,
        interval_hours INTEGER,
        ingested_ms INTEGER NOT NULL DEFAULT 0,
        UNIQUE(coin, venue, ts_ms)
    );
CREATE TABLE book_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        ts_ms INTEGER NOT NULL,
        bid_px REAL NOT NULL,
        bid_sz REAL NOT NULL,
        ask_px REAL NOT NULL,
        ask_sz REAL NOT NULL,
        mid_px REAL NOT NULL,
        spread_bps REAL NOT NULL,
        bid_depth_05 REAL NOT NULL,
        ask_depth_05 REAL NOT NULL,
        bid_depth_2 REAL NOT NULL,
        ask_depth_2 REAL NOT NULL,
        imb_05 REAL NOT NULL,
        imb_2 REAL NOT NULL
    );
CREATE TABLE whale_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        ts_ms INTEGER NOT NULL,
        side TEXT NOT NULL,
        px REAL NOT NULL,
        sz REAL NOT NULL,
        notional REAL NOT NULL,
        buyer TEXT,
        seller TEXT,
        tx_hash TEXT,
        tid INTEGER UNIQUE
    );
CREATE TABLE candles (
        coin TEXT NOT NULL,
        interval TEXT NOT NULL,
        t INTEGER NOT NULL,
        close_t INTEGER NOT NULL,
        o REAL NOT NULL,
        h REAL NOT NULL,
        l REAL NOT NULL,
        c REAL NOT NULL,
        v REAL NOT NULL,
        n INTEGER NOT NULL,
        PRIMARY KEY(coin, interval, t)
    );
CREATE INDEX idx_oi_coin_ts ON oi_snapshots(coin, ts_ms);
CREATE INDEX idx_oi1m_coin_min ON oi_1m(coin, minute_ms);
CREATE INDEX idx_delta_coin_min ON delta_buckets(coin, minute_ms);
CREATE INDEX idx_funding_coin_ts ON funding_snapshots(coin, ts_ms);
CREATE INDEX idx_funding_ingested ON funding_snapshots(ingested_ms);
CREATE INDEX idx_book_coin_ts ON book_snapshots(coin, ts_ms);
CREATE INDEX idx_whale_coin_ts ON whale_trades(coin, ts_ms);
CREATE INDEX idx_candles_coin_int_t ON candles(coin, interval, t);
