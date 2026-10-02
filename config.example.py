# Copia este archivo a config.py y ajusta los valores
#!/usr/bin/env python3
"""Config compartido por todos los scripts del pipeline Hyperliquid."""
import sqlite3

DB_PATH = "hyperT-data/oidata.db"
API_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"

COINS = [
    "BTC", "ETH", "HYPE", "SOL", "ZEC", "XRP", "PUMP", "LIT", "XMR", "AAVE",
    "LINK", "NEAR", "PAXG", "DOGE", "BNB", "FARTCOIN", "TAO", "ADA", "ZRO", "MON",
    "SUI", "WLD", "ENA", "XPL", "UNI", "VVV", "GRAM", "CASHCAT", "ONDO", "ASTER",
]
COINS_SET = frozenset(COINS)

CANDLE_INTERVALS = ["1m", "5m", "15m", "1h"]

WHALE_THRESHOLD_DEFAULT_USD = 25_000
WHALE_THRESHOLDS_USD = {
    "BTC": 250_000,
    "ETH": 150_000,
    "SOL": 100_000, "HYPE": 100_000, "BNB": 100_000, "XRP": 100_000,
    "DOGE": 50_000, "LINK": 50_000, "NEAR": 50_000, "ADA": 50_000,
    "TAO": 50_000, "SUI": 50_000, "AAVE": 50_000, "UNI": 50_000,
    "ONDO": 50_000, "WLD": 50_000, "PAXG": 50_000, "XMR": 50_000,
    "ZEC": 50_000, "ENA": 50_000,
}

def whale_threshold(coin: str) -> float:
    return WHALE_THRESHOLDS_USD.get(coin, WHALE_THRESHOLD_DEFAULT_USD)

# Retención por tabla (días). oi_snapshots mantiene 5 s de resolución solo
# 3 días; a partir de ahí se agrega a oi_1m (una fila por coin y minuto).
RETENTION_DAYS = {
    "oi_snapshots": 3,
    "oi_1m": 30,
    "delta_buckets": 30,
    "funding_snapshots": 90,
    "book_snapshots": 1,
    "whale_trades": 14,
}

# Candles tienen retención por intervalo.
CANDLE_RETENTION_DAYS = {
    "1m": 14,
    "5m": 60,
    "15m": 180,
    "1h": 180,
}

OI_DOWNSAMPLE_AFTER_DAYS = RETENTION_DAYS["oi_snapshots"]

BOOK_FLUSH_SECONDS = 5
FUNDING_POLL_SECONDS = 60
BOOK_DEPTH_BAND_SMALL = 0.005   # ±0.5 % del mid
BOOK_DEPTH_BAND_BIG = 0.02      # ±2 % del mid

# Cleanup diario / semanal.
CLEANUP_INTERVAL_SECONDS = 6 * 3600
VACUUM_INTERVAL_SECONDS = 7 * 86400
DELETE_BATCH_SIZE = 50_000

# Umbrales de /health.
HEALTH_WARN_DB_BYTES = 1 * 1024 ** 3        # 1 GB
HEALTH_WARN_WAL_BYTES = 200 * 1024 ** 2     # 200 MB

# Frescura por tabla (segundos). Un único umbral de 300 s daba falsos "stale":
# no todas las tablas se escriben con la misma cadencia.
# 300 s cubre con holgura oi_snapshots y book_snapshots (5 s), delta_buckets
# y candles (1 min) y funding_snapshots (FUNDING_POLL_SECONDS = 60 s).
HEALTH_FRESH_SECONDS_DEFAULT = 300

# oi_1m NO se escribe en vivo: solo lo rellena cleanup.py, que mueve ahí lo
# anterior a OI_DOWNSAMPLE_AFTER_DAYS. Entre dos pasadas de limpieza su fila
# más reciente envejece hasta OI_DOWNSAMPLE_AFTER_DAYS + un ciclo completo,
# así que el umbral es esa cota más 1 h de margen para la propia pasada
# (que puede incluir un VACUUM). Hoy: 3 d + 6 h + 1 h = 284 400 s.
HEALTH_FRESH_SECONDS_OI_1M = (
    OI_DOWNSAMPLE_AFTER_DAYS * 86_400 + CLEANUP_INTERVAL_SECONDS + 3_600
)

# whale_trades lo escribe trades_listener por evento de mercado: sin ballenas
# no hay filas (huecos observados de hasta 900 s). No es señal de salud del
# daemon, porque delta_buckets lo escribe el MISMO proceso cada minuto y ya
# lo cubre. Se sigue reportando en /health, pero no decide "status".
HEALTH_STATUS_EXCLUDE_TABLES = frozenset(("whale_trades",))

# --- Límites de los endpoints HTTP. Ventanas en segundos, LIMIT en filas. ---
# Se aplican cuando falta el parámetro: sin since_ms se usa la ventana por
# defecto, y sin limit se usa el máximo del endpoint. since_ms=0 o anterior a
# la ventana máxima se capa al inicio permitido, nunca sirve el histórico
# completo. El capado conserva SIEMPRE las filas más recientes.
ENDPOINT_DEFAULT_WINDOW_SECONDS = {"/oi": 6 * 3600, "/delta": 6 * 3600}

# /oi: 30 d = RETENTION_DAYS["oi_1m"]. /delta: 30 d = RETENTION_DAYS["delta_buckets"].
ENDPOINT_MAX_WINDOW_SECONDS = {"/oi": 30 * 86_400, "/delta": 30 * 86_400}

# El máximo de /oi queda por encima de las ~44 000 filas que son 72 h a 5 s,
# porque HyperT pide esa ventana entera en su backfill (su app.rs: OI_LOOKBACK_MS
# = 72 h) y no sabe reintentar lo que le falte. /delta a 48 h, lo que pide ese
# mismo cliente, son ~2 900 filas, muy por debajo de 10 000.
ENDPOINT_MAX_LIMIT = {"/oi": 60_000, "/delta": 10_000}

# Hasta esta ventana /oi sirve los 5 s crudos de oi_snapshots; por encima, una
# fila por minuto. 72 h crudas son 43 796 filas y ~4 MB de JSON por moneda,
# pero es lo que HyperT necesita a 5 s.
OI_RAW_WINDOW_SECONDS = 72 * 3600

# Margen al comparar la ventana pedida con OI_RAW_WINDOW_SECONDS. El cliente
# calcula since_ms con SU reloj y el servidor mide la ventana con el suyo al
# recibir la peticion, asi que una peticion de "justo 72 h" llega midiendo algo
# MAS de 72 h (latencia + desfase de relojes; en esta maquina se ven decimas de
# segundo hasta contra el exchange). Sin este margen esa peticion caeria del
# lado agregado y HyperT perderia la resolucion de 5 s que pide.
OI_RAW_WINDOW_GRACE_SECONDS = 300

# Checkpoint WAL cada N páginas (por defecto SQLite usa 1000).
WAL_AUTOCHECKPOINT_PAGES = 1000

def connect(timeout: int = 30) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES};")
    return conn
