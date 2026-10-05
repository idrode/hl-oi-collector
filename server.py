#!/usr/bin/env python3
"""HTTP :8787. /oi usa oi_snapshots (5 s) para los últimos
OI_DOWNSAMPLE_AFTER_DAYS días y oi_1m para lo anterior, uniendo ambos
tramos si el rango lo cruza. /health incluye tamaño de DB y WAL."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import json
import os
import sqlite3
import time

from config import (
    CANDLE_INTERVALS,
    COINS,
    COINS_SET,
    DB_PATH,
    ENDPOINT_DEFAULT_LIMIT,
    ENDPOINT_DEFAULT_WINDOW_SECONDS,
    ENDPOINT_MAX_LIMIT,
    ENDPOINT_MAX_WINDOW_SECONDS,
    HEALTH_FRESH_SECONDS_DEFAULT,
    HEALTH_FRESH_SECONDS_OI_1M,
    HEALTH_STATUS_EXCLUDE_TABLES,
    HEALTH_WARN_DB_BYTES,
    HEALTH_WARN_WAL_BYTES,
    OI_DOWNSAMPLE_AFTER_DAYS,
    OI_RAW_WINDOW_GRACE_SECONDS,
    OI_RAW_WINDOW_SECONDS,
)

PORT = 8787

# ---------------------------------------------------------------------------
# CONTRATO DE ENDPOINTS
#
# Todo lo que sigue sale del codigo de este fichero y de config.py. Si cambia
# una constante, cambia tambien esta cabecera.
#
# --- Transporte ------------------------------------------------------------
# Solo GET; cualquier otro metodo lo rechaza BaseHTTPRequestHandler con 501.
# Toda respuesta, errores incluidos, lleva Content-Type: application/json,
# Content-Length y Access-Control-Allow-Origin: *.
# Ruta desconocida  -> 404 {"error": "ruta no encontrada"}
# Excepcion no vista -> 500 {"error": "<str(excepcion)>"}
#
# --- Dos formas de respuesta ----------------------------------------------
# ARRAY de objetos, orden ASCENDENTE de tiempo, con cabeceras de ventana:
#   /oi  /delta  /funding  /whales  /candles  /book
# OBJETO, sin ventana ni cabeceras:  /health  /snapshot  /book/last
# ARRAY de strings:                  /coins
#
# Los metadatos van en CABECERAS, nunca dentro del cuerpo, para no cambiar la
# forma de la respuesta:
#   X-Window-Start-Ms  SIEMPRE en los seis endpoints de array, incluso con 0
#                      filas. Es el ts de la primera fila devuelta, o el
#                      since_ms ya capado si no hay filas.
#   X-Truncated: true  SOLO si se recorto algo. Significa "la ventana que te
#                      sirvo es mas corta que la que pediste": o since_ms
#                      caia antes del maximo permitido, o el LIMIT dejo fuera
#                      filas mas antiguas que SI estaban en la ventana. El
#                      cliente que la vea y quiera mas historico debe repetir
#                      con since_ms mas reciente o limit mayor. El recorte es
#                      SILENCIOSO: siempre 200, nunca 400.
#   X-Resolution       solo /oi (5s | 1m) y /candles (1m | 5m | 15m | 1h).
#
# --- Ventana y LIMIT (_window, constantes en config.py) -------------------
#   - Falta since_ms -> now - ENDPOINT_DEFAULT_WINDOW_SECONDS[endpoint].
#   - since_ms anterior a now - ENDPOINT_MAX_WINDOW_SECONDS -> se capa a ese
#     inicio y se marca X-Truncated. since_ms=0 (o negativo, o cualquier
#     epoch viejo) lo dispara SIEMPRE: NUNCA se sirve el historico completo.
#     Es deliberado, no un limite de paginacion.
#   - since_ms en el futuro se acepta tal cual: [] y sin X-Truncated.
#   - Falta limit -> ENDPOINT_DEFAULT_LIMIT si el endpoint tiene entrada, y
#     si no, su ENDPOINT_MAX_LIMIT. Hoy solo /whales (100) y /book (1 000)
#     tienen un defecto distinto del maximo.
#   - limit mayor que el maximo se recorta en silencio; limit < 1 se sube a 1;
#     un limit menor que el maximo se respeta tal cual.
#   - Al capar se conservan las filas MAS RECIENTES (_fetch_newest), en el
#     mismo orden ascendente de siempre.
#   - since_ms y limit se parsean con int(): "abc", "1.5" y "" son invalidos.
#     /funding /whales /candles /book -> 400 {"error": "since_ms y limit
#     deben ser enteros"} (_window_or_400). /oi y /delta NO estan migrados y
#     sueltan 500 con el texto del ValueError: legado, pendiente del cambio 4.
#
#   endpoint        ventana def.  ventana max.  LIMIT def.  LIMIT max.
#   /oi                    6 h          30 d      60 000     60 000
#   /delta                 6 h          30 d      10 000     10 000
#   /funding               7 d          90 d      20 000     20 000
#   /whales               24 h          14 d         100     10 000
#   /book                  6 h           1 d       1 000     18 000
#   /candles 1m            6 h          14 d      21 000     21 000
#   /candles 5m           24 h          60 d      18 000     18 000
#   /candles 15m           3 d         180 d      18 000     18 000
#   /candles 1h           30 d         180 d       4 500      4 500
#   Cada ventana maxima es la retencion de la tabla que sirve el endpoint.
#   /candles se parametriza por INTERVALO con la clave "/candles:<interval>".
#
# --- /oi?coin=&since_ms=&limit= -------------------------------------------
# coin: string obligatorio. since_ms, limit: enteros opcionales.
# Falta coin -> 400 {"error": "falta ?coin=XXX"}. NO valida contra COINS: una
# moneda desconocida da 200 [] (legado, ver mas abajo).
# Campos de salida en los dos tramos: ts_ms, oi, oi_notional, mark_px,
# funding. No sale ningun id.
#   ventana <= OI_RAW_WINDOW_SECONDS + OI_RAW_WINDOW_GRACE_SECONDS
#   (72 h + 300 s) -> oi_snapshots crudo a 5 s. X-Resolution: 5s.
#   ventana mayor  -> una fila por minuto. X-Resolution: 1m, con ts_ms
#   SIEMPRE redondeado al minuto.
# Costura de los 3 dias: frontera = ((now - OI_DOWNSAMPLE_AFTER_DAYS * 86 400
# * 1000) // 60 000) * 60 000, es decir ALINEADA al minuto para que cada
# minuto caiga en UN solo tramo. Sin alinear, el minuto partido por la
# frontera saldria dos veces, una desde oi_1m y otra desde el agregado, justo
# tras una pasada de cleanup.
#   tramo >= frontera: oi_snapshots agregado con GROUP BY ts_ms / 60000 y
#     MAX(ts_ms), o sea los valores del ULTIMO tick del minuto; misma
#     semantica que cleanup.downsample_oi, asi que los dos tramos empalman sin
#     costura. El redondeo al minuto se hace en Python, no en SQL, para no
#     romper la semantica de columnas desnudas de MAX(). Los valores no se
#     tocan.
#   tramo <  frontera: oi_1m (minute_ms AS ts_ms). Solo se consulta si el
#     tramo reciente no agoto el LIMIT: al capar manda lo mas nuevo.
# oi_1m es SOLO historico, de 3 a 30 dias: nada lo escribe en vivo, lo rellena
# cleanup.py al pasar OI_DOWNSAMPLE_AFTER_DAYS, asi que NO sirve para ventanas
# recientes y una ventana corta nunca sale de ahi. Por eso la ventana maxima
# de /oi es 30 d (retencion de oi_1m) aunque oi_snapshots solo guarde 3 d.
# La resolucion la decide la ventana PEDIDA medida con el reloj del servidor,
# no los datos que haya en la base. Los 300 s de gracia existen porque el
# cliente calcula since_ms con SU reloj: una peticion de "justo 72 h" llega
# midiendo algo mas (latencia + desfase) y sin margen caeria del lado
# agregado. HyperT pide OI_LOOKBACK_MS = 72 h a 5 s y no sabe reintentar
# huecos.
#
# --- /delta?coin=&since_ms=&limit= ----------------------------------------
# Misma validacion legada de coin que /oi (falta -> 400 "falta ?coin=XXX";
# desconocida -> 200 []). Sin X-Resolution.
# delta_buckets, un bucket por minuto. Campos: minute_ms, buy_vol, sell_vol.
# "partial": true SOLO en el bucket del minuto EN CURSO, el que cumple
# minute_ms == (now_ms // 60 000) * 60 000, porque sigue recibiendo trades.
# Los minutos ya cerrados NO llevan el campo (ausente, no false), y solo la
# ultima fila puede llevarlo.
#
# --- /funding?coin=&since_ms=&limit= --------------------------------------
# coin obligatorio y DENTRO de COINS; si no -> 400 {"error": "?coin= ausente
# o fuera de COINS"}.
# funding_snapshots filtrada por ts_ms. Campos: ts_ms, venue, funding_rate,
# premium, next_funding_time, interval_hours.
# Orden (ts_ms, venue), reordenado en Python: ts_ms NO es unico, cada venue
# aporta una fila por instante (hoy 5 en la base: BinPerp, BybitPerp, Hl,
# HlHist, HlPerp). Por eso el corte por valor de _fetch_newest puede devolver
# hasta n_venues - 1 filas POR ENCIMA de limit (hoy hasta 4). Es el UNICO
# endpoint donde limit es aproximado.
#
# --- /whales?coin=&since_ms=&limit= ---------------------------------------
# coin OBLIGATORIO y dentro de COINS (mismo 400 que /funding): no hay modo
# "todas las monedas".
# whale_trades. Campos servidos: ts_ms, side, px, sz, notional.
# Omitidos A PROPOSITO: buyer, seller y tx_hash son direcciones y hashes de
# cadena y el servidor escucha en 0.0.0.0 sin autenticacion; coin es
# redundante porque va en la query; tid es nullable y su orden numerico no es
# temporal. id se usa solo para ordenar y se BORRA antes de serializar.
# Ningun campo id ni _id sale nunca por HTTP, en ningun endpoint.
# trades_listener.py sigue guardando los campos de cadena: si algun dia hacen
# falta, iran tras un parametro explicito, nunca por defecto.
# Orden (ts_ms, id) -- el PK autoincremental, NOT NULL y monotonico con la
# ingesta -- y recorte final a las limit mas recientes, asi que aqui limit se
# cumple EXACTO. Hace falta porque hasta 32 fills comparten ts_ms en una
# moneda (una orden grande barriendo el libro entra como muchos fills del
# mismo milisegundo).
# Umbral por moneda en config.WHALE_THRESHOLDS_USD (defecto 25 000 USD): sin
# fills por encima del umbral no hay filas, y los huecos de minutos son
# normales, no un fallo.
#
# --- /candles?coin=&interval=&since_ms=&limit= ----------------------------
# coin dentro de COINS -> si no, 400 {"error": "?coin= ausente o fuera de
# COINS"}. interval OBLIGATORIO y uno de 1m, 5m, 15m, 1h -> si no, 400
# {"error": "?interval= ausente o no es uno de 1m, 5m, 15m, 1h"}.
# Campos: t, close_t, o, h, l, c, v, n. X-Resolution: el interval pedido.
# Ventana y LIMIT por intervalo (tabla de arriba). Las ventanas por defecto
# son las mismas que candles.BACKFILL_WINDOW_MS: es lo que el ingestor
# garantiza relleno tras un arranque en frio.
# close_t es el cierre nominal INCLUSIVO: close_t - t == interval_ms - 1
# (comprobado sobre las 261 105 velas de la base). NO es el ts del ultimo
# trade de la vela.
# "partial": true en la vela ABIERTA, definida como close_t >= now_ms: la
# unica a la que candles.py le sigue haciendo upsert. Solo puede ser la ultima
# fila, porque t es unico dentro de (coin, interval) y las filas van en orden
# ascendente. Las cerradas no llevan el campo.
# Como t forma el PK con (coin, interval) no hay empates, asi que el corte por
# valor cumple limit EXACTO sin el recorte extra de /funding y /whales.
#
# --- /book?coin=&since_ms=&limit= -----------------------------------------
# coin dentro de COINS (mismo 400 que /funding). Sin X-Resolution.
# book_snapshots, una fila cada BOOK_FLUSH_SECONDS (5 s).
# Campos (BOOK_COLS): ts_ms, bid_px, bid_sz, ask_px, ask_sz, mid_px,
# spread_bps, bid_depth_05, ask_depth_05, bid_depth_2, ask_depth_2, imb_05,
# imb_2.
# El LIMIT por defecto es 1 000, NO el maximo de 18 000: 6 h son 3 959 filas,
# asi que la peticion por defecto sirve las 1 000 mas recientes (~83 min) con
# X-Truncated: true, y para la ventana entera hay que pedir ?limit= mayor.
# Antes no tenia ventana ni LIMIT y since_ms=0 servia 18 414 filas / 6,4 MB.
# DUPLICADOS, resueltos SIN tocar el esquema: ts_ms viene REPETIDO en la tabla
# porque book_snapshots no tiene UNIQUE y el listener reinserta la misma marca
# (2 646 grupos de 552 421 filas, 0,48 %, siempre en PARES y con payload
# IDENTICO). El endpoint deduplica con GROUP BY ts_ms y MAX(id) AS _id: por la
# semantica de columnas desnudas de SQLite salen las columnas de la fila del
# id mayor. Asi ts_ms es unico en la respuesta y limit se cumple EXACTO; sin
# ese GROUP BY el corte por valor desborda (con el empate de ADA, limit=4
# devolvia 5 filas). Usa idx_book_coin_ts sin b-tree temporal: +22 ms en la
# ventana maxima (158 -> 181 ms). _id se borra antes de serializar. Poner el
# UNIQUE en la tabla es Fase 2 (#8).
# ts_ms es la marca del EXCHANGE (book["time"]), no del telefono, asi que un
# age_s calculado por el cliente puede salir ligeramente NEGATIVO (decimas de
# segundo): el reloj del telefono va algo por detras. No asumir ts_ms <= now
# local ni age_s >= 0. Misma causa que el age_s negativo de /health.
#
# --- /book/last?coin= -----------------------------------------------------
# coin dentro de COINS (mismo 400 que /funding). Sin since_ms ni limit.
# Un OBJETO con la ultima fila de la moneda y los mismos campos que /book, o
# {} si no hay ninguna. Sin cabeceras de ventana ni X-Resolution.
# Un solo seek por idx_book_coin_ts (medido 0,014-0,2 ms). No necesita dedup:
# ORDER BY ts_ms DESC LIMIT 1 ya da una fila y los duplicados traen el mismo
# payload. Mismo caveat del ts_ms del exchange que /book.
#
# --- /coins ---------------------------------------------------------------
# Array de strings con config.COINS en ORDEN DE CONFIG, no alfabetico; hoy 30
# monedas. Sin parametros. Es la lista que validan /funding, /whales,
# /candles, /book y /book/last.
#
# --- /snapshot ------------------------------------------------------------
# OBJETO con la ultima fila por moneda, via seek en (coin, ts) en vez de un
# GROUP BY sobre la tabla entera. Sin parametros, sin ventana, sin cabeceras.
# Solo monedas de config.COINS, y omite las que no tienen datos. Ninguna lista
# lleva "partial", ni siquiera la vela abierta de candles. Claves:
#   oi       [{coin, ts_ms, oi, oi_notional, mark_px, funding}]
#   delta    [{coin, minute_ms, buy_vol, sell_vol}]
#   book     [{coin, ts_ms, bid_px, ask_px, mid_px, spread_bps, imb_05,
#             imb_2, bid_depth_2, ask_depth_2}]  SUBCONJUNTO de /book: no trae
#             bid_sz, ask_sz, bid_depth_05 ni ask_depth_05.
#   funding  [{coin, venue, ts_ms, funding_rate, next_funding_time,
#             interval_hours}]  sin premium, al contrario que /funding. Es el
#             unico que sigue con MAX() agrupado: el venue no esta en config,
#             asi que no hay lista por la que hacer seeks.
#   candles  [{coin, interval, t, close_t, o, h, l, c, v, n}] una por cada
#             (coin, interval).
#
# --- /health --------------------------------------------------------------
# OBJETO, sin parametros, y SIEMPRE 200, tambien en "stale":
#   {status, now_ms, db_bytes, wal_bytes, warnings: [...], tables: {...}}
# status es "ok" solo si TODAS las tablas que cuentan estan fresh y warnings
# esta vacio; en cualquier otro caso "stale". No hay un tercer valor.
# warnings: "DB>1073741824 bytes" si db_bytes > HEALTH_WARN_DB_BYTES (1 GB) y
# "WAL>209715200 bytes" si wal_bytes > HEALTH_WARN_WAL_BYTES (200 MB). Los dos
# tamaños salen 0 si el fichero no se puede medir.
# tables trae una entrada por tabla de TIME_COLS, con la columna de tiempo que
# mide la frescura: oi_snapshots ts_ms, oi_1m minute_ms, delta_buckets
# minute_ms, funding_snapshots ingested_ms (NO ts_ms: la frescura mide cuando
# lo escribimos nosotros, mientras que /funding filtra por el ts_ms del
# venue), book_snapshots ts_ms, whale_trades ts_ms, candles t.
#   con datos:  {last_write_ms, age_s, fresh, fresh_threshold_s,
#                counts_for_status}
#   tabla vacia:{last_write_ms: null, age_s: null, fresh: false,
#                counts_for_status}  OJO: sin fresh_threshold_s.
#   error:      {error: "<texto>"} y el status global pasa a "stale".
# fresh = age_s < fresh_threshold_s. El umbral es
# HEALTH_FRESH_SECONDS_DEFAULT = 300 s para todas menos oi_1m, que usa
# HEALTH_FRESH_SECONDS_OI_1M = OI_DOWNSAMPLE_AFTER_DAYS * 86 400 +
# CLEANUP_INTERVAL_SECONDS + 3 600 = 284 400 s (3 d + 6 h + 1 h), porque no se
# escribe en vivo: solo cleanup.py lo rellena, asi que su fila mas reciente
# envejece hasta esa cota.
# counts_for_status es false SOLO para whale_trades
# (HEALTH_STATUS_EXCLUDE_TABLES): lo escribe trades_listener por evento de
# mercado y sin ballenas no hay filas (huecos de hasta 900 s medidos), asi que
# no es señal de salud del daemon; delta_buckets lo escribe el MISMO proceso
# cada minuto y ya lo cubre. Se sigue reportando, pero no decide status.
# age_s puede salir ligeramente NEGATIVO (decimas de segundo): los ts_ms
# vienen del exchange y el reloj del telefono va algo por detras, asi que un
# cliente no debe asumir age_s >= 0.
#
# --- Legado: moneda desconocida -------------------------------------------
# /oi y /delta solo comprueban que ?coin= este PRESENTE. Con una moneda que no
# esta en COINS la consulta no encuentra filas y responden 200 [] con
# X-Window-Start-Ms. /funding, /whales, /candles, /book y /book/last SI
# validan contra COINS_SET y responden 400. La diferencia se mantiene a
# proposito: HyperT consume /oi y /delta y un 400 nuevo le romperia el
# backfill. Un cliente nuevo no debe intentar distinguir "moneda inexistente"
# de "moneda sin datos aun" mirando el codigo de estado.
#
# --- Nota de despliegue ---------------------------------------------------
# main() escucha en 0.0.0.0:8787, o sea TODAS las interfaces, y NO hay
# autenticacion ni rate limit: cualquiera en la misma red puede pedir
# cualquier endpoint. Access-Control-Allow-Origin: * lo abre ademas a
# cualquier origen de navegador. Por eso /whales no sirve buyer, seller ni
# tx_hash. Si esto sale del telefono, va detras de un proxy con auth.
# La base se abre en mode=ro y con una conexion por hilo (ThreadingHTTPServer,
# daemon_threads), asi que el servidor no puede escribir ni cambiar PRAGMAs de
# la base que usan los pollers.
# _handle_simple() es codigo muerto: do_GET no lo llama, queda de los /oi y
# /delta antiguos y no forma parte del contrato.
#
# --- Medido 2026-10-05: aporta algo la banda de +-2 % en /book? -----------
# Casi nada. book_listener.compute_row suma las DOS bandas recorriendo el
# MISMO array levels del l2Book, asi que cuando todos los niveles que manda el
# exchange caen dentro de +-0,5 % del mid resulta bid_depth_2 == bid_depth_05,
# ask_depth_2 == ask_depth_05 e imb_2 == imb_05, identicos bit a bit.
# Filas con alguna diferencia, sobre la retencion entera de 1 d
# (17 026 filas por moneda, consulta de solo lectura en mode=ro):
#   BTC     (liquida)    0 / 17 026
#   LINK    (media)      0 / 17 026
#   CASHCAT (iliquida)   3 / 17 026   = 0,018 %
# Acotado a la ultima hora (667 filas por moneda) las tres dan 0, que es lo
# que ya se habia visto en BTC.
# Las 3 filas de CASHCAT son justo el caso que justifica dejar la banda: en
# una de ellas el libro se vacio dentro de +-0,5 % (bid_depth_05 =
# ask_depth_05 = 0,0, con lo que imb_05 cae al 0.0 del denominador cero,
# indistinguible de un libro equilibrado) mientras +-2 % veia 104 388 /
# 21 927 USD e imb_2 = +0,65.
# Conclusion: en monedas liquidas las columnas _2 son redundantes y un cliente
# puede ignorarlas; en libros finos imb_2 es la UNICA señal cuando imb_05
# colapsa a 0.0. Se dejan como estan, porque mirando solo imb_05 no se puede
# distinguir "equilibrado" de "banda vacia".
# ---------------------------------------------------------------------------


def ro_connect():
    """Conexion de SOLO LECTURA sobre la base en WAL.

    ThreadingHTTPServer crea un hilo por peticion, asi que abrir aqui una
    conexion y cerrarla en do_GET da exactamente una conexion por hilo, nunca
    compartida (sqlite3 lo prohibe entre hilos por defecto).

    mode=ro impide que el servidor escriba o cambie PRAGMAs de la base que
    usan los pollers; sigue viendo los commits nuevos del WAL."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn

# /health: tabla -> columna de tiempo para medir frescura.
TIME_COLS = {
    "oi_snapshots": "ts_ms",
    "oi_1m": "minute_ms",
    "delta_buckets": "minute_ms",
    "funding_snapshots": "ingested_ms",
    "book_snapshots": "ts_ms",
    "whale_trades": "ts_ms",
    "candles": "t",
}

# Tablas con índice (coin, <col>): su MAX() global recorre el índice entero,
# mientras que un seek por moneda (ORDER BY col DESC LIMIT 1) toca 30 hojas.
SEEK_BY_COIN = frozenset((
    "oi_snapshots", "oi_1m", "delta_buckets", "book_snapshots", "whale_trades",
))
# funding_snapshots queda fuera: idx_funding_ingested ya resuelve MAX() en un
# seek. candles necesita (coin, interval) para usar su índice → caso aparte.

# Columnas que sirve /book. El id autoincremental queda fuera a proposito: se
# usa solo para deduplicar ts_ms repetidos y filtraria el volumen de ingesta.
BOOK_COLS = ("ts_ms, bid_px, bid_sz, ask_px, ask_sz, mid_px, spread_bps, "
             "bid_depth_05, ask_depth_05, bid_depth_2, ask_depth_2, "
             "imb_05, imb_2")

class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200, headers=None):
        body = json.dumps(data).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _rows_to_json(self, rows):
        return [dict(r) for r in rows]

    def _window(self, path, qs):
        """Resuelve (since_ms, limit, now_ms, capada) para un endpoint.

        Ver el contrato en la cabecera del fichero. int() puede lanzar
        ValueError con un parámetro basura; de momento lo recoge el except
        genérico de do_GET y sale un 500 (el 400 llega en el cambio #4)."""
        now_ms = int(time.time() * 1000)
        mas_viejo = now_ms - ENDPOINT_MAX_WINDOW_SECONDS[path] * 1000
        crudo = qs.get("since_ms", [None])[0]
        if crudo is None:
            since_ms = now_ms - ENDPOINT_DEFAULT_WINDOW_SECONDS[path] * 1000
            capada = False
        else:
            since_ms = int(crudo)
            capada = since_ms < mas_viejo
            if capada:
                since_ms = mas_viejo
        max_limit = ENDPOINT_MAX_LIMIT[path]
        lim = qs.get("limit", [None])[0]
        if lim is None:
            limit = ENDPOINT_DEFAULT_LIMIT.get(path, max_limit)
        else:
            limit = max(1, min(int(lim), max_limit))
        return since_ms, limit, now_ms, capada

    def _fetch_newest(self, conn, inner_sql, params, tcol, limit):
        """Las `limit` filas más recientes de inner_sql, en orden ASCENDENTE.

        Devuelve (filas, hay_mas). Primero sondea el valor de corte: la fila
        número `limit` contando desde la más reciente, pidiendo 2 para saber de
        paso si queda algo más allá del límite. Si la sonda sale vacía la
        ventana entra entera y se recorre el índice en orden ascendente sin
        ordenación extra, que es la ruta del backfill de HyperT: así cuesta lo
        mismo que la consulta de siempre (medido: 106 ms para 72 h de BTC,
        frente a 133 ms si se ordena al revés y se le da la vuelta)."""
        sonda = conn.execute(
            f"SELECT {tcol} AS v FROM ({inner_sql}) "
            f"ORDER BY {tcol} DESC LIMIT 2 OFFSET ?",
            (*params, limit - 1)).fetchall()
        if not sonda:
            rows = conn.execute(f"{inner_sql} ORDER BY {tcol} ASC", params).fetchall()
            return self._rows_to_json(rows), False
        rows = conn.execute(
            f"SELECT * FROM ({inner_sql}) WHERE {tcol} >= ? ORDER BY {tcol} ASC",
            (*params, sonda[0]["v"])).fetchall()
        return self._rows_to_json(rows), len(sonda) > 1

    def _win_headers(self, rows, tcol, since_ms, truncada):
        h = {"X-Window-Start-Ms": str(rows[0][tcol] if rows else since_ms)}
        if truncada:
            h["X-Truncated"] = "true"
        return h

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        conn = ro_connect()
        try:
            path = parsed.path
            if path == "/health":
                self._send_json(self._health(conn))
                return
            if path == "/coins":
                self._send_json(list(COINS))
                return
            if path == "/snapshot":
                self._send_json(self._snapshot(conn))
                return
            if path == "/oi":
                self._handle_oi(conn, qs)
                return
            if path == "/delta":
                self._handle_delta(conn, qs)
                return
            if path == "/funding":
                self._handle_funding(conn, qs)
                return
            if path == "/book":
                self._handle_book(conn, qs)
                return
            if path == "/book/last":
                self._handle_book_last(conn, qs)
                return
            if path == "/whales":
                self._handle_whales(conn, qs)
                return
            if path == "/candles":
                self._handle_candles(conn, qs)
                return
            self._send_json({"error": "ruta no encontrada"}, 404)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)
        finally:
            conn.close()

    def _window_or_400(self, path, qs):
        """Como _window, pero contesta 400 y devuelve None si since_ms o limit
        no son enteros. Solo lo usan los endpoints nuevos: /oi y /delta siguen
        dando 500 hasta el cambio #4."""
        try:
            return self._window(path, qs)
        except ValueError:
            self._send_json({"error": "since_ms y limit deben ser enteros"}, 400)
            return None

    def _handle_funding(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if coin not in COINS_SET:
            self._send_json({"error": "?coin= ausente o fuera de COINS"}, 400)
            return
        v = self._window_or_400("/funding", qs)
        if v is None:
            return
        since_ms, limit, _now_ms, capada = v
        rows, hay_mas = self._fetch_newest(conn,
            "SELECT ts_ms, venue, funding_rate, premium, next_funding_time, "
            "interval_hours FROM funding_snapshots WHERE coin = ? AND ts_ms >= ?",
            (coin, since_ms), "ts_ms", limit)
        # ts_ms no es único aquí: cada venue aporta una fila por instante. El
        # corte por valor de _fetch_newest puede devolver hasta n_venues - 1
        # filas por encima de limit, y deja los empates en orden arbitrario;
        # se reordena por (ts_ms, venue) para que la serie sea estable.
        rows.sort(key=lambda r: (r["ts_ms"], r["venue"]))
        self._send_json(rows, headers=self._win_headers(
            rows, "ts_ms", since_ms, capada or hay_mas))

    def _handle_whales(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if coin not in COINS_SET:
            self._send_json({"error": "?coin= ausente o fuera de COINS"}, 400)
            return
        v = self._window_or_400("/whales", qs)
        if v is None:
            return
        since_ms, limit, _now_ms, capada = v
        # buyer, seller y tx_hash NO se sirven: son direcciones y hashes de
        # cadena, y el servidor escucha en 0.0.0.0. trades_listener.py los
        # sigue guardando; si algun dia hacen falta, van tras un parametro
        # explicito, nunca por defecto.
        rows, hay_mas = self._fetch_newest(conn,
            "SELECT id, ts_ms, side, px, sz, notional "
            "FROM whale_trades WHERE coin = ? AND ts_ms >= ?",
            (coin, since_ms), "ts_ms", limit)
        # Hasta 32 fills comparten ts_ms en una misma moneda (medido en
        # audit.db: una orden grande barriendo el libro entra como muchos
        # fills del mismo milisegundo), asi que el corte por valor de
        # _fetch_newest puede desbordar limit de sobra. Se ordena por
        # (ts_ms, id) -- el PK autoincremental, NOT NULL y monotonico con la
        # ingesta; tid no sirve porque es nullable y su orden numerico no es
        # temporal -- y se recortan las limit mas recientes, asi que aqui
        # limit se cumple EXACTO, al contrario que en /funding.
        rows.sort(key=lambda r: (r["ts_ms"], r["id"]))
        if len(rows) > limit:
            rows = rows[-limit:]
            hay_mas = True
        for r in rows:
            del r["id"]
        self._send_json(rows, headers=self._win_headers(
            rows, "ts_ms", since_ms, capada or hay_mas))

    def _handle_candles(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if coin not in COINS_SET:
            self._send_json({"error": "?coin= ausente o fuera de COINS"}, 400)
            return
        interval = qs.get("interval", [None])[0]
        if interval not in CANDLE_INTERVALS:
            self._send_json({"error": "?interval= ausente o no es uno de "
                                      + ", ".join(CANDLE_INTERVALS)}, 400)
            return
        v = self._window_or_400(f"/candles:{interval}", qs)
        if v is None:
            return
        since_ms, limit, now_ms, capada = v
        rows, hay_mas = self._fetch_newest(conn,
            "SELECT t, close_t, o, h, l, c, v, n FROM candles "
            "WHERE coin = ? AND interval = ? AND t >= ?",
            (coin, interval, since_ms), "t", limit)
        # t forma el PK con (coin, interval): sin empates, el corte por valor
        # de _fetch_newest cumple limit EXACTO (comprobado: limit=5000 devuelve
        # 5000). No hace falta el recorte extra de /funding y /whales.
        if rows and rows[-1]["close_t"] >= now_ms:
            rows[-1]["partial"] = True
        self._send_json(rows, headers={
            **self._win_headers(rows, "t", since_ms, capada or hay_mas),
            "X-Resolution": interval,
        })

    def _handle_book(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if coin not in COINS_SET:
            self._send_json({"error": "?coin= ausente o fuera de COINS"}, 400)
            return
        v = self._window_or_400("/book", qs)
        if v is None:
            return
        since_ms, limit, _now_ms, capada = v
        # GROUP BY ts_ms deduplica las reinserciones del listener quedandose
        # con la fila de id mayor: con un agregado MAX() SQLite saca las
        # columnas desnudas de la fila del maximo. Usa idx_book_coin_ts sin
        # b-tree temporal, +22 ms en la ventana maxima (158 -> 181 ms).
        rows, hay_mas = self._fetch_newest(conn,
            f"SELECT {BOOK_COLS}, MAX(id) AS _id FROM book_snapshots "
            "WHERE coin = ? AND ts_ms >= ? GROUP BY ts_ms",
            (coin, since_ms), "ts_ms", limit)
        # Deduplicado, ts_ms es unico dentro de la moneda, asi que el corte por
        # valor de _fetch_newest cumple limit EXACTO y no hace falta el recorte
        # extra de /funding y /whales.
        for r in rows:
            del r["_id"]
        self._send_json(rows, headers=self._win_headers(
            rows, "ts_ms", since_ms, capada or hay_mas))

    def _handle_book_last(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if coin not in COINS_SET:
            self._send_json({"error": "?coin= ausente o fuera de COINS"}, 400)
            return
        row = conn.execute(
            f"SELECT {BOOK_COLS} FROM book_snapshots WHERE coin = ? "
            "ORDER BY ts_ms DESC LIMIT 1", (coin,)).fetchone()
        self._send_json(dict(row) if row is not None else {})

    def _handle_simple(self, conn, qs, sql):
        coin = qs.get("coin", [None])[0]
        since_ms = int(qs.get("since_ms", [0])[0])
        if not coin:
            self._send_json({"error": "falta ?coin=XXX"}, 400)
            return
        rows = conn.execute(sql, (coin, since_ms)).fetchall()
        self._send_json(self._rows_to_json(rows))

    def _handle_delta(self, conn, qs):
        coin = qs.get("coin", [None])[0]
        if not coin:
            self._send_json({"error": "falta ?coin=XXX"}, 400)
            return
        since_ms, limit, now_ms, capada = self._window("/delta", qs)
        rows, hay_mas = self._fetch_newest(conn,
            "SELECT minute_ms, buy_vol, sell_vol FROM delta_buckets "
            "WHERE coin = ? AND minute_ms >= ?",
            (coin, since_ms), "minute_ms", limit)
        # El bucket del minuto en curso sigue recibiendo trades: se marca para
        # que el cliente no lo trate como un minuto ya cerrado.
        if rows and rows[-1]["minute_ms"] == (now_ms // 60_000) * 60_000:
            rows[-1]["partial"] = True
        self._send_json(rows, headers=self._win_headers(
            rows, "minute_ms", since_ms, capada or hay_mas))

    def _handle_oi(self, conn, qs):
        """Ventana corta → oi_snapshots a 5 s. Ventana larga → una fila por
        minuto, agregando oi_snapshots y tirando de oi_1m para el tramo que ya
        pasó por el downsample. Ver el contrato en la cabecera del fichero."""
        coin = qs.get("coin", [None])[0]
        if not coin:
            self._send_json({"error": "falta ?coin=XXX"}, 400)
            return
        since_ms, limit, now_ms, capada = self._window("/oi", qs)
        ventana_s = (now_ms - since_ms) / 1000.0
        if ventana_s <= OI_RAW_WINDOW_SECONDS + OI_RAW_WINDOW_GRACE_SECONDS:
            rows, hay_mas = self._fetch_newest(conn,
                "SELECT ts_ms, oi, oi_notional, mark_px, funding "
                "FROM oi_snapshots WHERE coin = ? AND ts_ms >= ?",
                (coin, since_ms), "ts_ms", limit)
            resolucion = "5s"
        else:
            resolucion = "1m"
            # Frontera alineada al minuto: así cada minuto cae en UN solo tramo.
            # Sin alinear, un minuto partido por la frontera saldría dos veces,
            # una desde oi_1m y otra desde el agregado, justo después de una
            # pasada de cleanup (cuando el máximo de oi_1m alcanza la frontera).
            frontera = (((now_ms - OI_DOWNSAMPLE_AFTER_DAYS * 86_400_000)
                         // 60_000) * 60_000)
            rows, hay_mas = self._fetch_newest(conn,
                "SELECT MAX(ts_ms) AS ts_ms, oi, oi_notional, mark_px, funding "
                "FROM oi_snapshots WHERE coin = ? AND ts_ms >= ? "
                "GROUP BY ts_ms / 60000",
                (coin, max(since_ms, frontera)), "ts_ms", limit)
            # El agregado trae el ts del último tick del minuto; se redondea al
            # minuto para que la serie salga alineada igual que las filas de
            # oi_1m (mismo criterio que cleanup.downsample_oi, que guarda el
            # minuto redondo con los valores de ese último tick). Los valores no
            # se tocan. En SQL no se hace para no romper la semántica de
            # columnas desnudas de MAX(), ya verificada contra cleanup.py.
            for r in rows:
                r["ts_ms"] = (r["ts_ms"] // 60_000) * 60_000
            # Solo se baja a oi_1m si el tramo reciente no agotó ya el LIMIT:
            # al capar manda lo más nuevo.
            if since_ms < frontera and not hay_mas and len(rows) < limit:
                viejas, hay_mas = self._fetch_newest(conn,
                    "SELECT minute_ms AS ts_ms, oi, oi_notional, mark_px, funding "
                    "FROM oi_1m WHERE coin = ? AND minute_ms >= ? AND minute_ms < ?",
                    (coin, since_ms, frontera), "ts_ms", limit - len(rows))
                rows = viejas + rows
        h = self._win_headers(rows, "ts_ms", since_ms, capada or hay_mas)
        h["X-Resolution"] = resolucion
        self._send_json(rows, headers=h)

    def _max_by_coin(self, conn, table, tcol):
        """MAX(tcol) como un seek por moneda sobre el índice (coin, tcol).
        Solo mira las monedas de config.COINS."""
        sql = (f"SELECT {tcol} AS v FROM {table} WHERE coin = ? "
               f"ORDER BY {tcol} DESC LIMIT 1")
        best = None
        for coin in COINS:
            row = conn.execute(sql, (coin,)).fetchone()
            if row is not None and row["v"] is not None:
                if best is None or row["v"] > best:
                    best = row["v"]
        return best

    def _max_candles(self, conn):
        best = None
        for coin in COINS:
            for interval in CANDLE_INTERVALS:
                row = conn.execute(
                    "SELECT t FROM candles WHERE coin = ? AND interval = ? "
                    "ORDER BY t DESC LIMIT 1", (coin, interval)).fetchone()
                if row is not None and (best is None or row["t"] > best):
                    best = row["t"]
        return best

    def _latest_per_coin(self, conn, sql):
        """Una fila por moneda de config.COINS; omite las que no tienen datos."""
        out = []
        for coin in COINS:
            row = conn.execute(sql, (coin,)).fetchone()
            if row is not None:
                out.append(dict(row))
        return out

    def _health(self, conn):
        now_ms = int(time.time() * 1000)
        tables = {}
        all_fresh = True
        for t, tcol in TIME_COLS.items():
            try:
                if t in SEEK_BY_COIN:
                    mx = self._max_by_coin(conn, t, tcol)
                elif t == "candles":
                    mx = self._max_candles(conn)
                else:
                    row = conn.execute(f"SELECT MAX({tcol}) AS mx FROM {t}").fetchone()
                    mx = row["mx"] if row else None
                counts = t not in HEALTH_STATUS_EXCLUDE_TABLES
                if mx is None:
                    tables[t] = {"last_write_ms": None, "age_s": None,
                                 "fresh": False, "counts_for_status": counts}
                    if counts:
                        all_fresh = False
                else:
                    age = (now_ms - int(mx)) / 1000.0
                    thresh = (HEALTH_FRESH_SECONDS_OI_1M if t == "oi_1m"
                              else HEALTH_FRESH_SECONDS_DEFAULT)
                    fresh = age < thresh
                    tables[t] = {"last_write_ms": int(mx),
                                 "age_s": round(age, 1), "fresh": fresh,
                                 "fresh_threshold_s": thresh,
                                 "counts_for_status": counts}
                    if not fresh and counts:
                        all_fresh = False
            except Exception as e:
                tables[t] = {"error": str(e)}
                all_fresh = False

        def sz(p):
            try: return os.path.getsize(p)
            except OSError: return 0
        db_bytes = sz(DB_PATH)
        wal_bytes = sz(DB_PATH + "-wal")
        warnings = []
        if db_bytes > HEALTH_WARN_DB_BYTES:
            warnings.append(f"DB>{HEALTH_WARN_DB_BYTES} bytes")
        if wal_bytes > HEALTH_WARN_WAL_BYTES:
            warnings.append(f"WAL>{HEALTH_WARN_WAL_BYTES} bytes")

        status = "ok" if all_fresh and not warnings else "stale"
        return {
            "status": status,
            "now_ms": now_ms,
            "db_bytes": db_bytes,
            "wal_bytes": wal_bytes,
            "warnings": warnings,
            "tables": tables,
        }

    def _snapshot(self, conn):
        """Última fila por moneda vía seek en (coin, ts_ms), no GROUP BY sobre
        la tabla entera. Devuelve exactamente una fila por moneda y solo las de
        config.COINS (el JOIN anterior duplicaba filas si empataba el ts_ms)."""
        out = {}
        out["oi"] = self._latest_per_coin(conn,
            "SELECT coin, ts_ms, oi, oi_notional, mark_px, funding "
            "FROM oi_snapshots WHERE coin = ? ORDER BY ts_ms DESC LIMIT 1")
        out["delta"] = self._latest_per_coin(conn,
            "SELECT coin, minute_ms, buy_vol, sell_vol "
            "FROM delta_buckets WHERE coin = ? ORDER BY minute_ms DESC LIMIT 1")
        out["book"] = self._latest_per_coin(conn,
            "SELECT coin, ts_ms, bid_px, ask_px, mid_px, spread_bps, "
            "       imb_05, imb_2, bid_depth_2, ask_depth_2 "
            "FROM book_snapshots WHERE coin = ? ORDER BY ts_ms DESC LIMIT 1")
        # funding_snapshots mantiene el MAX() global agrupado: el venue no está
        # en config, así que no hay lista por la que hacer seeks.
        out["funding"] = self._rows_to_json(conn.execute(
            "SELECT f.coin, f.venue, f.ts_ms, f.funding_rate, f.next_funding_time, f.interval_hours "
            "FROM funding_snapshots f JOIN ("
            "  SELECT coin, venue, MAX(ts_ms) AS mx FROM funding_snapshots GROUP BY coin, venue"
            ") m ON f.coin = m.coin AND f.venue = m.venue AND f.ts_ms = m.mx").fetchall())
        candles = []
        for coin in COINS:
            for interval in CANDLE_INTERVALS:
                row = conn.execute(
                    "SELECT coin, interval, t, close_t, o, h, l, c, v, n "
                    "FROM candles WHERE coin = ? AND interval = ? "
                    "ORDER BY t DESC LIMIT 1", (coin, interval)).fetchone()
                if row is not None:
                    candles.append(dict(row))
        out["candles"] = candles
        return out

    def log_message(self, format, *args):
        pass

class Server(ThreadingHTTPServer):
    daemon_threads = True   # no bloquear el cierre por peticiones en vuelo


def main():
    server = Server(("0.0.0.0", PORT), Handler)
    print(f"Servidor escuchando en el puerto {PORT} (multihilo, solo lectura)")
    server.serve_forever()

if __name__ == "__main__":
    main()
