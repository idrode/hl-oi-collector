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
# Todos los endpoints de serie temporal devuelven un ARRAY JSON de objetos en
# orden ascendente de tiempo. Los metadatos van en CABECERAS, nunca dentro del
# cuerpo, para no cambiar la forma de la respuesta:
#   X-Window-Start-Ms  inicio real de la ventana servida: el ts de la primera
#                      fila devuelta, o el since_ms ya capado si no hay filas.
#   X-Truncated: true  presente solo si se recortó algo, por ventana o por
#                      LIMIT. El recorte es SILENCIOSO: siempre 200, nunca 400.
#
# Reglas comunes (constantes en config.py):
#   - Falta since_ms  → se usa ENDPOINT_DEFAULT_WINDOW_SECONDS del endpoint.
#   - since_ms=0 o anterior a ENDPOINT_MAX_WINDOW_SECONDS → se capa al inicio
#     permitido. Nunca sirve el histórico completo.
#   - Falta limit     → se usa ENDPOINT_MAX_LIMIT del endpoint. Un limit mayor
#     que ese máximo se recorta en silencio; menor se respeta tal cual.
#   - Al capar se conservan las filas MÁS RECIENTES, en el mismo orden
#     ascendente de siempre.
#
# /oi?coin=&since_ms=&limit=       por defecto 6 h   máx 30 d   LIMIT 60 000
#     ventana ≤ 72 h → oi_snapshots, resolución 5 s (es lo que pide el backfill
#                      de HyperT, que no sabe reintentar lo que le falte).
#     ventana > 72 h → una fila por minuto: oi_snapshots agregado al último
#                      valor del minuto para el tramo reciente, y oi_1m para el
#                      anterior a OI_DOWNSAMPLE_AFTER_DAYS (3 d). Misma
#                      semántica que cleanup.downsample_oi, así que los dos
#                      tramos empalman sin costura.
#     oi_1m es SOLO histórico: nada lo escribe en vivo, lo rellena cleanup.py
#     al pasar los 3 d, así que las ventanas cortas nunca salen de ahí.
#     Cabecera extra X-Resolution: 5s | 1m.
#
# /delta?coin=&since_ms=&limit=    por defecto 6 h   máx 30 d   LIMIT 10 000
#     delta_buckets, un bucket por minuto. El bucket del minuto EN CURSO lleva
#     "partial": true porque sigue recibiendo trades; los minutos ya cerrados
#     no llevan el campo.
#
# /candles?coin=&interval=&since_ms=&limit=
#     Ventana, máximo y LIMIT POR INTERVALO (config.CANDLE_*): 1m 6 h/14 d,
#     5m 24 h/60 d, 15m 3 d/180 d, 1h 30 d/180 d. Cabecera extra
#     X-Resolution: 1m | 5m | 15m | 1h.
#     La vela ABIERTA lleva "partial": true. close_t es el cierre nominal
#     INCLUSIVO (close_t - t == interval_ms - 1, comprobado sobre las 261 105
#     velas de la base), asi que la vela abierta es la que cumple
#     close_t >= now_ms: la unica a la que candles.py le sigue haciendo
#     upsert. Solo puede ser la ultima fila, porque t es unico dentro de
#     (coin, interval) y las filas van en orden ascendente.
#
# /health y /snapshot devuelven un OBJETO, no un array, y no admiten
# parámetros. En /health, age_s puede salir ligeramente NEGATIVO (décimas de
# segundo): los ts_ms vienen del exchange y el reloj del teléfono va algo por
# detrás, así que un cliente no debe asumir age_s >= 0.
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
                self._handle_simple(conn, qs,
                    "SELECT ts_ms, bid_px, bid_sz, ask_px, ask_sz, mid_px, spread_bps, "
                    "bid_depth_05, ask_depth_05, bid_depth_2, ask_depth_2, imb_05, imb_2 "
                    "FROM book_snapshots WHERE coin = ? AND ts_ms >= ? ORDER BY ts_ms ASC")
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
