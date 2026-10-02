#!/data/data/com.termux/files/usr/bin/bash
cd ~/oi
export PYTHONUNBUFFERED=1

MAX_LOG_BYTES=$((10 * 1024 * 1024))  # 10 MB

rotate_log() {
    local f="$1"
    [ -f "$f" ] || return 0
    local sz
    sz=$(stat -c %s "$f" 2>/dev/null || echo 0)
    if [ "$sz" -gt "$MAX_LOG_BYTES" ]; then
        mv -f "$f" "${f}.1"
        : > "$f"
    fi
}

ensure() {
    # Primer argumento = cmdline exacto a buscar (y lo que se exec si no está).
    local cmdline="$1"
    local log="$2"
    # -fx exige match EXACTO del cmdline (evita falsos positivos con shells
    # que lleven "python3 X.py" como argumento de -c).
    if ! pgrep -fx "${cmdline}" > /dev/null; then
        echo "[$(date '+%H:%M:%S')] ${cmdline} caído, relanzando..."
        nohup bash -c "exec ${cmdline}" >> "${log}" 2>&1 &
    fi
}

while true; do
    for f in oi_poller.log trades_listener.log server.log \
             funding_poller.log book_listener.log candles.log \
             cleanup.log; do
        rotate_log "$f"
    done

    ensure "python3 oi_poller.py"       oi_poller.log
    ensure "python3 trades_listener.py" trades_listener.log
    ensure "python3 server.py"          server.log
    ensure "python3 funding_poller.py"  funding_poller.log
    ensure "python3 book_listener.py"   book_listener.log
    ensure "python3 candles.py"         candles.log
    ensure "python3 cleanup.py --daemon" cleanup.log

    sleep 60
done
