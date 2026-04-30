#!/bin/sh
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

CMD="${1:-}"

case "$CMD" in
    uvicorn)
        log "Uvicorn server mode detected"

        export LOG_LEVEL=${LOG_LEVEL:-info}
        export ACCESS_LOG=${ACCESS_LOG:-1}

        log "Skipping Alembic migrations (already applied)"
        # Миграции уже применены, пропускаем их выполнение
        # alembic upgrade head

        CMD_LINE="uvicorn main:app --host 0.0.0.0 --port 8000"
        CMD_LINE="$CMD_LINE --log-level ${LOG_LEVEL}"

        if [ "$ACCESS_LOG" = "1" ] || [ "$ACCESS_LOG" = "true" ]; then
            CMD_LINE="$CMD_LINE --access-log"
        else
            CMD_LINE="$CMD_LINE --no-access-log"
        fi

        if [ -n "${LOG_CONFIG:-}" ]; then
            CMD_LINE="$CMD_LINE --log-config $LOG_CONFIG"
        fi

        if [ -n "${UVICORN_WORKERS:-}" ]; then
            CMD_LINE="$CMD_LINE --workers $UVICORN_WORKERS"
        fi

        if [ "${UVICORN_PROXY_HEADERS:-false}" = "true" ]; then
            CMD_LINE="$CMD_LINE --proxy-headers"
        fi

        log "Starting server: $CMD_LINE"
        exec sh -c "$CMD_LINE"
        ;;

    *)
        log "Custom command detected: $*"
        exec "$@"
        ;;
esac
