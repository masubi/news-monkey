#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$SCRIPT_DIR/.app.pid"
DEFAULT_PORT="${PORT:-8001}"
MAX_TRIES=20

# Find a free port starting from DEFAULT_PORT
find_free_port() {
    local port=$DEFAULT_PORT
    local tries=0
    while [ $tries -lt $MAX_TRIES ]; do
        if ! lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "$port"
            return 0
        fi
        port=$((port + 1))
        tries=$((tries + 1))
    done
    echo "ERROR: Could not find a free port after $MAX_TRIES attempts" >&2
    return 1
}

# Check if already running
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(head -1 "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        OLD_PORT=$(tail -1 "$PIDFILE")
        echo "News Monkey is already running (PID $OLD_PID) at http://localhost:$OLD_PORT"
        exit 0
    else
        rm -f "$PIDFILE"
    fi
fi

PORT=$(find_free_port)

cd "$SCRIPT_DIR"
# Use local venv if available, otherwise system python
if [ -f "$SCRIPT_DIR/.venv/bin/python3" ]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python3"
else
    PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"
fi
mkdir -p "$SCRIPT_DIR/data"
nohup "$PYTHON" -m uvicorn app:app --host 0.0.0.0 --port "$PORT" > "$SCRIPT_DIR/data/app.log" 2>&1 &
APP_PID=$!

# Wait for startup
sleep 2
if ! kill -0 "$APP_PID" 2>/dev/null; then
    echo "ERROR: Failed to start News Monkey. Check data/app.log for details." >&2
    exit 1
fi

# Write PID and port
echo "$APP_PID" > "$PIDFILE"
echo "$PORT" >> "$PIDFILE"

echo "News Monkey started (PID $APP_PID) at http://localhost:$PORT"
