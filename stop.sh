#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$SCRIPT_DIR/.app.pid"

if [ ! -f "$PIDFILE" ]; then
    echo "News Monkey is not running (no pidfile found)"
    exit 0
fi

PID=$(head -1 "$PIDFILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "News Monkey process $PID is not running (stale pidfile)"
    rm -f "$PIDFILE"
    exit 0
fi

echo "Stopping News Monkey (PID $PID)..."

# Send SIGTERM for graceful shutdown
kill "$PID" 2>/dev/null || true

# Wait up to 5 seconds
for i in $(seq 1 10); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "News Monkey stopped gracefully"
        rm -f "$PIDFILE"
        exit 0
    fi
    sleep 0.5
done

# Force kill if still alive
echo "Sending SIGKILL..."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PIDFILE"
echo "News Monkey force-stopped"
