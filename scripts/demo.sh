#!/usr/bin/env bash
# Starts the background (headless) part of a local Conf-Chat network on Linux/macOS
# and prints the commands to open the chat peers in separate terminals.
# Usage: scripts/demo.sh            (Ctrl+C stops the headless nodes)
set -euo pipefail
cd "$(dirname "$0")/.."
BASE=${BASE:-5000}

python3 -m confchat --port "$BASE" --headless --status 10 &
sleep 1
python3 -m confchat --port $((BASE + 1)) --join "127.0.0.1:$BASE" --headless --status 10 &

echo
echo "Headless DHT nodes running on ports $BASE and $((BASE + 1))."
echo "Open one terminal per user and run, for example:"
for i in 1 2 3; do
  echo "  python3 -m confchat --port $((BASE + 100 + i)) --join 127.0.0.1:$BASE"
done
trap 'kill $(jobs -p) 2>/dev/null' INT TERM EXIT
wait
