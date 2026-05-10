#!/usr/bin/env bash
# start_machine_a.sh — run on Machine A (coordinator + workers 0 and 1)
#
# Usage:
#   chmod +x scripts/start_machine_a.sh
#   ./scripts/start_machine_a.sh --config cluster.2machine.conf \
#       --partition-dir data/partitions/ \
#       --output-dir output/
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CONFIG=""
PARTITION_DIR=""
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --config)        CONFIG="$2";        shift 2 ;;
        --partition-dir) PARTITION_DIR="$2"; shift 2 ;;
        --output-dir)    OUTPUT_DIR="$2";    shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[[ -z "$CONFIG" || -z "$PARTITION_DIR" || -z "$OUTPUT_DIR" ]] && {
    echo "usage: $0 --config <path> --partition-dir <path> --output-dir <path>"
    exit 1
}

mkdir -p "$OUTPUT_DIR"

cleanup() {
    echo "Stopping processes..."
    [[ -n "${COORD_PID:-}" ]] && kill "$COORD_PID" 2>/dev/null || true
    [[ -n "${W0_PID:-}" ]]    && kill "$W0_PID"    2>/dev/null || true
    [[ -n "${W1_PID:-}" ]]    && kill "$W1_PID"    2>/dev/null || true
}
trap cleanup EXIT

echo "[machine A] starting coordinator..."
"$ROOT_DIR/build/coordinator" \
    --config "$CONFIG" \
    --output-dir "$OUTPUT_DIR" \
    2>&1 | tee "$OUTPUT_DIR/coordinator.log" &
COORD_PID=$!

# Give coordinator a moment to open its listen socket
sleep 1

echo "[machine A] starting worker 0..."
"$ROOT_DIR/build/worker" \
    --id 0 \
    --config "$CONFIG" \
    --partition "$PARTITION_DIR/partition_0.bin" \
    --output-dir "$OUTPUT_DIR" \
    2>&1 | tee "$OUTPUT_DIR/worker_0.log" &
W0_PID=$!

echo "[machine A] starting worker 1..."
"$ROOT_DIR/build/worker" \
    --id 1 \
    --config "$CONFIG" \
    --partition "$PARTITION_DIR/partition_1.bin" \
    --output-dir "$OUTPUT_DIR" \
    2>&1 | tee "$OUTPUT_DIR/worker_1.log" &
W1_PID=$!

echo "[machine A] all processes running. Waiting for completion..."
wait "$W0_PID" "$W1_PID"
wait "$COORD_PID"
echo "[machine A] done."
