#!/usr/bin/env bash
# start_machine_b.sh — run on Machine B (workers 2 and 3)
#
# Usage:
#   chmod +x scripts/start_machine_b.sh
#   ./scripts/start_machine_b.sh --config cluster.2machine.conf \
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
    [[ -n "${W2_PID:-}" ]] && kill "$W2_PID" 2>/dev/null || true
    [[ -n "${W3_PID:-}" ]] && kill "$W3_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[machine B] starting worker 2..."
"$ROOT_DIR/build/worker" \
    --id 2 \
    --config "$CONFIG" \
    --partition "$PARTITION_DIR/partition_2.bin" \
    --output-dir "$OUTPUT_DIR" \
    2>&1 | tee "$OUTPUT_DIR/worker_2.log" &
W2_PID=$!

echo "[machine B] starting worker 3..."
"$ROOT_DIR/build/worker" \
    --id 3 \
    --config "$CONFIG" \
    --partition "$PARTITION_DIR/partition_3.bin" \
    --output-dir "$OUTPUT_DIR" \
    2>&1 | tee "$OUTPUT_DIR/worker_3.log" &
W3_PID=$!

echo "[machine B] workers running. Waiting for completion..."
wait "$W2_PID" "$W3_PID"
echo "[machine B] done."
