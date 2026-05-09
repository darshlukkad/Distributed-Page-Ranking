#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
OUTPUT_DIR="$ROOT_DIR/demo_out"
PARTITION_DIR="$OUTPUT_DIR/partitions"

build_project() {
  if command -v cmake >/dev/null 2>&1; then
    cmake -S "$ROOT_DIR" -B "$BUILD_DIR"
    cmake --build "$BUILD_DIR" -j4
    return
  fi

  make -C "$ROOT_DIR" all
}

cleanup() {
  if [[ -n "${COORD_PID:-}" ]]; then
    kill "$COORD_PID" 2>/dev/null || true
  fi
  if [[ -n "${WORKER_PIDS:-}" ]]; then
    for pid in $WORKER_PIDS; do
      kill "$pid" 2>/dev/null || true
    done
  fi
}

trap cleanup EXIT

rm -rf "$OUTPUT_DIR"
mkdir -p "$PARTITION_DIR"

build_project

python3 "$ROOT_DIR/scripts/preprocessor.py" \
  --input "$ROOT_DIR/data/sample/twitter_sample_snap.txt" \
  --workers 4 \
  --output-dir "$PARTITION_DIR"

"$BUILD_DIR/coordinator" \
  --config "$ROOT_DIR/cluster.local.conf" \
  --output-dir "$OUTPUT_DIR" \
  >"$OUTPUT_DIR/coordinator.stdout.log" 2>&1 &
COORD_PID=$!

sleep 1

WORKER_PIDS=""
for worker_id in 0 1 2 3; do
  "$BUILD_DIR/worker" \
    --id "$worker_id" \
    --config "$ROOT_DIR/cluster.local.conf" \
    --partition "$PARTITION_DIR/partition_${worker_id}.bin" \
    --output-dir "$OUTPUT_DIR" \
    >"$OUTPUT_DIR/worker_${worker_id}.stdout.log" 2>&1 &
  WORKER_PIDS="$WORKER_PIDS $!"
done

for pid in $WORKER_PIDS; do
  wait "$pid"
done
wait "$COORD_PID"

python3 "$ROOT_DIR/scripts/postprocess.py" \
  --top-k "$OUTPUT_DIR/top_k.txt" \
  --id-map "$ROOT_DIR/data/sample/twitter_sample_ids.csv" \
  --output "$OUTPUT_DIR/top_influencers.txt"

echo
echo "Top-K"
cat "$OUTPUT_DIR/top_k.txt"
echo
echo "Top Influencers"
cat "$OUTPUT_DIR/top_influencers.txt"
