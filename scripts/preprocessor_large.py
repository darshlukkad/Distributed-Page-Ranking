#!/usr/bin/env python3
"""
preprocessor_large.py — memory-efficient CSR partitioner for large graphs.

Requires: pip3 install numpy

Use this for graphs with > 100M edges (e.g. Twitter-2010 with 1.47B edges).
For the bundled sample graph, use preprocessor.py (stdlib, no deps).

Strategy — two passes:
  Pass 1: stream the gzip edge list → write per-worker temp binary files
           (compact uint32 src+dst pairs, 8 bytes per edge)
  Pass 2: per worker, load temp file with numpy, sort by src, build CSR,
           write partition_{w}.bin and delete the temp file

Memory per Pass 2 call: 2 × worker_edges × 8 bytes (data + argsort index)
  Twitter-2010, N=4: 2 × 368M × 8 ≈ 5.9 GB — safe on 16 GB machines.

Disk for temp files: total_edges × 8 bytes = ~11.7 GB for Twitter-2010.
Combined with partition output (~6.4 GB) you need ~20 GB free on the
preprocessing machine.

Usage:
    pip3 install numpy
    python3 scripts/preprocessor_large.py \\
        --input  data/twitter-2010.txt.gz \\
        --workers 4 \\
        --output-dir data/partitions/

    # optional: put temp files on a different disk
    python3 scripts/preprocessor_large.py \\
        --input data/twitter-2010.txt.gz \\
        --workers 4 \\
        --output-dir data/partitions/ \\
        --temp-dir /tmp/pagerank_tmp/
"""

import argparse
import gzip
import json
import os
import struct
import time
from pathlib import Path

import numpy as np

MAGIC = 0x50414745
VERSION = 1
HEADER_STRUCT = struct.Struct("<IIIIQQQ")  # matches partition_loader.cpp

EDGE_DTYPE = np.dtype([("src", np.uint32), ("dst", np.uint32)])
FLUSH_EDGES = 2_000_000  # edges per worker buffer before flushing to disk (~16 MB)
REPORT_EVERY = 100_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build per-worker CSR partition files for large graphs."
    )
    parser.add_argument("--input",      required=True, help="Edge list (.txt or .gz)")
    parser.add_argument("--workers",    required=True, type=int, help="Number of workers")
    parser.add_argument("--output-dir", required=True, help="Directory for partition files")
    parser.add_argument("--temp-dir",   default=None,
                        help="Directory for temp files (default: output-dir)")
    parser.add_argument("--no-reverse", action="store_true",
                        help="Keep edge direction. Default reverses edges for PageRank.")
    return parser.parse_args()


def open_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def pass1_stream(input_path: str, N: int, temp_paths: list, no_reverse: bool) -> tuple:
    """Stream edges → N temp binary files. Returns (total_edges, total_vertices)."""
    handles = [open(p, "wb") for p in temp_paths]
    buffers = [np.zeros(FLUSH_EDGES, dtype=EDGE_DTYPE) for _ in range(N)]
    counts  = [0] * N
    max_vid = 0
    total   = 0
    t0      = time.monotonic()

    def flush(w: int):
        buffers[w][:counts[w]].tofile(handles[w])
        counts[w] = 0

    try:
        with open_text(input_path) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == "#":
                    continue
                parts = line.split()
                if len(parts) != 2:
                    continue

                a, b = int(parts[0]), int(parts[1])
                src, dst = (b, a) if not no_reverse else (a, b)

                w = src % N
                i = counts[w]
                buffers[w][i]["src"] = src
                buffers[w][i]["dst"] = dst
                counts[w] += 1
                if counts[w] == FLUSH_EDGES:
                    flush(w)

                if src > max_vid: max_vid = src
                if dst > max_vid: max_vid = dst
                total += 1

                if total % REPORT_EVERY == 0:
                    elapsed = time.monotonic() - t0
                    rate = total / elapsed / 1_000_000
                    print(f"  pass 1: {total:>13,} edges  {rate:.1f}M edges/s", flush=True)
    finally:
        for w in range(N):
            flush(w)
            handles[w].close()

    total_vertices = max_vid + 1
    elapsed = time.monotonic() - t0
    print(f"  pass 1 done: {total:,} edges  {total_vertices:,} vertices  "
          f"({elapsed:.0f}s)", flush=True)
    return total, total_vertices


def pass2_build(w: int, N: int, total_vertices: int, temp_path: str,
                output_dir: Path) -> dict:
    """Load worker w's temp file, sort, build CSR, write partition."""
    print(f"  pass 2 worker {w}: loading {os.path.getsize(temp_path) // (1024*1024)} MB ...",
          flush=True)
    t0 = time.monotonic()

    raw = np.fromfile(temp_path, dtype=EDGE_DTYPE)
    os.unlink(temp_path)  # free disk immediately

    if len(raw):
        order = np.argsort(raw["src"], kind="stable")
        raw   = raw[order]
        srcs  = raw["src"]
        dsts  = raw["dst"]
    else:
        srcs = np.array([], dtype=np.uint32)
        dsts = np.array([], dtype=np.uint32)

    # stride partitioning: vertex v → worker (v % N), local index (v // N)
    local_vids = np.arange(w, total_vertices, N, dtype=np.uint32)

    # CSR row pointers via binary search
    offsets = np.searchsorted(srcs, local_vids, side="left").astype(np.uint64)
    offsets = np.append(offsets, np.uint64(len(dsts)))

    partition_path = output_dir / f"partition_{w}.bin"
    with open(partition_path, "wb") as fout:
        fout.write(HEADER_STRUCT.pack(
            MAGIC, VERSION,
            np.uint32(w), np.uint32(N),
            np.uint64(total_vertices),
            np.uint64(len(local_vids)),
            np.uint64(len(dsts)),
        ))
        fout.write(local_vids.tobytes())
        fout.write(offsets.tobytes())
        if len(dsts):
            fout.write(dsts.tobytes())

    size_mb = partition_path.stat().st_size / (1024 * 1024)
    elapsed = time.monotonic() - t0
    print(f"  partition_{w}.bin: {len(local_vids):,} vertices  "
          f"{len(dsts):,} edges  {size_mb:.0f} MB  ({elapsed:.0f}s)", flush=True)

    return {
        "worker_id":    int(w),
        "vertex_count": int(len(local_vids)),
        "edge_count":   int(len(dsts)),
        "path":         str(partition_path),
    }


def main() -> None:
    args = parse_args()
    N = args.workers
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(args.temp_dir) if args.temp_dir else output_dir
    temp_dir.mkdir(parents=True, exist_ok=True)

    temp_paths = [str(temp_dir / f"_tmp_w{w}.bin") for w in range(N)]

    print(f"preprocessor_large: {args.input}  N={N}  reverse={'yes' if not args.no_reverse else 'no'}")
    print(f"  output → {output_dir}   temp → {temp_dir}")

    total_edges, total_vertices = pass1_stream(
        args.input, N, temp_paths, args.no_reverse
    )

    manifest: dict = {
        "total_vertices": total_vertices,
        "total_edges":    total_edges,
        "workers":        [],
    }

    for w in range(N):
        entry = pass2_build(w, N, total_vertices, temp_paths[w], output_dir)
        manifest["workers"].append(entry)

    (output_dir / "partition_manifest.json").write_text(
        json.dumps(manifest, indent=2)
    )
    print(f"\nAll done. Wrote {N} partition files to {output_dir}", flush=True)


if __name__ == "__main__":
    main()
