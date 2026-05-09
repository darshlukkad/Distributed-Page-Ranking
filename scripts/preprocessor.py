#!/usr/bin/env python3

import argparse
import gzip
import json
import os
import struct
from collections import defaultdict
from pathlib import Path

MAGIC = 0x50414745
VERSION = 1
HEADER_STRUCT = struct.Struct("<IIIIQQQ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-worker CSR partition files.")
    parser.add_argument("--input", required=True, help="Edge list file (.txt or .gz).")
    parser.add_argument("--workers", required=True, type=int, help="Number of workers.")
    parser.add_argument("--output-dir", required=True, help="Directory for partition files.")
    parser.add_argument(
        "--no-reverse",
        action="store_true",
        help="Keep edge direction as-is. By default the preprocessor reverses edges.",
    )
    return parser.parse_args()


def open_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    adjacency: dict[int, dict[int, list[int]]] = {
        worker_id: defaultdict(list) for worker_id in range(args.workers)
    }
    max_vertex_id = -1
    edge_count = 0

    with open_text(args.input) as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split()
            if len(parts) != 2:
                raise ValueError(f"invalid edge on line {line_number}: {line}")

            first, second = (int(parts[0]), int(parts[1]))
            if args.no_reverse:
                source, destination = first, second
            else:
                source, destination = second, first

            worker_id = source % args.workers
            adjacency[worker_id][source].append(destination)
            max_vertex_id = max(max_vertex_id, source, destination)
            edge_count += 1

    if max_vertex_id < 0:
        raise ValueError("input graph is empty")

    total_vertices = max_vertex_id + 1
    manifest = {"total_vertices": total_vertices, "total_edges": edge_count, "workers": []}

    for worker_id in range(args.workers):
        local_vertex_ids: list[int] = []
        edge_offsets: list[int] = [0]
        edges: list[int] = []

        for vertex_id in range(worker_id, total_vertices, args.workers):
            local_vertex_ids.append(vertex_id)
            edges.extend(adjacency[worker_id].get(vertex_id, []))
            edge_offsets.append(len(edges))

        partition_path = output_dir / f"partition_{worker_id}.bin"
        with open(partition_path, "wb") as handle:
            handle.write(
                HEADER_STRUCT.pack(
                    MAGIC,
                    VERSION,
                    worker_id,
                    args.workers,
                    total_vertices,
                    len(local_vertex_ids),
                    len(edges),
                )
            )
            handle.write(struct.pack(f"<{len(local_vertex_ids)}I", *local_vertex_ids))
            handle.write(struct.pack(f"<{len(edge_offsets)}Q", *edge_offsets))
            if edges:
                handle.write(struct.pack(f"<{len(edges)}I", *edges))

        manifest["workers"].append(
            {
                "worker_id": worker_id,
                "vertex_count": len(local_vertex_ids),
                "edge_count": len(edges),
                "path": str(partition_path),
            }
        )

    manifest_path = output_dir / "partition_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote {args.workers} partition files to {output_dir}")


if __name__ == "__main__":
    main()
