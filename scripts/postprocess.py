#!/usr/bin/env python3

import argparse
import csv
import gzip
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join top_k.txt with a node-id mapping CSV.")
    parser.add_argument("--top-k", required=True, help="Path to top_k.txt")
    parser.add_argument("--id-map", required=True, help="Path to node_id,twitter_id CSV (plain or .gz)")
    parser.add_argument("--output", required=True, help="Path to top_influencers.txt")
    return parser.parse_args()


def open_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def load_id_map(path: str) -> dict[int, str]:
    with open_text(path) as handle:
        reader = csv.DictReader(handle)
        return {int(row["node_id"]): row["twitter_id"] for row in reader}


def load_topk(path: str) -> list[tuple[int, float]]:
    entries: list[tuple[int, float]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            node_id, rank = line.split()
            entries.append((int(node_id), float(rank)))
    return entries


def main() -> None:
    args = parse_args()
    id_map = load_id_map(args.id_map)
    topk = load_topk(args.top_k)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("rank node_id twitter_id pagerank_score\n")
        for rank_index, (node_id, score) in enumerate(topk, start=1):
            twitter_id = id_map.get(node_id, "UNKNOWN")
            handle.write(f"{rank_index} {node_id} {twitter_id} {score:.12f}\n")

    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
