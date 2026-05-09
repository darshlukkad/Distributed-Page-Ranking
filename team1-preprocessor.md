# Team 1 — Preprocessor & Data Pipeline

## Overview

Team 1 owns everything that happens **before** the distributed runtime starts and **after** it finishes. You convert raw SNAP data into per-worker binary files and produce the final human-readable output. No other team can start end-to-end testing without your partition files, so your binary format spec is the first contract that must be locked down.

---

## Goals

1. Build a Python preprocessor that converts `twitter-2010.txt.gz` into N binary partition files (`partition_0.bin` … `partition_N-1.bin`), one per worker.
2. Correctly reverse edge directions (SNAP convention is opposite of PageRank convention).
3. Output files in the exact CSR binary layout that Team 4 will load.
4. Build a Python post-processor that joins the coordinator's `top_k.txt` with `twitter-2010-ids.csv` to produce `top_influencers.txt`.
5. Support Experiment 4 (follower-count ranking) and Experiment 5 (spam injection) with standalone scripts.

---

## High-Level Architecture

```
twitter-2010.txt.gz
        │
        ▼
  [preprocessor.py]
        │  stream lines, reverse edges, hash-partition by (src % N)
        │
   ┌────┴────┬────────┬────────┐
   ▼         ▼        ▼        ▼
partition_0  partition_1  partition_2  partition_3   (.bin files)
        │
        ▼
  [partition_manifest.json]   ← vertex/edge counts per worker (sanity check)

After distributed run:
top_k.txt + twitter-2010-ids.csv
        │
        ▼
  [postprocess.py]
        │
        ▼
top_influencers.txt
```

---

## Partition File Format (CSR Binary) — Contract for Team 4

This is the most critical interface you own. Get it right in Week 1 and publish it before anyone else writes a single line of C++.

### Layout

```
[Header — 32 bytes]
[local_vertex_ids[]  — num_local_vertices × 4 bytes, uint32]
[edge_offsets[]      — (num_local_vertices + 1) × 8 bytes, uint64]
[edges[]             — num_local_edges × 4 bytes, uint32]
```

### Header (32 bytes, all little-endian)

| Offset | Size | Field | Value |
|--------|------|-------|-------|
| 0 | 4 B | `magic` | `0x50414745` (`"PAGE"`) |
| 4 | 4 B | `version` | `1` |
| 8 | 4 B | `worker_id` | 0-indexed worker this file belongs to |
| 12 | 4 B | `num_workers` | N (cluster size) |
| 16 | 8 B | `total_global_vertices` | 41,652,230 for full graph |
| 24 | 8 B | `num_local_vertices` | vertices owned by this worker |
| (implicit) | 8 B | `num_local_edges` | total outgoing edges in this partition |

> **Note:** `num_local_edges` must be derivable from `edge_offsets[num_local_vertices]`. Include it in the header explicitly for fast validation at load time.

Finalize exact header layout in a shared `format.md` doc and post it to the team repo on Day 1.

### Vertex Ownership Rule

Worker `w` owns vertex `v` if and only if `v % N == w`. For N=4, worker 2 owns `{2, 6, 10, 14, …}`.

Local index of global vertex `v` on its owning worker = `v / N` (integer division). This arithmetic trick is what lets Team 4 avoid a lookup table.

### Edge Direction (Critical)

SNAP edge `i j` means "j follows i" — the edge points **from followed to follower**, the reverse of PageRank convention.

You must flip it: read `i j`, store as edge `j → i` (j is the source, i is the destination).

This is a single swap of two variables. If you get it wrong, the PageRank top-K will contain accounts that follow lots of people instead of accounts that are followed by important people. Write a unit test for this.

### Dangling Vertices

A vertex with no outgoing edges (after reversal) is dangling. It will appear in `local_vertex_ids[]` with two equal consecutive values in `edge_offsets[]`. No special flag needed — the loop over its edges does zero iterations naturally.

---

## Module Breakdown

### `preprocessor.py`

- **Args:** `--input twitter-2010.txt.gz --workers N --output-dir ./partitions`
- Stream `gzip.open()` line by line — never load the full file into RAM.
- For each line `i j`: reverse to get edge `j → i`. Source is `j`, so this edge goes to worker `j % N`.
- Collect edges per worker. When memory budget is reached (configurable), flush sorted per-worker edges to temp files.
- Final pass: sort per-worker edges by source vertex, build CSR arrays, write binary file.
- Write `partition_manifest.json`: `{worker_id, vertex_count, edge_count}` for each worker.
- Print progress every 100M edges.

### `postprocess.py`

- **Args:** `--top-k top_k.txt --id-map twitter-2010-ids.csv --output top_influencers.txt`
- Read `top_k.txt` (format: `node_id rank`, one per line, sorted by rank).
- Load `twitter-2010-ids.csv` into a dict `{node_id: twitter_id}`.
- Join and write `top_influencers.txt` in table format.

### `follower_count.py` (for Experiment 4)

- Read the raw edge list (post-reversal), count in-degree per vertex.
- Output top-K by raw follower count as `(node_id, follower_count)`.

### `spam_inject.py` (for Experiment 5)

- Accept a small subgraph file and target vertex IDs.
- Inject 10,000 synthetic spam vertices: each follows every other spam vertex and all target vertices.
- Output augmented edge list for re-preprocessing.

---

## Deliverables

| Deliverable | When | Format |
|-------------|------|--------|
| `format.md` — binary format spec | End of Day 1, Week 1 | Markdown, shared with Team 4 |
| `preprocessor.py` — working on small graph (first 10M edges) | End of Week 1 | Python script |
| `partition_manifest.json` | Auto-generated by preprocessor | JSON |
| Full Twitter-2010 partition files (N=4) | End of Week 2 | 4 × ~1.59 GB `.bin` files |
| `postprocess.py` | End of Week 2 | Python script |
| `follower_count.py` | Week 3 | Python script |
| `spam_inject.py` | Week 3 | Python script |
| Validated correctness (small graph vs NetworkX) | End of Week 1 | Unit test output |

---

## Testing Plan

### Unit Tests

- **Edge reversal:** given input `5 3`, assert output edge is `3 → 5` (source=3, dest=5).
- **Partition assignment:** for N=4, vertex 10 should land in worker `10 % 4 = 2`.
- **CSR correctness:** for a 5-vertex, 8-edge synthetic graph, assert `edge_offsets[i+1] - edge_offsets[i]` equals the true out-degree of vertex `i`.
- **Dangling vertex:** a vertex with no outgoing edges must appear with equal consecutive offsets.
- **Rank-sum conservation:** after preprocessing a small graph and running sequential PageRank, verify `sum(ranks) ≈ 1.0`.

### Integration Test (Week 1 milestone)

Run preprocessor on the first 10M edges of Twitter-2010 (or the SNAP Pokec dataset). Load one partition file in Python, verify header fields, walk all edges, confirm total edge count matches manifest. This sanity-check script becomes a regression test.

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Full preprocessing takes 15–30 min | Always develop on the 10M-edge subgraph; run full graph only for final experiments |
| Memory blow-up from in-memory edge buffers | Stream and flush to temp files; limit in-memory buffer to ~2 GB |
| Edge-direction bug | Unit test with known-correct small graph, cross-check against NetworkX reference ranks |
| Binary format mismatch with Team 4 | Publish `format.md` on Day 1; provide a Python reader that Team 4 can diff against their C++ loader |

---

## Interface Contracts (Publish These on Day 1)

**You produce → Team 4 consumes:**
- `partition_K.bin` binary format (see format spec above)
- `partition_manifest.json` (consumed by Teams 3 and 4 for validation)

**You produce → Team 3 consumes:**
- `top_influencers.txt` (post-run, via `postprocess.py`)

**You produce → All teams:**
- Small (10M-edge) subgraph edge list for local testing during Weeks 1–3
