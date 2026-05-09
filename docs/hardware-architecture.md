# Hardware Architecture — Distributed PageRank

## Full System Overview

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                     DISTRIBUTED PAGERANK — HARDWARE ARCHITECTURE                    ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

 ┌──────────────────────────────────────────────────────────────────────────────────┐
 │                   PHASE 0 — PREPROCESSING  (offline, one-time)                   │
 │                                                                                  │
 │   ┌──────────────────────────────────────────────────────────────────────────┐   │
 │   │                      PREPROCESSING MACHINE                               │   │
 │   │                  Any team laptop with 30 GB free                         │   │
 │   │                  Not part of the runtime cluster                         │   │
 │   │                                                                          │   │
 │   │   INPUT:   twitter-2010.txt.gz          (5.6 GB compressed)              │   │
 │   │            twitter-2010-ids.csv.gz      (0.3 GB)                         │   │
 │   │                                                                          │   │
 │   │   RUNS:    preprocessor.py --workers 4                                   │   │
 │   │            → streams gzip, reverses edges, hash-partitions by src % N   │   │
 │   │            → takes 15–30 minutes                                         │   │
 │   │                                                                          │   │
 │   │   OUTPUT:  partition_0.bin  (1.6 GB) ──────────────────── scp ────────────────► Worker 0
 │   │            partition_1.bin  (1.6 GB) ──────────────────── scp ────────────────► Worker 1
 │   │            partition_2.bin  (1.6 GB) ──────────────────── scp ────────────────► Worker 2
 │   │            partition_3.bin  (1.6 GB) ──────────────────── scp ────────────────► Worker 3
 │   │            partition_manifest.json   ──────────────────── scp ────────────────► Coordinator
 │   └──────────────────────────────────────────────────────────────────────────┘   │
 └──────────────────────────────────────────────────────────────────────────────────┘

                         ▼  partition files distributed before runtime starts

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                  PHASE 1–6 — DISTRIBUTED RUNTIME                                    │
│                  All machines connected via Gigabit Ethernet switch                  │
│                                                                                     │
│           ┌──────────────────────────────────────────────────────────┐              │
│           │                      COORDINATOR                         │              │
│           │               192.168.1.10 : port 9000                   │              │
│           │                                                          │              │
│           │  RAM: 4 GB+   Disk: 2 GB free                            │              │
│           │  Runs: coordinator binary                                 │              │
│           │  Reads: cluster.conf, partition_manifest.json            │              │
│           │  Writes: top_k.txt, iteration_log.csv                    │              │
│           │          top_influencers.txt (post-run)                  │              │
│           │                                                          │              │
│           │  Responsibilities:                                        │              │
│           │  1. Accept N worker registrations                        │              │
│           │  2. Broadcast CLUSTER_INFO (peer list)                   │              │
│           │  3. Drive BSP iteration loop (START_ITERATION → STOP)    │              │
│           │  4. Aggregate dangling mass + convergence delta           │              │
│           │  5. Merge top-K results, write outputs                   │              │
│           └──────────────────────┬───────────────────────────────────┘              │
│                                  │                                                  │
│             ┌────────────────────┼────────────────────┐                             │
│   Control plane (TCP port 9000)  │  4 persistent connections, one per worker        │
│   HELLO, READY, START_ITERATION  │  STOP, DANGLING_REPORT, DELTA_REPORT, etc.       │
│             └────────────────────┼────────────────────┘                             │
│                                  │                                                  │
│        ┌─────────────────────────┼───────────────────────────────┐                  │
│        │                         │                               │                  │
│   ┌────▼──────┐  ┌───────────────▼┐  ┌──────────────┐  ┌────────▼─────┐            │
│   │ WORKER 0  │  │   WORKER 1     │  │   WORKER 2   │  │   WORKER 3   │            │
│   │           │  │                │  │              │  │              │            │
│   │192.168.   │  │192.168.        │  │192.168.      │  │192.168.      │            │
│   │1.11: 9001 │  │1.12: 9002      │  │1.13: 9003    │  │1.14: 9004    │            │
│   │           │  │                │  │              │  │              │            │
│   │RAM:  8GB+ │  │RAM:  8GB+      │  │RAM:  8GB+    │  │RAM:  8GB+    │            │
│   │Disk: 5GB  │  │Disk: 5GB       │  │Disk: 5GB     │  │Disk: 5GB     │            │
│   │           │  │                │  │              │  │              │            │
│   │partition  │  │partition       │  │partition     │  │partition     │            │
│   │_0.bin     │  │_1.bin          │  │_2.bin        │  │_3.bin        │            │
│   │(1.6 GB)   │  │(1.6 GB)        │  │(1.6 GB)      │  │(1.6 GB)      │            │
│   │           │  │                │  │              │  │              │            │
│   │Owns:      │  │Owns:           │  │Owns:         │  │Owns:         │            │
│   │v % 4 == 0 │  │v % 4 == 1      │  │v % 4 == 2    │  │v % 4 == 3    │            │
│   │~10.4M     │  │~10.4M          │  │~10.4M        │  │~10.4M        │            │
│   │vertices   │  │vertices        │  │vertices      │  │vertices      │            │
│   └─────┬─────┘  └───────┬────────┘  └──────┬───────┘  └──────┬──────┘            │
│         │                │                  │                  │                   │
│         └────────────────┴──────────────────┴──────────────────┘                   │
│                    Data plane mesh — 6 bidirectional TCP connections                │
│                    CONTRIBS messages (rank contributions per iteration)              │
│                    ~hundreds MB per iteration on the full graph                     │
│                    125 MB/s per link over Gigabit Ethernet                          │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Physical Network Diagram

```
                    ┌──────────────────────────────────┐
                    │      8-PORT GIGABIT SWITCH        │
                    │      TP-Link TL-SG108             │
                    │      (unmanaged, plug-and-play)   │
                    │                                   │
                    │   [1] [2] [3] [4] [5] [6] [7] [8]│
                    └────┬───┬───┬───┬───┬──────────────┘
                         │   │   │   │   │
          Cat6 Ethernet  │   │   │   │   │  (1–2m cables)
          ───────────────┘   │   │   │   └─────────────────────┐
          ┌────────────────── ┘   │   └──────────┐             │
          │               ┌───── ┘              │              │
          │               │                     │              │
    ┌─────┴──────┐  ┌─────┴──────┐  ┌───────────┴┐  ┌─────────┴──┐  ┌────────────┐
    │COORDINATOR │  │  WORKER 0  │  │  WORKER 1  │  │  WORKER 2  │  │  WORKER 3  │
    │MacBook     │  │MacBook     │  │MacBook     │  │MacBook     │  │MacBook     │
    │            │  │            │  │            │  │            │  │            │
    │USB-C       │  │USB-C       │  │USB-C       │  │USB-C       │  │USB-C       │
    │→ Ethernet  │  │→ Ethernet  │  │→ Ethernet  │  │→ Ethernet  │  │→ Ethernet  │
    │adapter     │  │adapter     │  │adapter     │  │adapter     │  │adapter     │
    └────────────┘  └────────────┘  └────────────┘  └────────────┘  └────────────┘
```

---

## Connection Summary

| Plane | Count | Between | Used For |
|---|---|---|---|
| Control | 4 | Each Worker → Coordinator | HELLO, START_ITERATION, STOP, DELTA_REPORT, DANGLING_REPORT, LOCAL_TOPK, SHUTDOWN |
| Data | 6 | Worker i ↔ Worker j (i < j) | CONTRIBS (rank contributions each iteration) |
| **Total** | **10** | | |

---

## Storage at a Glance

```
Preprocessing machine  ────────────────────────────  30 GB free required
│  twitter-2010.txt.gz           5.6 GB  (download)
│  twitter-2010-ids.csv.gz       0.3 GB  (download)
│  temp sort buffers             8–12 GB (cleared after preprocessing)
│  partition_0–3.bin             6.4 GB  (4 × 1.6 GB output)
│  Pokec dataset (dev)           0.3 GB

Coordinator  ───────────────────────────────────────   2 GB free required
│  coordinator binary            ~5 MB
│  twitter-2010-ids.csv.gz       0.3 GB  (post-processing)
│  top_k.txt, logs, outputs      <10 MB

Each Worker (×4)  ──────────────────────────────────   5 GB free required
   partition_K.bin               1.6 GB
   worker binary                 ~5 MB
   result_worker_K.txt           ~200 MB

Total across cluster             ~52 GB
```

---

## Per-Iteration Data Flow

```
Every BSP iteration follows this sequence:

  Coordinator                    Worker 0        Worker 1        Worker 2        Worker 3
      │                              │               │               │               │
      │──── START_ITERATION k ──────►│──────────────►│──────────────►│──────────────►│
      │                              │               │               │               │
      │               [Step 1: walk local vertices, compute contributions]            │
      │               [Route local→local directly; pack cross-worker into outboxes]  │
      │                              │               │               │               │
      │               [Step 2: exchange CONTRIBS over data-plane mesh]               │
      │                         W0◄──►W1  W0◄──►W2  W0◄──►W3  W1◄──►W2  W1◄──►W3  W2◄──►W3
      │                              │               │               │               │
      │               [Step 3: fold received contributions into incoming[]]          │
      │                              │               │               │               │
      │◄─── DANGLING_REPORT ─────────│───────────────│───────────────│───────────────│
      │     (local dangling mass)    │               │               │               │
      │                              │               │               │               │
      │  [sum → global_dangling]     │               │               │               │
      │                              │               │               │               │
      │──── GLOBAL_DANGLING ────────►│──────────────►│──────────────►│──────────────►│
      │                              │               │               │               │
      │               [Step 5: apply PageRank formula]                               │
      │               rank_next[i] = (1-d)/N + d*incoming[i] + d*dangling/N         │
      │                              │               │               │               │
      │◄─── DELTA_REPORT ────────────│───────────────│───────────────│───────────────│
      │     (local L1 delta)         │               │               │               │
      │                              │               │               │               │
      │  [sum → global_delta]        │               │               │               │
      │  [if global_delta < 1e-6]    │               │               │               │
      │                              │               │               │               │
      │──── STOP (or next k) ───────►│──────────────►│──────────────►│──────────────►│
      │                              │               │               │               │
```

---

## Key Numbers (Twitter-2010, N=4)

| Metric | Value |
|---|---|
| Total vertices | 41,652,230 |
| Total edges | 1,468,364,884 |
| Vertices per worker | ~10,413,058 |
| Edges per worker | ~367,091,221 |
| RAM per worker | ~1.85 GB |
| Partition file size | ~1.59 GB |
| Expected iterations to convergence | 30–60 |
| TCP connections in cluster | 10 |
