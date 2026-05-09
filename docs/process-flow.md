# Process Flow Diagrams

## 1. Full System Lifecycle

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                        DISTRIBUTED PAGERANK — FULL PROCESS FLOW                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │  PHASE 0 — DATA PREPARATION  (offline, one-time, before cluster starts)         │
 └─────────────────────────────────────────────────────────────────────────────────┘

        Download twitter-2010.txt.gz (5.6 GB)
                       │
                       ▼
        ┌──────────────────────────┐
        │     preprocessor.py      │
        │  --workers 4             │
        │                          │
        │  1. Stream gzip line by  │
        │     line (never unzip)   │
        │  2. Read edge "i j"      │
        │  3. Reverse → "j → i"   │
        │  4. Hash: src % N        │
        │  5. Build CSR arrays     │
        │  6. Write binary .bin    │
        └──────────┬───────────────┘
                   │
        ┌──────────┴──────────────────────────────────────────┐
        │          │                    │                      │
        ▼          ▼                    ▼                      ▼
 partition_0.bin  partition_1.bin  partition_2.bin  partition_3.bin
  (~800 MB)        (~800 MB)        (~800 MB)        (~800 MB)
        │          │                    │                      │
       scp        scp                 scp                    scp
        │          │                    │                      │
        ▼          ▼                    ▼                      ▼
    Worker 0   Worker 1            Worker 2               Worker 3
    192.168.   192.168.            192.168.               192.168.
      1.11       1.12                1.13                   1.14


 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │  PHASE 1 — CLUSTER BOOTSTRAP                                                    │
 └─────────────────────────────────────────────────────────────────────────────────┘

  All 4 workers start, read cluster.conf
                │
                ▼
  Worker i connects to Worker j  (if i < j to prevent deadlock)
                │
                ▼
  ┌─────────────────────────────────────────────────┐
  │            HELLO EXCHANGE                        │
  │                                                  │
  │  Each worker sends HELLO to all 3 peers:         │
  │    - worker_id                                   │
  │    - partition vertex_count                      │
  │    - partition edge_count                        │
  │                                                  │
  │  Each worker waits for HELLO from all 3 peers    │
  │                                                  │
  │  When all 3 HELLOs received → mesh is ready      │
  └─────────────────────────────────────────────────┘
                │
                ▼
  ┌─────────────────────────────────────────────────┐
  │            GRAPH LOADING (parallel)              │
  │                                                  │
  │  All 4 workers load partition_K.bin              │
  │  Validate: magic, worker_id, num_workers         │
  │  Allocate: rank_current[], rank_next[],          │
  │            incoming[], outboxes[]                │
  │  Init: rank_current[i] = 1.0 / 41,652,230       │
  └─────────────────────────────────────────────────┘
                │
                ▼
  Each worker sends READY to all peers
  Each worker waits for READY from all peers
                │
                ▼
        ═══ BEGIN ITERATION LOOP ═══


 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │  PHASE 2 — BSP ITERATION LOOP  (repeats until convergence)                     │
 └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 1 — BARRIER  (all workers synchronise before starting iteration k)    │
  │                                                                             │
  │  Each worker sends BARRIER_k to 3 peers                                    │
  │  Each worker waits for BARRIER_k from 3 peers                              │
  │  When all 3 received → everyone is at the same iteration                   │
  └────────────────────────────────────┬────────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 2 — LOCAL COMPUTE  (no network, pure CPU)                            │
  │                                                                             │
  │  For each local vertex v:                                                  │
  │    out_degree = edge_offsets[i+1] - edge_offsets[i]                        │
  │                                                                             │
  │    if out_degree == 0:                                                      │
  │      local_dangling_mass += rank_current[v]     ← dangling vertex          │
  │      continue                                                               │
  │                                                                             │
  │    contrib = rank_current[v] / out_degree                                  │
  │                                                                             │
  │    for each edge v → u:                                                    │
  │      owner = u % num_workers                                               │
  │      if owner == me:                                                        │
  │        incoming[u / num_workers] += contrib     ← local accumulation      │
  │      else:                                                                  │
  │        outbox[owner].append(u, contrib)         ← queue for network send  │
  └────────────────────────────────────┬────────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 3 — CONTRIBS EXCHANGE  (data-plane barrier)                          │
  │                                                                             │
  │  Send:  each outbox → CONTRIBS message → peer worker                      │
  │  Recv:  wait for CONTRIBS from all 3 peers                                 │
  │                                                                             │
  │  ┌──────────┐  CONTRIBS  ┌──────────┐  CONTRIBS  ┌──────────┐            │
  │  │ Worker 0 │◄──────────►│ Worker 1 │◄──────────►│ Worker 2 │            │
  │  └──────────┘            └──────────┘            └──────────┘            │
  │        ▲  CONTRIBS                      CONTRIBS       ▲                  │
  │        └──────────────────────────────────────────────►│                  │
  │                          ┌──────────┐                  │                  │
  │                          │ Worker 3 │◄─────────────────┘                  │
  │                          └──────────┘                                     │
  │                                                                            │
  │  Barrier: no worker advances until all 3 CONTRIBS received                │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 4 — FOLD INBOXES                                                     │
  │                                                                             │
  │  For each (vertex_id, contrib) in received CONTRIBS:                      │
  │    local_idx = vertex_id / num_workers                                     │
  │    incoming[local_idx] += contrib                                          │
  │                                                                             │
  │  After this step: incoming[i] = total contributions for local vertex i     │
  │  from ALL vertices in the entire graph                                     │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 5 — ALL-REDUCE: DANGLING MASS                                        │
  │                                                                             │
  │  Each worker broadcasts local_dangling_mass to 3 peers                    │
  │  Each worker collects 3 values, sums all 4 (including own)                │
  │  Result: every worker has identical global_dangling_mass                   │
  │                                                                             │
  │  W0: 0.0021 ──┐                                                            │
  │  W1: 0.0019 ──┼──► every worker sums → global_dangling = 0.0083          │
  │  W2: 0.0022 ──┤                                                            │
  │  W3: 0.0021 ──┘                                                            │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 6 — APPLY PAGERANK FORMULA                                           │
  │                                                                             │
  │  d = 0.85  (damping factor)                                                │
  │  N = 41,652,230  (total vertices)                                          │
  │                                                                             │
  │  for each local vertex i:                                                  │
  │    rank_next[i] = (1 - d) / N                ← teleport term              │
  │                 + d × incoming[i]             ← link contribution          │
  │                 + d × global_dangling / N     ← dangling redistribution   │
  │                                                                             │
  │  local_delta += |rank_next[i] - rank_current[i]|                          │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 7 — ALL-REDUCE: CONVERGENCE DELTA                                    │
  │                                                                             │
  │  Each worker broadcasts local_delta to 3 peers                            │
  │  Each worker collects 3 values, sums all 4                                │
  │  Result: every worker has identical global_delta                           │
  │                                                                             │
  │         ┌──────────────────────────────────────┐                          │
  │         │  global_delta < 1e-6 ?               │                          │
  │         └──────────┬───────────────────────────┘                          │
  │                    │                                                       │
  │            YES ────┤──── NO                                               │
  │             │              │                                               │
  │             ▼              ▼                                               │
  │         CONVERGED    Continue to                                           │
  │         → Phase 3    next iteration                                        │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │  (if not converged)
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STEP 8 — SWAP AND RESET                                                   │
  │                                                                             │
  │  swap(rank_current, rank_next)   ← O(1) pointer swap                      │
  │  fill(incoming, 0.0)             ← zero the buffer                        │
  │  clear all outboxes              ← already cleared in Step 3              │
  │                                                                             │
  │  → Go back to STEP 1 (BARRIER) for iteration k+1                         │
  └─────────────────────────────────────────────────────────────────────────────┘


 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │  PHASE 3 — RESULT COLLECTION                                                    │
 └─────────────────────────────────────────────────────────────────────────────────┘

  All 4 workers converged at same iteration k
                │
                ▼
  Each worker writes result_worker_K.txt
  (node_id  rank) for every local vertex
                │
                ▼
  Each worker computes local top-100
  using std::nth_element (no full sort)
                │
                ▼
  Workers 1, 2, 3 send LOCAL_TOPK to Worker 0
                │
                ▼
  ┌───────────────────────────┐
  │  Worker 0 (soft leader)   │
  │                           │
  │  Merge 4 × top-100 lists  │
  │  Sort by rank descending  │
  │  Take global top-100      │
  │  Write top_k.txt          │
  └───────────────────────────┘
                │
                ▼
  Worker 0 broadcasts SHUTDOWN to Workers 1, 2, 3
                │
                ▼
  All workers close connections, free memory, exit 0


 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │  PHASE 4 — POST PROCESSING  (offline, after cluster exits)                      │
 └─────────────────────────────────────────────────────────────────────────────────┘

  top_k.txt  +  twitter-2010-ids.csv
                       │
                       ▼
             ┌──────────────────┐
             │  postprocess.py  │
             └────────┬─────────┘
                      │
                      ▼
             top_influencers.txt
             ┌──────────────────────────────────────┐
             │ rank | node_id | twitter_id | score  │
             │   1  |   2415  |   813286   | 0.0023 │
             │   2  |    87   | 14224719   | 0.0019 │
             │  ...                                  │
             └──────────────────────────────────────┘
```

---

## 2. Worker Node Internal State Machine

```
╔══════════════════════════════════════════════════════════════════╗
║              WORKER — INTERNAL STATE MACHINE                     ║
╚══════════════════════════════════════════════════════════════════╝

  START
    │
    ▼
  ┌─────────────────┐
  │  CONNECTING     │  Read cluster.conf
  │                 │  Open listening socket
  │                 │  Connect to all peers (i<j convention)
  └────────┬────────┘
           │ all connections established
           ▼
  ┌─────────────────┐
  │  HANDSHAKING    │  Send HELLO to all peers
  │                 │  Receive HELLO from all peers
  │                 │  Validate cluster size matches config
  └────────┬────────┘
           │ all HELLOs received
           ▼
  ┌─────────────────┐
  │  LOADING        │  Open partition_K.bin
  │                 │  Validate header (magic, worker_id, num_workers)
  │                 │  Load CSR arrays into RAM
  │                 │  Init rank_current = 1/V for all local vertices
  └────────┬────────┘
           │ load complete
           ▼
  ┌─────────────────┐
  │  READY          │  Send READY to all peers
  │                 │  Wait for READY from all peers
  └────────┬────────┘
           │ all READY received
           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                     ITERATING                                │
  │                                                             │
  │   ┌─────────────────────────────────────────────────────┐  │
  │   │  barrier        → sync all workers                  │  │
  │   │  local_compute  → walk vertices, fill outboxes      │  │
  │   │  send_contribs  → send outboxes to peers            │  │
  │   │  recv_contribs  → wait for all peers (barrier)      │  │
  │   │  fold_inboxes   → add received contribs to incoming │  │
  │   │  allreduce_dang → get global dangling mass          │  │
  │   │  apply_formula  → compute rank_next                 │  │
  │   │  allreduce_delt → get global delta                  │  │
  │   │  swap_reset     → rank_current ← rank_next          │  │
  │   └─────────────────────┬───────────────────────────────┘  │
  │                         │                                   │
  │          ┌──────────────┴──────────────┐                   │
  │          │  global_delta < 1e-6 ?       │                   │
  │          └──────┬─────────────┬─────────┘                  │
  │                 │ YES         │ NO                          │
  │                 ▼             └──────────────────┐          │
  │            converged                     repeat  │          │
  │                 │                                │          │
  └─────────────────┼────────────────────────────────┘          │
                    ▼
  ┌─────────────────┐
  │  COLLECTING     │  Write result_worker_K.txt
  │                 │  Compute local top-100
  │                 │  Send LOCAL_TOPK to Worker 0 (or merge if Worker 0)
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  SHUTDOWN       │  Receive SHUTDOWN from Worker 0
  │                 │  Close all connections
  │                 │  Free memory
  └────────┬────────┘
           │
           ▼
         EXIT 0


  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ERROR PATH (any state):

  recv() returns ECONNRESET / EPIPE / ETIMEDOUT
    │
    ▼
  ┌─────────────────┐
  │  ABORTING       │  Log peer failure with timestamp
  │                 │  Broadcast ABORT to all remaining peers
  │                 │  Write partial ranks if any iterations completed
  └────────┬────────┘
           │
           ▼
         EXIT 1
```

---

## 3. Per-Iteration Communication Timeline

```
╔══════════════════════════════════════════════════════════════════════════════╗
║         ITERATION k — COMMUNICATION TIMELINE (4 workers, leaderless)        ║
╚══════════════════════════════════════════════════════════════════════════════╝

         Worker 0        Worker 1        Worker 2        Worker 3
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  START k
            │               │               │               │
            │──BARRIER k───►│               │               │
            │──BARRIER k────────────────────►               │
            │──BARRIER k─────────────────────────────────── ►│
            │◄─BARRIER k────│               │               │
            │◄─BARRIER k───────────────────-│               │
            │◄─BARRIER k────────────────────────────────────│
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  ALL SYNCED
            │               │               │               │
            │  [compute]    │  [compute]    │  [compute]    │  [compute]
            │  walk verts   │  walk verts   │  walk verts   │  walk verts
            │  fill outbox  │  fill outbox  │  fill outbox  │  fill outbox
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  COMPUTE DONE
            │               │               │               │
            │──CONTRIBS────►│               │               │
            │──CONTRIBS─────────────────────►               │
            │──CONTRIBS──────────────────────────────────── ►│
            │◄─CONTRIBS─────│               │               │
            │◄─CONTRIBS─────────────────────│               │
            │◄─CONTRIBS──────────────────────────────────── │
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  ALL CONTRIBS RECEIVED
            │               │               │               │
            │  [fold]       │  [fold]       │  [fold]       │  [fold]
            │               │               │               │
            │──DANG ────────►               │               │
            │──DANG ─────────────────────── ►               │
            │──DANG ──────────────────────────────────────── ►
            │◄─DANG ─────────               │               │
            │◄─DANG ────────────────────────│               │
            │◄─DANG ─────────────────────────────────────── │
            │               │               │               │
            │  [sum→global  │               │               │
            │   dangling]   │               │               │
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  GLOBAL DANGLING KNOWN
            │               │               │               │
            │  [formula]    │  [formula]    │  [formula]    │  [formula]
            │  rank_next    │  rank_next    │  rank_next    │  rank_next
            │               │               │               │
            │──DELTA───────►│               │               │
            │──DELTA─────────────────────── ►               │
            │──DELTA──────────────────────────────────────── ►
            │◄─DELTA──────── │               │               │
            │◄─DELTA─────────────────────── │               │
            │◄─DELTA──────────────────────────────────────── │
            │               │               │               │
            │  [sum→global  │               │               │
            │   delta]      │               │               │
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  CONVERGENCE CHECK
            │               │               │               │
            │  [swap+reset] │  [swap+reset] │  [swap+reset] │  [swap+reset]
            │               │               │               │
  ──────────┼───────────────┼───────────────┼───────────────┼──  END k → START k+1
```

---

## 4. Failure and Recovery Flow

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                   NODE FAILURE — DETECTION AND RECOVERY FLOW                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

  NORMAL RUN                        WORKER 3 GOES OFFLINE
  ─────────────────────────         ────────────────────────────────────────

  W0 W1 W2 W3 all running           W3 process dies at iteration 35
       │                                         │
       ▼                                         ▼
  All reach BARRIER k=35            TCP connection drops
       │                                         │
       ▼                                         ▼
  Send BARRIER to peers             W0, W1, W2 get ECONNRESET or
       │                            recv() times out (SO_RCVTIMEO = 30s)
       ▼                                         │
  ┌──────────────────────┐                       ▼
  │  FAILURE DETECTED    │          First worker to detect:
  │  by W0, W1, or W2    │          broadcasts ABORT to other 2 survivors
  └──────────────────────┘                       │
                                                 ▼
                                    All survivors receive ABORT
                                                 │
                                                 ▼
                                    ┌────────────────────────┐
                                    │  WITHOUT CHECKPOINTS   │
                                    │                        │
                                    │  35 iterations lost    │
                                    │  Restart from iter 1   │
                                    │  Need W3's machine     │
                                    │  back online           │
                                    └────────────────────────┘

                                    ┌────────────────────────┐
                                    │  WITH CHECKPOINTS      │
                                    │  (every 10 iters)      │
                                    │                        │
                                    │  Last checkpoint: k=30 │
                                    │  Only 5 iters lost     │
                                    │                        │
                                    │  W0 loads W3's replica │
                                    │  partition_3.bin       │
                                    │  (pre-copied before    │
                                    │  run started)          │
                                    │                        │
                                    │  All workers rollback  │
                                    │  to checkpoint k=30    │
                                    │                        │
                                    │  Resume with 3 workers │
                                    │  W0 carries double load│
                                    └────────────────────────┘


  LOAD DISTRIBUTION AFTER FAILURE (3 workers):

  Before:                           After (W3 failed, W0 absorbs):
  W0 ████████████  10.4M verts      W0 ████████████████████████  20.8M verts
  W1 ████████████  10.4M verts      W1 ████████████              10.4M verts
  W2 ████████████  10.4M verts      W2 ████████████              10.4M verts
  W3 ████████████  10.4M verts      [W3 offline]

  W0 now takes ~2× longer per iteration
  W1, W2 wait at BARRIER for W0
  Throughput drops ~50%
  Run completes — degraded but correct
```

---

## 5. Data Flow Summary

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                         DATA FLOW — WHAT MOVES WHERE                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

  BEFORE RUN (disk transfers):
  preprocessing machine ──scp──► each worker: partition_K.bin (~800 MB each)

  DURING EACH ITERATION (network transfers, in memory):

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Message         Size per message    Direction         Frequency        │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  BARRIER         5 bytes (header)   peer → peer       N(N-1) per iter  │
  │  CONTRIBS        ~500 MB            peer → peer       N(N-1) per iter  │
  │  ALLREDUCE_DANG  13 bytes           peer → peer       N(N-1) per iter  │
  │  ALLREDUCE_DELTA 13 bytes           peer → peer       N(N-1) per iter  │
  └─────────────────────────────────────────────────────────────────────────┘

  CONTRIBS dominates — ~500 MB × 12 messages = ~6 GB moved per iteration
  Over Gigabit Ethernet (125 MB/s): ~4–8 seconds network time per iteration

  AFTER RUN (disk writes, local):
  each worker ──local write──► result_worker_K.txt (~100 MB)
  worker 0    ──local write──► top_k.txt (tiny)
  worker 0    ──local write──► iteration_log.csv (tiny)
```
