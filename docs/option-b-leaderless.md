# Option B — Leaderless Architecture

## Decision Summary

| | |
|---|---|
| **Workers** | 4 |
| **Coordinator** | None — every node is equal |
| **Total machines** | 4 |
| **TCP connections** | 6 (full mesh, all data-plane) |
| **Switch** | 8-port Gigabit |
| **Risk level** | Medium — distributed debugging is harder |
| **Demo story** | "4 equal laptops, no master, pure peer-to-peer" |

---

## How It Works

```
  Worker 0 ◄──────────────────────────────► Worker 1
     ▲  ▲                                      ▲  ▲
     │   └──────────────────────────────────┘   │
     │                                          │
     ▼                                          ▼
  Worker 3 ◄──────────────────────────────► Worker 2

  6 bidirectional TCP connections.
  No coordinator. No master node. Every worker is identical.
```

### Per-Iteration Flow

```
All 4 workers simultaneously:

  ① BARRIER         send BARRIER_k to 3 peers, wait for 3 BARRIER_k
  ② LOCAL COMPUTE   walk vertices, fill outboxes + local incoming[]
  ③ CONTRIBS SEND   send outboxes to 3 peers
  ④ CONTRIBS RECV   wait for 3 CONTRIBS (second barrier)
  ⑤ FOLD INBOXES    add received contributions to incoming[]
  ⑥ ALLREDUCE DANG  broadcast local_dangling, collect 3, sum → global_dangling
  ⑦ APPLY FORMULA   compute rank_next using global_dangling
  ⑧ ALLREDUCE DELTA broadcast local_delta, collect 3, sum → global_delta
  ⑨ CHECK           if global_delta < 1e-6 → converge, else swap + reset → ①
```

### All-Reduce Pattern (replaces coordinator aggregation)

```
Each worker independently arrives at the same global value:

  W0 sends local_dangling=0.0021 to W1, W2, W3
  W1 sends local_dangling=0.0019 to W0, W2, W3
  W2 sends local_dangling=0.0022 to W0, W1, W3
  W3 sends local_dangling=0.0021 to W0, W1, W2

  Each worker receives 3 values, sums all 4 (including own):
  global_dangling = 0.0021 + 0.0019 + 0.0022 + 0.0021 = 0.0083

  All 4 workers get exactly 0.0083 — no coordinator needed.
```

---

## Infrastructure

| Item | Spec | Cost |
|---|---|---|
| Switch | **8-port** Gigabit (TP-Link TL-SG108) | ~$20 |
| Ethernet cables | 4 × Cat6 | ~$10 |
| USB-C adapters | 4 × Gigabit | ~$60 |
| Machines | 4 laptops | existing |

**Saves vs Option A:** 5 machines, smaller switch, 5 fewer cables and adapters (~$100 less hardware, 5 fewer laptops needed at demo)

**RAM per worker:** ~1 GB

**Disk per worker:** 3 GB free

---

## Team Structure

| Group | Owns | Depends on |
|---|---|---|
| **Group A** — Algorithm | Worker binary, sequential PageRank, iteration loop with all-reduce calls | Group B API, Group C partition files |
| **Group B** — Infrastructure | TCP layer, mesh, all-reduce primitive, cluster.conf, Worker 0 soft-leader output | Nothing — unblocks everyone |
| **Group C** — Data | preprocessor.py, partition files, postprocess.py | Nothing |
| **Group D** — Integration | Build system, experiments, plots, demo | All groups |

**Note:** Group B absorbs what was Team 3 (coordinator). The all-reduce primitive replaces the coordinator's aggregation functions. Net code reduction vs Option A.

---

## Worker Iteration Loop (Option B)

```cpp
// Simplified per-iteration logic — no coordinator involved
void run_iteration(uint32_t k) {

    // Step 1: distributed barrier
    barrier(peer_fds, k);

    // Step 2: local compute
    double local_dangling = walk_and_emit();

    // Step 3 & 4: CONTRIBS exchange (existing barrier)
    send_all_contribs(k);
    recv_all_contribs(k);

    // Step 5: fold inboxes
    fold_inboxes();

    // Step 6: all-reduce dangling mass
    double global_dangling = allreduce_sum(peer_fds, k, local_dangling);

    // Step 7: apply formula
    double local_delta = apply_formula(global_dangling);

    // Step 8: all-reduce convergence delta
    double global_delta = allreduce_sum(peer_fds, k, local_delta);

    // Step 9: every worker independently makes the same decision
    if (global_delta < epsilon) converged = true;

    // Step 10: swap and reset
    if (!converged) swap_and_reset();
}
```

### All-Reduce Implementation (Group B builds this)

```cpp
// Send local value to all peers, collect all, return sum
double allreduce_sum(const PeerMap& peers, uint32_t iter_k, double local_val) {

    // Send to all peers
    for (auto& [peer_id, fd] : peers)
        send_allreduce(fd, iter_k, local_val);

    // Collect from all peers
    double total = local_val;
    for (int i = 0; i < (int)peers.size(); i++) {
        auto msg = recv_allreduce(/* any peer */);
        total += msg.value;
    }
    return total;
}
```

---

## Message Protocol (Option B)

```
All messages are peer-to-peer only — no coordinator:

  HELLO           worker → all peers  (startup)
  READY           worker → all peers  (after mesh formed)
  BARRIER         worker → all peers  (iteration sync)
  CONTRIBS        worker → worker     (rank contributions)
  ALLREDUCE_DANG  worker → all peers  (local dangling mass)
  ALLREDUCE_DELTA worker → all peers  (local convergence delta)
  LOCAL_TOPK      worker → Worker 0   (final output only)
  SHUTDOWN        Worker 0 → all      (after writing top_k.txt)
  ABORT           any worker → all    (on detected failure)

Removed vs Option A:
  ✗ CLUSTER_INFO, START_ITERATION, GLOBAL_DANGLING
  ✗ DELTA_REPORT, DANGLING_REPORT, STOP
```

**Message count per iteration:** 12 BARRIER + 12 CONTRIBS + 12 ALLREDUCE_DANG + 12 ALLREDUCE_DELTA = 48 messages (vs 24 in Option A, but all CONTRIBS dominate in bytes anyway)

---

## Startup Sequence

```
No coordinator to wait for. Workers start in any order.

Each worker:
  1. Read cluster.conf (has all 4 peer addresses)
  2. Open listening socket on own port
  3. Connect to peers where my_id < peer_id
  4. Accept connections from peers where my_id > peer_id
  5. Send HELLO to all 3 peers (worker_id, vertex_count, edge_count)
  6. Wait for HELLO from all 3 peers
  7. Load partition_K.bin
  8. Send READY to all 3 peers
  9. Wait for READY from all 3 peers
  10. Begin iteration loop

Start command (any order, any terminal):
  ./worker --id 0 --config cluster.conf --partition partition_0.bin
  ./worker --id 1 --config cluster.conf --partition partition_1.bin
  ./worker --id 2 --config cluster.conf --partition partition_2.bin
  ./worker --id 3 --config cluster.conf --partition partition_3.bin
```

---

## Result Collection (Worker 0 as Soft Leader)

Worker 0 is a soft leader only for the final output step — it is NOT a coordinator. It runs the full PageRank computation identically to all other workers.

```
After convergence:

All workers:
  → Write result_worker_K.txt to local disk
  → Compute local top-100 using std::nth_element

Workers 1, 2, 3:
  → Send LOCAL_TOPK to Worker 0

Worker 0:
  → Receives LOCAL_TOPK from Workers 1, 2, 3
  → Merges with own top-100
  → Writes top_k.txt
  → Broadcasts SHUTDOWN to Workers 1, 2, 3
```

---

## Pros

- **4 machines only** — simpler demo setup, less hardware to buy/carry/configure.
- **No single point of failure** — no coordinator to crash and kill the run.
- **Stronger academic story** — true peer-to-peer distributed computing. More interesting for an HPC course.
- **Less code overall** — no coordinator binary. Group B builds all-reduce instead of coordinator state machine. Similar effort, less total surface area.
- **Symmetric design** — every node runs the same binary with only `--id` as a difference.
- **Deterministic convergence** — all-reduce means every worker makes the same stop/continue decision from the same global_delta. No risk of coordinator/worker disagreement.

## Cons

- **Harder to debug** — no central process seeing all global state. Bugs manifest across 4 logs simultaneously.
- **All-reduce must be correct** — if any worker sends its all-reduce message at the wrong time, all workers get a wrong global_dangling or global_delta. Silent wrong results are possible.
- **No natural monitoring point** — in Option A, the coordinator log shows every iteration's delta, timing, and dangling mass in one file. In Option B, you correlate 4 log files.
- **N=4 only for leaderless discussion** — scaling to N=8 leaderless means 28 all-reduce messages per iteration (N(N-1)) vs 8 for coordinator. Still viable but network overhead grows faster.
- **Less implementation guidance** — the original architecture.md describes Option A in full detail. Option B requires more design judgment from the team.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|---|---|---|
| All-reduce sends wrong iteration number | Medium | Tag every all-reduce with `iteration_k`, validate on recv |
| Worker sends ALLREDUCE before CONTRIBS received | Medium | Strict step ordering in iteration loop — CONTRIBS barrier must complete before any all-reduce |
| Hung worker (not crashed) blocks cluster silently | Medium | Set SO_RCVTIMEO = 30s on all sockets |
| Worker 0 soft-leader dies after convergence | Low | Other workers have result files on local disk |
| Debugging takes longer than estimated | High | Add iteration_k and worker_id to every log line from Day 1 |

**Overall risk: MEDIUM** — primarily from the all-reduce correctness requirement and harder debugging.

---

## Critical Implementation Rule

The all-reduce steps (⑥ and ⑧) must happen **after** the CONTRIBS barrier (step ④) completes. Never before.

```
WRONG — causes silent wrong results:
  send CONTRIBS to peers
  send ALLREDUCE_DANG to peers  ← too early, CONTRIBS not all received yet
  recv CONTRIBS from peers

CORRECT:
  send CONTRIBS to peers
  recv CONTRIBS from peers      ← full barrier, wait for all 3
  fold inboxes
  send ALLREDUCE_DANG to peers  ← now safe
```

---

## Timeline (7 days)

| Day | Milestone |
|---|---|
| May 9 | format.md + protocol.md for leaderless published. Group C preprocessor on small graph. Group B socket_utils + message framing + all-reduce. Group A sequential PageRank started. |
| May 10–11 | Group B completes all-reduce + CONTRIBS barrier. Group A partition loader + full iteration loop integrated. Group C full partition files. |
| May 12–13 | End-to-end localhost with 4 processes. Cross-machine test. Correctness validated. |
| May 14 | All 5 experiments. |
| May 15 | Code freeze. |
