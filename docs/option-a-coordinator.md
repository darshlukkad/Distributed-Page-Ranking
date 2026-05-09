# Option A — Coordinator-Based Architecture

## Decision Summary

| | |
|---|---|
| **Workers** | 8 |
| **Coordinator** | 1 dedicated process (no graph data) |
| **Total machines** | 9 |
| **TCP connections** | 36 (28 data-plane + 8 control-plane) |
| **Switch** | 16-port Gigabit |
| **Risk level** | Lower — simpler to debug |
| **Demo story** | "8-worker distributed cluster with central orchestration" |

---

## How It Works

```
                    ┌─────────────────────┐
                    │     COORDINATOR     │
                    │   (no graph data)   │
                    │                     │
                    │  - Drives iteration │
                    │  - Aggregates delta │
                    │  - Detects failure  │
                    │  - Merges top-K     │
                    └──────────┬──────────┘
                               │ control plane (8 connections)
          ┌────────────────────┼────────────────────┐
          │          │         │         │           │
       W0  W1        W2        W3        W4   W5    W6  W7
          └──────────┴─── data plane mesh ───┴──────┘
                         (28 connections)
```

### Per-Iteration Flow

```
Coordinator broadcasts START_ITERATION k
       │
Workers compute locally (walk vertices, fill outboxes)
       │
Workers exchange CONTRIBS over data-plane mesh (barrier)
       │
Each worker sends DANGLING_REPORT to coordinator
Coordinator sums → broadcasts GLOBAL_DANGLING back
       │
Workers apply PageRank formula
Each worker sends DELTA_REPORT to coordinator
Coordinator sums → checks threshold
       │
If converged → broadcasts STOP
Else → broadcasts START_ITERATION k+1
```

---

## Infrastructure

| Item | Spec | Cost |
|---|---|---|
| Switch | **16-port** Gigabit (TP-Link TL-SG116) | ~$35 |
| Ethernet cables | 9 × Cat6 | ~$15 |
| USB-C adapters | 9 × Gigabit | ~$135 |
| Machines | 9 laptops (1 coordinator + 8 workers) | existing |

**RAM per worker:** ~1 GB (partition ~800 MB + rank arrays ~250 MB)

**Disk per worker:** 3 GB free (800 MB partition + binary + output)

---

## Team Structure

| Group | Owns | Depends on |
|---|---|---|
| **Group A** — Algorithm | Worker binary, sequential PageRank, iteration loop | Group B API, Group C partition files |
| **Group B** — Infrastructure | TCP layer, mesh, coordinator binary, cluster.conf | Nothing — unblocks everyone |
| **Group C** — Data | preprocessor.py, partition files, postprocess.py | Nothing |
| **Group D** — Integration | Build system, experiments, plots, demo | All groups |

---

## Coordinator Responsibilities

```
Phase 1 — Bootstrap
  Accept N HELLO messages from workers
  Broadcast CLUSTER_INFO (peer list) to all workers
  Wait for N READY messages
  Wait for N LOAD_COMPLETE messages

Phase 2 — Iteration loop (per iteration)
  Broadcast START_ITERATION k
  Collect N DANGLING_REPORTs → sum → broadcast GLOBAL_DANGLING
  Collect N DELTA_REPORTs → sum global_delta
  If global_delta < 1e-6 → broadcast STOP
  Else → broadcast START_ITERATION k+1

Phase 3 — Results
  Collect N LOCAL_TOPKs → merge → write top_k.txt
  Broadcast SHUTDOWN

Error handling
  Any recv() failure → broadcast ABORT → exit 1
```

### Key modules (Group B builds these)

```
coordinator/
  registration.h/.cpp    ← Phase 1
  iteration_driver.h/.cpp ← Phase 2 loop
  aggregator.h           ← sum dangling + delta
  result_merger.h/.cpp   ← top-K merge
  logger.h/.cpp          ← iteration_log.csv
```

---

## Worker Iteration Loop (Option A)

```cpp
// Simplified per-iteration logic
void run_iteration(uint32_t k, double global_dangling) {

    // Step 1: local compute
    double local_dangling = walk_and_emit();

    // Step 2: network exchange
    send_all_contribs(k);
    recv_all_contribs(k);   // barrier — waits for N-1

    // Step 3: fold inboxes
    fold_inboxes();

    // Step 4: dangling round-trip with coordinator
    send_dangling_report(coordinator_fd, k, local_dangling);
    double global_dangling = recv_global_dangling(coordinator_fd, k);

    // Step 5: apply formula
    double local_delta = apply_formula(global_dangling);

    // Step 6: report delta to coordinator
    send_delta_report(coordinator_fd, k, local_delta);

    // Step 7: swap and reset
    swap_and_reset();
}
```

---

## Message Protocol (Option A)

```
Control plane (worker ↔ coordinator):
  HELLO           worker → coordinator
  CLUSTER_INFO    coordinator → worker
  READY           worker → coordinator
  LOAD_COMPLETE   worker → coordinator
  START_ITERATION coordinator → worker
  DANGLING_REPORT worker → coordinator
  GLOBAL_DANGLING coordinator → worker
  DELTA_REPORT    worker → coordinator
  STOP            coordinator → worker
  LOCAL_TOPK      worker → coordinator
  SHUTDOWN        coordinator → worker
  ABORT           coordinator → worker

Data plane (worker ↔ worker):
  CONTRIBS        worker → worker
```

---

## Pros

- **Simpler to debug** — coordinator sees all global state in one process. One log file tells you everything going wrong.
- **Failure detection is built-in** — coordinator detects broken worker connections immediately via recv() error and broadcasts ABORT.
- **Clearer separation of concerns** — coordinator owns orchestration, workers own computation. Teams don't step on each other.
- **Well-documented design** — the original architecture.md fully specifies this option. No ambiguity.
- **Scales to any N** — adding workers requires no protocol changes. Just update cluster.conf.
- **Textbook BSP** — professors will immediately recognise and appreciate the clean design.

## Cons

- **9 machines required** — coordinator needs a dedicated laptop. 9 machines at the demo.
- **Single point of failure** — coordinator dies, everything dies. No failover.
- **Coordinator is a bottleneck at large N** — at very high worker counts, all control messages funnel through one process. Not a concern at N=8 but limits future scaling.
- **More code** — coordinator binary is ~600 lines of C++ that workers do not need in Option B.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|---|---|---|
| Coordinator crashes during demo | Low | Run dry-run the night before |
| Message ordering bug (GLOBAL_DANGLING before DELTA_REPORT) | Medium | Protocol sequence is strictly enforced in iteration_driver |
| Worker registration timeout | Low | 60-second timeout — plenty for startup |
| One of 9 machines fails at demo | Medium | Test all hardware day before |

**Overall risk: LOW** — this is the simpler design with more implementation guidance available.

---

## Timeline (7 days)

| Day | Milestone |
|---|---|
| May 9 | format.md + protocol.md published. Group C preprocessor on small graph. Group B socket_utils + message framing. Group A sequential PageRank started. |
| May 10–11 | Group B coordinator registration + iteration driver. Group A partition loader + rank arrays. Group C full partition files. |
| May 12–13 | End-to-end localhost. Cross-machine. Correctness validated (L1 < 1e-6). |
| May 14 | All 5 experiments run. |
| May 15 | Code freeze. |
