# Team 3 — Coordinator

## Overview

Team 3 builds the coordinator process — the single orchestrator that drives the entire distributed run. The coordinator owns no graph data and does no PageRank math. Its job is to sequence the computation: manage worker registration, broadcast iteration signals, aggregate global statistics (dangling mass and convergence delta), detect convergence, collect and merge results, and shut everything down cleanly.

The coordinator is the first process that must be running before anything else starts. It is also the process that produces the final outputs.

---

## Goals

1. Implement the full coordinator process in C++ as a single binary (`coordinator`).
2. Accept N worker registrations, broadcast peer discovery, and drive the BSP iteration loop.
3. Perform two all-reduce aggregations per iteration: dangling mass and convergence delta.
4. Detect convergence and broadcast STOP; or broadcast STOP after max iterations.
5. Collect per-worker top-K results, merge globally, write `top_k.txt`.
6. Write structured per-iteration logs sufficient to produce all evaluation charts.
7. Handle worker crashes gracefully: detect broken connections, broadcast ABORT, exit with nonzero status.

---

## High-Level Architecture

```
coordinator binary
        │
        ├── Phase 1: Listen → accept N HELLO messages → broadcast CLUSTER_INFO
        │
        ├── Phase 2: Wait for N READY → Wait for N LOAD_COMPLETE
        │
        ├── Phase 3: Iteration Loop (BSP driver)
        │       │
        │       ├── Broadcast START_ITERATION k
        │       ├── Collect N DANGLING_REPORTs → sum → Broadcast GLOBAL_DANGLING
        │       ├── Collect N DELTA_REPORTs → sum global_delta
        │       └── If converged → Broadcast STOP; else → Broadcast START_ITERATION k+1
        │
        ├── Phase 4: Collect N LOCAL_TOPKs → merge → write top_k.txt
        │
        └── Phase 5: Broadcast SHUTDOWN → wait for disconnects → exit 0
```

---

## Module Breakdown

### `coordinator.cpp` — Main entry point

- Parse `cluster.conf`: coordinator port, worker count, worker addresses, damping factor `d`, convergence threshold `epsilon`, max iterations.
- Call `tcp_listen(port)` (Team 2 API).
- Run the phases described above.

### `coordinator/registration.h/.cpp`

Handles Phase 1 and Phase 2:

```cpp
struct WorkerInfo {
    int          fd;           // connected socket
    uint32_t     worker_id;
    std::string  ip;
    uint16_t     port;
    uint64_t     vertex_count;
    uint64_t     edge_count;
};

// Blocks until N workers connect and send HELLO.
// Returns vector of WorkerInfo, indexed by worker_id.
std::vector<WorkerInfo> await_registrations(int server_fd, int num_workers, int timeout_sec = 60);

// Sends CLUSTER_INFO to every registered worker.
void broadcast_cluster_info(const std::vector<WorkerInfo>& workers);

// Waits for READY from all workers, then LOAD_COMPLETE from all workers.
void await_ready_and_loaded(const std::vector<WorkerInfo>& workers);
```

Validation in `await_ready_and_loaded`:
- Sum of all workers' `vertex_count` must equal `total_global_vertices` (from `cluster.conf` or partition manifest).
- Log per-worker vertex and edge counts for the report.

### `coordinator/iteration_driver.h/.cpp`

The BSP loop:

```cpp
struct IterationResult {
    int      iteration;
    double   global_delta;
    double   global_dangling;
    double   wall_ms;       // total wall time for this iteration
    double   compute_ms;    // estimated (based on when DELTA_REPORTs arrived)
    double   comm_ms;       // wall_ms - compute_ms
};

// Runs iterations until convergence or max_iterations.
// Returns iteration log for all completed iterations.
std::vector<IterationResult> run_iterations(
    const std::vector<WorkerInfo>& workers,
    double epsilon,
    int max_iterations
);
```

Per-iteration logic:

1. Broadcast `START_ITERATION k` to all workers (Team 2 `send_message`).
2. Receive N `DANGLING_REPORT` messages (one per worker), sum into `global_dangling`.
3. Broadcast `GLOBAL_DANGLING` to all workers.
4. Receive N `DELTA_REPORT` messages (one per worker), sum into `global_delta`.
5. Log the iteration (see Logging section below).
6. If `global_delta < epsilon` or `k >= max_iterations`: broadcast `STOP`, return.
7. Else: `k++`, go to step 1.

> **Ordering constraint:** GLOBAL_DANGLING must be broadcast before workers apply the PageRank formula. Workers must finish applying the formula before they can compute and send DELTA_REPORT. This ordering is preserved naturally because workers wait for GLOBAL_DANGLING before computing ranks.

### `coordinator/aggregator.h`

Thin helpers:

```cpp
double collect_and_sum_dangling(const std::vector<WorkerInfo>& workers, uint32_t iteration_k);
double collect_and_sum_delta(const std::vector<WorkerInfo>& workers, uint32_t iteration_k);
```

Each blocks until N messages of the given type arrive. Uses Team 2's `recv_message`.

### `coordinator/result_merger.h/.cpp`

Post-convergence output:

```cpp
struct RankedNode {
    uint32_t node_id;
    double   rank;
};

// Collects LOCAL_TOPK from all workers, merges, sorts, returns top K.
std::vector<RankedNode> collect_and_merge_topk(
    const std::vector<WorkerInfo>& workers,
    int k = 100
);

// Writes top_k.txt: one "node_id rank" pair per line, sorted descending by rank.
void write_topk(const std::vector<RankedNode>& results, const std::string& path);
```

### `coordinator/logger.h/.cpp`

Structured per-iteration logging:

```cpp
void log_iteration(const IterationResult& r);
// Writes to stdout and to coordinator.log:
// iter  1: delta=8.21e-02 dangling=2.14e-03 compute=195ms comm=248ms total=443ms
```

At end of run, also write a machine-readable `iteration_log.csv` for chart generation:

```
iteration,global_delta,global_dangling,compute_ms,comm_ms,total_ms
1,8.21e-02,2.14e-03,195,248,443
...
```

This CSV is the data source for Experiments 2 and 3 in the evaluation.

---

## Configuration File (`cluster.conf`)

```ini
coordinator_host = 192.168.1.10
coordinator_port = 9000
num_workers = 4
damping_factor = 0.85
convergence_threshold = 1e-6
max_iterations = 100
top_k = 100

[worker_0]
host = 192.168.1.11
port = 9001

[worker_1]
host = 192.168.1.12
port = 9002

[worker_2]
host = 192.168.1.13
port = 9003

[worker_3]
host = 192.168.1.14
port = 9004
```

Both coordinator and workers read this file. Workers use it to know where to connect; coordinator uses it to know how many workers to expect.

---

## Error Handling

The scope is deliberate: no fault tolerance, no recovery. If anything goes wrong:

- Worker crash: `recv_message` returns an error (broken pipe / connection reset). Coordinator detects this, broadcasts `ABORT` to all remaining workers (best-effort), logs the failure, exits with status 1.
- Registration timeout: if fewer than N workers register within 60 seconds, log which IDs are missing and exit with status 1.
- Iteration count exceeded: if `k > max_iterations` without convergence, log a warning, still write whatever ranks exist, broadcast SHUTDOWN normally.

---

## Deliverables

| Deliverable | When | Format |
|-------------|------|--------|
| `cluster.conf` schema + parser | End of Week 1 | C++ |
| `coordinator/registration.h/.cpp` | End of Week 2 | C++ |
| `coordinator/iteration_driver.h/.cpp` | End of Week 2 | C++ |
| `coordinator/aggregator.h` | End of Week 2 | C++ |
| `coordinator/result_merger.h/.cpp` | End of Week 3 | C++ |
| `coordinator/logger.h/.cpp` + `iteration_log.csv` output | End of Week 2 | C++ |
| Full coordinator binary running on localhost with stub workers | End of Week 2 | Binary |
| End-to-end coordinator + real workers | End of Week 3 | |

---

## Testing Plan

### Unit Tests

- **Registration timeout:** mock server where only 3 of 4 workers connect; verify coordinator aborts after 60s.
- **CLUSTER_INFO broadcast:** verify all 4 workers receive correct peer lists.
- **Aggregation:** simulate N workers sending known delta values; verify coordinator sums them exactly.
- **Convergence detection:** send deltas `[0.5, 0.3, 0.1, 0.05]` for iterations 1–4 (total > 1e-6 for first 3, total < 1e-6 on iteration 4); verify STOP is broadcast on iteration 4.
- **Top-K merge:** send 4 partial top-100 lists with known values; verify global top-100 is sorted correctly and contains the right entries.

### Integration Tests

- **Stub worker test (Week 2):** write a minimal stub worker binary (Team 2 + Team 3 only, no algorithm). It connects, registers, sends fixed DELTA_REPORT and DANGLING_REPORT values each iteration, and exits on SHUTDOWN. Run with N=4 on localhost. Verify the coordinator drives 10 iterations and produces a `top_k.txt`.
- **Full system test (Week 3):** run with real worker binary on small graph. Verify `iteration_log.csv` has correct row count, final delta is below threshold.

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Race between GLOBAL_DANGLING broadcast and DELTA_REPORT receive | Protocol order is strict: coordinator broadcasts GLOBAL_DANGLING, then waits for DELTA_REPORTs. Workers cannot send DELTA_REPORT before receiving GLOBAL_DANGLING (they need it to compute the formula). No extra locking needed. |
| Worker sends DELTA_REPORT before DANGLING_REPORT (in wrong order) | `collect_and_sum_dangling` and `collect_and_sum_delta` each filter by message type; out-of-order arrivals are re-queued |
| Top-K merge produces wrong global result | Unit test with synthetic values that stress the boundary (e.g., best entry of worker 3 is better than worst entry of worker 0's top-100) |
| Log file grows large on long runs | Log one line per iteration; 100 iterations × ~100 bytes = 10 KB, negligible |

---

## Interface Contracts (Consume These)

**From Team 2:**
- `send_message(fd, type, payload, len)`
- `recv_message(fd)` → `Message`
- `tcp_listen(port)` → server fd
- `tcp_accept(server_fd)` → client fd
- All message type definitions and serializers/deserializers

**You produce → Team 4 consumes (indirectly via wire):**
- START_ITERATION signals (iteration number in payload)
- GLOBAL_DANGLING broadcasts (one per iteration)
- STOP signal
- SHUTDOWN signal
