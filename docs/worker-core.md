# Team 4 — Worker & PageRank Core

## Overview

Team 4 builds the worker process — the computational engine of the system. Workers own the graph data, run the PageRank iterations, exchange rank contributions with each other, and produce the final rank values. This is where almost all CPU time is spent and where correctness is most critical.

Your first task (Week 1) is a **sequential single-machine PageRank** that reads a partition file and runs the algorithm without any networking. This becomes the correctness reference that all distributed results are validated against.

---

## Goals

1. Build a sequential single-machine PageRank in C++ that reads SNAP edge lists and produces correct ranks (validated against NetworkX).
2. Build the worker binary that loads a `partition_K.bin` file into an in-memory CSR structure.
3. Implement the full 8-step per-iteration loop: emit contributions, exchange via network, fold inboxes, apply PageRank formula, compute convergence delta.
4. Integrate with Team 2's networking layer for contribution exchange and Team 3's coordinator signals.
5. Compute local top-K after convergence, write `result_worker_K.txt`, send LOCAL_TOPK to coordinator.
6. Pass correctness validation: distributed results must match the sequential reference to within L1 tolerance of `1e-6`.

---

## High-Level Architecture

```
Worker binary
        │
        ├── Phase 1: Connect to coordinator → send HELLO
        ├── Phase 2: Receive CLUSTER_INFO → setup_mesh() (Team 2)
        ├── Phase 3: Load partition_K.bin → init rank arrays → send LOAD_COMPLETE
        │
        ├── Phase 4: Iteration Loop
        │       │
        │       ├── Wait for START_ITERATION k
        │       ├── Step 1: Walk vertices → fill outboxes + local incoming[]
        │       ├── Step 2: Exchange CONTRIBS (send outboxes, recv_all_contribs)
        │       ├── Step 3: Fold inboxes into incoming[]
        │       ├── Step 4: Send DANGLING_REPORT → Recv GLOBAL_DANGLING
        │       ├── Step 5: Apply PageRank formula → compute local_delta
        │       ├── Step 6: Send DELTA_REPORT
        │       └── Step 7: Swap rank pointers, zero incoming[], clear outboxes
        │                 (loop until STOP received)
        │
        ├── Phase 5: Write result_worker_K.txt → compute top-100 → send LOCAL_TOPK
        └── Phase 6: Receive SHUTDOWN → close connections → exit 0
```

---

## Module Breakdown

### `worker/partition_loader.h/.cpp`

Load `partition_K.bin` into memory:

```cpp
struct Partition {
    uint32_t worker_id;
    uint32_t num_workers;
    uint64_t total_global_vertices;  // V = 41,652,230 for full graph
    uint64_t num_local_vertices;
    uint64_t num_local_edges;

    std::vector<uint32_t> local_vertex_ids;  // global IDs of local vertices
    std::vector<uint64_t> edge_offsets;      // CSR row pointers, length = num_local_vertices + 1
    std::vector<uint32_t> edges;             // flat edge array (destination global IDs)
};

Partition load_partition(const std::string& path);
```

Validation on load:
- Check magic number and version.
- Check `worker_id` matches the ID passed in at launch.
- Check `num_workers` matches `cluster.conf`.
- Check `edge_offsets[num_local_vertices]` equals `num_local_edges`.

### `worker/rank_arrays.h`

Simple struct holding the four per-worker arrays:

```cpp
struct RankArrays {
    std::vector<double> rank_current;    // length = num_local_vertices
    std::vector<double> rank_next;       // length = num_local_vertices
    std::vector<double> incoming;        // accumulates contributions this iteration
    std::vector<std::vector<std::pair<uint32_t,double>>> outboxes;  // [num_workers-1]
};

RankArrays init_rank_arrays(const Partition& p);
```

Initialization:
- `rank_current[i] = 1.0 / total_global_vertices` for all i.
- `rank_next`: uninitialized (will be overwritten each iteration).
- `incoming`: all zeros.
- `outboxes`: empty vectors, one per peer worker.

### `worker/iteration.h/.cpp`

The core of the system. Called once per iteration:

```cpp
struct IterationStats {
    double local_dangling_mass;
    double local_delta;
    double compute_ms;
    double comm_ms;
};

IterationStats run_iteration(
    const Partition&               partition,
    RankArrays&                    arrays,
    const std::unordered_map<int,int>& peer_fds,   // peer_worker_id → fd (Team 2)
    int                            coordinator_fd,
    uint32_t                       iteration_k,
    double                         global_dangling,  // received from coordinator
    double                         damping_factor
);
```

#### Step 1 — Walk vertices, emit contributions

```cpp
double local_dangling_mass = 0.0;

for (uint64_t i = 0; i < partition.num_local_vertices; i++) {
    uint64_t start = partition.edge_offsets[i];
    uint64_t end   = partition.edge_offsets[i + 1];
    uint32_t out_degree = end - start;

    if (out_degree == 0) {
        local_dangling_mass += arrays.rank_current[i];
        continue;
    }

    double contrib = arrays.rank_current[i] / out_degree;

    for (uint64_t e = start; e < end; e++) {
        uint32_t dest  = partition.edges[e];
        uint32_t owner = dest % partition.num_workers;

        if (owner == partition.worker_id) {
            uint64_t local_idx = dest / partition.num_workers;
            arrays.incoming[local_idx] += contrib;
        } else {
            arrays.outboxes[owner].emplace_back(dest, contrib);
        }
    }
}
```

> **Key arithmetic:** local index of global vertex `v` on worker `w` = `v / N`. This is the O(1) lookup that makes hash partitioning efficient. No lookup table.

#### Step 2 — Network exchange (barrier)

```cpp
// Send outboxes
for (auto& [peer_id, fd] : peer_fds) {
    auto msg = serialize_contribs(iteration_k, arrays.outboxes[peer_id]);
    send_message(fd, CONTRIBS, msg.data(), msg.size());
    arrays.outboxes[peer_id].clear();
}

// Receive all inboxes (barrier — blocks until N-1 CONTRIBS messages arrive)
auto inbox_messages = recv_all_contribs(peer_fds, iteration_k, partition.num_workers);
```

#### Step 3 — Fold inboxes

```cpp
for (auto& msg : inbox_messages) {
    auto pairs = deserialize_contribs(msg);
    for (auto [dest, contrib] : pairs) {
        uint64_t local_idx = dest / partition.num_workers;
        arrays.incoming[local_idx] += contrib;
    }
}
```

#### Step 4 — Dangling mass aggregation

```cpp
// Send local dangling mass to coordinator
send_dangling_report(coordinator_fd, iteration_k, local_dangling_mass);

// Receive global dangling mass back
double global_dangling = recv_global_dangling(coordinator_fd, iteration_k);
```

#### Step 5 — Apply PageRank formula

```cpp
double d = damping_factor;  // 0.85
double N = (double)partition.total_global_vertices;
double teleport = (1.0 - d) / N;
double dangling_term = d * global_dangling / N;

double local_delta = 0.0;

for (uint64_t i = 0; i < partition.num_local_vertices; i++) {
    arrays.rank_next[i] = teleport
                        + d * arrays.incoming[i]
                        + dangling_term;
    local_delta += std::abs(arrays.rank_next[i] - arrays.rank_current[i]);
}
```

**Formula decomposed:**
- `teleport` = `(1-d)/N` — uniform rank distributed to every vertex.
- `d * incoming[i]` — rank flowing in through actual links.
- `dangling_term` = `d * global_dangling / N` — redistributed dangling mass.

Do **not** apply `d` during contribution emission. Apply it exactly once here, after all contributions are accumulated. Applying at emission time would double-count on cross-worker edges.

#### Step 6 — Report convergence delta

```cpp
send_delta_report(coordinator_fd, iteration_k, local_delta);
// (Coordinator will broadcast STOP or START_ITERATION k+1 next)
```

#### Step 7 — Swap and reset

```cpp
std::swap(arrays.rank_current, arrays.rank_next);
std::fill(arrays.incoming.begin(), arrays.incoming.end(), 0.0);
// outboxes already cleared in Step 2
```

### `worker/topk.h/.cpp`

After convergence, extract local top-K:

```cpp
// Returns top-K (node_id, rank) pairs from local vertices, sorted descending.
std::vector<std::pair<uint32_t,double>> compute_local_topk(
    const Partition&    partition,
    const RankArrays&   arrays,
    int k = 100
);
```

Use `std::partial_sort` or `std::nth_element` — do not full-sort 10M elements.

### `worker/output_writer.h/.cpp`

```cpp
// Writes result_worker_K.txt: one "node_id rank" pair per line.
void write_result_file(
    const Partition&  partition,
    const RankArrays& arrays,
    const std::string& path
);
```

### `sequential_pagerank.cpp` (Week 1 standalone)

A self-contained C++ binary for correctness validation:
- Reads a plain-text SNAP edge list (already reversed, or with `--reverse` flag).
- Runs PageRank to convergence on a single machine.
- Outputs `(node_id, rank)` to stdout.
- Used in Experiment 1 to diff against distributed results.

---

## Sequential Reference (Week 1 Priority)

This is your most important Week 1 deliverable. Before writing any networking code, the sequential implementation must:

1. Run on the SNAP Pokec dataset (~1.6M nodes, 30M edges) and converge in under 60 iterations.
2. Produce ranks that match NetworkX's PageRank (same damping factor, same convergence threshold) with L1 error < 1e-6.
3. Handle dangling vertices correctly (verify `sum(ranks) ≈ 1.0` after each iteration).

This binary is the ground truth for the entire project.

---

## Memory Sizing (Twitter-2010, N=4)

| Array | Entries | Per-entry | Size |
|-------|---------|-----------|------|
| `rank_current` | 10.4M | 8 B (double) | ~83 MB |
| `rank_next` | 10.4M | 8 B | ~83 MB |
| `incoming` | 10.4M | 8 B | ~83 MB |
| `local_vertex_ids` | 10.4M | 4 B (uint32) | ~42 MB |
| `edge_offsets` | 10.4M + 1 | 8 B (uint64) | ~83 MB |
| `edges` | 367M | 4 B (uint32) | ~1.47 GB |
| **Total** | | | **~1.85 GB** |

Each worker needs ~2 GB of RAM. A 16 GB laptop is fine for N=4.

---

## Deliverables

| Deliverable | When | Format |
|-------------|------|--------|
| `sequential_pagerank.cpp` — standalone, validated against NetworkX | End of Week 1 | C++ binary |
| `worker/partition_loader.h/.cpp` — loads and validates `partition_K.bin` | End of Week 1 | C++ |
| `worker/rank_arrays.h` — init arrays | End of Week 1 | C++ |
| `worker/iteration.h/.cpp` — full 7-step iteration loop | End of Week 2 | C++ |
| `worker/topk.h/.cpp` | End of Week 3 | C++ |
| `worker/output_writer.h/.cpp` | End of Week 3 | C++ |
| Full worker binary integrated with Teams 2 & 3 | End of Week 3 | Binary |
| Correctness validation: distributed ranks match sequential on small graph | End of Week 3 | Test output |

---

## Testing Plan

### Unit Tests

- **Partition loader:** load a synthetic 5-vertex partition file; assert header fields, vertex IDs, edge_offsets, and edges match expected values.
- **Out-degree = 0 (dangling):** vertex with equal consecutive offsets must add its rank to `local_dangling_mass` and emit no contributions.
- **Local index arithmetic:** for worker 2, N=4, vertex 18 → local_idx = 18/4 = 4. Test several values.
- **Owner arithmetic:** vertex 4421, N=4 → owner = 4421%4 = 1. Test cross-worker routing.
- **Formula:** given `incoming[i]=0.001`, `global_dangling=0.01`, `d=0.85`, `N=100`, verify `rank_next[i]` equals the hand-computed value.
- **Rank sum:** after one iteration on a small closed graph, verify `sum(rank_next) ≈ 1.0` (within floating point tolerance).

### Integration Tests

- **Sequential vs NetworkX (Week 1):** run on Pokec dataset; L1 diff < 1e-6.
- **Partition loader round-trip:** preprocess a 100-edge synthetic graph with Team 1's preprocessor; load with Team 4's loader; verify edge count and vertex list match source.
- **Distributed vs sequential (Week 3 milestone):** run distributed on first 10M edges of Twitter-2010 with N=4; compare final ranks from all workers against sequential reference; assert global L1 < 1e-6.

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Dangling mass forgotten | Sanity check: assert `sum(rank_next)` stays within 1e-4 of 1.0 after every iteration during development |
| Damping applied at emission instead of formula step | Unit test: run 1 iteration on a 2-vertex graph with known edges; hand-verify the formula output |
| Edge direction bug from Team 1's preprocessor | Load the partition, walk edges, verify on a small known graph that in-edges and out-edges are consistent with the reversed convention |
| Large CONTRIBS message stalls under TCP backpressure | `send_message` loops until all bytes sent; no partial sends |
| Load imbalance (power-law degree distribution) | Report, don't fix — this is an expected result for Experiment 2 |
| Memory fragmentation from outbox vectors | Pre-allocate outboxes to expected average size at start of each iteration |

---

## Interface Contracts (Consume These)

**From Team 1:**
- `partition_K.bin` binary format (read `format.md` published Day 1 of Week 1)
- Small subgraph edge list for local development

**From Team 2:**
- `send_message(fd, type, payload, len)`
- `recv_message(fd)` → `Message`
- `setup_mesh(my_worker_id, peers)` → `{peer_id → fd}`
- `recv_all_contribs(peer_fds, iteration_k, num_workers)` → `vector<Message>`
- CONTRIBS serializer / deserializer

**From Team 3 (via wire):**
- `START_ITERATION k` — signals start of each iteration
- `GLOBAL_DANGLING` — received after sending DANGLING_REPORT
- `STOP` — received in place of START_ITERATION when converged
- `SHUTDOWN` — received after sending LOCAL_TOPK
