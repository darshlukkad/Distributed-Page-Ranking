# Distributed PageRank for Influencer Selection — Architecture Document

## 1. Project Goal

We are building a distributed system that identifies the most valuable influencer accounts on a large social follow graph, by computing PageRank scores across a small cluster of commodity laptops connected over TCP.

The motivating use case is **influencer marketing**. A brand running a campaign needs to select a small number of accounts (say, 50) to send products to, out of millions of candidates. Sorting candidates by raw follower count is the obvious approach, but it is a poor signal: follower counts are inflated by bots and purchased engagement, mega-accounts have low engagement and shallow audience overlap, and the ranking is trivially gamed. PageRank on the follow graph gives a structurally robust alternative — an account scores highly only when it is followed by *other* well-followed accounts, which is much harder to manipulate.

The contribution of this project is the **system**, not the algorithm. PageRank itself is fifty-year-old math. What we are demonstrating is that we can take a real-world follow graph too large for a single machine, partition it across multiple machines, run the algorithm to convergence with all communication over raw TCP sockets that we wrote ourselves, and characterize how the system scales.

### Goals

- Produce PageRank scores on the Twitter-2010 follow graph that match a single-machine sequential reference implementation to within an L1 tolerance of `1e-6`.
- Run correctly across at least 4 worker processes on at least 2 physical machines.
- Produce structured per-iteration logs sufficient to plot speedup, parallel efficiency, and a communication-vs-computation breakdown.
- Demonstrate the marketing value of the system by comparing the top-K accounts ranked by raw follower count against those ranked by PageRank, on the same graph.
- Demonstrate manipulation robustness by injecting synthetic spam accounts and showing that they dominate follower-count rankings but are suppressed by PageRank.

### Non-goals

- Fault tolerance: if any worker crashes mid-run, the whole run aborts. No retry, no reconfiguration.
- Dynamic membership: the worker set is fixed at launch.
- Encryption or authentication on the wire.
- Support for graphs that exceed total cluster memory.
- Asynchronous or pipelined iteration models. We use strict bulk-synchronous iterations.
- A general-purpose graph processing framework. We are building one algorithm, well.

---

## 2. Dataset

### Source

We use the **Twitter follower network** dataset published by SNAP (Stanford):

> https://snap.stanford.edu/data/twitter-2010.html

This is a snapshot of the Twitter follow graph from 2010. The dataset comes from the paper *"What is Twitter, a social network or a news media?"* (Kwak et al., 2010).

### Statistics

| Property | Value |
|---|---|
| Nodes (users) | 41,652,230 |
| Edges (follow relationships) | 1,468,364,884 |
| Format | Plain-text edge list, gzipped |
| Compressed size | ~5.6 GB |
| Uncompressed size | ~26 GB |

The dataset ships as two files:

- **`twitter-2010.txt.gz`** — the directed follow edge list. Format: two integers per line, space-separated, no header, no comments. Each line is a single edge.
- **`twitter-2010-ids.csv.gz`** — a CSV mapping our internal node IDs to original Twitter user IDs. Header: `"node_id","twitter_id"`. Used only at the end of a run to produce human-readable output.

### Edge semantics — important

According to the SNAP page, an edge `i j` means **"j is a follower of i"** — in other words, the edge points from the *followed* account to the *follower*. This is the opposite of what PageRank conventionally expects.

For PageRank to assign **high scores to popular accounts** (the ones being followed), edges must point in the direction of endorsement: from follower to followed. Our preprocessor therefore **reverses every edge** when it builds the partition files. Concretely: when we read `i j` from the file, we treat it as an edge from `j` to `i`.

This is a one-line change in the preprocessor, but if you get it wrong, your top-K list will contain accounts that follow lots of people instead of accounts that are followed by lots of important people. The preprocessor's edge-reversal step is therefore one of the few places that warrants a unit test.

### Node IDs

The edge file uses **dense node IDs** in the range `[0, 41652229]`. SNAP has already remapped the original Twitter user IDs (which are sparse 64-bit integers) to compact contiguous integers, so we do not need to do any ID remapping ourselves. 32-bit integers are sufficient for storing IDs.

The mapping back to original Twitter IDs lives in the CSV, which is **not used by the PageRank computation itself**. It is loaded once at the end of the run, by the coordinator, when producing the final top-K output.

---

## 3. Inputs and Outputs

### Inputs

The system has three logical inputs:

1. **The graph data**, supplied as a gzipped edge list at a known local path on the machine that runs preprocessing. This is the Twitter-2010 file described above.
2. **The cluster configuration**, supplied as a single config file (`cluster.conf`) shared by all processes. It contains the coordinator's address, the list of workers and their addresses, the damping factor (default `0.85`), the convergence threshold (default `1e-6`), and the maximum iteration count (default `100`).
3. **The per-worker partition file**, produced by the preprocessor and placed on each worker's local disk before launch. Workers do not download or share this file at runtime.

### Outputs

The system produces three outputs at the end of a successful run:

1. **Per-worker rank files** (`result_worker_0.txt`, `result_worker_1.txt`, ...) — each worker writes the final rank of every vertex it owns, as `(node_id, rank)` pairs.
2. **A merged global top-K file** (`top_k.txt`) — the coordinator collects each worker's local top-K, merges them, sorts by rank, and writes a single file with the top K accounts globally. K is configurable, default 100.
3. **A human-readable influencer report** (`top_influencers.txt`) — a small post-processing step joins `top_k.txt` against the SNAP CSV mapping and produces a final table:

   ```
   rank | node_id | twitter_id | pagerank_score
   ```

   The Twitter IDs can be looked up via TweeterID or the Twitter API (where still permitted) to recover handles.

### Logs

Throughout the run, both coordinator and workers write structured per-iteration logs containing wall-clock timings, message counts, byte counts, and convergence deltas. These logs are the data source for every chart in the evaluation section of the final report.

---

## 4. Preprocessing

Preprocessing is a one-time offline step that runs **before** any distributed run. It converts the raw SNAP edge list into one binary partition file per worker. It is implemented as a small Python script, not part of the C++ runtime.

### What the preprocessor does

1. **Read the gzipped edge list line by line**, streaming through the file without ever fully decompressing it to disk.
2. **Reverse the edge direction** — when a line says `i j`, treat it as an edge from `j` to `i` (so the edge points from follower to followed).
3. **Hash-partition by source vertex** — for an edge `j → i`, the source vertex is `j`. The edge belongs to worker `j % N`, where N is the number of workers.
4. **Emit binary partition files** in compressed sparse row (CSR) format, one per worker. The file format is described in detail in Section 6.
5. **Write a small partition manifest** with statistics: vertex count and edge count per worker. The coordinator uses this at startup to sanity-check that each worker loaded a consistent partition.

### Why hash partitioning

We use the simplest possible partition function: vertex `v` is owned by worker `v % N`. Three reasons:

- **It is implicit.** There is no partition table to store or distribute. Any process that knows a vertex's global ID can compute its owner with one modulus operation.
- **It is fast.** Routing a contribution to its destination is `O(1)` arithmetic, not a hash table lookup.
- **It is balanced on vertex count.** Each worker gets approximately `V/N` vertices, deterministically.

The downside is that hash partitioning ignores graph structure. On power-law graphs like Twitter's, a few super-high-degree vertices dominate the edge count. If those vertices happen to land on a single worker, that worker has more outgoing edges to process and more contributions to send. We will measure this load imbalance in the evaluation section — it is a feature, not a bug, of the experimental story (it motivates more sophisticated partitioning as future work).

### Cost

For a 1.47B-edge graph on a modern laptop, preprocessing takes on the order of 15–30 minutes, dominated by gzip decompression and disk I/O. It does not need to be re-run unless the graph changes or N changes. Each generated partition file is approximately 1.5 GB for N=4.

---

## 5. System Architecture

### Topology

The cluster is a **star-plus-mesh** topology:

- One **coordinator** process. It does not own any graph data and does no PageRank computation. Its job is to orchestrate iteration boundaries, aggregate convergence statistics, and decide when to stop.
- N **worker** processes. Each owns a disjoint partition of the vertices and their outgoing edges, and does the actual PageRank computation.
- **Star edges** (control plane): every worker has a single TCP connection to the coordinator. Used for registration, iteration start/stop signals, and convergence reporting.
- **Mesh edges** (data plane): every worker has a TCP connection to every other worker. Used for exchanging rank contributions during iterations. With N workers, that is N(N-1)/2 worker-to-worker connections.

For N=4: 4 control-plane connections (worker→coordinator) plus 6 data-plane connections (worker↔worker) = 10 total TCP connections in the cluster.

### Why not pure peer-to-peer

A fully peer-to-peer design with no coordinator is feasible but adds substantial complexity: every worker would need to participate in distributed barrier and all-reduce operations, leader election, and graceful shutdown coordination. With three weeks and raw sockets, the coordinator is a pragmatic simplification that costs us almost nothing in performance — the coordinator is idle most of the time, and only handles tiny control messages.

### Communication model

We use **bulk synchronous parallel (BSP)**. Computation proceeds in lockstep: every worker performs the same iteration `k` at the same time, separated by global barriers. No worker advances to iteration `k+1` until every other worker has finished iteration `k`.

This is slower than asynchronous designs (the slowest worker dictates the pace) but it is dramatically simpler to reason about, debug, and prove correct. For a 3-week project, this is the right tradeoff.

### Process roles

The coordinator's responsibilities, in order of importance:

1. Wait for all workers to register at startup.
2. Tell every worker who its peers are (so workers can establish the mesh).
3. Broadcast `START_ITERATION k` signals.
4. Aggregate the global L1 convergence delta from per-worker reports.
5. Aggregate the global dangling-vertex mass from per-worker reports.
6. Decide when to stop and broadcast `STOP`.
7. Collect per-worker top-K results and merge them into a global top-K.

Each worker's responsibilities:

1. Load its partition file from local disk.
2. Establish TCP connections to the coordinator and all peers.
3. In each iteration: walk local vertices, compute outgoing contributions, route them locally or over TCP to the right peer.
4. Receive contributions from peers, accumulate them into the local incoming buffer.
5. Apply the PageRank formula, compute local convergence delta, report it.
6. Repeat until coordinator says stop.
7. Write final ranks to local disk and report local top-K to coordinator.

---

## 6. Partition File Format

Each worker has a single binary file on local disk: `partition_K.bin` where K is the worker ID. This file is the only graph data the worker has — it knows nothing about other workers' vertices or edges.

### What the file contains

The file holds, for the vertices this worker owns, their **outgoing edges** in compressed sparse row (CSR) format. It does **not** contain incoming edges (those arrive at runtime as TCP messages), or vertices owned by other workers, or original Twitter IDs.

### Layout

The file is laid out as three contiguous sections: a header, a vertex section, and an edge section.

**Header (32 bytes):**

| Field | Size | Description |
|---|---|---|
| `magic_number` | 4 B | Format identifier, e.g. `0xPAGE0001` |
| `format_version` | 4 B | Version, currently 1 |
| `worker_id` | 4 B | Which worker this file is for |
| `num_workers` | 4 B | Total cluster size |
| `total_global_vertices` | 8 B | V across the entire graph (41,652,230) |
| `num_local_vertices` | 8 B | How many vertices this worker owns |
| `num_local_edges` | 8 B | How many outgoing edges total in this partition |

The header is used at load time to validate consistency: the worker checks its ID, confirms the cluster size matches the config, and verifies the global vertex count matches what other workers reported.

**Vertex section (CSR row pointers):**

Two parallel arrays.

- `local_vertex_ids[]` — the global IDs of all vertices this worker owns, sorted ascending. For worker 2 with N=4, this is `[2, 6, 10, 14, 18, 22, ..., 41652226]`.
- `edge_offsets[]` — for each local vertex, the starting position in the edge array where its outgoing edges begin. Has `num_local_vertices + 1` entries (the extra entry at the end is a sentinel that lets us look up any vertex's edge range without a special case for the last vertex).

**Edge section:**

- `edges[]` — a flat array of destination global vertex IDs, concatenated. Vertex `i`'s outgoing edges live at `edges[edge_offsets[i] .. edge_offsets[i+1]]`.

### Why CSR

The compressed sparse row format is the standard representation for sparse graphs. Three properties make it the right choice:

- **Compact.** Almost no memory overhead beyond the 4 bytes per destination ID. The naive alternative (per-vertex `vector<int>`) wastes hundreds of megabytes on allocation metadata.
- **Cache-friendly.** Iterating through a vertex's edges is a sequential read of contiguous memory, which the CPU prefetcher handles efficiently.
- **Memory-mappable.** The on-disk layout matches the in-memory layout, so loading is essentially a single large `mmap` or `fread` — no parsing.

On a 367M-edge partition, processing every edge once takes 2–4 seconds on a single core when stored as CSR. The same workload on a `vector<vector<int>>` representation can be 5–10× slower due to cache misses.

### Worked example

Suppose N=4 workers, total graph has 12 vertices (IDs 0–11), and we are looking at `partition_2.bin` (worker 2). Hash partitioning means worker 2 owns vertices `{2, 6, 10}`.

After edge reversal in the preprocessor, suppose:
- Vertex 2 has outgoing edges to `{5, 7, 9}`
- Vertex 6 has outgoing edges to `{2, 11}`
- Vertex 10 has no outgoing edges (dangling)

The file contains:

```
header:
  worker_id = 2
  num_workers = 4
  total_global_vertices = 12
  num_local_vertices = 3
  num_local_edges = 5

local_vertex_ids = [2, 6, 10]
edge_offsets     = [0, 3, 5, 5]
edges            = [5, 7, 9, 2, 11]
```

To find the outgoing edges of local vertex at index `i`:

- `i=0` (vertex 2): `edges[0..3]` = `[5, 7, 9]` — three edges
- `i=1` (vertex 6): `edges[3..5]` = `[2, 11]` — two edges
- `i=2` (vertex 10): `edges[5..5]` = `[]` — empty, it is dangling

Notice that **dangling vertices fall out for free**: when a vertex has no edges, two consecutive offsets are equal and the slice is naturally empty. No flag, no null check, the inner loop simply does zero iterations.

### Real-world sizing for Twitter-2010 with N=4

| Section | Size on disk |
|---|---|
| Header | 32 B |
| `local_vertex_ids` (~10.4M × 4 B) | ~42 MB |
| `edge_offsets` (~10.4M × 8 B) | ~83 MB |
| `edges` (~367M × 4 B) | ~1.47 GB |
| **Total per partition file** | **~1.59 GB** |

Each laptop holds one file. None of the four laptops needs the full 26 GB of source data.

---

## 7. Lifecycle of a Run

This section walks through what happens from the moment you launch the system to the moment results are written.

### Phase 1: Process launch

The **coordinator** starts first. It reads `cluster.conf`, opens a TCP listening socket on the configured port, prints `coordinator listening on 0.0.0.0:9000, waiting for 4 workers...`, and blocks waiting for incoming connections.

Then each **worker** is launched on its assigned machine, with arguments specifying its worker ID, the path to the config file, and the path to its partition file. Each worker reads the config, opens its own listening socket (for receiving from peer workers), and opens an outbound connection to the coordinator.

### Phase 2: Cluster bootstrap

When a worker connects to the coordinator, it sends a `HELLO` message with its worker ID, listening address, and a checksum of its partition's metadata. The coordinator records each registration and waits until all N workers have registered. If a worker is missing after a 60-second timeout, the coordinator aborts.

Once all workers are registered, the coordinator broadcasts `CLUSTER_INFO` — the full list of `(worker_id, address, port)` tuples — to every worker.

### Phase 3: Peer mesh setup

Each worker now establishes a TCP connection to every other worker. To avoid race conditions where both sides try to connect simultaneously, we use a simple convention: worker `i` initiates a connection to worker `j` only if `i < j`. Worker `j` accepts. After this step, every pair of workers has exactly one TCP connection between them, used bidirectionally for the rest of the run.

Each worker has now established:
- One control-plane connection to the coordinator.
- N−1 data-plane connections to peer workers.

When the mesh is complete, each worker sends `READY` to the coordinator. The coordinator waits for `READY` from all workers.

### Phase 4: Graph loading

Workers load their partition files into memory in parallel. Each worker reads `partition_K.bin`, allocates the CSR arrays, and initializes:

- `rank_current[]`, with every entry set to `1.0 / total_global_vertices`. For Twitter-2010 that is `1 / 41652230 ≈ 2.4 × 10⁻⁸`.
- `rank_next[]`, uninitialized.
- `incoming[]`, an array of length `num_local_vertices`, all zeros. This is where contributions destined for this worker's vertices accumulate.
- N−1 outboxes, one per peer, each empty. These hold contributions destined for other workers.

When loading completes, each worker reports `LOAD_COMPLETE` to the coordinator with its vertex and edge counts. The coordinator logs the totals as a sanity check.

### Phase 5: The iteration loop

This is the heart of the system. The coordinator broadcasts `START_ITERATION k=1`. Every iteration follows seven steps, described in detail in Section 8.

Per-iteration log lines from the coordinator look like:

```
iter 12: global_delta = 4.23e-04, compute=180ms, comm=240ms, total=420ms
```

A typical run on Twitter-2010 with damping `d=0.85` converges in 30–60 iterations.

### Phase 6: Result aggregation

Once the coordinator broadcasts `STOP`, each worker writes its local rank array to disk as `result_worker_K.txt`. Each worker also computes its local top-100 vertices, serializes them, and sends them to the coordinator. The coordinator merges the N partial top-100 lists into a single global top-100 and writes `top_k.txt`.

A separate small post-processing script (Python) joins `top_k.txt` against the SNAP `twitter-2010-ids.csv` mapping and produces `top_influencers.txt` with human-readable Twitter IDs.

### Phase 7: Shutdown

The coordinator broadcasts `SHUTDOWN`. Each worker closes its peer connections, closes its connection to the coordinator, frees memory, and exits. The coordinator waits for all workers to disconnect, then exits.

If anything goes wrong during the run — a worker crashes, a TCP connection breaks — the coordinator detects the broken connection, broadcasts `ABORT` to surviving workers, logs the failure, and exits with a nonzero status. The current scope does not include retry logic.

---

## 8. The Iteration Loop in Depth

This is the core data flow of the system. Every iteration on every worker follows these seven steps.

### Step 1: Walk local vertices and emit contributions

The worker iterates through every vertex it owns. For each local vertex `v`:

1. Compute `out_degree(v) = edge_offsets[i+1] − edge_offsets[i]`.
2. If `out_degree(v) == 0`, the vertex is **dangling**. Add its current rank to a running `local_dangling_mass` counter and move on (no contributions to emit).
3. Otherwise, compute the per-edge contribution: `contrib = rank_current[v] / out_degree(v)`.
4. For each outgoing edge `v → u`:
   - Compute the destination's owner: `owner = u % num_workers`.
   - If `owner == my_worker_id`: this contribution is for me. Add it directly to `incoming[local_index_of(u)]`.
   - Otherwise: append the pair `(u, contrib)` to `outbox_to_worker[owner]`.

After this walk completes, two things have happened:

- `incoming[]` already contains partial sums — every contribution from a local vertex to a local destination has been folded in.
- The N−1 outboxes hold packed lists of `(global_dest_id, contribution)` pairs ready to send.

This is **all local computation**. No network activity yet. On a 367M-edge partition this takes 2–4 seconds on one core.

#### Worked example

Take vertex 6 on worker 2. Suppose its current rank is `0.00000312`, and it has 4 outgoing edges to `[18, 4421, 99201, 13679000]`.

The per-edge contribution is `0.00000312 / 4 = 0.00000078`. We route each:

- Destination 18: `18 % 4 = 2` → that's me. Add `0.00000078` to `incoming[local_index_of(18)]`.
- Destination 4421: `4421 % 4 = 1` → goes to worker 1. Append to `outbox_to_worker[1]`.
- Destination 99201: `99201 % 4 = 3` → goes to worker 3. Append to `outbox_to_worker[3]`.
- Destination 13679000: `13679000 % 4 = 0` → goes to worker 0. Append to `outbox_to_worker[0]`.

Repeat for every other local vertex. By the end, every local vertex has emitted its contributions and they are sitting in either `incoming[]` or one of the outboxes.

### Step 2: Compute the local-index lookup efficiently

When a contribution destined for global vertex `u` arrives at me, I need to find its local index in my arrays.

Because we use hash partitioning, this is pure arithmetic: if `u % N == my_worker_id`, then `u`'s local index is `u / N`. For example, if I am worker 2 and vertex 18 lands in my incoming bucket, its local index is `18 / 4 = 4`. No lookup table required.

This is a major reason hash partitioning is attractive: with more sophisticated partitioners like METIS, every worker would need to load a global vertex-to-owner table costing hundreds of megabytes of memory and a hash lookup per contribution.

### Step 3: Send outboxes, receive inboxes (network exchange)

Each worker now sends its outboxes to peers. Each outbox becomes one `CONTRIBS` message, tagged with the iteration number, sent over the existing TCP connection. The message format is `[MSG_TYPE=CONTRIBS][iteration_k][num_pairs][(vertex_id, contribution) × num_pairs]`.

Each worker also receives N−1 inboxes — one `CONTRIBS` message from each peer.

**This step is a barrier.** The worker does not proceed until it has received exactly one `CONTRIBS` message from every peer. Concretely, the receive loop counts: it processes incoming messages until the count reaches N−1, then stops.

### Step 4: Why the barrier is necessary

A natural worry: what if I finish processing my own contributions, start applying the PageRank formula, and *then* a late `CONTRIBS` message arrives from a slow peer? The contribution would be lost.

The protocol prevents this by construction:

- Every peer sends exactly one `CONTRIBS` message per iteration, tagged with the iteration number.
- No worker proceeds to step 5 until it has received N−1 messages for the current iteration.
- No worker can send iteration k+1's contributions before the cluster has finished iteration k, because the iteration boundary is gated by a coordinator broadcast.

So late arrivals are impossible if everyone follows the protocol. The cluster moves in lockstep. This is **bulk synchronous parallel (BSP)**, and it is the simplest correct model for distributed iterative algorithms.

### Step 5: Fold inboxes into incoming[]

For each `(global_dest_id, contribution)` pair received from peers:

```
local_idx = global_dest_id / num_workers
incoming[local_idx] += contribution
```

After this fold, `incoming[i]` contains the **total** of all contributions destined for local vertex `i`, from every vertex in the entire graph that links to it. The order of accumulation doesn't matter — addition is associative.

### Step 6: All-reduce dangling mass and apply the formula

Before applying the formula, we need the **global dangling mass** — the sum of `rank_current[v]` for every dangling vertex `v` across all workers. Each worker computed its local dangling mass during step 1; now we aggregate.

The aggregation is one float per worker: each worker sends `local_dangling_mass` to the coordinator, the coordinator sums them into `global_dangling_mass`, and broadcasts the result back. This is a tiny number of bytes — negligible compared to the contribution exchange.

Now each worker applies the PageRank formula to every local vertex:

```
rank_next[i] = (1 − d) / N
             + d × incoming[i]
             + d × global_dangling_mass / N
```

Where:
- `d` is the damping factor (default 0.85).
- `N` is `total_global_vertices` (41,652,230 for Twitter-2010).
- The first term `(1−d)/N` is the **teleport** — uniform rank distributed to every vertex regardless of structure. This guarantees convergence even on disconnected graphs.
- The second term `d × incoming[i]` is the **link contribution** — the rank that flowed in through actual edges.
- The third term redistributes the rank that would otherwise be lost at dangling vertices.

#### Why dangling vertices need this fix

If a vertex has no outgoing edges, its rank has nowhere to go. Without the dangling-mass redistribution, every iteration leaks a little rank mass and the total drifts below 1. Across many iterations this breaks convergence. The fix — sum the dangling mass globally and redistribute it uniformly — is small but absolutely required. This is one of the most common bugs in PageRank implementations.

#### Why the formula is applied at the end, not during emission

We send raw `rank/degree` contributions across the network, not damped contributions. The `× d` and the `+ (1−d)/N` happen exactly once, at step 6, after all contributions for a vertex have been accumulated. Applying damping at emission time would double-count it on cross-worker edges and produce subtly wrong results.

### Step 7: Convergence check

Each worker computes the local L1 difference between old and new ranks:

```
local_delta = sum over local i of |rank_next[i] − rank_current[i]|
```

It sends this single float to the coordinator. The coordinator sums all N local deltas to get `global_delta`. If `global_delta < threshold` (default `1e-6`) or if the iteration count hits the max, the coordinator broadcasts `STOP`. Otherwise it broadcasts `START_ITERATION k+1`.

### Step 8: Swap and reset

Each worker swaps `rank_current` and `rank_next` (by pointer swap, an `O(1)` operation), zeroes the `incoming[]` array, and clears all outboxes. The data structures are now ready for the next iteration.

---

## 9. Convergence Behavior

PageRank with damping factor 0.85 converges geometrically. Each iteration reduces the L1 error by roughly a factor of `d = 0.85`. So:

- After 10 iterations, error ≈ initial × 0.20
- After 30 iterations, error ≈ initial × 0.0076
- After 50 iterations, error ≈ initial × 0.00029

A threshold of `1e-6` is typically reached in 30–60 iterations for graphs of this size and density. We set the maximum iteration count to 100 as a safety cap; if a run hits this, something is wrong (likely a bug in dangling-mass handling or formula application) and the result should be treated as suspect.

---

## 10. Output: Top Influencers

The coordinator's final job is to produce a meaningful answer to the marketing question: *which accounts should we target?*

### Local top-K computation

Each worker, after the run converges, computes its top-100 vertices by rank locally. This is a simple partial sort over its 10.4M owned vertices, taking under a second. It packages the result as 100 `(node_id, rank)` pairs and sends them to the coordinator.

### Global merge

The coordinator receives N partial top-100 lists. It merges them — concatenates, sorts by rank descending, takes the top 100 — and writes `top_k.txt`. Total cost: trivial, since input is only 100×N entries.

### Joining with the Twitter ID mapping

A small Python post-processing script reads `top_k.txt` and the SNAP `twitter-2010-ids.csv`, joins the two on `node_id`, and produces `top_influencers.txt`:

```
rank | node_id  | twitter_id  | pagerank_score
1    | 2415     | 813286      | 0.00231
2    | 87       | 14224719    | 0.00198
3    | 15032    | 17919972    | 0.00187
...
```

The CSV is loaded only here, only by the coordinator (or a separate post-processing script), and only once. It is **never** loaded by workers. The PageRank computation is identifier-agnostic and works entirely with `node_id`.

### Interpreting Twitter IDs

A historical aside that might appear in the report's discussion section: smaller `twitter_id` values correspond to older accounts. Twitter assigned IDs sequentially starting from 1 in 2006, so an ID of 12 belongs to one of the very first users (Jack Dorsey, the founder). By 2010, when this snapshot was taken, new accounts had IDs in the hundreds of millions. The top-PageRank accounts in our results will skew toward small Twitter IDs — early adopters who had years to accumulate links from other influential early accounts. This is consistent with the algorithm: PageRank rewards being embedded in a dense core, and Twitter's dense core in 2010 was its early users.

---

## 11. Evaluation Plan

The system's value is demonstrated through five experiments, each producing one or more figures for the final report.

### Experiment 1: Correctness validation

Run a sequential single-machine PageRank on a small subgraph (the first 10M edges of Twitter-2010, or the SNAP Pokec dataset). Run the distributed system on the same input. Compare the resulting rank vectors element-wise. Pass criterion: L1 difference below `1e-6`.

This experiment is non-negotiable. Without it, no other measurement is meaningful.

### Experiment 2: Strong scaling

Fix the graph size (use a fixed-size subgraph of Twitter-2010, e.g. 100M edges). Run on N = 1, 2, 3, 4 workers. Plot wall-clock time per iteration vs N, and parallel efficiency `(T_1 / (N × T_N))` vs N.

The expected result is sub-linear scaling: efficiency falls below 1 because of communication overhead and load imbalance. The shape and magnitude of the falloff is the interesting result.

### Experiment 3: Communication-vs-computation breakdown

For a fixed configuration (4 workers, full graph or large subgraph), instrument every iteration with timers separating: local computation (step 1 and step 5–6), send time, receive/wait time. Plot a stacked bar chart per iteration.

This reveals whether the bottleneck is compute or network, and whether the bottleneck shifts across iterations.

### Experiment 4: Top-K comparison vs follower count

Compute two rankings on the same graph: top-50 by raw follower count (a single-pass count per vertex), and top-50 by PageRank. Report the overlap (how many accounts appear in both lists), and qualitatively examine accounts that appear in one but not the other.

This is the marketing-relevance evidence. We expect substantial divergence: PageRank will surface accounts whose followers are themselves well-followed, while pure follower-count ranking will surface accounts with broad but shallow audiences.

### Experiment 5: Robustness to follower inflation

On a smaller subgraph (so the manipulation is inspectable), inject 10,000 synthetic spam accounts. Each spam account follows every other spam account (a clique) and follows a small set of "target" real accounts that we want to artificially boost. Re-run both rankings.

Expected result: the target accounts dominate the follower-count top-K (each gained 10,000 followers) but are barely affected in the PageRank ranking (the spam followers are all followed only by other spam followers, so their PageRank is negligible, so their endorsements carry negligible weight).

This is the manipulation-robustness evidence — the structural argument for why PageRank is a better influencer signal than follower count.

---

## 12. Implementation Timeline (4 Weeks)

**Week 1 — Sequential reference and preprocessor**

- Implement a single-machine sequential PageRank in C++ that reads SNAP edge lists and produces ranks.
- Implement the Python preprocessor that produces partition files.
- Validate sequential PageRank against a reference (e.g., NetworkX) on a small graph.
- Outcome: a known-correct baseline. Every distributed run will be diff'd against this.

**Week 2 — Networking layer, no algorithm**

- Build coordinator and worker binaries that exchange hello messages, do a barrier, and shut down cleanly.
- Solidify TCP setup, message framing, peer mesh formation, clean shutdown.
- Test all workers on `localhost` first, then across two laptops on the same Wi-Fi.
- Outcome: a working distributed runtime that doesn't yet do PageRank.

**Week 3 — Distributed PageRank**

- Plug the algorithm into the networking layer.
- Implement contribution exchange, dangling-mass aggregation, convergence check.
- Verify ranks match the sequential reference on a small subgraph.
- Outcome: end-to-end distributed PageRank that produces correct results.

**Week 4 — Experiments and report**

- Run all five evaluation experiments.
- Produce all figures.
- Write the report.
- Outcome: deliverable.

---

## 13. Risks and Mitigations

**Risk: Edge direction bug.** The SNAP convention (`i j` means `j` follows `i`) is the opposite of what PageRank expects. Mitigation: explicit reversal in the preprocessor, with a unit test.

**Risk: Dangling mass forgotten or applied incorrectly.** Mitigation: explicit dangling-mass step in the iteration code, with a sanity check that `sum(rank) ≈ 1` after each iteration.

**Risk: TCP connection setup races.** Mitigation: the `i < j` connection convention prevents simultaneous bidirectional connect attempts.

**Risk: Network instability across laptops.** Wi-Fi is flaky; cross-machine TCP can drop. Mitigation: develop everything on `localhost` first; treat cross-machine runs as a deployment exercise, not a debugging environment.

**Risk: Load imbalance on power-law graphs.** A few super-popular vertices in the partition assigned to one worker can make that worker much slower than the others. Mitigation: report this as a finding rather than fight it; it motivates the future-work discussion of better partitioning.

**Risk: Dataset preprocessing time.** 1.47B edges takes 15–30 minutes to preprocess. Mitigation: develop on a smaller subgraph (first 10M edges); only run the full preprocessor for final scaling experiments.

---

## 14. Glossary

- **BSP (Bulk Synchronous Parallel)** — a computation model where parallel processes alternate between local-computation phases and global communication phases, separated by barriers.
- **Coordinator** — the single non-worker process that orchestrates iterations and aggregates global statistics.
- **CSR (Compressed Sparse Row)** — a sparse-matrix / graph storage format using a flat edge array plus per-row offset pointers.
- **Damping factor (d)** — the probability that a random walker follows an edge rather than teleporting. Default 0.85.
- **Dangling vertex** — a vertex with zero outgoing edges. Requires special handling in PageRank.
- **Hash partitioning** — assigning vertex `v` to worker `v mod N`. Simple, balanced on vertex count, computable without a lookup table.
- **Inbox / `incoming[]`** — the per-worker buffer where contributions destined for local vertices accumulate, regardless of source.
- **Outbox** — a per-peer buffer holding contributions destined for that peer's local vertices, populated during the local walk.
- **PageRank** — an iterative algorithm that scores graph vertices by the rank-weighted in-degree of their predecessors.
- **Teleport term** — the `(1−d)/N` component of the PageRank formula; uniform rank distributed to every vertex per iteration.
- **Worker** — a process that owns a partition of the graph and does the iterative computation.

---

*Document version 1. To be revised as implementation progresses.*
