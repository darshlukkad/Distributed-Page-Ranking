# Distributed PageRank

Distributed PageRank on the Twitter-2010 social graph (~41.6M nodes, 1.47B edges) across a cluster of commodity machines communicating over raw TCP sockets. Built as a capstone project.

**Presentation:** May 19 | **Code freeze:** May 15

---

## What It Does

Identifies the most valuable influencer accounts on a large social follow graph by computing PageRank scores across a cluster of machines. PageRank is structurally more robust than raw follower counts — an account scores highly only when it is followed by other well-followed accounts, making it much harder to game with bots or purchased engagement.

---

## Team Structure

| Group | Component | Language |
|---|---|---|
| Group A | Worker process & PageRank core | C++ |
| Group B | Networking layer (TCP framing, peer mesh, coordinator or all-reduce) | C++ |
| Group C | Preprocessor & data pipeline | Python |
| Group D | Build system, experiments, integration | CMake / Python |

---

## Repository Layout

```
Distributed-Page-Ranking/
├── README.md
├── CONTRIBUTING.md              # Coding standards, git workflow, PR policy
├── .gitignore
├── docs/
│   ├── architecture.md          # Original full system design document
│   ├── hardware-architecture.md # Hardware topology and per-iteration flow diagrams
│   ├── infra-requirements.md    # Storage, network, MacBook setup, pre-demo checklist
│   ├── process-flow.md          # System lifecycle, state machines, data flow
│   ├── option-a-coordinator.md  # Architecture option: N=8 workers + coordinator
│   ├── option-b-leaderless.md   # Architecture option: N=4 workers, no coordinator
│   ├── data-pipeline.md         # Group C spec: preprocessor, file format, postprocess
│   ├── networking-layer.md      # Group B spec: TCP framing, message protocol, mesh
│   └── worker-core.md           # Group A spec: partition loader, PageRank loop
├── src/                         # C++ source (coordinator, worker, net/)
├── scripts/                     # Python scripts (preprocessor, postprocess, etc.)
└── data/                        # gitignored — partition files, raw dataset
```

---

## Dataset

Twitter-2010 follow graph published by SNAP (Stanford):

| File | Size | Purpose |
|---|---|---|
| `twitter-2010.txt.gz` | 5.6 GB | Edge list (compressed) |
| `twitter-2010-ids.csv.gz` | ~0.3 GB | Node ID → Twitter ID mapping |

Download both files before running preprocessing.

**Memory warning:** the preprocessor reads the gzip file line-by-line but stores the entire edge list in a Python dict in RAM before writing partitions. For the full Twitter-2010 graph (~1.47B edges) this requires ~40–60 GB of RAM. A streaming/sort-based rewrite is required before the full graph can be preprocessed on typical hardware.

For development and testing, use the sample graph in `data/sample/` (bundled) or the SNAP Pokec dataset (~260 MB).

---

## Prerequisites

### All machines in the cluster

```bash
# macOS
xcode-select --install      # gives clang++, make, git

# Linux
sudo apt install g++ make git

# Python scripts (preprocessor, postprocess, monitoring) use stdlib only — no pip installs needed
```

### Build

```bash
# Primary: plain make from repo root
make all
# Outputs: build/coordinator  build/worker

# Alternative: CMake (requires cmake ≥ 3.16)
cmake -S . -B build && cmake --build build -j4
```

### Quickest verified run (localhost, sample graph)

```bash
./scripts/run_local_demo.sh
```

Builds both binaries, preprocesses `data/sample/twitter_sample_snap.txt`, starts 1 coordinator + 4 workers on `127.0.0.1`, and writes `demo_out/top_k.txt`, `demo_out/top_influencers.txt`, and `demo_out/iteration_log.csv`.

---

## Quick Start

> **Verified configuration:** N=4 workers, coordinator-based, localhost only.
> The N=8 multi-machine path is aspirational — it has not been end-to-end tested.

### Step 1 — Preprocess the graph (one-time, offline)

```bash
# Tested: sample graph bundled in repo
python3 scripts/preprocessor.py \
    --input  data/sample/twitter_sample_snap.txt \
    --workers 4 \
    --output-dir data/partitions/

# Untested at scale: full Twitter-2010 graph (requires ~40–60 GB RAM — see Dataset note)
python3 scripts/preprocessor.py \
    --input  data/twitter-2010.txt.gz \
    --workers 4 \
    --output-dir data/partitions/
```

Copy each partition to its worker machine:

```bash
scp data/partitions/partition_0.bin user@192.168.1.11:~/partition_0.bin
scp data/partitions/partition_1.bin user@192.168.1.12:~/partition_1.bin
scp data/partitions/partition_2.bin user@192.168.1.13:~/partition_2.bin
scp data/partitions/partition_3.bin user@192.168.1.14:~/partition_3.bin
```

### Step 2 — Create cluster.conf

Use `cluster.local.conf` as a template. For a 4-worker cluster on static IPs:

```ini
coordinator_host = 192.168.1.10
coordinator_port = 9000
num_workers      = 4
damping_factor   = 0.85
convergence_threshold = 1e-6
max_iterations   = 100
top_k            = 100

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

### Step 3 — Start the cluster

Binaries are output to `build/` and require `--output-dir`. Start coordinator first:

```bash
# On coordinator machine (192.168.1.10)
./build/coordinator --config cluster.conf --output-dir ./output/

# On each worker machine (run in separate terminals / SSH sessions)
./build/worker --id 0 --config cluster.conf --partition partition_0.bin --output-dir ./output/
./build/worker --id 1 --config cluster.conf --partition partition_1.bin --output-dir ./output/
./build/worker --id 2 --config cluster.conf --partition partition_2.bin --output-dir ./output/
./build/worker --id 3 --config cluster.conf --partition partition_3.bin --output-dir ./output/
```

### Step 4 — Post-process results

```bash
python3 scripts/postprocess.py \
    --top-k  output/top_k.txt \
    --id-map data/twitter-2010-ids.csv.gz \
    --output output/top_influencers.txt
```

---

## Network Setup (Demo)

All runtime machines connected via **Gigabit Ethernet switch** (no Wi-Fi). Set static IPs manually — no router or DHCP needed.

| Machine | IP |
|---|---|
| Coordinator | 192.168.1.10 |
| Worker 0 | 192.168.1.11 |
| Worker 1 | 192.168.1.12 |
| Worker 2 | 192.168.1.13 |
| Worker 3 | 192.168.1.14 |

Verify connectivity before starting:

```bash
ping 192.168.1.11   # from coordinator — should reply
```

**MacBook note:** Every MacBook needs a USB-C to Gigabit Ethernet adapter. Verify the adapter is Gigabit (1000 Mbps), not 100 Mbps.

See [docs/infra-requirements.md](docs/infra-requirements.md) for full hardware setup and MacBook-specific instructions.

---

## Outputs

All outputs are written to the directory passed via `--output-dir`.

| File | Description |
|---|---|
| `result_worker_K.txt` | Final rank of every vertex owned by worker K |
| `top_k.txt` | Global top-K vertices by PageRank score |
| `top_influencers.txt` | Top-K with original Twitter IDs (requires postprocess.py) |
| `iteration_log.csv` | Per-iteration convergence delta and total wall time |
| `worker_metrics.csv` | Per-worker per-iteration breakdown: compute\_ms, exchange\_ms, apply\_ms, bytes sent, memory MB |

## Monitoring

Two optional scripts for live visibility during a run:

```bash
# On each worker machine — start before the worker binary
python3 scripts/monitoring_agent.py --id 0 --log-dir output/
# Serves GET /metrics and GET /history on port 9200+worker_id (no external deps)

# On any machine — live terminal dashboard polling all agents
python3 scripts/monitor.py --config cluster.conf
```

The agent logs system CPU%, memory, and network throughput to `agent_{id}.csv` every second and exposes a JSON HTTP endpoint for the dashboard.

---

## Documentation

### Architecture Decision (choose one before May 9)

| Document | Description |
|---|---|
| [docs/option-a-coordinator.md](docs/option-a-coordinator.md) | Option A: N=8 workers + coordinator, 9 machines, lower risk |
| [docs/option-b-leaderless.md](docs/option-b-leaderless.md) | Option B: N=4 workers, no coordinator, all-reduce, 4 machines |

### Design Reference

| Document | Description |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Original full system design document |
| [docs/hardware-architecture.md](docs/hardware-architecture.md) | Hardware topology and per-iteration flow diagrams |
| [docs/infra-requirements.md](docs/infra-requirements.md) | Storage, network, MacBook setup, pre-demo checklist |
| [docs/process-flow.md](docs/process-flow.md) | System lifecycle, state machines, per-iteration data flow |

### Component Specs

| Document | Description |
|---|---|
| [docs/data-pipeline.md](docs/data-pipeline.md) | Group C: preprocessor, binary format contract, postprocess |
| [docs/networking-layer.md](docs/networking-layer.md) | Group B: TCP framing, message protocol, peer mesh |
| [docs/worker-core.md](docs/worker-core.md) | Group A: partition loader, PageRank iteration loop, top-K |
