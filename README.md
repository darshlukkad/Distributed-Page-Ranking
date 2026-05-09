# Distributed PageRank

Distributed PageRank on the Twitter-2010 social graph (~41.6M nodes, 1.47B edges) across a cluster of commodity machines communicating over raw TCP sockets. Built as a capstone project.

**Presentation:** May 19 | **Code freeze:** May 15

---

## What It Does

Identifies the most valuable influencer accounts on a large social follow graph by computing PageRank scores across a cluster of machines. PageRank is structurally more robust than raw follower counts — an account scores highly only when it is followed by other well-followed accounts, making it much harder to game with bots or purchased engagement.

---

## Team Structure

| Team | Component | Language |
|---|---|---|
| Team 1 | Preprocessor & data pipeline | Python |
| Team 2 | Networking layer (TCP framing, peer mesh) | C++ |
| Team 3 | Coordinator process | C++ |
| Team 4 | Worker process & PageRank core | C++ |

---

## Repository Layout

```
Distributed-Page-Ranking/
├── README.md
├── .gitignore
├── docs/
│   ├── architecture.md          # Full system design document
│   ├── hardware-architecture.md # Hardware topology and data flow diagrams
│   ├── infra-requirements.md    # Infrastructure, storage, network, MacBook setup
│   ├── team1-preprocessor.md   # Team 1 spec and deliverables
│   ├── team2-networking.md     # Team 2 spec and deliverables
│   ├── team3-coordinator.md    # Team 3 spec and deliverables
│   └── team4-worker.md         # Team 4 spec and deliverables
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

Download both files before running preprocessing. The full graph is never decompressed to disk — the preprocessor streams through the gzip file.

For development and testing, use the SNAP Pokec dataset (~260 MB) as a smaller substitute.

---

## Prerequisites

### All machines in the cluster

```bash
# macOS
xcode-select --install      # gives clang++, make, git

# Linux
sudo apt install g++ make git

# Python (preprocessing machine only)
pip3 install networkx
```

### Build

```bash
mkdir build && cd build
cmake ..
make -j4

# or with make directly
make all
```

---

## Quick Start

### Step 1 — Preprocess the graph (one-time, offline)

Run on any machine with 30 GB free disk:

```bash
python3 scripts/preprocessor.py \
    --input  data/twitter-2010.txt.gz \
    --workers 4 \
    --output-dir data/partitions/

# Copy each partition file to its worker machine
scp data/partitions/partition_0.bin user@192.168.1.11:~/partition_0.bin
scp data/partitions/partition_1.bin user@192.168.1.12:~/partition_1.bin
scp data/partitions/partition_2.bin user@192.168.1.13:~/partition_2.bin
scp data/partitions/partition_3.bin user@192.168.1.14:~/partition_3.bin
```

### Step 2 — Edit cluster.conf

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

Start the coordinator first, then workers in any order:

```bash
# On coordinator machine (192.168.1.10)
./coordinator --config cluster.conf

# On each worker machine (run in separate terminals / SSH sessions)
./worker --id 0 --config cluster.conf --partition partition_0.bin
./worker --id 1 --config cluster.conf --partition partition_1.bin
./worker --id 2 --config cluster.conf --partition partition_2.bin
./worker --id 3 --config cluster.conf --partition partition_3.bin
```

### Step 4 — Post-process results

```bash
python3 scripts/postprocess.py \
    --top-k      top_k.txt \
    --id-map     data/twitter-2010-ids.csv.gz \
    --output     top_influencers.txt
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

| File | Description |
|---|---|
| `result_worker_K.txt` | Final rank of every vertex on worker K |
| `top_k.txt` | Global top-100 vertices by PageRank score |
| `top_influencers.txt` | Top-100 with original Twitter IDs |
| `iteration_log.csv` | Per-iteration timings and convergence delta |

---

## Documentation

| Document | Description |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Full system design — algorithm, protocol, data flow |
| [docs/hardware-architecture.md](docs/hardware-architecture.md) | Hardware topology and per-iteration flow diagrams |
| [docs/infra-requirements.md](docs/infra-requirements.md) | Storage, network, MacBook setup, pre-demo checklist |
| [docs/team1-preprocessor.md](docs/team1-preprocessor.md) | Preprocessor spec, file format contract, deliverables |
| [docs/team2-networking.md](docs/team2-networking.md) | TCP layer, message protocol, mesh setup |
| [docs/team3-coordinator.md](docs/team3-coordinator.md) | Coordinator spec, BSP loop, aggregation |
| [docs/team4-worker.md](docs/team4-worker.md) | Worker spec, iteration loop, PageRank formula |
