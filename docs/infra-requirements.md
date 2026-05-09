# Infrastructure & Hardware Requirements

## Project Context

Distributed PageRank on the Twitter-2010 follow graph (~41.6M nodes, 1.47B edges) across a cluster of 5 machines communicating over raw TCP sockets. 13-member team split into 4 implementation teams.

**Code freeze:** May 15 | **Presentation:** May 19

---

## Team Structure

| Team | Responsibility | Members |
|---|---|---|
| Team 1 | Preprocessor & data pipeline (Python) | 3–4 |
| Team 2 | Networking layer — TCP framing, mesh (C++) | 3–4 |
| Team 3 | Coordinator process (C++) | 3 |
| Team 4 | Worker process & PageRank core (C++) | 3–4 |

---

## Machines Required

### Runtime cluster (5 machines)

| Role | Count | RAM | Free Disk | Notes |
|---|---|---|---|---|
| Coordinator | 1 | 4 GB+ | 2 GB | Idle during computation — any laptop works |
| Worker | 4 | **8 GB+** | 5 GB each | Use the 4 beefiest machines |

### Preprocessing machine (offline, one-time)

| Role | Count | RAM | Free Disk | Notes |
|---|---|---|---|---|
| Preprocessing | 1 | 4 GB+ | **30 GB** | Can be one of the 13 dev laptops, not needed at demo |

With 13 team laptops, there are more than enough machines. Pick the 4 with the most RAM for workers.

---

## Storage Breakdown

### Preprocessing machine

| Item | Size | Notes |
|---|---|---|
| `twitter-2010.txt.gz` | 5.6 GB | Download from SNAP — start immediately |
| `twitter-2010-ids.csv.gz` | ~0.3 GB | ID mapping for post-processing |
| Temp sort buffers (during preprocessing) | ~8–12 GB | Cleared after partition files are written |
| Output: 4 × `partition_K.bin` | 6.4 GB | 4 × ~1.6 GB, one per worker |
| Pokec dataset (development/testing) | ~0.3 GB | Small graph for Week 1 dev |
| **Total** | **~25 GB** | Round up — keep 30 GB free |

### Each worker machine

| Item | Size | Notes |
|---|---|---|
| `partition_K.bin` | ~1.6 GB | Its own slice only |
| Worker binary | ~5 MB | |
| `result_worker_K.txt` (output) | ~200 MB | Written after convergence |
| **Total** | **~2 GB** | Keep 5 GB free for headroom |

### Coordinator machine

| Item | Size | Notes |
|---|---|---|
| Coordinator binary | ~5 MB | |
| `twitter-2010-ids.csv.gz` | ~0.3 GB | For post-processing only |
| `top_k.txt`, `iteration_log.csv`, outputs | <10 MB | |
| **Total** | **<1 GB** | |

### Total across cluster

| | |
|---|---|
| Preprocessing machine | 30 GB free |
| Coordinator | 2 GB free |
| 4 × Worker | 5 GB free each |
| **Total** | **~52 GB** |

---

## Network Architecture

### Hardware to buy

| Item | Spec | Cost | Notes |
|---|---|---|---|
| Unmanaged Gigabit switch | 8-port, e.g. TP-Link TL-SG108 or Netgear GS308 | ~$20–25 | 8 ports = 5 laptops + room to spare |
| Ethernet cables | Cat5e or Cat6, 1–2m, ×5 | ~$10 for a pack | One per laptop |
| USB-C → Gigabit Ethernet adapters | Must say Gigabit / 1000 Mbps | ~$15–20 each | Required for every MacBook — verify Gigabit |
| Power strip | 6+ outlets | ~$10 | Switch + 5 laptop chargers |

**Total hardware cost: ~$50–80**

> **Warning:** Cheap USB-C adapters are often only 100 Mbps. Check the box says Gigabit. A 100 Mbps adapter is 10× slower than a real Gigabit link and will visibly hurt scaling results.

### Topology diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║              DEMO NETWORK TOPOLOGY — GIGABIT ETHERNET OVER SWITCH               ║
╚══════════════════════════════════════════════════════════════════════════════════╝

                   ┌──────────────────────────────┐
                   │   8-PORT GIGABIT SWITCH       │
                   │   TP-Link TL-SG108            │
                   │   (unmanaged, plug-and-play)  │
                   │                               │
                   │  [1][2][3][4][5][6][7][8]     │
                   └──┬──┬──┬──┬──┬───────────────┘
                      │  │  │  │  │  Cat6 Ethernet cables (1–2m)
                      │  │  │  │  │
       ┌──────────────┘  │  │  │  └──────────────────────┐
       │        ┌────────┘  │  └──────────┐              │
       │        │       ┌───┘             │              │
       ▼        ▼       ▼                 ▼              ▼
┌──────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
│COORDINATOR│ │  WORKER 0  │ │  WORKER 1  │ │  WORKER 2  │ │  WORKER 3  │
│          │ │            │ │            │ │            │ │            │
│192.168.  │ │192.168.    │ │192.168.    │ │192.168.    │ │192.168.    │
│  1.10    │ │  1.11      │ │  1.12      │ │  1.13      │ │  1.14      │
│          │ │            │ │            │ │            │ │            │
│RAM: 4GB+ │ │RAM:  8GB+  │ │RAM:  8GB+  │ │RAM:  8GB+  │ │RAM:  8GB+  │
│Disk: 2GB │ │Disk: 5GB   │ │Disk: 5GB   │ │Disk: 5GB   │ │Disk: 5GB   │
│          │ │            │ │            │ │            │ │            │
│Port: 9000│ │Port: 9001  │ │Port: 9002  │ │Port: 9003  │ │Port: 9004  │
│          │ │partition   │ │partition   │ │partition   │ │partition   │
│          │ │_0.bin      │ │_1.bin      │ │_2.bin      │ │_3.bin      │
│          │ │(1.6 GB)    │ │(1.6 GB)    │ │(1.6 GB)    │ │(1.6 GB)    │
└──────────┘ └────────────┘ └────────────┘ └────────────┘ └────────────┘

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  LOGICAL CONNECTIONS (over the same physical switch):

  Control plane (TCP port 9000):
  Coordinator ◄──── HELLO / READY / DELTA_REPORT / DANGLING_REPORT ──── Worker 0
  Coordinator ◄──────────────────────────────────────────────────────── Worker 1
  Coordinator ◄──────────────────────────────────────────────────────── Worker 2
  Coordinator ◄──────────────────────────────────────────────────────── Worker 3
  Coordinator ────── START_ITERATION / GLOBAL_DANGLING / STOP ────────► All

  Data plane (CONTRIBS messages, ~hundreds MB per iteration):
  Worker 0 ◄──────────────────────────────────────────────────────────► Worker 1
  Worker 0 ◄──────────────────────────────────────────────────────────► Worker 2
  Worker 0 ◄──────────────────────────────────────────────────────────► Worker 3
  Worker 1 ◄──────────────────────────────────────────────────────────► Worker 2
  Worker 1 ◄──────────────────────────────────────────────────────────► Worker 3
  Worker 2 ◄──────────────────────────────────────────────────────────► Worker 3
                              6 bidirectional connections

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  TCP CONNECTIONS SUMMARY:
  Control plane  4 connections   Worker i → Coordinator (port 9000)
  Data plane     6 connections   Worker i ↔ Worker j, i < j (ports 9001–9004)
                 ─────────────
                 10 total TCP connections in the cluster

  SWITCH BANDWIDTH:
  1 Gbps per port, full-duplex, non-blocking
  All 5 machines communicate simultaneously — switch handles in parallel
  Effective throughput per worker-to-worker link: ~125 MB/s
```

### Static IP configuration

No router or DHCP needed — just a switch. Set IPs manually on each machine.

**macOS:**
```
System Settings → Network → Ethernet → Details
→ Configure IPv4: Manually
→ IP Address:  192.168.1.1x
→ Subnet Mask: 255.255.255.0
→ Router:      (leave blank)
```

**Linux:**
```bash
sudo ip addr add 192.168.1.1x/24 dev eth0
sudo ip link set eth0 up
```

**Windows:**
```
Control Panel → Network → Ethernet → Properties → IPv4
→ Use the following IP address
→ IP: 192.168.1.1x   Subnet: 255.255.255.0   Gateway: (leave blank)
```

| Machine | IP |
|---|---|
| Coordinator | 192.168.1.10 |
| Worker 0 | 192.168.1.11 |
| Worker 1 | 192.168.1.12 |
| Worker 2 | 192.168.1.13 |
| Worker 3 | 192.168.1.14 |

Verify connectivity before every run:
```bash
ping 192.168.1.11   # from coordinator, should reply
ping 192.168.1.10   # from each worker, should reply
```

---

## MacBook-Specific Setup

All modern MacBooks (post-2016) have no built-in Ethernet — every runtime Mac needs a USB-C to Gigabit Ethernet adapter.

### Build tools

Install on every Mac in the cluster:

```bash
# Xcode Command Line Tools (gives clang++, make, git)
xcode-select --install

# Verify
clang++ --version   # must show C++17 support
python3 --version

# Python packages (only on preprocessing machine)
pip3 install networkx
```

### Compile on each machine separately

If the team has a mix of Apple Silicon (M1/M2/M3/M4) and Intel Macs, **binaries are not cross-compatible**. Each Mac must compile its own binary from source.

```bash
# Run on EACH runtime laptop before demo day
cd Distributed-Page-Ranking
make all
./coordinator --help   # verify it runs
./worker --help        # verify it runs
```

Pure C++17 with no platform-specific intrinsics compiles cleanly on both ARM and Intel.

### Endianness

Both Apple Silicon and Intel Macs are **little-endian**. The protocol spec uses little-endian throughout. No byte-swapping needed.

### Firewall

macOS will show a popup the first time a binary listens on a port. Handle this before demo day:

```
Run coordinator and worker once on each machine.
Click "Allow" when macOS asks about incoming connections.
```

Or disable the firewall for the demo (safe on a private Ethernet-only network):
```
System Settings → Network → Firewall → turn OFF
```

### SIGPIPE handling

macOS does not support `MSG_NOSIGNAL` in `send()`. Add this one line in `main()` before opening any sockets:

```cpp
#include <signal.h>
signal(SIGPIPE, SIG_IGN);
```

Without this, a crashed worker can silently kill the coordinator process via SIGPIPE.

---

## Dataset

| File | Size | Source |
|---|---|---|
| `twitter-2010.txt.gz` | 5.6 GB compressed | SNAP: snap.stanford.edu/data/twitter-2010.html |
| `twitter-2010-ids.csv.gz` | ~0.3 GB | Same page |
| `soc-pokec-relationships.txt.gz` | ~260 MB | SNAP Pokec — use for Week 1 development |

**Download twitter-2010.txt.gz immediately.** On a 50 Mbps connection it takes 15–20 minutes. On a slow connection it can take hours. The preprocessing machine needs 30 GB free before download.

The preprocessor streams through the gzip file — the full 26 GB is never written to disk.

---

## 7-Day Sprint Timeline

| Date | Milestone |
|---|---|
| **May 8** | Dataset download starts. Adapter orders placed. `format.md` and `protocol.md` published. All teams begin in parallel. |
| **May 9** | Sequential PageRank (T4) working on Pokec. Preprocessor (T1) working on 10M-edge subgraph. Networking framing (T2) tested. Cluster.conf parser (T3) done. Full Twitter-2010 preprocessing kicked off. |
| **May 10–11** | Partition files ready. Coordinator iteration loop working with stub workers. Worker iteration loop implemented. Mesh setup tested. |
| **May 12–13** | End-to-end integration on localhost. Cross-machine test over Ethernet. Correctness validation: distributed ranks match sequential (L1 < 1e-6). |
| **May 14** | All 5 experiments run. Plots generated. |
| **May 15** | Code freeze. |
| **May 15–18** | Report and slides. |
| **May 19** | Presentation. |

---

## Pre-Demo Checklist

### Hardware (buy by May 10)
- [ ] 8-port Gigabit switch purchased and tested
- [ ] 5 × Cat6 Ethernet cables (test each — LED on switch port lights up green)
- [ ] USB-C to Gigabit Ethernet adapters for every MacBook in cluster (verify Gigabit)
- [ ] Power strip with enough outlets

### Software (complete by May 14)
- [ ] `xcode-select --install` run on all 5 cluster Macs
- [ ] Binaries compiled natively on each of the 5 cluster Macs
- [ ] Firewall allowed or disabled on all 5 Macs
- [ ] `signal(SIGPIPE, SIG_IGN)` in coordinator and worker `main()`

### Data (complete by May 12)
- [ ] Twitter-2010 dataset downloaded
- [ ] Preprocessing complete — 4 × `partition_K.bin` files generated
- [ ] Partition files copied to each worker laptop (`scp` or USB)
- [ ] `twitter-2010-ids.csv.gz` on coordinator machine

### Network (complete by May 13)
- [ ] Static IPs set on all 5 machines
- [ ] `ping` test: every machine pings every other — all reply
- [ ] Port test: `nc -l 9001` on Worker 0, `nc 192.168.1.11 9001` from Coordinator — connects
- [ ] `cluster.conf` has correct static IPs for all machines

### Dry run (May 18 — day before presentation)
- [ ] Full cluster boots: coordinator starts, 4 workers register, mesh forms
- [ ] Run 10 iterations on small subgraph — coordinator logs look correct
- [ ] `sum(ranks) ≈ 1.0` after first iteration confirmed
- [ ] `top_k.txt` written successfully
- [ ] All 5 machines stay connected for a full run with no crashes
