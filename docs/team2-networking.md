# Team 2 — Networking Layer

## Overview

Team 2 owns every byte that moves over TCP: message framing, serialization, the coordinator's listening socket, the worker-to-worker mesh, barrier enforcement, and clean shutdown. You build the communication substrate that Teams 3 and 4 plug into. Your API is the second critical contract (after the binary file format) that must be locked down in Week 1.

No data moves without you. Teams 3 and 4 should be able to write coordinator and worker logic without touching a single socket call — that is the goal of your API.

---

## Goals

1. Define and implement all message types for control-plane (worker↔coordinator) and data-plane (worker↔worker) communication.
2. Build a reliable framing layer over TCP so messages are never split, duplicated, or reassembled incorrectly.
3. Implement the worker peer-mesh setup with the `i < j` convention to prevent connection races.
4. Provide a clean C++ API (`send_message`, `recv_message`, connection setup, barrier) that hides raw sockets from Teams 3 and 4.
5. Test the full mesh on `localhost`, then across two physical machines on the same network.

---

## High-Level Architecture

```
                    ┌─────────────┐
                    │ Coordinator │
                    └──────┬──────┘
          Control-plane     │  (4 × TCP connections, one per worker)
          ┌─────────────────┼──────────────────┐
          │                 │                  │
     ┌────▼────┐      ┌─────▼────┐      ┌─────▼────┐      ┌──────────┐
     │Worker 0 │      │ Worker 1 │      │ Worker 2 │      │ Worker 3 │
     └────┬────┘      └────┬─────┘      └────┬─────┘      └────┬─────┘
          │                │                 │                  │
          └────────────────┴─────────────────┴──────────────────┘
               Data-plane mesh (6 × bidirectional TCP connections)
```

**Control-plane:** Worker → Coordinator TCP. Used for HELLO, READY, LOAD_COMPLETE, DELTA_REPORT, DANGLING_REPORT, LOCAL_TOPK, and coordinator broadcasts.

**Data-plane:** Worker ↔ Worker TCP. Used exclusively for CONTRIBS messages during iterations.

---

## Message Protocol

### Wire Format

Every message uses the same framing:

```
[msg_type : uint8]  [payload_length : uint32]  [payload : bytes]
```

Total overhead: 5 bytes per message. The receiver reads the 5-byte header first, then reads exactly `payload_length` bytes. This prevents partial reads from corrupting the stream.

### Message Type Enum

```cpp
enum MsgType : uint8_t {
    HELLO           = 0x01,   // worker → coordinator, at registration
    CLUSTER_INFO    = 0x02,   // coordinator → worker, peer list
    READY           = 0x03,   // worker → coordinator, mesh established
    LOAD_COMPLETE   = 0x04,   // worker → coordinator, partition loaded
    START_ITERATION = 0x05,   // coordinator → worker, begin iteration k
    CONTRIBS        = 0x06,   // worker → worker, rank contributions
    DELTA_REPORT    = 0x07,   // worker → coordinator, local L1 delta
    DANGLING_REPORT = 0x08,   // worker → coordinator, local dangling mass
    GLOBAL_DANGLING = 0x09,   // coordinator → worker, global dangling mass
    STOP            = 0x0A,   // coordinator → worker, converged
    LOCAL_TOPK      = 0x0B,   // worker → coordinator, local top-K
    SHUTDOWN        = 0x0C,   // coordinator → worker, exit
    ABORT           = 0x0D,   // coordinator → worker, fatal error
};
```

### Payload Layouts

**HELLO** (worker → coordinator):
```
worker_id       : uint32
listen_port     : uint16
vertex_count    : uint64   (from partition header)
edge_count      : uint64
```

**CLUSTER_INFO** (coordinator → worker, sent to each worker):
```
num_workers     : uint32
[per worker]:
  worker_id     : uint32
  ip_addr       : 4 bytes (IPv4)
  port          : uint16
```

**READY** / **LOAD_COMPLETE**: empty payload (0 bytes). Worker ID is implicit (connection is per-worker).

**START_ITERATION** (coordinator → worker):
```
iteration_k     : uint32
```

**CONTRIBS** (worker → worker):
```
iteration_k     : uint32
num_pairs       : uint32
[num_pairs × ]:
  vertex_id     : uint32
  contribution  : float64
```

**DELTA_REPORT** (worker → coordinator):
```
iteration_k     : uint32
local_delta     : float64
```

**DANGLING_REPORT** (worker → coordinator):
```
iteration_k     : uint32
local_dangling  : float64
```

**GLOBAL_DANGLING** (coordinator → worker):
```
iteration_k     : uint32
global_dangling : float64
```

**STOP** (coordinator → worker): empty payload.

**LOCAL_TOPK** (worker → coordinator):
```
num_entries     : uint32
[num_entries × ]:
  node_id       : uint32
  rank          : float64
```

**SHUTDOWN** / **ABORT**: empty payload.

All multi-byte integers are **little-endian**. Floats are IEEE 754.

---

## Module Breakdown

### `net/socket_utils.h/.cpp`

Low-level helpers:

- `int tcp_listen(uint16_t port)` — create, bind, listen; return server fd.
- `int tcp_connect(const char* host, uint16_t port)` — blocking connect with retry (up to 30s for coordinator availability).
- `int tcp_accept(int server_fd)` — blocking accept; return client fd.
- `void set_nodelay(int fd)` — disable Nagle (important: large CONTRIBS messages will stall otherwise).
- `void send_exact(int fd, const void* buf, size_t n)` — loop until all bytes sent.
- `void recv_exact(int fd, void* buf, size_t n)` — loop until all bytes received.

### `net/message.h/.cpp`

Mid-level message framing:

```cpp
void send_message(int fd, MsgType type, const void* payload, uint32_t len);
Message recv_message(int fd);  // blocks until full message arrives

struct Message {
    MsgType type;
    std::vector<uint8_t> payload;
};
```

Serialization helpers for each message type (encode to `vector<uint8_t>`, decode from `uint8_t*`).

### `net/mesh.h/.cpp`

Peer-mesh setup:

```cpp
// Called by each worker after receiving CLUSTER_INFO.
// Returns map: peer_worker_id → connected fd.
std::unordered_map<int, int> setup_mesh(
    int my_worker_id,
    const std::vector<PeerInfo>& peers   // from CLUSTER_INFO
);
```

**`i < j` convention:** worker `i` calls `tcp_connect` to worker `j` only if `i < j`. Worker `j` calls `tcp_accept` for workers with lower IDs. This ensures exactly one connection per pair with no deadlock. Implemented as: for each peer `j`, if `my_id < j` → connect; else → accept.

### `net/barrier.h`

Thin wrapper around the inbox-counting logic used during CONTRIBS exchange:

```cpp
// Blocks until exactly (num_workers - 1) CONTRIBS messages for iteration_k
// have been received from all peers. Returns them in order of arrival.
std::vector<Message> recv_all_contribs(
    const std::vector<int>& peer_fds,
    uint32_t iteration_k,
    int num_workers
);
```

---

## Connection Lifecycle

```
Coordinator starts → listens on port 9000
Workers start → each calls tcp_connect to coordinator port 9000
                sends HELLO
Coordinator → receives N HELLOs → broadcasts CLUSTER_INFO to each worker
Workers → setup_mesh() → sends READY to coordinator
Coordinator → waits for N READYs → workers begin loading partitions
Workers → load partition → sends LOAD_COMPLETE to coordinator
Coordinator → waits for N LOAD_COMPLETEs → begins iteration loop

[Iteration k:]
  Coordinator → broadcasts START_ITERATION k
  Workers → exchange CONTRIBS over data-plane mesh (barrier)
  Workers → send DANGLING_REPORT to coordinator
  Coordinator → sums, broadcasts GLOBAL_DANGLING
  Workers → compute ranks → send DELTA_REPORT to coordinator
  Coordinator → sums, checks threshold → broadcasts STOP or START_ITERATION k+1

[After STOP:]
  Workers → write result files → send LOCAL_TOPK to coordinator
  Coordinator → merges → writes top_k.txt → broadcasts SHUTDOWN
  Workers → close all connections → exit
```

---

## Deliverables

| Deliverable | When | Format |
|-------------|------|--------|
| `protocol.md` — message type enum + wire layouts | End of Week 1, Day 2 | Markdown, shared with Teams 3 & 4 |
| `net/socket_utils.h/.cpp` | End of Week 1 | C++ |
| `net/message.h/.cpp` — framing + all serializers | End of Week 1 | C++ |
| `net/mesh.h/.cpp` — peer mesh setup | End of Week 2 | C++ |
| `net/barrier.h` — recv_all_contribs | End of Week 2 | C++ |
| Working `localhost` ping-pong test (4 processes) | End of Week 1 | Shell script + test binary |
| Working cross-machine test (2 laptops) | End of Week 2 | |
| Clean shutdown and ABORT handling | End of Week 2 | |

---

## Testing Plan

### Unit Tests

- **Framing:** send a 0-byte, 1-byte, 1 MB, and 64 MB payload; verify recv gets back exactly the same bytes.
- **Partial reads:** use a mock fd that delivers data 1 byte at a time; verify `recv_message` reassembles correctly.
- **CONTRIBS serialization:** encode a vector of `(vertex_id, contribution)` pairs, decode, verify round-trip.
- **`i < j` mesh:** launch 4 threads simulating workers; verify each pair ends up with exactly 1 connection.

### Integration Tests

- **4-process barrier:** 4 processes on localhost each send CONTRIBS to all 3 peers; verify each receives exactly 3 before unblocking.
- **Cross-machine test:** run coordinator on machine A and 4 workers split across machines A and B; verify CLUSTER_INFO broadcast reaches all workers and mesh forms.
- **ABORT propagation:** kill one worker process mid-iteration; verify coordinator detects broken connection and broadcasts ABORT to remaining workers within 5 seconds.

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Partial TCP reads silently corrupting messages | `recv_exact` loops until all bytes are consumed; no partial delivery to upper layers |
| Nagle algorithm coalescing small DELTA_REPORT messages with CONTRIBS traffic | Call `set_nodelay` on all fds at connection setup |
| `i < j` deadlock if both sides call connect simultaneously | Strictly enforce: `i < j` → connect, else accept; test with 4 concurrent workers |
| CONTRIBS message for iteration k+1 arriving before iteration k is processed | Tag every CONTRIBS with `iteration_k`; `recv_all_contribs` ignores messages with wrong k (should not happen in BSP but provides a safety check) |
| Wi-Fi flakiness in cross-laptop tests | Always pass cross-machine tests last; primary dev target is localhost |

---

## Interface Contracts (Publish on Day 2 of Week 1)

**You produce → Team 3 consumes:**
- `send_message` / `recv_message` API
- All message type definitions and serialization helpers
- `tcp_listen` / `tcp_accept` for coordinator's server socket

**You produce → Team 4 consumes:**
- `send_message` / `recv_message` API
- `setup_mesh()` function
- `recv_all_contribs()` barrier
- CONTRIBS message serialization/deserialization
