#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace dpr {

enum class MsgType : std::uint8_t {
    HELLO = 0x01,
    CLUSTER_INFO = 0x02,
    READY = 0x03,
    LOAD_COMPLETE = 0x04,
    START_ITERATION = 0x05,
    CONTRIBS = 0x06,
    DELTA_REPORT = 0x07,
    DANGLING_REPORT = 0x08,
    GLOBAL_DANGLING = 0x09,
    STOP = 0x0A,
    LOCAL_TOPK = 0x0B,
    SHUTDOWN = 0x0C,
    ABORT = 0x0D,
    METRICS = 0x0E,
};

struct Message {
    MsgType type;
    std::vector<std::uint8_t> payload;
};

struct HelloPayload {
    std::uint32_t worker_id;
    std::uint16_t listen_port;
    std::uint64_t vertex_count;
    std::uint64_t edge_count;
};

struct PeerInfo {
    std::uint32_t worker_id;
    std::string host;
    std::uint16_t port;
};

struct ContribPair {
    std::uint32_t vertex_id;
    double contribution;
};

struct ContribPayload {
    std::uint32_t iteration;
    std::vector<ContribPair> pairs;
};

struct TopKEntry {
    std::uint32_t node_id;
    double rank;
};

struct WorkerMetrics {
    std::uint32_t worker_id;
    std::uint32_t iteration;
    double compute_ms;       // local vertex walk (walk_and_emit)
    double exchange_ms;      // full CONTRIBS round-robin send+recv
    double apply_ms;         // PageRank formula application
    std::uint64_t contribs_bytes; // bytes sent as CONTRIBS this iteration
    std::uint32_t memory_mb; // resident set size in MB
};

void send_message(int fd, MsgType type, const std::vector<std::uint8_t>& payload);
Message recv_message(int fd);

std::vector<std::uint8_t> serialize_hello(const HelloPayload& payload);
HelloPayload deserialize_hello(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_cluster_info(const std::vector<PeerInfo>& peers);
std::vector<PeerInfo> deserialize_cluster_info(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_start_iteration(std::uint32_t iteration);
std::uint32_t deserialize_start_iteration(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_iteration_value(std::uint32_t iteration, double value);
std::pair<std::uint32_t, double> deserialize_iteration_value(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_contribs(std::uint32_t iteration, const std::vector<ContribPair>& pairs);
ContribPayload deserialize_contribs(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_topk(const std::vector<TopKEntry>& entries);
std::vector<TopKEntry> deserialize_topk(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> serialize_worker_metrics(const WorkerMetrics& m);
WorkerMetrics deserialize_worker_metrics(const std::vector<std::uint8_t>& payload);

}  // namespace dpr
