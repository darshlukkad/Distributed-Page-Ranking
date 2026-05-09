#include "common/config.h"
#include "net/message.h"
#include "net/socket_utils.h"

#include <signal.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace dpr {
namespace {

struct RegisteredWorker {
    int fd = -1;
    std::uint32_t worker_id = 0;
    std::uint16_t listen_port = 0;
    std::uint64_t vertex_count = 0;
    std::uint64_t edge_count = 0;
};

[[noreturn]] void usage() {
    throw std::runtime_error("usage: coordinator --config <path> --output-dir <path>");
}

std::string get_arg(int argc, char** argv, const std::string& flag) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (argv[i] == flag) {
            return argv[i + 1];
        }
    }
    return "";
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void expect_type(const Message& message, MsgType expected, const std::string& context) {
    if (message.type != expected) {
        throw std::runtime_error("unexpected message in " + context);
    }
}

void broadcast(const std::vector<RegisteredWorker>& workers,
               MsgType type,
               const std::vector<std::uint8_t>& payload) {
    for (const auto& worker : workers) {
        send_message(worker.fd, type, payload);
    }
}

void broadcast_empty(const std::vector<RegisteredWorker>& workers, MsgType type) {
    broadcast(workers, type, {});
}

std::vector<TopKEntry> merge_topk(const std::vector<TopKEntry>& entries, std::uint32_t top_k) {
    std::vector<TopKEntry> merged = entries;
    std::sort(merged.begin(), merged.end(), [](const TopKEntry& lhs, const TopKEntry& rhs) {
        if (lhs.rank == rhs.rank) {
            return lhs.node_id < rhs.node_id;
        }
        return lhs.rank > rhs.rank;
    });
    if (merged.size() > top_k) {
        merged.resize(top_k);
    }
    return merged;
}

}  // namespace

int run(int argc, char** argv) {
    const std::string config_path = get_arg(argc, argv, "--config");
    const std::string output_dir = get_arg(argc, argv, "--output-dir");
    if (config_path.empty() || output_dir.empty()) {
        usage();
    }

    const auto config = load_config(config_path);
    std::filesystem::create_directories(output_dir);

    const int server_fd = tcp_listen(config.coordinator_port);
    std::vector<RegisteredWorker> workers(config.num_workers);
    std::vector<bool> seen(config.num_workers, false);

    std::cout << "coordinator listening on " << config.coordinator_host << ":"
              << config.coordinator_port << ", waiting for " << config.num_workers
              << " workers..." << std::endl;

    std::uint32_t connected = 0;
    while (connected < config.num_workers) {
        const int worker_fd = tcp_accept(server_fd);
        const Message message = recv_message(worker_fd);
        expect_type(message, MsgType::HELLO, "registration");

        const auto hello = deserialize_hello(message.payload);
        require(hello.worker_id < config.num_workers, "worker_id out of range");
        require(!seen[hello.worker_id], "duplicate worker registration");
        require(hello.listen_port == config.workers[hello.worker_id].port,
                "worker listen port does not match config");

        workers[hello.worker_id] = RegisteredWorker{
            worker_fd,
            hello.worker_id,
            hello.listen_port,
            hello.vertex_count,
            hello.edge_count
        };
        seen[hello.worker_id] = true;
        ++connected;
    }

    std::vector<PeerInfo> peers;
    peers.reserve(config.num_workers);
    for (std::uint32_t worker_id = 0; worker_id < config.num_workers; ++worker_id) {
        peers.push_back(PeerInfo{
            worker_id,
            config.workers[worker_id].host,
            config.workers[worker_id].port
        });
    }
    broadcast(workers, MsgType::CLUSTER_INFO, serialize_cluster_info(peers));

    for (const auto& worker : workers) {
        expect_type(recv_message(worker.fd), MsgType::READY, "READY barrier");
    }
    for (const auto& worker : workers) {
        expect_type(recv_message(worker.fd), MsgType::LOAD_COMPLETE, "LOAD_COMPLETE barrier");
    }

    std::ofstream log_csv(std::filesystem::path(output_dir) / "iteration_log.csv");
    log_csv << "iteration,global_delta,global_dangling,compute_contribs_ms,apply_formula_ms,total_ms\n";

    for (std::uint32_t iteration = 1; iteration <= config.max_iterations; ++iteration) {
        const auto start = std::chrono::steady_clock::now();
        broadcast(workers, MsgType::START_ITERATION, serialize_start_iteration(iteration));

        double global_dangling = 0.0;
        for (const auto& worker : workers) {
            const Message message = recv_message(worker.fd);
            expect_type(message, MsgType::DANGLING_REPORT, "dangling aggregation");
            const auto [message_iteration, dangling] = deserialize_iteration_value(message.payload);
            require(message_iteration == iteration, "dangling iteration mismatch");
            global_dangling += dangling;
        }

        // phase1: START_ITERATION → all DANGLING_REPORTs received (worker compute + CONTRIBS exchange)
        const auto after_dangling = std::chrono::steady_clock::now();

        broadcast(workers,
                  MsgType::GLOBAL_DANGLING,
                  serialize_iteration_value(iteration, global_dangling));

        double global_delta = 0.0;
        for (const auto& worker : workers) {
            const Message message = recv_message(worker.fd);
            expect_type(message, MsgType::DELTA_REPORT, "delta aggregation");
            const auto [message_iteration, delta] = deserialize_iteration_value(message.payload);
            require(message_iteration == iteration, "delta iteration mismatch");
            global_delta += delta;
        }

        const auto after_delta = std::chrono::steady_clock::now();
        const double compute_ms = std::chrono::duration<double, std::milli>(after_dangling - start).count();
        const double apply_ms   = std::chrono::duration<double, std::milli>(after_delta - after_dangling).count();
        const double total_ms   = compute_ms + apply_ms;

        log_csv << iteration << ","
                << std::setprecision(17) << global_delta << ","
                << global_dangling << ","
                << compute_ms << ","
                << apply_ms << ","
                << total_ms << "\n";

        std::cout << "iter " << iteration
                  << ": delta=" << std::setprecision(8) << std::scientific << global_delta
                  << " dangling=" << global_dangling
                  << " total_ms=" << std::fixed << total_ms << std::defaultfloat << std::endl;

        if (global_delta < config.convergence_threshold || iteration == config.max_iterations) {
            broadcast_empty(workers, MsgType::STOP);
            break;
        }
    }

    std::vector<TopKEntry> combined_entries;
    for (const auto& worker : workers) {
        const Message message = recv_message(worker.fd);
        expect_type(message, MsgType::LOCAL_TOPK, "top-k collection");
        const auto local_entries = deserialize_topk(message.payload);
        combined_entries.insert(combined_entries.end(), local_entries.begin(), local_entries.end());
    }

    const auto merged_topk = merge_topk(combined_entries, config.top_k);
    std::ofstream topk_file(std::filesystem::path(output_dir) / "top_k.txt");
    for (const auto& entry : merged_topk) {
        topk_file << entry.node_id << " " << std::setprecision(17) << entry.rank << "\n";
    }

    broadcast_empty(workers, MsgType::SHUTDOWN);

    for (const auto& worker : workers) {
        close_quietly(worker.fd);
    }
    close_quietly(server_fd);

    std::cout << "wrote outputs to " << output_dir << std::endl;
    return 0;
}

}  // namespace dpr

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);

    try {
        return dpr::run(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << "coordinator error: " << error.what() << std::endl;
        return 1;
    }
}
