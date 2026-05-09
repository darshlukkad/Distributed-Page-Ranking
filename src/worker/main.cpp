#include "common/config.h"
#include "net/message.h"
#include "net/socket_utils.h"
#include "worker/partition_loader.h"

#include <signal.h>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace dpr {
namespace {

struct RankArrays {
    std::vector<double> rank_current;
    std::vector<double> rank_next;
    std::vector<double> incoming;
    std::vector<std::vector<ContribPair>> outboxes;
};

[[noreturn]] void usage() {
    throw std::runtime_error(
        "usage: worker --id <worker-id> --config <path> --partition <path> --output-dir <path>");
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

static std::uint32_t rss_mb() {
#ifdef __APPLE__
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS) {
        return static_cast<std::uint32_t>(info.resident_size / (1024ULL * 1024ULL));
    }
    return 0;
#else
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            std::uint64_t kb = 0;
            std::sscanf(line.c_str(), "VmRSS: %llu kB", &kb);
            return static_cast<std::uint32_t>(kb / 1024);
        }
    }
    return 0;
#endif
}

using Clock = std::chrono::steady_clock;
static double ms_since(Clock::time_point t) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t).count();
}

RankArrays init_rank_arrays(const Partition& partition) {
    RankArrays arrays;
    const auto local_vertices = static_cast<size_t>(partition.num_local_vertices);
    const double initial_rank = 1.0 / static_cast<double>(partition.total_global_vertices);
    arrays.rank_current.assign(local_vertices, initial_rank);
    arrays.rank_next.assign(local_vertices, 0.0);
    arrays.incoming.assign(local_vertices, 0.0);
    arrays.outboxes.resize(partition.num_workers);
    return arrays;
}

std::unordered_map<std::uint32_t, int> setup_mesh(std::uint32_t worker_id,
                                                  const std::vector<PeerInfo>& peers,
                                                  int listen_fd) {
    std::unordered_map<std::uint32_t, int> peer_fds;

    for (const auto& peer : peers) {
        if (peer.worker_id <= worker_id) {
            continue;
        }
        const int fd = tcp_connect(peer.host, peer.port);
        send_exact(fd, &worker_id, sizeof(worker_id));
        peer_fds[peer.worker_id] = fd;
    }

    for (std::uint32_t expected = 0; expected < worker_id; ++expected) {
        const int fd = tcp_accept(listen_fd);
        std::uint32_t remote_worker_id = 0;
        recv_exact(fd, &remote_worker_id, sizeof(remote_worker_id));
        require(remote_worker_id < worker_id, "invalid mesh handshake");
        peer_fds[remote_worker_id] = fd;
    }

    require(peer_fds.size() + 1 == peers.size(), "peer mesh incomplete");
    return peer_fds;
}

WorkerMetrics run_iteration(std::uint32_t iteration,
                            const Partition& partition,
                            RankArrays& arrays,
                            const std::unordered_map<std::uint32_t, int>& peer_fds,
                            int coordinator_fd,
                            double damping_factor) {
    WorkerMetrics metrics{};
    metrics.worker_id = partition.worker_id;
    metrics.iteration = iteration;

    // ── Phase 1: local vertex walk ────────────────────────────────────────────
    const auto t_compute = Clock::now();
    double local_dangling_mass = 0.0;

    for (std::uint64_t index = 0; index < partition.num_local_vertices; ++index) {
        const std::uint64_t start = partition.edge_offsets[static_cast<size_t>(index)];
        const std::uint64_t end = partition.edge_offsets[static_cast<size_t>(index + 1)];
        const std::uint32_t out_degree = static_cast<std::uint32_t>(end - start);

        if (out_degree == 0) {
            local_dangling_mass += arrays.rank_current[static_cast<size_t>(index)];
            continue;
        }

        const double contribution = arrays.rank_current[static_cast<size_t>(index)] / out_degree;
        for (std::uint64_t edge = start; edge < end; ++edge) {
            const std::uint32_t destination = partition.edges[static_cast<size_t>(edge)];
            const std::uint32_t owner = destination % partition.num_workers;
            const auto local_index = static_cast<size_t>(destination / partition.num_workers);

            if (owner == partition.worker_id) {
                arrays.incoming[local_index] += contribution;
            } else {
                arrays.outboxes[owner].push_back(ContribPair{destination, contribution});
            }
        }
    }

    metrics.compute_ms = ms_since(t_compute);

    // ── Phase 2: CONTRIBS round-robin exchange ────────────────────────────────
    const auto t_exchange = Clock::now();

    std::unordered_map<std::uint32_t, std::vector<std::uint8_t>> serialized;
    for (const auto& [peer_id, fd] : peer_fds) {
        (void)fd;
        serialized[peer_id] = serialize_contribs(iteration, arrays.outboxes[peer_id]);
        arrays.outboxes[peer_id].clear();
        metrics.contribs_bytes += serialized[peer_id].size() + 5; // +5 for frame header
    }

    // Round-robin exchange: in round r, worker i sends to (i+r)%N and receives
    // from (i-r+N)%N. Each send is matched with exactly one receive on the other
    // side, preventing kernel buffer exhaustion with large CONTRIBS payloads.
    const std::uint32_t N = partition.num_workers;
    const std::uint32_t my_id = partition.worker_id;
    for (std::uint32_t r = 1; r < N; ++r) {
        const std::uint32_t send_to   = (my_id + r) % N;
        const std::uint32_t recv_from = (my_id + N - r) % N;

        if (peer_fds.count(send_to)) {
            send_message(peer_fds.at(send_to), MsgType::CONTRIBS, serialized.at(send_to));
        }
        if (peer_fds.count(recv_from)) {
            const Message message = recv_message(peer_fds.at(recv_from));
            expect_type(message, MsgType::CONTRIBS, "contribution receive");
            const auto payload = deserialize_contribs(message.payload);
            require(payload.iteration == iteration, "contribution iteration mismatch");
            for (const auto& pair : payload.pairs) {
                const auto local_index = static_cast<size_t>(pair.vertex_id / partition.num_workers);
                arrays.incoming[local_index] += pair.contribution;
            }
        }
    }

    metrics.exchange_ms = ms_since(t_exchange);

    send_message(coordinator_fd,
                 MsgType::DANGLING_REPORT,
                 serialize_iteration_value(iteration, local_dangling_mass));

    const Message dangling_message = recv_message(coordinator_fd);
    expect_type(dangling_message, MsgType::GLOBAL_DANGLING, "global dangling receive");
    const auto [dangling_iteration, global_dangling] = deserialize_iteration_value(dangling_message.payload);
    require(dangling_iteration == iteration, "global dangling iteration mismatch");

    // ── Phase 3: apply PageRank formula ──────────────────────────────────────
    const auto t_apply = Clock::now();

    const double total_vertices = static_cast<double>(partition.total_global_vertices);
    const double teleport = (1.0 - damping_factor) / total_vertices;
    const double dangling_term = damping_factor * global_dangling / total_vertices;

    double local_delta = 0.0;
    for (std::uint64_t index = 0; index < partition.num_local_vertices; ++index) {
        const size_t local_index = static_cast<size_t>(index);
        arrays.rank_next[local_index] =
            teleport + damping_factor * arrays.incoming[local_index] + dangling_term;
        local_delta += std::fabs(arrays.rank_next[local_index] - arrays.rank_current[local_index]);
    }

    metrics.apply_ms  = ms_since(t_apply);
    metrics.memory_mb = rss_mb();

    send_message(coordinator_fd,
                 MsgType::DELTA_REPORT,
                 serialize_iteration_value(iteration, local_delta));

    std::swap(arrays.rank_current, arrays.rank_next);
    std::fill(arrays.incoming.begin(), arrays.incoming.end(), 0.0);

    return metrics;
}

std::vector<TopKEntry> compute_local_topk(const Partition& partition,
                                          const RankArrays& arrays,
                                          std::uint32_t top_k) {
    std::vector<TopKEntry> entries;
    entries.reserve(static_cast<size_t>(partition.num_local_vertices));
    for (std::uint64_t index = 0; index < partition.num_local_vertices; ++index) {
        entries.push_back(TopKEntry{
            partition.local_vertex_ids[static_cast<size_t>(index)],
            arrays.rank_current[static_cast<size_t>(index)]
        });
    }

    const auto cmp = [](const TopKEntry& lhs, const TopKEntry& rhs) {
        if (lhs.rank == rhs.rank) return lhs.node_id < rhs.node_id;
        return lhs.rank > rhs.rank;
    };

    if (entries.size() > top_k) {
        std::nth_element(entries.begin(), entries.begin() + top_k, entries.end(), cmp);
        entries.resize(top_k);
    }
    std::sort(entries.begin(), entries.end(), cmp);
    return entries;
}

void write_result_file(const Partition& partition,
                       const RankArrays& arrays,
                       std::uint32_t worker_id,
                       const std::filesystem::path& output_dir) {
    std::ofstream output(output_dir / ("result_worker_" + std::to_string(worker_id) + ".txt"));
    for (std::uint64_t index = 0; index < partition.num_local_vertices; ++index) {
        output << partition.local_vertex_ids[static_cast<size_t>(index)] << " "
               << std::setprecision(17) << arrays.rank_current[static_cast<size_t>(index)] << "\n";
    }
}

}  // namespace

int run(int argc, char** argv) {
    const std::string worker_id_arg = get_arg(argc, argv, "--id");
    const std::string config_path = get_arg(argc, argv, "--config");
    const std::string partition_path = get_arg(argc, argv, "--partition");
    const std::string output_dir = get_arg(argc, argv, "--output-dir");
    if (worker_id_arg.empty() || config_path.empty() || partition_path.empty() || output_dir.empty()) {
        usage();
    }

    const auto worker_id = static_cast<std::uint32_t>(std::stoul(worker_id_arg));
    const auto config = load_config(config_path);
    require(worker_id < config.num_workers, "worker id out of range");

    const auto partition = load_partition(partition_path);
    require(partition.worker_id == worker_id, "partition worker_id mismatch");
    require(partition.num_workers == config.num_workers, "partition num_workers mismatch");

    std::filesystem::create_directories(output_dir);

    RankArrays arrays = init_rank_arrays(partition);

    const int listen_fd = tcp_listen(config.workers[worker_id].port);
    const int coordinator_fd = tcp_connect(config.coordinator_host, config.coordinator_port);

    send_message(coordinator_fd,
                 MsgType::HELLO,
                 serialize_hello(HelloPayload{
                     worker_id,
                     config.workers[worker_id].port,
                     partition.num_local_vertices,
                     partition.num_local_edges
                 }));

    const Message cluster_message = recv_message(coordinator_fd);
    expect_type(cluster_message, MsgType::CLUSTER_INFO, "cluster info receive");
    const auto peers = deserialize_cluster_info(cluster_message.payload);

    const auto peer_fds = setup_mesh(worker_id, peers, listen_fd);
    close_quietly(listen_fd);

    send_message(coordinator_fd, MsgType::READY, {});
    send_message(coordinator_fd, MsgType::LOAD_COMPLETE, {});

    while (true) {
        const Message message = recv_message(coordinator_fd);
        if (message.type == MsgType::START_ITERATION) {
            const auto iteration = deserialize_start_iteration(message.payload);
            const auto metrics = run_iteration(iteration, partition, arrays, peer_fds, coordinator_fd, config.damping_factor);
            send_message(coordinator_fd, MsgType::METRICS, serialize_worker_metrics(metrics));
            continue;
        }

        if (message.type == MsgType::STOP) {
            break;
        }

        if (message.type == MsgType::ABORT) {
            throw std::runtime_error("received ABORT from coordinator");
        }

        throw std::runtime_error("unexpected coordinator message");
    }

    write_result_file(partition, arrays, worker_id, output_dir);
    const auto local_topk = compute_local_topk(partition, arrays, config.top_k);
    send_message(coordinator_fd, MsgType::LOCAL_TOPK, serialize_topk(local_topk));

    const Message shutdown = recv_message(coordinator_fd);
    expect_type(shutdown, MsgType::SHUTDOWN, "shutdown receive");

    for (const auto& [peer_id, fd] : peer_fds) {
        (void)peer_id;
        close_quietly(fd);
    }
    close_quietly(coordinator_fd);

    return 0;
}

}  // namespace dpr

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);

    try {
        return dpr::run(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << "worker error: " << error.what() << std::endl;
        return 1;
    }
}
