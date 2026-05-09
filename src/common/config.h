#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dpr {

struct WorkerEndpoint {
    std::string host;
    uint16_t port = 0;
};

struct ClusterConfig {
    std::string coordinator_host;
    uint16_t coordinator_port = 0;
    uint32_t num_workers = 0;
    double damping_factor = 0.85;
    double convergence_threshold = 1e-6;
    uint32_t max_iterations = 100;
    uint32_t top_k = 100;
    std::vector<WorkerEndpoint> workers;
};

ClusterConfig load_config(const std::string& path);

}  // namespace dpr
