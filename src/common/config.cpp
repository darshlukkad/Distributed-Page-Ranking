#include "common/config.h"

#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace dpr {
namespace {

std::string trim(const std::string& value) {
    const auto start = value.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        return "";
    }
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(start, end - start + 1);
}

std::string strip_comment(const std::string& value) {
    const auto hash = value.find('#');
    const auto semicolon = value.find(';');
    const auto pos = std::min(hash, semicolon);
    if (pos == std::string::npos) {
        return value;
    }
    return value.substr(0, pos);
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

}  // namespace

ClusterConfig load_config(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open config: " + path);
    }

    ClusterConfig config;
    std::map<uint32_t, WorkerEndpoint> worker_map;
    std::optional<uint32_t> current_worker_id;

    std::string line;
    size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        line = trim(strip_comment(line));
        if (line.empty()) {
            continue;
        }

        if (line.front() == '[' && line.back() == ']') {
            const std::string section = line.substr(1, line.size() - 2);
            current_worker_id.reset();
            if (section.rfind("worker_", 0) == 0) {
                current_worker_id = static_cast<uint32_t>(std::stoul(section.substr(7)));
                worker_map[*current_worker_id] = WorkerEndpoint{};
            }
            continue;
        }

        const auto equals = line.find('=');
        if (equals == std::string::npos) {
            throw std::runtime_error("invalid config line " + std::to_string(line_number));
        }

        const std::string key = trim(line.substr(0, equals));
        const std::string value = trim(line.substr(equals + 1));

        if (current_worker_id.has_value()) {
            auto& worker = worker_map[*current_worker_id];
            if (key == "host") {
                worker.host = value;
            } else if (key == "port") {
                worker.port = static_cast<uint16_t>(std::stoul(value));
            } else {
                throw std::runtime_error("unknown worker key '" + key + "'");
            }
            continue;
        }

        if (key == "coordinator_host") {
            config.coordinator_host = value;
        } else if (key == "coordinator_port") {
            config.coordinator_port = static_cast<uint16_t>(std::stoul(value));
        } else if (key == "num_workers") {
            config.num_workers = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "damping_factor") {
            config.damping_factor = std::stod(value);
        } else if (key == "convergence_threshold") {
            config.convergence_threshold = std::stod(value);
        } else if (key == "max_iterations") {
            config.max_iterations = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "top_k") {
            config.top_k = static_cast<uint32_t>(std::stoul(value));
        } else {
            throw std::runtime_error("unknown config key '" + key + "'");
        }
    }

    require(config.num_workers > 0, "num_workers must be set");
    require(config.coordinator_port > 0, "coordinator_port must be set");
    require(!config.coordinator_host.empty(), "coordinator_host must be set");
    require(config.damping_factor > 0.0 && config.damping_factor < 1.0,
            "damping_factor must be in (0, 1)");

    config.workers.resize(config.num_workers);
    for (uint32_t worker_id = 0; worker_id < config.num_workers; ++worker_id) {
        const auto it = worker_map.find(worker_id);
        if (it == worker_map.end()) {
            throw std::runtime_error("missing [worker_" + std::to_string(worker_id) + "] section");
        }
        require(!it->second.host.empty(),
                "worker_" + std::to_string(worker_id) + " host must be set");
        require(it->second.port > 0,
                "worker_" + std::to_string(worker_id) + " port must be set");
        config.workers[worker_id] = it->second;
    }

    return config;
}

}  // namespace dpr
