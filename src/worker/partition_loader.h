#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dpr {

struct Partition {
    std::uint32_t worker_id = 0;
    std::uint32_t num_workers = 0;
    std::uint64_t total_global_vertices = 0;
    std::uint64_t num_local_vertices = 0;
    std::uint64_t num_local_edges = 0;
    std::vector<std::uint32_t> local_vertex_ids;
    std::vector<std::uint64_t> edge_offsets;
    std::vector<std::uint32_t> edges;
};

Partition load_partition(const std::string& path);

}  // namespace dpr
