#include "worker/partition_loader.h"

#include <fstream>
#include <stdexcept>

namespace dpr {
namespace {

constexpr std::uint32_t kMagic = 0x50414745;
constexpr std::uint32_t kVersion = 1;

template <typename T>
void read_exact(std::ifstream& input, T& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(T));
    if (!input) {
        throw std::runtime_error("failed to read partition file");
    }
}

template <typename T>
void read_vector(std::ifstream& input, std::vector<T>& values) {
    if (values.empty()) {
        return;
    }
    input.read(reinterpret_cast<char*>(values.data()),
               static_cast<std::streamsize>(values.size() * sizeof(T)));
    if (!input) {
        throw std::runtime_error("failed to read partition vector");
    }
}

}  // namespace

Partition load_partition(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open partition: " + path);
    }

    std::uint32_t magic = 0;
    std::uint32_t version = 0;

    Partition partition;
    read_exact(input, magic);
    read_exact(input, version);
    read_exact(input, partition.worker_id);
    read_exact(input, partition.num_workers);
    read_exact(input, partition.total_global_vertices);
    read_exact(input, partition.num_local_vertices);
    read_exact(input, partition.num_local_edges);

    if (magic != kMagic) {
        throw std::runtime_error("invalid partition magic");
    }
    if (version != kVersion) {
        throw std::runtime_error("unsupported partition version");
    }

    partition.local_vertex_ids.resize(static_cast<size_t>(partition.num_local_vertices));
    partition.edge_offsets.resize(static_cast<size_t>(partition.num_local_vertices + 1));
    partition.edges.resize(static_cast<size_t>(partition.num_local_edges));

    read_vector(input, partition.local_vertex_ids);
    read_vector(input, partition.edge_offsets);
    read_vector(input, partition.edges);

    if (partition.edge_offsets.empty() || partition.edge_offsets.back() != partition.num_local_edges) {
        throw std::runtime_error("partition edge offset mismatch");
    }

    return partition;
}

}  // namespace dpr
