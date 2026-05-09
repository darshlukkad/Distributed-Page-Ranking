#include "net/message.h"

#include "net/socket_utils.h"

#include <arpa/inet.h>

#include <array>
#include <cstring>
#include <stdexcept>

namespace dpr {
namespace {

template <typename T>
void append_integral(std::vector<std::uint8_t>& buffer, T value) {
    using UnsignedT = typename std::make_unsigned<T>::type;
    const auto raw = static_cast<UnsignedT>(value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        buffer.push_back(static_cast<std::uint8_t>((raw >> (8 * i)) & 0xFF));
    }
}

void append_double(std::vector<std::uint8_t>& buffer, double value) {
    std::uint64_t raw = 0;
    static_assert(sizeof(raw) == sizeof(value), "double must be 64-bit");
    std::memcpy(&raw, &value, sizeof(raw));
    append_integral(buffer, raw);
}

template <typename T>
T read_integral(const std::vector<std::uint8_t>& buffer, size_t& offset) {
    if (offset + sizeof(T) > buffer.size()) {
        throw std::runtime_error("payload underflow");
    }

    using UnsignedT = typename std::make_unsigned<T>::type;
    UnsignedT raw = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        raw |= static_cast<UnsignedT>(buffer[offset + i]) << (8 * i);
    }
    offset += sizeof(T);
    return static_cast<T>(raw);
}

double read_double(const std::vector<std::uint8_t>& buffer, size_t& offset) {
    const auto raw = read_integral<std::uint64_t>(buffer, offset);
    double value = 0.0;
    std::memcpy(&value, &raw, sizeof(value));
    return value;
}

void require_payload_size(const std::vector<std::uint8_t>& payload, size_t expected) {
    if (payload.size() != expected) {
        throw std::runtime_error("unexpected payload size");
    }
}

std::array<std::uint8_t, 4> ipv4_to_bytes(const std::string& host) {
    std::array<std::uint8_t, 4> bytes{};
    if (::inet_pton(AF_INET, host.c_str(), bytes.data()) != 1) {
        throw std::runtime_error("invalid IPv4 address: " + host);
    }
    return bytes;
}

std::string bytes_to_ipv4(const std::uint8_t* bytes) {
    char buffer[INET_ADDRSTRLEN];
    if (::inet_ntop(AF_INET, bytes, buffer, sizeof(buffer)) == nullptr) {
        throw std::runtime_error("failed to decode IPv4 address");
    }
    return buffer;
}

}  // namespace

void send_message(int fd, MsgType type, const std::vector<std::uint8_t>& payload) {
    std::vector<std::uint8_t> header;
    header.reserve(5);
    header.push_back(static_cast<std::uint8_t>(type));
    append_integral<std::uint32_t>(header, static_cast<std::uint32_t>(payload.size()));
    send_exact(fd, header.data(), header.size());
    if (!payload.empty()) {
        send_exact(fd, payload.data(), payload.size());
    }
}

Message recv_message(int fd) {
    std::uint8_t header[5];
    recv_exact(fd, header, sizeof(header));

    std::vector<std::uint8_t> header_vec(header, header + sizeof(header));
    size_t offset = 1;
    const auto payload_length = read_integral<std::uint32_t>(header_vec, offset);

    Message message{
        static_cast<MsgType>(header[0]),
        std::vector<std::uint8_t>(payload_length)
    };
    if (payload_length > 0) {
        recv_exact(fd, message.payload.data(), message.payload.size());
    }
    return message;
}

std::vector<std::uint8_t> serialize_hello(const HelloPayload& payload) {
    std::vector<std::uint8_t> buffer;
    buffer.reserve(22);
    append_integral(buffer, payload.worker_id);
    append_integral(buffer, payload.listen_port);
    append_integral(buffer, payload.vertex_count);
    append_integral(buffer, payload.edge_count);
    return buffer;
}

HelloPayload deserialize_hello(const std::vector<std::uint8_t>& payload) {
    require_payload_size(payload, 22);
    size_t offset = 0;
    HelloPayload hello{};
    hello.worker_id = read_integral<std::uint32_t>(payload, offset);
    hello.listen_port = read_integral<std::uint16_t>(payload, offset);
    hello.vertex_count = read_integral<std::uint64_t>(payload, offset);
    hello.edge_count = read_integral<std::uint64_t>(payload, offset);
    return hello;
}

std::vector<std::uint8_t> serialize_cluster_info(const std::vector<PeerInfo>& peers) {
    std::vector<std::uint8_t> buffer;
    buffer.reserve(4 + peers.size() * 10);
    append_integral(buffer, static_cast<std::uint32_t>(peers.size()));
    for (const auto& peer : peers) {
        append_integral(buffer, peer.worker_id);
        const auto bytes = ipv4_to_bytes(peer.host);
        buffer.insert(buffer.end(), bytes.begin(), bytes.end());
        append_integral(buffer, peer.port);
    }
    return buffer;
}

std::vector<PeerInfo> deserialize_cluster_info(const std::vector<std::uint8_t>& payload) {
    size_t offset = 0;
    const auto num_workers = read_integral<std::uint32_t>(payload, offset);
    std::vector<PeerInfo> peers;
    peers.reserve(num_workers);
    for (std::uint32_t index = 0; index < num_workers; ++index) {
        if (offset + 10 > payload.size()) {
            throw std::runtime_error("invalid CLUSTER_INFO payload");
        }
        PeerInfo peer{};
        peer.worker_id = read_integral<std::uint32_t>(payload, offset);
        peer.host = bytes_to_ipv4(payload.data() + offset);
        offset += 4;
        peer.port = read_integral<std::uint16_t>(payload, offset);
        peers.push_back(peer);
    }
    return peers;
}

std::vector<std::uint8_t> serialize_start_iteration(std::uint32_t iteration) {
    std::vector<std::uint8_t> buffer;
    append_integral(buffer, iteration);
    return buffer;
}

std::uint32_t deserialize_start_iteration(const std::vector<std::uint8_t>& payload) {
    require_payload_size(payload, 4);
    size_t offset = 0;
    return read_integral<std::uint32_t>(payload, offset);
}

std::vector<std::uint8_t> serialize_iteration_value(std::uint32_t iteration, double value) {
    std::vector<std::uint8_t> buffer;
    buffer.reserve(12);
    append_integral(buffer, iteration);
    append_double(buffer, value);
    return buffer;
}

std::pair<std::uint32_t, double> deserialize_iteration_value(const std::vector<std::uint8_t>& payload) {
    require_payload_size(payload, 12);
    size_t offset = 0;
    const auto iteration = read_integral<std::uint32_t>(payload, offset);
    const auto value = read_double(payload, offset);
    return {iteration, value};
}

std::vector<std::uint8_t> serialize_contribs(std::uint32_t iteration, const std::vector<ContribPair>& pairs) {
    std::vector<std::uint8_t> buffer;
    buffer.reserve(8 + pairs.size() * 12);
    append_integral(buffer, iteration);
    append_integral(buffer, static_cast<std::uint32_t>(pairs.size()));
    for (const auto& pair : pairs) {
        append_integral(buffer, pair.vertex_id);
        append_double(buffer, pair.contribution);
    }
    return buffer;
}

ContribPayload deserialize_contribs(const std::vector<std::uint8_t>& payload) {
    size_t offset = 0;
    ContribPayload result{};
    result.iteration = read_integral<std::uint32_t>(payload, offset);
    const auto num_pairs = read_integral<std::uint32_t>(payload, offset);
    result.pairs.reserve(num_pairs);
    for (std::uint32_t index = 0; index < num_pairs; ++index) {
        ContribPair pair{};
        pair.vertex_id = read_integral<std::uint32_t>(payload, offset);
        pair.contribution = read_double(payload, offset);
        result.pairs.push_back(pair);
    }
    return result;
}

std::vector<std::uint8_t> serialize_topk(const std::vector<TopKEntry>& entries) {
    std::vector<std::uint8_t> buffer;
    buffer.reserve(4 + entries.size() * 12);
    append_integral(buffer, static_cast<std::uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        append_integral(buffer, entry.node_id);
        append_double(buffer, entry.rank);
    }
    return buffer;
}

std::vector<TopKEntry> deserialize_topk(const std::vector<std::uint8_t>& payload) {
    size_t offset = 0;
    const auto num_entries = read_integral<std::uint32_t>(payload, offset);
    std::vector<TopKEntry> entries;
    entries.reserve(num_entries);
    for (std::uint32_t index = 0; index < num_entries; ++index) {
        TopKEntry entry{};
        entry.node_id = read_integral<std::uint32_t>(payload, offset);
        entry.rank = read_double(payload, offset);
        entries.push_back(entry);
    }
    return entries;
}

}  // namespace dpr
