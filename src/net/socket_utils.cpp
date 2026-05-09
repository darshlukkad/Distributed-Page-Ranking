#include "net/socket_utils.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>

namespace dpr {
namespace {

std::string errno_message(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

}  // namespace

void set_nodelay(int fd) {
    const int flag = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) != 0) {
        throw std::runtime_error(errno_message("setsockopt(TCP_NODELAY) failed"));
    }
}

int tcp_listen(uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    addrinfo* results = nullptr;
    const auto port_string = std::to_string(port);
    if (::getaddrinfo(nullptr, port_string.c_str(), &hints, &results) != 0) {
        throw std::runtime_error("getaddrinfo failed for listen port " + port_string);
    }

    int server_fd = -1;
    for (addrinfo* result = results; result != nullptr; result = result->ai_next) {
        server_fd = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        if (server_fd < 0) {
            continue;
        }

        const int reuse = 1;
        ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        if (::bind(server_fd, result->ai_addr, result->ai_addrlen) == 0 &&
            ::listen(server_fd, SOMAXCONN) == 0) {
            break;
        }

        ::close(server_fd);
        server_fd = -1;
    }

    ::freeaddrinfo(results);

    if (server_fd < 0) {
        throw std::runtime_error("failed to bind/listen on port " + port_string);
    }

    return server_fd;
}

int tcp_connect(const std::string& host, uint16_t port, int retries, int retry_delay_ms) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* results = nullptr;
    const auto port_string = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_string.c_str(), &hints, &results) != 0) {
        throw std::runtime_error("getaddrinfo failed for host " + host + ":" + port_string);
    }

    for (int attempt = 0; attempt < retries; ++attempt) {
        for (addrinfo* result = results; result != nullptr; result = result->ai_next) {
            const int fd = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
            if (fd < 0) {
                continue;
            }

            if (::connect(fd, result->ai_addr, result->ai_addrlen) == 0) {
                ::freeaddrinfo(results);
                set_nodelay(fd);
                return fd;
            }

            ::close(fd);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
    }

    ::freeaddrinfo(results);
    throw std::runtime_error("failed to connect to " + host + ":" + port_string);
}

int tcp_accept(int server_fd) {
    const int fd = ::accept(server_fd, nullptr, nullptr);
    if (fd < 0) {
        throw std::runtime_error(errno_message("accept failed"));
    }
    set_nodelay(fd);
    return fd;
}

void send_exact(int fd, const void* buffer, std::size_t size) {
    const auto* bytes = static_cast<const std::uint8_t*>(buffer);
    std::size_t sent = 0;
    while (sent < size) {
        const auto rc = ::send(fd, bytes + sent, size - sent, 0);
        if (rc <= 0) {
            throw std::runtime_error(errno_message("send failed"));
        }
        sent += static_cast<std::size_t>(rc);
    }
}

void recv_exact(int fd, void* buffer, std::size_t size) {
    auto* bytes = static_cast<std::uint8_t*>(buffer);
    std::size_t received = 0;
    while (received < size) {
        const auto rc = ::recv(fd, bytes + received, size - received, 0);
        if (rc <= 0) {
            throw std::runtime_error(errno_message("recv failed"));
        }
        received += static_cast<std::size_t>(rc);
    }
}

void close_quietly(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

}  // namespace dpr
