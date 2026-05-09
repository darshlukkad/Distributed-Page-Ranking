#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace dpr {

int tcp_listen(uint16_t port);
int tcp_connect(const std::string& host, uint16_t port, int retries = 50, int retry_delay_ms = 200);
int tcp_accept(int server_fd);
void set_nodelay(int fd);
void send_exact(int fd, const void* buffer, std::size_t size);
void recv_exact(int fd, void* buffer, std::size_t size);
void close_quietly(int fd);

}  // namespace dpr
