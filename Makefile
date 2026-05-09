CXX ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -Isrc

COMMON_SOURCES = \
	src/common/config.cpp \
	src/net/socket_utils.cpp \
	src/net/message.cpp \
	src/worker/partition_loader.cpp

COORDINATOR_SOURCES = src/coordinator/main.cpp $(COMMON_SOURCES)
WORKER_SOURCES = src/worker/main.cpp $(COMMON_SOURCES)

.PHONY: all clean

all: build/coordinator build/worker

build:
	mkdir -p build

build/coordinator: $(COORDINATOR_SOURCES) | build
	$(CXX) $(CXXFLAGS) $(COORDINATOR_SOURCES) -o $@

build/worker: $(WORKER_SOURCES) | build
	$(CXX) $(CXXFLAGS) $(WORKER_SOURCES) -o $@

clean:
	rm -rf build demo_out
