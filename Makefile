CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic -Iinclude
LDFLAGS ?= -pthread

BUILD_DIR := build
LIB_SRC := src/bloom_filter.cpp src/db.cpp src/lru_cache.cpp src/sstable.cpp src/status.cpp src/wal.cpp

.PHONY: all test clean

all: $(BUILD_DIR)/minikv_cli $(BUILD_DIR)/minikv_bench $(BUILD_DIR)/minikv_tests

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/minikv_cli: $(LIB_SRC) tools/minikv_cli.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

$(BUILD_DIR)/minikv_bench: $(LIB_SRC) tools/minikv_bench.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

$(BUILD_DIR)/minikv_tests: $(LIB_SRC) tests/minikv_tests.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

test: $(BUILD_DIR)/minikv_tests
	./$(BUILD_DIR)/minikv_tests

clean:
	rm -rf $(BUILD_DIR)
