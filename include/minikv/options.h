#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace minikv {

struct Options {
    std::string dir = "minikv_data";
    std::size_t memtable_bytes_limit = 4 * 1024 * 1024;
    std::size_t block_cache_capacity = 16 * 1024 * 1024;
    std::size_t bloom_bits_per_key = 10;
    std::size_t level0_compaction_trigger = 4;
    std::size_t max_level = 3;
    bool sync_wal = false;
    bool background_compaction = true;
    std::uint64_t compaction_interval_ms = 1000;
};

} // namespace minikv
