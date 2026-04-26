#pragma once

#include "minikv/bloom_filter.h"
#include "minikv/status.h"
#include "record.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace minikv {

struct TableBuildOptions {
    std::size_t bloom_bits_per_key = 10;
};

class SSTable {
public:
    struct IndexEntry {
        std::string key;
        std::uint64_t offset = 0;
        std::uint64_t sequence = 0;
        ValueType type = ValueType::kPut;
    };

    static Status Build(const std::string& path,
                        const std::vector<Record>& records,
                        const TableBuildOptions& options);
    static Status Open(const std::string& path, std::shared_ptr<SSTable>* table);

    Status Get(const std::string& key, Record* record) const;
    const std::string& path() const;
    const std::string& smallest_key() const;
    const std::string& largest_key() const;
    std::uint64_t max_sequence() const;
    const std::vector<IndexEntry>& index() const;
    Status ReadAll(std::vector<Record>* records) const;

private:
    explicit SSTable(std::string path);

    std::string path_;
    std::string smallest_key_;
    std::string largest_key_;
    std::uint64_t max_sequence_{0};
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
};

} // namespace minikv
