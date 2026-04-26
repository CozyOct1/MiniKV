#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace minikv {

class BloomFilter {
public:
    BloomFilter() = default;
    BloomFilter(std::size_t expected_keys, std::size_t bits_per_key);

    void Add(const std::string& key);
    bool MayContain(const std::string& key) const;

    const std::vector<std::uint8_t>& bits() const;
    void Load(std::vector<std::uint8_t> bits, std::uint32_t bit_count, std::uint32_t hash_count);
    std::uint32_t bit_count() const;
    std::uint32_t hash_count() const;

private:
    static std::uint64_t Hash(const std::string& key, std::uint64_t seed);

    std::vector<std::uint8_t> bits_;
    std::uint32_t bit_count_{0};
    std::uint32_t hash_count_{0};
};

} // namespace minikv
