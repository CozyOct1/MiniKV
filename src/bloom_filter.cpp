#include "minikv/bloom_filter.h"

#include <algorithm>

namespace minikv {

BloomFilter::BloomFilter(std::size_t expected_keys, std::size_t bits_per_key) {
    bit_count_ = static_cast<std::uint32_t>(std::max<std::size_t>(64, expected_keys * bits_per_key));
    hash_count_ = static_cast<std::uint32_t>(std::max<std::size_t>(1, std::min<std::size_t>(8, bits_per_key * 69 / 100)));
    bits_.assign((bit_count_ + 7) / 8, 0);
}

void BloomFilter::Add(const std::string& key) {
    if (bit_count_ == 0) {
        return;
    }
    const auto h1 = Hash(key, 0x9e3779b97f4a7c15ULL);
    const auto h2 = Hash(key, 0xc2b2ae3d27d4eb4fULL);
    for (std::uint32_t i = 0; i < hash_count_; ++i) {
        const auto bit = static_cast<std::uint32_t>((h1 + i * h2) % bit_count_);
        bits_[bit / 8] |= static_cast<std::uint8_t>(1U << (bit % 8));
    }
}

bool BloomFilter::MayContain(const std::string& key) const {
    if (bit_count_ == 0) {
        return true;
    }
    const auto h1 = Hash(key, 0x9e3779b97f4a7c15ULL);
    const auto h2 = Hash(key, 0xc2b2ae3d27d4eb4fULL);
    for (std::uint32_t i = 0; i < hash_count_; ++i) {
        const auto bit = static_cast<std::uint32_t>((h1 + i * h2) % bit_count_);
        if ((bits_[bit / 8] & static_cast<std::uint8_t>(1U << (bit % 8))) == 0) {
            return false;
        }
    }
    return true;
}

const std::vector<std::uint8_t>& BloomFilter::bits() const {
    return bits_;
}

void BloomFilter::Load(std::vector<std::uint8_t> bits, std::uint32_t bit_count, std::uint32_t hash_count) {
    bits_ = std::move(bits);
    bit_count_ = bit_count;
    hash_count_ = hash_count;
}

std::uint32_t BloomFilter::bit_count() const {
    return bit_count_;
}

std::uint32_t BloomFilter::hash_count() const {
    return hash_count_;
}

std::uint64_t BloomFilter::Hash(const std::string& key, std::uint64_t seed) {
    std::uint64_t h = 1469598103934665603ULL ^ seed;
    for (unsigned char c : key) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return h;
}

} // namespace minikv
