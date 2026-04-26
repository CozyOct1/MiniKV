#include "minikv/lru_cache.h"

namespace minikv {

LRUCache::LRUCache(std::size_t capacity_bytes) : capacity_bytes_(capacity_bytes) {}

std::optional<std::string> LRUCache::Get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }
    items_.splice(items_.begin(), items_, it->second);
    return it->second->value;
}

void LRUCache::Put(std::string key, std::string value) {
    if (capacity_bytes_ == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const std::size_t bytes = key.size() + value.size();
    auto it = index_.find(key);
    if (it != index_.end()) {
        size_bytes_ -= it->second->bytes;
        items_.erase(it->second);
        index_.erase(it);
    }
    items_.push_front(Node{std::move(key), std::move(value), bytes});
    index_[items_.front().key] = items_.begin();
    size_bytes_ += bytes;
    EvictIfNeeded();
}

void LRUCache::Erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) {
        return;
    }
    size_bytes_ -= it->second->bytes;
    items_.erase(it->second);
    index_.erase(it);
}

void LRUCache::Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    items_.clear();
    index_.clear();
    size_bytes_ = 0;
}

std::size_t LRUCache::SizeBytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return size_bytes_;
}

void LRUCache::EvictIfNeeded() {
    while (size_bytes_ > capacity_bytes_ && !items_.empty()) {
        auto& victim = items_.back();
        size_bytes_ -= victim.bytes;
        index_.erase(victim.key);
        items_.pop_back();
    }
}

} // namespace minikv
