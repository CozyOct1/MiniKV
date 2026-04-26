#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace minikv {

class LRUCache {
public:
    explicit LRUCache(std::size_t capacity_bytes);

    std::optional<std::string> Get(const std::string& key);
    void Put(std::string key, std::string value);
    void Erase(const std::string& key);
    void Clear();
    std::size_t SizeBytes() const;

private:
    struct Node {
        std::string key;
        std::string value;
        std::size_t bytes;
    };

    void EvictIfNeeded();

    std::size_t capacity_bytes_;
    std::size_t size_bytes_{0};
    std::list<Node> items_;
    std::unordered_map<std::string, std::list<Node>::iterator> index_;
    mutable std::mutex mu_;
};

} // namespace minikv
