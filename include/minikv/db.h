#pragma once

#include "minikv/options.h"
#include "minikv/status.h"

#include <memory>
#include <optional>
#include <string>

namespace minikv {

class DB {
public:
    static Status Open(const Options& options, std::unique_ptr<DB>* db);

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
    ~DB();

    Status Put(const std::string& key, const std::string& value);
    Status Delete(const std::string& key);
    Status Get(const std::string& key, std::string* value) const;
    Status Flush();
    Status Compact();

    std::size_t ApproximateMemoryUsage() const;
    std::uint64_t CurrentSequence() const;

private:
    class Impl;
    explicit DB(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

} // namespace minikv
