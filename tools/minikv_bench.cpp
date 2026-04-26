#include "minikv/db.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

std::string Key(std::uint64_t i) {
    return "key-" + std::to_string(i);
}

std::uint64_t Percentile(std::vector<std::uint64_t> values, double p) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const auto idx = static_cast<std::size_t>((values.size() - 1) * p);
    return values[idx];
}

std::uint64_t DirBytes(const std::filesystem::path& path) {
    std::uint64_t total = 0;
    if (!std::filesystem::exists(path)) {
        return 0;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (entry.is_regular_file()) {
            total += entry.file_size();
        }
    }
    return total;
}

} // namespace

int main(int argc, char** argv) {
    minikv::Options options;
    options.dir = "bench_data";
    options.memtable_bytes_limit = 1 * 1024 * 1024;
    std::size_t n = 100000;
    if (argc > 1) {
        n = static_cast<std::size_t>(std::stoull(argv[1]));
    }
    if (argc > 2) {
        options.dir = argv[2];
    }

    std::filesystem::remove_all(options.dir);
    std::unique_ptr<minikv::DB> db;
    auto status = minikv::DB::Open(options, &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << '\n';
        return 1;
    }

    std::vector<std::uint64_t> latencies;
    latencies.reserve(n);
    const auto write_start = Clock::now();
    for (std::size_t i = 0; i < n; ++i) {
        const auto t0 = Clock::now();
        db->Put(Key(i), "value-" + std::to_string(i));
        const auto t1 = Clock::now();
        latencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    }
    db->Flush();
    const auto write_end = Clock::now();
    const double write_seconds = std::chrono::duration<double>(write_end - write_start).count();

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<std::uint64_t> dist(0, n - 1);
    std::vector<std::uint64_t> read_latencies;
    read_latencies.reserve(n);
    std::string value;
    const auto read_start = Clock::now();
    for (std::size_t i = 0; i < n; ++i) {
        const auto t0 = Clock::now();
        db->Get(Key(dist(rng)), &value);
        const auto t1 = Clock::now();
        read_latencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    }
    const auto read_end = Clock::now();
    const double read_seconds = std::chrono::duration<double>(read_end - read_start).count();

    const auto before_compact = DirBytes(options.dir);
    db->Compact();
    const auto after_compact = DirBytes(options.dir);

    std::cout << "items=" << n << '\n';
    std::cout << "write_qps=" << static_cast<std::uint64_t>(n / write_seconds)
              << " p95_us=" << Percentile(latencies, 0.95)
              << " p99_us=" << Percentile(latencies, 0.99) << '\n';
    std::cout << "read_qps=" << static_cast<std::uint64_t>(n / read_seconds)
              << " p95_us=" << Percentile(read_latencies, 0.95)
              << " p99_us=" << Percentile(read_latencies, 0.99) << '\n';
    std::cout << "disk_before_compact=" << before_compact
              << " disk_after_compact=" << after_compact << '\n';
    return 0;
}
