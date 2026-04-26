#include "minikv/db.h"

#include "minikv/lru_cache.h"
#include "sstable.h"
#include "wal.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <thread>

namespace minikv {
namespace fs = std::filesystem;

class DB::Impl {
public:
    explicit Impl(Options options)
        : options_(std::move(options)),
          wal_(WalPath(), options_.sync_wal),
          cache_(options_.block_cache_capacity) {}

    ~Impl() {
        stop_.store(true);
        cv_.notify_all();
        if (bg_.joinable()) {
            bg_.join();
        }
        std::unique_lock<std::shared_mutex> lock(mu_);
        FlushLocked();
        wal_.Close();
    }

    Status Open() {
        fs::create_directories(options_.dir);
        auto status = LoadTables();
        if (!status.ok()) {
            return status;
        }
        status = RecoverWal();
        if (!status.ok()) {
            return status;
        }
        status = wal_.OpenForAppend();
        if (!status.ok()) {
            return status;
        }
        if (options_.background_compaction) {
            bg_ = std::thread([this] { BackgroundLoop(); });
        }
        return Status::OK();
    }

    Status Put(const std::string& key, const std::string& value) {
        if (key.empty()) {
            return Status::InvalidArgument("empty key");
        }
        std::unique_lock<std::shared_mutex> lock(mu_);
        Record record{++sequence_, ValueType::kPut, key, value};
        auto status = wal_.Append(record);
        if (!status.ok()) {
            return status;
        }
        ApplyToMemtable(record);
        if (memtable_bytes_ >= options_.memtable_bytes_limit) {
            status = FlushLocked();
            cv_.notify_all();
        }
        return status;
    }

    Status Delete(const std::string& key) {
        if (key.empty()) {
            return Status::InvalidArgument("empty key");
        }
        std::unique_lock<std::shared_mutex> lock(mu_);
        Record record{++sequence_, ValueType::kDelete, key, ""};
        auto status = wal_.Append(record);
        if (!status.ok()) {
            return status;
        }
        ApplyToMemtable(record);
        if (memtable_bytes_ >= options_.memtable_bytes_limit) {
            status = FlushLocked();
            cv_.notify_all();
        }
        return status;
    }

    Status Get(const std::string& key, std::string* value) const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        const auto mem = memtable_.find(key);
        if (mem != memtable_.end()) {
            if (mem->second.type == ValueType::kDelete) {
                return Status::NotFound("deleted");
            }
            *value = mem->second.value;
            return Status::OK();
        }

        const auto cached = cache_.Get(key);
        if (cached.has_value()) {
            *value = *cached;
            return Status::OK();
        }

        Record best;
        bool found = false;
        for (const auto& table : tables_) {
            Record candidate;
            auto status = table->Get(key, &candidate);
            if (status.ok() && (!found || candidate.sequence > best.sequence)) {
                best = std::move(candidate);
                found = true;
            }
        }
        if (!found) {
            return Status::NotFound("key not found");
        }
        if (best.type == ValueType::kDelete) {
            cache_.Erase(key);
            return Status::NotFound("deleted");
        }
        cache_.Put(key, best.value);
        *value = best.value;
        return Status::OK();
    }

    Status Flush() {
        std::unique_lock<std::shared_mutex> lock(mu_);
        return FlushLocked();
    }

    Status Compact() {
        std::unique_lock<std::shared_mutex> lock(mu_);
        return CompactLocked();
    }

    std::size_t ApproximateMemoryUsage() const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return memtable_bytes_ + cache_.SizeBytes();
    }

    std::uint64_t CurrentSequence() const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return sequence_;
    }

private:
    std::string WalPath() const {
        return (fs::path(options_.dir) / "current.wal").string();
    }

    std::string TablePath(std::uint64_t file_id, int level) const {
        std::ostringstream os;
        os << "level-" << level << "-" << file_id << ".sst";
        return (fs::path(options_.dir) / os.str()).string();
    }

    Status LoadTables() {
        tables_.clear();
        next_file_id_ = 1;
        if (!fs::exists(options_.dir)) {
            return Status::OK();
        }
        for (const auto& entry : fs::directory_iterator(options_.dir)) {
            if (entry.path().extension() != ".sst") {
                continue;
            }
            std::shared_ptr<SSTable> table;
            auto status = SSTable::Open(entry.path().string(), &table);
            if (!status.ok()) {
                return status;
            }
            tables_.push_back(std::move(table));
            sequence_ = std::max(sequence_, tables_.back()->max_sequence());
            next_file_id_ = std::max(next_file_id_, ParseFileId(entry.path().filename().string()) + 1);
        }
        SortTablesNewestFirst();
        return Status::OK();
    }

    Status RecoverWal() {
        std::vector<Record> records;
        auto status = WAL::Replay(WalPath(), &records);
        if (!status.ok()) {
            return status;
        }
        for (const auto& record : records) {
            sequence_ = std::max(sequence_, record.sequence);
            ApplyToMemtable(record);
        }
        return Status::OK();
    }

    static std::uint64_t ParseFileId(const std::string& filename) {
        const auto second_dash = filename.find('-', filename.find('-') + 1);
        const auto dot = filename.find('.', second_dash);
        if (second_dash == std::string::npos || dot == std::string::npos) {
            return 0;
        }
        return std::stoull(filename.substr(second_dash + 1, dot - second_dash - 1));
    }

    void ApplyToMemtable(const Record& record) {
        auto previous = memtable_.find(record.key);
        if (previous != memtable_.end()) {
            memtable_bytes_ -= previous->second.key.size() + previous->second.value.size() + 32;
        }
        memtable_[record.key] = record;
        memtable_bytes_ += record.key.size() + record.value.size() + 32;
        if (record.type == ValueType::kDelete) {
            cache_.Erase(record.key);
        } else {
            cache_.Put(record.key, record.value);
        }
    }

    Status FlushLocked() {
        if (memtable_.empty()) {
            return Status::OK();
        }
        std::vector<Record> records;
        records.reserve(memtable_.size());
        for (const auto& [_, record] : memtable_) {
            records.push_back(record);
        }
        const auto path = TablePath(next_file_id_++, 0);
        auto status = SSTable::Build(path, records, TableBuildOptions{options_.bloom_bits_per_key});
        if (!status.ok()) {
            return status;
        }
        std::shared_ptr<SSTable> table;
        status = SSTable::Open(path, &table);
        if (!status.ok()) {
            return status;
        }
        tables_.push_back(std::move(table));
        SortTablesNewestFirst();
        memtable_.clear();
        memtable_bytes_ = 0;
        wal_.Close();
        std::ofstream truncate(WalPath(), std::ios::binary | std::ios::trunc);
        truncate.close();
        return wal_.OpenForAppend();
    }

    Status CompactLocked() {
        if (tables_.size() <= 1) {
            return Status::OK();
        }
        std::map<std::string, Record> latest;
        std::vector<std::string> old_paths;
        for (const auto& table : tables_) {
            std::vector<Record> records;
            auto status = table->ReadAll(&records);
            if (!status.ok()) {
                return status;
            }
            old_paths.push_back(table->path());
            for (const auto& record : records) {
                auto it = latest.find(record.key);
                if (it == latest.end() || record.sequence > it->second.sequence) {
                    latest[record.key] = record;
                }
            }
        }

        std::vector<Record> merged;
        for (const auto& [_, record] : latest) {
            if (record.type == ValueType::kPut) {
                merged.push_back(record);
            }
        }
        if (merged.empty()) {
            tables_.clear();
        } else {
            const auto path = TablePath(next_file_id_++, 1);
            auto status = SSTable::Build(path, merged, TableBuildOptions{options_.bloom_bits_per_key});
            if (!status.ok()) {
                return status;
            }
            std::shared_ptr<SSTable> table;
            status = SSTable::Open(path, &table);
            if (!status.ok()) {
                return status;
            }
            tables_.clear();
            tables_.push_back(std::move(table));
        }
        for (const auto& path : old_paths) {
            std::error_code ec;
            fs::remove(path, ec);
        }
        cache_.Clear();
        return Status::OK();
    }

    void SortTablesNewestFirst() {
        std::sort(tables_.begin(), tables_.end(), [](const auto& a, const auto& b) {
            return a->max_sequence() > b->max_sequence();
        });
    }

    void BackgroundLoop() {
        std::unique_lock<std::mutex> lock(bg_mu_);
        while (!stop_.load()) {
            cv_.wait_for(lock, std::chrono::milliseconds(options_.compaction_interval_ms));
            if (stop_.load()) {
                break;
            }
            std::unique_lock<std::shared_mutex> db_lock(mu_);
            if (tables_.size() >= options_.level0_compaction_trigger) {
                CompactLocked();
            }
        }
    }

    Options options_;
    WAL wal_;
    mutable LRUCache cache_;
    mutable std::shared_mutex mu_;
    std::map<std::string, Record> memtable_;
    std::size_t memtable_bytes_{0};
    std::vector<std::shared_ptr<SSTable>> tables_;
    std::uint64_t sequence_{0};
    std::uint64_t next_file_id_{1};
    std::atomic<bool> stop_{false};
    std::thread bg_;
    std::mutex bg_mu_;
    std::condition_variable cv_;
};

Status DB::Open(const Options& options, std::unique_ptr<DB>* db) {
    if (options.dir.empty()) {
        return Status::InvalidArgument("empty db dir");
    }
    auto impl = std::make_unique<Impl>(options);
    auto status = impl->Open();
    if (!status.ok()) {
        return status;
    }
    db->reset(new DB(std::move(impl)));
    return Status::OK();
}

DB::DB(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

DB::~DB() = default;

Status DB::Put(const std::string& key, const std::string& value) {
    return impl_->Put(key, value);
}

Status DB::Delete(const std::string& key) {
    return impl_->Delete(key);
}

Status DB::Get(const std::string& key, std::string* value) const {
    return impl_->Get(key, value);
}

Status DB::Flush() {
    return impl_->Flush();
}

Status DB::Compact() {
    return impl_->Compact();
}

std::size_t DB::ApproximateMemoryUsage() const {
    return impl_->ApproximateMemoryUsage();
}

std::uint64_t DB::CurrentSequence() const {
    return impl_->CurrentSequence();
}

} // namespace minikv
