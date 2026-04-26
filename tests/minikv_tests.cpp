#include "minikv/db.h"

#include <cassert>
#include <filesystem>
#include <memory>
#include <string>

namespace {

std::unique_ptr<minikv::DB> OpenDb(const std::string& dir) {
    minikv::Options options;
    options.dir = dir;
    options.memtable_bytes_limit = 128;
    options.background_compaction = false;
    std::unique_ptr<minikv::DB> db;
    auto status = minikv::DB::Open(options, &db);
    assert(status.ok());
    return db;
}

void BasicPutGetDelete() {
    const std::string dir = "test_data_basic";
    std::filesystem::remove_all(dir);
    {
        auto db = OpenDb(dir);
        assert(db->Put("a", "1").ok());
        std::string value;
        assert(db->Get("a", &value).ok());
        assert(value == "1");
        assert(db->Delete("a").ok());
        assert(db->Get("a", &value).not_found());
    }
    std::filesystem::remove_all(dir);
}

void FlushAndReopen() {
    const std::string dir = "test_data_reopen";
    std::filesystem::remove_all(dir);
    {
        auto db = OpenDb(dir);
        for (int i = 0; i < 100; ++i) {
            assert(db->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok());
        }
        assert(db->Flush().ok());
    }
    {
        auto db = OpenDb(dir);
        std::string value;
        assert(db->Get("k42", &value).ok());
        assert(value == "v42");
    }
    std::filesystem::remove_all(dir);
}

void WalRecovery() {
    const std::string dir = "test_data_wal";
    std::filesystem::remove_all(dir);
    {
        auto db = OpenDb(dir);
        assert(db->Put("durable", "yes").ok());
    }
    {
        auto db = OpenDb(dir);
        std::string value;
        assert(db->Get("durable", &value).ok());
        assert(value == "yes");
    }
    std::filesystem::remove_all(dir);
}

void CompactKeepsLatest() {
    const std::string dir = "test_data_compact";
    std::filesystem::remove_all(dir);
    {
        auto db = OpenDb(dir);
        assert(db->Put("k", "old").ok());
        assert(db->Flush().ok());
        assert(db->Put("k", "new").ok());
        assert(db->Flush().ok());
        assert(db->Compact().ok());
        std::string value;
        assert(db->Get("k", &value).ok());
        assert(value == "new");
    }
    std::filesystem::remove_all(dir);
}

} // namespace

int main() {
    BasicPutGetDelete();
    FlushAndReopen();
    WalRecovery();
    CompactKeepsLatest();
    return 0;
}
