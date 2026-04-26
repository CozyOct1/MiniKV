#include "sstable.h"

#include "io_util.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

namespace minikv {
namespace {

constexpr std::uint32_t kTableMagic = 0x3154424dU;

Status ReadRecordAt(const std::string& path, std::uint64_t offset, Record* record) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return Status::IOError("open sstable failed: " + path);
    }
    in.seekg(static_cast<std::streamoff>(offset));
    std::uint64_t sequence = 0;
    std::uint32_t type = 0;
    if (!ReadFixed64(in, &sequence) || !ReadFixed32(in, &type)) {
        return Status::Corruption("truncated sstable record header");
    }
    record->sequence = sequence;
    record->type = static_cast<ValueType>(type);
    if (!ReadString(in, &record->key) || !ReadString(in, &record->value)) {
        return Status::Corruption("truncated sstable record body");
    }
    return Status::OK();
}

} // namespace

SSTable::SSTable(std::string path) : path_(std::move(path)) {}

Status SSTable::Build(const std::string& path,
                      const std::vector<Record>& records,
                      const TableBuildOptions& options) {
    if (records.empty()) {
        return Status::InvalidArgument("cannot build empty sstable");
    }
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        return Status::IOError("create sstable failed: " + path);
    }

    WriteFixed32(out, kTableMagic);
    BloomFilter bloom(records.size(), options.bloom_bits_per_key);
    std::vector<IndexEntry> index;
    index.reserve(records.size());
    std::uint64_t max_sequence = 0;

    for (const auto& record : records) {
        const auto offset = static_cast<std::uint64_t>(out.tellp());
        WriteFixed64(out, record.sequence);
        WriteFixed32(out, static_cast<std::uint32_t>(record.type));
        WriteString(out, record.key);
        WriteString(out, record.value);
        bloom.Add(record.key);
        max_sequence = std::max(max_sequence, record.sequence);
        index.push_back(IndexEntry{record.key, offset, record.sequence, record.type});
    }

    const auto index_offset = static_cast<std::uint64_t>(out.tellp());
    WriteFixed32(out, static_cast<std::uint32_t>(index.size()));
    for (const auto& item : index) {
        WriteString(out, item.key);
        WriteFixed64(out, item.offset);
        WriteFixed64(out, item.sequence);
        WriteFixed32(out, static_cast<std::uint32_t>(item.type));
    }

    const auto bloom_offset = static_cast<std::uint64_t>(out.tellp());
    WriteFixed32(out, bloom.bit_count());
    WriteFixed32(out, bloom.hash_count());
    WriteFixed32(out, static_cast<std::uint32_t>(bloom.bits().size()));
    out.write(reinterpret_cast<const char*>(bloom.bits().data()), static_cast<std::streamsize>(bloom.bits().size()));

    WriteFixed64(out, index_offset);
    WriteFixed64(out, static_cast<std::uint64_t>(index.size()));
    WriteFixed64(out, bloom_offset);
    WriteFixed64(out, static_cast<std::uint64_t>(bloom.bits().size() + 12));
    WriteFixed64(out, max_sequence);
    WriteFixed32(out, kTableMagic);
    if (!out) {
        return Status::IOError("write sstable failed: " + path);
    }
    return Status::OK();
}

Status SSTable::Open(const std::string& path, std::shared_ptr<SSTable>* table) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return Status::IOError("open sstable failed: " + path);
    }
    std::uint32_t magic = 0;
    if (!ReadFixed32(in, &magic) || magic != kTableMagic) {
        return Status::Corruption("bad sstable magic: " + path);
    }

    in.seekg(0, std::ios::end);
    const auto file_size = static_cast<std::uint64_t>(in.tellg());
    if (file_size < 64) {
        return Status::Corruption("sstable too small: " + path);
    }

    auto result = std::shared_ptr<SSTable>(new SSTable(path));

    if (file_size < 44) {
        return Status::Corruption("sstable missing footer: " + path);
    }
    in.seekg(static_cast<std::streamoff>(file_size - 44));
    std::uint64_t index_offset = 0;
    std::uint64_t index_count = 0;
    std::uint64_t bloom_offset = 0;
    std::uint64_t bloom_size = 0;
    if (!ReadFixed64(in, &index_offset) || !ReadFixed64(in, &index_count) ||
        !ReadFixed64(in, &bloom_offset) || !ReadFixed64(in, &bloom_size) ||
        !ReadFixed64(in, &result->max_sequence_) || !ReadFixed32(in, &magic) ||
        magic != kTableMagic) {
        return Status::Corruption("bad sstable footer: " + path);
    }

    in.seekg(static_cast<std::streamoff>(index_offset));
    std::uint32_t stored_index_count = 0;
    if (!ReadFixed32(in, &stored_index_count) || stored_index_count != index_count) {
        return Status::Corruption("bad sstable index count: " + path);
    }
    result->index_.reserve(static_cast<std::size_t>(index_count));
    for (std::uint64_t i = 0; i < index_count; ++i) {
        IndexEntry entry;
        std::uint32_t type = 0;
        if (!ReadString(in, &entry.key) || !ReadFixed64(in, &entry.offset) ||
            !ReadFixed64(in, &entry.sequence) || !ReadFixed32(in, &type)) {
            return Status::Corruption("truncated sstable index: " + path);
        }
        entry.type = static_cast<ValueType>(type);
        result->index_.push_back(std::move(entry));
    }

    in.seekg(static_cast<std::streamoff>(bloom_offset));
    std::uint32_t bit_count = 0;
    std::uint32_t hash_count = 0;
    std::uint32_t byte_count = 0;
    if (!ReadFixed32(in, &bit_count) || !ReadFixed32(in, &hash_count) || !ReadFixed32(in, &byte_count) ||
        bloom_size != static_cast<std::uint64_t>(byte_count + 12)) {
        return Status::Corruption("bad bloom metadata");
    }
    std::vector<std::uint8_t> bits(byte_count);
    if (!in.read(reinterpret_cast<char*>(bits.data()), byte_count)) {
        return Status::Corruption("truncated bloom filter");
    }
    result->bloom_.Load(std::move(bits), bit_count, hash_count);
    if (!result->index_.empty()) {
        result->smallest_key_ = result->index_.front().key;
        result->largest_key_ = result->index_.back().key;
    }
    *table = std::move(result);
    return Status::OK();
}

Status SSTable::Get(const std::string& key, Record* record) const {
    if (key < smallest_key_ || key > largest_key_) {
        return Status::NotFound("outside table key range");
    }
    if (!bloom_.MayContain(key)) {
        return Status::NotFound("filtered by bloom");
    }
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
                               [](const IndexEntry& entry, const std::string& target) {
                                   return entry.key < target;
                               });
    if (it == index_.end() || it->key != key) {
        return Status::NotFound("key not found in sstable");
    }
    return ReadRecordAt(path_, it->offset, record);
}

const std::string& SSTable::path() const {
    return path_;
}

const std::string& SSTable::smallest_key() const {
    return smallest_key_;
}

const std::string& SSTable::largest_key() const {
    return largest_key_;
}

std::uint64_t SSTable::max_sequence() const {
    return max_sequence_;
}

const std::vector<SSTable::IndexEntry>& SSTable::index() const {
    return index_;
}

Status SSTable::ReadAll(std::vector<Record>* records) const {
    records->clear();
    records->reserve(index_.size());
    for (const auto& item : index_) {
        Record record;
        auto status = ReadRecordAt(path_, item.offset, &record);
        if (!status.ok()) {
            return status;
        }
        records->push_back(std::move(record));
    }
    return Status::OK();
}

} // namespace minikv
