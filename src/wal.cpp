#include "wal.h"

#include "io_util.h"

#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace minikv {
namespace {

constexpr std::uint32_t kWalMagic = 0x314c4157U;

std::uint32_t Checksum(const Record& record) {
    std::uint32_t h = 2166136261U;
    auto mix = [&h](unsigned char c) {
        h ^= c;
        h *= 16777619U;
    };
    for (int i = 0; i < 8; ++i) {
        mix(static_cast<unsigned char>((record.sequence >> (i * 8)) & 0xff));
    }
    mix(static_cast<unsigned char>(record.type));
    for (unsigned char c : record.key) {
        mix(c);
    }
    for (unsigned char c : record.value) {
        mix(c);
    }
    return h;
}

} // namespace

WAL::WAL(std::string path, bool sync_on_write) : path_(std::move(path)), sync_on_write_(sync_on_write) {}

WAL::~WAL() {
    Close();
}

Status WAL::OpenForAppend() {
    out_.open(path_, std::ios::binary | std::ios::app);
    if (!out_) {
        return Status::IOError("open wal failed: " + path_);
    }
    return Status::OK();
}

Status WAL::Append(const Record& record) {
    if (!out_) {
        return Status::IOError("wal is not open");
    }
    WriteFixed32(out_, kWalMagic);
    WriteFixed64(out_, record.sequence);
    out_.put(static_cast<char>(record.type));
    WriteString(out_, record.key);
    WriteString(out_, record.value);
    WriteFixed32(out_, Checksum(record));
    out_.flush();
    if (!out_) {
        return Status::IOError("write wal failed: " + path_);
    }
    if (sync_on_write_) {
        const int fd = ::open(path_.c_str(), O_RDONLY);
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
        }
    }
    return Status::OK();
}

Status WAL::Close() {
    if (out_.is_open()) {
        out_.close();
    }
    return Status::OK();
}

Status WAL::Replay(const std::string& path, std::vector<Record>* records) {
    if (!std::filesystem::exists(path)) {
        return Status::OK();
    }
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return Status::IOError("open wal for replay failed: " + path);
    }
    while (in.peek() != EOF) {
        std::uint32_t magic = 0;
        if (!ReadFixed32(in, &magic)) {
            break;
        }
        if (magic != kWalMagic) {
            return Status::Corruption("bad wal magic");
        }
        Record record;
        if (!ReadFixed64(in, &record.sequence)) {
            break;
        }
        const int type = in.get();
        if (type != static_cast<int>(ValueType::kPut) && type != static_cast<int>(ValueType::kDelete)) {
            return Status::Corruption("bad wal record type");
        }
        record.type = static_cast<ValueType>(type);
        if (!ReadString(in, &record.key) || !ReadString(in, &record.value)) {
            break;
        }
        std::uint32_t checksum = 0;
        if (!ReadFixed32(in, &checksum)) {
            break;
        }
        if (checksum != Checksum(record)) {
            return Status::Corruption("wal checksum mismatch");
        }
        records->push_back(std::move(record));
    }
    return Status::OK();
}

} // namespace minikv
