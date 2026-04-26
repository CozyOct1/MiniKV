#pragma once

#include <cstdint>
#include <ios>
#include <istream>
#include <ostream>
#include <string>

namespace minikv {

inline void WriteFixed32(std::ostream& out, std::uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out.put(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

inline void WriteFixed64(std::ostream& out, std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.put(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

inline bool ReadFixed32(std::istream& in, std::uint32_t* value) {
    std::uint32_t result = 0;
    for (int i = 0; i < 4; ++i) {
        const int c = in.get();
        if (c == EOF) {
            return false;
        }
        result |= static_cast<std::uint32_t>(static_cast<unsigned char>(c)) << (i * 8);
    }
    *value = result;
    return true;
}

inline bool ReadFixed64(std::istream& in, std::uint64_t* value) {
    std::uint64_t result = 0;
    for (int i = 0; i < 8; ++i) {
        const int c = in.get();
        if (c == EOF) {
            return false;
        }
        result |= static_cast<std::uint64_t>(static_cast<unsigned char>(c)) << (i * 8);
    }
    *value = result;
    return true;
}

inline void WriteString(std::ostream& out, const std::string& value) {
    WriteFixed32(out, static_cast<std::uint32_t>(value.size()));
    out.write(value.data(), static_cast<std::streamsize>(value.size()));
}

inline bool ReadString(std::istream& in, std::string* value) {
    std::uint32_t len = 0;
    if (!ReadFixed32(in, &len)) {
        return false;
    }
    value->resize(len);
    return static_cast<bool>(in.read(&(*value)[0], len));
}

} // namespace minikv
