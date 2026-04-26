#pragma once

#include <cstdint>
#include <string>

namespace minikv {

enum class ValueType : std::uint8_t {
    kPut = 1,
    kDelete = 2
};

struct Record {
    std::uint64_t sequence = 0;
    ValueType type = ValueType::kPut;
    std::string key;
    std::string value;
};

} // namespace minikv
