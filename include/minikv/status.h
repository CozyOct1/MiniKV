#pragma once

#include <string>

namespace minikv {

class Status {
public:
    enum class Code {
        kOk,
        kNotFound,
        kCorruption,
        kIOError,
        kInvalidArgument
    };

    Status() = default;
    Status(Code code, std::string message);

    static Status OK();
    static Status NotFound(std::string message);
    static Status Corruption(std::string message);
    static Status IOError(std::string message);
    static Status InvalidArgument(std::string message);

    bool ok() const;
    bool not_found() const;
    Code code() const;
    const std::string& message() const;
    std::string ToString() const;

private:
    Code code_{Code::kOk};
    std::string message_;
};

} // namespace minikv
