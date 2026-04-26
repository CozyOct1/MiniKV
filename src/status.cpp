#include "minikv/status.h"

namespace minikv {

Status::Status(Code code, std::string message) : code_(code), message_(std::move(message)) {}

Status Status::OK() {
    return {};
}

Status Status::NotFound(std::string message) {
    return {Code::kNotFound, std::move(message)};
}

Status Status::Corruption(std::string message) {
    return {Code::kCorruption, std::move(message)};
}

Status Status::IOError(std::string message) {
    return {Code::kIOError, std::move(message)};
}

Status Status::InvalidArgument(std::string message) {
    return {Code::kInvalidArgument, std::move(message)};
}

bool Status::ok() const {
    return code_ == Code::kOk;
}

bool Status::not_found() const {
    return code_ == Code::kNotFound;
}

Status::Code Status::code() const {
    return code_;
}

const std::string& Status::message() const {
    return message_;
}

std::string Status::ToString() const {
    switch (code_) {
    case Code::kOk:
        return "OK";
    case Code::kNotFound:
        return "NotFound: " + message_;
    case Code::kCorruption:
        return "Corruption: " + message_;
    case Code::kIOError:
        return "IOError: " + message_;
    case Code::kInvalidArgument:
        return "InvalidArgument: " + message_;
    }
    return "Unknown";
}

} // namespace minikv
