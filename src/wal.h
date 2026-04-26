#pragma once

#include "minikv/status.h"
#include "record.h"

#include <fstream>
#include <string>
#include <vector>

namespace minikv {

class WAL {
public:
    WAL(std::string path, bool sync_on_write);
    ~WAL();

    Status OpenForAppend();
    Status Append(const Record& record);
    Status Close();
    static Status Replay(const std::string& path, std::vector<Record>* records);

private:
    std::string path_;
    bool sync_on_write_;
    std::ofstream out_;
};

} // namespace minikv
