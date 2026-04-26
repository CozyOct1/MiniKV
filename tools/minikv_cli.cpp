#include "minikv/db.h"

#include <iostream>
#include <memory>
#include <sstream>
#include <string>

int main(int argc, char** argv) {
    minikv::Options options;
    if (argc > 1) {
        options.dir = argv[1];
    }

    std::unique_ptr<minikv::DB> db;
    auto status = minikv::DB::Open(options, &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << '\n';
        return 1;
    }

    std::cout << "MiniKV CLI. Commands: put <key> <value>, get <key>, del <key>, flush, compact, stats, exit\n";
    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        std::istringstream input(line);
        std::string cmd;
        input >> cmd;
        if (cmd == "exit" || cmd == "quit") {
            break;
        }
        if (cmd == "put") {
            std::string key;
            std::string value;
            input >> key;
            std::getline(input, value);
            if (!value.empty() && value[0] == ' ') {
                value.erase(0, 1);
            }
            status = db->Put(key, value);
            std::cout << status.ToString() << '\n';
        } else if (cmd == "get") {
            std::string key;
            input >> key;
            std::string value;
            status = db->Get(key, &value);
            if (status.ok()) {
                std::cout << value << '\n';
            } else {
                std::cout << status.ToString() << '\n';
            }
        } else if (cmd == "del" || cmd == "delete") {
            std::string key;
            input >> key;
            std::cout << db->Delete(key).ToString() << '\n';
        } else if (cmd == "flush") {
            std::cout << db->Flush().ToString() << '\n';
        } else if (cmd == "compact") {
            std::cout << db->Compact().ToString() << '\n';
        } else if (cmd == "stats") {
            std::cout << "seq=" << db->CurrentSequence()
                      << " memory=" << db->ApproximateMemoryUsage() << " bytes\n";
        } else if (!cmd.empty()) {
            std::cout << "unknown command\n";
        }
    }
    return 0;
}
