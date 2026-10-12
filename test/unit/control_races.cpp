#include "differential_control.hpp"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>
using namespace differential;
void Require(bool v, const char *message) {
    if (!v)
        throw std::runtime_error(message);
}
int main() {
    char pattern[] = "/tmp/differential-races-XXXXXX";
    auto path = mkdtemp(pattern);
    if (!path)
        return 2;
    try {
        auto store = std::make_shared<Store>(path);
        Control writer(store);
        auto create = writer.CreateDatabase("race", "create");
        auto lease = writer.Acquire(writer.GetBranch("race", "main"));
        auto file = store->Open(lease->branch.FileName(), true, false, false);
        Control reader(std::make_shared<Store>(path)), collector(std::make_shared<Store>(path));
        unsigned pinned = 0, rejected = 0;
        for (unsigned round = 0; round < 36; round++) {
            auto publish = [&](unsigned char value) {
                std::vector<unsigned char> bytes(70000, value);
                store->Write(file.id, bytes.data(), bytes.size(), 0);
                auto b = writer.GetBranch("race", "main");
                auto capture = writer.NewPin();
                auto root = store->Seal(file.id, capture->id);
                auto operation = writer.BeginSnapshot(b, {}, "");
                return writer.PublishSnapshot(operation, b, root, Control::Now());
            };
            auto before = publish(static_cast<unsigned char>(round + 1));
            auto s = writer.GetSnapshot("race", before.snapshot);
            publish(250); // displace the current branch and generation retention roots
            std::atomic<bool> start{false}, retired{false}, opened{false};
            std::shared_ptr<Pin> pin;
            std::exception_ptr problem;
            std::thread attaching([&] {
                while (!start.load())
                    std::this_thread::yield();
                if (round % 3 == 0)
                    while (!retired.load())
                        std::this_thread::yield();
                try {
                    pin = reader.NewPin(s.root, s.database, s.id);
                } catch (const std::exception &e) {
                    auto message = std::string(e.what());
                    if (message.find("retired") == std::string::npos)
                        problem = std::current_exception();
                }
                opened = true;
            });
            start = true;
            if (round % 3 == 1)
                while (!opened.load())
                    std::this_thread::yield();
            collector.Retire("race", s.id, "");
            retired = true;
            collector.Collect(false, 1000, "");
            collector.Compact(3, "");
            attaching.join();
            if (problem)
                std::rethrow_exception(problem);
            if (pin) {
                pinned++;
                std::vector<unsigned char> bytes(70000);
                Require(reader.store->ReadRoot(s.root, bytes.data(), bytes.size(), 0) == bytes.size(),
                        "pinned length reclaimed");
                for (auto value : bytes)
                    Require(value == round + 1, "pin/retire/GC/compaction corrupted reader");
            } else
                rejected++;
            pin.reset();
            collector.Collect(false, 1000, "");
            Require(!store->HasRoot(s.root), "unpinned retired root not reclaimed");
            bool rejected_root = false;
            try {
                unsigned char byte;
                store->ReadRoot(s.root, &byte, 1, 0);
            } catch (const std::exception &) {
                rejected_root = true;
            }
            Require(rejected_root, "reclaimed root read as a sparse hole");
            rejected_root = false;
            try {
                store->Fork(s.root, "reclaimed-fork");
            } catch (const std::exception &) {
                rejected_root = true;
            }
            Require(rejected_root, "reclaimed root forked as a sparse file");
        }
        Require(pinned > 0 && rejected > 0, "both race outcomes must execute");
        for (unsigned round = 0; round < 12; ++round) {
            auto branch = writer.GetBranch("race", "main");
            auto operation = writer.BeginSnapshot(branch, {}, "");
            auto capture = writer.NewPin();
            std::vector<unsigned char> before(70000, static_cast<unsigned char>(100 + round));
            std::vector<unsigned char> later(70000, 240), actual(before.size());
            store->Write(file.id, before.data(), before.size(), 0);
            auto root = store->Seal(file.id, capture->id);
            // A later checkpoint displaces the generation; only the capture pin retains this root.
            store->Write(file.id, later.data(), later.size(), 0);
            store->Seal(file.id);
            std::exception_ptr problem;
            std::thread collecting([&] {
                try {
                    for (unsigned i = 0; i < 3; ++i)
                        collector.Collect(false, 1000, "");
                } catch (...) {
                    problem = std::current_exception();
                }
            });
            auto published = writer.PublishSnapshot(operation, branch, root, Control::Now());
            capture.reset();
            collecting.join();
            if (problem)
                std::rethrow_exception(problem);
            auto snapshot = reader.GetSnapshot("race", published.snapshot);
            auto protected_reader = reader.NewPin("", snapshot.database, snapshot.id);
            reader.store->ReadRoot(snapshot.root, actual.data(), actual.size(), 0);
            Require(actual == before, "publication/capture-pin/GC lost immutable boundary");
        }
        store->Verify();
        std::cout << "PASS 36 pin/retire/GC/compaction races across independent metadata connections; pinned="
                  << pinned << " retired=" << rejected << '\n';
        std::cout << "PASS 12 capture-pin/publication/GC races after a later internal generation\n";
    } catch (const std::exception &e) {
        std::cerr << e.what() << " fixture=" << path << '\n';
        return 1;
    }
    std::filesystem::remove_all(path);
    return 0;
}
