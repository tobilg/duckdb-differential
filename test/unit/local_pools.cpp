#include "differential_control.hpp"
#include "differential_local.hpp"
#include <filesystem>
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdlib>
using namespace differential;
void Require(bool value, const char *message) {
    if (!value)
        throw std::runtime_error(message);
}
int main() {
    char temp[] = "/tmp/differential-local-pools-XXXXXX";
    Require(mkdtemp(temp), "temporary fixture");
    auto base = std::string(temp);
    try {
        unsigned cases = 0;
        for (auto point : {"payload_written", "payload_synced", "payload_linked", "payload_durable",
                           "object_inserted", "mapping_updated", "before_commit", "after_commit"}) {
            auto repository = base + "/" + point;
            auto store = std::make_shared<Store>(repository);
            Control control(store);
            auto pool = control.EnsureLocalPool(base + "/payloads");
            auto file = store->Open("data", true, true, false, "", pool);
            std::vector<unsigned char> old(131077, 17), changed(old.size(), 29), actual(old.size());
            store->Write(file.id, old.data(), old.size(), 0);
            auto root = store->Seal(file.id);
            auto pin = control.NewPin(root.id);
            auto pid = fork();
            Require(pid >= 0, "fork");
            if (pid == 0) {
                Store child(repository);
                auto handle = child.Open("data", false, false, false, "", pool);
                std::string selected(point);
                setenv(selected.rfind("payload_", 0) == 0 ? "DIFFERENTIAL_LOCAL_CRASH"
                                                          : "DIFFERENTIAL_STORE_CRASH",
                       point, 1);
                child.Write(handle.id, changed.data(), changed.size(), 0);
                child.Sync();
                _exit(1);
            }
            int status;
            Require(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 86,
                    "crash point missed");
            store->Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == old || actual == changed, "mixed external generation");
            if (std::string(point) == "after_commit")
                Require(actual == changed, "acknowledged external write lost");
            // The child's write completed before its publication crashed: replaying its
            // journal (here via collection) recovers it exactly.
            control.Collect(false, 100, "");
            store->Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == changed, "completed external write lost after journal replay");
            store->ReadRoot(root.id, actual.data(), actual.size(), 0, pool);
            Require(actual == old, "external history changed");
            for (int i = 0; i < 4; ++i)
                control.Collect(false, 100, "");
            store->Verify();
            LocalPool physical(base + "/payloads", control.RepositoryID(), pool);
            for (auto &name : physical.List("", 100))
                Require(name.size() - name.rfind('/') - 1 == 64, "abandoned staging file not collected");
            ++cases;
        }
        auto repository = base + "/concurrency";
        auto store = std::make_shared<Store>(repository);
        Control control(store);
        auto pool = control.EnsureLocalPool(base + "/payloads");
        auto file = store->Open("data", true, true, false, "", pool);
        std::vector<unsigned char> a(150000, 31), b(a.size(), 47), actual(a.size());
        store->Write(file.id, a.data(), a.size(), 0);
        auto root = store->Seal(file.id);
        auto pin = control.NewPin(root.id);
        std::exception_ptr error;
        std::atomic<bool> done{false};
        std::thread worker([&] {
            try {
                auto other = std::make_shared<Store>(repository);
                Control maintenance(other);
                for (int i = 0; i < 12; ++i) {
                    auto &value = i % 2 ? a : b;
                    other->Write(file.id, value.data(), value.size(), 0);
                    maintenance.Collect(false, 100, "");
                }
            } catch (...) {
                error = std::current_exception();
            }
            done = true;
        });
        unsigned reads = 0;
        while (!done) {
            store->Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == a || actual == b, "read/GC mapping race");
            ++reads;
        }
        worker.join();
        if (error)
            std::rethrow_exception(error);
        Require(reads > 0, "concurrent read not exercised");
        store->Write(file.id, b.data(), b.size(), 0);
        store->Sync();
        // Unrooted overwritten b objects are released at publication and queued for deletion,
        // while a remains pinned.
        store->Write(file.id, a.data(), a.size(), 0);
        store->Sync();
        auto pid = fork();
        Require(pid >= 0, "GC fork");
        if (pid == 0) {
            auto child = std::make_shared<Store>(repository);
            Control c(child);
            setenv("DIFFERENTIAL_LOCAL_CRASH", "payload_deleted", 1);
            c.Collect(false, 100, "delete-crash");
            _exit(1);
        }
        int status;
        Require(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 86,
                "deletion crash missed");
        control.Collect(false, 100, "delete-crash");
        store->ReadRoot(root.id, actual.data(), actual.size(), 0, pool);
        Require(actual == a, "queued deletion harmed retained root");
        store->Verify();
        LocalPool physical(base + "/payloads", control.RepositoryID(), pool);
        // Fan-out layout: <pool>/<first two hex characters>/<sha256>.
        auto corrupt = std::filesystem::path(physical.directory) / "00" / std::string(64, '0');
        std::filesystem::create_directories(corrupt.parent_path());
        {
            std::ofstream output(corrupt, std::ios::binary);
            std::vector<char> zeros(65536);
            output.write(zeros.data(), zeros.size());
        }
        bool rejected = false;
        try {
            physical.Get(std::string(64, '0'));
        } catch (const std::exception &) {
            rejected = true;
        }
        Require(rejected, "corrupt external object accepted");
        std::filesystem::resize_file(corrupt, 3);
        rejected = false;
        try {
            physical.Get(std::string(64, '0'));
        } catch (const std::exception &) {
            rejected = true;
        }
        Require(rejected, "short external object accepted");
        std::filesystem::remove(corrupt);
        rejected = false;
        try {
            physical.Get(std::string(64, '0'));
        } catch (const std::exception &) {
            rejected = true;
        }
        Require(rejected, "missing external object accepted");
        pin.reset();
        store.reset();
        std::filesystem::remove_all(base);
        std::cout << "PASS " << cases << " external publication crashes, queued deletion crash/retry, "
                  << reads << " concurrent read/GC checks\n";
    } catch (const std::exception &error) {
        std::cerr << error.what() << " fixture=" << base << '\n';
        return 1;
    }
}
