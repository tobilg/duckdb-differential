// Deterministic SQLite VFS write exhaustion. This is an injected SQLITE_FULL test,
// not a claim of physical disk exhaustion or power-loss testing.
#include "differential_store.hpp"
#include <sqlite3.h>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <unistd.h>
using differential::Store;
namespace {
std::atomic<bool> full{false};
struct File {
    sqlite3_file base;
    sqlite3_file *real;
};
File *F(sqlite3_file *f) { return reinterpret_cast<File *>(f); }
int Close(sqlite3_file *f) { return F(f)->real->pMethods->xClose(F(f)->real); }
int Read(sqlite3_file *f, void *p, int n, sqlite3_int64 o) {
    return F(f)->real->pMethods->xRead(F(f)->real, p, n, o);
}
int Write(sqlite3_file *f, const void *p, int n, sqlite3_int64 o) {
    return full ? SQLITE_FULL : F(f)->real->pMethods->xWrite(F(f)->real, p, n, o);
}
int Truncate(sqlite3_file *f, sqlite3_int64 n) { return F(f)->real->pMethods->xTruncate(F(f)->real, n); }
int Sync(sqlite3_file *f, int n) { return F(f)->real->pMethods->xSync(F(f)->real, n); }
int Size(sqlite3_file *f, sqlite3_int64 *n) { return F(f)->real->pMethods->xFileSize(F(f)->real, n); }
int Lock(sqlite3_file *f, int n) { return F(f)->real->pMethods->xLock(F(f)->real, n); }
int Unlock(sqlite3_file *f, int n) { return F(f)->real->pMethods->xUnlock(F(f)->real, n); }
int Reserved(sqlite3_file *f, int *n) { return F(f)->real->pMethods->xCheckReservedLock(F(f)->real, n); }
int Control(sqlite3_file *f, int n, void *p) { return F(f)->real->pMethods->xFileControl(F(f)->real, n, p); }
int Sector(sqlite3_file *f) { return F(f)->real->pMethods->xSectorSize(F(f)->real); }
int Device(sqlite3_file *f) { return F(f)->real->pMethods->xDeviceCharacteristics(F(f)->real); }
int Map(sqlite3_file *f, int p, int n, int e, void volatile **out) {
    return F(f)->real->pMethods->xShmMap(F(f)->real, p, n, e, out);
}
int ShmLock(sqlite3_file *f, int o, int n, int flags) {
    return F(f)->real->pMethods->xShmLock(F(f)->real, o, n, flags);
}
void Barrier(sqlite3_file *f) { F(f)->real->pMethods->xShmBarrier(F(f)->real); }
int Unmap(sqlite3_file *f, int n) { return F(f)->real->pMethods->xShmUnmap(F(f)->real, n); }
sqlite3_io_methods methods = {2,       Close,   Read,     Write,   Truncate, Sync,   Size,
                              Lock,    Unlock,  Reserved, Control, Sector,   Device, Map,
                              ShmLock, Barrier, Unmap,    nullptr, nullptr};
constexpr size_t offset =
    (sizeof(File) + alignof(std::max_align_t) - 1) / alignof(std::max_align_t) * alignof(std::max_align_t);
int Open(sqlite3_vfs *v, const char *path, sqlite3_file *f, int flags, int *out) {
    auto original = static_cast<sqlite3_vfs *>(v->pAppData);
    auto wrapper = F(f);
    wrapper->real = reinterpret_cast<sqlite3_file *>(reinterpret_cast<char *>(f) + offset);
    int rc = original->xOpen(original, path, wrapper->real, flags, out);
    if (rc == SQLITE_OK)
        wrapper->base.pMethods = &methods;
    return rc;
}
void Require(bool condition, const char *message) {
    if (!condition)
        throw std::runtime_error(message);
}
void Sql(const std::string &path, const char *sql) {
    sqlite3 *db = nullptr;
    Require(sqlite3_open(path.c_str(), &db) == SQLITE_OK, "open corruption fixture");
    auto rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    sqlite3_close(db);
    Require(rc == SQLITE_OK, "modify corruption fixture");
}
} // namespace
int main() {
    auto original = sqlite3_vfs_find(nullptr);
    auto wrapper = *original;
    wrapper.zName = "differential-test-full";
    wrapper.pAppData = original;
    wrapper.szOsFile = int(offset + original->szOsFile);
    wrapper.xOpen = Open;
    Require(sqlite3_vfs_register(&wrapper, 1) == SQLITE_OK, "register test VFS");
    char temporary[] = "/tmp/differential-faults-XXXXXX";
    Require(mkdtemp(temporary) != nullptr, "fixture directory");
    auto path = std::string(temporary);
    try {
        std::vector<unsigned char> initial(200000, 0x51), changed(400000, 0x83), actual(initial.size());
        std::string root;
        {
            Store store(path);
            auto file = store.Open("data", true, true, false);
            store.Write(file.id, initial.data(), initial.size(), 0);
            root = store.Seal(file.id).id;
            store.Write(file.id, changed.data(), changed.size(), 0);
            full = true;
            bool failed = false;
            try {
                store.Sync();
            } catch (const std::exception &e) {
                failed = std::string(e.what()).find("full") != std::string::npos;
            }
            full = false;
            Require(failed, "exhaustion did not report disk full");
            {
                // The failed publication left the durable state untouched.
                Store observer(path);
                auto committed = observer.Open("data", false, false, false);
                Require(committed.size == initial.size(), "failed sync changed committed length");
                observer.Read(committed.id, actual.data(), actual.size(), 0);
                Require(actual == initial, "failed sync changed acknowledged bytes");
            }
            // The overlay keeps the accepted write and the retry publishes it.
            store.Sync();
            store.Verify();
            std::vector<unsigned char> retried(changed.size());
            store.Read(file.id, retried.data(), retried.size(), 0);
            Require(retried == changed, "retry after exhaustion lost the accepted write");
        }
        {
            Store store(path);
            auto file = store.Open("data", false, false, false);
            std::vector<unsigned char> retried(changed.size());
            store.Read(file.id, retried.data(), retried.size(), 0);
            Require(retried == changed, "reopen after retried sync lost bytes");
            store.Verify();
            store.ReadRoot(root, actual.data(), actual.size(), 0);
            Require(actual == initial, "retry changed historical bytes");
        }
        Sql(path + "/engine.sqlite", "UPDATE objects SET data=zeroblob(65536)");
        Store::DropCaches();
        {
            Store store(path);
            bool failed = false;
            try {
                store.ReadRoot(root, actual.data(), actual.size(), 0);
            } catch (const std::exception &) {
                failed = true;
            }
            Require(failed, "corrupt payload silently accepted");
        }
        Sql(path + "/engine.sqlite", "DELETE FROM objects");
        Store::DropCaches();
        {
            Store store(path);
            bool failed = false;
            try {
                store.ReadRoot(root, actual.data(), actual.size(), 0);
            } catch (const std::exception &) {
                failed = true;
            }
            Require(failed, "missing referenced object treated as a sparse hole");
        }
        Sql(path + "/engine.sqlite", "DELETE FROM root_pages");
        {
            Store store(path);
            bool failed = false;
            try {
                store.RootSize(root);
            } catch (const std::exception &) {
                failed = true;
            }
            Require(failed, "corrupt manifest silently accepted");
        }
        std::filesystem::remove_all(path);
        sqlite3_vfs_register(original, 1);
        sqlite3_vfs_unregister(&wrapper);
        std::cout << "PASS injected disk full at the sync boundary: durable state unchanged, overlay retained, "
                     "retry/reopen; corrupt and missing objects, corrupt manifest\n";
    } catch (const std::exception &e) {
        full = false;
        std::cerr << e.what() << " fixture=" << path << '\n';
        return 1;
    }
}
