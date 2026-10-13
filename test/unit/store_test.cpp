#include "differential_store.hpp"
#include "differential_metrics.hpp"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>
#include <unistd.h>
#include <sqlite3.h>
#include <fstream>

using namespace differential;
static void Require(bool condition, const char *message) {
    if (!condition)
        throw std::runtime_error(message);
}
static int64_t Scalar(const std::string &directory, const char *sql) {
    sqlite3 *db = nullptr;
    Require(sqlite3_open((directory + "/engine.sqlite").c_str(), &db) == SQLITE_OK, "open metadata");
    sqlite3_stmt *query = nullptr;
    Require(sqlite3_prepare_v2(db, sql, -1, &query, nullptr) == SQLITE_OK, "prepare metadata query");
    Require(sqlite3_step(query) == SQLITE_ROW, "metadata query row");
    auto value = sqlite3_column_int64(query, 0);
    sqlite3_finalize(query);
    sqlite3_close(db);
    return value;
}
static std::vector<std::filesystem::path> Journals(const std::string &directory) {
    std::vector<std::filesystem::path> files;
    for (auto &entry : std::filesystem::directory_iterator(directory + "/journal"))
        files.push_back(entry.path());
    return files;
}
// Crash modes. "<op>:<failpoint>" kills inside the publishing transaction; "unsynced:<n>"
// kills after n completed but never synced writes; "lost:<op>:<failpoint>" additionally
// discards the unsynced journal before recovery (power loss of unsynced kernel state);
// "torn" cuts the journal inside its last record.
int main(int argc, char **argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash") {
        Store store(argv[2]);
        auto file = store.Open("crash", true, false, false);
        std::vector<unsigned char> initial(150000, 0x11);
        store.Write(file.id, initial.data(), initial.size(), 0);
        auto original = store.Seal(file.id);
        std::ofstream(std::string(argv[2]) + "/expected-root") << original.id;
        std::string point(argv[3]);
        if (point.rfind("lost:", 0) == 0)
            point = point.substr(5);
        if (point == "unsynced" || point == "torn") {
            std::vector<unsigned char> a(70000, 0x55), b(70000, 0x66);
            store.Write(file.id, a.data(), a.size(), 0);
            store.Write(file.id, b.data(), b.size(), 80000);
            _exit(86);
        }
        auto colon = point.find(':');
        auto operation = colon == std::string::npos ? "write" : point.substr(0, colon);
        auto failpoint = colon == std::string::npos ? point : point.substr(colon + 1);
        if (operation == "rename") {
            auto target = store.Open("target", true, true, false);
            unsigned char byte = 0x33;
            store.Write(target.id, &byte, 1, 0);
            store.Sync();
        }
        if (operation == "seal") {
            unsigned char byte = 0x44;
            store.Write(file.id, &byte, 1, 0);
            store.Sync();
        }
        setenv("DIFFERENTIAL_STORE_CRASH", failpoint.c_str(), 1);
        if (operation == "rename")
            store.Rename("crash", "target");
        else if (operation == "remove")
            store.Remove("crash");
        else if (operation == "seal")
            store.Seal(file.id);
        else if (operation == "truncate") {
            store.Truncate(file.id, 70001);
            store.Sync();
        } else {
            std::vector<unsigned char> changed(220000, 0x22);
            store.Write(file.id, changed.data(), changed.size(), 1000);
            store.Sync();
        }
        return 3;
    }
    if (argc == 4 && std::string(argv[1]) == "--recover") {
        std::string point(argv[3]);
        bool lost = point.rfind("lost:", 0) == 0;
        if (lost) {
            point = point.substr(5);
            for (auto &journal : Journals(argv[2]))
                std::filesystem::remove(journal);
        }
        if (point == "torn") {
            auto journals = Journals(argv[2]);
            Require(journals.size() == 1, "expected one dead journal");
            std::filesystem::resize_file(journals[0], std::filesystem::file_size(journals[0]) - 1000);
        }
        Store store(argv[2]);
        std::string root;
        std::ifstream(std::string(argv[2]) + "/expected-root") >> root;
        std::vector<unsigned char> saved(150000);
        Require(store.ReadRoot(root, saved.data(), saved.size(), 0) == saved.size(), "lost historical length");
        Require(std::all_of(saved.begin(), saved.end(), [](unsigned char c) { return c == 0x11; }),
                "lost historical data");
        Require(Journals(argv[2]).empty(), "dead journal not retired after replay");
        if (point == "unsynced" || point == "torn") {
            auto file = store.Open("crash", false, false, false);
            Require(file.size == (point == "torn" ? 150000u : 150000u), "unexpected replayed length");
            std::vector<unsigned char> data(file.size);
            store.Read(file.id, data.data(), data.size(), 0);
            for (size_t i = 0; i < data.size(); i++) {
                auto expected = i < 70000                                         ? 0x55
                                : point == "unsynced" && i >= 80000 && i < 150000 ? 0x66
                                                                                  : 0x11;
                Require(data[i] == expected, "journal replay is not the exact completed-write prefix");
            }
            store.Verify();
            return 0;
        }
        auto colon = point.find(':');
        auto operation = colon == std::string::npos ? "write" : point.substr(0, colon);
        auto failpoint = colon == std::string::npos ? point : point.substr(colon + 1);
        bool committed = failpoint == "after_commit";
        // A completed write/truncate survives process death through the journal even when the
        // publishing transaction did not commit, unless the unsynced journal itself was lost.
        bool data_applied = committed || ((operation == "write" || operation == "truncate") && !lost);
        if (operation == "remove") {
            Require(store.Exists("crash") != committed, "torn remove namespace");
        } else {
            std::string name = operation == "rename" && committed ? "target" : "crash";
            auto file = store.Open(name, false, false, false);
            auto size = operation == "write" && data_applied      ? 221000
                        : operation == "truncate" && data_applied ? 70001
                                                                  : 150000;
            Require(file.size == uint64_t(size), "torn logical length after crash");
            std::vector<unsigned char> data(file.size);
            store.Read(file.id, data.data(), data.size(), 0);
            for (size_t i = 0; i < data.size(); i++) {
                auto expected = operation == "write" && data_applied && i >= 1000 ? 0x22
                                : operation == "seal" && i == 0                   ? 0x44
                                                                                  : 0x11;
                Require(data[i] == expected, "mixed data/mapping generation");
            }
            if (operation == "rename") {
                Require(store.Exists("crash") != committed, "torn source namespace");
                if (!committed) {
                    auto target = store.Open("target", false, false, false);
                    unsigned char value = 0;
                    Require(store.Read(target.id, &value, 1, 0) == 1 && value == 0x33, "torn target replacement");
                }
            }
            if (operation == "seal") {
                sqlite3 *db = nullptr;
                Require(sqlite3_open((std::string(argv[2]) + "/engine.sqlite").c_str(), &db) == SQLITE_OK,
                        "open metadata");
                sqlite3_stmt *query = nullptr;
                Require(sqlite3_prepare_v2(db, "SELECT root FROM generations", -1, &query, nullptr) == SQLITE_OK,
                        "read generation");
                Require(sqlite3_step(query) == SQLITE_ROW, "generation missing");
                auto generation = std::string(reinterpret_cast<const char *>(sqlite3_column_text(query, 0)));
                Require((generation != root) == committed, "torn manifest/generation publication");
                sqlite3_finalize(query);
                sqlite3_close(db);
                store.RootSize(generation);
            }
        }
        store.Verify();
        return 0;
    }
    char path[] = "/tmp/differential-store-XXXXXX";
    if (!mkdtemp(path))
        return 2;
    try {
        std::vector<unsigned char> oracle;
        Root historical;
        std::vector<unsigned char> old;
        int64_t id;
        {
            Store store(path);
            id = store.Open("db", true, true, false).id;
            std::mt19937_64 random(0xD1FF);
            for (unsigned i = 0; i < 250; i++) {
                if (i % 7 == 0) {
                    auto length = random() % 240000;
                    store.Truncate(id, length);
                    oracle.resize(length, 0);
                } else {
                    auto offset = random() % 280000, size = random() % 90000;
                    std::vector<unsigned char> bytes(size);
                    for (auto &b : bytes)
                        b = random() & 255;
                    store.Write(id, bytes.data(), size, offset);
                    if (size) {
                        oracle.resize(std::max(oracle.size(), size_t(offset + size)), 0);
                        std::copy(bytes.begin(), bytes.end(), oracle.begin() + offset);
                    }
                }
                // Interleave overlay-only, NORMAL-published and synced states.
                if (i % 11 == 0)
                    store.Sync();
                else if (i % 5 == 0)
                    store.Flush();
                std::vector<unsigned char> actual(oracle.size() + 19, 0xAA);
                auto n = store.Read(id, actual.data(), actual.size(), 0);
                Require(n == oracle.size(), "short/EOF read mismatch");
                Require(store.Size(id) == oracle.size(), "logical size mismatch");
                Require(std::equal(oracle.begin(), oracle.end(), actual.begin()), "byte reconstruction mismatch");
                Require(actual.back() == 0xAA, "read wrote beyond EOF");
                if (i == 99) {
                    historical = store.Seal(id);
                    old = oracle;
                }
            }
            auto root = store.Seal(id);
            auto count = store.ObjectCount();
            Require(store.Seal(id).id == root.id, "unchanged root not reused");
            auto fork = store.Fork(root.id, "fork");
            Require(store.ObjectCount() == count, "fork duplicated payload");
            const unsigned char byte = 7;
            store.Write(fork.id, &byte, 1, 0);
            store.Rename("db", "renamed");
            Require(!store.Exists("db") && store.Exists("renamed"), "rename namespace mismatch");
            Require(store.Remove("fork"), "unlink failed");
            unsigned char read = 0;
            Require(store.Read(fork.id, &read, 1, 0) == 1 && read == 7, "unlink invalidated open inode");
            std::vector<unsigned char> saved(old.size());
            Require(store.ReadRoot(historical.id, saved.data(), saved.size(), 0) == saved.size() && saved == old,
                    "historical bytes changed");
            store.Verify();
        }
        {
            Store reopened(path);
            auto file = reopened.Open("renamed", false, false, false);
            Require(file.id == id, "inode identity changed on reopen");
            std::vector<unsigned char> actual(oracle.size());
            reopened.Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == oracle, "reopen bytes mismatch");
            std::vector<unsigned char> saved(old.size());
            reopened.ReadRoot(historical.id, saved.data(), saved.size(), 0);
            Require(saved == old, "historical reopen mismatch");
            reopened.Verify();
        }
        std::cout << "PASS 250 deterministic overlap/hole/EOF/truncate/regrow cases across overlay, NORMAL and "
                     "synced states, manifest reuse, zero-copy fork, unlink handles, immutable history and reopen\n";
        {
            Store store(path);
            auto file = store.Open("aligned", true, true, false);
            // Model a native header and two distinct, incompressible 256 KiB blocks.
            std::vector<unsigned char> bytes(12288 + 2 * 262144);
            std::mt19937_64 random(123);
            for (auto &byte : bytes)
                byte = random() & 255;
            store.Write(file.id, bytes.data(), bytes.size(), 0);
            auto original = store.Seal(file.id);
            auto refs = store.RootChunks(original.id, 0, 100);
            Require(refs.size() == 9, "header and eight data chunks expected");
            auto header = store.Object(refs[0].object);
            Require(std::equal(bytes.begin(), bytes.begin() + 12288, header->begin()) &&
                        std::all_of(header->begin() + 12288, header->end(), [](unsigned char b) { return b == 0; }),
                    "header object padding is not zero");
            // A full native block overwrite must not read either neighboring chunk.
            Store::DropCaches();
            auto before = counters[size_t(Counter::LocalMaterializedBytes)].load();
            std::fill(bytes.begin() + 12288, bytes.begin() + 12288 + 262144, 0xA7);
            store.Write(file.id, bytes.data() + 12288, 262144, 12288);
            Require(counters[size_t(Counter::LocalMaterializedBytes)].load() == before,
                    "aligned full block overwrite read old payload");
            auto changed = store.Seal(file.id);
            auto after = store.RootChunks(changed.id, 0, 100);
            unsigned replaced = 0;
            for (size_t i = 0; i < refs.size(); i++)
                replaced += refs[i].object != after[i].object;
            Require(replaced == 4 && refs[0].object == after[0].object,
                    "native block overwrite must replace exactly four chunks and preserve the header");
            // Truncation at either side of each kind of boundary, followed by sparse regrowth,
            // must not reveal previously stored bytes. Forks keep the original immutable image.
            for (uint64_t cut : {0ull, 12287ull, 12288ull, 12289ull, 77823ull, 77824ull, 77825ull}) {
                auto fork = store.Fork(changed.id, "cut-" + std::to_string(cut));
                store.Truncate(fork.id, cut);
                store.Sync();
                store.Truncate(fork.id, bytes.size());
                auto sealed = store.Seal(fork.id);
                std::vector<unsigned char> actual(bytes.size(), 0xFF);
                store.ReadRoot(sealed.id, actual.data(), actual.size(), 0);
                Require(std::equal(bytes.begin(), bytes.begin() + cut, actual.begin()), "truncate lost prefix");
                Require(std::all_of(actual.begin() + cut, actual.end(), [](unsigned char b) { return b == 0; }),
                        "truncate/regrow exposed old bytes at a chunk boundary");
            }
            std::vector<unsigned char> actual(bytes.size());
            store.ReadRoot(changed.id, actual.data(), actual.size(), 0);
            Require(actual == bytes, "truncation of forks changed their immutable source");
            store.Verify();
        }
        std::cout << "PASS isolated header padding, four-chunk native block overwrite without old reads, "
                     "header/data boundary truncation, regrowth and immutable forks\n";
        {
            // Paged manifests: a one-chunk change re-hashes one 16 MiB page, and the new root
            // shares every other page with its predecessor.
            Store store(path);
            auto file = store.Open("paged", true, true, false);
            std::vector<unsigned char> bytes(40 * 1024 * 1024);
            std::mt19937_64 random(7);
            for (auto &b : bytes)
                b = random() & 255;
            store.Write(file.id, bytes.data(), bytes.size(), 0);
            auto first = store.Seal(file.id);
            auto pages = Scalar(path, "SELECT count(*) FROM manifest_pages");
            unsigned char byte = 1;
            store.Write(file.id, &byte, 1, 20 * 1024 * 1024);
            auto second = store.Seal(file.id);
            Require(first.id != second.id, "changed file kept root identity");
            Require(Scalar(path, "SELECT count(*) FROM manifest_pages") == pages + 1, "seal rewrote unchanged pages");
            Require(Scalar(path, ("SELECT count(*) FROM root_pages a JOIN root_pages b ON a.page_no=b.page_no AND "
                                  "a.page=b.page WHERE a.root='" + first.id + "' AND b.root='" + second.id + "'")
                                     .c_str()) == 2,
                    "consecutive roots do not share unchanged pages");
            std::vector<unsigned char> check(bytes.size());
            store.ReadRoot(first.id, check.data(), check.size(), 0);
            Require(check == bytes, "first paged root changed");
            bytes[20 * 1024 * 1024] = 1;
            store.ReadRoot(second.id, check.data(), check.size(), 0);
            Require(check == bytes, "second paged root mismatch");
            // Overlay pressure: more than the dirty bound without a sync commits with NORMAL
            // durability, keeps reads exact and keeps the journal bounded.
            std::vector<unsigned char> large(96 * 1024 * 1024, 0x5a);
            store.Write(file.id, large.data(), large.size(), 0);
            uint64_t journal = 0;
            for (auto &entry : Journals(path))
                journal += std::filesystem::file_size(entry);
            Require(journal <= 64ull * 1024 * 1024 + 4096, "journal not reset by pressure publication");
            std::vector<unsigned char> sample(4096);
            store.Read(file.id, sample.data(), sample.size(), 90 * 1024 * 1024);
            Require(std::all_of(sample.begin(), sample.end(), [](unsigned char c) { return c == 0x5a; }),
                    "pressure-published overlay read mismatch");
            store.Verify();
            // Commits do not copy WAL frames into the database file; the background
            // checkpointer does, without blocking further commits.
            store.Sync();
            bool advanced = false;
            for (int i = 0; i < 100 && !advanced; i++) {
                usleep(50000);
                advanced = counters[size_t(Counter::WalCheckpoints)].load() > 0;
            }
            Require(advanced, "background WAL checkpoint did not run");
            // An open reader keeps checkpoints from resetting the WAL, so it grows past 64 MiB.
            // Once the reader is gone, the next checkpoint and commit truncate it to the limit.
            auto wal = std::string(path) + "/engine.sqlite-wal";
            sqlite3 *reader = nullptr;
            Require(sqlite3_open((std::string(path) + "/engine.sqlite").c_str(), &reader) == SQLITE_OK, "open WAL reader");
            Require(sqlite3_exec(reader, "BEGIN; SELECT count(*) FROM files;", nullptr, nullptr, nullptr) == SQLITE_OK,
                    "begin WAL reader");
            for (size_t offset = 0; offset < 80ull * 1024 * 1024; offset += bytes.size()) {
                for (auto &b : bytes)
                    b = random() & 255;
                store.Write(file.id, bytes.data(), bytes.size(), offset);
                store.Sync();
            }
            Require(std::filesystem::file_size(wal) > 64ull * 1024 * 1024, "WAL did not grow behind the reader");
            sqlite3_exec(reader, "COMMIT;", nullptr, nullptr, nullptr);
            sqlite3_close(reader);
            bool truncated = false;
            for (int i = 0; i < 100 && !truncated; i++) {
                store.Write(file.id, &byte, 1, 0);
                byte++;
                store.Sync();
                usleep(50000);
                truncated = std::filesystem::file_size(wal) <= 64ull * 1024 * 1024;
            }
            Require(truncated, "WAL not truncated to journal_size_limit");
        }
        std::cout << "PASS paged manifests share unchanged 16 MiB pages; overlay pressure publication bounds memory "
                     "and journal; background WAL checkpoint; WAL truncated to 64 MiB after a lagging checkpoint\n";
        std::filesystem::remove_all(path);
    } catch (const std::exception &e) {
        std::cerr << e.what() << " (fixture retained at " << path << ")\n";
        return 1;
    }
}
