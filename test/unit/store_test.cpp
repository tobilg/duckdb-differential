#include "differential_store.hpp"
#include "differential_metrics.hpp"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>
#include <map>
#include <set>
#include <unistd.h>
#include <sqlite3.h>
#include <fstream>
#include <atomic>
#include <thread>

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
// Active segments (journal/): the write journal and the payload it already holds.
static std::vector<std::filesystem::path> Journals(const std::string &directory) {
    std::vector<std::filesystem::path> files;
    for (auto &entry : std::filesystem::directory_iterator(directory + "/journal"))
        files.push_back(entry.path());
    return files;
}
static uint64_t FolderBytes(const std::string &folder) {
    uint64_t total = 0;
    for (auto &entry : std::filesystem::directory_iterator(folder))
        total += std::filesystem::file_size(entry);
    return total;
}
// Crash modes. "<op>:<failpoint>" kills inside the publishing transaction; "unsynced:<n>"
// kills after n completed but never synced writes; "lost:<op>:<failpoint>" additionally
// discards every unsynced segment byte before recovery (power loss of unsynced kernel state:
// each segment is cut back to its published, synced watermark); "torn" cuts the segment inside
// its last record.
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
        int64_t other = -1;
        if (operation == "group") {
            // A second database in the same repository, committed in one batch with the first.
            other = store.Open("crash-b", true, false, false).id;
            store.Write(other, initial.data(), initial.size(), 0);
            store.Sync(other);
            std::vector<unsigned char> changed(220000, 0x22);
            store.Write(file.id, changed.data(), changed.size(), 1000);
            store.Write(other, changed.data(), changed.size(), 1000);
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
        } else if (operation == "group") {
            setenv("DIFFERENTIAL_TEST_GROUP_COMMIT_WAIT", "2", 1);
            std::thread first([&] { store.Sync(file.id); }), second([&] { store.Sync(other); });
            first.join();
            second.join();
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
        auto dead = Journals(argv[2]);
        if (lost) {
            point = point.substr(5);
            for (auto &journal : dead) {
                // Bytes up to the watermark were synced before the transaction that recorded it.
                auto name = journal.filename().string();
                auto committed = Scalar(argv[2], ("SELECT coalesce((SELECT committed FROM segments WHERE id=" +
                                                  std::to_string(std::stoll(name, nullptr, 16)) + "),0)")
                                                     .c_str());
                std::filesystem::resize_file(journal, uint64_t(committed));
            }
        }
        if (point == "torn") {
            Require(dead.size() == 1, "expected one dead segment");
            std::filesystem::resize_file(dead[0], std::filesystem::file_size(dead[0]) - 1000);
        }
        Store store(argv[2]);
        std::string root;
        std::ifstream(std::string(argv[2]) + "/expected-root") >> root;
        std::vector<unsigned char> saved(150000);
        Require(store.ReadRoot(root, saved.data(), saved.size(), 0) == saved.size(), "lost historical length");
        Require(std::all_of(saved.begin(), saved.end(), [](unsigned char c) { return c == 0x11; }),
                "lost historical data");
        for (auto &journal : dead)
            Require(!std::filesystem::exists(journal), "dead segment not retired after replay");
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
        bool data_applied =
            committed || ((operation == "write" || operation == "truncate" || operation == "group") && !lost);
        if (operation == "group") {
            // Both databases of the batch, or neither: never one of them.
            for (auto name : {"crash", "crash-b"}) {
                auto file = store.Open(name, false, false, false);
                Require(file.size == (data_applied ? 221000u : 150000u), "group commit published one database");
                std::vector<unsigned char> data(file.size);
                store.Read(file.id, data.data(), data.size(), 0);
                for (size_t i = 0; i < data.size(); i++)
                    Require(data[i] == (data_applied && i >= 1000 ? 0x22 : 0x11), "mixed group commit state");
            }
            store.Verify();
            return 0;
        }
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
    {
        // Latency distributions: percentiles within the histogram's ~12% resolution, exact standard
        // deviation from the sum of squares.
        Measurements m;
        auto record = [&](uint64_t ns) {
            ++m.calls;
            m.nanoseconds += ns;
            ++m.histogram[Measurements::Bucket(ns)];
            m.squares = m.squares.load() + double(ns) / 1000 * double(ns) / 1000;
            if (ns > m.max_nanoseconds)
                m.max_nanoseconds = ns;
        };
        for (uint64_t i = 1; i <= 1000; i++)
            record(i * 1000); // 1 µs .. 1 ms, uniform
        auto near = [](uint64_t actual, double expected) { return actual > expected * 0.85 && actual < expected * 1.15; };
        Require(near(m.Percentile(0.5), 500000) && near(m.Percentile(0.95), 950000) && near(m.Percentile(0.99), 990000),
                "latency percentiles off");
        Require(near(m.StandardDeviation(), 288675), "latency standard deviation off");
        for (uint64_t ns : {0ull, 1ull, 3ull, 4ull, 7ull, 1000ull, 123456789ull, ~0ull >> 1})
            Require(Measurements::Bucket(ns) < Measurements::BUCKETS &&
                        (ns < 4 || (Measurements::Value(Measurements::Bucket(ns)) > ns * 0.8 &&
                                    Measurements::Value(Measurements::Bucket(ns)) < ns * 1.25)),
                    "latency bucket out of range");
        std::cout << "PASS latency histogram percentiles and standard deviation\n";
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
            // Model a native header and three distinct, incompressible 256 KiB blocks.
            std::vector<unsigned char> bytes(12288 + 3 * 262144);
            std::mt19937_64 random(123);
            for (auto &byte : bytes)
                byte = random() & 255;
            store.Write(file.id, bytes.data(), bytes.size(), 0);
            auto original = store.Seal(file.id);
            auto refs = store.RootChunks(original.id, 0, 100);
            Require(refs.size() == 4, "header and three block chunks expected");
            auto header = store.Object(refs[0].object);
            Require(std::equal(bytes.begin(), bytes.begin() + 12288, header->begin()) &&
                        std::all_of(header->begin() + 12288, header->end(), [](unsigned char b) { return b == 0; }),
                    "header object padding is not zero");
            // A full native block overwrite must not read either neighboring chunk, and its segment
            // record is the chunk's object: publishing it appends nothing more.
            Store::DropCaches();
            auto before = counters[size_t(Counter::LocalMaterializedBytes)].load();
            auto segment_bytes = FolderBytes(std::string(path) + "/journal");
            std::fill(bytes.begin() + 12288, bytes.begin() + 12288 + 262144, 0xA7);
            store.Write(file.id, bytes.data() + 12288, 262144, 12288);
            Require(counters[size_t(Counter::LocalMaterializedBytes)].load() == before,
                    "aligned full block overwrite read old payload");
            auto changed = store.Seal(file.id);
            Require(FolderBytes(std::string(path) + "/journal") == segment_bytes + 262144 + 48,
                    "aligned block was stored twice");
            auto after = store.RootChunks(changed.id, 0, 100);
            unsigned replaced = 0;
            for (size_t i = 0; i < refs.size(); i++)
                replaced += refs[i].object != after[i].object;
            Require(replaced == 1 && refs[0].object == after[0].object && refs[2].object == after[2].object,
                    "native block overwrite must replace exactly its chunk and preserve the header");
            // Whole-chunk reads are verified straight into the caller's buffer, partial ones through
            // the verified chunk cache; both see the same bytes.
            Store::DropCaches();
            std::vector<unsigned char> block(262144), part(1000);
            store.Read(file.id, block.data(), block.size(), 12288 + 262144);
            store.Read(file.id, part.data(), part.size(), 12288 + 262144 + 5000);
            Require(std::equal(block.begin(), block.end(), bytes.begin() + 12288 + 262144) &&
                        std::equal(part.begin(), part.end(), bytes.begin() + 12288 + 262144 + 5000),
                    "zero-copy and cached reads disagree");
            // Truncation at either side of each kind of boundary, followed by sparse regrowth,
            // must not reveal previously stored bytes. Forks keep the original immutable image.
            for (uint64_t cut : {0ull, 12287ull, 12288ull, 12289ull, 274431ull, 274432ull, 274433ull}) {
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
        std::cout << "PASS isolated header padding, one-chunk native block overwrite stored once without old reads, "
                     "zero-copy whole-chunk reads, header/data boundary truncation, regrowth and immutable forks\n";
        {
            // Publication groups: a sync publishes its file and the file's sidecars, never another
            // database's buffered writes.
            Store store(path);
            auto a = store.Open("group-a", true, true, false);
            auto wal = store.Open("group-a.wal", true, true, false);
            auto b = store.Open("group-b", true, true, false);
            std::vector<unsigned char> bytes(100000, 0x42);
            store.Write(a.id, bytes.data(), bytes.size(), 0);
            store.Write(wal.id, bytes.data(), 5000, 0);
            store.Write(b.id, bytes.data(), bytes.size(), 0);
            store.Sync(wal.id);
            auto size = [&](const char *name) {
                return Scalar(path, (std::string("SELECT size FROM files WHERE name='") + name + "'").c_str());
            };
            Require(size("group-a") == 100000 && size("group-a.wal") == 5000, "sync did not publish its group");
            Require(size("group-b") == 0, "sync published another group's buffered writes");
            Require(store.Size(b.id) == 100000, "unpublished group lost its overlay");
            store.Flush(b.id);
            Require(size("group-b") == 100000, "group flush did not publish");
            store.Verify();
        }
        std::cout << "PASS publication groups: a sync commits its file and sidecars only\n";
        {
            // Log files (sidecars) map byte ranges onto the records that wrote them: appends store only
            // the appended bytes; overwrites, truncation and regrowth stay exact across overlay,
            // NORMAL, synced and reopened states.
            std::vector<unsigned char> oracle;
            int64_t id;
            {
                Store store(path);
                id = store.Open("logged.wal", true, true, false).id;
                Require(Scalar(path, "SELECT kind FROM files WHERE name='logged.wal'") == 1, "sidecar is not a log file");
                auto before = FolderBytes(std::string(path) + "/journal");
                std::vector<unsigned char> entry(3000, 0x31);
                for (int i = 0; i < 20; i++) {
                    store.Write(id, entry.data(), entry.size(), oracle.size());
                    oracle.insert(oracle.end(), entry.begin(), entry.end());
                    store.Sync(id);
                }
                Require(FolderBytes(std::string(path) + "/journal") - before == 20 * (3000 + 48),
                        "log appends stored more than the appended bytes");
                Require(Scalar(path, "SELECT count(*) FROM ranges r JOIN files f ON f.id=r.file WHERE f.name='logged.wal'") ==
                            20,
                        "one range per append");
                std::mt19937_64 random(0x10C);
                for (unsigned i = 0; i < 300; i++) {
                    if (i % 9 == 0) {
                        auto length = random() % 90000;
                        store.Truncate(id, length);
                        oracle.resize(length, 0);
                    } else {
                        auto offset = random() % 100000, size = random() % 9000 + 1;
                        std::vector<unsigned char> bytes(size);
                        for (auto &b : bytes)
                            b = random() & 255;
                        store.Write(id, bytes.data(), size, offset);
                        oracle.resize(std::max(oracle.size(), size_t(offset + size)), 0);
                        std::copy(bytes.begin(), bytes.end(), oracle.begin() + offset);
                    }
                    if (i % 7 == 0)
                        store.Sync(id);
                    else if (i % 5 == 0)
                        store.Flush(id);
                    if (i % 3 == 0)
                        Store::DropCaches();
                    std::vector<unsigned char> actual(oracle.size() + 7, 0xEE);
                    Require(store.Read(id, actual.data(), actual.size(), 0) == oracle.size() &&
                                std::equal(oracle.begin(), oracle.end(), actual.begin()),
                            "log file reconstruction mismatch");
                    // Small sequential reads, as native WAL replay issues them.
                    if (i % 50 == 0 && oracle.size() > 4096) {
                        std::vector<unsigned char> step(512);
                        for (uint64_t at = 0; at + step.size() <= oracle.size(); at += 4096) {
                            store.Read(id, step.data(), step.size(), at);
                            Require(std::equal(step.begin(), step.end(), oracle.begin() + at), "small log read mismatch");
                        }
                    }
                }
                Require(Scalar(path, "SELECT count(*) FROM ranges a JOIN ranges b ON a.file=b.file AND a.start<b.start "
                                     "AND a.start+a.length>b.start") == 0,
                        "overlapping log ranges");
                store.Verify();
            }
            Store reopened(path);
            auto file = reopened.Open("logged.wal", false, false, false);
            Require(file.id == id && file.size == oracle.size(), "log file length after reopen");
            std::vector<unsigned char> actual(oracle.size());
            reopened.Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == oracle, "log file bytes after reopen");
            Require(reopened.Remove("logged.wal"), "unlink log file");
        }
        {
            // Shipping deltas: applying each delta to a replica reproduces the group exactly, through
            // writes, overwrites, truncation, regrowth, a WAL replaced by rename and removal; a chain
            // base (Capture) covers everything up to its sequence.
            struct Replica : Store::ShipSink {
                std::map<std::string, std::vector<unsigned char>> files;
                std::map<std::string, int64_t> known;
                std::map<std::string, std::vector<unsigned char>> pending;
                std::map<std::string, int64_t> pending_known;
                uint64_t chunks = 0, tail_bytes = 0;
                void Reset() override {
                    pending = files;
                    pending_known = known;
                    chunks = tail_bytes = 0;
                }
                void Group(const std::vector<Store::ShipFile> &group) override {
                    std::set<std::string> names;
                    for (auto &file : group)
                        names.insert(file.name);
                    for (auto it = pending.begin(); it != pending.end();)
                        it = names.count(it->first) ? std::next(it) : pending.erase(it);
                    for (auto it = pending_known.begin(); it != pending_known.end();)
                        it = names.count(it->first) ? std::next(it) : pending_known.erase(it);
                }
                void File(const Store::ShipFile &file, bool full) override {
                    auto &bytes = pending[file.name];
                    if (full)
                        bytes.assign(file.size, 0);
                    else
                        bytes.resize(file.size, 0);
                    pending_known[file.name] = file.id;
                }
                void Chunk(const Store::ShipFile &file, uint64_t chunk, const std::string &, const SharedBytes &data) override {
                    ++chunks;
                    auto &bytes = pending[file.name];
                    auto offset = Store::ChunkOffset(chunk);
                    auto length = std::min(Store::ChunkLength(chunk), file.size - offset);
                    if (data)
                        std::copy(data->begin(), data->begin() + int64_t(length), bytes.begin() + int64_t(offset));
                    else
                        std::fill(bytes.begin() + int64_t(offset), bytes.begin() + int64_t(offset + length), 0);
                }
                void Tail(const Store::ShipFile &file, uint64_t offset, const std::vector<unsigned char> &data) override {
                    tail_bytes += data.size();
                    auto &bytes = pending[file.name];
                    bytes.resize(offset);
                    bytes.insert(bytes.end(), data.begin(), data.end());
                    Require(bytes.size() == file.size, "log tail does not end at the file size");
                }
                void Commit() {
                    files = pending;
                    known = pending_known;
                }
            };
            Store store(path);
            store.SetShipping("shipped", true);
            Require(store.Shipping("shipped") && !store.Shipping("other"), "shipping flag");
            auto main = store.Open("shipped", true, true, false).id;
            auto wal = store.Open("shipped.wal", true, true, false).id;
            Replica replica;
            auto ship = [&] {
                auto sequence = store.ReadShipDelta("shipped", replica.known, replica);
                replica.Commit();
                store.AckShipped("shipped", sequence);
                for (auto &name : {"shipped", "shipped.wal", "shipped.wal.recovery"}) {
                    if (!store.Exists(name)) {
                        Require(!replica.files.count(name), "removed file still replicated");
                        continue;
                    }
                    auto id = store.FileId(name);
                    std::vector<unsigned char> actual(store.Size(id));
                    store.Read(id, actual.data(), actual.size(), 0);
                    Require(replica.files.count(name) && replica.files[name] == actual, "shipped replica differs");
                }
            };
            std::mt19937_64 random(0x5419);
            std::vector<unsigned char> block(Store::CHUNK);
            for (unsigned i = 0; i < 120; i++) {
                auto op = random() % 10;
                if (op < 4) {
                    for (auto &b : block)
                        b = random() & 255;
                    store.Write(main, block.data(), block.size(), Store::ChunkOffset(1 + random() % 12));
                } else if (op < 7) {
                    std::vector<unsigned char> entry(random() % 5000 + 1, (unsigned char)(i + 1));
                    store.Write(wal, entry.data(), entry.size(), store.Size(wal));
                } else if (op == 7)
                    store.Truncate(main, Store::ChunkOffset(random() % 12) + random() % 1000);
                else if (op == 8)
                    store.Truncate(wal, store.Size(wal) / 2);
                else {
                    // Native recovery: a new WAL replaces the old one by rename.
                    auto recovery = store.Open("shipped.wal.recovery", true, false, true).id;
                    std::vector<unsigned char> entry(3000, (unsigned char)(200 + i % 50));
                    store.Write(recovery, entry.data(), entry.size(), 0);
                    store.Sync(recovery);
                    store.Rename("shipped.wal.recovery", "shipped.wal");
                    wal = store.FileId("shipped.wal");
                }
                store.Sync(main);
                if (i % 3 == 0)
                    ship();
            }
            ship();
            // An unchanged group ships nothing.
            Replica idle;
            idle.known = replica.known;
            idle.files = replica.files;
            store.ReadShipDelta("shipped", idle.known, idle);
            Require(idle.chunks == 0 && idle.tail_bytes == 0, "unchanged group shipped data");
            // An append ships only its bytes.
            std::vector<unsigned char> entry(777, 9);
            store.Write(wal, entry.data(), entry.size(), store.Size(wal));
            store.Sync(wal);
            Replica append;
            append.known = replica.known;
            append.files = replica.files;
            store.ReadShipDelta("shipped", append.known, append);
            Require(append.chunks == 0 && append.tail_bytes == 777, "an append shipped more than its bytes");
            // A chain base includes everything up to its sequence: acknowledging it leaves later changes.
            auto pin_root = store.Capture(main, "", replica.chunks);
            Require(!pin_root.id.empty() && store.Generation(main) != pin_root.id, "capture became the generation");
            Require(store.ShipSequence() >= replica.chunks, "capture sequence beyond the store's");
            // Untracked groups record nothing.
            auto other = store.Open("other", true, true, false).id;
            store.Write(other, block.data(), block.size(), Store::ChunkOffset(1));
            store.Sync(other);
            Require(Scalar(path, ("SELECT count(*) FROM ship_lows WHERE file=" + std::to_string(other)).c_str()) == 0,
                    "an untracked group recorded changes");
            store.Verify();
        }
        std::cout << "PASS shipping deltas: 120 random writes, overwrites, truncations, WAL renames reproduced by "
                     "replicas; idle ships nothing; appends ship only their bytes; chain base capture\n";
        std::cout << "PASS log files: appends store only their bytes; 300 overlapping writes, truncations and "
                     "regrowth exact across overlay, NORMAL, synced and reopened states; small sequential reads\n";
        {
            // Segment collection: retired segments nothing references are deleted; sparse ones are
            // rewritten first. Reads, historical roots and verification are unaffected.
            Store store(path);
            auto file = store.Open("churn", true, true, false);
            std::vector<unsigned char> block(Store::CHUNK);
            std::mt19937_64 random(0xC0117);
            auto fill = [&] {
                for (auto &b : block)
                    b = random() & 255;
            };
            // Four blocks written once, then ~3 segments of overwrites of the same 8 blocks: the first
            // segment keeps only the four (sparse: compacted), the others nothing (deleted).
            for (uint64_t k = 0; k < 4; k++) {
                fill();
                store.Write(file.id, block.data(), block.size(), Store::ChunkOffset(20 + k));
            }
            for (int round = 0; round < 100; round++) {
                for (uint64_t k = 0; k < 8; k++) {
                    fill();
                    store.Write(file.id, block.data(), block.size(), Store::ChunkOffset(1 + k));
                }
                store.Sync(file.id);
            }
            auto root = store.Seal(file.id);
            std::vector<unsigned char> expected(root.size);
            store.Read(file.id, expected.data(), expected.size(), 0);
            store.Sync(); // retire nothing more; flush the remaining overlay
            auto deleted = counters[size_t(Counter::LocalSegmentsDeleted)].load();
            auto compacted = counters[size_t(Counter::LocalSegmentBytesCompacted)].load();
            store.CollectSegments(64ull * 1024 * 1024);
            Require(counters[size_t(Counter::LocalSegmentsDeleted)].load() > deleted, "no segment collected");
            Require(counters[size_t(Counter::LocalSegmentBytesCompacted)].load() >= compacted + 4 * Store::CHUNK,
                    "sparse segment not compacted");
            auto retired = FolderBytes(std::string(path) + "/segments");
            Require(retired < 64ull * 1024 * 1024, "garbage segments not reclaimed");
            Store::DropCaches();
            std::vector<unsigned char> actual(root.size);
            store.Read(file.id, actual.data(), actual.size(), 0);
            Require(actual == expected, "collection changed live bytes");
            store.ReadRoot(root.id, actual.data(), actual.size(), 0);
            Require(actual == expected, "collection changed a root");
            store.Verify();
        }
        std::cout << "PASS segment collection: unreferenced retired segments deleted, sparse ones compacted, live "
                     "bytes and roots intact\n";
        {
            // Group commit: concurrent syncs of different databases share FULL transactions.
            constexpr int THREADS = 16, ROUNDS = 100;
            auto counter = [](Counter c) { return counters[size_t(c)].load(); };
            auto run = [&](Store &store, int threads, int rounds, const std::string &prefix) {
                std::vector<int64_t> ids;
                for (int t = 0; t < threads; t++)
                    ids.push_back(store.Open(prefix + std::to_string(t), true, true, false).id);
                std::atomic<int> failures {0};
                std::vector<std::thread> workers;
                for (int t = 0; t < threads; t++)
                    workers.emplace_back([&, t] {
                        try {
                            for (int k = 0; k < rounds; k++) {
                                std::vector<unsigned char> bytes(1000, (unsigned char)((t * 31 + k) & 255));
                                store.Write(ids[t], bytes.data(), bytes.size(), uint64_t(k) * 1000);
                                store.Sync(ids[t]);
                            }
                        } catch (...) {
                            ++failures;
                        }
                    });
                for (auto &worker : workers)
                    worker.join();
                Require(failures == 0, "a concurrent sync failed");
                return ids;
            };
            auto verify = [&](Store &store, int threads, int rounds, const std::string &prefix) {
                for (int t = 0; t < threads; t++) {
                    auto file = store.Open(prefix + std::to_string(t), false, false, false);
                    Require(file.size == uint64_t(rounds) * 1000, "group commit lost a write");
                    std::vector<unsigned char> data(file.size);
                    store.Read(file.id, data.data(), data.size(), 0);
                    for (int k = 0; k < rounds; k++)
                        Require(data[size_t(k) * 1000] == ((t * 31 + k) & 255), "group commit bytes mismatch");
                }
            };
            {
                Store store(path);
                auto requests = counter(Counter::LocalSyncRequests);
                auto transactions = counter(Counter::LocalSyncTransactions);
                run(store, THREADS, ROUNDS, "gc-");
                auto made = counter(Counter::LocalSyncRequests) - requests;
                auto committed = counter(Counter::LocalSyncTransactions) - transactions;
                Require(made == uint64_t(THREADS) * ROUNDS, "sync requests not counted");
                Require(committed < made && counter(Counter::LocalSyncBatchMax) >= 2, "concurrent syncs were not batched");
                // Off: every sync commits on its own.
                Store::SetGroupCommit(false);
                requests = counter(Counter::LocalSyncRequests);
                transactions = counter(Counter::LocalSyncTransactions);
                run(store, 4, 20, "solo-");
                Require(counter(Counter::LocalSyncTransactions) - transactions ==
                            counter(Counter::LocalSyncRequests) - requests,
                        "group commit off still batched");
                Store::SetGroupCommit(true);
                // A failing sync in a batch fails alone: the batch is retried one sync at a time.
                auto failing = store.Open("fail-a", true, true, false).id;
                auto healthy = store.Open("fail-b", true, true, false).id;
                std::vector<unsigned char> bytes(5000, 0x5a);
                store.Write(failing, bytes.data(), bytes.size(), 0);
                store.Write(healthy, bytes.data(), bytes.size(), 0);
                auto fallbacks = counter(Counter::LocalSyncBatchFallbacks);
                setenv("DIFFERENTIAL_TEST_GROUP_COMMIT_WAIT", "2", 1);
                setenv("DIFFERENTIAL_TEST_FAIL_APPLY_FILE", std::to_string(failing).c_str(), 1);
                std::atomic<int> failed {0}, succeeded {0};
                auto sync = [&](int64_t id) {
                    try {
                        store.Sync(id);
                        ++succeeded;
                    } catch (const std::exception &) {
                        ++failed;
                    }
                };
                std::thread a(sync, failing), b(sync, healthy);
                a.join();
                b.join();
                unsetenv("DIFFERENTIAL_TEST_GROUP_COMMIT_WAIT");
                unsetenv("DIFFERENTIAL_TEST_FAIL_APPLY_FILE");
                Require(failed == 1 && succeeded == 1, "a failing sync failed its batch");
                Require(counter(Counter::LocalSyncBatchFallbacks) == fallbacks + 1, "batch fallback not counted");
                Require(store.Size(failing) == 5000, "a failed sync dropped its buffered writes");
                store.Sync(failing); // the retry publishes them
            }
            Store reopened(path);
            verify(reopened, THREADS, ROUNDS, "gc-");
            verify(reopened, 4, 20, "solo-");
            Require(reopened.Size(reopened.FileId("fail-a")) == 5000 && reopened.Size(reopened.FileId("fail-b")) == 5000,
                    "fallback syncs not durable");
            reopened.Verify();
        }
        std::cout << "PASS group commit: 16 databases x 100 concurrent syncs share transactions; off switch; a "
                     "failing sync in a batch fails alone and keeps its writes\n";
        {
            // Paged manifests: a one-chunk change re-hashes one 64 MiB page, and the new root
            // shares every other page with its predecessor.
            Store store(path);
            auto file = store.Open("paged", true, true, false);
            std::vector<unsigned char> bytes(136 * 1024 * 1024);
            std::mt19937_64 random(7);
            for (auto &b : bytes)
                b = random() & 255;
            store.Write(file.id, bytes.data(), bytes.size(), 0);
            auto first = store.Seal(file.id);
            auto pages = Scalar(path, "SELECT count(*) FROM manifest_pages");
            unsigned char byte = 1;
            store.Write(file.id, &byte, 1, 80 * 1024 * 1024);
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
            bytes[80 * 1024 * 1024] = 1;
            store.ReadRoot(second.id, check.data(), check.size(), 0);
            Require(check == bytes, "second paged root mismatch");
            // Overlay pressure: more than the dirty bound without a sync commits with NORMAL
            // durability, keeps reads exact and retires the full segment.
            std::vector<unsigned char> large(96 * 1024 * 1024, 0x5a);
            store.Write(file.id, large.data(), large.size(), 0);
            Require(Scalar(path, "SELECT size FROM files WHERE name='paged'") == int64_t(bytes.size()) &&
                        Scalar(path, "SELECT count(*) FROM extents e JOIN files f ON f.id=e.file WHERE f.name='paged' "
                                     "AND e.chunk=200") == 1,
                    "pressure publication did not publish");
            Require(FolderBytes(std::string(path) + "/journal") <= Store::SEGMENT_LIMIT,
                    "full segment not retired by pressure publication");
            // Below the writer's bound, buffered writes beyond 16 MiB are published in the background.
            auto background = counters[size_t(Counter::LocalBackgroundPublications)].load();
            std::vector<unsigned char> medium(24 * 1024 * 1024, 0x5b);
            store.Write(file.id, medium.data(), medium.size(), 100 * 1024 * 1024);
            for (int i = 0; i < 200 && counters[size_t(Counter::LocalBackgroundPublications)].load() == background; i++)
                usleep(10000);
            Require(counters[size_t(Counter::LocalBackgroundPublications)].load() > background,
                    "no background publication");
            std::vector<unsigned char> sample(4096);
            store.Read(file.id, sample.data(), sample.size(), 90 * 1024 * 1024);
            Require(std::all_of(sample.begin(), sample.end(), [](unsigned char c) { return c == 0x5a; }),
                    "pressure-published overlay read mismatch");
            store.Verify();
            // Commits do not copy WAL frames into the database file; the background
            // checkpointer does, without blocking further commits.
            store.Sync();
            bool advanced = false;
            for (uint64_t i = 0; i < 4000 && !advanced; i++) {
                unsigned char value = (unsigned char)(i & 255);
                store.Write(file.id, &value, 1, (i * 7919 % 540) * Store::CHUNK + i / 540);
                store.Seal(file.id); // one new manifest page of metadata per seal
                advanced = counters[size_t(Counter::WalCheckpoints)].load() > 0;
            }
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
            // Payload lives in segments, so metadata grows the WAL: one new manifest page per seal.
            for (uint64_t i = 0; i < 20000 && std::filesystem::file_size(wal) <= 64ull * 1024 * 1024; i++) {
                unsigned char value = (unsigned char)(i & 255);
                store.Write(file.id, &value, 1, (i * 7919 % 540) * Store::CHUNK + i / 540);
                store.Seal(file.id);
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
        std::cout << "PASS paged manifests share unchanged 64 MiB pages; overlay pressure publication bounds memory "
                     "and retires full segments; background publication; background WAL checkpoint; WAL truncated to "
                     "64 MiB after a lagging checkpoint\n";
        std::filesystem::remove_all(path);
    } catch (const std::exception &e) {
        std::cerr << e.what() << " (fixture retained at " << path << ")\n";
        return 1;
    }
}
