// Shared databases below the S3 transport: a published head's map is adopted without its bytes
// (under the same root identity as sealing them), remote-only objects are read through the fetch
// hook in batches, verification accepts them, local overwrites keep working, unexported snapshots
// are renumbered with everything that refers to them, and collection forgets remote-only objects.
#include "differential_control.hpp"
#include "differential_metrics.hpp"
#include <sqlite3.h>
#include <algorithm>
#include <filesystem>
#include <set>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
using namespace differential;
static void Check(bool result, const char *message) {
    if (!result)
        throw std::runtime_error(message);
}
template <class F> static void Reject(F f, const char *message) {
    bool failed = false;
    try {
        f();
    } catch (const std::exception &) {
        failed = true;
    }
    Check(failed, message);
}
static int64_t Scalar(const std::string &directory, const std::string &sql) {
    sqlite3 *db = nullptr;
    Check(sqlite3_open((directory + "/engine.sqlite").c_str(), &db) == SQLITE_OK, "open metadata");
    sqlite3_stmt *query = nullptr;
    Check(sqlite3_prepare_v2(db, sql.c_str(), -1, &query, nullptr) == SQLITE_OK, "prepare metadata query");
    Check(sqlite3_step(query) == SQLITE_ROW, "metadata query row");
    auto value = sqlite3_column_int64(query, 0);
    sqlite3_finalize(query);
    sqlite3_close(db);
    return value;
}
int main() {
    char source_pattern[] = "/tmp/differential-shared-source-XXXXXX";
    char target_pattern[] = "/tmp/differential-shared-target-XXXXXX";
    auto source_path = mkdtemp(source_pattern);
    auto target_path = mkdtemp(target_pattern);
    if (!source_path || !target_path)
        return 2;
    try {
        constexpr uint64_t CHUNK = Store::CHUNK;
        // The published image (another host): two full chunks, a hole, and a partial chunk on the
        // second manifest page.
        Store source(source_path);
        auto image = source.Open("image", true, false, false);
        std::vector<unsigned char> a(CHUNK, 'a'), b(CHUNK, 'b'), c(1000, 'c');
        source.Write(image.id, a.data(), a.size(), 0);
        source.Write(image.id, b.data(), b.size(), CHUNK);
        source.Write(image.id, c.data(), c.size(), 300 * CHUNK);
        auto root = source.Seal(image.id);
        auto chunks = source.RootChunks(root.id, 0, 301);
        Check(chunks.size() == 3, "source chunk map");

        auto store = std::make_shared<Store>(target_path);
        Control control(store);
        std::vector<size_t> batches;
        store->SetRemoteFetch([&](const std::vector<std::string> &hashes, const void *) {
            batches.push_back(hashes.size());
            std::map<std::string, SharedBytes> out;
            for (auto &hash : hashes)
                out[hash] = source.Object(hash);
            return out;
        });
        S3Config config;
        config.bucket = "bucket";
        config.prefix = "prefix/";
        auto provider = control.EnsureS3Provider(config, {});
        std::string id = "0b1c2d3e-4f50-8a61-9b72-c3d4e5f60718";
        control.CreateSharedDatabase(id, "team", provider);
        Check(control.Writers("team") == "shared", "shared mode recorded");
        RemoteHead head;
        head.key = "dbs/" + id + "/branches/main";
        head.etag = "\"etag-1\"";
        head.write_id = std::string(32, '1');
        head.branch = "1a2b3c4d-5e6f-4a0b-8c1d-2e3f4a5b6c7d";
        head.branch_name = "main";
        head.descriptor = "descriptors/" + std::string(64, 'd');
        head.root = root.id;
        head.snapshot = 5;
        head.published_at = 1000;
        head.size = root.size;
        head.chunks = chunks;
        for (size_t i = 0; i < chunks.size(); i++)
            head.locations[chunks[i].object] = RemoteChunk{std::string(64, 'e'), id, i};
        // A conflict recorded on a branch is cleared when the branch adopts a head.
        auto local = control.CreateEmptyBranch(id, "main");
        control.RecordConflict(BranchConflict{local.id, "", 7, 6, 0});
        auto conflict = control.Conflict(local.id);
        Check(conflict && conflict->snapshot == 7 && conflict->remote_snapshot == 6 && conflict->kept_branch.empty(),
              "conflict recorded");
        control.RecordConflict(BranchConflict{local.id, "main-conflict-7", 7, 6, 0});
        Check(control.Conflict(local.id)->kept_branch == "main-conflict-7", "conflict updated");
        auto bad = head;
        bad.root = std::string(64, '0');
        Reject([&] { control.AdoptRemoteHead(id, provider, bad, true); }, "a map that does not match its root");
        auto branch = control.AdoptRemoteHead(id, provider, head, true);
        Check(branch.id == head.branch && branch.head == 5 && branch.root == root.id, "adopted branch");
        auto branches = control.Branches(id);
        Check(!control.Conflict(local.id) && branches.size() == 2 &&
                  std::any_of(branches.begin(), branches.end(),
                              [&](const Branch &b) { return b.id == local.id && b.state == "deleted"; }),
              "the unchanged local branch was replaced by the published one");
        Check(store->RootSize(root.id) == root.size, "adopted root identity");
        Check(Scalar(target_path, "SELECT count(*) FROM objects WHERE data IS NOT NULL") == 0, "nothing downloaded");
        auto base = control.HeadWriteFor(provider, head.key);
        Check(base && base->etag == head.etag && base->snapshot == 5, "head version recorded as the base");
        auto adopted = control.GetSnapshot(id, 5, false);
        Check(adopted.evicted && adopted.export_state == "complete" && adopted.export_key == head.descriptor,
              "adopted snapshot recorded as exported and remote");

        // Reads of the live file fetch remote-only objects, one batch per read.
        auto file = store->Open(branch.FileName(), false, false, false);
        std::vector<unsigned char> read(3 * CHUNK);
        Check(store->Read(file.id, read.data(), read.size(), 0) == read.size(), "read live file");
        Check(std::equal(a.begin(), a.end(), read.begin()) && std::equal(b.begin(), b.end(), read.begin() + CHUNK) &&
                  std::all_of(read.begin() + 2 * CHUNK, read.end(), [](unsigned char x) { return x == 0; }),
              "remote bytes and hole");
        Check(!batches.empty() && batches.back() == 2, "two remote chunks in one fetch");
        std::vector<unsigned char> tail(1000);
        store->ReadRoot(root.id, tail.data(), tail.size(), 300 * CHUNK);
        Check(tail == c, "remote partial chunk through the root");
        store->Verify();

        // A local write over a remote-only chunk reads it first, then stores its own copy.
        auto lease = control.Acquire(branch);
        std::vector<unsigned char> z(100, 'z');
        store->Write(file.id, z.data(), z.size(), 0);
        auto operation = control.BeginSnapshot(lease->branch, {}, "local");
        auto published = control.PublishSnapshot(operation, lease->branch, store->Seal(file.id), 2000);
        Check(published.snapshot == 6, "local snapshot after the adopted one");
        std::vector<unsigned char> first(200);
        store->Read(file.id, first.data(), first.size(), 0);
        Check(first[0] == 'z' && first[99] == 'z' && first[100] == 'a', "partial overwrite of a remote chunk");
        Check(Scalar(target_path, "SELECT count(*) FROM objects WHERE data IS NOT NULL") == 1, "one local object");
        store->Verify();

        // Another writer published 6..9: the unexported snapshot moves above them.
        Check(control.RenumberSnapshots(id, 6, 9) == 4, "renumbered by four");
        Reject([&] { control.GetSnapshot(id, 6, false); }, "old number gone");
        auto moved = control.GetSnapshot(id, 10, false);
        Check(moved.export_state == "pending" && control.PendingExports(id) == std::vector<int64_t>{10},
              "export follows its snapshot");
        Check(control.GetBranch(id, "main").head == 10 && control.CurrentSnapshot(id) == 10, "branch and allocator");
        Check(control.GetOperation(published.id).snapshot == 10, "operation follows its snapshot");
        Check(control.ParentSnapshot(id, 10) == 5, "history follows its snapshot");
        Check(control.RenumberSnapshots(id, 11, 3) == 0, "nothing to move");
        Reject([&] { control.RenumberSnapshots(id, 5, 20); }, "exported snapshots keep their numbers");
        store->Verify();

        // Other writers' history: cached commit records resolve as remote snapshots, expire, and lead
        // a time lookup along their parents.
        RemoteCommit other;
        other.database = id;
        other.snapshot = 9;
        other.parent = 5;
        other.published_at = 1500;
        other.branch = head.branch;
        other.branch_name = "main";
        other.descriptor = "descriptors/" + std::string(64, 'f');
        other.name = "theirs";
        control.CacheCommits(id, {other});
        Check(control.CachedCommits(id) == std::set<int64_t>{9}, "commit cached");
        auto theirs = control.FindSnapshot(id, 9);
        Check(theirs.remote && theirs.evicted && theirs.export_key == other.descriptor && theirs.parent == 5 &&
                  theirs.name == "theirs",
              "remote snapshot from its commit record");
        auto listed = control.Snapshots(id);
        Check(listed.size() == 3 && listed[1].id == 9 && listed[1].remote, "listing merges remote snapshots");
        Check(control.SnapshotAt(id, "main", 1600, 9).id == 9, "time lookup from a remote snapshot");
        Check(control.SnapshotAt(id, "main", 1200, 9).id == 5, "time lookup follows the record's parent");
        Check(control.SnapshotAt(id, "main", Control::Now()).id == 10, "time lookup from the branch head");
        control.ExpireCommits(id, {9});
        Reject([&] { control.FindSnapshot(id, 9); }, "expired remote snapshot is retired");
        Check(control.SnapshotAt(id, "main", 1600, 9).id == 5, "time lookup skips expired snapshots");

        // Once nothing references the adopted image, its remote-only objects are forgotten.
        lease.reset();
        for (int i = 0; i < 100 && control.Collect(false, 10000, "").remaining_candidates; i++) {
        }
        Check(Scalar(target_path, "SELECT count(*) FROM objects o JOIN remote_objects r ON r.hash=o.hash WHERE "
                                  "o.hash='" + chunks[0].object + "'") == 0,
              "replaced remote-only object collected");
        Check(Scalar(target_path, "SELECT count(*) FROM remote_objects r WHERE NOT EXISTS(SELECT 1 FROM objects o "
                                  "WHERE o.hash=r.hash)") == 0,
              "no dangling remote marks");
        store->Verify();

        // Read-ahead: sequential reads with a reader context fetch the following remote-only chunks in
        // the same batch (here 40 distinct chunks, read 4 at a time).
        auto big = source.Open("big", true, false, false);
        for (uint64_t chunk = 0; chunk < 40; chunk++) {
            std::vector<unsigned char> bytes(CHUNK, static_cast<unsigned char>(chunk + 1));
            source.Write(big.id, bytes.data(), bytes.size(), chunk * CHUNK);
        }
        auto big_root = source.Seal(big.id);
        RemoteHead large = head;
        large.key = "dbs/" + id + "/branches/big";
        large.branch = "2b3c4d5e-6f70-4a1b-8c2d-3e4f5a6b7c8d";
        large.branch_name = "big";
        large.root = big_root.id;
        large.snapshot = 30;
        large.size = big_root.size;
        large.chunks = source.RootChunks(big_root.id, 0, 40);
        large.locations.clear();
        for (size_t i = 0; i < large.chunks.size(); i++)
            large.locations[large.chunks[i].object] = RemoteChunk{std::string(64, 'c'), id, i};
        auto big_branch = control.AdoptRemoteHead(id, provider, large, true);
        auto big_file = store->Open(big_branch.FileName(), false, false, false);
        Store::DropCaches();
        batches.clear();
        auto ahead_before = counters[size_t(Counter::RemoteObjectAheadBytes)].load();
        int context = 0;
        std::vector<unsigned char> block(4 * CHUNK);
        for (uint64_t offset = 0; offset < 40 * CHUNK; offset += block.size()) {
            Check(store->Read(big_file.id, block.data(), block.size(), offset, &context) == block.size(), "read big");
            Check(block[0] == static_cast<unsigned char>(offset / CHUNK + 1) && block.back() == block[3 * CHUNK],
                  "read-ahead keeps reads correct");
        }
        Check(counters[size_t(Counter::RemoteObjectAheadBytes)].load() > ahead_before, "chunks fetched ahead");
        Check(*std::max_element(batches.begin(), batches.end()) > 4, "a batch larger than one read");
        batches.clear();
        auto without = counters[size_t(Counter::RemoteObjectAheadBytes)].load();
        Store::DropCaches();
        for (uint64_t offset = 0; offset < 40 * CHUNK; offset += block.size())
            store->Read(big_file.id, block.data(), block.size(), offset);
        Check(counters[size_t(Counter::RemoteObjectAheadBytes)].load() == without &&
                  *std::max_element(batches.begin(), batches.end()) <= 4,
              "no read-ahead without a reader context");
        store->Verify();
        std::cout << "PASS adopted head map (same root, nothing downloaded), batched remote reads, verification, "
                     "local overwrite, renumbering, collection of remote-only objects, read-ahead\n";
    } catch (const std::exception &e) {
        std::cerr << e.what() << " (fixtures retained at " << source_path << ", " << target_path << ")\n";
        return 1;
    }
    std::filesystem::remove_all(source_path);
    std::filesystem::remove_all(target_path);
    return 0;
}
