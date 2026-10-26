#include "differential_control.hpp"
#include <sqlite3.h>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
using namespace differential;
void Check(bool result) {
    if (!result)
        throw std::runtime_error("Control invariant failed");
}
template <class F> void Reject(F f) {
    bool failed = false;
    try {
        f();
    } catch (const std::exception &) {
        failed = true;
    }
    Check(failed);
}
int main() {
    char pattern[] = "/tmp/differential-control-XXXXXX";
    auto path = mkdtemp(pattern);
    if (!path)
        return 2;
    try {
        auto store = std::make_shared<Store>(path);
        int64_t clock = 100;
        Control control(store, [&] { return clock; });
        auto db = control.CreateDatabase("sales", "create");
        auto branch = control.GetBranch(db.database, "main");
        auto lease = control.Acquire(branch);
        branch = lease->branch;
        auto file = store->Open(branch.FileName(), true, false, false);
        auto snapshot = [&](Branch b, int64_t inode, const std::string &bytes, const std::string &name,
                            const std::string &request) {
            b.head = control.GetBranch(b.database, b.id, true).head;
            store->Write(inode, bytes.data(), bytes.size(), 0);
            auto operation = control.BeginSnapshot(b, {name, "", "", ""}, request);
            return control.PublishSnapshot(operation, b, store->Seal(inode), clock);
        };
        auto one = snapshot(branch, file.id, "first", "reserved", "first");
        Check(one.snapshot == 1);
        clock = 200;
        auto two = snapshot(branch, file.id, "second", "", "second");
        Check(two.snapshot == 2);
        Check(control.SnapshotAt("sales", "main", 200).id == 2);
        auto three = snapshot(branch, file.id, "third", "", "third");
        Check(three.snapshot == 3);
        Check(control.SnapshotAt("sales", "main", 200).id == 3); // equal-time event sequence wins
        control.Retire("sales", 3, "retire-third");
        Check(control.SnapshotAt("sales", "main", 200).id == 2);
        Check(control.SnapshotAt("sales", "main", 199).id == 1);
        clock = 250;
        auto fork = control.CreateBranch("sales", 1, "experiment", "fork", false);
        Reject([&] { control.SnapshotAt("sales", "experiment", 249); });
        auto base = control.SnapshotAt("sales", "experiment", 250);
        Check(base.id == 1 && base.published_at == 100 && base.event_at == 250);
        auto experiment = control.GetBranch("sales", "experiment");
        auto other = control.Acquire(experiment);
        experiment = other->branch;
        auto otherfile = store->Open(experiment.FileName(), false, false, false);
        clock = 300;
        auto four = snapshot(experiment, otherfile.id, "branch", "", "fourth");
        Check(four.snapshot == 4);
        Check(control.SnapshotAt("sales", "main", 300).id == 2);
        control.Retire("sales", 1, "retire-first");
        Reject([&] { control.SnapshotAt("sales", "experiment", 299); }); // do not select later 4
        Reject([&] { snapshot(branch, file.id, "name", "reserved", "conflict"); });
        auto stale = branch;
        lease.reset();
        clock = 400;
        auto prepared = control.PrepareCompound("sales", "main", 2, "restore", true);
        Reject([&] { control.BeginSnapshot(stale, {}, "stale"); });
        auto restored =
            control.FinishCompound(prepared.first, prepared.second->branch, store->Seal(file.id), true);
        Check(restored.preservation == 5 && restored.snapshot == 6);
        Check(control.SnapshotAt("sales", "main", 400).id == 6);
        Check(control.PrepareCompound("sales", "main", 2, "restore", true).first.snapshot == 6);
        prepared.second.reset();
        other.reset();
        clock = 500;
        auto drop = control.PrepareCompound("sales", "experiment", 0, "drop", false);
        control.FinishCompound(drop.first, drop.second->branch, store->Seal(otherfile.id), false);
        drop.second.reset();
        Check(control.SnapshotAt("sales", experiment.id, 500).id ==
              4); // deleted UUID remains addressable for history
        auto clone = control.CreateBranch("sales", 2, "clone", "clone", true);
        Check(clone.snapshot == 1 && clone.database != db.database);
        sqlite3 *metadata = nullptr;
        Check(sqlite3_open((std::filesystem::path(path) / "engine.sqlite").c_str(), &metadata) == SQLITE_OK);
        Check(sqlite3_exec(metadata,
                           "UPDATE databases SET last_snapshot=9223372036854775807 WHERE name='clone'",
                           nullptr, nullptr, nullptr) == SQLITE_OK);
        sqlite3_close(metadata);
        auto cb = control.GetBranch("clone", "main");
        auto cl = control.Acquire(cb);
        cb = cl->branch;
        auto cf = store->Open(cb.FileName(), false, false, false);
        Reject([&] { snapshot(cb, cf.id, "end", "", "exhausted"); });
        Check(control.Snapshots("clone").size() == 1);
        auto merge = control.BeginMerge("sales", 2, 2, 2, "diagnostics", "diagnostics");
        auto request_lock = control.LockMerge(merge, [] {});
        auto inspect = [&] {
            auto rows = control.MergeRequests("sales", "diagnostics", "", 1, [] {});
            Check(rows.size() == 1 && rows[0].attempt.has_value());
            return *rows[0].attempt;
        };
        auto first_attempt = control.BeginMergeAttempt(merge);
        Check(first_attempt.count == 1 && first_attempt.started_at == 500);
        clock = 450; // Wall clock regression cannot invert timestamps.
        control.UpdateMergeAttempt(merge, first_attempt.id, "comparison");
        Check(inspect().updated_at == 500);
        auto second_attempt = control.BeginMergeAttempt(merge);
        Check(second_attempt.id != first_attempt.id && second_attempt.count == 2 && second_attempt.started_at == 500);
        control.UpdateMergeAttempt(merge, first_attempt.id, "staging", "merge_limit");
        Check(inspect().phase == "request_locked" && inspect().state == "incomplete");
        Reject([&] { control.UpdateMergeAttempt(merge, second_attempt.id, "untrusted phase"); });
        control.UpdateMergeAttempt(merge, second_attempt.id, "comparison", "arbitrary sensitive error text");
        Check(inspect().error_code == "merge_error" && inspect().state == "failed");
        clock = 600;
        auto third_attempt = control.BeginMergeAttempt(merge);
        auto original = control.GetSnapshot("sales", 2);
        auto result = control.PublishMerge(merge, {original.root, uint64_t(original.size)}, "");
        Check(result.state == "complete" && inspect().count == 3 && inspect().state == "succeeded");
        Check(inspect().finished_at == 600 && inspect().error_code.empty());
        control.UpdateMergeAttempt(merge, third_attempt.id, "publishing", "cancelled");
        Check(inspect().state == "succeeded");
        Reject([&] { control.BeginMergeAttempt(merge); });
        std::cout << "PASS merge attempt token fencing, clock regression, safe codes and atomic completion\n";
        // Ancestry is committed metadata, not a second pending export operation.
        S3Config config;
        config.bucket = "unused-unit-bucket";
        config.prefix = "control-test/";
        auto provider = control.EnsureS3Provider(config, {});
        auto remote = control.CreateDatabase("single_s3", "s3-create", provider);
        auto remote_lease = control.Acquire(control.GetBranch(remote.database, "main"));
        auto remote_file = store->Open(remote_lease->branch.FileName(), true, false, false);
        snapshot(remote_lease->branch, remote_file.id, "remote", "", "s3-snapshot");
        int pending = 0, ancestry = 0;
        for (auto &operation : control.Operations()) {
            if (operation.database != remote.database) continue;
            pending += operation.state == "local_complete_export_pending";
            if (operation.kind == "snapshot_ancestry") {
                Check(operation.state == "complete");
                ancestry++;
            }
        }
        Check(pending == 1 && ancestry == 1);
        pending = ancestry = 0;
        auto cursor = control.List("operations");
        std::vector<MetadataCell> row;
        while (cursor->Next(row)) {
            if (row[4].text != remote.database) continue;
            pending += row[3].text == "local_complete_export_pending";
            if (row[2].text == "snapshot_ancestry") {
                Check(row[3].text == "complete");
                ancestry++;
            }
        }
        cursor.reset();
        Check(pending == 1 && ancestry == 1);
        remote_lease.reset();
        std::cout << "PASS S3 ancestry stays complete in both operation listings while its export is pending\n";
        store->Verify();
        std::cout << "PASS timestamp ties, branch creation, retired candidates, interleaving, restore, "
                     "deleted UUID history, stale epoch and allocator exhaustion\n";
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        std::filesystem::remove_all(path);
        return 1;
    }
    std::filesystem::remove_all(path);
    return 0;
}
