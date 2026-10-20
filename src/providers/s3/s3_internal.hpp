#pragma once
// Shared by the S3 provider sources (snapshot.cpp: exports, reads, heads; remote_gc.cpp: remote
// garbage collection). Not part of the extension's interface.
#include "differential_export.hpp"
#include <functional>
#include <optional>
#include <set>

namespace differential {
// The process's client of a registered S3 location (kept alive between operations).
std::shared_ptr<S3Provider> S3Client(Control &, const std::string &provider);
// Run tasks on at most `width` threads; the first failure is rethrown after all finish.
void ParallelTasks(size_t count, unsigned width, const std::function<void(size_t)> &task);
// Stage and upload an immutable object named by `key` (create-only; an identical object is kept).
void UploadImmutable(Control &, S3Provider &, const std::string &key, const std::vector<unsigned char> &bytes);
Descriptor DecodeDescriptor(const std::vector<unsigned char> &bytes);
// The packs a verified index page references (dbs/<uuid>/packs/<sha256>); throws for a corrupt page.
std::vector<std::string> IndexPackKeys(const Bytes &index);
// A branch head object: its head, or nothing for a dropped branch's tombstone.
std::optional<Head> DecodeHeadObject(const std::vector<unsigned char> &bytes);
std::optional<RemoteCommit> DecodeCommitRecord(const std::vector<unsigned char> &bytes);
// Remote collection (remote_gc.cpp): learn the location's condemnation records this repository has
// not seen. Exports stop referencing condemned objects, and local objects stored only in S3 whose
// pack is condemned are copied locally first. Returns the keys condemned by newly learned records.
std::set<std::string> SyncCondemned(Control &, const std::string &provider);
std::vector<std::pair<std::string, uint64_t>> RemoteCollectionMetrics();
// Continuous WAL shipping: upload the base of a chain, an internal export of the branch's captured
// root `root` (a descriptor with snapshot 0, no commit record), and the database's names entry.
// Returns the descriptor key.
std::string ExportBase(Control &, const std::string &provider, const Branch &branch, const Root &root,
                       const std::function<void()> &check_cancel);
} // namespace differential
