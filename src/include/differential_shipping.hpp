#pragma once
// Continuous WAL shipping (opt-in per database: ATTACH ... SHIP_WAL 'async' | 'sync').
//
// A single-writer S3 database ships the published state of each writable branch's file group (main
// file and WAL sidecars) to S3 as it changes, so another host can recover the branch as of the last
// shipped commit and DuckDB replays its WAL natively. Shipped state forms chains:
//
//   dbs/<db uuid>/log/<branch name>/<branch uuid>/<9999999999 - chain>/<9999999999 - epoch>
//
// Epoch 0 of a chain names a base, an internal export of the main file (a descriptor without a
// snapshot number), and carries the WAL files whole plus the main file's changes since the base.
// Every later epoch carries the group's changes since the previous one: changed 256 KiB chunks of
// the main file and the appended (or rewritten) tail of each WAL file, inline. Epochs are
// create-only and each names the SHA-256 of its predecessor, so a gap or a fork is detected. A new
// chain starts every 256 epochs or 64 MiB of epoch data; the superseded chain ends with a redirect
// epoch naming its successor. Newest chains and epochs list first.
#include "differential_export.hpp"
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace differential {
// Per database, stored in the metadata store: applies to every writable attachment until turned off.
struct ShipConfig {
    std::string mode = "off"; // "off", "async" (a commit returns after the local commit) or "sync"
                              // (a commit also waits until its state is in S3)
    int64_t interval_ms = 1000; // async: minimum time between two epochs of a branch
};
ShipConfig GetShipConfig(Control &, const std::string &database);
// Needs a single-writer database with an S3 DATA_PATH. Turning shipping off stops tracking.
void SetShipConfig(Control &, const std::string &database, const ShipConfig &);

// A writable attachment of a branch of a shipping database: track its file group and ship it on
// the process's shipping worker.
void StartShipping(const std::shared_ptr<Control> &, const Branch &);
// After a durable sync of a branch's file: async schedules a round; sync waits until the shipped
// state includes the sync (throws [differential:ship_failed] when it cannot be shipped).
// `checkpoint`: the sync completed a DuckDB checkpoint; the next epoch starts a new chain from it.
void AfterSync(Control &, const std::string &branch, bool checkpoint = false);
// The branch's main file closes: ship what is left (bounded wait; errors are recorded, not raised)
// and stop shipping the branch from this process.
void ShipOnClose(Control &, const std::string &branch);

struct ShipStatus {
    std::string mode;           // "off" when the branch is not shipped
    int64_t chain = 0, epoch = -1;
    uint64_t shipped_sequence = 0, pending_sequence = 0;
    int64_t shipped_at = 0;     // microseconds; 0 before the first epoch
    std::string error;          // the last shipping failure, "" after a success
};
ShipStatus GetShipStatus(Control &, const std::string &database, const std::string &branch);
std::vector<std::pair<std::string, uint64_t>> ShippingMetrics();

// One decoded epoch.
struct Epoch {
    struct File {
        std::string suffix; // "" for the main file, else ".wal", ".wal.checkpoint", ...
        bool full = false, log = false;
        uint64_t size = 0;
        struct Chunk {
            uint64_t index = 0;
            std::string hash; // "" for a hole
            uint64_t payload = 0;
        };
        std::vector<Chunk> chunks;
        uint64_t tail_offset = 0, tail_length = 0, tail_payload = 0;
        std::string tail_hash;
    };
    std::string database, branch, branch_name, previous, base;
    int64_t chain = 0, epoch = 0, next_chain = -1, published_at = 0;
    uint64_t sequence = 0;
    std::vector<File> files;
    std::vector<std::string> removed;
    std::vector<unsigned char> payload;
    std::string hash; // SHA-256 of the whole object
};
// The newest epoch of a branch's newest chain in S3: {chain, epoch}; nothing when it never shipped.
// `branch` (UUID) may be empty: the incarnation of `branch_name` with the newest epoch.
struct ChainPosition {
    std::string branch;
    int64_t chain = 0, epoch = 0;
};
std::optional<ChainPosition> NewestEpoch(S3Provider &, const std::string &database, const std::string &branch_name,
                                         const std::string &branch = "");
std::optional<ChainPosition> NewestEpoch(Control &, const std::string &provider, const std::string &database,
                                         const std::string &branch_name);
// heads/<database>[/<database uuid>][/<branch>] path of a branch that ships (or exported a
// snapshot): its database identity and name and the branch name (branch heads are not needed).
struct BranchLocation {
    std::string database, database_name, branch_name;
};
BranchLocation ResolveBranchLocation(Control &, const std::string &provider, const std::string &path);
// The version (ETag) of a branch's head object in S3, "" when it has none: one HEAD request.
std::string BranchHeadVersion(Control &, const std::string &provider, const BranchLocation &);
// Read and verify one epoch (std::nullopt when it does not exist).
std::optional<Epoch> ReadEpoch(S3Provider &, const std::string &database, const std::string &branch_name,
                               const std::string &branch, int64_t chain, int64_t epoch);

// Recovery: a new local database `name` (with `same_identity`: the original database and branch
// identities, for a failover of the writer) whose branch is the shipped state of `branch_name` of
// database `database` in S3 location `provider`: the newest chain's base, imported without
// downloading, plus every consecutive verified epoch. DuckDB replays the WAL at the first ATTACH.
struct RecoveredBranch {
    std::string database, branch;
    int64_t chain = 0, epoch = 0;
    uint64_t sequence = 0;
};
RecoveredBranch RecoverBranch(Control &, const std::string &provider, const std::string &database,
                              const std::string &branch_name, const std::string &name, bool same_identity);

// Followers: an immutable image of a shipped state (base export plus applied epochs), served
// read-only as diff://follow/<id> (the main file) and diff://follow/<id>.wal (WAL files).
class FollowImage {
  public:
    std::shared_ptr<RemoteSnapshot> base;
    uint64_t size = 0;
    std::map<uint64_t, SharedBytes> chunks; // main-file chunks newer than the base (null: a hole)
    std::map<std::string, std::shared_ptr<const Bytes>> logs; // by suffix
    int64_t chain = 0, epoch = 0;
    uint64_t sequence = 0;
    int64_t published_at = 0;
    uint64_t Read(const std::string &suffix, void *data, uint64_t length, uint64_t offset) const;
    uint64_t Size(const std::string &suffix) const;
    bool Exists(const std::string &suffix) const { return suffix.empty() || logs.count(suffix); }
    // Whether DuckDB can open it read-only (no checkpoint in progress, which recovery would rewrite).
    bool Openable() const { return !logs.count(".wal.checkpoint") && !logs.count(".wal.recovery"); }
};
std::string RegisterFollowImage(std::shared_ptr<const FollowImage>);
std::shared_ptr<const FollowImage> FindFollowImage(const std::string &id);
void ReleaseFollowImage(const std::string &id);
// Follows one shipped branch: Advance() applies new epochs (or starts over on a new chain) and
// returns whether the image changed.
class ChainFollower {
  public:
    ChainFollower(Control &, std::string provider, std::string database, std::string branch_name, std::string branch);
    // One GET when nothing changed (the next epoch does not exist yet).
    bool Advance();
    std::shared_ptr<const FollowImage> Image() const { return image; }

  private:
    Control &control;
    std::shared_ptr<S3Provider> provider;
    std::string provider_id, database, branch_name, branch;
    std::shared_ptr<FollowImage> image;
    std::string last_hash;
    void Start(int64_t chain);
    void Apply(const Epoch &);
};
} // namespace differential
