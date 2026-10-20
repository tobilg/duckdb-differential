#pragma once
// Followers: ATTACH 'differential:s3://…/heads/<database>[/<uuid>][/<branch>]' (FOLLOW true,
// FOLLOW_INTERVAL n) is a read-only attachment that moves to the branch's newest state: every shipped
// commit when the branch ships its WAL (SHIP_WAL), else every exported snapshot.
//
// A background poller (one per process) checks each follower once per interval: one GET of the next
// epoch (or one HEAD of the branch head) when nothing changed. A new state is opened as a hidden
// read-only attachment by the poller, never while a query binds. The alias's catalog routes every
// lookup to the state its transaction already reads, else to the newest prepared one, so one
// transaction always reads one state.
#include "duckdb.hpp"
#include "differential_shipping.hpp"
#include <chrono>
#include <mutex>

namespace duckdb {
class Follower {
  public:
    Follower(DatabaseInstance &instance, std::shared_ptr<differential::Control> control, std::string provider,
             differential::BranchLocation location, int64_t interval_ms, std::string alias);
    // Resolves the newest state now (S3 requests); returns the alias's own internal path.
    std::string Start();
    // The state a lookup of this transaction reads (registered per transaction): a hidden
    // attachment, or nullptr for the alias's own (initial) state.
    shared_ptr<AttachedDatabase> Choose(ClientContext &context);
    // One poll (poller thread): returns whether a newer state was prepared.
    bool Poll();
    // The alias is detached: close its states.
    void Stop(ClientContext &context);
    struct Status {
        std::string alias, database, branch, mode, error;
        int64_t interval_ms = 0, chain = 0, epoch = 0, snapshot = 0, published_at = 0, checked_at = 0;
        int64_t polls = 0, changes = 0;
    };
    Status GetStatus();
    std::chrono::steady_clock::time_point due;
    const int64_t interval_ms;
    bool Stopped();

  private:
    weak_ptr<DatabaseInstance> instance;
    std::shared_ptr<differential::Control> control;
    std::string provider, alias;
    differential::BranchLocation location;
    std::mutex mutex;
    bool commits = false, stopped = false;
    std::unique_ptr<differential::ChainFollower> chain;
    std::string head_version, descriptor;
    int64_t snapshot = 0;
    struct State {
        std::string name, image; // hidden attachment, follower image id ("" for an export)
    };
    std::vector<State> states; // newest last
    uint64_t generation = 0;
    Status status;
    std::string Path(const std::shared_ptr<const differential::FollowImage> &image, std::string &image_id);
};
// Register a follower with the process's poller.
void Follow(const std::shared_ptr<Follower> &);
// Every live follower of this process (differential_followers()).
std::vector<Follower::Status> Followers();
} // namespace duckdb
