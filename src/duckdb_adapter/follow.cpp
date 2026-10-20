// Followers (follow.hpp): a background poller per process prepares newer states of followed
// branches as hidden read-only attachments; each transaction reads one state.
#include "follow.hpp"
#include "differential_catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include <condition_variable>
#include <thread>

namespace duckdb {
namespace {
constexpr size_t KEEP_STATES = 4; // older states are detached (queries reading them keep them)
// The state each follower serves the current transaction.
struct FollowChoice : ClientContextState {
    std::map<const Follower *, std::string> chosen; // hidden attachment name, "" for the alias's own
    void TransactionCommit(MetaTransaction &, ClientContext &) override { chosen.clear(); }
    void TransactionRollback(MetaTransaction &, ClientContext &) override { chosen.clear(); }
    void TransactionRollback(MetaTransaction &, ClientContext &, optional_ptr<ErrorData>) override { chosen.clear(); }
};
// Never destroyed: the poller thread may still run while static objects are destroyed at exit
// (it is stopped and joined by Joiner, which exits before the AWS SDK it uses is shut down).
struct Poller {
    std::mutex mutex;
    std::condition_variable signal;
    std::vector<std::weak_ptr<Follower>> followers;
    std::thread thread;
    bool running = false;
};
Poller &P() {
    static auto *poller = new Poller();
    return *poller;
}
#define poller_mutex P().mutex
#define poller_signal P().signal
#define followers P().followers
#define poller P().thread
#define poller_running P().running
void Poll() {
    std::unique_lock<std::mutex> lock(poller_mutex);
    while (poller_running) {
        auto now = std::chrono::steady_clock::now();
        auto wake = now + std::chrono::seconds(1);
        std::vector<std::shared_ptr<Follower>> due;
        for (auto it = followers.begin(); it != followers.end();) {
            auto follower = it->lock();
            if (!follower || follower->Stopped()) {
                it = followers.erase(it);
                continue;
            }
            if (follower->due <= now)
                due.push_back(follower);
            else
                wake = std::min(wake, follower->due);
            ++it;
        }
        if (due.empty()) {
            poller_signal.wait_until(lock, wake);
            continue;
        }
        lock.unlock();
        for (auto &follower : due) {
            follower->due = std::chrono::steady_clock::now() + std::chrono::milliseconds(follower->interval_ms);
            follower->Poll(); // records its own errors
        }
        due.clear();
        lock.lock();
    }
}
} // namespace

Follower::Follower(DatabaseInstance &instance, std::shared_ptr<differential::Control> control, std::string provider,
                   differential::BranchLocation location, int64_t interval_ms, std::string alias)
    : due(std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms)), interval_ms(interval_ms),
      instance(instance.shared_from_this()), control(std::move(control)), provider(std::move(provider)),
      alias(std::move(alias)), location(std::move(location)) {
    status.alias = this->alias;
    status.database = this->location.database_name;
    status.branch = this->location.branch_name;
    status.interval_ms = interval_ms;
}
std::string Follower::Path(const std::shared_ptr<const differential::FollowImage> &image, std::string &image_id) {
    image_id = differential::RegisterFollowImage(image);
    return "diff://follow/" + image_id;
}
std::string Follower::Start() {
    std::lock_guard<std::mutex> guard(mutex);
    // Commit level when the branch ships its WAL, else its exported snapshots.
    auto s3 = differential::NewestEpoch(*control, provider, location.database, location.branch_name);
    status.checked_at = control->Clock();
    if (s3) {
        commits = true;
        status.mode = "commits";
        chain = std::make_unique<differential::ChainFollower>(*control, provider, location.database,
                                                              location.branch_name, s3->branch);
        auto image = chain->Image();
        if (!image->Openable()) {
            // Mid-checkpoint: the main file alone is the last checkpoint's consistent state.
            auto settled = std::make_shared<differential::FollowImage>(*image);
            settled->logs.clear();
            image = settled;
        }
        status.chain = image->chain;
        status.epoch = image->epoch;
        status.published_at = image->published_at;
        std::string id;
        auto path = Path(image, id);
        states.push_back({"", id});
        return path;
    }
    status.mode = "snapshots";
    head_version = differential::BranchHeadVersion(*control, provider, location);
    auto head = differential::ResolveHead(*control, provider,
                                          location.database_name + "/" + location.database + "/" + location.branch_name);
    descriptor = head.descriptor;
    status.snapshot = snapshot = head.snapshot;
    status.published_at = head.published_at;
    states.push_back({"", ""});
    return "diff://export/" + provider + "/" + descriptor;
}
bool Follower::Poll() {
    std::string path, image_id;
    try {
        std::lock_guard<std::mutex> guard(mutex);
        if (!stopped && !instance.lock())
            stopped = true; // the database closed without a DETACH
        if (stopped)
            return false;
        ++status.polls;
        status.checked_at = control->Clock();
        if (commits) {
            if (!chain->Advance())
                return false;
            auto image = chain->Image();
            if (!image->Openable())
                return false; // a checkpoint in progress: its end is the next state
            status.chain = image->chain;
            status.epoch = image->epoch;
            status.published_at = image->published_at;
            path = Path(image, image_id);
        } else {
            auto version = differential::BranchHeadVersion(*control, provider, location);
            if (version.empty() || version == head_version)
                return false;
            head_version = version;
            auto head = differential::ResolveHead(*control, provider,
                                                  location.database_name + "/" + location.database + "/" +
                                                      location.branch_name);
            if (head.descriptor == descriptor)
                return false;
            descriptor = head.descriptor;
            status.snapshot = snapshot = head.snapshot;
            status.published_at = head.published_at;
            path = "diff://export/" + provider + "/" + descriptor;
        }
        status.error.clear();
    } catch (const std::exception &error) {
        std::lock_guard<std::mutex> guard(mutex);
        status.error = error.what();
        return false;
    }
    // Open the new state outside the follower's lock (lookups keep using the previous one meanwhile).
    auto db = instance.lock();
    if (!db)
        return false;
    auto name = "__differential_follow_" + alias + "_" + std::to_string(++generation);
    std::vector<State> retired;
    try {
        Connection connection(*db);
        auto &context = *connection.context;
        context.RunFunctionInTransaction([&] {
            AttachInfo info;
            info.name = Identifier(name);
            info.path = path;
            info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
            info.options["type"] = Value("differential");
            info.options[TIME_TRAVEL_OPTION] = Value::BOOLEAN(true);
            AttachOptions options(info.options, AccessMode::READ_ONLY);
            options.visibility = AttachVisibility::HIDDEN;
            DatabaseManager::Get(context).AttachDatabase(context, info, options);
        });
        {
            std::lock_guard<std::mutex> guard(mutex);
            if (stopped)
                retired.push_back({name, image_id});
            else {
                states.push_back({name, image_id});
                ++status.changes;
                while (states.size() > KEEP_STATES) {
                    retired.push_back(states.front());
                    states.erase(states.begin());
                }
            }
        }
        // Detaching only unlists: a transaction that reads a retired state keeps it until it ends.
        for (auto &state : retired)
            if (!state.name.empty())
                context.RunFunctionInTransaction([&] {
                    DatabaseManager::Get(context).DetachDatabase(context, Identifier(state.name), OnEntryNotFound::RETURN_NULL);
                });
    } catch (const std::exception &error) {
        std::lock_guard<std::mutex> guard(mutex);
        status.error = error.what();
        retired.push_back({"", image_id});
    }
    for (auto &state : retired)
        if (!state.image.empty() && state.image != image_id)
            differential::ReleaseFollowImage(state.image);
    return true;
}
shared_ptr<AttachedDatabase> Follower::Choose(ClientContext &context) {
    auto choice = context.registered_state->GetOrCreate<FollowChoice>("differential_follow");
    auto &manager = DatabaseManager::Get(context);
    auto found = choice->chosen.find(this);
    if (found != choice->chosen.end() && !found->second.empty()) {
        auto attached = MetaTransaction::Get(context).GetReferencedDatabaseOwning(Identifier(found->second));
        if (!attached)
            attached = manager.GetDatabase(Identifier(found->second));
        if (attached)
            return attached;
    } else if (found != choice->chosen.end())
        return nullptr; // this transaction reads the alias's own state
    std::string name;
    {
        std::lock_guard<std::mutex> guard(mutex);
        name = states.empty() ? "" : states.back().name;
    }
    choice->chosen[this] = name;
    return name.empty() ? nullptr : manager.GetDatabase(Identifier(name));
}
void Follower::Stop(ClientContext &context) {
    std::vector<State> closing;
    {
        std::lock_guard<std::mutex> guard(mutex);
        stopped = true;
        closing.swap(states);
    }
    for (auto &state : closing) {
        if (!state.name.empty())
            DatabaseManager::Get(context).DetachDatabase(context, Identifier(state.name), OnEntryNotFound::RETURN_NULL);
        if (!state.image.empty())
            differential::ReleaseFollowImage(state.image);
    }
}
bool Follower::Stopped() {
    std::lock_guard<std::mutex> guard(mutex);
    return stopped;
}
Follower::Status Follower::GetStatus() {
    std::lock_guard<std::mutex> guard(mutex);
    return status;
}
void Follow(const std::shared_ptr<Follower> &follower) {
    std::lock_guard<std::mutex> lock(poller_mutex);
    followers.push_back(follower);
    if (!poller_running) {
        if (poller.joinable())
            poller.join();
        poller_running = true;
        poller = std::thread(Poll);
        static struct Joiner {
            ~Joiner() {
                {
                    std::lock_guard<std::mutex> guard(poller_mutex);
                    poller_running = false;
                }
                poller_signal.notify_all();
                if (poller.joinable())
                    poller.join();
            }
        } joiner;
    }
    poller_signal.notify_all();
}
std::vector<Follower::Status> Followers() {
    std::vector<std::shared_ptr<Follower>> live;
    {
        std::lock_guard<std::mutex> lock(poller_mutex);
        for (auto &weak : followers)
            if (auto follower = weak.lock())
                live.push_back(follower);
    }
    std::vector<Follower::Status> result;
    for (auto &follower : live)
        if (!follower->Stopped())
            result.push_back(follower->GetStatus());
    return result;
}
#undef poller_mutex
#undef poller_signal
#undef followers
#undef poller
#undef poller_running
} // namespace duckdb
