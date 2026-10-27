#include "duckdb/common/file_system.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "differential_store.hpp"
#include "differential_control.hpp"
#include "differential_hooks.hpp"
#include "differential_repository.hpp"
#include "differential_export.hpp"
#include "differential_shipping.hpp"
#include "differential_recovery.hpp"
#include "remote_cache.hpp"
#include "spill_policy.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_opener.hpp"
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <filesystem>
#include <memory>
#include <cerrno>
#include <map>

namespace duckdb {
static_assert(differential::Store::HEADER == 3 * Storage::FILE_HEADER_SIZE,
              "Differential chunk geometry must match DuckDB's data-block start");
void ProbeEvent(const char *, const string &, int64_t = 0, idx_t = 0);
namespace {
// Leases whose buffered writes could not be published at close. Holding them keeps other
// processes from taking the branch until this process publishes or dies (journal replay).
std::mutex retained_mutex;
std::vector<std::shared_ptr<differential::BranchLock>> retained_leases;
struct DurableHandle : FileHandle {
    std::shared_ptr<differential::Store> store;
    std::shared_ptr<differential::Control> control;
    int64_t inode = -1;
    string root, pool;
    std::shared_ptr<differential::RemoteSnapshot> remote;
    // A follower's image of a shipped state (diff://follow/<id>[.suffix]), read-only.
    std::shared_ptr<const differential::FollowImage> follow;
    string follow_suffix;
    uint64_t root_size = 0;
    std::shared_ptr<differential::Pin> pin;
    std::shared_ptr<differential::MergeWriteBudget> merge_budget;
    uint64_t position = 0;
    int lock_fd = -1;
    bool closed = false;
    bool main = false;          // managed main database file
    bool header_written = false; // a native header write awaits its sync
    bool maintain = true;       // bounded background collection after checkpoints
    bool snapshot_on_checkpoint = false; // publish each completed checkpoint as a public snapshot
    bool shared = false;  // a shared database's writer: checkpoints are snapshots published to S3
    bool created = false; // this open created the file: its first header sync creates the database
    bool announced = false; // a writer marker of this session is in S3
    // Live files: caches for remote-only objects (a shared writer's base snapshot), and whether
    // reading ahead is worth it (something keeps what is read ahead).
    differential::RemoteCaching caching;
    bool remote_caches = false;
    // The memory cache this handle retains (released at close).
    std::shared_ptr<differential::RemoteMemoryCache> cache_user;
    std::shared_ptr<differential::BranchLock> branch_lock;
    DurableHandle(FileSystem &fs, string path, FileOpenFlags flags, std::shared_ptr<differential::Store> store,
                  std::shared_ptr<differential::Control> control)
        : FileHandle(fs, std::move(path), flags), store(std::move(store)), control(std::move(control)) {}
    ~DurableHandle() override { Close(); }
    void Close() override {
        if (closed)
            return;
        closed = true;
        if (inode >= 0 && flags.OpenForWriting()) {
            // Native close does not sync. Publish the file group's buffered bytes without a sync so
            // another process that takes the released branch observes them.
            bool flushed = false;
            try {
                store->Flush(inode);
                flushed = true;
            } catch (...) {
                if (branch_lock) {
                    std::lock_guard<std::mutex> guard(retained_mutex);
                    retained_leases.push_back(branch_lock);
                }
            }
            // PUBLISH_ON_DETACH: the final checkpoint is done; publish it while the branch is still ours.
            if (flushed && shared && main && branch_lock &&
                differential::GetSharedWriting(*control, branch_lock->branch.id).publish_on_detach)
                differential::PublishOnClose(*control, branch_lock->branch.database, branch_lock->branch.id);
            // SHIP_WAL: ship what is left while the branch is still ours (errors are recorded).
            if (flushed && main && branch_lock && !shared)
                differential::ShipOnClose(*control, branch_lock->branch.id);
        }
        // Also when the open failed after announcing (no inode): the marker would otherwise be
        // renewed for the life of the process.
        if (announced)
            differential::WithdrawWriter(*control, branch_lock->branch.id);
        pin.reset();
        remote.reset();
        follow.reset();
        branch_lock.reset();
        if (cache_user) {
            cache_user->Release();
            cache_user.reset();
        }
        if (lock_fd >= 0) {
            close(lock_fd);
            lock_fd = -1;
        }
    }
};
class DurableFileSystem : public FileSystem {
    std::shared_ptr<differential::Repository> repository;
    std::shared_ptr<differential::RemoteMemoryCache> memory_cache; // this instance's external file cache
    std::once_flag initialized;
    std::mutex owners_mutex;
    std::map<string, std::weak_ptr<differential::BranchLock>> owners;
    std::shared_ptr<differential::Store> store;
    std::shared_ptr<differential::Control> control;
    static DurableHandle &H(FileHandle &h) {
        auto &handle = static_cast<DurableHandle &>(h);
        if (handle.closed)
            throw IOException("Differential file handle is closed");
        return handle;
    }
    static bool Probes() { return differential::Hook("DIFFERENTIAL_ENABLE_PROBES") != nullptr; }
    // The store's context for remote-only objects of a live file: its caches (enabling read-ahead),
    // or none when nothing would keep data read ahead.
    static const void *Context(DurableHandle &handle) { return handle.remote_caches ? &handle.caching : nullptr; }
    string Name(const string &uri) {
        if (uri.rfind("diff://", 0) != 0)
            throw IOException("Invalid differential URI");
        auto name = uri.substr(7);
        if (name.rfind("managed/", 0) == 0 || name.rfind("imports/", 0) == 0 || name.rfind("rehydrate/", 0) == 0)
            throw IOException("Internal branch filenames are not attachable URIs");
        if (name.empty() || name[0] == '/' || name.find("..") != string::npos ||
            name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-/.") !=
                string::npos)
            throw IOException("Invalid differential URI");
        return name;
    }
    bool IsRoot(const string &uri) { return Probes() && uri.rfind("diff://roots/", 0) == 0; }
    // diff://follow/<image id>[.suffix]: a follower's image, served from memory and S3.
    static bool Follow(const string &uri, string &id, string &suffix) {
        if (uri.rfind("diff://follow/", 0) != 0)
            return false;
        id = uri.substr(14);
        auto dot = id.find('.');
        suffix = dot == string::npos ? "" : id.substr(dot);
        id = id.substr(0, dot);
        if (id.empty() || id.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789") != string::npos)
            throw IOException("Invalid differential follower URI");
        return true;
    }
    std::shared_ptr<differential::BranchLock> Owner(const string &branch) {
        std::lock_guard<std::mutex> guard(owners_mutex);
        auto found = owners.find(branch);
        return found == owners.end() ? nullptr : found->second.lock();
    }
    differential::ResolvedPath Resolve(const string &uri) {
        if (!Probes() && uri.find("/branches/") == string::npos && uri.find("/snapshots/") == string::npos &&
            uri.find("/snapshot-names/") == string::npos && uri.rfind("diff://export/", 0) != 0 &&
            uri.rfind("diff://merge_stage/", 0) != 0 && uri.rfind("diff://merge_input/", 0) != 0)
            throw IOException("Expected a managed database branch or public snapshot URI");
        std::call_once(initialized, [&] {
            control = repository->Get();
            store = control->store;
        });
        auto path = control->Resolve(uri);
        if (!path.managed && !path.merge_stage && !Probes())
            throw IOException("Expected a managed database branch or public snapshot URI");
        return path;
    }

  public:
    DurableFileSystem(std::shared_ptr<differential::Repository> repository,
                      std::shared_ptr<differential::RemoteMemoryCache> memory_cache)
        : repository(std::move(repository)), memory_cache(std::move(memory_cache)) {}
    string GetName() const override { return "differential_durable"; }
    bool CanHandleFile(const string &uri) override { return uri.rfind("diff:", 0) == 0; }
    bool CanSeek() override { return true; }
    // DuckDB's REMOTE_ONLY block prefetch uses this classification. Only export
    // handles are wholly remote; local files and mutable overlays retain local behavior.
    bool OnDiskFile(FileHandle &h) override { return !H(h).remote && !H(h).follow; }
    bool IsPathAbsolute(const string &) override { return true; }
    string PathSeparator(const string &) override { return "/"; }
    string CanonicalizePath(const string &uri, optional_ptr<FileOpener> = nullptr) override {
        string id, suffix;
        if (Follow(uri, id, suffix))
            return uri;
        Name(uri);
        return Resolve(uri).canonical;
    }
    unique_ptr<FileHandle> OpenFile(const string &uri, FileOpenFlags flags,
                                    optional_ptr<FileOpener> opener = nullptr) override {
        string follow_id, follow_suffix;
        if (Follow(uri, follow_id, follow_suffix)) {
            auto image = differential::FindFollowImage(follow_id);
            if (!image || !image->Exists(follow_suffix)) {
                if (flags.ReturnNullIfNotExists())
                    return nullptr;
                throw IOException("[differential:not_found] Follower file does not exist");
            }
            if (flags.OpenForWriting())
                throw IOException("[differential:read_only_attachment] A follower is read-only");
            auto h = make_uniq<DurableHandle>(*this, uri, flags, store, control);
            h->follow = image;
            h->follow_suffix = follow_suffix;
            return std::move(h);
        }
        auto name = Name(uri);
        auto resolved = Resolve(uri);
        if (resolved.managed && !resolved.historical && !resolved.remote)
            name = resolved.file;
        ProbeEvent("open", uri, flags.GetFlagsInternal(), static_cast<idx_t>(flags.Lock()));
        auto h = make_uniq<DurableHandle>(*this, uri, flags, store, control);
        h->merge_budget = resolved.merge_budget;
        Value setting;
        if (FileOpener::TryGetCurrentSetting(opener, "differential_auto_gc", setting) && !setting.IsNull())
            h->maintain = setting.GetValue<bool>();
        if (FileOpener::TryGetCurrentSetting(opener, "differential_snapshot_on_checkpoint", setting) &&
            !setting.IsNull())
            h->snapshot_on_checkpoint = setting.GetValue<bool>();
        if (resolved.remote) {
            if (!resolved.suffix.empty()) {
                if (flags.ReturnNullIfNotExists())
                    return nullptr;
                throw IOException("Export sidecar does not exist");
            }
            if (flags.OpenForWriting())
                throw IOException("[differential:read_only_attachment] Remote export is read-only");
            differential::RemoteCaching caching;
            caching.memory = memory_cache;
            caching.disk_limit = RemoteDiskCacheLimit(opener);
            if (memory_cache) {
                memory_cache->Retain();
                h->cache_user = memory_cache;
            }
            h->remote = std::make_shared<differential::RemoteSnapshot>(*control, resolved.provider, resolved.descriptor,
                                                                       std::move(caching));
            return std::move(h);
        }
        if (IsRoot(uri) || resolved.historical) {
            if (name.find('.') != string::npos || !resolved.suffix.empty()) {
                if (flags.ReturnNullIfNotExists())
                    return nullptr;
                throw IOException("Snapshot sidecar does not exist");
            }
            if (flags.OpenForWriting())
                throw IOException("[differential:read_only_attachment] Immutable root is read-only");
            h->pin = resolved.merge_input ? control->NewPin(resolved.root)
                     : resolved.historical ? control->NewPin("", resolved.snapshot.database, resolved.snapshot.id)
                                         : control->NewPin(name.substr(6));
            h->root = h->pin->root;
            if (resolved.historical && !resolved.merge_input)
                h->pool = control->LocalDatabasePool(resolved.snapshot.database);
            h->root_size = store->RootSize(h->root);
            return std::move(h);
        }
        if (flags.Lock() == FileLockType::READ_LOCK)
            throw IOException("[differential:read_only_attachment] Independent readers must attach an "
                              "immutable root, not a live file");
        if (resolved.managed && resolved.suffix.empty() && flags.OpenForWriting() &&
            flags.Lock() != FileLockType::WRITE_LOCK)
            throw IOException("Managed main-file writes require a native writer lock");
        if (resolved.managed && !resolved.suffix.empty()) {
            h->branch_lock = Owner(resolved.branch.id);
            if (flags.OpenForWriting() && !h->branch_lock)
                throw IOException("Managed sidecar writes require branch ownership");
        }
        if (flags.Lock() == FileLockType::WRITE_LOCK) {
            if (resolved.managed && resolved.suffix.empty()) {
                auto context = FileOpener::TryGetClientContext(opener);
                auto recovery =
                    context ? context->registered_state->Get<DifferentialRecoveryState>("differential_recovery")
                            : nullptr;
                bool recovering = recovery && recovery->lease->branch.id == resolved.branch.id;
                h->branch_lock = recovering ? recovery->lease : control->Acquire(resolved.branch);
                // Management captures (restore, conflict resolution) publish nothing themselves.
                h->shared = !recovering && control->Writers(resolved.branch.database) == "shared";
                if (h->shared) {
                    // Advisory "being written" marker: other live writers warn (or refuse, ON_BUSY 'error').
                    auto on_busy = differential::GetSharedWriting(*control, resolved.branch.id).on_busy;
                    std::vector<differential::WriterMarker> others;
                    try {
                        others = differential::AnnounceWriter(control, h->branch_lock->branch, on_busy);
                    } catch (const std::exception &error) {
                        throw IOException("%s", error.what());
                    }
                    h->announced = true;
                    if (!others.empty() && on_busy == "warn")
                        Printer::Print("Warning: " + differential::DescribeWriters(others) +
                                       " Concurrent updates of the branch are detected when they are published "
                                       "(branch_conflict).");
                }
                std::lock_guard<std::mutex> guard(owners_mutex);
                for (auto it = owners.begin(); it != owners.end();) {
                    if (it->second.expired())
                        it = owners.erase(it);
                    else
                        ++it;
                }
                owners[resolved.branch.id] = h->branch_lock;
            } else {
                auto lockpath = std::filesystem::path(store->directory) / "locks";
                store->permissions.CreateDirectories(lockpath);
                // URI encoding admits only safe components. Separate main/WAL locks mirror native opens.
                string lockname = name;
                for (auto &c : lockname)
                    if (c == '/')
                        c = '_';
                h->lock_fd = store->permissions.Open(lockpath / lockname, O_RDWR | O_CLOEXEC);
                if (h->lock_fd < 0 || flock(h->lock_fd, LOCK_EX | LOCK_NB) != 0)
                    throw IOException("[differential:writer_busy] differential writer busy: cannot acquire file lock");
            }
        }
        if (!store->Exists(name) && flags.ReturnNullIfNotExists())
            return nullptr;
        h->created = !store->Exists(name);
        if (store->Exists(name) && flags.ReturnNullIfExists())
            return nullptr;
        // Live files of shared databases may read their base snapshot from S3, through this instance's
        // memory cache and the repository disk cache.
        if (resolved.managed) {
            h->caching.memory = memory_cache;
            h->caching.disk_limit = RemoteDiskCacheLimit(opener);
            if (memory_cache) {
                memory_cache->Retain();
                h->cache_user = memory_cache;
            }
            h->remote_caches = h->caching.disk_limit || (memory_cache && memory_cache->Enabled());
            differential::SetRemoteObjectCacheLimit(h->caching.disk_limit);
        }
        h->pin = control->NewPin();
        h->inode = store
                       ->Open(name, flags.CreateFileIfNotExists() || flags.OverwriteExistingFile(),
                              flags.ExclusiveCreate(), flags.OverwriteExistingFile(), h->pin->id,
                              resolved.managed ? control->LocalDatabasePool(resolved.branch.database)
                                               : resolved.merge_pool)
                       .id;
        h->main = resolved.managed ? resolved.suffix.empty() : name.find('.') == string::npos;
        if (flags.OpenForAppending())
            h->position = store->Size(h->inode);
        return std::move(h);
    }
    int64_t GetFileSize(FileHandle &h) override {
        if (H(h).remote)
            return int64_t(H(h).remote->descriptor.size);
        if (H(h).follow)
            return int64_t(H(h).follow->Size(H(h).follow_suffix));
        return int64_t(H(h).root.empty() ? store->Size(H(h).inode) : H(h).root_size);
    }
    FileType GetFileType(FileHandle &) override { return FileType::FILE_TYPE_REGULAR; }
    timestamp_t GetLastModifiedTime(FileHandle &) override { return timestamp_t(0); }
    void Read(FileHandle &h, void *b, int64_t n, idx_t p) override {
        if (n < 0)
            throw IOException("Negative read");
        ProbeEvent("pread", h.path, n, p);
        auto count = H(h).follow         ? H(h).follow->Read(H(h).follow_suffix, b, uint64_t(n), p)
                     : H(h).remote         ? H(h).remote->Read(b, uint64_t(n), p)
                     : H(h).root.empty() ? store->Read(H(h).inode, b, uint64_t(n), p, Context(H(h)))
                                         : store->ReadRoot(H(h).root, b, uint64_t(n), p, H(h).pool);
        if (count != uint64_t(n))
            throw IOException("Short read from differential file");
    }
    int64_t Read(FileHandle &h, void *b, int64_t n) override {
        if (n < 0)
            throw IOException("Negative read");
        ProbeEvent("read", h.path, n, H(h).position);
        auto count = H(h).follow         ? H(h).follow->Read(H(h).follow_suffix, b, uint64_t(n), H(h).position)
                     : H(h).remote         ? H(h).remote->Read(b, uint64_t(n), H(h).position)
                     : H(h).root.empty() ? store->Read(H(h).inode, b, uint64_t(n), H(h).position, Context(H(h)))
                                         : store->ReadRoot(H(h).root, b, uint64_t(n), H(h).position, H(h).pool);
        H(h).position += count;
        return int64_t(count);
    }
    void Write(FileHandle &h, void *b, int64_t n, idx_t p) override {
        if (n < 0 || !h.flags.OpenForWriting() || !H(h).root.empty() || H(h).remote || H(h).follow)
            throw IOException("[differential:read_only_attachment] Invalid write or read-only root");
        ProbeEvent("pwrite", h.path, n, p);
        if (H(h).merge_budget) {
            H(h).merge_budget->Reserve(uint64_t(n));
            if (auto fault = differential::Hook("DIFFERENTIAL_MERGE_IO_FAULT"))
                if (string(fault) == "write")
                    throw IOException("[differential:merge_io] Injected staging write failure");
        }
        store->Write(H(h).inode, b, uint64_t(n), p);
        // DuckDB writes its alternating database headers after syncing all checkpoint blocks;
        // the sync that follows a header write completes a checkpoint (internal generation).
        constexpr auto header = Storage::FILE_HEADER_SIZE;
        if (H(h).main && n > 0 && p < 3 * header && p + idx_t(n) > header)
            H(h).header_written = true;
        ProbeEvent("pwrite.end", h.path, n, p);
    }
    int64_t Write(FileHandle &h, void *b, int64_t n) override {
        Write(h, b, n, H(h).position);
        H(h).position += uint64_t(n);
        return n;
    }
    void FileSync(FileHandle &h) override {
        auto &handle = H(h);
        ProbeEvent("sync.begin", h.path);
        if (handle.merge_budget)
            if (auto fault = differential::Hook("DIFFERENTIAL_MERGE_IO_FAULT"))
                if (string(fault) == "sync")
                    throw IOException("[differential:merge_io] Injected staging sync failure");
        if (handle.inode >= 0 && h.flags.OpenForWriting()) {
            bool checkpoint = handle.header_written;
            if (handle.header_written) {
                // Durably publish every buffered write and seal the completed checkpoint image
                // as the file's latest internal generation, in one authority transaction.
                auto root = store->Seal(handle.inode);
                handle.header_written = false;
                auto &lease = handle.branch_lock;
                // Shared: not for the sync that creates the database, nor during an explicit snapshot.
                bool publish = handle.shared && lease && !handle.created &&
                               differential::GetSharedWriting(*control, lease->branch.id).publish_on_checkpoint &&
                               !differential::ExplicitSnapshotInProgress(*control, lease->branch.id);
                handle.created = false;
                if ((handle.snapshot_on_checkpoint || publish) && handle.main && lease) {
                    // A shared database's checkpoint is a snapshot, published to S3 in the background.
                    auto published = control->CheckpointSnapshot(lease->branch, root);
                    if (publish && published.snapshot)
                        differential::SchedulePublication(control, lease->branch.database, lease->branch.id);
                }
                if (handle.maintain)
                    control->ScheduleMaintenance();
            } else
                store->Sync(handle.inode); // this file's group: other databases keep writing
            // SHIP_WAL: async schedules shipping; sync returns once the state is in S3.
            if (handle.branch_lock && !handle.shared) {
                try {
                    differential::AfterSync(*control, handle.branch_lock->branch.id, checkpoint);
                } catch (const std::exception &error) {
                    throw IOException("%s", error.what());
                }
            }
        }
        ProbeEvent("sync.end", h.path);
    }
    void Truncate(FileHandle &h, int64_t n) override {
        if (n < 0 || !h.flags.OpenForWriting() || !H(h).root.empty() || H(h).remote || H(h).follow)
            throw IOException("[differential:read_only_attachment] Invalid truncate or read-only root");
        ProbeEvent("truncate", h.path, n);
        if (H(h).merge_budget) {
            auto old_size = GetFileSize(h);
            if (n > old_size)
                H(h).merge_budget->Reserve(uint64_t(n - old_size));
        }
        store->Truncate(H(h).inode, uint64_t(n));
    }
    bool Trim(FileHandle &, idx_t, idx_t) override { return false; } // Native hint; preserve all stored bytes.
    void Seek(FileHandle &h, idx_t p) override { H(h).position = p; }
    idx_t SeekPosition(FileHandle &h) override { return H(h).position; }
    void Reset(FileHandle &h) override { H(h).position = 0; }
    bool FileExists(const string &uri, optional_ptr<FileOpener> = nullptr) override {
        string follow_id, follow_suffix;
        if (Follow(uri, follow_id, follow_suffix)) {
            auto image = differential::FindFollowImage(follow_id);
            return image && image->Exists(follow_suffix);
        }
        auto name = Name(uri);
        auto resolved = Resolve(uri);
        ProbeEvent("exists", uri);
        if (resolved.historical || resolved.remote)
            return resolved.suffix.empty();
        if (resolved.managed)
            return store->Exists(resolved.file);
        if (IsRoot(uri))
            return name.find('.') == string::npos && store->HasRoot(name.substr(6));
        return store->Exists(name);
    }
    bool IsPipe(const string &, optional_ptr<FileOpener> = nullptr) override { return false; }
    bool DirectoryExists(const string &, optional_ptr<FileOpener> = nullptr) override { return false; }
    void CreateDirectory(const string &path, optional_ptr<FileOpener> = nullptr) override {
        ValidateDifferentialTempDirectory(path);
        throw IOException("Differential does not support directory creation");
    }
    void MoveFile(const string &a, const string &b, optional_ptr<FileOpener> = nullptr) override {
        string id, suffix;
        if (Follow(a, id, suffix) || Follow(b, id, suffix))
            throw IOException("[differential:read_only_attachment] A follower is read-only");
        auto source = Resolve(a);
        auto target = Resolve(b);
        if (IsRoot(a) || IsRoot(b) || source.historical || target.historical || source.remote || target.remote)
            throw IOException("[differential:read_only_attachment] Immutable root namespace");
        if (source.managed != target.managed || (source.managed && source.branch.id != target.branch.id))
            throw IOException("Cannot move between branch namespaces");
        ProbeEvent("move.from", a);
        ProbeEvent("move.to", b);
        store->Rename(source.managed ? source.file : Name(a), target.managed ? target.file : Name(b));
    }
    bool TryRemoveFile(const string &uri, optional_ptr<FileOpener> = nullptr) override {
        string id, suffix;
        if (Follow(uri, id, suffix))
            throw IOException("[differential:read_only_attachment] A follower is read-only");
        auto name = Name(uri);
        auto resolved = Resolve(uri);
        ProbeEvent("try_remove", uri);
        if (IsRoot(uri) || resolved.historical || resolved.remote)
            throw IOException("[differential:read_only_attachment] Immutable root namespace");
        return store->Remove(resolved.managed ? resolved.file : name);
    }
    void RemoveFile(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
        if (!TryRemoveFile(uri, opener))
            throw IOException("[differential:not_found] Differential file not found");
    }
};
} // namespace
std::shared_ptr<differential::Repository> RegisterDurableFileSystem(ExtensionLoader &loader) {
    const char *configured = std::getenv("DIFFERENTIAL_STORE_ROOT");
    if (!configured && differential::Hook("DIFFERENTIAL_PROBE_ROOT"))
        return nullptr;
    string root;
    if (configured)
        root = configured;
    else {
        auto home = std::getenv("HOME");
        if (!home)
            throw InvalidInputException("Set DIFFERENTIAL_STORE_ROOT to an absolute local repository path");
        root = (std::filesystem::path(home) / ".duckdb" / "differential").string();
    }
    if (!std::filesystem::path(root).is_absolute())
        throw InvalidInputException("Repository root must be absolute");
    auto repository = std::make_shared<differential::Repository>(root);
    repository->on_open = differential::InstallRemoteFetch;
    auto &db = loader.GetDatabaseInstance();
    FileSystem::GetFileSystem(db).RegisterSubSystem(make_uniq<DurableFileSystem>(repository, ExternalFileMemoryCache(db)));
    return repository;
}
} // namespace duckdb
