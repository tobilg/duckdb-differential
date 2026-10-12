// Reader pins, incremental garbage collection, compaction and storage statistics.
#include "control_internal.hpp"
#include "differential_local.hpp"
#include "differential_metrics.hpp"
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace differential {
using namespace control;

struct MetadataCursor::Impl {
    sqlite3 *db = nullptr;
    std::unique_ptr<Query> query;
    bool finished = false;
    ~Impl() {
        query.reset();
        if (db)
            sqlite3_close_v2(db);
    }
};
MetadataCursor::MetadataCursor(const std::string &directory, const std::string &sql,
                               const std::vector<std::string> &parameters)
    : impl(std::make_unique<Impl>()) {
    if (sqlite3_open_v2((std::filesystem::path(directory) / "engine.sqlite").c_str(), &impl->db,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nullptr) != SQLITE_OK)
        throw std::runtime_error("Cannot open metadata listing cursor");
    sqlite3_busy_timeout(impl->db, 10000);
    Execute(impl->db, "PRAGMA cache_size=-1024; PRAGMA temp_store=FILE; BEGIN;");
    impl->query = std::make_unique<Query>(impl->db, sql.c_str());
    for (size_t i = 0; i < parameters.size(); i++)
        impl->query->Text(int(i + 1), parameters[i]);
}
MetadataCursor::~MetadataCursor() = default;
bool MetadataCursor::Next(std::vector<MetadataCell> &row) {
    if (impl->finished)
        return false;
    if (!impl->query->Step()) {
        impl->finished = true;
        return false;
    }
    auto q = impl->query->s;
    row.clear();
    for (int i = 0; i < sqlite3_column_count(q); i++) {
        MetadataCell value;
        value.null = sqlite3_column_type(q, i) == SQLITE_NULL;
        if (!value.null) {
            value.number = sqlite3_column_int64(q, i);
            value.text = impl->query->Text(i);
        }
        row.push_back(std::move(value));
    }
    return true;
}

Pin::~Pin() {
    Timer timer(Metric::PinUpdate);
    bool removed = false;
    if (store && !id.empty()) {
        try {
            // Pins express process liveness (their lock file is authoritative after a crash),
            // so release is published without a device flush.
            std::lock_guard<std::recursive_mutex> guard(store->mutex);
            Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
            Query(store->db, "DELETE FROM pins WHERE id=?").Text(1, id).Run();
            tx.Commit();
            removed = true;
        } catch (...) { /* Keep a durable conservative pin for later stale-owner reconciliation. */
        }
    }
    if (fd >= 0)
        close(fd);
    if (removed) {
        std::error_code error;
        std::filesystem::remove(std::filesystem::path(store->directory) / "pins" / id, error);
    }
}
std::shared_ptr<Pin> Control::NewPin(const std::string &root, const std::string &database, int64_t snapshot) {
    Timer timer(Metric::PinUpdate);
    auto pin = std::make_shared<Pin>();
    pin->store = store;
    pin->id = UUID();
    auto path = std::filesystem::path(store->directory) / "pins";
    store->permissions.CreateDirectories(path);
    pin->fd = store->permissions.Open(path / pin->id, O_EXCL | O_RDWR | O_CLOEXEC);
    if (pin->fd < 0 || flock(pin->fd, LOCK_EX | LOCK_NB)) {
        Count(Counter::PinConflicts);
        throw std::runtime_error("[differential:pin_conflict] Cannot acquire reader pin");
    }
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Visible to every process at commit; durability across power loss is unnecessary because
    // the pinning process does not survive it either.
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    pin->root = database.empty() ? root : GetSnapshot(database, snapshot).root;
    if (!pin->root.empty())
        store->RootSizeTx(pin->root);
    Query(store->db, "INSERT INTO pins(id,root) VALUES(?,NULLIF(?,''))").Text(1, pin->id).Text(2, pin->root).Run();
    tx.Commit();
    return pin;
}
void Control::RetainRootPermanently(const std::string &root) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    store->RootSizeTx(root);
    Query(store->db, "INSERT OR IGNORE INTO pins(id,root) VALUES(?,?)").Text(1, UUID()).Text(2, root).Run();
    tx.Commit();
}
// A pin whose lock file can be locked belongs to a dead process. A missing pin file is
// treated as live: damaged metadata never becomes a reason to release data.
void Control::ReconcilePins(Collection &result, int64_t limit, bool apply) {
    std::vector<std::string> ids, stale;
    auto collect = [&](sqlite3 *db) {
        Query pins(db, "SELECT id FROM pins");
        while (pins.Step())
            ids.push_back(pins.Text(0));
    };
    if (apply)
        collect(store->db);
    else
        store->WithReader([&](Reader &reader) { collect(reader.db); });
    for (auto &id : ids) {
        if (!IsUUID(id))
            throw std::runtime_error("[differential:corrupt_data] Corrupt pin identity");
        auto path = std::filesystem::path(store->directory) / "pins" / id;
        int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
        if (fd >= 0 && flock(fd, LOCK_EX | LOCK_NB) == 0) {
            ++result.stale_pins;
            if (stale.size() < uint64_t(limit))
                stale.push_back(id);
        } else
            ++result.live_pins;
        if (fd >= 0)
            close(fd);
    }
    if (!apply)
        return;
    for (auto &id : stale)
        Query(store->db, "DELETE FROM pins WHERE id=?").Text(1, id).Run();
    result.released_pins = std::move(stale);
}
namespace {
void RemovePinFiles(const std::string &directory, const std::vector<std::string> &ids) {
    for (auto &id : ids) {
        std::error_code error;
        std::filesystem::remove(std::filesystem::path(directory) / "pins" / id, error);
    }
}
} // namespace
// One bounded pass over the collector queues; triggers keep them complete. Each examined
// candidate and each deleted mapping row costs one unit of `budget`. Cascades (file ->
// root -> page -> object) continue within the same call while budget remains.
int64_t Control::CollectStep(int64_t budget, int64_t &deleted) {
    auto db = store->db;
    int64_t work = 0;
    auto ids = [&](const char *sql) {
        std::vector<std::string> out;
        Query q(db, sql);
        q.Int(1, std::max<int64_t>(1, budget - work));
        while (q.Step())
            out.push_back(q.Text(0));
        return out;
    };
    auto exists = [&](const char *sql, const std::string &value) {
        Query q(db, sql);
        q.Text(1, value);
        return q.Step() && q.Int(0) != 0;
    };
    bool progress = true;
    while (work < budget && progress) {
        progress = false;
        for (auto &text : ids("SELECT id FROM gc_files LIMIT ?")) {
            if (work >= budget)
                break;
            ++work;
            auto id = std::stoll(text);
            Query live(db, "SELECT name IS NOT NULL OR EXISTS(SELECT 1 FROM pins p WHERE p.file=f.id) FROM files f WHERE "
                           "id=?");
            live.Int(1, id);
            bool found = live.Step();
            if (found && live.Int(0) != 0) {
                Query(db, "DELETE FROM gc_files WHERE id=?").Int(1, id).Run();
                continue;
            }
            if (found) {
                work += Query(db, "DELETE FROM extents WHERE (file,chunk) IN (SELECT file,chunk FROM extents WHERE "
                                  "file=? LIMIT ?)")
                            .Int(1, id)
                            .Int(2, std::max<int64_t>(1, budget - work))
                            .Run();
                progress = true;
                Query more(db, "SELECT 1 FROM extents WHERE file=? LIMIT 1");
                more.Int(1, id);
                if (more.Step())
                    continue;
                Query(db, "DELETE FROM generations WHERE file=?").Int(1, id).Run();
                Query(db, "DELETE FROM file_pages WHERE file=?").Int(1, id).Run();
                Query(db, "DELETE FROM file_pools WHERE file=?").Int(1, id).Run();
                Query(db, "DELETE FROM files WHERE id=?").Int(1, id).Run();
            }
            Query(db, "DELETE FROM gc_files WHERE id=?").Int(1, id).Run();
        }
        for (auto &root : ids("SELECT hash FROM gc_roots LIMIT ?")) {
            if (work >= budget)
                break;
            ++work;
            if (exists(ROOT_LIVE_SQL, root) || !exists("SELECT count(*) FROM roots WHERE hash=?", root)) {
                Query(db, "DELETE FROM gc_roots WHERE hash=?").Text(1, root).Run();
                continue;
            }
            // Reclaimed roots stop resolving immediately; their maps are removed in bounded batches.
            Query(db, "INSERT OR IGNORE INTO reclaimed_roots VALUES(?)").Text(1, root).Run();
            work += Query(db, "DELETE FROM root_pages WHERE (root,page_no) IN (SELECT root,page_no FROM root_pages "
                              "WHERE root=? LIMIT ?)")
                        .Text(1, root)
                        .Int(2, std::max<int64_t>(1, budget - work))
                        .Run();
            progress = true;
            if (!exists("SELECT EXISTS(SELECT 1 FROM root_pages WHERE root=?1)", root))
                Query(db, "DELETE FROM gc_roots WHERE hash=?").Text(1, root).Run();
        }
        for (auto &page : ids("SELECT hash FROM gc_pages LIMIT ?")) {
            if (work >= budget)
                break;
            ++work;
            if (!exists("SELECT EXISTS(SELECT 1 FROM root_pages WHERE page=?1) OR EXISTS(SELECT 1 FROM file_pages "
                        "WHERE page=?1)",
                        page)) {
                work += Query(db, "DELETE FROM page_entries WHERE page=?").Text(1, page).Run();
                Query(db, "DELETE FROM manifest_pages WHERE hash=?").Text(1, page).Run();
                progress = true;
            }
            Query(db, "DELETE FROM gc_pages WHERE hash=?").Text(1, page).Run();
        }
        for (auto &object : ids("SELECT hash FROM gc_objects LIMIT ?")) {
            if (work >= budget)
                break;
            ++work;
            if (exists("SELECT EXISTS(SELECT 1 FROM extents WHERE object=?1) OR EXISTS(SELECT 1 FROM page_entries "
                       "WHERE object=?1)",
                       object)) {
                Query(db, "DELETE FROM gc_objects WHERE hash=?").Text(1, object).Run();
                continue;
            }
            // The delete trigger queues external replicas and removes the queue entry.
            if (Query(db, "DELETE FROM objects WHERE hash=?").Text(1, object).Run())
                ++deleted;
            else
                Query(db, "DELETE FROM gc_objects WHERE hash=?").Text(1, object).Run();
            progress = true;
        }
    }
    Query remaining(db, "SELECT (SELECT count(*) FROM gc_files)+(SELECT count(*) FROM gc_roots)+(SELECT count(*) FROM "
                        "gc_pages)+(SELECT count(*) FROM gc_objects)");
    remaining.Step();
    return remaining.Int(0);
}
// Exact whole-repository estimate on a read snapshot: no authority write lock and no
// payload fence, so it never stalls writers or readers.
int64_t Control::DryRun(Collection &result) {
    store->WithReader([&](Reader &reader) {
        Query candidates(reader.db, (std::string(LIVE_CTE) + R"SQL(SELECT count(*),coalesce(sum(coalesce(length(data),0)+(SELECT count(*)*65536 FROM local_objects x WHERE x.hash=o.hash)),0) FROM objects o WHERE
NOT EXISTS(SELECT 1 FROM extents e WHERE e.object=o.hash AND e.file IN (SELECT id FROM live_files)) AND
NOT EXISTS(SELECT 1 FROM page_entries e WHERE e.object=o.hash AND e.page IN (SELECT page FROM live_pages)))SQL")
                                                 .c_str());
        candidates.Step();
        result.candidate_objects = candidates.Int(0);
        result.candidate_bytes = candidates.Int(1);
        Query queued(reader.db, "SELECT (SELECT count(*) FROM gc_files)+(SELECT count(*) FROM gc_roots)+(SELECT "
                                "count(*) FROM gc_pages)+(SELECT count(*) FROM gc_objects)");
        queued.Step();
        result.remaining_candidates = queued.Int(0);
    });
    return result.candidate_objects;
}
Collection Control::Collect(bool dry_run, int64_t limit, const std::string &request) {
    if (limit < 1 || limit > 10000)
        throw std::runtime_error("GC batch limit must be 1..10000");
    Collection result;
    if (dry_run) {
        ReconcilePins(result, limit, false);
        DryRun(result);
        return result;
    }
    store->ReplayJournals();
    // Lock order everywhere: payload fence, then the authority connection.
    PayloadFence fence(store->directory, true);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    store->DrainLocalDeletions(uint64_t(limit));
    Transaction tx(store->db);
    auto operation = Begin("gc", Payload({std::to_string(limit)}), request);
    result.operation = operation.id;
    if (operation.state == "complete") {
        Query prior(store->db, "SELECT candidates,bytes,deleted,live_pins,stale_pins FROM collection_results WHERE "
                               "operation_id=?");
        prior.Text(1, operation.id);
        if (!prior.Step())
            throw std::runtime_error("GC result missing");
        result.candidate_objects = prior.Int(0);
        result.candidate_bytes = prior.Int(1);
        result.deleted_objects = prior.Int(2);
        result.live_pins = prior.Int(3);
        result.stale_pins = prior.Int(4);
        tx.Commit();
        return result;
    }
    ReconcilePins(result, limit, true);
    int64_t deleted = 0;
    result.remaining_candidates = CollectStep(limit, deleted);
    result.deleted_objects = deleted;
    result.candidate_objects = deleted + result.remaining_candidates;
    result.candidate_bytes = deleted * int64_t(Store::CHUNK);
    Query(store->db, "INSERT INTO collection_results VALUES(?,?,?,?,?,?)")
        .Text(1, operation.id)
        .Int(2, result.candidate_objects)
        .Int(3, result.candidate_bytes)
        .Int(4, result.deleted_objects)
        .Int(5, result.live_pins)
        .Int(6, result.stale_pins)
        .Run();
    Complete(operation);
    Crash("gc_deleted");
    tx.Commit();
    Crash("gc_committed");
    RemovePinFiles(store->directory, result.released_pins);
    store->DrainLocalDeletions(uint64_t(limit));
    store->SweepLocalOrphans(uint64_t(limit));
    return result;
}
bool Control::Maintain(int64_t budget, int64_t milliseconds) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(milliseconds);
    // Each short transaction takes and releases the payload fence and the authority without
    // waiting, so foreground I/O and management are never queued behind a long backlog.
    while (true) {
        std::unique_ptr<PayloadFence> fence;
        if (!PayloadFence::TryExclusive(store->directory, fence))
            return false;
        std::unique_lock<std::recursive_mutex> guard(store->mutex, std::try_to_lock);
        if (!guard.owns_lock())
            return false;
        int64_t remaining = 0;
        Collection step;
        sqlite3_busy_timeout(store->db, 0); // never wait for another process's transaction
        try {
            Transaction tx(store->db);
            ReconcilePins(step, budget, true);
            int64_t deleted = 0;
            remaining = CollectStep(budget, deleted);
            tx.Commit();
        } catch (...) {
            sqlite3_busy_timeout(store->db, 10000);
            return false;
        }
        sqlite3_busy_timeout(store->db, 10000);
        RemovePinFiles(store->directory, step.released_pins);
        try {
            store->DrainLocalDeletions(uint64_t(budget));
        } catch (...) {
            return false;
        }
        if (remaining == 0)
            return true;
        if (std::chrono::steady_clock::now() >= deadline)
            return true; // the backlog continues after the next checkpoint
        guard.unlock();
        fence.reset();
        std::this_thread::yield();
    }
}
void Control::ScheduleMaintenance() {
    std::lock_guard<std::mutex> guard(maintenance_mutex);
    if (maintenance_stopping)
        return;
    maintenance_requested = true;
    if (!maintenance_worker.joinable())
        maintenance_worker = std::thread([this] {
            std::unique_lock<std::mutex> lock(maintenance_mutex);
            while (true) {
                maintenance_signal.wait(lock, [this] { return maintenance_requested || maintenance_stopping; });
                if (maintenance_stopping)
                    return;
                maintenance_requested = false;
                lock.unlock();
                for (int attempt = 0; attempt < 3; ++attempt) {
                    bool done = false;
                    try {
                        done = Maintain();
                    } catch (...) {
                        done = true; // advisory; explicit differential_cleanup_old_files reports errors
                    }
                    if (done)
                        break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(20)); // fence/authority busy
                }
                lock.lock();
            }
        });
    maintenance_signal.notify_one();
}
Control::~Control() {
    {
        std::lock_guard<std::mutex> guard(maintenance_mutex);
        maintenance_stopping = true;
    }
    maintenance_signal.notify_all();
    if (maintenance_worker.joinable())
        maintenance_worker.join();
}
uint64_t Control::DeleteOrphanedFiles(bool dry_run, int64_t limit) {
    if (limit < 1 || limit > 10000)
        throw std::runtime_error("Orphan batch limit must be 1..10000");
    store->ReplayJournals();
    // Exclusive fence: a concurrent publication cannot have a staged file mid-flight.
    PayloadFence fence(store->directory, true);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    return store->SweepLocalOrphans(uint64_t(limit), dry_run);
}
Compaction Control::Compact(int64_t pages, const std::string &request) {
    Timer timer(Metric::Compact);
    if (pages < 1 || pages > 10000)
        throw std::runtime_error("Compaction batch must be 1..10000 pages");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto op = Begin("compact", Payload({std::to_string(pages)}), request);
    Compaction result;
    result.operation = op.id;
    if (op.state == "complete") {
        Query q(store->db, "SELECT candidates,bytes,deleted FROM collection_results WHERE operation_id=?");
        q.Text(1, op.id);
        if (!q.Step())
            throw std::runtime_error("Compaction result missing");
        result.candidate_objects = q.Int(0);
        result.candidate_bytes = q.Int(1);
        result.deleted_objects = q.Int(2);
        Query detail(store->db, "SELECT roots,payload_bytes,export_retained_bytes,page_size FROM compaction_details "
                                "WHERE operation_id=?");
        detail.Text(1, op.id);
        if (!detail.Step())
            throw std::runtime_error("[differential:incompatible_format] Compaction result predates detailed reporting");
        result.roots = detail.Int(0);
        result.payload_bytes = detail.Int(1);
        result.export_retained_bytes = detail.Int(2);
        result.page_size = detail.Int(3);
        tx.Commit();
        return result;
    }
    Query before(store->db, "PRAGMA freelist_count");
    before.Step();
    result.candidate_objects = before.Int(0);
    Query size(store->db, "PRAGMA page_size");
    size.Step();
    result.page_size = size.Int(0);
    result.candidate_bytes = result.candidate_objects * result.page_size;
    // Export-retained bytes: local payload still required by pending (not yet remotely
    // durable) exports. Completed exports do not retain local payload.
    Query retained(store->db, R"SQL(WITH pending AS (SELECT root FROM export_roots WHERE state='pending'),
needed AS (SELECT DISTINCT e.object AS hash FROM root_pages r JOIN page_entries e ON e.page=r.page WHERE r.root IN (SELECT root FROM pending))
SELECT (SELECT count(*) FROM roots WHERE hash NOT IN(SELECT root FROM reclaimed_roots)),
 (SELECT count(*) FROM objects WHERE data IS NOT NULL)*65536+(SELECT count(*) FROM local_objects)*65536,
 (SELECT coalesce(sum(coalesce(length(o.data),0)+(SELECT count(*)*65536 FROM local_objects l WHERE l.hash=o.hash)),0)
  FROM objects o WHERE o.hash IN (SELECT hash FROM needed)))SQL");
    retained.Step();
    result.roots = retained.Int(0);
    result.payload_bytes = retained.Int(1);
    result.export_retained_bytes = retained.Int(2);
    Execute(store->db, ("PRAGMA incremental_vacuum(" + std::to_string(pages) + ")").c_str());
    Crash("compaction_moved");
    Query after(store->db, "PRAGMA freelist_count");
    after.Step();
    result.deleted_objects = result.candidate_objects - after.Int(0);
    Query(store->db, "INSERT INTO collection_results VALUES(?,?,?,?,0,0)")
        .Text(1, op.id)
        .Int(2, result.candidate_objects)
        .Int(3, result.candidate_bytes)
        .Int(4, result.deleted_objects)
        .Run();
    Query(store->db, "INSERT INTO compaction_details VALUES(?,?,?,?,?)")
        .Text(1, op.id)
        .Int(2, result.roots)
        .Int(3, result.payload_bytes)
        .Int(4, result.export_retained_bytes)
        .Int(5, result.page_size)
        .Run();
    Complete(op);
    tx.Commit();
    Crash("compaction_completed");
    return result;
}
std::vector<RetentionRoot> Control::RetentionRoots() {
    std::vector<RetentionRoot> result;
    auto cursor = List("roots");
    std::vector<MetadataCell> row;
    while (cursor->Next(row))
        result.push_back({row[0].text, row[1].text, row[2].text});
    return result;
}
std::unique_ptr<MetadataCursor> Control::List(const std::string &kind, const std::string &database) {
    std::string sql;
    std::vector<std::string> params;
    if (kind == "branches") {
        {
            std::lock_guard<std::recursive_mutex> guard(store->mutex);
            params = {DatabaseID(database)};
        }
        // Shared databases: the branch's state relative to its S3 head as last seen (no S3 request).
        sql = R"SQL(SELECT b.database_id,d.name,b.id,b.name,b.state,b.root,b.epoch,b.head,b.created_at,b.source_database,b.source_snapshot,
'diff://db/'||b.database_id||'/branches/'||b.id,
CASE WHEN d.writers!='shared' OR b.state!='active' THEN NULL WHEN c.branch_id IS NOT NULL AND c.kept_branch!='' THEN 'forked'
 WHEN c.branch_id IS NOT NULL THEN 'diverged' WHEN w.key IS NULL THEN 'unpublished' WHEN b.head>w.snapshot THEN 'ahead' ELSE 'in_sync' END,
CASE WHEN d.writers='shared' AND b.state='active' THEN coalesce(c.remote_snapshot,w.snapshot) END,
NULLIF(c.kept_branch,'')
FROM branches b JOIN databases d ON d.id=b.database_id LEFT JOIN branch_conflicts c ON c.branch_id=b.id
LEFT JOIN branch_head_writes w ON w.key='dbs/'||b.database_id||'/branches/'||b.name AND w.provider_id=(SELECT provider_id FROM database_profiles p WHERE p.database_id=b.database_id)
WHERE b.database_id=? ORDER BY b.created_at,b.id)SQL";
    } else if (kind == "operations") {
        sql = R"SQL(SELECT o.id,o.request,o.kind,CASE WHEN o.state='complete' AND o.kind NOT IN ('retire','evict') AND EXISTS(
SELECT 1 FROM exports e JOIN export_roots r ON r.export_id=e.id WHERE e.database_id=o.database_id AND e.snapshot_id IN(o.snapshot_id,o.preservation) AND r.state='pending'
) THEN 'local_complete_export_pending' ELSE o.state END,o.database_id,o.branch_id,o.snapshot_id,o.preservation FROM operations o ORDER BY o.created_at,o.id)SQL";
    } else if (kind == "databases") {
        sql = DATABASES_SQL;
    } else if (kind == "providers") {
        params = {RepositoryID()};
        sql = R"SQL(SELECT ?,NULL,'locally durable commits and snapshots (default pool)',NULL,NULL,'none',NULL,NULL,NULL,NULL,NULL,NULL
UNION ALL SELECT p.id,'s3://'||p.bucket||'/'||p.prefix,'local commits; remote published snapshots',p.region,NULLIF(p.endpoint,''),
CASE WHEN p.credential_profile='' THEN 'AWS SDK default chain' ELSE 'AWS SDK profile:'||p.credential_profile END,
NULLIF(l.max_requests,0),coalesce(l.retries,2),coalesce(l.connect_timeout_ms,1000),coalesce(l.request_timeout_ms,5000),
coalesce(l.max_range_bytes,8388608),coalesce(l.multipart_threshold,16777216) FROM providers p LEFT JOIN provider_limits l ON l.provider_id=p.id
UNION ALL SELECT id,path,'locally durable commits and snapshots',NULL,NULL,'none',NULL,NULL,NULL,NULL,NULL,NULL FROM local_pools ORDER BY 2 NULLS FIRST)SQL";
    } else if (kind == "roots") {
        sql = RETENTION_ROOTS_SQL;
    } else
        throw std::runtime_error("Unknown metadata listing");
    return std::make_unique<MetadataCursor>(store->directory, sql, params);
}
// Cheap, lock-free introspection: one read snapshot plus file-system sizes. It never runs
// a collection pass; use differential_cleanup_old_files(dry_run => true) for an exact reclaimable estimate.
std::vector<std::pair<std::string, uint64_t>> Control::Statistics() {
    std::vector<std::pair<std::string, uint64_t>> result;
    store->WithReader([&](Reader &reader) {
        Query q(reader.db, R"SQL(
SELECT 'local_payload_bytes',(SELECT count(*) FROM objects WHERE data IS NOT NULL)*65536+(SELECT count(*) FROM local_objects)*65536
UNION ALL SELECT 'external_local_payload_objects',count(*) FROM local_objects
UNION ALL SELECT 'external_local_pending_deletions',count(*) FROM local_deletions
UNION ALL SELECT 'local_live_extent_entries',count(*) FROM extents
UNION ALL SELECT 'local_logical_extent_runs',count(*) FROM extents e WHERE NOT EXISTS(SELECT 1 FROM extents prior WHERE prior.file=e.file AND prior.chunk=e.chunk-1)
UNION ALL SELECT 'local_chunk_size_bytes',65536
UNION ALL SELECT 'local_manifest_page_chunks',256
UNION ALL SELECT 'local_manifest_entries',count(*) FROM page_entries
UNION ALL SELECT 'local_manifest_pages',count(*) FROM manifest_pages
UNION ALL SELECT 'local_manifest_count',count(*) FROM roots WHERE hash NOT IN(SELECT root FROM reclaimed_roots)
UNION ALL SELECT 'local_logical_file_bytes',coalesce(sum(size),0) FROM files WHERE name IS NOT NULL
UNION ALL SELECT 'local_retained_snapshots',count(*) FROM snapshots WHERE retired=0 AND evicted=0
UNION ALL SELECT 'local_evicted_snapshots',count(*) FROM snapshots WHERE evicted=1
UNION ALL SELECT 'local_gc_queued_files',count(*) FROM gc_files
UNION ALL SELECT 'local_gc_queued_roots',count(*) FROM gc_roots
UNION ALL SELECT 'local_gc_queued_pages',count(*) FROM gc_pages
UNION ALL SELECT 'local_gc_queued_objects',count(*) FROM gc_objects
UNION ALL SELECT 'remote_pending_exports',count(*) FROM export_roots WHERE state='pending'
UNION ALL SELECT 'remote_complete_exports',count(*) FROM export_roots WHERE state='complete'
UNION ALL SELECT 'remote_packs',count(*) FROM remote_packs
UNION ALL SELECT 'remote_pack_bytes',coalesce(sum(size),0) FROM remote_packs
UNION ALL SELECT 'remote_chunks',count(*) FROM remote_chunks
UNION ALL SELECT 'last_remote_publication_time_us',coalesce(max(s.published_at),0) FROM snapshots s
JOIN exports e ON e.database_id=s.database_id AND e.snapshot_id=s.id JOIN export_roots r ON r.export_id=e.id WHERE r.state='complete'
)SQL");
        while (q.Step())
            result.emplace_back(q.Text(0), uint64_t(q.Int(1)));
    });
    auto directory = std::filesystem::path(store->directory);
    for (auto &entry : std::vector<std::pair<std::string, std::string>>{
             {"local_authority_main_bytes", "engine.sqlite"}, {"local_authority_wal_bytes", "engine.sqlite-wal"}}) {
        std::error_code error;
        auto bytes = std::filesystem::file_size(directory / entry.second, error);
        result.emplace_back(entry.first, error ? 0 : bytes);
    }
    auto folder_bytes = [](const std::filesystem::path &path, const char *prefix, size_t length) {
        uint64_t total = 0;
        std::error_code error;
        if (!std::filesystem::exists(path, error))
            return total;
        for (auto &entry : std::filesystem::directory_iterator(path, error)) {
            auto name = entry.path().filename().string();
            if ((!prefix || name.rfind(prefix, 0) == 0) && (!length || name.size() == length) &&
                std::filesystem::is_regular_file(entry.symlink_status(error))) {
                auto n = entry.file_size(error);
                if (!error)
                    total += n;
            }
        }
        return total;
    };
    result.emplace_back("local_derived_staging_bytes", folder_bytes(directory / "staging" / RepositoryID(), "upload-", 13));
    result.emplace_back("local_journal_bytes", folder_bytes(directory / "journal", nullptr, 36));
    Collection pins;
    ReconcilePins(pins, 1, false);
    result.emplace_back("local_live_reader_pins", pins.live_pins);
    result.emplace_back("local_stale_reader_pins", pins.stale_pins);
    return result;
}
} // namespace differential
