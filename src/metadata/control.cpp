#include "control_internal.hpp"
#include "differential_metrics.hpp"
#include "differential_local.hpp"
#include <openssl/rand.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace differential {
namespace control {
std::string UUID() {
    unsigned char bytes[16];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1)
        throw std::runtime_error("Cannot allocate secure identity");
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const char *hex = "0123456789abcdef";
    std::string result;
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            result += '-';
        result += hex[bytes[i] >> 4];
        result += hex[bytes[i] & 15];
    }
    return result;
}
bool IsUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
}
void ValidateName(const std::string &name, bool optional) {
    if (optional && name.empty())
        return;
    if (name.empty() || name.size() > 128 ||
        name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") !=
            std::string::npos)
        throw std::runtime_error("Invalid name: use 1..128 letters, digits, underscore or hyphen");
}
std::string Payload(std::initializer_list<std::string> parts) {
    std::string result;
    for (const auto &part : parts)
        result += std::to_string(part.size()) + ":" + part;
    return result;
}
std::vector<std::string> DecodePayload(const std::string &payload) {
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos < payload.size()) {
        auto colon = payload.find(':', pos);
        if (colon == std::string::npos)
            throw std::runtime_error("[differential:corrupt_data] Corrupt operation payload");
        auto n = std::stoull(payload.substr(pos, colon - pos));
        parts.push_back(payload.substr(colon + 1, n));
        pos = colon + 1 + n;
    }
    return parts;
}
Operation ReadOperation(Query &q) {
    Operation o;
    o.id = q.Text(0);
    o.request = q.Text(1);
    o.kind = q.Text(2);
    o.payload = q.Text(3);
    o.state = q.Text(4);
    o.database = q.Text(5);
    o.branch = q.Text(6);
    o.snapshot = q.Int(7);
    o.preservation = q.Int(8);
    return o;
}
Branch ReadBranch(Query &q) {
    Branch b;
    b.database = q.Text(0);
    b.database_name = q.Text(1);
    b.id = q.Text(2);
    b.name = q.Text(3);
    b.state = q.Text(4);
    b.root = q.Text(5);
    b.epoch = q.Int(6);
    b.head = q.Int(7);
    b.created_at = q.Int(8);
    b.source_database = q.Text(9);
    b.source_snapshot = q.Int(10);
    return b;
}
Snapshot ReadSnapshot(Query &q) {
    Snapshot s;
    s.database = q.Text(0);
    s.database_name = q.Text(1);
    s.branch = q.Text(2);
    s.id = q.Int(3);
    s.name = q.Text(4);
    s.root = q.Text(5);
    s.published_at = q.Int(6);
    s.captured_at = q.Int(7);
    s.size = q.Int(8);
    s.retired = q.Int(9) != 0;
    s.source_database = q.Text(10);
    s.source_snapshot = q.Int(11);
    s.evicted = q.Int(12) != 0;
    s.author = q.Text(13);
    s.message = q.Text(14);
    s.extra_info = q.Text(15);
    return s;
}
} // namespace control
using namespace control;

int64_t Control::Now() {
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    // Test builds: a host whose clock is off (DIFFERENTIAL_TEST_CLOCK_SKEW_MS, may be negative).
    if (auto skew = Hook("DIFFERENTIAL_TEST_CLOCK_SKEW_MS"))
        now += std::stoll(skew) * 1000;
    return now;
}
BranchLock::~BranchLock() {
    if (fd >= 0)
        close(fd);
    if (management_fd >= 0)
        close(management_fd);
}
Control::Control(std::shared_ptr<Store> store, std::function<int64_t()> clock)
    : store(std::move(store)), clock(std::move(clock)) {
    std::lock_guard<std::recursive_mutex> guard(this->store->mutex);
    auto db = this->store->db;
    Transaction tx(db);
    // One control format. A new repository gets the schema; any other version is refused.
    Query existing(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_format'");
    if (existing.Step()) {
        Query v(db, "SELECT version FROM control_format");
        if (!v.Step() || v.Int(0) != CONTROL_FORMAT)
            throw std::runtime_error("[differential:incompatible_format] Unsupported differential control format");
    } else {
        Execute(db, R"SQL(
CREATE TABLE control_format(version INTEGER NOT NULL, repository_id TEXT NOT NULL);
CREATE TABLE databases(id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, last_snapshot INTEGER NOT NULL DEFAULT 0 CHECK(last_snapshot>=0), created_at INTEGER NOT NULL, writers TEXT NOT NULL DEFAULT 'single' CHECK(writers IN ('single','shared'))) WITHOUT ROWID;
CREATE TABLE branches(id TEXT PRIMARY KEY, database_id TEXT NOT NULL REFERENCES databases(id), name TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','deleted')), root TEXT REFERENCES roots(hash), epoch INTEGER NOT NULL DEFAULT 0 CHECK(epoch>=0), head INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, source_database TEXT, source_snapshot INTEGER) WITHOUT ROWID;
CREATE UNIQUE INDEX active_branch_names ON branches(database_id,name) WHERE state='active';
CREATE TABLE snapshots(database_id TEXT NOT NULL REFERENCES databases(id), id INTEGER NOT NULL CHECK(id>0), branch_id TEXT NOT NULL REFERENCES branches(id), name TEXT, root TEXT NOT NULL REFERENCES roots(hash), published_at INTEGER NOT NULL, captured_at INTEGER NOT NULL, retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)), evicted INTEGER NOT NULL DEFAULT 0 CHECK(evicted IN (0,1)), author TEXT, commit_message TEXT, commit_extra_info TEXT, PRIMARY KEY(database_id,id), UNIQUE(database_id,name)) WITHOUT ROWID;
CREATE TABLE history(sequence INTEGER PRIMARY KEY AUTOINCREMENT, branch_id TEXT NOT NULL REFERENCES branches(id), database_id TEXT NOT NULL, snapshot_id INTEGER NOT NULL, effective_at INTEGER NOT NULL, FOREIGN KEY(database_id,snapshot_id) REFERENCES snapshots(database_id,id));
CREATE INDEX history_lookup ON history(branch_id,effective_at,sequence);
CREATE TABLE operations(id TEXT PRIMARY KEY, request TEXT UNIQUE, kind TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','complete')), database_id TEXT, branch_id TEXT, snapshot_id INTEGER NOT NULL DEFAULT 0, preservation INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE export_roots(export_id TEXT PRIMARY KEY, root TEXT NOT NULL REFERENCES roots(hash), state TEXT NOT NULL CHECK(state IN ('pending','complete','abandoned'))) WITHOUT ROWID;
CREATE TABLE providers(id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, bucket TEXT NOT NULL, prefix TEXT NOT NULL, region TEXT NOT NULL, endpoint TEXT NOT NULL, credential_profile TEXT NOT NULL, path_style INTEGER NOT NULL, development_http INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE provider_limits(provider_id TEXT PRIMARY KEY REFERENCES providers(id), max_requests INTEGER NOT NULL, retries INTEGER NOT NULL, connect_timeout_ms INTEGER NOT NULL, request_timeout_ms INTEGER NOT NULL, max_range_bytes INTEGER NOT NULL, multipart_threshold INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE database_profiles(database_id TEXT PRIMARY KEY REFERENCES databases(id), provider_id TEXT NOT NULL REFERENCES providers(id)) WITHOUT ROWID;
CREATE TABLE database_local_profiles(database_id TEXT PRIMARY KEY REFERENCES databases(id), pool TEXT NOT NULL REFERENCES local_pools(id)) WITHOUT ROWID;
CREATE TABLE exports(id TEXT PRIMARY KEY REFERENCES export_roots(export_id), provider_id TEXT NOT NULL REFERENCES providers(id), database_id TEXT NOT NULL, snapshot_id INTEGER NOT NULL, descriptor TEXT NOT NULL DEFAULT '', UNIQUE(database_id,snapshot_id), FOREIGN KEY(database_id,snapshot_id) REFERENCES snapshots(database_id,id)) WITHOUT ROWID;
CREATE TABLE verified_remote_objects(provider_id TEXT NOT NULL REFERENCES providers(id), key TEXT NOT NULL, hash TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(provider_id,key)) WITHOUT ROWID;
CREATE TABLE collection_results(operation_id TEXT PRIMARY KEY REFERENCES operations(id), candidates INTEGER NOT NULL, bytes INTEGER NOT NULL, deleted INTEGER NOT NULL, live_pins INTEGER NOT NULL, stale_pins INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE compaction_details(operation_id TEXT PRIMARY KEY REFERENCES operations(id), roots INTEGER NOT NULL, payload_bytes INTEGER NOT NULL, export_retained_bytes INTEGER NOT NULL, page_size INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE compound_intents(operation_id TEXT PRIMARY KEY REFERENCES operations(id), branch_id TEXT NOT NULL UNIQUE REFERENCES branches(id), expected_head INTEGER NOT NULL, expected_epoch INTEGER NOT NULL, target_root TEXT REFERENCES roots(hash)) WITHOUT ROWID;
CREATE INDEX snapshots_root ON snapshots(root);
CREATE INDEX branches_root ON branches(root);
CREATE INDEX compound_intents_target ON compound_intents(target_root);
CREATE INDEX export_roots_root ON export_roots(root);
CREATE TABLE remote_packs(provider_id TEXT NOT NULL REFERENCES providers(id), pool TEXT NOT NULL, pack TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(provider_id,pool,pack)) WITHOUT ROWID;
CREATE TABLE remote_chunks(provider_id TEXT NOT NULL, hash TEXT NOT NULL, pool TEXT NOT NULL, pack TEXT NOT NULL, slot INTEGER NOT NULL CHECK(slot>=0), PRIMARY KEY(provider_id,hash), FOREIGN KEY(provider_id,pool,pack) REFERENCES remote_packs(provider_id,pool,pack)) WITHOUT ROWID;
CREATE TABLE remote_commits(database_id TEXT NOT NULL REFERENCES databases(id), id INTEGER NOT NULL CHECK(id>0), parent INTEGER NOT NULL, branch_id TEXT NOT NULL, branch_name TEXT NOT NULL, published_at INTEGER NOT NULL, descriptor TEXT NOT NULL, name TEXT, author TEXT, commit_message TEXT, commit_extra_info TEXT, expired INTEGER NOT NULL DEFAULT 0 CHECK(expired IN (0,1)), PRIMARY KEY(database_id,id)) WITHOUT ROWID;
CREATE TABLE branch_conflicts(branch_id TEXT PRIMARY KEY REFERENCES branches(id), snapshot INTEGER NOT NULL, remote_snapshot INTEGER NOT NULL, kept_branch TEXT NOT NULL DEFAULT '', detected_at INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE branch_head_writes(provider_id TEXT NOT NULL REFERENCES providers(id), key TEXT NOT NULL, etag TEXT NOT NULL, write_id TEXT NOT NULL, pending TEXT NOT NULL, snapshot INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(provider_id,key)) WITHOUT ROWID;
CREATE TRIGGER gc_snapshot_release AFTER UPDATE OF retired,evicted ON snapshots BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root); END;
CREATE TRIGGER gc_branch_release AFTER UPDATE OF root,state ON branches WHEN OLD.root IS NOT NULL BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root); END;
CREATE TRIGGER gc_intent_release AFTER DELETE ON compound_intents WHEN OLD.target_root IS NOT NULL BEGIN
INSERT INTO gc_roots SELECT OLD.target_root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.target_root); END;
CREATE TRIGGER gc_export_release AFTER UPDATE OF state ON export_roots BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root); END;
)SQL");
        Query(db, "INSERT INTO control_format VALUES(?,?)").Int(1, CONTROL_FORMAT).Text(2, UUID()).Run();
    }
    // Remote garbage collection: condemnation records learned per S3 location, and the condemned
    // objects with their earliest condemnation (S3 LastModified of the record).
    Execute(db, R"SQL(
CREATE TABLE IF NOT EXISTS remote_gc_records(provider_id TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(provider_id,key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS remote_condemned(provider_id TEXT NOT NULL, key TEXT NOT NULL, condemned_at INTEGER NOT NULL, PRIMARY KEY(provider_id,key)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS remote_chunks_pack ON remote_chunks(provider_id,pool,pack);
CREATE TABLE IF NOT EXISTS remote_index_packs(provider_id TEXT NOT NULL, index_key TEXT NOT NULL, pack_key TEXT NOT NULL, PRIMARY KEY(provider_id,index_key,pack_key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS remote_descriptor_pages(provider_id TEXT NOT NULL, descriptor_key TEXT NOT NULL, index_key TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(provider_id,descriptor_key,index_key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS remote_sync_state(database_id TEXT PRIMARY KEY, commits_full_at INTEGER NOT NULL, expiries_full_at INTEGER NOT NULL, expiries_watermark TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS remote_commit_descriptors(provider_id TEXT NOT NULL, commit_key TEXT NOT NULL, descriptor_key TEXT NOT NULL, PRIMARY KEY(provider_id,commit_key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ship_config(database_id TEXT PRIMARY KEY REFERENCES databases(id), mode TEXT NOT NULL CHECK(mode IN ('async','sync')), interval_ms INTEGER NOT NULL CHECK(interval_ms>=0)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ship_chains(branch_id TEXT PRIMARY KEY, database_id TEXT NOT NULL, chain INTEGER NOT NULL, epoch INTEGER NOT NULL, epoch_hash TEXT NOT NULL, base TEXT NOT NULL, known TEXT NOT NULL, shipped_sequence INTEGER NOT NULL, chain_bytes INTEGER NOT NULL, shipped_at INTEGER NOT NULL, pending TEXT NOT NULL DEFAULT '') WITHOUT ROWID;
)SQL");
    tx.Commit();
}
std::string Control::RepositoryID() {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT repository_id FROM control_format");
    if (!q.Step())
        throw std::runtime_error("[differential:corrupt_data] Repository identity missing");
    auto id = q.Text(0);
    if (!IsUUID(id) || q.Step())
        throw std::runtime_error("[differential:corrupt_data] Invalid repository identity");
    return id;
}
std::string Control::DatabaseID(const std::string &name) {
    Query q(store->db, "SELECT id FROM databases WHERE id=? OR name=? ORDER BY id=? DESC");
    q.Text(1, name).Text(2, name).Text(3, name);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] Database not found");
    return q.Text(0);
}
Operation Control::Begin(const std::string &kind, const std::string &payload, const std::string &request) {
    if (request.size() > 256)
        throw std::runtime_error("Request ID exceeds 256 bytes");
    if (!request.empty()) {
        Query q(store->db, (std::string(OP_COLUMNS) + " WHERE request=?").c_str());
        q.Text(1, request);
        if (q.Step()) {
            auto o = ReadOperation(q);
            if (o.kind != kind || o.payload != payload)
                throw std::runtime_error("Request ID reused with different payload");
            return o;
        }
    }
    Operation o;
    o.id = UUID();
    o.request = request;
    o.kind = kind;
    o.payload = payload;
    o.state = "pending";
    Query(store->db, "INSERT INTO operations(id,request,kind,payload,state,created_at) "
                     "VALUES(?,NULLIF(?,''),?,?,'pending',?)")
        .Text(1, o.id)
        .Text(2, request)
        .Text(3, kind)
        .Text(4, payload)
        .Int(5, Timestamp())
        .Run();
    return o;
}
void Control::Complete(Operation &o) {
    if (o.kind == "merge" || o.kind == "merge_branch") CompleteMergeAttempt(o);
    Query(store->db, "UPDATE operations SET "
                     "state='complete',database_id=?,branch_id=?,snapshot_id=?,preservation=? WHERE id=?")
        .Text(1, o.database)
        .Text(2, o.branch)
        .Int(3, o.snapshot)
        .Int(4, o.preservation)
        .Text(5, o.id)
        .Run();
    o.state = "complete";
    Crash("reference_published");
}
Branch Control::NewBranch(const std::string &database, const std::string &name, const std::string &root,
                          int64_t head, const std::string &source_db, int64_t source_snapshot,
                          const std::string &reserved_id) {
    CheckMergeDestination(database, name);
    Branch b;
    b.database = database;
    b.id = reserved_id.empty() ? UUID() : reserved_id;
    b.name = name;
    b.state = "active";
    b.root = root;
    b.head = head;
    b.created_at = Timestamp();
    b.source_database = source_db;
    b.source_snapshot = source_snapshot;
    Query(store->db, "INSERT INTO "
                     "branches(id,database_id,name,state,root,head,created_at,source_database,source_"
                     "snapshot) VALUES(?,?,?,'active',NULLIF(?,''),?,?,NULLIF(?,''),?)")
        .Text(1, b.id)
        .Text(2, database)
        .Text(3, name)
        .Text(4, root)
        .Int(5, head)
        .Int(6, b.created_at)
        .Text(7, source_db)
        .Int(8, source_snapshot)
        .Run();
    if (!root.empty()) {
        // Copy the map, never payload: a branch shares every unchanged immutable chunk.
        auto file = store->CreateFileTx(b.FileName(), store->RootSizeTx(root), LocalDatabasePool(database));
        store->InstallRootTx(file, root);
    }
    return b;
}
Operation Control::CreateDatabase(const std::string &name, const std::string &request,
                                  const std::string &provider) {
    ValidateName(name);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto o = Begin("create_database", Payload({name, provider}), request);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    o.database = UUID();
    Query(store->db, "INSERT INTO databases(id,name,created_at) VALUES(?,?,?)")
        .Text(1, o.database)
        .Text(2, name)
        .Int(3, Timestamp())
        .Run();
    AssignProvider(o.database, provider);
    auto b = NewBranch(o.database, "main", "", 0, "", 0);
    o.branch = b.id;
    Complete(o);
    tx.Commit();
    return o;
}
Branch Control::CreateFromShippedBase(const std::string &name, const std::string &provider,
                                      const std::string &database_id, const std::string &branch_id,
                                      const std::string &branch_name, const RemoteHead &base) {
    ValidateName(name);
    ValidateName(branch_name);
    if (provider == "local" || !LocalProvider(provider).empty())
        throw std::runtime_error("Recovering a shipped branch needs its S3 location");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = database_id.empty() ? UUID() : database_id;
    if (!IsUUID(id) || (!branch_id.empty() && !IsUUID(branch_id)))
        throw std::runtime_error("[differential:corrupt_data] Corrupt shipped identity");
    Query existing(store->db, "SELECT name FROM databases WHERE id=? OR name=?");
    existing.Text(1, id).Text(2, name);
    if (existing.Step())
        throw std::runtime_error("[differential:name_conflict] Database \"" + existing.Text(0) +
                                 "\" already exists in this repository" +
                                 (database_id.empty() ? "" : " (a failover recovers into a repository without it)"));
    Query(store->db, "INSERT INTO databases(id,name,created_at) VALUES(?,?,?)")
        .Text(1, id)
        .Text(2, name)
        .Int(3, Timestamp())
        .Run();
    AssignProvider(id, provider);
    auto root = ImportMapTx(provider, base);
    Branch b;
    b.database = id;
    b.id = branch_id.empty() ? UUID() : branch_id;
    b.name = branch_name;
    Query(store->db, "INSERT INTO branches(id,database_id,name,state,head,created_at) VALUES(?,?,?,'active',0,?)")
        .Text(1, b.id)
        .Text(2, id)
        .Text(3, branch_name)
        .Int(4, Timestamp())
        .Run();
    auto file = store->CreateFileTx(b.FileName(), base.size, LocalDatabasePool(id));
    store->InstallRootTx(file, root);
    tx.Commit();
    return GetBranch(id, b.id, true);
}
std::optional<std::pair<std::string, int64_t>> Control::ShipConfigOf(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT mode,interval_ms FROM ship_config WHERE database_id=?");
    q.Text(1, database);
    if (!q.Step())
        return std::nullopt;
    return std::make_pair(q.Text(0), q.Int(1));
}
void Control::SetShipConfigOf(const std::string &database, const std::string &mode, int64_t interval_ms) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    if (mode == "off")
        Query(store->db, "DELETE FROM ship_config WHERE database_id=?").Text(1, database).Run();
    else
        Query(store->db, "INSERT INTO ship_config VALUES(?,?,?) ON CONFLICT(database_id) DO UPDATE SET "
                         "mode=excluded.mode,interval_ms=excluded.interval_ms")
            .Text(1, database)
            .Text(2, mode)
            .Int(3, interval_ms)
            .Run();
    tx.Commit();
}
std::optional<Control::ShipChainRow> Control::ShipChainOf(const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT chain,epoch,epoch_hash,base,known,shipped_sequence,chain_bytes,shipped_at,pending FROM "
                       "ship_chains WHERE branch_id=?");
    q.Text(1, branch);
    if (!q.Step())
        return std::nullopt;
    ShipChainRow row;
    row.chain = q.Int(0);
    row.epoch = q.Int(1);
    row.hash = q.Text(2);
    row.base = q.Text(3);
    row.known = q.Text(4);
    row.sequence = q.Int(5);
    row.bytes = q.Int(6);
    row.shipped_at = q.Int(7);
    row.pending = q.Text(8);
    return row;
}
void Control::SetShipChainOf(const std::string &database, const std::string &branch, const ShipChainRow &row) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Shipping state survives process death; its epochs are synced with the next FULL commit (an epoch
    // shipped again after a power loss is found in S3 with the same identity).
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    Query(store->db, "INSERT INTO ship_chains VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(branch_id) DO UPDATE SET "
                     "chain=excluded.chain,epoch=excluded.epoch,epoch_hash=excluded.epoch_hash,base=excluded.base,"
                     "known=excluded.known,shipped_sequence=excluded.shipped_sequence,chain_bytes=excluded.chain_bytes,"
                     "shipped_at=excluded.shipped_at,pending=excluded.pending")
        .Text(1, branch)
        .Text(2, database)
        .Int(3, row.chain)
        .Int(4, row.epoch)
        .Text(5, row.hash)
        .Text(6, row.base)
        .Text(7, row.known)
        .Int(8, row.sequence)
        .Int(9, row.bytes)
        .Int(10, row.shipped_at)
        .Text(11, row.pending)
        .Run();
    tx.Commit();
}
void Control::CreateSharedDatabase(const std::string &id, const std::string &name, const std::string &provider) {
    ValidateName(name);
    if (!IsUUID(id) || provider == "local" || !LocalProvider(provider).empty())
        throw std::runtime_error("A shared database needs an S3 DATA_PATH");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query existing(store->db, "SELECT id,writers FROM databases WHERE id=? OR name=?");
    existing.Text(1, id).Text(2, name);
    if (existing.Step()) {
        if (existing.Text(0) != id || existing.Text(1) != "shared")
            throw std::runtime_error("[differential:name_conflict] Database \"" + name + "\" already exists in this "
                                     "repository and is not shared");
        tx.Commit();
        return;
    }
    Query(store->db, "INSERT INTO databases(id,name,created_at,writers) VALUES(?,?,?,'shared')")
        .Text(1, id)
        .Text(2, name)
        .Int(3, Timestamp())
        .Run();
    AssignProvider(id, provider);
    tx.Commit();
}
std::string Control::Writers(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT writers FROM databases WHERE id=?");
    q.Text(1, DatabaseID(database));
    return q.Step() ? q.Text(0) : "single";
}
Branch Control::CreateEmptyBranch(const std::string &database, const std::string &name) {
    ValidateName(name);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    Query existing(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
    existing.Text(1, id).Text(2, name);
    if (!existing.Step())
        NewBranch(id, name, "", 0, "", 0);
    tx.Commit();
    return GetBranch(id, name);
}
void Control::UnlinkBranchFiles(const Branch &b) {
    Query(store->db, "UPDATE files SET name=NULL WHERE name IN (?,?,?,?)")
        .Text(1, b.FileName())
        .Text(2, b.FileName() + ".wal")
        .Text(3, b.FileName() + ".wal.checkpoint")
        .Text(4, b.FileName() + ".wal.recovery")
        .Run();
}
Branch Control::AdoptRemoteHead(const std::string &database, const std::string &provider, const RemoteHead &head,
                                bool replace) {
    ValidateName(head.branch_name);
    if (!IsUUID(head.branch) || head.snapshot <= 0)
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    if (!replace) {
        Query adopted(store->db, "SELECT id FROM branches WHERE database_id=? AND name=? AND state='active'");
        adopted.Text(1, id).Text(2, head.branch_name);
        if (adopted.Step()) {
            auto branch = adopted.Text(0);
            tx.Commit();
            return GetBranch(id, branch, true);
        }
    }
    // The head's snapshot number: the same snapshot (adopted before), or a local unexported one that
    // has to make room (another writer published that number first).
    bool known = false;
    Query existing(store->db, "SELECT s.root,coalesce(e.descriptor,''),coalesce(r.state,'') FROM snapshots s LEFT "
                              "JOIN exports e ON e.database_id=s.database_id AND e.snapshot_id=s.id LEFT JOIN "
                              "export_roots r ON r.export_id=e.id WHERE s.database_id=? AND s.id=?");
    existing.Text(1, id).Int(2, head.snapshot);
    if (existing.Step()) {
        if (existing.Text(2) == "complete") {
            if (existing.Text(0) != head.root || existing.Text(1) != head.descriptor)
                throw std::runtime_error("[differential:corrupt_data] Local snapshot " + std::to_string(head.snapshot) +
                                         " differs from the snapshot published in S3 under that number");
            known = true;
        } else
            RenumberTx(id, head.snapshot, head.snapshot);
    }
    auto root = ImportMapTx(provider, head);
    // The local branch: replaced by the published one (same identity: moved; another identity: the
    // unpublished local branch is dropped, it had no changes).
    Branch b;
    Query local(store->db, (std::string(BRANCH_COLUMNS) + " WHERE b.database_id=? AND b.name=? AND b.state='active'").c_str());
    local.Text(1, id).Text(2, head.branch_name);
    if (local.Step()) {
        auto current = ReadBranch(local);
        UnlinkBranchFiles(current);
        Query(store->db, "DELETE FROM branch_conflicts WHERE branch_id=?").Text(1, current.id).Run();
        if (current.id != head.branch)
            Query(store->db, "UPDATE branches SET state='deleted' WHERE id=?").Text(1, current.id).Run();
    }
    Query same(store->db, "SELECT state FROM branches WHERE id=?");
    same.Text(1, head.branch);
    if (same.Step()) {
        if (same.Text(0) != "active")
            throw std::runtime_error("[differential:retired] Branch " + head.branch_name + " was dropped in this "
                                     "repository");
        Query(store->db, "UPDATE branches SET root=?,head=?,epoch=epoch+1 WHERE id=?")
            .Text(1, root)
            .Int(2, head.snapshot)
            .Text(3, head.branch)
            .Run();
    } else
        Query(store->db, "INSERT INTO branches(id,database_id,name,state,root,head,created_at) "
                         "VALUES(?,?,?,'active',?,?,?)")
            .Text(1, head.branch)
            .Text(2, id)
            .Text(3, head.branch_name)
            .Text(4, root)
            .Int(5, head.snapshot)
            .Int(6, Timestamp())
            .Run();
    b = GetBranch(id, head.branch, true);
    auto file = store->CreateFileTx(b.FileName(), head.size, LocalDatabasePool(id));
    store->InstallRootTx(file, root);
    if (!known)
        InsertRemoteSnapshotTx(id, provider, head, root, b.id);
    Event(b, head.snapshot, Timestamp());
    Query(store->db, "DELETE FROM branch_conflicts WHERE branch_id=?").Text(1, b.id).Run();
    Query(store->db, "UPDATE databases SET last_snapshot=max(last_snapshot,?) WHERE id=?")
        .Int(1, head.snapshot)
        .Text(2, id)
        .Run();
    RecordHeadWrite(provider, head.key, HeadWrite{head.etag, head.write_id, "", head.snapshot});
    Failpoint("DIFFERENTIAL_SHARED_CRASH", "head_adopted");
    tx.Commit();
    return b;
}
std::string Control::ImportMapTx(const std::string &provider, const RemoteHead &head) {
    for (auto &location : head.locations) {
        Query(store->db, "INSERT OR IGNORE INTO remote_packs VALUES(?,?,?,0)")
            .Text(1, provider)
            .Text(2, location.second.pool)
            .Text(3, location.second.pack)
            .Run();
        // A location in a condemned pack gives way to the published one.
        Query(store->db, "INSERT INTO remote_chunks VALUES(?,?,?,?,?) ON CONFLICT(provider_id,hash) DO UPDATE SET "
                         "pool=excluded.pool,pack=excluded.pack,slot=excluded.slot WHERE EXISTS(SELECT 1 FROM "
                         "remote_condemned d WHERE d.provider_id=remote_chunks.provider_id AND "
                         "d.key='dbs/'||remote_chunks.pool||'/packs/'||remote_chunks.pack)")
            .Text(1, provider)
            .Text(2, location.first)
            .Text(3, location.second.pool)
            .Text(4, location.second.pack)
            .Int(5, int64_t(location.second.slot))
            .Run();
    }
    auto root = store->ImportRootTx(head.size, head.chunks);
    if (root != head.root)
        throw std::runtime_error("[differential:corrupt_data] Imported map does not match the exported root");
    return root;
}
void Control::InsertRemoteSnapshotTx(const std::string &id, const std::string &provider, const RemoteHead &head,
                                     const std::string &root, const std::string &branch) {
    // Its bytes are in S3 only: readers of the snapshot read the export (like an evicted one). Its name
    // is kept unless a local snapshot already uses it.
    Query(store->db, "INSERT INTO snapshots(database_id,id,branch_id,root,published_at,captured_at,evicted,name,author,"
                     "commit_message,commit_extra_info) VALUES(?1,?2,?3,?4,?5,?5,1,CASE WHEN ?6='' OR EXISTS(SELECT 1 "
                     "FROM snapshots WHERE database_id=?1 AND name=?6) THEN NULL ELSE ?6 END,NULLIF(?7,''),"
                     "NULLIF(?8,''),NULLIF(?9,''))")
        .Text(1, id)
        .Int(2, head.snapshot)
        .Text(3, branch)
        .Text(4, root)
        .Int(5, head.published_at)
        .Text(6, head.info.name)
        .Text(7, head.info.author)
        .Text(8, head.info.message)
        .Text(9, head.info.extra_info)
        .Run();
    auto export_id = UUID();
    Query(store->db, "INSERT INTO export_roots VALUES(?,?,'complete')").Text(1, export_id).Text(2, root).Run();
    Query(store->db, "INSERT INTO exports(id,provider_id,database_id,snapshot_id,descriptor) VALUES(?,?,?,?,?)")
        .Text(1, export_id)
        .Text(2, provider)
        .Text(3, id)
        .Int(4, head.snapshot)
        .Text(5, head.descriptor)
        .Run();
}
void Control::ImportRemoteSnapshot(const std::string &database, const std::string &provider, const RemoteHead &head,
                                   const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    // A local snapshot with that number is the same one (exported), or unexported and moves up.
    Query existing(store->db, "SELECT coalesce(r.state,'') FROM snapshots s LEFT JOIN exports e ON "
                              "e.database_id=s.database_id AND e.snapshot_id=s.id LEFT JOIN export_roots r ON "
                              "r.export_id=e.id WHERE s.database_id=? AND s.id=?");
    existing.Text(1, id).Int(2, head.snapshot);
    bool known = existing.Step();
    if (known && existing.Text(0) != "complete") {
        RenumberTx(id, head.snapshot, head.snapshot);
        known = false;
    }
    if (!known)
        InsertRemoteSnapshotTx(id, provider, head, ImportMapTx(provider, head), branch);
    tx.Commit();
}
int64_t Control::RenumberTx(const std::string &id, int64_t from, int64_t taken) {
    auto delta = taken + 1 - from;
    if (delta <= 0)
        return 0;
    Query exported(store->db, "SELECT count(*) FROM exports e JOIN export_roots r ON r.export_id=e.id WHERE "
                              "e.database_id=? AND e.snapshot_id>=? AND r.state='complete'");
    exported.Text(1, id).Int(2, from);
    exported.Step();
    if (exported.Int(0))
        throw std::runtime_error("[differential:corrupt_data] Cannot renumber snapshots that were already exported");
    // Children follow at commit; highest first, so no moved number meets one not yet moved.
    Execute(store->db, "PRAGMA defer_foreign_keys=ON");
    std::vector<int64_t> moving;
    {
        Query q(store->db, "SELECT id FROM snapshots WHERE database_id=? AND id>=? ORDER BY id DESC");
        q.Text(1, id).Int(2, from);
        while (q.Step())
            moving.push_back(q.Int(0));
    }
    for (auto n : moving) {
        Query(store->db, "UPDATE snapshots SET id=? WHERE database_id=? AND id=?").Int(1, n + delta).Text(2, id).Int(3, n).Run();
        Query(store->db, "UPDATE exports SET snapshot_id=? WHERE database_id=? AND snapshot_id=?")
            .Int(1, n + delta)
            .Text(2, id)
            .Int(3, n)
            .Run();
    }
    for (auto sql : {"UPDATE history SET snapshot_id=snapshot_id+?1 WHERE database_id=?2 AND snapshot_id>=?3",
                     "UPDATE operations SET snapshot_id=snapshot_id+?1 WHERE database_id=?2 AND snapshot_id>=?3",
                     "UPDATE operations SET preservation=preservation+?1 WHERE database_id=?2 AND preservation>=?3",
                     "UPDATE branches SET head=head+?1 WHERE database_id=?2 AND head>=?3",
                     "UPDATE branches SET source_snapshot=source_snapshot+?1 WHERE source_database=?2 AND "
                     "source_snapshot>=?3",
                     "UPDATE compound_intents SET expected_head=expected_head+?1 WHERE expected_head>=?3 AND branch_id "
                     "IN (SELECT id FROM branches WHERE database_id=?2)",
                     "UPDATE databases SET last_snapshot=CASE WHEN last_snapshot>=?3 THEN last_snapshot+?1 ELSE "
                     "max(last_snapshot,?3+?1-1) END WHERE id=?2"})
        Query(store->db, sql).Int(1, delta).Text(2, id).Int(3, from).Run();
    return delta;
}
int64_t Control::RenumberSnapshots(const std::string &database, int64_t from, int64_t taken) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto delta = RenumberTx(DatabaseID(database), from, taken);
    tx.Commit();
    return delta;
}
void Control::RecordConflict(const BranchConflict &conflict) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query(store->db, "INSERT INTO branch_conflicts VALUES(?,?,?,?,?) ON CONFLICT(branch_id) DO UPDATE SET "
                     "snapshot=excluded.snapshot,remote_snapshot=excluded.remote_snapshot,"
                     "kept_branch=excluded.kept_branch,detected_at=excluded.detected_at")
        .Text(1, conflict.branch)
        .Int(2, conflict.snapshot)
        .Int(3, conflict.remote_snapshot)
        .Text(4, conflict.kept_branch)
        .Int(5, conflict.detected_at ? conflict.detected_at : Timestamp())
        .Run();
}
std::optional<BranchConflict> Control::Conflict(const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT snapshot,remote_snapshot,kept_branch,detected_at FROM branch_conflicts WHERE branch_id=?");
    q.Text(1, branch);
    if (!q.Step())
        return std::nullopt;
    return BranchConflict{branch, q.Text(2), q.Int(0), q.Int(1), q.Int(3)};
}
void Control::ClearConflict(const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query(store->db, "DELETE FROM branch_conflicts WHERE branch_id=?").Text(1, branch).Run();
}
Operation Control::BeginOperation(const std::string &kind, const std::vector<std::string> &payload,
                                  const std::string &request) {
    std::string encoded;
    for (auto &part : payload)
        encoded += std::to_string(part.size()) + ":" + part;
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto o = Begin(kind, encoded, request);
    tx.Commit();
    return o;
}
Operation Control::CompleteOperation(Operation o) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Complete(o);
    tx.Commit();
    return o;
}
std::set<int64_t> Control::CachedCommits(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::set<int64_t> result;
    Query q(store->db, "SELECT id FROM remote_commits WHERE database_id=?");
    q.Text(1, DatabaseID(database));
    while (q.Step())
        result.insert(q.Int(0));
    return result;
}
void Control::CacheCommits(const std::string &database, const std::vector<RemoteCommit> &commits) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    for (auto &c : commits)
        Query(store->db, "INSERT OR IGNORE INTO remote_commits VALUES(?,?,?,?,?,?,?,NULLIF(?,''),NULLIF(?,''),"
                         "NULLIF(?,''),NULLIF(?,''),0)")
            .Text(1, id)
            .Int(2, c.snapshot)
            .Int(3, c.parent)
            .Text(4, c.branch)
            .Text(5, c.branch_name)
            .Int(6, c.published_at)
            .Text(7, c.descriptor)
            .Text(8, c.name)
            .Text(9, c.author)
            .Text(10, c.message)
            .Text(11, c.extra_info)
            .Run();
    tx.Commit();
}
Control::RemoteSyncState Control::GetRemoteSyncState(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    RemoteSyncState state;
    Query q(store->db, "SELECT commits_full_at,expiries_full_at,expiries_watermark FROM remote_sync_state WHERE "
                       "database_id=?");
    q.Text(1, DatabaseID(database));
    if (q.Step())
        state = {q.Int(0), q.Int(1), q.Text(2)};
    return state;
}
void Control::SetRemoteSyncState(const std::string &database, const RemoteSyncState &state) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false); // re-derived by a full listing if lost
    Query(store->db, "INSERT INTO remote_sync_state VALUES(?,?,?,?) ON CONFLICT(database_id) DO UPDATE SET "
                     "commits_full_at=excluded.commits_full_at,expiries_full_at=excluded.expiries_full_at,"
                     "expiries_watermark=excluded.expiries_watermark")
        .Text(1, DatabaseID(database))
        .Int(2, state.commits_full_at)
        .Int(3, state.expiries_full_at)
        .Text(4, state.expiries_watermark)
        .Run();
    tx.Commit();
}
void Control::ExpireCommits(const std::string &database, const std::vector<int64_t> &snapshots) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    for (auto n : snapshots) {
        Query(store->db, "UPDATE remote_commits SET expired=1 WHERE database_id=? AND id=?").Text(1, id).Int(2, n).Run();
        Query(store->db, "UPDATE snapshots SET retired=1 WHERE database_id=? AND id=? AND retired=0")
            .Text(1, id)
            .Int(2, n)
            .Run();
    }
    tx.Commit();
}
Snapshot Control::FindSnapshot(const std::string &database, int64_t snapshot, bool retained) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto id = DatabaseID(database);
    Query local(store->db, "SELECT 1 FROM snapshots WHERE database_id=? AND id=?");
    local.Text(1, id).Int(2, snapshot);
    if (local.Step())
        return GetSnapshot(id, snapshot, retained);
    Query q(store->db, "SELECT c.parent,c.branch_id,c.published_at,c.descriptor,c.name,c.author,c.commit_message,"
                       "c.commit_extra_info,c.expired,d.name FROM remote_commits c JOIN databases d ON "
                       "d.id=c.database_id WHERE c.database_id=? AND c.id=?");
    q.Text(1, id).Int(2, snapshot);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] Snapshot not found");
    Snapshot s;
    s.database = id;
    s.database_name = q.Text(9);
    s.id = snapshot;
    s.parent = q.Int(0);
    s.branch = q.Text(1);
    s.published_at = s.captured_at = q.Int(2);
    s.export_key = q.Text(3);
    s.name = q.Text(4);
    s.author = q.Text(5);
    s.message = q.Text(6);
    s.extra_info = q.Text(7);
    s.retired = q.Int(8) != 0;
    s.remote = s.evicted = true;
    s.export_state = "complete";
    s.export_provider = Profile(id);
    if (retained && s.retired)
        throw std::runtime_error("[differential:retired] Snapshot retired");
    return s;
}
void Control::DropBranchLocally(const std::string &database, const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto b = GetBranch(database, branch, true);
    UnlinkBranchFiles(b);
    Query(store->db, "UPDATE branches SET state='deleted',epoch=epoch+1 WHERE id=?").Text(1, b.id).Run();
    Query(store->db, "DELETE FROM branch_conflicts WHERE branch_id=?").Text(1, b.id).Run();
    tx.Commit();
}
Operation Control::GetOperation(const std::string &id) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, (std::string(OP_COLUMNS) + " WHERE id=?").c_str());
    q.Text(1, id);
    if (!q.Step())
        throw std::runtime_error("Operation missing");
    return ReadOperation(q);
}
Branch Control::GetBranch(const std::string &database, const std::string &branch, bool stable) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto id = DatabaseID(database);
    Query q(store->db, (std::string(BRANCH_COLUMNS) +
                        (stable ? " WHERE b.database_id=? AND b.id=?"
                                : " WHERE b.database_id=? AND b.name=? AND b.state='active'"))
                           .c_str());
    q.Text(1, id).Text(2, branch);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] Branch not found");
    auto b = ReadBranch(q);
    if (b.state != "active")
        throw std::runtime_error("[differential:retired] Branch deleted");
    return b;
}
Snapshot Control::GetSnapshot(const std::string &database, int64_t snapshot, bool retained) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto id = DatabaseID(database);
    Query q(store->db, (std::string(SNAPSHOT_COLUMNS) + " WHERE s.database_id=? AND s.id=?").c_str());
    q.Text(1, id).Int(2, snapshot);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] Snapshot not found");
    auto s = ReadSnapshot(q);
    auto e = Export(s.database, s.id);
    if (!e.id.empty()) {
        s.export_state = e.state;
        s.export_key = e.descriptor;
        s.export_provider = e.provider;
    }
    if (retained && s.retired)
        throw std::runtime_error("[differential:retired] Snapshot retired");
    return s;
}
void Control::Validate(const Branch &b) {
    auto current = GetBranch(b.database, b.id, true);
    if (current.epoch != b.epoch || current.head != b.head)
        throw std::runtime_error("[differential:stale_epoch] Stale branch epoch or head");
}
std::shared_ptr<BranchLock> Control::Acquire(const Branch &b) {
    auto result = std::make_shared<BranchLock>();
    auto path = std::filesystem::path(store->directory) / "locks";
    store->permissions.CreateDirectories(path);
    result->fd = store->permissions.Open(path / ("branch-" + b.id), O_RDWR | O_CLOEXEC);
    if (result->fd < 0 || flock(result->fd, LOCK_EX | LOCK_NB)) {
        Count(Counter::WriterConflicts);
        throw Error(ErrorCode::WriterBusy,
            "[differential:writer_busy] Writer busy: branch has a live attachment or management owner");
    }
    // A previous owner may have died with unsynced journaled writes: apply them before this
    // writer reads or extends the branch.
    store->ReplayJournals();
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query intent(store->db, "SELECT operation_id FROM compound_intents WHERE branch_id=?");
    intent.Text(1, b.id);
    if (intent.Step()) {
        Count(Counter::WriterConflicts);
        throw Error(ErrorCode::WriterBusy,
            "[differential:writer_busy] Branch has a pending compound operation; retry its request ID");
    }
    result->branch = GetBranch(b.database, b.id, true);
    if (result->branch.epoch == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("Writer epoch exhausted");
    ++result->branch.epoch;
    Query(store->db, "UPDATE branches SET epoch=? WHERE id=?").Int(1, result->branch.epoch).Text(2, b.id).Run();
    tx.Commit();
    return result;
}
Operation Control::BeginSnapshot(const Branch &b, const SnapshotInfo &info, const std::string &request) {
    ValidateName(info.name, true);
    if (info.author.size() > 1024 || info.message.size() > 65536 || info.extra_info.size() > 65536)
        throw std::runtime_error("Snapshot author/message/extra_info exceed 1 KiB/64 KiB/64 KiB");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto payload = Payload({b.database, b.id, info.name, info.author, info.message, info.extra_info});
    auto o = Begin("snapshot", payload, request);
    if (o.state != "complete") {
        // Head may have advanced through another completed snapshot on this writer; epoch is the fence.
        auto current = GetBranch(b.database, b.id, true);
        if (current.epoch != b.epoch)
            throw std::runtime_error("[differential:stale_epoch] Stale writer epoch");
        o.database = b.database;
        o.branch = b.id;
        Query(store->db, "UPDATE operations SET database_id=?,branch_id=? WHERE id=?")
            .Text(1, b.database)
            .Text(2, b.id)
            .Text(3, o.id)
            .Run();
    }
    tx.Commit();
    return o;
}
void Control::Event(const Branch &b, int64_t snapshot, int64_t timestamp) {
    Query(store->db, "INSERT INTO history(branch_id,database_id,snapshot_id,effective_at) VALUES(?,?,?,?)")
        .Text(1, b.id)
        .Text(2, b.database)
        .Int(3, snapshot)
        .Int(4, timestamp)
        .Run();
}
Snapshot Control::Publish(const Branch &b, const Root &root, const SnapshotInfo &info, int64_t captured_at,
                          std::optional<SnapshotParents> parents) {
    const auto &name = info.name;
    Query count(store->db, "SELECT last_snapshot FROM databases WHERE id=?");
    count.Text(1, b.database);
    count.Step();
    if (count.Int(0) == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("Snapshot ID exhausted");
    Snapshot s;
    s.database = b.database;
    s.branch = b.id;
    s.id = count.Int(0) + 1;
    s.name = name;
    s.author = info.author;
    s.message = info.message;
    s.extra_info = info.extra_info;
    s.root = root.id;
    s.size = int64_t(root.size);
    s.captured_at = captured_at;
    s.published_at = Timestamp();
    Query(store->db, "INSERT INTO snapshots(database_id,id,branch_id,name,root,published_at,captured_at,author,"
                     "commit_message,commit_extra_info) VALUES(?,?,?,NULLIF(?,''),?,?,?,NULLIF(?,''),NULLIF(?,''),"
                     "NULLIF(?,''))")
        .Text(1, s.database)
        .Int(2, s.id)
        .Text(3, s.branch)
        .Text(4, name)
        .Text(5, s.root)
        .Int(6, s.published_at)
        .Int(7, captured_at)
        .Text(8, info.author)
        .Text(9, info.message)
        .Text(10, info.extra_info)
        .Run();
    Query(store->db, "UPDATE databases SET last_snapshot=? WHERE id=?").Int(1, s.id).Text(2, s.database).Run();
    Query(store->db, "UPDATE branches SET head=?,root=? WHERE id=?").Int(1, s.id).Text(2, s.root).Text(3, b.id).Run();
    auto profile = Profile(b.database);
    if (Writers(b.database) == "single")
        RecordAncestry(s, parents.value_or(SnapshotParents{b.head ? "linear" : "root", b.head, 0}));
    if (profile != "local") {
        auto export_id = UUID();
        Query(store->db, "INSERT INTO export_roots VALUES(?,?,'pending')").Text(1, export_id).Text(2, s.root).Run();
        Query(store->db, "INSERT INTO exports(id,provider_id,database_id,snapshot_id) VALUES(?,?,?,?)")
            .Text(1, export_id)
            .Text(2, profile)
            .Text(3, s.database)
            .Int(4, s.id)
            .Run();
        s.export_state = "pending";
    }
    Event(b, s.id, s.published_at);
    return s;
}
Operation Control::PublishSnapshot(const Operation &operation, const Branch &b, const Root &root,
                                   int64_t captured_at) {
    Timer timer(Metric::SnapshotPublish);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query q(store->db, (std::string(OP_COLUMNS) + " WHERE id=?").c_str());
    q.Text(1, operation.id);
    if (!q.Step())
        throw std::runtime_error("Operation missing");
    auto o = ReadOperation(q);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    auto current = GetBranch(b.database, b.id, true);
    if (current.epoch != b.epoch || current.head != b.head)
        throw std::runtime_error(
            "[differential:stale_epoch] Stale branch epoch or head; retry the same request ID");
    if (o.kind != "snapshot" || o.database != b.database || o.branch != b.id)
        throw std::runtime_error("Operation identity mismatch");
    // Decode the length-prefixed payload, keeping request identity independent of mutable aliases.
    auto fields = DecodePayload(o.payload);
    if (fields.size() != 6)
        throw std::runtime_error("[differential:corrupt_data] Corrupt snapshot operation payload");
    SnapshotInfo info;
    info.name = fields[2];
    info.author = fields[3];
    info.message = fields[4];
    info.extra_info = fields[5];
    auto s = Publish(current, root, info, captured_at);
    o.snapshot = s.id;
    Complete(o);
    tx.Commit();
    return o;
}
Operation Control::CreateBranch(const std::string &database, int64_t snapshot, const std::string &name,
                                const std::string &request, bool clone) {
    ValidateName(name);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto source = DatabaseID(database);
    auto o = Begin(clone ? "clone" : "branch", Payload({source, std::to_string(snapshot), name}), request);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    auto s = GetSnapshot(source, snapshot);
    o.database = source;
    if (clone) {
        o.database = UUID();
        Query(store->db, "INSERT INTO databases(id,name,created_at) VALUES(?,?,?)")
            .Text(1, o.database)
            .Text(2, name)
            .Int(3, Timestamp())
            .Run();
    }
    if (clone && Profile(source) != "local")
        Query(store->db, "INSERT INTO database_profiles VALUES(?,?)")
            .Text(1, o.database)
            .Text(2, Profile(source))
            .Run();
    auto local_pool = LocalDatabasePool(source);
    if (clone && !local_pool.empty())
        Query(store->db, "INSERT INTO database_local_profiles VALUES(?,?)")
            .Text(1, o.database)
            .Text(2, local_pool)
            .Run();
    auto b = NewBranch(o.database, clone ? "main" : name, s.root, clone ? 0 : s.id, source, s.id);
    o.branch = b.id;
    if (clone)
        o.snapshot = Publish(b, {s.root, uint64_t(s.size)},
                             {"", "", "clone of " + s.database_name + " snapshot " + std::to_string(s.id), ""},
                             Timestamp(), SnapshotParents{"clone", 0, 0})
                         .id;
    else {
        o.snapshot = s.id;
        Event(b, s.id, b.created_at);
    }
    Complete(o);
    tx.Commit();
    return o;
}
Operation Control::Retire(const std::string &database, int64_t snapshot, const std::string &request) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    auto o = Begin("retire", Payload({id, std::to_string(snapshot)}), request);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    auto s = GetSnapshot(id, snapshot, false);
    Query merging(store->db, "SELECT 1 FROM operations WHERE kind='merge_publication' AND state='pending' "
                             "AND database_id=? AND snapshot_id=?");
    merging.Text(1, id).Int(2, snapshot);
    if (merging.Step())
        throw std::runtime_error("[differential:merge_pending_operation] Snapshot is the result of a pending S3 merge; retry or abort that merge before expiry");
    Query(store->db, "UPDATE snapshots SET retired=1 WHERE database_id=? AND id=?").Text(1, id).Int(2, snapshot).Run();
    o.database = id;
    o.branch = s.branch;
    o.snapshot = s.id;
    Complete(o);
    tx.Commit();
    return o;
}
Operation Control::Evict(const std::string &database, int64_t snapshot, const std::string &request) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    auto o = Begin("evict", Payload({id, std::to_string(snapshot)}), request);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    auto s = GetSnapshot(id, snapshot, false);
    if (s.export_state != "complete")
        throw std::runtime_error("[differential:unsupported_provider_capability] Only a snapshot with a "
                                 "completed S3 export can drop its local copy");
    Query(store->db, "UPDATE snapshots SET evicted=1 WHERE database_id=? AND id=?").Text(1, id).Int(2, snapshot).Run();
    o.database = id;
    o.branch = s.branch;
    o.snapshot = s.id;
    Complete(o);
    tx.Commit();
    return o;
}
Operation Control::RetryExport(const std::string &database, int64_t snapshot, const std::string &request) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    auto op = Begin("retry_export", Payload({id, std::to_string(snapshot)}), request);
    if (op.state != "complete") {
        auto source = GetSnapshot(id, snapshot, false);
        if (Export(id, snapshot).id.empty())
            throw std::runtime_error("[differential:unsupported_provider_capability] Snapshot has no S3 export");
        op.database = id;
        op.branch = source.branch;
        op.snapshot = snapshot;
        Complete(op);
    }
    tx.Commit();
    return op;
}
std::pair<Operation, std::shared_ptr<BranchLock>>
Control::PrepareCompound(const std::string &database, const std::string &branch, int64_t target,
                         const std::string &request, bool restore) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto database_id = DatabaseID(database);
    auto o = Begin(restore ? "restore" : "drop_branch", Payload({database_id, branch, std::to_string(target)}),
                   request);
    if (o.state == "complete") {
        tx.Commit();
        return {o, nullptr};
    }
    auto b = o.branch.empty() ? GetBranch(database_id, branch) : GetBranch(database_id, o.branch, true);
    auto lock = std::make_shared<BranchLock>();
    auto path = std::filesystem::path(store->directory) / "locks";
    store->permissions.CreateDirectories(path);
    lock->management_fd = store->permissions.Open(path / ("management-" + database_id), O_RDWR | O_CLOEXEC);
    if (lock->management_fd < 0 || flock(lock->management_fd, LOCK_EX | LOCK_NB)) {
        Count(Counter::WriterConflicts);
        throw Error(ErrorCode::WriterBusy, "[differential:writer_busy] Database management busy");
    }
    lock->fd = store->permissions.Open(path / ("branch-" + b.id), O_RDWR | O_CLOEXEC);
    if (lock->fd < 0 || flock(lock->fd, LOCK_EX | LOCK_NB)) {
        Count(Counter::WriterConflicts);
        throw std::runtime_error(
            "[differential:active_attachment_or_transaction] Branch has an active attachment or writer");
    }
    if (!restore) {
        // A durable pending deletion is already committed to removing its branch.
        // Do not accept another deletion that would strand that operation as the
        // last active (and fenced) branch after a crash releases management locks.
        Query count(store->db, "SELECT count(*) FROM branches b WHERE database_id=? AND state='active' "
                               "AND NOT EXISTS(SELECT 1 FROM compound_intents c JOIN operations o ON "
                               "o.id=c.operation_id WHERE c.branch_id=b.id AND o.kind='drop_branch' AND o.id!=?)");
        count.Text(1, database_id).Text(2, o.id);
        count.Step();
        if (count.Int(0) <= 1)
            throw std::runtime_error("Cannot delete the last active branch");
    }
    Query pending(store->db, "SELECT operation_id,expected_head FROM compound_intents WHERE branch_id=?");
    pending.Text(1, b.id);
    if (pending.Step()) {
        if (pending.Text(0) != o.id)
            throw Error(ErrorCode::WriterBusy, "[differential:writer_busy] Branch has another pending compound operation");
        if (pending.Int(1) != b.head)
            throw std::runtime_error("[differential:stale_epoch] Stale compound operation head");
    } else {
        std::string target_root;
        if (restore)
            target_root = GetSnapshot(database_id, target).root;
        Query(store->db, "INSERT INTO compound_intents VALUES(?,?,?,?,NULLIF(?,''))")
            .Text(1, o.id)
            .Text(2, b.id)
            .Int(3, b.head)
            .Int(4, b.epoch)
            .Text(5, target_root)
            .Run();
    }
    if (b.epoch == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("Writer epoch exhausted");
    ++b.epoch;
    lock->branch = b;
    o.database = database_id;
    o.branch = b.id;
    Query(store->db, "UPDATE branches SET epoch=? WHERE id=?").Int(1, b.epoch).Text(2, b.id).Run();
    Query(store->db, "UPDATE compound_intents SET expected_epoch=? WHERE operation_id=?")
        .Int(1, b.epoch)
        .Text(2, o.id)
        .Run();
    Query(store->db, "UPDATE operations SET database_id=?,branch_id=? WHERE id=?")
        .Text(1, database_id)
        .Text(2, b.id)
        .Text(3, o.id)
        .Run();
    tx.Commit();
    Crash("compound_intent");
    return {o, lock};
}
Operation Control::FinishCompound(const Operation &operation, const Branch &b, const Root &preserved,
                                  bool restore) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query q(store->db, (std::string(OP_COLUMNS) + " WHERE id=?").c_str());
    q.Text(1, operation.id);
    if (!q.Step())
        throw std::runtime_error("Operation missing");
    auto o = ReadOperation(q);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    Validate(b);
    Query intent(store->db, "SELECT expected_head,expected_epoch,target_root FROM compound_intents WHERE "
                            "operation_id=? AND branch_id=?");
    intent.Text(1, o.id).Text(2, b.id);
    if (!intent.Step() || intent.Int(0) != b.head || intent.Int(1) != b.epoch)
        throw std::runtime_error("[differential:stale_epoch] Stale compound intent");
    auto target = intent.Text(2);
    // Native recovery/checkpoint must have finished while exclusive ownership remains held.
    Query wal(store->db, "SELECT count(*) FROM files WHERE name IN (?,?,?) AND size>0");
    wal.Text(1, b.FileName() + ".wal").Text(2, b.FileName() + ".wal.checkpoint").Text(3, b.FileName() + ".wal.recovery");
    wal.Step();
    if (wal.Int(0))
        throw std::runtime_error("Native recovery sidecars remain; refusing main-file-only replacement");
    Query prior(store->db,
                "SELECT id FROM snapshots WHERE database_id=? AND root=? AND retired=0 ORDER BY id DESC LIMIT 1");
    prior.Text(1, b.database).Text(2, preserved.id);
    if (prior.Step()) {
        o.preservation = prior.Int(0);
        Event(b, o.preservation, Timestamp());
    } else
        o.preservation =
            Publish(b, preserved, {"", "", restore ? "preserved before restore" : "preserved before branch deletion", ""},
                    Timestamp())
                .id;
    Crash("compound_preservation");
    Query(store->db, "UPDATE files SET name=NULL WHERE name IN (?,?,?,?)")
        .Text(1, b.FileName())
        .Text(2, b.FileName() + ".wal")
        .Text(3, b.FileName() + ".wal.checkpoint")
        .Text(4, b.FileName() + ".wal.recovery")
        .Run();
    if (restore) {
        auto size = store->RootSizeTx(target);
        Query target_id(store->db, "SELECT id FROM snapshots WHERE database_id=? AND root=? ORDER BY id DESC LIMIT 1");
        target_id.Text(1, b.database).Text(2, target);
        auto label = target_id.Step() ? "restored to snapshot " + std::to_string(target_id.Int(0)) : "restored";
        // A restore is a new state change, not a merge with the copied snapshot.
        // A newly published preservation is its parent. A same-root history event
        // does not move the head and must never invent an ancestry edge.
        auto current = GetBranch(b.database, b.id, true);
        o.snapshot = Publish(current, {target, size}, {"", "", label, ""}, Timestamp(),
                             SnapshotParents{current.head ? "restore" : "root", current.head, 0}).id;
        auto file = store->CreateFileTx(b.FileName(), size, LocalDatabasePool(b.database));
        store->InstallRootTx(file, target);
    } else {
        Query(store->db, "UPDATE branches SET state='deleted' WHERE id=?").Text(1, b.id).Run();
    }
    if (b.epoch == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("Writer epoch exhausted");
    Query(store->db, "UPDATE branches SET epoch=epoch+1 WHERE id=?").Text(1, b.id).Run();
    Query(store->db, "DELETE FROM compound_intents WHERE operation_id=?").Text(1, o.id).Run();
    Complete(o);
    Crash("compound_replaced");
    tx.Commit();
    Crash("compound_completed");
    return o;
}
Snapshot Control::SnapshotAt(const std::string &database, const std::string &branch, int64_t timestamp,
                             int64_t start) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Lookups only (also while binding AT): a read snapshot, never the authority write lock.
    ReadTransaction tx(store->db);
    auto database_id = DatabaseID(database);
    if (Writers(database_id) == "shared") {
        auto current = start;
        if (!current) {
            Query head(store->db, "SELECT head FROM branches WHERE database_id=? AND (id=? OR (name=? AND "
                                  "state='active')) ORDER BY id=? DESC LIMIT 1");
            head.Text(1, database_id).Text(2, branch).Text(3, branch).Text(4, branch);
            if (!head.Step())
                throw std::runtime_error("[differential:not_found] Branch not found");
            current = head.Int(0);
        }
        std::set<int64_t> seen;
        while (current > 0 && seen.insert(current).second) {
            Snapshot s;
            try {
                s = FindSnapshot(database_id, current, false);
            } catch (const std::exception &) {
                break; // history not known beyond this point
            }
            if (!s.retired && s.published_at <= timestamp) {
                s.event_at = s.published_at;
                return s;
            }
            Query parent(store->db, "SELECT parent FROM remote_commits WHERE database_id=? AND id=?");
            parent.Text(1, database_id).Int(2, current);
            current = parent.Step() ? parent.Int(0) : ParentSnapshot(database_id, current);
        }
        throw std::runtime_error("[differential:not_found] No retained snapshot at or before timestamp on this branch");
    }
    Query identity(store->db, "SELECT id FROM branches WHERE database_id=? AND (id=? OR (name=? AND "
                              "state='active')) ORDER BY id=? DESC LIMIT 1");
    identity.Text(1, database_id).Text(2, branch).Text(3, branch).Text(4, branch);
    if (!identity.Step())
        throw std::runtime_error("[differential:not_found] Branch not found");
    auto branch_id = identity.Text(0);
    Query q(store->db, "SELECT h.snapshot_id,h.effective_at FROM history h JOIN snapshots s ON "
                       "s.database_id=h.database_id AND s.id=h.snapshot_id WHERE h.branch_id=? AND h.effective_at<=? "
                       "AND s.retired=0 ORDER BY h.effective_at DESC,h.sequence DESC LIMIT 1");
    q.Text(1, branch_id).Int(2, timestamp);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] No retained snapshot at or before timestamp on this branch");
    auto s = GetSnapshot(database_id, q.Int(0));
    s.event_at = q.Int(1);
    return s;
}
std::vector<Snapshot> Control::Snapshots(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::vector<Snapshot> result;
    Query q(store->db, (std::string(SNAPSHOT_COLUMNS) + " WHERE s.database_id=? ORDER BY s.id").c_str());
    q.Text(1, DatabaseID(database));
    while (q.Step()) {
        auto snapshot = ReadSnapshot(q);
        auto e = Export(snapshot.database, snapshot.id);
        if (!e.id.empty()) {
            snapshot.export_state = e.state;
            snapshot.export_key = e.descriptor;
            snapshot.export_provider = e.provider;
        }
        result.push_back(std::move(snapshot));
    }
    // Shared databases: snapshots other writers published, known from their commit records.
    std::vector<int64_t> remote;
    {
        Query r(store->db, "SELECT c.id FROM remote_commits c WHERE c.database_id=? AND NOT EXISTS(SELECT 1 FROM "
                           "snapshots s WHERE s.database_id=c.database_id AND s.id=c.id) ORDER BY c.id");
        r.Text(1, DatabaseID(database));
        while (r.Step())
            remote.push_back(r.Int(0));
    }
    for (auto n : remote)
        result.push_back(FindSnapshot(database, n, false));
    std::sort(result.begin(), result.end(), [](const Snapshot &a, const Snapshot &b) { return a.id < b.id; });
    return result;
}
std::vector<Branch> Control::Branches(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::vector<Branch> result;
    Query q(store->db, (std::string(BRANCH_COLUMNS) + " WHERE b.database_id=? ORDER BY b.created_at,b.id").c_str());
    q.Text(1, DatabaseID(database));
    while (q.Step())
        result.push_back(ReadBranch(q));
    return result;
}
std::vector<Operation> Control::Operations() {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::vector<Operation> result;
    Query q(store->db, (std::string(OP_COLUMNS) + " ORDER BY created_at,id").c_str());
    while (q.Step()) {
        auto o = ReadOperation(q);
        if ((o.kind == "merge" || o.kind == "merge_branch") && MergeAborted(o))
            o.state = "aborted";
        if (o.state == "complete" && (o.snapshot || o.preservation) && o.kind != "retire" && o.kind != "evict" &&
            o.kind != "snapshot_ancestry") {
            for (auto id : {o.snapshot, o.preservation})
                if (id && Export(o.database, id).state == "pending")
                    o.state = "local_complete_export_pending";
        }
        result.push_back(std::move(o));
    }
    return result;
}
std::vector<DatabaseRecord> Control::Databases() {
    std::vector<DatabaseRecord> result;
    auto cursor = List("databases");
    std::vector<MetadataCell> row;
    while (cursor->Next(row))
        result.push_back({row[0].text, row[1].text, row[2].text, row[3].number, row[4].number, row[5].text});
    return result;
}
std::vector<ProviderRecord> Control::Providers() {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::vector<ProviderRecord> result;
    Query q(store->db, "SELECT id FROM providers ORDER BY name");
    while (q.Step())
        result.push_back(Provider(q.Text(0)));
    return result;
}
std::string Control::Profile(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT provider_id FROM database_profiles WHERE database_id=?");
    q.Text(1, DatabaseID(database));
    return q.Step() ? q.Text(0) : "local";
}
std::string Control::LocalProvider(const std::string &name) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT id FROM local_pools WHERE id=? OR name=? ORDER BY id=? DESC");
    q.Text(1, name).Text(2, name).Text(3, name);
    return q.Step() ? q.Text(0) : "";
}
std::string Control::LocalDatabasePool(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT pool FROM database_local_profiles WHERE database_id=?");
    q.Text(1, DatabaseID(database));
    return q.Step() ? q.Text(0) : "";
}
void Control::AssignProvider(const std::string &database, const std::string &provider) {
    if (provider == "local")
        return;
    auto pool = LocalProvider(provider);
    if (!pool.empty())
        Query(store->db, "INSERT INTO database_local_profiles VALUES(?,?)").Text(1, database).Text(2, pool).Run();
    else
        Query(store->db, "INSERT INTO database_profiles VALUES(?,?)")
            .Text(1, database)
            .Text(2, Provider(provider).id)
            .Run();
}
std::string Control::FindDatabase(const std::string &name) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT id FROM databases WHERE id=? OR name=? ORDER BY id=? DESC");
    q.Text(1, name).Text(2, name).Text(3, name);
    return q.Step() ? q.Text(0) : "";
}
std::string Control::EnsureS3Provider(const S3Config &config, const std::vector<std::string> &explicit_options) {
    if (config.prefix.empty())
        throw std::runtime_error("S3 DATA_PATH needs an owned prefix: s3://bucket/prefix/");
    auto location = config.Identity();
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query existing(store->db, "SELECT id FROM providers WHERE name=?");
    existing.Text(1, location);
    if (existing.Step()) {
        auto id = existing.Text(0);
        auto stored = Provider(id).config;
        // Provider configuration is immutable: options given again must agree with it.
        auto differs = [&](const std::string &option) {
            if (option == "region")
                return stored.region != config.region;
            if (option == "endpoint")
                return stored.endpoint != config.endpoint;
            if (option == "credential_profile")
                return stored.credential_profile != config.credential_profile;
            if (option == "path_style")
                return stored.path_style != config.path_style;
            if (option == "allow_http")
                return stored.development_http != config.development_http;
            if (option == "max_requests")
                return stored.max_requests != config.max_requests;
            if (option == "retries")
                return stored.retries != config.retries;
            if (option == "connect_timeout_ms")
                return stored.connect_timeout_ms != config.connect_timeout_ms;
            if (option == "request_timeout_ms")
                return stored.request_timeout_ms != config.request_timeout_ms;
            if (option == "max_range_bytes")
                return stored.max_range_bytes != config.max_range_bytes;
            if (option == "multipart_threshold")
                return stored.multipart_threshold != config.multipart_threshold;
            return false;
        };
        for (auto &option : explicit_options)
            if (differs(option))
                throw std::runtime_error("[differential:name_conflict] " + location + " is already registered with a "
                                         "different " + option);
        tx.Commit();
        return id;
    }
    auto id = UUID();
    Query(store->db, "INSERT INTO providers VALUES(?,?,?,?,?,?,?,?,?)")
        .Text(1, id)
        .Text(2, location)
        .Text(3, config.bucket)
        .Text(4, config.prefix)
        .Text(5, config.region)
        .Text(6, config.endpoint)
        .Text(7, config.credential_profile)
        .Int(8, config.path_style)
        .Int(9, config.development_http)
        .Run();
    Query(store->db, "INSERT INTO provider_limits VALUES(?,?,?,?,?,?,?)")
        .Text(1, id)
        .Int(2, config.max_requests)
        .Int(3, config.retries)
        .Int(4, config.connect_timeout_ms)
        .Int(5, config.request_timeout_ms)
        .Int(6, int64_t(config.max_range_bytes))
        .Int(7, int64_t(config.multipart_threshold))
        .Run();
    tx.Commit();
    return id;
}
std::string Control::EnsureLocalPool(const std::string &path) {
    auto canonical = LocalPool::Canonical(path);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query existing(store->db, "SELECT id FROM local_pools WHERE path=? ORDER BY id LIMIT 1");
    existing.Text(1, canonical);
    if (existing.Step()) {
        auto id = existing.Text(0);
        tx.Commit();
        return id;
    }
    auto id = UUID();
    Query(store->db, "INSERT INTO local_pools(id,name,path,repository_id) VALUES(?,?,?,?)")
        .Text(1, id)
        .Text(2, canonical)
        .Text(3, canonical)
        .Text(4, RepositoryID())
        .Run();
    tx.Commit();
    return id;
}
std::string Control::DataPath(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto id = DatabaseID(database);
    Query s3(store->db, "SELECT p.bucket,p.prefix FROM database_profiles f JOIN providers p ON p.id=f.provider_id "
                        "WHERE f.database_id=?");
    s3.Text(1, id);
    if (s3.Step())
        return "s3://" + s3.Text(0) + "/" + s3.Text(1);
    Query local(store->db, "SELECT l.path FROM database_local_profiles f JOIN local_pools l ON l.id=f.pool WHERE "
                           "f.database_id=?");
    local.Text(1, id);
    return local.Step() ? local.Text(0) : "";
}
std::vector<Snapshot> Control::SelectSnapshots(const std::string &database, const std::vector<int64_t> &versions,
                                               std::optional<int64_t> older_than, bool for_eviction) {
    if (versions.empty() && !older_than)
        throw std::runtime_error("Select snapshots with versions => [...] and/or older_than => <timestamp>");
    std::vector<Snapshot> result;
    // Explicit versions are always selected (repeating a request replays its result); only
    // the age criterion skips snapshots that were already expired or evicted.
    for (auto version : versions)
        result.push_back(FindSnapshot(database, version, false));
    if (older_than)
        for (auto &s : Snapshots(database)) {
            bool listed = std::any_of(result.begin(), result.end(), [&](const Snapshot &r) { return r.id == s.id; });
            bool eligible = !s.retired && (!for_eviction || (!s.evicted && s.export_state == "complete"));
            if (!listed && eligible && s.published_at < *older_than)
                result.push_back(s);
        }
    std::sort(result.begin(), result.end(), [](const Snapshot &a, const Snapshot &b) { return a.id < b.id; });
    return result;
}
int64_t Control::CurrentSnapshot(const std::string &database, const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    if (!branch.empty())
        return GetBranch(database, branch, true).head;
    Query q(store->db, "SELECT last_snapshot FROM databases WHERE id=?");
    q.Text(1, DatabaseID(database));
    return q.Step() ? q.Int(0) : 0;
}
Operation Control::CheckpointSnapshot(const Branch &lease, const Root &root) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Operation o;
    auto current = GetBranch(lease.database, lease.id, true);
    Query intent(store->db, "SELECT 1 FROM compound_intents WHERE branch_id=?");
    intent.Text(1, lease.id);
    // A newer owner or a pending compound operation owns the branch history; an unchanged
    // image needs no new snapshot.
    if (current.epoch != lease.epoch || intent.Step() || current.root == root.id) {
        tx.Commit();
        return o;
    }
    o = Begin("snapshot", Payload({current.database, current.id, "", "", "checkpoint", ""}), "");
    o.database = current.database;
    o.branch = current.id;
    o.snapshot = Publish(current, root, {"", "", "checkpoint", ""}, Timestamp()).id;
    Complete(o);
    tx.Commit();
    return o;
}
std::vector<int64_t> Control::PendingExports(const std::string &database) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    std::vector<int64_t> result;
    Query q(store->db, "SELECT e.snapshot_id FROM exports e JOIN export_roots r ON r.export_id=e.id WHERE "
                       "e.database_id=? AND r.state='pending' ORDER BY e.snapshot_id");
    q.Text(1, DatabaseID(database));
    while (q.Step())
        result.push_back(q.Int(0));
    return result;
}
std::vector<ProviderRecord> Control::S3Providers() {
    std::vector<std::string> ids;
    {
        std::lock_guard<std::recursive_mutex> guard(store->mutex);
        Query q(store->db, "SELECT id FROM providers ORDER BY id");
        while (q.Step())
            ids.push_back(q.Text(0));
    }
    std::vector<ProviderRecord> providers;
    for (auto &id : ids)
        providers.push_back(Provider(id));
    return providers;
}
ProviderRecord Control::Provider(const std::string &name) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT id,name,bucket,prefix,region,endpoint,credential_profile,path_style,development_http "
                       "FROM providers WHERE id=? OR name=? ORDER BY id=? DESC");
    q.Text(1, name).Text(2, name).Text(3, name);
    if (!q.Step())
        throw std::runtime_error("[differential:not_found] Provider not found");
    ProviderRecord p;
    p.id = q.Text(0);
    p.name = q.Text(1);
    p.config.bucket = q.Text(2);
    p.config.prefix = q.Text(3);
    p.config.region = q.Text(4);
    p.config.endpoint = q.Text(5);
    p.config.credential_profile = q.Text(6);
    p.config.path_style = q.Int(7) != 0;
    p.config.development_http = q.Int(8) != 0;
    Query limits(store->db, "SELECT max_requests,retries,connect_timeout_ms,request_timeout_ms,max_range_bytes,"
                            "multipart_threshold FROM provider_limits WHERE provider_id=?");
    limits.Text(1, p.id);
    if (limits.Step()) {
        p.config.max_requests = unsigned(limits.Int(0));
        p.config.retries = unsigned(limits.Int(1));
        p.config.connect_timeout_ms = unsigned(limits.Int(2));
        p.config.request_timeout_ms = unsigned(limits.Int(3));
        p.config.max_range_bytes = uint64_t(limits.Int(4));
        p.config.multipart_threshold = uint64_t(limits.Int(5));
    }
    return p;
}
std::optional<HeadWrite> Control::HeadWriteFor(const std::string &provider, const std::string &key) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT etag,write_id,pending,snapshot FROM branch_head_writes WHERE provider_id=? AND key=?");
    q.Text(1, provider).Text(2, key);
    if (!q.Step())
        return std::nullopt;
    return HeadWrite{q.Text(0), q.Text(1), q.Text(2), q.Int(3)};
}
void Control::RecordHeadWrite(const std::string &provider, const std::string &key, const HeadWrite &write) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query(store->db, "INSERT INTO branch_head_writes VALUES(?,?,?,?,?,?) ON CONFLICT(provider_id,key) DO UPDATE SET "
                     "etag=excluded.etag,write_id=excluded.write_id,pending=excluded.pending,snapshot=excluded.snapshot")
        .Text(1, provider)
        .Text(2, key)
        .Text(3, write.etag)
        .Text(4, write.write_id)
        .Text(5, write.pending)
        .Int(6, write.snapshot)
        .Run();
}
std::vector<BranchHead> Control::ExportedHeads(const std::string &database, std::string &provider) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto id = DatabaseID(database);
    provider = Profile(id);
    std::vector<BranchHead> heads;
    if (provider == "local") {
        provider.clear();
        return heads;
    }
    Query q(store->db, "SELECT b.id,b.name,d.name,h.snapshot_id,s.published_at,e.descriptor FROM branches b "
                       "JOIN databases d ON d.id=b.database_id JOIN history h ON h.branch_id=b.id "
                       "JOIN snapshots s ON s.database_id=h.database_id AND s.id=h.snapshot_id AND s.retired=0 "
                       "JOIN exports e ON e.database_id=s.database_id AND e.snapshot_id=s.id AND e.provider_id=? "
                       "AND e.descriptor!='' JOIN export_roots r ON r.export_id=e.id AND r.state='complete' "
                       "WHERE b.database_id=? AND b.state='active' ORDER BY b.id,h.sequence DESC");
    q.Text(1, provider).Text(2, id);
    while (q.Step()) {
        if (!heads.empty() && heads.back().branch == q.Text(0))
            continue;
        BranchHead head;
        head.database = id;
        head.branch = q.Text(0);
        head.branch_name = q.Text(1);
        head.database_name = q.Text(2);
        head.snapshot = q.Int(3);
        head.published_at = q.Int(4);
        head.descriptor = q.Text(5);
        heads.push_back(head);
    }
    return heads;
}
ExportRecord Control::Export(const std::string &database, int64_t snapshot) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT e.id,e.provider_id,e.database_id,e.snapshot_id,r.root,r.state,e.descriptor FROM exports "
                       "e JOIN export_roots r ON r.export_id=e.id WHERE e.database_id=? AND e.snapshot_id=?");
    q.Text(1, database).Int(2, snapshot);
    ExportRecord e;
    if (!q.Step())
        return e;
    e.id = q.Text(0);
    e.provider = q.Text(1);
    e.database = q.Text(2);
    e.snapshot = q.Int(3);
    e.root = q.Text(4);
    e.state = q.Text(5);
    e.descriptor = q.Text(6);
    return e;
}
Operation Control::BeginImport(const std::string &name, const std::string &source, const std::string &request,
                               const std::string &provider, const std::string &kind) {
    ValidateName(name);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto o = Begin(kind, Payload({name, source, provider}), request);
    tx.Commit();
    return o;
}
Operation Control::CompleteImport(const Operation &operation, const Root &root, const std::string &source_database,
                                  int64_t source_snapshot, const std::string &provider) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query q(store->db, (std::string(OP_COLUMNS) + " WHERE id=?").c_str());
    q.Text(1, operation.id);
    if (!q.Step())
        throw std::runtime_error("Import intent missing");
    auto o = ReadOperation(q);
    if (o.state == "complete") {
        tx.Commit();
        return o;
    }
    if (o.kind != "recover_export" && o.kind != "import_native")
        throw std::runtime_error("Operation kind mismatch");
    auto colon = o.payload.find(':');
    auto length = std::stoull(o.payload.substr(0, colon));
    auto name = o.payload.substr(colon + 1, length);
    o.database = UUID();
    Query(store->db, "INSERT INTO databases(id,name,created_at) VALUES(?,?,?)")
        .Text(1, o.database)
        .Text(2, name)
        .Int(3, Timestamp())
        .Run();
    AssignProvider(o.database, provider);
    auto b = NewBranch(o.database, "main", root.id, 0, source_database, source_snapshot);
    o.branch = b.id;
    o.snapshot =
        Publish(b, root, {"", "", o.kind == "import_native" ? "imported native file" : "recovered from export", ""},
                Timestamp(), SnapshotParents{"import", 0, 0})
            .id;
    Query(store->db, "UPDATE files SET name=NULL WHERE name=?").Text(1, "imports/" + o.id).Run();
    Complete(o);
    tx.Commit();
    return o;
}
bool Control::RemoteVerified(const std::string &provider, const std::string &key, const std::string &hash,
                             uint64_t size) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT hash,size FROM verified_remote_objects WHERE provider_id=? AND key=?");
    q.Text(1, provider).Text(2, key);
    return q.Step() && q.Text(0) == hash && uint64_t(q.Int(1)) == size;
}
void Control::RecordRemoteVerified(const std::string &provider, const std::string &key, const std::string &hash,
                                   uint64_t size) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // A cache of uploads: losing it after a crash only means a conditional re-upload. No sync of its own.
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    Query(store->db, "INSERT INTO verified_remote_objects VALUES(?,?,?,?) ON CONFLICT(provider_id,key) DO "
                     "UPDATE SET hash=excluded.hash,size=excluded.size")
        .Text(1, provider)
        .Text(2, key)
        .Text(3, hash)
        .Int(4, int64_t(size))
        .Run();
    tx.Commit();
}
std::map<std::string, RemoteChunk> Control::FindRemoteChunks(const std::string &provider,
                                                             const std::vector<std::string> &hashes, bool reusable) {
    std::map<std::string, RemoteChunk> result;
    store->WithReader([&](Reader &reader) {
        for (auto &hash : hashes) {
            if (result.count(hash))
                continue;
            Query q(reader.db,
                    reader.Statement(reusable ? "SELECT c.pack,c.pool,c.slot FROM remote_chunks c WHERE c.provider_id=?1 "
                                                "AND c.hash=?2 AND NOT EXISTS(SELECT 1 FROM remote_condemned d WHERE "
                                                "d.provider_id=?1 AND d.key='dbs/'||c.pool||'/packs/'||c.pack)"
                                              : "SELECT pack,pool,slot FROM remote_chunks WHERE provider_id=? AND hash=?"));
            q.Text(1, provider).Text(2, hash);
            if (q.Step())
                result.emplace(hash, RemoteChunk{q.Text(0), q.Text(1), uint64_t(q.Int(2))});
        }
    });
    return result;
}
void Control::RecordPack(const std::string &provider, const std::string &pool, const std::string &pack,
                         uint64_t size, const std::vector<std::string> &chunks) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Synced with the export's completion (a later FULL commit); before that only a cache of uploads.
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    Query(store->db, "INSERT OR IGNORE INTO remote_packs VALUES(?,?,?,?)")
        .Text(1, provider)
        .Text(2, pool)
        .Text(3, pack)
        .Int(4, int64_t(size))
        .Run();
    for (size_t slot = 0; slot < chunks.size(); ++slot)
        Query(store->db, "INSERT INTO remote_chunks VALUES(?,?,?,?,?) ON CONFLICT(provider_id,hash) DO UPDATE SET "
                         "pool=excluded.pool,pack=excluded.pack,slot=excluded.slot")
            .Text(1, provider)
            .Text(2, chunks[slot])
            .Text(3, pool)
            .Text(4, pack)
            .Int(5, int64_t(slot))
            .Run();
    tx.Commit();
}
bool Control::AbandonExport(const std::string &database, int64_t snapshot) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Releases the pending retention root (gc_export_release); the snapshot itself stays local.
    Query(store->db, "UPDATE export_roots SET state='abandoned' WHERE state='pending' AND export_id=(SELECT id FROM "
                     "exports WHERE database_id=? AND snapshot_id=?)")
        .Text(1, DatabaseID(database))
        .Int(2, snapshot)
        .Run();
    return sqlite3_changes(store->db) > 0;
}
void Control::AdvanceSnapshotNumbers(const std::string &database, int64_t taken) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query(store->db, "UPDATE databases SET last_snapshot=max(last_snapshot,?) WHERE id=?")
        .Int(1, taken)
        .Text(2, DatabaseID(database))
        .Run();
}
std::string Control::BranchName(const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT name FROM branches WHERE id=?");
    q.Text(1, branch);
    return q.Step() ? q.Text(0) : "";
}
int64_t Control::ParentSnapshot(const std::string &database, int64_t snapshot) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // The first time the snapshot entered its publishing branch's history; the entry before it.
    Query q(store->db, "SELECT p.snapshot_id FROM snapshots s JOIN history h ON h.branch_id=s.branch_id AND "
                       "h.database_id=s.database_id AND h.snapshot_id=s.id JOIN history p ON p.branch_id=h.branch_id "
                       "AND p.sequence<h.sequence WHERE s.database_id=? AND s.id=? ORDER BY h.sequence ASC, "
                       "p.sequence DESC LIMIT 1");
    q.Text(1, DatabaseID(database)).Int(2, snapshot);
    return q.Step() ? q.Int(0) : 0;
}
namespace {
// "dbs/<uuid>/packs/<sha256>" -> {pool, pack}; false for other keys.
bool PackKey(const std::string &key, std::string &pool, std::string &pack) {
    if (key.rfind("dbs/", 0) != 0 || key.size() != 4 + 36 + 7 + 64 || key.compare(40, 7, "/packs/") != 0)
        return false;
    pool = key.substr(4, 36);
    pack = key.substr(47);
    return true;
}
} // namespace
bool Control::KnownCondemnation(const std::string &provider, const std::string &record) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT 1 FROM remote_gc_records WHERE provider_id=? AND key=?");
    q.Text(1, provider).Text(2, record);
    return q.Step();
}
void Control::RecordCondemnation(const std::string &provider, const std::string &record, int64_t at,
                                 const std::vector<std::string> &keys, bool learned) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query known(store->db, "SELECT 1 FROM remote_gc_records WHERE provider_id=? AND key=?");
    known.Text(1, provider).Text(2, record);
    if (known.Step()) {
        tx.Commit();
        return;
    }
    for (auto &key : keys) {
        Query(store->db, "INSERT INTO remote_condemned VALUES(?,?,?) ON CONFLICT(provider_id,key) DO UPDATE SET "
                         "condemned_at=min(condemned_at,excluded.condemned_at)")
            .Text(1, provider)
            .Text(2, key)
            .Int(3, at)
            .Run();
        // A later export uploads it again rather than trusting an earlier verification.
        Query(store->db, "DELETE FROM verified_remote_objects WHERE provider_id=? AND key=?").Text(1, provider).Text(2, key).Run();
        std::string pool, pack;
        if (!PackKey(key, pool, pack))
            continue;
        // Exports place these chunks in new packs; locations local remote-only objects read stay until
        // those objects are stored locally.
        Query(store->db, "DELETE FROM remote_chunks WHERE provider_id=? AND pool=? AND pack=? AND hash NOT IN "
                         "(SELECT r.hash FROM remote_objects r JOIN objects o ON o.hash=r.hash WHERE o.segment IS NULL)")
            .Text(1, provider)
            .Text(2, pool)
            .Text(3, pack)
            .Run();
        Query(store->db, "DELETE FROM remote_packs WHERE provider_id=?1 AND pool=?2 AND pack=?3 AND NOT EXISTS(SELECT 1 "
                         "FROM remote_chunks c WHERE c.provider_id=?1 AND c.pool=?2 AND c.pack=?3)")
            .Text(1, provider)
            .Text(2, pool)
            .Text(3, pack)
            .Run();
    }
    if (learned)
        Query(store->db, "INSERT INTO remote_gc_records VALUES(?,?)").Text(1, provider).Text(2, record).Run();
    tx.Commit();
}
std::map<std::string, int64_t> Control::Condemnations(const std::string &provider) {
    std::map<std::string, int64_t> result;
    store->WithReader([&](Reader &reader) {
        Query q(reader.db, "SELECT key,condemned_at FROM remote_condemned WHERE provider_id=?");
        q.Text(1, provider);
        while (q.Step())
            result.emplace(q.Text(0), q.Int(1));
    });
    return result;
}
bool Control::Condemned(const std::string &provider, const std::string &key) {
    bool found = false;
    store->WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT 1 FROM remote_condemned WHERE provider_id=? AND key=?"));
        q.Text(1, provider).Text(2, key);
        found = q.Step();
    });
    return found;
}
std::vector<std::string> Control::LiveExportDescriptors(const std::string &provider) {
    std::vector<std::string> result;
    store->WithReader([&](Reader &reader) {
        Query q(reader.db, "SELECT DISTINCT e.descriptor FROM exports e JOIN export_roots r ON r.export_id=e.id JOIN "
                           "snapshots s ON s.database_id=e.database_id AND s.id=e.snapshot_id WHERE e.provider_id=? AND "
                           "r.state='complete' AND s.retired=0 AND e.descriptor!=''");
        q.Text(1, provider);
        while (q.Step())
            result.push_back(q.Text(0));
    });
    return result;
}
std::set<std::string> Control::RemoteOnlyPacks(const std::string &provider) {
    std::set<std::string> result;
    store->WithReader([&](Reader &reader) {
        Query q(reader.db, "SELECT DISTINCT 'dbs/'||c.pool||'/packs/'||c.pack FROM remote_objects r JOIN objects o ON "
                           "o.hash=r.hash AND o.segment IS NULL JOIN remote_chunks c ON c.hash=r.hash AND c.provider_id=?");
        q.Text(1, provider);
        while (q.Step())
            result.insert(q.Text(0));
    });
    return result;
}
std::vector<std::string> Control::RemoteOnlyObjectsIn(const std::string &provider, const std::set<std::string> &packs) {
    std::vector<std::string> result;
    store->WithReader([&](Reader &reader) {
        for (auto &key : packs) {
            std::string pool, pack;
            if (!PackKey(key, pool, pack))
                continue;
            Query q(reader.db, reader.Statement("SELECT c.hash FROM remote_chunks c JOIN remote_objects r ON "
                                                "r.hash=c.hash JOIN objects o ON o.hash=c.hash AND o.segment IS NULL "
                                                "WHERE c.provider_id=? AND c.pool=? AND c.pack=?"));
            q.Text(1, provider).Text(2, pool).Text(3, pack);
            while (q.Step())
                result.push_back(q.Text(0));
        }
    });
    return result;
}
void Control::MaterializeRemoteObjects(const std::map<std::string, SharedBytes> &objects) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    store->MaterializeObjectsTx(objects);
    for (auto &object : objects)
        Query(store->db, "DELETE FROM remote_objects WHERE hash=?").Text(1, object.first).Run();
    tx.Commit();
}
// An object with no references is cached with one row whose reference is '' (cached, references nothing).
Control::RemoteReferences Control::LoadRemoteReferences(const std::string &provider) {
    RemoteReferences result;
    store->WithReader([&](Reader &reader) {
        Query packs(reader.db, "SELECT index_key,pack_key FROM remote_index_packs WHERE provider_id=?");
        packs.Text(1, provider);
        while (packs.Step()) {
            auto &entry = result.index_packs[packs.Text(0)];
            if (!packs.Text(1).empty())
                entry.push_back(packs.Text(1));
        }
        Query pages(reader.db, "SELECT descriptor_key,index_key,size FROM remote_descriptor_pages WHERE provider_id=?");
        pages.Text(1, provider);
        while (pages.Step()) {
            auto &entry = result.descriptor_pages[pages.Text(0)];
            if (!pages.Text(1).empty())
                entry.emplace_back(pages.Text(1), uint64_t(pages.Int(2)));
        }
        Query commits(reader.db, "SELECT commit_key,descriptor_key FROM remote_commit_descriptors WHERE provider_id=?");
        commits.Text(1, provider);
        while (commits.Step())
            result.commit_descriptors.emplace(commits.Text(0), commits.Text(1));
    });
    return result;
}
void Control::StoreRemoteReferences(const std::string &provider, const RemoteReferences &references) {
    if (references.index_packs.empty() && references.descriptor_pages.empty() && references.commit_descriptors.empty())
        return;
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false); // a cache: rebuilt from S3 if lost
    for (auto &entry : references.index_packs) {
        auto packs = entry.second.empty() ? std::vector<std::string> {""} : entry.second;
        for (auto &pack : packs)
            Query(store->db, "INSERT OR IGNORE INTO remote_index_packs VALUES(?,?,?)")
                .Text(1, provider)
                .Text(2, entry.first)
                .Text(3, pack)
                .Run();
    }
    for (auto &entry : references.descriptor_pages) {
        auto pages = entry.second.empty() ? std::vector<std::pair<std::string, uint64_t>> {{"", 0}} : entry.second;
        for (auto &page : pages)
            Query(store->db, "INSERT OR IGNORE INTO remote_descriptor_pages VALUES(?,?,?,?)")
                .Text(1, provider)
                .Text(2, entry.first)
                .Text(3, page.first)
                .Int(4, int64_t(page.second))
                .Run();
    }
    for (auto &entry : references.commit_descriptors)
        Query(store->db, "INSERT OR IGNORE INTO remote_commit_descriptors VALUES(?,?,?)")
            .Text(1, provider)
            .Text(2, entry.first)
            .Text(3, entry.second)
            .Run();
    tx.Commit();
}
void Control::ForgetRemoteObjects(const std::string &provider, const std::vector<std::string> &keys) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    for (auto &key : keys) {
        Query(store->db, "DELETE FROM verified_remote_objects WHERE provider_id=? AND key=?").Text(1, provider).Text(2, key).Run();
        Query(store->db, "DELETE FROM remote_index_packs WHERE provider_id=? AND index_key=?").Text(1, provider).Text(2, key).Run();
        Query(store->db, "DELETE FROM remote_descriptor_pages WHERE provider_id=? AND descriptor_key=?")
            .Text(1, provider)
            .Text(2, key)
            .Run();
        std::string pool, pack;
        if (!PackKey(key, pool, pack))
            continue;
        Query(store->db, "DELETE FROM remote_chunks WHERE provider_id=? AND pool=? AND pack=?")
            .Text(1, provider)
            .Text(2, pool)
            .Text(3, pack)
            .Run();
        Query(store->db, "DELETE FROM remote_packs WHERE provider_id=? AND pool=? AND pack=?")
            .Text(1, provider)
            .Text(2, pool)
            .Text(3, pack)
            .Run();
    }
    tx.Commit();
}
std::vector<std::pair<std::string, int64_t>> Control::PruneMetadata(int64_t older_than, bool dry_run) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    // Completed operations that no pending compound intent still names. Snapshot ancestry is the
    // parent graph of branch merging and is never pruned. A merge companion (frozen policy, context,
    // guard and choices, attempt, publication, reservation, abort marker, target audit) goes only with
    // its request: a pending or aborted merge keeps everything that defines its retry or terminal state.
    std::set<std::string> operations;
    std::vector<std::pair<std::string, std::string>> companions; // companion id, merge operation id
    {
        Query q(store->db, "SELECT id,kind,payload,branch_id FROM operations o WHERE o.state='complete' AND "
                           "o.created_at<? AND o.kind<>'snapshot_ancestry' AND NOT EXISTS(SELECT 1 FROM "
                           "compound_intents c WHERE c.operation_id=o.id)");
        q.Int(1, older_than);
        static const std::set<std::string> payload_parent = {"merge_target_result", "merge_schema",
            "merge_expression_context", "merge_expected_inputs", "merge_attempt", "merge_publication"};
        while (q.Step()) {
            auto id = q.Text(0), kind = q.Text(1);
            if (payload_parent.count(kind)) {
                auto fields = DecodePayload(q.Text(2));
                companions.emplace_back(id, fields.size() > 1 ? fields[1] : "");
            } else if (kind == "merge_aborted" || kind == "merge_destination")
                companions.emplace_back(id, q.Text(2)); // the payload is the merge operation id
            else if (kind == "merge_resolutions")
                companions.emplace_back(id, q.Text(3));
            else
                operations.insert(id);
        }
    }
    Query parent(store->db, "SELECT 1 FROM operations WHERE id=?");
    for (auto &companion : companions) {
        parent.Text(1, companion.second);
        if (!companion.second.empty() && (operations.count(companion.second) || !parent.Step()))
            operations.insert(companion.first);
        sqlite3_reset(parent.s);
        sqlite3_clear_bindings(parent.s);
    }
    // History of retired snapshots (time travel and branch heads only use retained ones), except
    // snapshots whose export is still pending (their commit record names a parent from history).
    const std::string history = "SELECT h.sequence FROM history h JOIN snapshots s ON s.database_id=h.database_id AND "
                                "s.id=h.snapshot_id WHERE s.retired=1 AND h.effective_at<?1 AND NOT EXISTS(SELECT 1 "
                                "FROM exports e JOIN export_roots r ON r.export_id=e.id WHERE "
                                "e.database_id=s.database_id AND e.snapshot_id=s.id AND r.state='pending')";
    auto count = [&](const std::string &sql) {
        Query q(store->db, ("SELECT count(*) FROM (" + sql + ")").c_str());
        q.Int(1, older_than);
        q.Step();
        return q.Int(0);
    };
    std::vector<std::pair<std::string, int64_t>> result = {{"operations", int64_t(operations.size())},
                                                           {"history", count(history)}};
    if (!dry_run) {
        for (auto &id : operations) {
            for (auto table : {"collection_results", "compaction_details"})
                Query(store->db, (std::string("DELETE FROM ") + table + " WHERE operation_id=?").c_str()).Text(1, id).Run();
            Query(store->db, "DELETE FROM operations WHERE id=?").Text(1, id).Run();
        }
        Query(store->db, ("DELETE FROM history WHERE sequence IN (" + history + ")").c_str()).Int(1, older_than).Run();
    }
    tx.Commit();
    return result;
}
void Control::ExportComplete(const std::string &id, const std::string &descriptor) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    Query q(store->db, "SELECT descriptor FROM exports WHERE id=?");
    q.Text(1, id);
    if (!q.Step())
        throw std::runtime_error("Export intent missing");
    if (!q.Text(0).empty() && q.Text(0) != descriptor)
        throw std::runtime_error("[differential:corrupt_data] Immutable export identity conflict");
    Query(store->db, "UPDATE exports SET descriptor=? WHERE id=?").Text(1, descriptor).Text(2, id).Run();
    Query(store->db, "UPDATE export_roots SET state='complete' WHERE export_id=?").Text(1, id).Run();
    tx.Commit();
}
} // namespace differential

namespace differential {
std::shared_ptr<Pin> Control::MergeReader(const std::string &root) {
    auto pin = NewPin(root);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    for (auto it = merge_stages.begin(); it != merge_stages.end();)
        if (it->second.first.expired()) it = merge_stages.erase(it); else ++it;
    merge_stages[pin->id] = {pin, ""};
    return pin;
}
std::shared_ptr<Pin> Control::MergeStage(const std::string &root, const std::string &pool,
                                         std::shared_ptr<MergeWriteBudget> budget) {
    auto pin = NewPin(root);
    pin->merge_stage = true;
    pin->merge_budget = std::move(budget);
    store->Fork(root, "merge_stage/" + pin->id, pool);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    for (auto it = merge_stages.begin(); it != merge_stages.end();)
        if (it->second.first.expired())
            it = merge_stages.erase(it);
        else
            ++it;
    merge_stages[pin->id] = {pin, pool};
    return pin;
}
Operation Control::BeginMerge(const std::string &database, int64_t base, int64_t ours, int64_t theirs,
                              const std::string &name, const std::string &request, const std::string &resolution_spec,
                              const std::string &schema_mode) {
    control::ValidateName(name);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto id = DatabaseID(database);
    if (Writers(id) != "single")
        throw std::runtime_error(
            "[differential:merge_unsupported] Snapshot merge requires a single-writer database");
    if (Profile(id) != "local" && request.empty())
        throw std::runtime_error("[differential:merge_request_required] S3 merge requires a nonempty request_id for recovery");
    std::vector<std::string> arguments{id, std::to_string(base), std::to_string(ours),
                                       std::to_string(theirs), name, "logical-cell-v1"};
    // The semantic request identity is the five caller arguments. Completed records
    // remain replayable after an algorithm upgrade; unknown pending algorithms must not
    // be silently recomputed by a different implementation.
    if (!request.empty()) {
        Query previous(store->db, (std::string(OP_COLUMNS) + " WHERE request=?").c_str());
        previous.Text(1, request);
        if (previous.Step()) {
            auto old = ReadOperation(previous);
            auto fields = control::DecodePayload(old.payload);
            if (old.kind != "merge" || fields.size() != arguments.size() ||
                !std::equal(fields.begin(), fields.begin() + 5, arguments.begin()))
                throw std::runtime_error("Request ID reused with different payload");
            CheckMergeActive(old);
            ReserveMergeSchema(old, schema_mode, false);
            ReserveMergeResolutions(old, resolution_spec);
            if (old.state == "complete") {
                tx.Commit();
                return old;
            }
            if (fields[5] != arguments[5])
                throw std::runtime_error("[differential:merge_unsupported] Pending merge algorithm is unavailable");
        }
    }
    auto op = Begin("merge", control::Payload({arguments[0], arguments[1], arguments[2],
                                               arguments[3], arguments[4], arguments[5]}), request);
    CheckMergeActive(op);
    ReserveMergeSchema(op, schema_mode, true);
    ReserveMergeResolutions(op, resolution_spec);
    if (op.state != "complete") {
        CheckMergeDestination(id, name, op.id);
        Query exists(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
        exists.Text(1, id).Text(2, name);
        if (exists.Step())
            throw std::runtime_error("[differential:name_conflict] Merge destination already exists");
    }
    tx.Commit();
    return op;
}
BranchMergeInputs Control::ResolveMergeBranches(const std::string &database, const std::string &ours,
                                                const std::string &theirs, const std::string &destination,
                                                const std::string &request, int64_t max_nodes,
                                                const std::function<void()> &check, const std::string &resolution_spec,
                                                const std::string &schema_mode, const MergeExpectedInputs &expected,
                                                bool inspect_pending) {
    expected.Validate();
    auto started = std::chrono::steady_clock::now();
    if (max_nodes < 1 || max_nodes > 1000000)
        throw std::runtime_error("[differential:merge_limit] max_ancestry_nodes must be 1..1000000");
    const bool preview = destination.empty();
    if (inspect_pending && !preview) throw std::runtime_error("Pending-input inspection is read-only");
    control::ValidateName(ours);
    control::ValidateName(theirs);
    if (!preview)
        control::ValidateName(destination);
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // Resolve both heads and their ancestry from one authority transaction. A
    // mutation records that selection in the same FULL commit, before data work.
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", !preview);
    auto id = DatabaseID(database);
    if (Writers(id) != "single")
        throw std::runtime_error("[differential:merge_unsupported] Branch merge requires single-writer storage");
    if (!preview && Profile(id) != "local" && request.empty())
        throw std::runtime_error("[differential:merge_request_required] S3 merge requires a request_id");
    auto check_destination = [&] {
        if (preview || destination == ours)
            return;
        Query exists(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
        exists.Text(1, id).Text(2, destination);
        if (exists.Step())
            throw std::runtime_error("[differential:name_conflict] Merge destination already exists");
    };
    if (!preview && !request.empty()) {
        Query previous(store->db, (std::string(OP_COLUMNS) + " WHERE request=?").c_str());
        previous.Text(1, request);
        if (previous.Step()) {
            BranchMergeInputs result;
            result.operation = ReadOperation(previous);
            auto fields = control::DecodePayload(result.operation.payload);
            if (result.operation.kind != "merge_branch" || fields.size() != 11 || fields[0] != id ||
                fields[1] != ours || fields[2] != theirs || fields[3] != destination)
                throw std::runtime_error("Request ID reused with different payload");
            CheckMergeActive(result.operation);
            ReserveMergeExpectations(result.operation, expected, false);
            expected.Check(std::stoll(fields[8]), std::stoll(fields[9]), std::stoll(fields[10]));
            ReserveMergeSchema(result.operation, schema_mode, false);
            ReserveMergeResolutions(result.operation, resolution_spec);
            if (result.operation.state != "complete") {
                if ((fields[4] != "sibling-fork-v1" && fields[4] != "unique-base-v1") || fields[5] != "logical-cell-v1")
                    throw std::runtime_error("[differential:merge_unsupported] Pending branch merge algorithm is unavailable");
                check_destination();
            }
            result.ours_branch = fields[6];
            result.theirs_branch = fields[7];
            result.selection = fields[4];
            result.base = std::stoll(fields[8]);
            result.ours = std::stoll(fields[9]);
            result.theirs = std::stoll(fields[10]);
            tx.Commit();
            return result;
        }
    }
    check_destination();
    auto left = GetBranch(id, ours);
    auto right = GetBranch(id, theirs);
    // Head comparison precedes traversal and operation reservation. The same FULL
    // transaction later freezes these exact inputs, so there is no check/use gap.
    if (expected.Enabled() && (left.head != expected.ours || right.head != expected.theirs))
        throw std::runtime_error("[differential:merge_inputs_changed] Branch heads differ from the reviewed snapshots; "
                                 "preview again (ours=" + std::to_string(left.head) +
                                 ", theirs=" + std::to_string(right.head) + ")");
    auto unavailable = [] {
        throw std::runtime_error(
            "[differential:merge_base_unavailable] Automatic base selection needs distinct branches with "
            "proven ancestry and no pending lifecycle change; "
            "use differential_merge_snapshots with an explicit base");
    };
    if (left.id == right.id)
        unavailable();
    BranchMergeInputs result;
    result.ours = left.head;
    result.theirs = right.head;
    result.ours_branch = left.id;
    result.theirs_branch = right.id;
    for (auto &branch : {left, right}) {
        Query pending(store->db, "SELECT 1 FROM compound_intents WHERE branch_id=?");
        pending.Text(1, branch.id);
        if ((pending.Step() && !inspect_pending) || branch.head <= 0)
            unavailable();
        auto head = GetSnapshot(id, branch.head);
        if (head.remote || (head.evicted && (head.export_state != "complete" || head.export_key.empty())))
            throw std::runtime_error("[differential:merge_unsupported] Branch merge inputs require local metadata and a readable snapshot or completed export");
    }
    result.base = FindMergeBase(id, left.head, right.head, max_nodes, check, result.ancestry_nodes);
    if (!result.base) {
        // Existing pre-ancestry snapshots stay unknown. Preserve only the already
        // proven direct-sibling case, never infer a linear chain from IDs or roots.
        if (left.source_database != id || right.source_database != id || left.source_snapshot <= 0 ||
            left.source_snapshot != right.source_snapshot)
            unavailable();
        result.base = left.source_snapshot;
        result.selection = "sibling-fork-v1";
        for (auto &branch : {left, right}) {
            if (check)
                check();
            Query birth(store->db, "SELECT payload FROM operations WHERE kind='branch' AND state='complete' "
                                   "AND database_id=? AND branch_id=? AND snapshot_id=?");
            birth.Text(1, id).Text(2, branch.id).Int(3, result.base);
            if (!birth.Step() || control::DecodePayload(birth.Text(0)) !=
                                    std::vector<std::string>{id, std::to_string(result.base), branch.name})
                unavailable();
            // The fork proof (history without parent records, as written by 1.0.0)
            // is valid only before any lifecycle operation
            // that can give this branch additional or replaced ancestry. Never
            // reuse its original fork as a substitute for an unknown merge base.
            Query restored(store->db, "SELECT 1 FROM operations WHERE kind IN ('restore','merge_target_result') "
                                      "AND database_id=? AND branch_id=? LIMIT 1");
            restored.Text(1, id).Text(2, branch.id);
            if (restored.Step() || (branch.head != result.base && GetSnapshot(id, branch.head).branch != branch.id))
                unavailable();
        }
    }
    expected.Check(result.base, result.ours, result.theirs);
    auto base = GetSnapshot(id, result.base); // Expiry is an error, never a substitute base.
    if (base.remote || (base.evicted && (base.export_state != "complete" || base.export_key.empty())))
        throw std::runtime_error("[differential:merge_unsupported] Branch merge base requires a readable snapshot or completed export");
    if (!preview) {
        result.operation = Begin("merge_branch", control::Payload({id, ours, theirs, destination,
                                     result.selection, "logical-cell-v1", left.id, right.id,
                                     std::to_string(result.base), std::to_string(result.ours),
                                     std::to_string(result.theirs)}), request);
        ReserveMergeExpectations(result.operation, expected, true);
        ReserveMergeSchema(result.operation, schema_mode, true);
        ReserveMergeResolutions(result.operation, resolution_spec);
    }
    tx.Commit();
    result.selection_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
    return result;
}
Operation Control::PublishMerge(const Operation &operation, const Root &root, const std::string &provenance) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(operation);
    auto op = GetOperation(operation.id);
    if (op.state == "complete") {
        tx.Commit();
        return op;
    }
    auto fields = control::DecodePayload(op.payload);
    if (op.kind == "merge_branch") {
        if (fields.size() != 11 || (fields[4] != "sibling-fork-v1" && fields[4] != "unique-base-v1") ||
            fields[5] != "logical-cell-v1")
            throw std::runtime_error("[differential:corrupt_data] Invalid branch merge operation");
        fields = {fields[0], fields[8], fields[9], fields[10], fields[3], fields[5]};
    } else if (op.kind != "merge")
        throw std::runtime_error("[differential:corrupt_data] Invalid merge operation kind");
    if (fields.size() != 6 || fields[5] != "logical-cell-v1")
        throw std::runtime_error("[differential:corrupt_data] Invalid merge operation");
    if (Profile(fields[0]) != "local" || Writers(fields[0]) != "single")
        throw std::runtime_error("[differential:merge_unsupported] Merge requires local storage");
    Query exists(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
    exists.Text(1, fields[0]).Text(2, fields[4]);
    if (exists.Step())
        throw std::runtime_error("[differential:name_conflict] Merge destination already exists");
    op.database = fields[0];
    auto branch = NewBranch(op.database, fields[4], root.id, 0, op.database, std::stoll(fields[2]));
    op.branch = branch.id;
    op.snapshot = Publish(branch, root, {"", "", "logical merge", provenance}, Timestamp(),
                          SnapshotParents{"merge", std::stoll(fields[2]), std::stoll(fields[3])}).id;
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "ancestry_recorded");
    Complete(op);
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "publication_transaction");
    tx.Commit();
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "published");
    return op;
}
} // namespace differential
