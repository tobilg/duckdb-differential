// Catalog-scoped functions and AT (VERSION/TIMESTAMP) time travel.
//
// Functions: a default generator on the main schema's table-function set creates internal
// entries on first lookup. They live only in memory, are never checkpointed into the native
// file, and a user object with the same name in an existing file shadows them.
//
// Time travel: an AT clause resolves to a public snapshot of the attached database. The
// snapshot is opened as a hidden read-only attachment (taking its reader pin while the query
// binds; an approved exception to execution-only effects) and its schema is returned
// through a read-only facade, so the binder reads the snapshot's native catalog and storage.
#include "differential_catalog.hpp"
#include "follow.hpp"
#include "sql_options.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {
namespace {
class ScopedFunctionGenerator : public DefaultGenerator {
  public:
    ScopedFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema,
                            std::shared_ptr<differential::Repository> repository)
        : DefaultGenerator(catalog), schema(schema), repository(std::move(repository)) {}
    unique_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction, const Identifier &name) override {
        return CreateScopedFunction(repository, catalog, schema, name.GetIdentifierName());
    }
    vector<Identifier> GetDefaultEntries() override {
        vector<Identifier> names;
        for (auto &name : ScopedFunctionNames())
            names.emplace_back(name);
        return names;
    }

  private:
    SchemaCatalogEntry &schema;
    std::shared_ptr<differential::Repository> repository;
};

// Read-only view of a snapshot catalog's schema, returned from another catalog's lookup.
// Entry lookups run under the snapshot catalog's own transaction.
class SnapshotSchemaEntry : public SchemaCatalogEntry {
  public:
    SnapshotSchemaEntry(Catalog &snapshot, SchemaCatalogEntry &inner, CreateSchemaInfo &info)
        : SchemaCatalogEntry(snapshot, info, inner.GetParentSchema()), snapshot(snapshot), inner(inner) {}

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup) override {
        return inner.LookupEntry(Own(transaction), Plain(lookup));
    }
    CatalogSet::EntryLookup LookupEntryDetailed(CatalogTransaction transaction, const EntryLookupInfo &lookup) override {
        return inner.LookupEntryDetailed(Own(transaction), Plain(lookup));
    }
    SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup) override {
        return inner.GetSimilarEntry(Own(transaction), Plain(lookup));
    }
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
        inner.Scan(context, type, callback);
    }
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
        inner.Scan(type, callback);
    }
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) override {
        ReadOnly();
    }
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction, CreateFunctionInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction, BoundCreateTableInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction, CreateViewInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction, CreateSequenceInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) override {
        ReadOnly();
    }
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) override {
        ReadOnly();
    }
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction, CreateCollationInfo &) override { ReadOnly(); }
    optional_ptr<CatalogEntry> CreateCoordinateSystem(CatalogTransaction, CreateCoordinateSystemInfo &) override {
        ReadOnly();
    }
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction, CreateTypeInfo &) override { ReadOnly(); }
    void DropEntry(ClientContext &, DropInfo &) override { ReadOnly(); }
    void Alter(CatalogTransaction, AlterInfo &) override { ReadOnly(); }

  private:
    [[noreturn]] static void ReadOnly() {
        throw BinderException("[differential:read_only_attachment] AT (...) reads an immutable snapshot");
    }
    CatalogTransaction Own(CatalogTransaction transaction) {
        return snapshot.GetCatalogTransaction(transaction.GetContext());
    }
    static EntryLookupInfo Plain(const EntryLookupInfo &lookup) {
        return EntryLookupInfo(lookup.GetCatalogType(), QualifiedName(lookup.GetEntryIdentifier()), lookup.GetErrorContext());
    }
    Catalog &snapshot;
    SchemaCatalogEntry &inner;
};
} // namespace

DifferentialCatalog::DifferentialCatalog(AttachedDatabase &db, std::shared_ptr<differential::Repository> repository,
                                         std::shared_ptr<TimeTravelCache> cache, bool time_travel_snapshot, bool remote)
    : DuckCatalog(db), repository(std::move(repository)), cache(std::move(cache)),
      time_travel_snapshot(time_travel_snapshot), remote(remote) {}

// Installed after the native load (checkpoint + WAL replay), so a user object that already
// uses one of the names loads first and shadows the generated function.
void DifferentialCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
    DuckCatalog::FinalizeLoad(context);
    if (remote || time_travel_snapshot)
        return;
    auto system = CatalogTransaction::GetSystemTransaction(GetDatabase());
    auto schema = DuckCatalog::LookupSchema(system, EntryLookupInfo(CatalogType::SCHEMA_ENTRY, QualifiedName(Identifier::DefaultSchema())),
                                            OnEntryNotFound::RETURN_NULL);
    if (!schema)
        return;
    auto &duck_schema = schema->Cast<DuckSchemaEntry>();
    duck_schema.GetCatalogSet(CatalogType::TABLE_FUNCTION_ENTRY)
        .SetDefaultGenerator(make_uniq<ScopedFunctionGenerator>(*this, duck_schema, repository));
}

optional_ptr<SchemaCatalogEntry> DifferentialCatalog::LookupSchema(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &schema_lookup,
                                                                   OnEntryNotFound if_not_found) {
    auto at = schema_lookup.GetAtClause();
    if (follower && !time_travel_snapshot && transaction.HasContext()) {
        if (at)
            throw BinderException("AT (...) is not available on a follower (FOLLOW true): it reads the newest state");
        auto &context = transaction.GetContext();
        if (auto state = follower->Choose(context))
            return state->GetCatalog().Cast<DifferentialCatalog>().SnapshotSchema(context, schema_lookup,
                                                                                  if_not_found);
        return DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
    }
    if (!at || time_travel_snapshot)
        return DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
    auto &context = transaction.GetContext();
    // Resolve the clause to a retained public snapshot of this attachment's database.
    differential::Snapshot snapshot;
    try {
        auto control = repository->Get();
        auto resolved = control->Resolve(GetAttached().GetStorageManager().GetDBPath());
        auto database = resolved.historical ? resolved.snapshot.database : resolved.branch.database;
        // Shared databases: other writers' snapshots resolve from the cached S3 commit records (refreshed
        // by ATTACH and differential_snapshots, never while binding).
        auto unit = StringUtil::Upper(at->Unit().GetIdentifierName());
        int64_t id;
        if (unit == "VERSION")
            id = BigIntValue::Get(at->GetValue().DefaultCastAs(LogicalType::BIGINT));
        else if (unit == "TIMESTAMP")
            // From a snapshot attachment, a shared database's ancestry starts at that snapshot.
            id = control
                     ->SnapshotAt(database, resolved.historical ? resolved.snapshot.branch : resolved.branch.id,
                                  Microseconds(at->GetValue()), resolved.historical ? resolved.snapshot.id : 0)
                     .id;
        else
            throw BinderException("AT (%s => ...) is not supported; use VERSION or TIMESTAMP", at->Unit());
        snapshot = control->FindSnapshot(database, id);
    } catch (const Exception &) {
        throw;
    } catch (const std::exception &error) {
        throw BinderException(error.what());
    }
    // Open (or reuse) the hidden attachment. Opening an evicted snapshot reads its export from S3, so it
    // happens outside the cache lock, under a lock of its own name only.
    auto name = "__differential_" + snapshot.database + "_" + std::to_string(snapshot.id);
    auto &manager = DatabaseManager::Get(context);
    std::shared_ptr<std::mutex> opening;
    {
        std::lock_guard<std::mutex> guard(cache->mutex);
        auto &entry = cache->opening[name];
        if (!entry)
            entry = std::make_shared<std::mutex>();
        opening = entry;
    }
    std::unique_lock<std::mutex> open_lock(*opening);
    // A transaction that already reads this snapshot keeps its attachment, even after another
    // connection evicted it: one name must name one database within a transaction.
    auto attached = MetaTransaction::Get(context).GetReferencedDatabaseOwning(Identifier(name));
    bool listed = !attached || manager.GetDatabase(Identifier(name)) == attached; // an evicted one is not cached again
    if (!attached)
        attached = manager.GetDatabase(Identifier(name));
    if (!attached) {
        AttachInfo info;
        info.name = Identifier(name);
        info.path = snapshot.URI();
        info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
        info.options["type"] = Value("differential");
        info.options[TIME_TRAVEL_OPTION] = Value::BOOLEAN(true);
        AttachOptions options(info.options, AccessMode::READ_ONLY);
        options.visibility = AttachVisibility::HIDDEN;
        attached = manager.AttachDatabase(context, info, options);
        if (!attached)
            attached = manager.GetDatabase(Identifier(name));
        if (!attached)
            throw BinderException("Could not open snapshot %d for AT", snapshot.id);
    }
    open_lock.unlock();
    // Evict the least recently used beyond capacity. Detaching only unlists an attachment: queries
    // that use it keep it (and its reader pin) until they finish, and a later lookup reopens it.
    vector<string> victims;
    {
        std::lock_guard<std::mutex> guard(cache->mutex);
        if (listed) {
            cache->recent.remove(name);
            cache->recent.push_front(name);
        }
        while (cache->recent.size() > TimeTravelCache::CAPACITY) {
            victims.push_back(cache->recent.back());
            cache->recent.pop_back();
        }
        // Names neither recent nor being opened need no lock entry.
        for (auto it = cache->opening.begin(); it != cache->opening.end();)
            if (it->second.use_count() == 1 &&
                std::find(cache->recent.begin(), cache->recent.end(), it->first) == cache->recent.end())
                it = cache->opening.erase(it);
            else
                ++it;
    }
    for (auto &victim : victims)
        manager.DetachDatabase(context, Identifier(victim), OnEntryNotFound::RETURN_NULL);
    {
        std::lock_guard<std::mutex> guard(mutex);
        hidden.insert(name);
    }
    return attached->GetCatalog().Cast<DifferentialCatalog>().SnapshotSchema(context, schema_lookup,
                                                                             if_not_found);
}

optional_ptr<SchemaCatalogEntry> DifferentialCatalog::SnapshotSchema(ClientContext &context, const EntryLookupInfo &schema_lookup,
                                                                     OnEntryNotFound if_not_found) {
    // Starting this catalog's transaction also keeps the attachment alive for the query.
    auto own = GetCatalogTransaction(context);
    // The full schema path (nested schemas included), without the AT clause this catalog resolved.
    auto inner = DuckCatalog::LookupSchema(own, EntryLookupInfo(schema_lookup, nullptr), if_not_found);
    if (!inner)
        return nullptr;
    std::lock_guard<std::mutex> guard(mutex);
    auto &facade = facades[inner->GetSchemaName()];
    if (!facade) {
        CreateSchemaInfo info;
        info.SetQualifiedName(QualifiedName::FromCatalogSchema(GetName(), inner->GetSchemaPath(), Identifier()));
        facade = make_uniq<SnapshotSchemaEntry>(*this, *inner, info);
    }
    return facade.get();
}

// Detaching a database also releases the snapshots its AT clauses opened (and their pins).
void DifferentialCatalog::OnDetach(ClientContext &context) {
    if (follower)
        follower->Stop(context);
    std::set<string> names;
    {
        std::lock_guard<std::mutex> guard(mutex);
        names.swap(hidden);
    }
    if (!names.empty()) {
        std::lock_guard<std::mutex> guard(cache->mutex);
        for (auto &name : names) {
            cache->recent.remove(name);
            DatabaseManager::Get(context).DetachDatabase(context, Identifier(name), OnEntryNotFound::RETURN_NULL);
        }
    }
    DuckCatalog::OnDetach(context);
}
} // namespace duckdb
