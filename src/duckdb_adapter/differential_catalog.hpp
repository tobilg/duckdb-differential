#pragma once
// Native catalog of a differential attachment: DuckLake-style catalog-scoped
// functions (sales.snapshots()) and AT (VERSION/TIMESTAMP) time travel. Storage stays native;
// the subclass only adds lookups.
#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "differential_repository.hpp"
#include <list>
#include <map>
#include <mutex>
#include <set>

namespace duckdb {
class Follower;
// Hidden read-only snapshot attachments that serve AT lookups, shared by every catalog and
// connection of one DuckDB instance and capped by least-recent use. Detaching one is safe while
// queries still read it: their transactions hold the attachment until they finish.
struct TimeTravelCache {
    static constexpr size_t CAPACITY = 8;
    std::mutex mutex; // guards the recency list and `opening` (attach and detach run outside it)
    std::list<string> recent; // most recently used first
    // Serializes attaching one snapshot (two connections must not attach the same name at once);
    // attaching different snapshots runs in parallel.
    std::map<string, std::shared_ptr<std::mutex>> opening;
};
// ATTACH option marking a hidden time-travel attachment (consumed by the attach callback).
constexpr const char *TIME_TRAVEL_OPTION = "__differential_time_travel";

class DifferentialCatalog : public DuckCatalog {
  public:
    DifferentialCatalog(AttachedDatabase &db, std::shared_ptr<differential::Repository> repository,
                        std::shared_ptr<TimeTravelCache> cache, bool time_travel_snapshot, bool remote);

    void FinalizeLoad(optional_ptr<ClientContext> context) override;
    bool SupportsTimeTravel() const override { return !remote; }
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
                                                  OnEntryNotFound if_not_found) override;
    void OnDetach(ClientContext &context) override;
    // FOLLOW: lookups go to the follower's newest prepared state (one per transaction).
    void SetFollower(std::shared_ptr<Follower> follower) { this->follower = std::move(follower); }
    // A hidden time-travel catalog reports no version, so a prepared statement that read it
    // rebinds through the live catalog (reopening the snapshot if it was evicted) instead of
    // failing because the hidden attachment is gone.
    optional_idx GetCatalogVersion(ClientContext &context) override {
        return time_travel_snapshot ? optional_idx() : DuckCatalog::GetCatalogVersion(context);
    }

  private:
    // The read-only schema of this (hidden snapshot) catalog, as seen from another catalog.
    optional_ptr<SchemaCatalogEntry> SnapshotSchema(ClientContext &context, const Identifier &schema,
                                                    OnEntryNotFound if_not_found);

    std::shared_ptr<differential::Repository> repository;
    std::shared_ptr<TimeTravelCache> cache;
    // A hidden time-travel attachment is already at its version: AT clauses propagated into
    // its view bodies resolve against itself.
    const bool time_travel_snapshot;
    const bool remote; // an attached S3 export has no history to travel
    std::mutex mutex;
    std::map<string, unique_ptr<SchemaCatalogEntry>> facades; // by schema name (snapshot catalogs)
    std::set<string> hidden; // time-travel attachments this catalog opened
    std::shared_ptr<Follower> follower;
};

// Catalog-scoped functions generated per differential catalog (control_functions.cpp).
const vector<string> &ScopedFunctionNames();
unique_ptr<CatalogEntry> CreateScopedFunction(std::shared_ptr<differential::Repository> repository, Catalog &catalog,
                                              SchemaCatalogEntry &schema, const string &name);
} // namespace duckdb
