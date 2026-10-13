// Test-only SQLLogicTest adapter. No DuckDB or production extension modifications.
// A marker table in the runner's otherwise unused host preserves identity across
// restart. `load` deletes that host, so it also discards the marker and starts fresh.
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {
struct BridgePlan : ObjectCacheEntry {
    vector<string> statements;
    string alias;
    bool prepared = false;
    static string ObjectType() { return "upstream_bridge"; }
    string GetObjectType() override { return ObjectType(); }
    optional_idx GetEstimatedCacheMemory() const override { return optional_idx(); }
};

static string Setup(ClientContext &context, const FunctionParameters &params) {
    auto phase = params.values[2].GetValue<int32_t>();
    if (phase < 0 || phase > 7) {
        throw InvalidInputException("invalid upstream bridge phase");
    }
    auto &cache = ObjectCache::GetObjectCache(context);
    auto plan = cache.GetOrCreate<BridgePlan>("upstream_bridge");
    if (plan->prepared) {
        return "SELECT '__upstream_bridge_connection_ready';";
    }
    if (!plan->statements.empty()) {
        if (phase == 7) {
            plan->prepared = true;
        }
        return plan->statements[phase];
    }
    const auto mode = params.values[0].ToString();
    const auto directory = params.values[1].ToString();
    if (mode != "native" && mode != "differential") {
        throw InvalidInputException("upstream bridge mode must be native or differential");
    }
    auto &manager = DatabaseManager::Get(context);
    auto host = manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
    auto alias = host->GetName();
    auto quoted_alias = KeywordHelper::WriteOptionallyQuoted(alias);
    const string prefix = "__upstream_identity_";
    string identity;
    bool fixture = false;
    host->GetCatalog().GetSchema(context, "main").Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
        if (entry.name.compare(0, prefix.size(), prefix) == 0) {
            identity = entry.name.substr(prefix.size());
        } else {
            fixture = true;
        }
    });
    if (fixture || (identity.empty() && host->IsReadOnly())) {
        if (!host->IsReadOnly()) {
            throw InvalidInputException("upstream bridge requires pre-existing native fixtures to be loaded read-only");
        }
        if (mode == "native") {
            // Preserve the original binary image on the native side.
            plan->statements.assign(8, "SELECT '__upstream_native_fixture';");
            return plan->statements[phase];
        }
        auto &fs = FileSystem::GetFileSystem(context);
        auto source = host->GetStorageManager().GetDBPath();
        if (!fs.IsPathAbsolute(source)) {
            source = fs.JoinPath(FileSystem::GetWorkingDirectory(), source);
        }
        auto name = "upstream-fixture-" + UUID::ToString(UUID::GenerateRandomUUID());
        auto quoted_source = KeywordHelper::WriteQuoted(source, '\'');
        auto quoted_name = KeywordHelper::WriteQuoted(name, '\'');
        // Import runs only after DETACH closes the source. Its byte-for-byte
        // copy and initial snapshot preserve the fixture's original format.
        // No import or snapshot work happens in this pragma expansion.
        plan->statements = {
            "ATTACH ':memory:' AS __upstream_switch (READ_WRITE);",
            "USE __upstream_switch;",
            "DETACH " + quoted_alias + ";",
            "CALL differential_import_database(" + quoted_source + ", " + quoted_name + ");",
            "ATTACH " + KeywordHelper::WriteQuoted("differential:" + name, '\'') + " AS " + quoted_alias + " (READ_ONLY);",
            "USE " + quoted_alias + ";",
            "DETACH __upstream_switch;",
            "RESET search_path;"
        };
        return plan->statements[phase];
    }
    string marker_sql = "SELECT '__upstream_bridge_existing_identity';";
    if (identity.empty()) {
        if (host->IsReadOnly()) {
            throw InvalidInputException("upstream bridge cannot remap an uninitialized read-only native fixture");
        }
        identity = UUID::ToString(UUID::GenerateRandomUUID());
        // Returned SQL executes normally; the pragma only inspects catalog metadata.
        marker_sql = "CREATE TABLE " + KeywordHelper::WriteOptionallyQuoted(prefix + identity) + "(unused BOOLEAN);";
    }
    auto path = mode == "native" ? directory + "/native-" + identity + ".duckdb" : "differential:upstream-" + identity;
    auto options = host->IsReadOnly() ? " (READ_ONLY)" : "";
    plan->alias = quoted_alias;
    // One statement per pragma: DuckDB wraps multi-statement pragma expansions
    // in a transaction, which correctly forbids detaching the modified host.
    plan->statements = {
        marker_sql,
        "ATTACH ':memory:' AS __upstream_switch (READ_WRITE);",
        "USE __upstream_switch;",
        "DETACH " + quoted_alias + ";",
        "ATTACH " + KeywordHelper::WriteQuoted(path, '\'') + " AS " + quoted_alias + options + ";",
        "USE " + quoted_alias + ";",
        "DETACH __upstream_switch;",
        "RESET search_path;"
    };
    return plan->statements[phase];
}
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(upstream_bridge, loader) {
    loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall("upstream_bridge", duckdb::Setup,
                            {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER}));
}
}
