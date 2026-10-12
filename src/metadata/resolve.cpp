// diff:// URI resolution to managed branches, local snapshots and remote exports.
#include "control_internal.hpp"
#include <stdexcept>

namespace differential {
using namespace control;

ResolvedPath Control::Resolve(const std::string &uri) {
    ResolvedPath p;
    p.canonical = uri;
    if (uri.rfind("diff://", 0) != 0)
        throw std::runtime_error("Invalid differential URI");
    std::vector<std::string> parts;
    size_t pos = 7;
    while (pos < uri.size()) {
        auto end = uri.find('/', pos);
        parts.push_back(uri.substr(pos, end == std::string::npos ? end : end - pos));
        if (end == std::string::npos)
            break;
        pos = end + 1;
    }
    auto split_suffix = [&](std::string &identity) {
        auto dot = identity.find('.');
        if (dot != std::string::npos) {
            p.suffix = identity.substr(dot);
            identity.resize(dot);
        }
    };
    // diff://export/<provider UUID>/descriptors/<sha256>[.wal...]: a read-only S3 export, attached
    // by its location; no local registration is needed.
    if (parts.size() == 4 && parts[0] == "export" && parts[2] == "descriptors") {
        auto identity = parts[3];
        split_suffix(identity);
        if (!IsUUID(parts[1]) || identity.size() != 64 ||
            identity.find_first_not_of("0123456789abcdef") != std::string::npos)
            throw std::runtime_error("Invalid differential export URI");
        p.managed = true;
        p.remote = true;
        p.provider = Provider(parts[1]).id;
        p.descriptor = "descriptors/" + identity;
        p.canonical = "diff://export/" + p.provider + "/descriptors/" + identity + p.suffix;
        return p;
    }
    bool stable = parts.size() == 4 && parts[0] == "db";
    if (parts.size() != (stable ? 4u : 3u))
        return p; // diagnostic probe paths are handled separately
    p.managed = true;
    std::string database = parts[stable ? 1 : 0], kind = parts[stable ? 2 : 1], identity = parts.back();
    split_suffix(identity);
    if (p.suffix != "" && p.suffix != ".wal" && p.suffix != ".wal.checkpoint" && p.suffix != ".wal.recovery")
        throw std::runtime_error("[differential:unsupported_provider_capability] Unsupported native sidecar");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query database_lookup(store->db, stable ? "SELECT id FROM databases WHERE id=?" : "SELECT id FROM databases WHERE name=?");
    database_lookup.Text(1, database);
    if (!database_lookup.Step())
        throw std::runtime_error("[differential:not_found] Database not found");
    database = database_lookup.Text(0);
    if (kind == "branches") {
        p.branch = GetBranch(database, identity, stable);
        p.file = p.branch.FileName() + p.suffix;
        p.canonical = p.branch.URI() + p.suffix;
    } else if (kind == "snapshots" || kind == "snapshot-names") {
        int64_t id;
        if (kind == "snapshot-names") {
            // Local snapshots first, then (shared databases) other writers' commit records.
            Query q(store->db, "SELECT id FROM snapshots WHERE database_id=?1 AND name=?2 AND retired=0 UNION ALL "
                               "SELECT id FROM remote_commits WHERE database_id=?1 AND name=?2 AND expired=0 LIMIT 1");
            q.Text(1, database).Text(2, identity);
            if (!q.Step())
                throw std::runtime_error("[differential:not_found] Snapshot name not found");
            id = q.Int(0);
        } else {
            if (identity.empty() || identity.find_first_not_of("0123456789") != std::string::npos)
                throw std::runtime_error("Invalid snapshot ID");
            id = std::stoll(identity);
            if (id <= 0)
                throw std::runtime_error("Invalid snapshot ID");
        }
        p.historical = true;
        p.snapshot = FindSnapshot(database, id);
        p.root = p.snapshot.root;
        p.canonical = p.snapshot.URI() + p.suffix;
        if (p.snapshot.evicted) {
            // The local copy was released after a completed export: read through the
            // immutable remote descriptor instead.
            if (p.snapshot.export_state != "complete" || p.snapshot.export_key.empty())
                throw std::runtime_error("[differential:corrupt_data] Evicted snapshot has no completed export");
            p.remote = true;
            p.provider = p.snapshot.export_provider;
            p.descriptor = p.snapshot.export_key;
        }
    } else
        throw std::runtime_error("Unknown differential URI kind");
    return p;
}
} // namespace differential
