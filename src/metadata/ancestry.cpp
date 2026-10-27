#include "control_internal.hpp"
#include <openssl/evp.h>
#include <unordered_map>
#include <algorithm>

namespace differential {
namespace {
// A reserved UUIDv8 namespace gives indexed point lookups in the existing operations
// table, without a table migration, a user request ID, or changes to commit_extra_info.
std::string AncestryID(const std::string &database, int64_t snapshot) {
    auto name = "differential-snapshot-ancestry-v1\n" + database + "\n" + std::to_string(snapshot);
    unsigned char bytes[32];
    unsigned length = 0;
    if (EVP_Digest(name.data(), name.size(), bytes, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("Cannot hash snapshot ancestry identity");
    bytes[6] = (bytes[6] & 15) | 128;
    bytes[8] = (bytes[8] & 63) | 128;
    const char *hex = "0123456789abcdef";
    std::string id;
    for (size_t i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            id += '-';
        id += hex[bytes[i] >> 4];
        id += hex[bytes[i] & 15];
    }
    return id;
}
[[noreturn]] void Corrupt() {
    throw std::runtime_error("[differential:corrupt_data] Invalid snapshot ancestry record");
}
int64_t ParentID(const std::string &text) {
    try {
        auto id = std::stoll(text);
        if (id >= 0 && std::to_string(id) == text)
            return id;
    } catch (const std::exception &) {
    }
    Corrupt();
}
std::vector<std::string> DecodeParents(const std::string &payload) {
    if (payload.size() > 256)
        Corrupt();
    std::vector<std::string> fields;
    size_t offset = 0;
    while (offset < payload.size() && fields.size() < 4) {
        auto colon = payload.find(':', offset);
        if (colon == std::string::npos)
            Corrupt();
        auto length = ParentID(payload.substr(offset, colon - offset));
        if (uint64_t(length) > payload.size() - colon - 1)
            Corrupt();
        fields.push_back(payload.substr(colon + 1, size_t(length)));
        offset = colon + 1 + size_t(length);
    }
    if (offset != payload.size() || fields.size() != 4)
        Corrupt();
    return fields;
}
void ValidateParents(const SnapshotParents &p, int64_t snapshot) {
    if (p.first < 0 || p.second < 0 || p.first == snapshot || p.second == snapshot)
        Corrupt();
    if (p.kind == "root" || p.kind == "import" || p.kind == "clone") {
        if (p.first || p.second)
            Corrupt();
    } else if (p.kind == "linear" || p.kind == "restore") {
        if (!p.first || p.second)
            Corrupt();
    } else if (p.kind == "merge") {
        if (!p.first || p.first == p.second)
            Corrupt();
    } else
        Corrupt();
}
} // namespace

// Called only inside the FULL publication transaction. These records contain IDs,
// never roots or files: they deliberately add no retention reason to the byte store.
void Control::RecordAncestry(const Snapshot &s, SnapshotParents parents) {
    if (parents.second == parents.first)
        parents.second = 0;
    ValidateParents(parents, s.id);
    for (auto parent : {parents.first, parents.second}) {
        if (!parent)
            continue;
        Query exists(store->db, "SELECT 1 FROM snapshots WHERE database_id=? AND id=?");
        exists.Text(1, s.database).Int(2, parent);
        if (!exists.Step())
            Corrupt();
    }
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,branch_id,snapshot_id,created_at) "
                     "VALUES(?,'snapshot_ancestry',?,'complete',?,?,?,?)")
        .Text(1, AncestryID(s.database, s.id))
        .Text(2, control::Payload({"snapshot-parents-v1", parents.kind, std::to_string(parents.first),
                                  std::to_string(parents.second)}))
        .Text(3, s.database).Text(4, s.branch).Int(5, s.id).Int(6, s.published_at).Run();
    control::Crash("snapshot_ancestry");
}

// O(V+E) visits of a bounded reachable graph, with indexed metadata lookups. Neither
// snapshot numbers, publication times nor byte roots define graph edges or ordering.
// Zero means incomplete proof; the caller may still prove the original sibling case.
MergeAncestry Control::WalkMergeAncestry(const std::string &database, int64_t ours, int64_t theirs,
                                        int64_t max_nodes, const std::function<void()> &check) {
    struct Node {
        SnapshotParents parents;
        unsigned reach = 0, children = 0;
        bool known = false, dominated = false;
    };
    std::unordered_map<int64_t, Node> graph;
    Query record(store->db, "SELECT kind,payload,state,database_id,snapshot_id FROM operations WHERE id=?");
    for (auto side : {std::make_pair(ours, 1U), std::make_pair(theirs, 2U)}) {
        std::vector<int64_t> pending{side.first};
        while (!pending.empty()) {
            if (check)
                check();
            auto id = pending.back();
            pending.pop_back();
            if (!graph.count(id)) {
                if (graph.size() >= size_t(max_nodes))
                    throw std::runtime_error("[differential:merge_limit] Ancestry node limit exceeded");
                Node node;
                record.Text(1, AncestryID(database, id));
                if (record.Step()) {
                    if (record.Text(0) != "snapshot_ancestry" || record.Text(2) != "complete" ||
                        record.Text(3) != database || record.Int(4) != id)
                        Corrupt();
                    auto fields = DecodeParents(record.Text(1));
                    if (fields[0] != "snapshot-parents-v1")
                        throw std::runtime_error("[differential:merge_unsupported] Snapshot ancestry version is unavailable");
                    node.parents = {fields[1], ParentID(fields[2]), ParentID(fields[3])};
                    ValidateParents(node.parents, id);
                    node.known = true;
                }
                sqlite3_reset(record.s);
                sqlite3_clear_bindings(record.s);
                graph.emplace(id, std::move(node));
            }
            auto &node = graph.at(id);
            if (node.reach & side.second)
                continue;
            node.reach |= side.second;
            for (auto parent : {node.parents.first, node.parents.second})
                if (parent)
                    pending.push_back(parent);
        }
    }
    auto visited = int64_t(graph.size());
    bool incomplete = false;
    for (auto &entry : graph) {
        if (check)
            check();
        auto &node = entry.second;
        // Unknown ancestry is safe only below a common node. All its undiscovered
        // ancestors are then also common and cannot supersede the chosen best base.
        if (!node.known && node.reach != 3)
            incomplete = true;
        for (auto parent : {node.parents.first, node.parents.second}) {
            if (!parent)
                continue;
            auto &ancestor = graph.at(parent);
            ancestor.children++;
            if (node.reach == 3)
                ancestor.dominated = true;
        }
    }
    // Kahn traversal detects corrupt cycles without recursive C++ stack growth.
    std::vector<int64_t> pending;
    int64_t best = 0, candidates = 0, processed = 0;
    for (auto &entry : graph) {
        if (!entry.second.children)
            pending.push_back(entry.first);
        if (entry.second.reach == 3 && !entry.second.dominated) {
            best = entry.first;
            candidates++;
        }
    }
    while (!pending.empty()) {
        if (check)
            check();
        auto id = pending.back();
        pending.pop_back();
        processed++;
        auto &node = graph.at(id);
        for (auto parent : {node.parents.first, node.parents.second})
            if (parent && !--graph.at(parent).children)
                pending.push_back(parent);
    }
    if (processed != visited)
        Corrupt();
    MergeAncestry result;
    result.state = incomplete ? "incomplete" : candidates > 1 ? "ambiguous" : !candidates ? "unrelated" : "unique";
    if (result.state == "unique")
        result.selected = best;
    for (auto &entry : graph) {
        auto &n = entry.second;
        result.nodes.push_back({entry.first, n.parents, n.reach, n.known, n.reach == 3 && !n.dominated});
    }
    return result;
}
int64_t Control::FindMergeBase(const std::string &database, int64_t ours, int64_t theirs,
                               int64_t max_nodes, const std::function<void()> &check, int64_t &visited) {
    auto result = WalkMergeAncestry(database, ours, theirs, max_nodes, check);
    visited = int64_t(result.nodes.size());
    if (result.state == "incomplete")
        return 0;
    if (result.state == "ambiguous")
        throw std::runtime_error("[differential:merge_base_ambiguous] Multiple best common ancestors; "
                                 "use differential_merge_snapshots with an explicit base");
    if (result.state == "unrelated")
        throw std::runtime_error("[differential:merge_base_unavailable] No common ancestor; "
                                 "use differential_merge_snapshots with an explicit base");
    return result.selected;
}
MergeAncestry Control::InspectMergeAncestry(const std::string &database, int64_t ours, int64_t theirs,
                                            int64_t max_nodes, const std::function<void()> &check) {
    if (max_nodes < 1 || max_nodes > 1000000)
        throw std::runtime_error("max_ancestry_nodes must be 1..1000000");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    auto id = DatabaseID(database);
    if (Writers(id) != "single")
        throw std::runtime_error("[differential:merge_unsupported] Ancestry inspection requires a single-writer database");
    for (auto input : {ours, theirs}) {
        Query exists(store->db, "SELECT 1 FROM snapshots WHERE database_id=? AND id=?");
        exists.Text(1, id).Int(2, input);
        if (!exists.Step())
            throw std::runtime_error("[differential:missing_snapshot] Ancestry input does not exist");
    }
    auto result = WalkMergeAncestry(id, ours, theirs, max_nodes, check);
    std::sort(result.nodes.begin(), result.nodes.end(), [](const MergeAncestor &a, const MergeAncestor &b) {
        return a.snapshot < b.snapshot; // presentation order only, never ancestry inference
    });
    Query status(store->db, "SELECT branch_id,retired,evicted FROM snapshots WHERE database_id=? AND id=?");
    for (auto &node : result.nodes) {
        if (check) check();
        status.Text(1, id).Int(2, node.snapshot);
        if (status.Step()) {
            node.branch = status.Text(0);
            node.retired = status.Int(1) != 0;
            node.readable = !node.retired && (!status.Int(2) || Export(id, node.snapshot).state == "complete");
        }
        sqlite3_reset(status.s);
        sqlite3_clear_bindings(status.s);
    }
    tx.Commit();
    return result;
}
// Reserve the option identity in the same FULL transaction as snapshot selection.
// The source data is read later under a DuckDB transaction and frozen atomically.
void Control::ReserveMergeResolutions(const Operation &op, const std::string &spec) {
    auto id = AncestryID("merge-resolutions:" + op.id, 0);
    Query existing(store->db, "SELECT kind,payload FROM operations WHERE id=?");
    existing.Text(1, id);
    if (existing.Step()) {
        auto fields = control::DecodePayload(existing.Text(1));
        if (existing.Text(0) != "merge_resolutions" || fields.size() != 3)
            throw std::runtime_error("[differential:corrupt_data] Invalid resolution record");
        if (fields[1] != spec) throw std::runtime_error("Request ID reused with different resolution_tables");
        if (op.state != "complete" && fields[0] != "typed-resolutions-duckdb-2-v1")
            throw std::runtime_error("[differential:merge_unsupported] Resolution record version is unavailable");
        return;
    }
    if (op.state == "complete")
        throw std::runtime_error("[differential:corrupt_data] Resolution reservation missing");
    if (spec.size() > 65536) throw std::runtime_error("[differential:merge_limit] Resolution specification exceeds 64 KiB");
    auto fields = control::DecodePayload(op.payload);
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,branch_id,created_at) "
                     "VALUES(?,'merge_resolutions',?,'pending',?,?,?)")
        .Text(1, id).Text(2, control::Payload({"typed-resolutions-duckdb-2-v1", spec, ""}))
        .Text(3, fields.at(0)).Text(4, op.id).Int(5, Timestamp()).Run();
}
// Decisions are bounded DuckDB 2.0 Value envelopes. They retain no snapshot roots.
std::optional<std::string> Control::MergeResolutions(const Operation &op, const std::string &spec,
                                                     std::optional<std::string> data) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", bool(data));
    auto id = AncestryID("merge-resolutions:" + op.id, 0);
    Query existing(store->db, "SELECT kind,payload,state FROM operations WHERE id=?");
    existing.Text(1, id);
    if (!existing.Step())
        throw std::runtime_error("[differential:corrupt_data] Resolution reservation missing");
    auto fields = control::DecodePayload(existing.Text(1));
    if (existing.Text(0) != "merge_resolutions" || fields.size() != 3 || fields[0] != "typed-resolutions-duckdb-2-v1")
        throw std::runtime_error("[differential:merge_unsupported] Resolution record version is unavailable");
    if (fields[1] != spec) throw std::runtime_error("Request ID reused with different resolution_tables");
    if (existing.Text(2) == "complete") {
        tx.Commit();
        return fields[2];
    }
    if (data) {
        if (data->size() > 8388608) throw std::runtime_error("[differential:merge_limit] Resolution record exceeds 8 MiB");
        Query(store->db, "UPDATE operations SET payload=?,state='complete' WHERE id=?")
            .Text(1, control::Payload({fields[0], spec, *data})).Text(2, id).Run();
    }
    tx.Commit();
    return data;
}
std::string Control::ReadMergeResolutions(const std::string &database, const std::string &request) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto database_id = DatabaseID(database);
    Query op(store->db, (std::string(control::OP_COLUMNS) + " WHERE request=?").c_str());
    op.Text(1, request);
    if (!op.Step()) throw std::runtime_error("[differential:not_found] Merge request not found");
    auto operation = control::ReadOperation(op);
    auto fields = control::DecodePayload(operation.payload);
    if ((operation.kind != "merge" && operation.kind != "merge_branch") || fields.empty() || fields[0] != database_id)
        throw std::runtime_error("[differential:not_found] Merge request not found in database");
    Query decisions(store->db, "SELECT payload FROM operations WHERE id=? AND kind='merge_resolutions'");
    decisions.Text(1, AncestryID("merge-resolutions:" + operation.id, 0));
    if (!decisions.Step()) return "";
    auto payload = control::DecodePayload(decisions.Text(0));
    if (payload.size() != 3 || payload[0] != "typed-resolutions-duckdb-2-v1")
        throw std::runtime_error("[differential:merge_unsupported] Resolution record version is unavailable");
    return payload[2];
}
} // namespace differential
