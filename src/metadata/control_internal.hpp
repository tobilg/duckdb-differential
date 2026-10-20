#pragma once
// Shared helpers and SQL for the metadata authority (control.cpp, retention.cpp, resolve.cpp).
#include "differential_control.hpp"
#include "differential_sql.hpp"
#include "differential_error.hpp"
#include <initializer_list>
#include <string>

namespace differential {
namespace control {
inline void Crash(const char *point) { Failpoint("DIFFERENTIAL_CONTROL_CRASH", point); }
std::string UUID();
bool IsUUID(const std::string &);
void ValidateName(const std::string &name, bool optional = false);
std::string Payload(std::initializer_list<std::string> parts);
std::vector<std::string> DecodePayload(const std::string &payload);
Operation ReadOperation(Query &q);
Branch ReadBranch(Query &q);
Snapshot ReadSnapshot(Query &q);

constexpr const char *OP_COLUMNS =
    "SELECT id,request,kind,payload,state,database_id,branch_id,snapshot_id,preservation FROM operations";
constexpr const char *BRANCH_COLUMNS =
    "SELECT b.database_id,d.name,b.id,b.name,b.state,b.root,b.epoch,b.head,b.created_at,b.source_database,"
    "b.source_snapshot FROM branches b JOIN databases d ON b.database_id=d.id";
constexpr const char *SNAPSHOT_COLUMNS =
    "SELECT s.database_id,d.name,s.branch_id,s.id,s.name,s.root,s.published_at,s.captured_at,r.size,s.retired,"
    "b.source_database,b.source_snapshot,s.evicted,s.author,s.commit_message,s.commit_extra_info FROM snapshots s "
    "JOIN databases d ON s.database_id=d.id JOIN "
    "roots r ON s.root=r.hash JOIN branches b ON b.id=s.branch_id";
constexpr const char *DATABASES_SQL =
    "SELECT d.id,d.name,CASE WHEN p.id IS NOT NULL THEN 's3://'||p.bucket||'/'||p.prefix ELSE l.path END,"
    "d.last_snapshot,d.created_at,d.writers FROM databases d LEFT JOIN "
    "database_profiles f ON f.database_id=d.id LEFT JOIN providers p ON p.id=f.provider_id LEFT JOIN "
    "database_local_profiles lp ON lp.database_id=d.id LEFT JOIN local_pools l ON l.id=lp.pool ORDER BY "
    "d.created_at,d.id";
// Every local retention reason. A completed S3 export no longer retains local payload: its
// immutable graph is remotely durable, and readers of an evicted snapshot use the export.
constexpr const char *RETENTION_ROOTS_SQL = R"SQL(
SELECT 'snapshot',database_id||'/'||id,root FROM snapshots WHERE retired=0 AND evicted=0
UNION ALL SELECT 'branch',id,root FROM branches WHERE state='active' AND root IS NOT NULL
UNION ALL SELECT 'reader_pin',id,root FROM pins WHERE root IS NOT NULL
UNION ALL SELECT 'generation',cast(g.file AS TEXT),g.root FROM generations g JOIN files f ON f.id=g.file WHERE f.name IS NOT NULL OR EXISTS(SELECT 1 FROM pins p WHERE p.file=f.id)
UNION ALL SELECT 'compound_target',operation_id,target_root FROM compound_intents WHERE target_root IS NOT NULL
UNION ALL SELECT 'export_pending',export_id,root FROM export_roots WHERE state='pending'
UNION ALL SELECT 'native_file',coalesce(name,cast(id AS TEXT)),'inode:'||id FROM files WHERE name IS NOT NULL OR EXISTS(SELECT 1 FROM pins p WHERE p.file=files.id)
ORDER BY 1,2)SQL";
// The same reasons as indexed point lookups for one root (?1).
constexpr const char *ROOT_LIVE_SQL = R"SQL(SELECT
EXISTS(SELECT 1 FROM snapshots WHERE root=?1 AND retired=0 AND evicted=0)
OR EXISTS(SELECT 1 FROM branches WHERE root=?1 AND state='active')
OR EXISTS(SELECT 1 FROM pins WHERE root=?1)
OR EXISTS(SELECT 1 FROM generations g JOIN files f ON f.id=g.file WHERE g.root=?1 AND (f.name IS NOT NULL OR EXISTS(SELECT 1 FROM pins p WHERE p.file=f.id)))
OR EXISTS(SELECT 1 FROM compound_intents WHERE target_root=?1)
OR EXISTS(SELECT 1 FROM export_roots WHERE root=?1 AND state='pending'))SQL";
// Whole-repository reachability, used only by explicit dry runs on a read snapshot.
constexpr const char *LIVE_CTE = R"SQL(WITH live_files AS (SELECT id FROM files f WHERE name IS NOT NULL OR EXISTS(SELECT 1 FROM pins p WHERE p.file=f.id)),
live_roots AS (SELECT root FROM snapshots WHERE retired=0 AND evicted=0 UNION SELECT root FROM branches WHERE state='active' AND root IS NOT NULL
UNION SELECT root FROM pins WHERE root IS NOT NULL UNION SELECT root FROM generations WHERE file IN (SELECT id FROM live_files)
UNION SELECT target_root FROM compound_intents WHERE target_root IS NOT NULL UNION SELECT root FROM export_roots WHERE state='pending'),
live_pages AS (SELECT page FROM root_pages WHERE root IN (SELECT root FROM live_roots) UNION SELECT page FROM file_pages WHERE page IS NOT NULL AND file IN (SELECT id FROM live_files))
)SQL";
} // namespace control
} // namespace differential
