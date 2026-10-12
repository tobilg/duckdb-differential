#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <vector>
namespace differential {
enum class Metric : size_t {
    NativeCommit,
    ExplicitCheckpoint,
    LocalRead,
    LocalWrite,
    ManifestSeal,
    SnapshotPublish,
    Export,
    Compact,
    SdkRequest,
    ExtentLookup,
    RemoteManifestLookup,
    LocalSync,
    NamespaceUpdate,
    PinUpdate,
    // ATTACH of S3 data, step by step
    AttachResolve,        // the ATTACH callback: repository, secrets, provider registration, head lookup
    BranchHeadResolve,    // finding and reading a branch head (LIST, GET) and checking its descriptor
    RemoteDescriptorOpen, // opening an export: S3 client, descriptor HEAD and GET
    S3ClientCreate,       // constructing an SDK client
    S3Credentials,        // credential provider calls (one per signed request; the first resolves)
    // Per request, from the SDK's HTTP client (from request start; DNS and connect for new
    // connections only; connect = DNS + TCP + TLS)
    S3DnsLookup,
    S3Connect,
    S3FirstByte,
    // Export of one pack (thread time, summed over parallel uploads)
    ExportPackBuild, // reading the pack's chunks from the local store
    ExportStage,     // waiting for the staging lock and writing the staging file
    ExportPut,       // waiting for a request permit, checksumming the staged file and the PUT
    ExportVerify,    // HEAD and read-back verification
    // Parts of ATTACH and snapshot creation
    AttachRepositoryOpen,   // opening (or creating) the repository's metadata store
    S3SecretLookup,         // DuckDB secret lookups for S3 credentials and settings
    S3ProviderRegistration, // registering or verifying an S3 location in the metadata store
    SnapshotCheckpoint,     // DuckDB's checkpoint before a snapshot is sealed
    BranchHeadPublish,      // updating the S3 branch heads after a snapshot or branch
    Count
};
inline constexpr const char *metric_names[] = {"native_commit",
                                               "explicit_checkpoint",
                                               "local_read",
                                               "local_write",
                                               "manifest_seal",
                                               "snapshot_publish",
                                               "export",
                                               "compaction",
                                               "sdk_request",
                                               "extent_lookup",
                                               "remote_manifest_lookup",
                                               "local_sync",
                                               "namespace_update",
                                               "pin_update",
                                               "attach_resolve",
                                               "branch_head_resolve",
                                               "remote_descriptor_open",
                                               "s3_client_create",
                                               "s3_credentials",
                                               "s3_dns_lookup",
                                               "s3_connect",
                                               "s3_first_byte",
                                               "export_pack_build",
                                               "export_stage",
                                               "export_put",
                                               "export_verify",
                                               "attach_repository_open",
                                               "s3_secret_lookup",
                                               "s3_provider_registration",
                                               "snapshot_checkpoint",
                                               "branch_head_publish"};
static_assert(sizeof(metric_names) / sizeof(metric_names[0]) == static_cast<size_t>(Metric::Count),
              "every metric needs a name");
struct Measurements {
    std::atomic<uint64_t> calls{0}, nanoseconds{0}, max_nanoseconds{0};
};
inline std::array<Measurements, static_cast<size_t>(Metric::Count)> measurements;
inline void Record(Metric metric, uint64_t nanoseconds) {
    auto &measurement = measurements[static_cast<size_t>(metric)];
    ++measurement.calls;
    measurement.nanoseconds += nanoseconds;
    auto previous = measurement.max_nanoseconds.load();
    while (previous < nanoseconds && !measurement.max_nanoseconds.compare_exchange_weak(previous, nanoseconds)) {
    }
}
enum class Counter : size_t {
    LogicalLocalReadBytes,
    LocalMaterializedBytes,
    LocalPoolReadBytes,
    LocalPoolWriteBytes,
    WriterConflicts,
    PinConflicts,
    WalCheckpoints,
    RemoteObjectReadBytes, // remote-only objects read on demand through the store (shared writers)
    RemoteObjectAheadBytes, // ... and fetched ahead of sequential reads
    Count
};
inline std::array<std::atomic<uint64_t>, static_cast<size_t>(Counter::Count)> counters{};
inline void Count(Counter counter, uint64_t amount = 1) { counters[static_cast<size_t>(counter)] += amount; }
class Timer {
    Metric metric;
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

  public:
    explicit Timer(Metric metric) : metric(metric) {}
    ~Timer() {
        Record(metric, uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now() - start)
                                    .count()));
    }
};
inline std::vector<std::pair<std::string, uint64_t>> Timings() {
    std::vector<std::pair<std::string, uint64_t>> result;
    for (size_t i = 0; i < measurements.size(); ++i) {
        result.emplace_back(std::string(metric_names[i]) + "_calls", measurements[i].calls.load());
        result.emplace_back(std::string(metric_names[i]) + "_nanoseconds",
                            measurements[i].nanoseconds.load());
        result.emplace_back(std::string(metric_names[i]) + "_max_nanoseconds",
                            measurements[i].max_nanoseconds.load());
    }
    const char *names[] = {"local_logical_read_bytes", "local_materialized_payload_bytes",
                           "local_pool_read_bytes",    "local_pool_write_bytes",
                           "writer_conflicts",         "pin_conflicts",
                           "local_wal_checkpoints",    "local_remote_object_read_bytes",
                           "local_remote_object_ahead_bytes"};
    for (size_t i = 0; i < counters.size(); ++i)
        result.emplace_back(names[i], counters[i].load());
    return result;
}
} // namespace differential
