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
    // One SDK operation each, with the SDK's retries, excluding the wait for a request permit
    S3RequestGet,              // whole small object
    S3RequestRangeGet,         // byte range of a pack, index page or descriptor
    S3RequestPut,              // create-only immutable object
    S3RequestConditionalPut,   // branch heads, records, markers (If-Match / If-None-Match)
    S3RequestMultipartPart,    // one part of a multipart upload
    S3RequestMultipartControl, // create, complete or abort a multipart upload
    S3RequestHead,
    S3RequestList,
    S3RequestDelete,
    // The two halves of a durable local publication
    LocalSegmentSync,  // fsync of a file group's segment
    LocalMetadataCommit, // the SQLite transaction commit (FULL or NORMAL)
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
                                               "branch_head_publish",
                                               "s3_request_get",
                                               "s3_request_range_get",
                                               "s3_request_put",
                                               "s3_request_conditional_put",
                                               "s3_request_multipart_part",
                                               "s3_request_multipart_control",
                                               "s3_request_head",
                                               "s3_request_list",
                                               "s3_request_delete",
                                               "local_segment_sync",
                                               "local_metadata_commit"};
static_assert(sizeof(metric_names) / sizeof(metric_names[0]) == static_cast<size_t>(Metric::Count),
              "every metric needs a name");
// Latency distribution of one metric, lock-free: a log-linear histogram with four buckets per power
// of two (percentiles within about 12%), and the sum of squares for the standard deviation.
struct Measurements {
    static constexpr size_t BUCKETS = 64 * 4;
    std::atomic<uint64_t> calls{0}, nanoseconds{0}, max_nanoseconds{0};
    std::atomic<double> squares{0}; // sum of squared microseconds
    std::array<std::atomic<uint64_t>, BUCKETS> histogram{};
    static size_t Bucket(uint64_t ns) {
        if (ns < 4)
            return size_t(ns);
        auto exponent = 63 - __builtin_clzll(ns);
        return size_t(exponent) * 4 + ((ns >> (exponent - 2)) & 3);
    }
    // The midpoint of a bucket's range.
    static uint64_t Value(size_t bucket) {
        if (bucket < 4)
            return bucket;
        auto exponent = bucket / 4, sub = bucket % 4;
        auto low = (uint64_t(4 | sub)) << (exponent - 2);
        return low + (uint64_t(1) << (exponent - 2)) / 2;
    }
    uint64_t Percentile(double q) const {
        uint64_t total = 0;
        for (auto &count : histogram)
            total += count.load();
        if (!total)
            return 0;
        auto rank = uint64_t(q * double(total - 1)) + 1, seen = uint64_t(0);
        for (size_t i = 0; i < BUCKETS; ++i) {
            seen += histogram[i].load();
            if (seen >= rank)
                return Value(i);
        }
        return max_nanoseconds.load();
    }
    uint64_t StandardDeviation() const {
        auto n = double(calls.load());
        if (n < 2)
            return 0;
        auto mean = double(nanoseconds.load()) / n / 1000.0; // microseconds
        auto variance = squares.load() / n - mean * mean;
        return variance > 0 ? uint64_t(__builtin_sqrt(variance) * 1000.0) : 0;
    }
};
inline std::array<Measurements, static_cast<size_t>(Metric::Count)> measurements;
inline void Record(Metric metric, uint64_t nanoseconds) {
    auto &measurement = measurements[static_cast<size_t>(metric)];
    ++measurement.calls;
    measurement.nanoseconds += nanoseconds;
    ++measurement.histogram[Measurements::Bucket(nanoseconds)];
    auto micros = double(nanoseconds) / 1000.0;
    auto squares = measurement.squares.load();
    while (!measurement.squares.compare_exchange_weak(squares, squares + micros * micros)) {
    }
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
    LocalSyncRequests,      // durable syncs of file groups (native FileSync)
    LocalSyncTransactions,  // FULL transactions that committed them (group commit: fewer)
    LocalSyncBatchMax,      // most syncs one transaction committed (a maximum, not a sum)
    LocalSyncBatchFallbacks, // batches retried one sync at a time after a failure
    LocalBackgroundPublications, // overlays published in the background (NORMAL commits)
    LocalSegmentBytesCompacted,  // live payload rewritten out of sparse retired segments
    LocalSegmentsDeleted,        // retired segments deleted once nothing referenced them
    Count
};
inline std::array<std::atomic<uint64_t>, static_cast<size_t>(Counter::Count)> counters{};
inline void Count(Counter counter, uint64_t amount = 1) { counters[static_cast<size_t>(counter)] += amount; }
inline void CountMax(Counter counter, uint64_t value) {
    auto &slot = counters[static_cast<size_t>(counter)];
    auto current = slot.load();
    while (value > current && !slot.compare_exchange_weak(current, value)) {
    }
}
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
        result.emplace_back(std::string(metric_names[i]) + "_stddev_nanoseconds",
                            measurements[i].StandardDeviation());
        result.emplace_back(std::string(metric_names[i]) + "_p50_nanoseconds", measurements[i].Percentile(0.50));
        result.emplace_back(std::string(metric_names[i]) + "_p95_nanoseconds", measurements[i].Percentile(0.95));
        result.emplace_back(std::string(metric_names[i]) + "_p99_nanoseconds", measurements[i].Percentile(0.99));
    }
    const char *names[] = {"local_logical_read_bytes", "local_materialized_payload_bytes",
                           "local_pool_read_bytes",    "local_pool_write_bytes",
                           "writer_conflicts",         "pin_conflicts",
                           "local_wal_checkpoints",    "local_remote_object_read_bytes",
                           "local_remote_object_ahead_bytes", "local_sync_requests",
                           "local_sync_transactions",  "local_sync_batch_max",
                           "local_sync_batch_fallbacks", "local_background_publications",
                           "local_segment_bytes_compacted", "local_segments_deleted"};
    static_assert(sizeof(names) / sizeof(names[0]) == static_cast<size_t>(Counter::Count),
                  "every counter needs a name");
    for (size_t i = 0; i < counters.size(); ++i)
        result.emplace_back(names[i], counters[i].load());
    return result;
}
} // namespace differential
