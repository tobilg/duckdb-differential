#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <stdexcept>
namespace differential {
struct S3Config {
    std::string bucket, prefix, region = "us-east-1", endpoint, credential_profile;
    bool path_style = false, development_http = false;
    unsigned max_requests = 0; // per-location cap on parallel requests; 0 = the process limit
    unsigned retries = 2;
    unsigned connect_timeout_ms = 1000, request_timeout_ms = 5000;
    uint64_t max_range_bytes = 8 * 1024 * 1024, multipart_threshold = 16 * 1024 * 1024;
    // Registration identity: location plus endpoint, so equal bucket/prefix names on different
    // S3-compatible services never share a registration.
    std::string Identity() const {
        return "s3://" + bucket + "/" + prefix + (endpoint.empty() ? "" : " @ " + endpoint);
    }
};
// Credentials taken from a DuckDB secret for one provider identity. They live in process memory
// only: never in the metadata store, descriptors, heads or messages. Without an entry (or with a
// CREDENTIAL_PROFILE registration) the AWS SDK resolves credentials itself.
struct SecretCredentials {
    std::string secret_name, key_id, secret, session_token;
    bool sdk_chain = false; // a credential_chain secret created with REFRESH: the SDK resolves and refreshes
    std::string profile;    // ... from this profile (empty: the default chain)
};
void UseSecretCredentials(const std::string &identity, std::optional<SecretCredentials> credentials);
// Parallel S3 requests per process (differential_s3_max_requests): 1 to MAX_PARALLEL_REQUESTS.
constexpr unsigned MAX_PARALLEL_REQUESTS = 64;
void SetRequestLimit(unsigned limit);
unsigned RequestLimit();
std::optional<SecretCredentials> SecretCredentialsFor(const std::string &identity);
// s3://bucket/key... -> {bucket, key}. Throws for anything else.
struct S3Location {
    std::string bucket, key;
    static bool Is(const std::string &url) { return url.rfind("s3://", 0) == 0; }
    static S3Location Parse(const std::string &url) {
        if (!Is(url))
            throw std::runtime_error("Expected an s3://bucket/prefix/ location");
        auto rest = url.substr(5);
        auto slash = rest.find('/');
        S3Location location{rest.substr(0, slash), slash == std::string::npos ? "" : rest.substr(slash + 1)};
        if (location.bucket.empty() ||
            location.bucket.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789.-") != std::string::npos)
            throw std::runtime_error("Invalid S3 bucket in " + url);
        return location;
    }
};
struct ObjectStat {
    uint64_t size;
    std::string sha256;
    std::string etag; // as returned by S3, quoted
};
struct ObjectPage {
    std::vector<std::string> keys;
    std::vector<uint64_t> sizes;   // per key
    std::vector<int64_t> modified; // per key: S3 LastModified, microseconds since the epoch
    std::string continuation;
};
// Transport only. Publication, ownership and retention belong to the metadata authority.
class S3Provider {
  public:
    explicit S3Provider(S3Config config);
    ~S3Provider();
    S3Provider(const S3Provider &) = delete;
    ObjectStat Head(const std::string &key);
    std::optional<ObjectStat> TryHead(const std::string &key);
    std::vector<unsigned char> ReadRange(const std::string &key, uint64_t offset, uint64_t length,
                                         uint64_t total, const std::string &range_sha256);
    // A small immutable object named by its SHA-256, in one GET (no HEAD): at most `limit` bytes,
    // verified against `sha256`.
    std::vector<unsigned char> ReadObject(const std::string &key, uint64_t limit, const std::string &sha256);
    // Bounds-checked range of a larger immutable object. The caller verifies every chunk
    // it contains against the chunk identities recorded in a verified index.
    std::vector<unsigned char> ReadSpan(const std::string &key, uint64_t offset, uint64_t length);
    // Returns true when this call created the immutable object, false when an identical
    // object already existed (conditional PUT precondition).
    bool PutFile(const std::string &key, const std::string &file, const std::string &sha256);
    // The same for an object held in memory (below the multipart threshold), without staging it.
    bool PutBytes(const std::string &key, const std::vector<unsigned char> &bytes, const std::string &sha256);
    const S3Config &Config() const;
    // Requests this provider may run at once: its MAX_REQUESTS, within the process limit.
    unsigned Concurrency() const;
    // Keys under the owned prefix (optionally only those under `within`), relative to the prefix, in key
    // order; with `start_after`, only keys after that one.
    ObjectPage List(const std::string &continuation = "", unsigned limit = 1000, const std::string &within = "",
                    const std::string &start_after = "");
    // Overwrite a small mutable object (branch heads only: dbs/<uuid>/branches/<name>). Every other
    // object is immutable and written once with PutFile.
    // Write a small mutable object (branch heads only: dbs/<uuid>/branches/<name>) conditionally: only
    // if its current ETag is `if_match`, or, with an empty `if_match`, only if it does not exist.
    // Returns the new ETag, or nothing when the condition failed (someone else wrote it).
    std::optional<std::string> PutMutable(const std::string &key, const std::vector<unsigned char> &bytes,
                                          const std::string &if_match);
    // Advisory writer markers (dbs/<uuid>/writers/<branch>/<writer>): written unconditionally by
    // their one writer session and deleted when it ends (or by anyone once expired).
    void PutMarker(const std::string &key, const std::vector<unsigned char> &bytes);
    void DeleteMarker(const std::string &key);
    // A small create-only record (snapshot expiry: dbs/<uuid>/expired/<n>, and its entry in the
    // time-ordered dbs/<uuid>/expiries/ log); false if it existed.
    bool PutRecord(const std::string &key, const std::vector<unsigned char> &bytes);
    // Current content and ETag of a small database record (branch head, writer marker, commit or
    // expiry record) in one GET; nothing when it does not exist.
    std::optional<std::pair<std::vector<unsigned char>, std::string>> ReadMutable(const std::string &key);
    // Remote garbage collection only: delete an immutable data object (descriptors/<sha256>,
    // dbs/<uuid>/indexes/<sha256> or dbs/<uuid>/packs/<sha256>). Deleting a missing object succeeds.
    void DeleteImmutable(const std::string &key);
    // Test fixture setup only; requires an explicitly enabled loopback HTTP endpoint.
    void CreateTestBucket();
    static std::vector<std::pair<std::string, uint64_t>> Metrics();

  private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};
} // namespace differential
