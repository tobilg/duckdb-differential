#include "differential_metrics.hpp"
#include "differential_s3.hpp"
#include "differential_error.hpp"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/monitoring/MonitoringFactory.h>
#include <aws/core/monitoring/MonitoringInterface.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <openssl/evp.h>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <streambuf>

namespace differential {
namespace {
std::atomic<uint64_t> get_calls{0}, head_calls{0}, put_calls{0}, list_calls{0}, delete_calls{0}, get_bytes{0},
    put_bytes{0}, failures{0};
std::atomic<uint64_t> retry_decisions{0};
std::mutex request_mutex;
std::condition_variable request_condition;
unsigned active_requests = 0;
std::atomic<unsigned> request_limit{16}; // differential_s3_max_requests default
struct CountedRetryStrategy : Aws::Client::DefaultRetryStrategy {
    explicit CountedRetryStrategy(long retries) : DefaultRetryStrategy(retries, 25) {}
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                     long attempts) const override {
        auto retry = DefaultRetryStrategy::ShouldRetry(error, attempts);
        if (retry)
            ++retry_decisions;
        return retry;
    }
};
// The SDK's HTTP client reports per-request timings in milliseconds since the request started:
// DNS lookup, connection ready (DNS + TCP + TLS) for new connections, and time to the first byte.
// (Its SslLatency is not used: with current curl the SDK reports it in nanoseconds.)
class ConnectionTimings : public Aws::Monitoring::MonitoringInterface {
    static void Collect(const Aws::Monitoring::CoreMetricsCollection &metrics) {
        using Type = Aws::Monitoring::HttpClientMetricsType;
        auto take = [&](Type type, Metric metric, bool positive_only) {
            auto found = metrics.httpClientMetrics.find(Aws::Monitoring::GetHttpClientMetricNameByType(type));
            if (found != metrics.httpClientMetrics.end() && found->second >= 0 && (!positive_only || found->second > 0))
                Record(metric, uint64_t(found->second) * 1000000);
        };
        take(Type::DnsLatency, Metric::S3DnsLookup, true);
        take(Type::TimeToConnect, Metric::S3Connect, true);
        take(Type::TimeToFirstByte, Metric::S3FirstByte, false);
    }

  public:
    void *OnRequestStarted(const Aws::String &, const Aws::String &,
                           const std::shared_ptr<const Aws::Http::HttpRequest> &) const override {
        return nullptr;
    }
    void OnRequestSucceeded(const Aws::String &, const Aws::String &, const std::shared_ptr<const Aws::Http::HttpRequest> &,
                            const Aws::Client::HttpResponseOutcome &,
                            const Aws::Monitoring::CoreMetricsCollection &metrics, void *) const override {
        Collect(metrics);
    }
    void OnRequestFailed(const Aws::String &, const Aws::String &, const std::shared_ptr<const Aws::Http::HttpRequest> &,
                         const Aws::Client::HttpResponseOutcome &, const Aws::Monitoring::CoreMetricsCollection &metrics,
                         void *) const override {
        Collect(metrics);
    }
    void OnRequestRetry(const Aws::String &, const Aws::String &, const std::shared_ptr<const Aws::Http::HttpRequest> &,
                        void *) const override {}
    void OnFinish(const Aws::String &, const Aws::String &, const std::shared_ptr<const Aws::Http::HttpRequest> &,
                  void *) const override {}
};
struct ConnectionTimingsFactory : Aws::Monitoring::MonitoringFactory {
    Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> CreateMonitoringInstance() const override {
        return Aws::MakeUnique<ConnectionTimings>("differential");
    }
};
struct SDK {
    Aws::SDKOptions options;
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> executor;
    // The SDK's default credential chain, built once per process on first use: building it
    // constructs providers whose own client configurations may query the EC2 metadata service.
    std::once_flag chain_once;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> chain;
    Aws::Auth::AWSCredentialsProvider &DefaultChain() {
        std::call_once(chain_once,
                       [this] { chain = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("differential"); });
        return *chain;
    }
    SDK() {
        options.monitoringOptions.customizedMonitoringFactory_create_fn.push_back(
            [] { return Aws::MakeUnique<ConnectionTimingsFactory>("differential"); });
        Aws::InitAPI(options);
        executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("differential", 4);
    }
    ~SDK() {
        chain.reset();
        executor.reset();
        Aws::ShutdownAPI(options);
    }
};
std::shared_ptr<SDK> Runtime() {
    // One initialization per module/process lifetime; avoids Init/Shutdown races
    // between concurrent last-owner destruction and creation of another provider.
    static auto runtime = std::make_shared<SDK>();
    return runtime;
}
std::string Digest(const void *bytes, size_t size) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned n = 0;
    if (EVP_Digest(bytes, size, hash, &n, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("SHA256 failure");
    const char *hex = "0123456789abcdef";
    std::string out;
    for (unsigned i = 0; i < n; i++) {
        out += hex[hash[i] >> 4];
        out += hex[hash[i] & 15];
    }
    return out;
}
Aws::String Base64Hash(const std::string &hex) {
    unsigned char bytes[32];
    unsigned char encoded[45];
    for (unsigned i = 0; i < 32; i++)
        bytes[i] = static_cast<unsigned char>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
    auto n = EVP_EncodeBlock(encoded, bytes, 32);
    return Aws::String(reinterpret_cast<char *>(encoded), size_t(n));
}
std::mutex secret_mutex;
std::map<std::string, SecretCredentials> &SecretRegistry() {
    static std::map<std::string, SecretCredentials> registry;
    return registry;
}
// Credentials of a provider without CREDENTIAL_PROFILE, read on every request: the DuckDB
// secret registered for its identity, else the SDK default chain. Secrets created, replaced
// or dropped later apply to existing clients once the extension looks them up again.
struct SecretAwareCredentials : Aws::Auth::AWSCredentialsProvider {
    std::string identity;
    std::shared_ptr<SDK> sdk = Runtime();
    std::mutex mutex;
    std::map<std::string, std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> profiles;
    explicit SecretAwareCredentials(std::string identity) : identity(std::move(identity)) {}
    Aws::Auth::AWSCredentials GetAWSCredentials() override {
        Timer timer(Metric::S3Credentials);
        auto credentials = SecretCredentialsFor(identity);
        if (!credentials || (credentials->sdk_chain && credentials->profile.empty()))
            return sdk->DefaultChain().GetAWSCredentials();
        if (credentials->sdk_chain) {
            std::lock_guard<std::mutex> guard(mutex);
            auto &profile = profiles[credentials->profile];
            if (!profile)
                profile = Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
                    "differential", credentials->profile.c_str());
            return profile->GetAWSCredentials();
        }
        return Aws::Auth::AWSCredentials(credentials->key_id.c_str(), credentials->secret.c_str(),
                                         credentials->session_token.c_str());
    }
};
// A credentials provider whose calls count as s3_credentials time.
struct TimedCredentials : Aws::Auth::AWSCredentialsProvider {
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> inner;
    explicit TimedCredentials(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> inner) : inner(std::move(inner)) {}
    Aws::Auth::AWSCredentials GetAWSCredentials() override {
        Timer timer(Metric::S3Credentials);
        return inner->GetAWSCredentials();
    }
};
bool IsHash(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
struct BoundedBuffer : std::streambuf {
    std::vector<unsigned char> bytes;
    explicit BoundedBuffer(size_t n) : bytes(n) { Reset(); }
    size_t Count() const { return size_t(pptr() - pbase()); }
    void Reset() {
        auto base = reinterpret_cast<char *>(bytes.data());
        setp(base, base + bytes.size());
        setg(base, base, base);
    }

  protected:
    int_type underflow() override {
        auto base = reinterpret_cast<char *>(bytes.data());
        auto offset = gptr() - eback();
        setg(base, base + offset, base + Count());
        return gptr() == egptr() ? traits_type::eof() : traits_type::to_int_type(*gptr());
    }
    pos_type seekoff(off_type offset, std::ios_base::seekdir direction,
                     std::ios_base::openmode mode) override {
        auto base = reinterpret_cast<char *>(bytes.data());
        off_type current = (mode & std::ios_base::in) ? gptr() - eback() : pptr() - pbase();
        off_type target = offset + (direction == std::ios_base::beg   ? 0
                                    : direction == std::ios_base::cur ? current
                                                                      : off_type(Count()));
        if (target < 0 || uint64_t(target) > bytes.size() ||
            ((mode & std::ios_base::in) && uint64_t(target) > Count()))
            return pos_type(off_type(-1));
        auto written = Count();
        if (mode & std::ios_base::out) {
            setp(base, base + bytes.size());
            pbump(int(target));
        }
        if (mode & std::ios_base::in)
            setg(base, base + target, base + written);
        return pos_type(target);
    }
    pos_type seekpos(pos_type position, std::ios_base::openmode mode) override {
        return seekoff(off_type(position), std::ios_base::beg, mode);
    }
};
template <class Outcome> void Check(const Outcome &outcome, const char *operation) {
    if (!outcome.IsSuccess()) {
        ++failures;
        // Deliberately exclude SDK messages: they may contain endpoints, signed URLs or server-reflected
        // data.
        throw std::runtime_error(
            std::string(outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN
                            ? "[differential:access_denied] "
                            : "[differential:unavailable_object] ") +
            "differential S3 " + operation + " failed; HTTP " +
            std::to_string(int(outcome.GetError().GetResponseCode())) + "; SDK " +
            std::to_string(int(outcome.GetError().GetErrorType())));
    }
}
} // namespace
void UseSecretCredentials(const std::string &identity, std::optional<SecretCredentials> credentials) {
    std::lock_guard<std::mutex> guard(secret_mutex);
    if (credentials)
        SecretRegistry()[identity] = std::move(*credentials);
    else
        SecretRegistry().erase(identity);
}
std::optional<SecretCredentials> SecretCredentialsFor(const std::string &identity) {
    std::lock_guard<std::mutex> guard(secret_mutex);
    auto found = SecretRegistry().find(identity);
    if (found == SecretRegistry().end())
        return std::nullopt;
    return found->second;
}
void SetRequestLimit(unsigned limit) {
    if (limit == 0 || limit > MAX_PARALLEL_REQUESTS)
        throw std::runtime_error("differential_s3_max_requests must be between 1 and " +
                                 std::to_string(MAX_PARALLEL_REQUESTS));
    {
        std::lock_guard<std::mutex> lock(request_mutex);
        request_limit = limit;
    }
    request_condition.notify_all();
}
unsigned RequestLimit() { return request_limit.load(); }
// One SDK call, timed per operation type (retries included, the request permit wait excluded).
template <class F>
auto Timed(Metric metric, F &&call) {
    Timer timer(metric);
    return call();
}
struct S3Provider::Impl {
    std::shared_ptr<SDK> sdk = Runtime(); // Construct before and destroy after the client.
    S3Config config;
    std::unique_ptr<Aws::S3::S3Client> client;
    unsigned active = 0;
    struct Permit {
        Impl &owner;
        Timer timer{Metric::SdkRequest}; // Includes bounded-permit wait and all SDK retries.
        explicit Permit(Impl &owner) : owner(owner) {
            std::unique_lock<std::mutex> lock(request_mutex);
            request_condition.wait(
                lock, [&] { return active_requests < request_limit.load() && owner.active < owner.Concurrency(); });
            ++active_requests;
            ++owner.active;
        }
        ~Permit() {
            std::lock_guard<std::mutex> lock(request_mutex);
            --active_requests;
            --owner.active;
            request_condition.notify_all();
        }
    };
    unsigned Concurrency() const {
        auto limit = request_limit.load();
        return config.max_requests ? std::min(config.max_requests, limit) : limit;
    }
    explicit Impl(S3Config cfg) : config(std::move(cfg)) {
        Timer timer(Metric::S3ClientCreate);
        if (config.endpoint.find_first_of("@?#\r\n") != std::string::npos)
            throw std::runtime_error("Endpoint must not contain credentials, query or fragment");
        if (config.bucket.empty() || config.prefix.empty() || config.prefix.back() != '/' ||
            config.prefix.find("..") != std::string::npos)
            throw std::runtime_error("S3 requires a bucket and an owned prefix ending in /");
        if (config.multipart_threshold < 5 * 1024 * 1024 || config.multipart_threshold > 512 * 1024 * 1024 ||
            config.max_requests > MAX_PARALLEL_REQUESTS || config.max_range_bytes < 1024 * 1024 ||
            config.max_range_bytes > 8 * 1024 * 1024 || config.retries > 10 ||
            config.connect_timeout_ms == 0 || config.connect_timeout_ms > 30000 ||
            config.request_timeout_ms == 0 || config.request_timeout_ms > 300000)
            throw std::runtime_error("Invalid S3 resource bounds");
        if (config.endpoint.rfind("http://", 0) == 0 && !config.development_http)
            throw std::runtime_error("HTTP endpoints require explicit development mode");
        // The region is always explicit. Without this flag the SDK asks the EC2 instance metadata
        // service for one whenever none is configured in the environment or profile: on hosts
        // outside EC2 that request can wait for its timeout, adding seconds to client creation.
        // Credentials (including EC2 instance roles) come from separately built providers.
        Aws::Client::ClientConfigurationInitValues init;
        init.shouldDisableIMDS = true;
        Aws::Client::ClientConfiguration options(init);
        options.region = config.region.c_str();
        options.endpointOverride = config.endpoint.c_str();
        options.verifySSL = true;
        options.maxConnections = config.max_requests ? config.max_requests : MAX_PARALLEL_REQUESTS;
        options.connectTimeoutMs = config.connect_timeout_ms;
        options.requestTimeoutMs = config.request_timeout_ms;
        options.retryStrategy = Aws::MakeShared<CountedRetryStrategy>("differential", config.retries);
        options.executor = sdk->executor;
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials;
        if (config.credential_profile.empty())
            credentials = Aws::MakeShared<SecretAwareCredentials>("differential", config.Identity());
        else
            credentials = Aws::MakeShared<TimedCredentials>(
                "differential", Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
                                    "differential", config.credential_profile.c_str()));
        client = std::make_unique<Aws::S3::S3Client>(
            credentials, options, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            !config.path_style);
    }
    Aws::String Key(const std::string &key) {
        if (key.empty() || key[0] == '/' || key.find("..") != std::string::npos ||
            key.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-/.") !=
                std::string::npos)
            throw std::runtime_error("Invalid owned S3 key");
        return (config.prefix + key).c_str();
    }
    std::optional<ObjectStat> TryHead(const std::string &key) {
        Aws::S3::Model::HeadObjectRequest req;
        req.SetBucket(config.bucket.c_str());
        req.SetKey(Key(key));
        ++head_calls;
        auto out = Timed(Metric::S3RequestHead, [&] { return client->HeadObject(req); });
        if (!out.IsSuccess() && int(out.GetError().GetResponseCode()) == 404)
            return std::nullopt;
        Check(out, "HEAD");
        auto &result = out.GetResult();
        auto it = result.GetMetadata().find("sha256");
        if (result.GetContentLength() < 0)
            throw std::runtime_error("[differential:corrupt_data] Invalid S3 object length");
        return ObjectStat{uint64_t(result.GetContentLength()),
                          it == result.GetMetadata().end() ? "" : it->second.c_str(), result.GetETag().c_str()};
    }
    // One create-only PUT of an immutable object whose SHA-256 the service verifies
    // (x-amz-checksum-sha256). False when an identical object already exists.
    bool PutOnce(const std::string &key, const std::shared_ptr<Aws::IOStream> &body, uint64_t size,
                 const std::string &sha) {
        Aws::S3::Model::PutObjectRequest req;
        req.SetBucket(config.bucket.c_str());
        req.SetKey(Key(key));
        req.SetContentLength(int64_t(size));
        req.SetMetadata(Aws::Map<Aws::String, Aws::String> {{"sha256", sha.c_str()}});
        req.SetIfNoneMatch("*");
        req.SetChecksumSHA256(Base64Hash(sha));
        req.SetBody(body);
        ++put_calls;
        put_bytes += size;
        auto out = Timed(Metric::S3RequestPut, [&] { return client->PutObject(req); });
        if (!out.IsSuccess() && out.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
            auto existing = Head(key);
            if (existing.size == size && existing.sha256 == sha)
                return false;
            throw Error(ErrorCode::ObjectConflict, "[differential:corrupt_data] Immutable object identity conflict");
        }
        Check(out, "PUT");
        return true;
    }
    ObjectStat Head(const std::string &key) {
        auto result = TryHead(key);
        if (!result) {
            ++failures;
            throw std::runtime_error(
                "[differential:unavailable_object] differential S3 HEAD object unavailable; HTTP 404");
        }
        return *result;
    }
};
S3Provider::S3Provider(S3Config config) : impl(std::make_unique<Impl>(std::move(config))) {}
S3Provider::~S3Provider() = default;
std::optional<ObjectStat> S3Provider::TryHead(const std::string &key) {
    Impl::Permit permit(*impl);
    return impl->TryHead(key);
}
ObjectStat S3Provider::Head(const std::string &key) {
    Impl::Permit permit(*impl);
    return impl->Head(key);
}
namespace {
// Shared GET path: exact Range request, response bounds and length checks.
std::vector<unsigned char> RangeGet(Aws::S3::S3Client &client, const S3Config &config, const Aws::String &key,
                                    uint64_t offset, uint64_t length, const std::string &total_suffix) {
    auto buffer = std::make_shared<BoundedBuffer>(size_t(std::max<uint64_t>(length, 4096)));
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(config.bucket.c_str());
    req.SetKey(key);
    std::string range = "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1);
    req.SetRange(range.c_str());
    req.SetResponseStreamFactory([buffer] {
        buffer->Reset();
        return Aws::New<Aws::IOStream>("differential", buffer.get());
    });
    ++get_calls;
    auto outcome = Timed(Metric::S3RequestRangeGet, [&] { return client.GetObject(req); });
    Check(outcome, "range GET");
    auto &result = outcome.GetResult();
    std::string expected = "bytes " + std::to_string(offset) + "-" + std::to_string(offset + length - 1) + "/";
    std::string actual = result.GetContentRange().c_str();
    if (actual.rfind(expected, 0) != 0 || (!total_suffix.empty() && actual != expected + total_suffix) ||
        result.GetContentLength() != int64_t(length) || buffer->Count() != length) {
        ++failures;
        throw std::runtime_error("[differential:corrupt_data] Invalid S3 range response bounds or length");
    }
    buffer->bytes.resize(size_t(length));
    return std::move(buffer->bytes);
}
} // namespace
std::vector<unsigned char> S3Provider::ReadRange(const std::string &key, uint64_t offset, uint64_t length,
                                                 uint64_t total, const std::string &sha) {
    if (length == 0)
        return {};
    if (offset > total || length > total - offset || length > impl->config.max_range_bytes || !IsHash(sha))
        throw std::runtime_error("Invalid or unbounded S3 range");
    Impl::Permit permit(*impl);
    auto bytes = RangeGet(*impl->client, impl->config, impl->Key(key), offset, length, std::to_string(total));
    if (Digest(bytes.data(), bytes.size()) != sha) {
        ++failures;
        throw std::runtime_error("[differential:corrupt_data] Corrupt S3 range checksum");
    }
    get_bytes += length;
    return bytes;
}
std::vector<unsigned char> S3Provider::ReadObject(const std::string &key, uint64_t limit, const std::string &sha) {
    if (limit == 0 || limit > impl->config.max_range_bytes || !IsHash(sha))
        throw std::runtime_error("Invalid or unbounded S3 object read");
    Impl::Permit permit(*impl);
    // A range covering the limit returns the whole object (206 with its total size) when it fits.
    auto buffer = std::make_shared<BoundedBuffer>(size_t(limit));
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    req.SetRange(("bytes=0-" + std::to_string(limit - 1)).c_str());
    req.SetResponseStreamFactory([buffer] {
        buffer->Reset();
        return Aws::New<Aws::IOStream>("differential", buffer.get());
    });
    ++get_calls;
    auto outcome = Timed(Metric::S3RequestGet, [&] { return impl->client->GetObject(req); });
    Check(outcome, "GET");
    auto &result = outcome.GetResult();
    auto size = uint64_t(buffer->Count());
    std::string range = result.GetContentRange().c_str();
    bool whole = range.empty() ? uint64_t(result.GetContentLength()) == size
                               : range == "bytes 0-" + std::to_string(size - 1) + "/" + std::to_string(size);
    if (size == 0 || !whole || uint64_t(result.GetContentLength()) != size) {
        ++failures;
        throw std::runtime_error("[differential:corrupt_data] S3 object larger than its bound or truncated");
    }
    buffer->bytes.resize(size_t(size));
    if (Digest(buffer->bytes.data(), buffer->bytes.size()) != sha) {
        ++failures;
        throw std::runtime_error("[differential:corrupt_data] Corrupt S3 object checksum");
    }
    get_bytes += size;
    return std::move(buffer->bytes);
}
std::vector<unsigned char> S3Provider::ReadSpan(const std::string &key, uint64_t offset, uint64_t length) {
    if (length == 0)
        return {};
    if (length > impl->config.max_range_bytes)
        throw std::runtime_error("Invalid or unbounded S3 range");
    Impl::Permit permit(*impl);
    auto bytes = RangeGet(*impl->client, impl->config, impl->Key(key), offset, length, "");
    get_bytes += length;
    return bytes;
}
const S3Config &S3Provider::Config() const { return impl->config; }
unsigned S3Provider::Concurrency() const { return impl->Concurrency(); }
bool S3Provider::PutFile(const std::string &key, const std::string &file, const std::string &sha) {
    if (!IsHash(sha))
        throw std::runtime_error("Invalid object checksum");
    Impl::Permit permit(*impl);
    auto size = std::filesystem::file_size(file);
    if (size > uint64_t(5) * 1024 * 1024 * 10000)
        throw std::runtime_error("Object exceeds bounded multipart limit");
    auto metadata = Aws::Map<Aws::String, Aws::String>{{"sha256", sha.c_str()}};
    // A single PUT carries the SHA-256 the caller computed (x-amz-checksum-sha256): the service rejects a
    // staging file that changed. Multipart parts are checked against it below as they are read.
    if (size < impl->config.multipart_threshold)
        return impl->PutOnce(
            key, Aws::MakeShared<Aws::FStream>("differential", file.c_str(), std::ios::in | std::ios::binary), size,
            sha);
    Aws::S3::Model::CreateMultipartUploadRequest create;
    create.SetBucket(impl->config.bucket.c_str());
    create.SetKey(impl->Key(key));
    create.SetMetadata(metadata);
    create.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::SHA256);
    auto start = Timed(Metric::S3RequestMultipartControl, [&] { return impl->client->CreateMultipartUpload(create); });
    Check(start, "multipart create");
    auto upload = start.GetResult().GetUploadId();
    try {
        constexpr size_t PART = 5 * 1024 * 1024;
        if ((size + PART - 1) / PART > 10000)
            throw std::runtime_error("Multipart object exceeds configured part limit");
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uploaded(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        if (!uploaded || EVP_DigestInit_ex(uploaded.get(), EVP_sha256(), nullptr) != 1)
            throw std::runtime_error("Checksum initialization failure");
        std::ifstream input(file, std::ios::binary);
        std::vector<char> bytes(PART);
        Aws::S3::Model::CompletedMultipartUpload completed;
        int number = 0;
        while (input) {
            input.read(bytes.data(), bytes.size());
            auto n = input.gcount();
            if (n == 0)
                break;
            if (EVP_DigestUpdate(uploaded.get(), bytes.data(), size_t(n)) != 1)
                throw std::runtime_error("Multipart checksum failure");
            auto part_hash = Base64Hash(Digest(bytes.data(), size_t(n)));
            auto body = Aws::MakeShared<Aws::StringStream>("differential");
            body->write(bytes.data(), n);
            Aws::S3::Model::UploadPartRequest part;
            part.SetBucket(impl->config.bucket.c_str());
            part.SetKey(impl->Key(key));
            part.SetUploadId(upload);
            part.SetPartNumber(++number);
            part.SetContentLength(n);
            part.SetBody(body);
            part.SetChecksumSHA256(part_hash);
            auto out = Timed(Metric::S3RequestMultipartPart, [&] { return impl->client->UploadPart(part); });
            Check(out, "multipart part");
            Aws::S3::Model::CompletedPart done;
            done.SetPartNumber(number);
            done.SetETag(out.GetResult().GetETag());
            done.SetChecksumSHA256(part_hash);
            completed.AddParts(done);
        }
        if (!input.eof())
            throw std::runtime_error("Cannot read multipart staging");
        unsigned char actual_bytes[32], actual_base64[45];
        unsigned actual_size;
        if (EVP_DigestFinal_ex(uploaded.get(), actual_bytes, &actual_size) != 1 || actual_size != 32)
            throw std::runtime_error("Multipart checksum finalization failure");
        EVP_EncodeBlock(actual_base64, actual_bytes, 32);
        if (Base64Hash(sha) != reinterpret_cast<char *>(actual_base64))
            throw std::runtime_error("Staging changed during multipart upload");
        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(impl->config.bucket.c_str());
        req.SetKey(impl->Key(key));
        req.SetUploadId(upload);
        req.SetMultipartUpload(completed);
        req.SetIfNoneMatch("*");
        auto out = Timed(Metric::S3RequestMultipartControl, [&] { return impl->client->CompleteMultipartUpload(req); });
        if (!out.IsSuccess() &&
            out.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
            auto existing = impl->Head(key);
            if (existing.size == size && existing.sha256 == sha) {
                Aws::S3::Model::AbortMultipartUploadRequest abort;
                abort.SetBucket(impl->config.bucket.c_str());
                abort.SetKey(impl->Key(key));
                abort.SetUploadId(upload);
                auto cleanup = Timed(Metric::S3RequestMultipartControl, [&] { return impl->client->AbortMultipartUpload(abort); });
                Check(cleanup, "duplicate multipart abort");
                return false;
            }
        }
        Check(out, "multipart complete");
        return true;
    } catch (...) {
        Aws::S3::Model::AbortMultipartUploadRequest abort;
        abort.SetBucket(impl->config.bucket.c_str());
        abort.SetKey(impl->Key(key));
        abort.SetUploadId(upload);
        Timed(Metric::S3RequestMultipartControl, [&] { return impl->client->AbortMultipartUpload(abort); });
        // Process death can leave an upload: durable upload intents/reconciliation are a later integration
        // gate.
        throw;
    }
}
bool S3Provider::PutBytes(const std::string &key, const std::vector<unsigned char> &bytes, const std::string &sha) {
    if (!IsHash(sha) || bytes.size() >= impl->config.multipart_threshold)
        throw std::runtime_error("Invalid checksum, or an object for a multipart upload");
    Impl::Permit permit(*impl);
    // Streams the caller's buffer without copying it (the SDK rewinds it to retry).
    Aws::Utils::Stream::PreallocatedStreamBuf buffer(const_cast<unsigned char *>(bytes.data()), bytes.size());
    return impl->PutOnce(key, Aws::MakeShared<Aws::IOStream>("differential", &buffer), bytes.size(), sha);
}
std::optional<std::string> S3Provider::PutMutable(const std::string &key, const std::vector<unsigned char> &bytes,
                                                  const std::string &if_match) {
    if (key.rfind("dbs/", 0) != 0 || key.find("/branches/") == std::string::npos || bytes.size() > 64 * 1024)
        throw std::runtime_error("Only small branch-head objects are mutable");
    auto sha = Digest(bytes.data(), bytes.size());
    Impl::Permit permit(*impl);
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    req.SetContentLength(int64_t(bytes.size()));
    req.SetMetadata(Aws::Map<Aws::String, Aws::String>{{"sha256", sha.c_str()}});
    req.SetChecksumSHA256(Base64Hash(sha));
    auto body = Aws::MakeShared<Aws::StringStream>("differential");
    body->write(reinterpret_cast<const char *>(bytes.data()), std::streamsize(bytes.size()));
    req.SetBody(body);
    if (if_match.empty())
        req.SetIfNoneMatch("*");
    else
        req.SetIfMatch(if_match.c_str());
    ++put_calls;
    put_bytes += bytes.size();
    auto outcome = Timed(Metric::S3RequestConditionalPut, [&] { return impl->client->PutObject(req); });
    auto status = outcome.IsSuccess() ? Aws::Http::HttpResponseCode::OK : outcome.GetError().GetResponseCode();
    // 412: the object changed (or exists); 409: a concurrent conditional write to the same key.
    if (status == Aws::Http::HttpResponseCode::PRECONDITION_FAILED || status == Aws::Http::HttpResponseCode::CONFLICT)
        return std::nullopt;
    Check(outcome, "PUT");
    return std::string(outcome.GetResult().GetETag().c_str());
}
std::optional<std::pair<std::vector<unsigned char>, std::string>> S3Provider::ReadMutable(const std::string &key) {
    if (key.rfind("dbs/", 0) != 0 ||
        (key.find("/branches/") == std::string::npos && key.find("/writers/") == std::string::npos &&
         key.find("/snapshots/") == std::string::npos && key.find("/expired/") == std::string::npos))
        throw std::runtime_error("Only small database records are read whole");
    constexpr uint64_t LIMIT = 64 * 1024;
    Impl::Permit permit(*impl);
    auto buffer = std::make_shared<BoundedBuffer>(size_t(LIMIT));
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    req.SetResponseStreamFactory([buffer] {
        buffer->Reset();
        return Aws::New<Aws::IOStream>("differential", buffer.get());
    });
    ++get_calls;
    auto outcome = Timed(Metric::S3RequestGet, [&] { return impl->client->GetObject(req); });
    if (!outcome.IsSuccess() && outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        return std::nullopt;
    Check(outcome, "GET");
    auto &result = outcome.GetResult();
    auto size = uint64_t(buffer->Count());
    if (size == 0 || size >= LIMIT || uint64_t(result.GetContentLength()) != size) {
        ++failures;
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head object");
    }
    buffer->bytes.resize(size_t(size));
    get_bytes += size;
    return std::make_pair(std::move(buffer->bytes), std::string(result.GetETag().c_str()));
}
ObjectPage S3Provider::List(const std::string &continuation, unsigned limit, const std::string &within,
                            const std::string &start_after) {
    if (!limit || limit > 1000)
        throw std::runtime_error("S3 list page must be bounded to 1..1000");
    Impl::Permit permit(*impl);
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetPrefix(within.empty() ? impl->config.prefix.c_str() : impl->Key(within));
    req.SetMaxKeys(int(limit));
    if (!continuation.empty())
        req.SetContinuationToken(continuation.c_str());
    else if (!start_after.empty())
        req.SetStartAfter(impl->Key(start_after));
    ++list_calls;
    auto out = Timed(Metric::S3RequestList, [&] { return impl->client->ListObjectsV2(req); });
    Check(out, "LIST");
    ObjectPage page;
    for (auto &object : out.GetResult().GetContents()) {
        std::string key = object.GetKey().c_str();
        if (key.rfind(impl->config.prefix, 0) != 0)
            throw std::runtime_error("S3 LIST returned foreign key");
        page.keys.push_back(key.substr(impl->config.prefix.size()));
        page.sizes.push_back(object.GetSize() < 0 ? 0 : uint64_t(object.GetSize()));
        page.modified.push_back(object.GetLastModified().Millis() * 1000);
    }
    page.continuation = out.GetResult().GetNextContinuationToken().c_str();
    return page;
}
namespace {
void CheckMarkerKey(const std::string &key) {
    if (key.rfind("dbs/", 0) != 0 || key.find("/writers/") == std::string::npos)
        throw std::runtime_error("Only writer markers are advisory");
}
} // namespace
void S3Provider::PutMarker(const std::string &key, const std::vector<unsigned char> &bytes) {
    CheckMarkerKey(key);
    if (bytes.empty() || bytes.size() > 64 * 1024)
        throw std::runtime_error("Writer markers are small");
    auto sha = Digest(bytes.data(), bytes.size());
    Impl::Permit permit(*impl);
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    req.SetContentLength(int64_t(bytes.size()));
    req.SetChecksumSHA256(Base64Hash(sha));
    auto body = Aws::MakeShared<Aws::StringStream>("differential");
    body->write(reinterpret_cast<const char *>(bytes.data()), std::streamsize(bytes.size()));
    req.SetBody(body);
    ++put_calls;
    put_bytes += bytes.size();
    Check(Timed(Metric::S3RequestConditionalPut, [&] { return impl->client->PutObject(req); }), "PUT");
}
bool S3Provider::PutRecord(const std::string &key, const std::vector<unsigned char> &bytes) {
    if (key.rfind("dbs/", 0) != 0 ||
        (key.find("/expired/") == std::string::npos && key.find("/expiries/") == std::string::npos) || bytes.empty() ||
        bytes.size() > 64 * 1024)
        throw std::runtime_error("Only small expiry records are written this way");
    auto sha = Digest(bytes.data(), bytes.size());
    Impl::Permit permit(*impl);
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    req.SetContentLength(int64_t(bytes.size()));
    req.SetChecksumSHA256(Base64Hash(sha));
    req.SetIfNoneMatch("*");
    auto body = Aws::MakeShared<Aws::StringStream>("differential");
    body->write(reinterpret_cast<const char *>(bytes.data()), std::streamsize(bytes.size()));
    req.SetBody(body);
    ++put_calls;
    put_bytes += bytes.size();
    auto outcome = Timed(Metric::S3RequestConditionalPut, [&] { return impl->client->PutObject(req); });
    auto status = outcome.IsSuccess() ? Aws::Http::HttpResponseCode::OK : outcome.GetError().GetResponseCode();
    if (status == Aws::Http::HttpResponseCode::PRECONDITION_FAILED || status == Aws::Http::HttpResponseCode::CONFLICT)
        return false;
    Check(outcome, "PUT");
    return true;
}
void S3Provider::DeleteMarker(const std::string &key) {
    CheckMarkerKey(key);
    Impl::Permit permit(*impl);
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    ++delete_calls;
    Check(Timed(Metric::S3RequestDelete, [&] { return impl->client->DeleteObject(req); }), "DELETE");
}
void S3Provider::DeleteImmutable(const std::string &key) {
    auto slash = key.rfind('/');
    auto folder = slash == std::string::npos ? std::string() : key.substr(0, slash + 1);
    bool shape = folder == "descriptors/";
    bool epoch = false;
    if (folder.rfind("dbs/", 0) == 0 && folder.size() > 40) {
        auto database = folder.substr(4, 36), kind = folder.substr(40);
        bool uuid = database.find_first_not_of("0123456789abcdef-") == std::string::npos;
        shape = uuid && (kind == "/packs/" || kind == "/indexes/");
        // Shipping epochs: dbs/<uuid>/log/<branch name>/<branch uuid>/<10 digits>/<10 digits>.
        auto name = key.substr(slash + 1);
        epoch = uuid && kind.rfind("/log/", 0) == 0 && name.size() == 10 &&
                name.find_first_not_of("0123456789") == std::string::npos &&
                std::count(kind.begin(), kind.end(), '/') == 5;
    }
    if (!epoch && (!shape || !IsHash(key.substr(slash + 1))))
        throw std::runtime_error("Deletion requires an immutable descriptor, index, pack or shipping epoch key");
    Impl::Permit permit(*impl);
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    req.SetKey(impl->Key(key));
    ++delete_calls;
    auto out = Timed(Metric::S3RequestDelete, [&] { return impl->client->DeleteObject(req); });
    Check(out, "DELETE");
}
void S3Provider::CreateTestBucket() {
    if (!impl->config.development_http || impl->config.endpoint.rfind("http://127.0.0.1:", 0) != 0)
        throw std::runtime_error("Test bucket setup requires loopback development endpoint");
    auto port = impl->config.endpoint.substr(std::string("http://127.0.0.1:").size());
    if (port.empty() || port.size() > 5 || port.find_first_not_of("0123456789") != std::string::npos ||
        std::stoul(port) == 0 || std::stoul(port) > 65535)
        throw std::runtime_error("Invalid loopback test endpoint");
    Impl::Permit permit(*impl);
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(impl->config.bucket.c_str());
    auto out = impl->client->CreateBucket(req);
    Check(out, "test bucket create");
}
std::vector<std::pair<std::string, uint64_t>> S3Provider::Metrics() {
    return {{"s3_get_calls", get_calls.load()},
            {"s3_head_calls", head_calls.load()},
            {"s3_put_calls", put_calls.load()},
            {"s3_list_calls", list_calls.load()},
            {"s3_delete_calls", delete_calls.load()},
            {"s3_verified_get_bytes", get_bytes.load()},
            {"s3_single_put_input_bytes", put_bytes.load()},
            {"s3_failed_calls", failures.load()},
            {"s3_retry_decisions", retry_decisions.load()}};
}
} // namespace differential
