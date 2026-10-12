#pragma once
// DATA_PATH and S3 provider options shared by ATTACH and the management functions.
#include "duckdb.hpp"
#include "differential_control.hpp"
#include "differential_local.hpp"
#include "differential_s3.hpp"
#include <algorithm>
#include <set>

namespace duckdb {
// Options are explicit (given in the statement) or defaults from a secret. Explicit options win,
// must match an existing registration and fix DATA_PATH; defaults only fill what is unset.
struct DataPathOptions {
    string data_path;
    bool data_path_given = false; // DATA_PATH given explicitly
    string secret;                // SECRET: a TYPE differential secret
    differential::S3Config s3;
    vector<string> explicit_s3;   // S3 options given explicitly (must match an existing registration)
    std::set<string> chosen;      // every option set so far, explicit or from a secret

    // Consume one explicit option; false when the name is not a data-path/provider option.
    bool Take(const string &raw_name, const Value &value) {
        auto name = StringUtil::Lower(raw_name);
        if (value.IsNull())
            throw InvalidInputException("Option %s cannot be NULL", name);
        if (name == "secret") {
            secret = value.ToString();
            return true;
        }
        if (!Set(name, value))
            return false;
        if (name == "data_path")
            data_path_given = true;
        else
            explicit_s3.push_back(name);
        return true;
    }
    // A default from a secret: applies only when the option is still unset.
    void Default(const string &raw_name, const Value &value) {
        auto name = StringUtil::Lower(raw_name);
        if (!value.IsNull() && !chosen.count(name) && !Set(name, value))
            throw InvalidInputException("Unknown option %s", name);
    }
    // The validated provider configuration for an s3://bucket/prefix/ location.
    differential::S3Config ConfigFor(const string &location) const {
        auto parsed = differential::S3Location::Parse(location);
        auto config = s3;
        config.bucket = parsed.bucket;
        config.prefix = parsed.key;
        if (!config.prefix.empty() && config.prefix.back() != '/')
            config.prefix += '/';
        differential::S3Provider validate(config); // bounds, owned prefix, HTTP opt-in
        return config;
    }

  private:
    bool Set(const string &name, const Value &value) {
        if (name == "data_path")
            data_path = value.ToString();
        else if (name == "region")
            s3.region = value.ToString();
        else if (name == "endpoint")
            s3.endpoint = value.ToString();
        else if (name == "credential_profile")
            s3.credential_profile = value.ToString();
        else if (name == "path_style")
            s3.path_style = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
        else if (name == "allow_http")
            s3.development_http = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
        else if (name == "max_requests" || name == "retries" || name == "connect_timeout_ms" ||
                 name == "request_timeout_ms" || name == "max_range_bytes" || name == "multipart_threshold") {
            auto number = BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT));
            if (number < 0 || number > 512 * 1024 * 1024)
                throw InvalidInputException("S3 option %s outside supported range", name);
            if (name == "max_requests")
                s3.max_requests = unsigned(number);
            else if (name == "retries")
                s3.retries = unsigned(number);
            else if (name == "connect_timeout_ms")
                s3.connect_timeout_ms = unsigned(number);
            else if (name == "request_timeout_ms")
                s3.request_timeout_ms = unsigned(number);
            else if (name == "max_range_bytes")
                s3.max_range_bytes = uint64_t(number);
            else
                s3.multipart_threshold = uint64_t(number);
        } else
            return false;
        chosen.insert(name);
        return true;
    }
};
// Microseconds since the epoch of a TIMESTAMP/TIMESTAMPTZ/VARCHAR option value.
inline int64_t Microseconds(const Value &value) {
    if (value.type().id() == LogicalTypeId::TIMESTAMP_TZ)
        return value.GetValue<timestamp_tz_t>().value;
    return value.DefaultCastAs(LogicalType::TIMESTAMP).GetValue<timestamp_t>().value;
}
// s3://bucket/prefix/heads/<database>[/<database uuid>][/<branch>] -> {s3://bucket/prefix/, the rest}.
// The leftmost "/heads/" followed by a valid head path wins, so a prefix may itself contain "heads".
inline bool SplitHeadLocation(const string &url, std::pair<string, string> &out) {
    if (!differential::S3Location::Is(url))
        return false;
    auto name = [](const string &s) {
        return !s.empty() && s.size() <= 128 &&
               s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") == string::npos;
    };
    auto uuid = [](const string &s) {
        return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == string::npos;
    };
    for (auto pos = url.find("/heads/"); pos != string::npos; pos = url.find("/heads/", pos + 1)) {
        auto rest = url.substr(pos + 7);
        auto parts = StringUtil::Split(rest, '/');
        bool valid = !parts.empty() && parts.size() <= 3 && rest.back() != '/' && name(parts[0]) &&
                     (parts.size() < 2 || name(parts.back())) && (parts.size() < 3 || uuid(parts[1]));
        if (valid) {
            out = {url.substr(0, pos + 1), rest};
            return true;
        }
    }
    return false;
}
// s3://bucket/prefix/descriptors/<sha256> -> {s3://bucket/prefix/, descriptors/<sha256>}
inline std::pair<string, string> SplitExportLocation(const string &url) {
    auto marker = url.rfind("/descriptors/");
    if (!differential::S3Location::Is(url) || marker == string::npos)
        throw InvalidInputException("Expected an export descriptor location s3://bucket/prefix/descriptors/<sha256>");
    auto descriptor = url.substr(marker + 1);
    auto hash = descriptor.substr(12);
    if (hash.size() != 64 || hash.find_first_not_of("0123456789abcdef") != string::npos)
        throw InvalidInputException("Expected an export descriptor location s3://bucket/prefix/descriptors/<sha256>");
    return {url.substr(0, marker + 1), descriptor};
}
} // namespace duckdb
