#include "secrets.hpp"
#include "differential_metrics.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
namespace {
constexpr const char *SECRET_TYPE = "differential";
constexpr const char *DEFAULT_SECRET = "__default_differential";

const vector<std::pair<const char *, LogicalType>> &SecretOptions() {
    static const vector<std::pair<const char *, LogicalType>> options = {
        {"data_path", LogicalType::VARCHAR},          {"region", LogicalType::VARCHAR},
        {"endpoint", LogicalType::VARCHAR},           {"credential_profile", LogicalType::VARCHAR},
        {"path_style", LogicalType::BOOLEAN},         {"allow_http", LogicalType::BOOLEAN},
        {"max_requests", LogicalType::BIGINT},        {"retries", LogicalType::BIGINT},
        {"connect_timeout_ms", LogicalType::BIGINT},  {"request_timeout_ms", LogicalType::BIGINT},
        {"max_range_bytes", LogicalType::BIGINT},     {"multipart_threshold", LogicalType::BIGINT}};
    return options;
}
// CREATE SECRET (TYPE differential, DATA_PATH …, REGION …): settings only. Credentials belong in
// TYPE s3 secrets (or the AWS SDK chain), so the named parameters include none.
unique_ptr<BaseSecret> CreateDifferentialSecret(ClientContext &, CreateSecretInput &input) {
    if (!input.scope.empty())
        throw InvalidInputException("TYPE differential secrets take no SCOPE: name one in ATTACH (SECRET name), or "
                                    "create it without a name to apply it to every differential ATTACH");
    DataPathOptions validate;
    auto secret = make_uniq<KeyValueSecret>(vector<string>(), SECRET_TYPE, "config", input.name);
    for (auto &option : input.options) {
        auto name = StringUtil::Lower(option.first);
        if (!validate.Take(name, option.second))
            throw InvalidInputException("Unknown option %s for a TYPE differential secret", name);
        secret->secret_map[Identifier(name)] = option.second;
    }
    if (!validate.data_path.empty() && differential::S3Location::Is(validate.data_path))
        validate.ConfigFor(validate.data_path); // bucket, prefix and S3 option bounds
    return std::move(secret);
}
CatalogTransaction SystemTransaction(ClientContext &context) {
    return CatalogTransaction::GetSystemCatalogTransaction(context);
}
// The s3 secret DuckDB would use for this location (longest matching SCOPE), if any.
unique_ptr<KeyValueSecret> S3Secret(ClientContext &context, const string &location) {
    auto match = SecretManager::Get(context).LookupSecret(SystemTransaction(context), location, "s3");
    if (!match.HasMatch())
        return nullptr;
    auto secret = dynamic_cast<const KeyValueSecret *>(&match.GetSecret());
    return secret ? make_uniq<KeyValueSecret>(*secret) : nullptr;
}
string Text(const KeyValueSecret &secret, const string &key) {
    Value value;
    return secret.TryGetValue(Identifier(key), value) && !value.IsNull() ? value.ToString() : string();
}
// The s3 secret's connection settings, as defaults: REGION, ENDPOINT (+ USE_SSL), URL_STYLE.
void ApplyS3Secret(ClientContext &context, DataPathOptions &options, const string &location) {
    auto secret = S3Secret(context, location);
    if (!secret)
        return;
    auto region = Text(*secret, "region");
    if (!region.empty())
        options.Default("region", Value(region));
    auto endpoint = Text(*secret, "endpoint");
    // httpfs stores AWS's own endpoint for credential_chain secrets; that needs no override.
    bool aws = endpoint.empty() || endpoint == "s3.amazonaws.com" ||
               StringUtil::EndsWith(endpoint, ".amazonaws.com");
    if (!aws) {
        auto ssl = Text(*secret, "use_ssl");
        bool use_ssl = ssl.empty() || BooleanValue::Get(Value(ssl).DefaultCastAs(LogicalType::BOOLEAN));
        if (endpoint.find("://") == string::npos)
            endpoint = (use_ssl ? "https://" : "http://") + endpoint;
        options.Default("endpoint", Value(endpoint));
        if (!use_ssl)
            options.Default("allow_http", Value::BOOLEAN(true));
    }
    if (StringUtil::Lower(Text(*secret, "url_style")) == "path")
        options.Default("path_style", Value::BOOLEAN(true));
}
// Credentials for a registered provider: none with CREDENTIAL_PROFILE (the profile decides),
// else those of the s3 secret matching its location.
void UseS3Secret(ClientContext &context, const differential::S3Config &config) {
    auto identity = config.Identity();
    if (!config.credential_profile.empty()) {
        differential::UseSecretCredentials(identity, std::nullopt);
        return;
    }
    auto secret = S3Secret(context, "s3://" + config.bucket + "/" + config.prefix);
    std::optional<differential::SecretCredentials> credentials;
    if (secret) {
        differential::SecretCredentials found;
        found.secret_name = secret->GetName().GetIdentifierName();
        Value refresh;
        if (secret->GetProvider() == "credential_chain" && secret->TryGetValue("refresh_info", refresh) &&
            !refresh.IsNull()) {
            // Created with REFRESH: let the SDK resolve (and refresh) from the same profile.
            found.sdk_chain = true;
            if (refresh.type().id() == LogicalTypeId::STRUCT) {
                auto &names = StructType::GetChildTypes(refresh.type());
                auto &values = StructValue::GetChildren(refresh);
                for (idx_t i = 0; i < names.size(); i++)
                    if (names[i].first == "profile" && !values[i].IsNull())
                        found.profile = values[i].ToString();
            }
            credentials = found;
        } else {
            found.key_id = Text(*secret, "key_id");
            found.secret = Text(*secret, "secret");
            found.session_token = Text(*secret, "session_token");
            if (!found.key_id.empty())
                credentials = found;
        }
    }
    differential::UseSecretCredentials(identity, credentials);
}
} // namespace

void RegisterDifferentialSecret(ExtensionLoader &loader) {
    SecretType type;
    type.name = SECRET_TYPE;
    type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
    type.default_provider = "config";
    type.extension = "differential";
    loader.RegisterSecretType(type);
    CreateSecretFunction function;
    function.secret_type = SECRET_TYPE;
    function.provider = "config";
    function.function = CreateDifferentialSecret;
    for (auto &option : SecretOptions())
        function.named_parameters[option.first] = option.second;
    loader.RegisterFunction(function);
}
void ApplyDifferentialSecret(ClientContext &context, DataPathOptions &options) {
    differential::Timer timer(differential::Metric::S3SecretLookup);
    auto &manager = SecretManager::Get(context);
    auto name = options.secret.empty() ? string(DEFAULT_SECRET) : options.secret;
    auto entry = manager.GetSecretByName(SystemTransaction(context), name);
    if (!entry) {
        if (!options.secret.empty())
            throw InvalidInputException("Secret \"%s\" does not exist", options.secret);
        return;
    }
    if (entry->secret->GetType() != SECRET_TYPE) {
        if (!options.secret.empty())
            throw InvalidInputException("Secret \"%s\" is a TYPE %s secret; SECRET takes a TYPE differential secret "
                                        "(TYPE s3 secrets are found by their SCOPE)",
                                        options.secret, entry->secret->GetType());
        return;
    }
    auto secret = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
    if (!secret)
        return;
    for (auto &option : secret->secret_map)
        options.Default(StringUtil::Lower(option.first.GetIdentifierName()), option.second);
}
string EnsureS3(ClientContext &context, differential::Control &control, const DataPathOptions &options,
                const string &location) {
    differential::Timer timer(differential::Metric::S3ProviderRegistration);
    auto resolved = options;
    ApplyS3Secret(context, resolved, location);
    auto id = control.EnsureS3Provider(resolved.ConfigFor(location), resolved.explicit_s3);
    UseS3Secret(context, control.Provider(id).config);
    return id;
}
string EnsureDataPath(ClientContext &context, differential::Control &control, const DataPathOptions &options) {
    if (options.data_path.empty()) {
        if (!options.explicit_s3.empty())
            throw InvalidInputException("S3 options require DATA_PATH 's3://bucket/prefix/'");
        return "local";
    }
    if (differential::S3Location::Is(options.data_path))
        return EnsureS3(context, control, options, options.data_path);
    if (!options.explicit_s3.empty())
        throw InvalidInputException("S3 options apply only to an s3:// DATA_PATH");
    return control.EnsureLocalPool(options.data_path);
}
void RefreshS3Credentials(ClientContext &context, differential::Control &control) {
    differential::Timer timer(differential::Metric::S3SecretLookup);
    for (auto &provider : control.S3Providers())
        UseS3Secret(context, provider.config);
}
} // namespace duckdb
