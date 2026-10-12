#include "differential_s3.hpp"
#include <openssl/evp.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <filesystem>
#include <thread>
#include <atomic>
static std::string Hash(const std::vector<unsigned char> &bytes) {
    unsigned char hash[32];
    unsigned n;
    if (EVP_Digest(bytes.data(), bytes.size(), hash, &n, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("Hash failed");
    std::string result;
    const char *hex = "0123456789abcdef";
    for (auto b : hash) {
        result += hex[b >> 4];
        result += hex[b & 15];
    }
    return result;
}
int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "usage: s3_test loopback_endpoint disposable_directory\n";
        return 2;
    }
    try {
        differential::S3Config config;
        config.bucket = "differential-sdk-test";
        config.prefix = "owned-test/";
        config.endpoint = argv[1];
        config.path_style = true;
        config.development_http = true;
        config.multipart_threshold = 6 * 1024 * 1024;
        // Explicit existing-namespace runner: never creates a bucket or enumerates
        // outside its freshly allocated prefix. Credentials remain in the SDK chain.
        const char *existing = std::getenv("DIFFERENTIAL_TEST_EXISTING_BUCKET");
        if (existing) {
            auto value = [](const char *name, const char *fallback = "") {
                const char *p = std::getenv(name);
                return std::string(p ? p : fallback);
            };
            config.bucket = existing;
            config.prefix = value("DIFFERENTIAL_TEST_PREFIX");
            if (config.prefix.empty())
                throw std::runtime_error("Existing-provider test requires its disposable prefix");
            config.region = value("DIFFERENTIAL_TEST_REGION", "us-east-1");
            config.credential_profile = value("DIFFERENTIAL_TEST_PROFILE");
            config.path_style = value("DIFFERENTIAL_TEST_PATH_STYLE") == "1";
            config.development_http = value("DIFFERENTIAL_TEST_ALLOW_HTTP") == "1";
        }
        differential::S3Provider provider(config);
        if (const char *mode = std::getenv("DIFFERENTIAL_S3_FAULT_MODE")) {
            std::vector<unsigned char> expected(11);
            for (size_t i = 0; i < expected.size(); i++)
                expected[i] = static_cast<unsigned char>(i + 3);
            if (std::string(mode) == "parallel") {
                // Three clients share the process-wide request limit (differential_s3_max_requests).
                differential::SetRequestLimit(4);
                std::atomic<bool> failure{false};
                differential::S3Provider second(config), third(config);
                std::vector<std::thread> threads;
                for (unsigned i = 0; i < 12; i++)
                    threads.emplace_back([&, i] {
                        try {
                            if ((i % 3 == 0   ? provider
                                 : i % 3 == 1 ? second
                                              : third)
                                    .ReadRange("objects/test", 3, 11, 64, Hash(expected)) != expected)
                                failure = true;
                        } catch (...) {
                            failure = true;
                        }
                    });
                for (auto &thread : threads)
                    thread.join();
                if (failure)
                    throw std::runtime_error("Concurrent range read failed");
                std::cout << "PASS SDK global request concurrency bound across three clients\n";
                return 0;
            }
            bool failed = false;
            try {
                auto result = provider.ReadRange("objects/test", 3, 11, 64, Hash(expected));
                if (result != expected)
                    throw std::runtime_error("Mismatched bytes");
            } catch (const std::exception &error) {
                auto canary = std::getenv("DIFFERENTIAL_ERROR_CANARY");
                if (canary && std::string(error.what()).find(canary) != std::string::npos)
                    throw std::runtime_error("Provider error exposed reflected private text");
                failed = true;
            }
            if (failed == (std::string(mode) == "valid"))
                throw std::runtime_error("Unexpected range fault result");
            std::cout << "PASS SDK range fault " << mode << "\n";
            return 0;
        }
        if (!existing && !std::getenv("DIFFERENTIAL_TEST_BUCKET_EXISTS"))
            provider.CreateTestBucket();
        for (auto size : {size_t(65536), size_t(11 * 1024 * 1024)}) {
            std::vector<unsigned char> bytes(size);
            for (size_t i = 0; i < size; i++)
                bytes[i] = static_cast<unsigned char>(i * 7);
            auto hash = Hash(bytes), key = "objects/" + hash;
            auto file = std::filesystem::path(argv[2]) / "payload";
            {
                std::ofstream output(file, std::ios::binary);
                output.write(reinterpret_cast<char *>(bytes.data()), bytes.size());
            }
            provider.PutFile(key, file.string(), hash);
            auto stat = provider.Head(key);
            if (stat.size != size || stat.sha256 != hash)
                throw std::runtime_error("HEAD mismatch");
            std::vector<unsigned char> expected(bytes.begin() + 123, bytes.begin() + 4567);
            auto range = provider.ReadRange(key, 123, expected.size(), size, Hash(expected));
            if (range != expected)
                throw std::runtime_error("Range mismatch");
            provider.PutFile(key, file.string(),
                             hash); // Conditional idempotent PUT and multipart retry/abort.
            auto listed = provider.List("", 1);
            if (listed.keys.empty())
                throw std::runtime_error("LIST missing object");
            bool corrupt = false;
            try {
                provider.ReadRange(key, 123, expected.size(), size, std::string(64, '0'));
            } catch (const std::exception &) {
                corrupt = true;
            }
            if (!corrupt)
                throw std::runtime_error("Checksum mismatch accepted");
            provider.DeleteOwnedObject(key);
            bool missing = false;
            try {
                provider.Head(key);
            } catch (const std::exception &) {
                missing = true;
            }
            if (!missing)
                throw std::runtime_error("Deleted object still present");
            std::cout << "PASS SDK " << (size == 65536 ? "PUT" : "multipart")
                      << ", HEAD, exact range/checksum, LIST, DELETE, missing object\n";
        }
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}
