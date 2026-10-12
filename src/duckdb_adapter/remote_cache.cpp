#include "remote_cache.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include <cstring>

namespace duckdb {
namespace {
constexpr const char *VERSION_TAG = "immutable"; // remote objects are content-addressed
constexpr idx_t MAX_RANGE = 16ull * 1024 * 1024;  // larger than any inserted span (8 MiB)

// Ranges are verified spans of immutable objects (index pages, coalesced pack slots). Entries
// are never invalidated; the buffer manager evicts their memory, and an evicted range is
// replaced by the next insert that covers it.
class ExternalFileCacheAdapter : public differential::RemoteMemoryCache {
  public:
    explicit ExternalFileCacheAdapter(DatabaseInstance &db) : db(db) {}
    bool Enabled() const override { return ExternalFileCache::Get(db).IsEnabled(); }

    bool Read(const std::string &object, uint64_t offset, uint64_t length, unsigned char *out) override {
        auto &cache = ExternalFileCache::Get(db);
        if (!cache.IsEnabled())
            return false;
        auto file = cache.GetOrCreateCachedFile(object);
        auto guard = file->lock.GetSharedLock();
        auto &ranges = file->Ranges(guard);
        auto it = ranges.upper_bound(offset);
        while (it != ranges.begin()) {
            --it;
            auto &range = *it->second;
            if (range.location + MAX_RANGE <= offset)
                break;
            if (range.location <= offset && range.location + range.nr_bytes >= offset + length) {
                auto pin = cache.GetBufferManager().Pin(range.block_handle);
                if (pin.IsValid()) {
                    std::memcpy(out, pin.Ptr() + (offset - range.location), length);
                    return true;
                }
            }
        }
        return false;
    }
    void Insert(const std::string &object, uint64_t offset, const unsigned char *data, uint64_t length) override {
        auto &cache = ExternalFileCache::Get(db);
        if (!cache.IsEnabled() || length == 0 || length > MAX_RANGE)
            return;
        try {
            auto pin = cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
            std::memcpy(pin.Ptr(), data, length);
            auto range = make_shared_ptr<ExternalFileCache::CachedFileRange>(pin.GetBlockHandle(), length, offset,
                                                                             string(VERSION_TAG));
            auto file = cache.GetOrCreateCachedFile(object);
            auto guard = file->lock.GetExclusiveLock();
            auto &ranges = file->Ranges(guard);
            // Drop ranges the new one covers entirely (including evicted ones).
            for (auto it = ranges.lower_bound(offset); it != ranges.end() && it->second->location < offset + length;) {
                if (it->second->location + it->second->nr_bytes <= offset + length)
                    it = ranges.erase(it);
                else
                    ++it;
            }
            ranges[offset] = std::move(range);
            file->VersionTag(guard) = VERSION_TAG;
        } catch (const std::exception &) {
            // Out of memory for the cache: the read itself already succeeded.
        }
    }

  private:
    DatabaseInstance &db;
};
} // namespace

std::shared_ptr<differential::RemoteMemoryCache> ExternalFileMemoryCache(DatabaseInstance &db) {
    return std::make_shared<ExternalFileCacheAdapter>(db);
}
uint64_t ParseRemoteCacheSize(const Value &value) {
    if (value.IsNull())
        return 0;
    auto text = StringUtil::Lower(StringUtil::Replace(value.ToString(), " ", ""));
    if (text.empty() || text == "0" || text == "0b" || text == "off" || text == "false")
        return 0;
    return DBConfig::ParseMemoryLimit(text);
}
uint64_t RemoteDiskCacheLimit(optional_ptr<FileOpener> opener) {
    Value setting;
    if (!opener || !FileOpener::TryGetCurrentSetting(opener, "differential_remote_cache_size", setting))
        return 0;
    return ParseRemoteCacheSize(setting);
}
uint64_t RemoteDiskCacheLimit(ClientContext &context) {
    Value setting;
    if (!context.TryGetCurrentSetting("differential_remote_cache_size", setting))
        return 0;
    return ParseRemoteCacheSize(setting);
}
} // namespace duckdb
