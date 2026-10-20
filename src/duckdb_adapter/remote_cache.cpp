#include "remote_cache.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache.hpp"
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace duckdb {
namespace {
constexpr const char *VERSION_TAG = "immutable"; // remote objects are content-addressed
constexpr uint64_t MAX_PENDING = 32ull * 1024 * 1024; // queued insert bytes; more are dropped

// Spans are verified ranges of immutable objects (index pages, coalesced pack slots), each cached
// as one loaded block of the object's CachedFile. Blocks are never invalidated; the buffer manager
// evicts (or spills) their memory, and the next insert covering an evicted block replaces it.
//
// Reads arrive while DuckDB loads a block and holds that block's lock, so they must never make the
// buffer manager evict: eviction locks queued blocks, one of which another loading thread may hold
// while it evicts in turn. Reads therefore use only resident blocks, and inserts (which allocate)
// are queued and made by a worker thread that holds no block lock. The worker runs while file
// handles that read through the cache are open (Retain/Release), which ends before DuckDB tears
// down the cache and the buffer manager.
class ExternalFileCacheAdapter : public differential::RemoteMemoryCache {
  public:
    explicit ExternalFileCacheAdapter(DatabaseInstance &db) : db(db) {}
    ~ExternalFileCacheAdapter() override { Stop(); }
    bool Enabled() const override { return ExternalFileCache::Get(db).IsEnabled(); }

    bool Read(const std::string &object, uint64_t offset, uint64_t length, unsigned char *out) override {
        auto &cache = ExternalFileCache::Get(db);
        if (!cache.IsEnabled())
            return false;
        return ReadPending(object, offset, length, out) || ReadCached(cache, object, offset, length, out);
    }
    void Insert(const std::string &object, uint64_t offset, const unsigned char *data, uint64_t length) override {
        if (length == 0 || length > MAX_PENDING || !ExternalFileCache::Get(db).IsEnabled())
            return;
        std::lock_guard<std::mutex> guard(mutex);
        if (users == 0 || pending_bytes + length > MAX_PENDING)
            return; // best effort: the read itself already succeeded
        pending.push_back({object, offset, std::vector<unsigned char>(data, data + length)});
        pending_bytes += length;
        wake.notify_one();
    }
    void Retain() override {
        std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
        {
            std::lock_guard<std::mutex> guard(mutex);
            if (users++ > 0)
                return;
            stopping = false;
        }
        worker = std::thread([this] { Work(); });
    }
    void Release() override {
        std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
        {
            std::lock_guard<std::mutex> guard(mutex);
            if (users == 0 || --users > 0)
                return;
        }
        Stop();
    }

  private:
    struct Pending {
        std::string object;
        uint64_t offset;
        std::vector<unsigned char> bytes;
    };

    // A span still queued (or being inserted) serves reads that follow right after its fetch.
    bool ReadPending(const std::string &object, uint64_t offset, uint64_t length, unsigned char *out) {
        std::lock_guard<std::mutex> guard(mutex);
        auto covers = [&](const Pending &item) {
            return item.object == object && item.offset <= offset && item.offset + item.bytes.size() >= offset + length;
        };
        const Pending *found = current && covers(*current) ? current.get() : nullptr;
        for (auto it = pending.rbegin(); !found && it != pending.rend(); ++it)
            if (covers(*it))
                found = &*it;
        if (!found)
            return false;
        std::memcpy(out, found->bytes.data() + (offset - found->offset), length);
        return true;
    }
    bool ReadCached(ExternalFileCache &cache, const std::string &object, uint64_t offset, uint64_t length,
                    unsigned char *out) {
        auto file = cache.GetOrCreateCachedFile(object);
        shared_ptr<CacheBlock> block;
        {
            const annotated_lock_guard<annotated_mutex> guard(file->map_lock);
            auto it = file->blocks.upper_bound(offset);
            if (it == file->blocks.begin())
                return false;
            --it; // blocks do not overlap: only this one can cover offset
            if (it->first + it->second->size < offset + length)
                return false;
            block = it->second;
        }
        shared_ptr<BlockHandle> handle;
        {
            const annotated_lock_guard<annotated_mutex> guard(block->mtx);
            if (block->state != CacheBlockState::LOADED || !block->block_handle ||
                block->location + block->nr_bytes < offset + length)
                return false;
            handle = block->block_handle;
        }
        // Pin only if resident (the fast path of BufferManager::Pin); the handle is released after
        // the block lock, as Pin does.
        BufferHandle pin;
        {
            auto &memory = handle->GetMemory();
            auto lock = memory.GetLock();
            if (memory.GetState() != BlockState::BLOCK_LOADED)
                return false;
            pin = handle->Load(lock, QueryContext());
        }
        if (!pin.IsValid())
            return false;
        std::memcpy(out, pin.Ptr() + (offset - block->location), length);
        return true;
    }
    // Discards what is pending and joins the worker (after the insert it is making).
    void Stop() {
        {
            std::lock_guard<std::mutex> guard(mutex);
            stopping = true;
            pending.clear();
            pending_bytes = current ? current->bytes.size() : 0;
            wake.notify_all();
        }
        if (worker.joinable())
            worker.join();
    }
    void Work() {
        std::unique_lock<std::mutex> lock(mutex);
        for (;;) {
            wake.wait(lock, [&] { return stopping || !pending.empty(); });
            if (stopping)
                return;
            current = std::make_unique<Pending>(std::move(pending.front()));
            pending.pop_front();
            lock.unlock();
            Store(current->object, current->offset, current->bytes.data(), current->bytes.size());
            lock.lock();
            pending_bytes -= current->bytes.size();
            current.reset();
        }
    }
    void Store(const std::string &object, uint64_t offset, const unsigned char *data, uint64_t length) {
        auto &cache = ExternalFileCache::Get(db);
        if (!cache.IsEnabled())
            return;
        try {
            auto pin = ExternalFileCache::AllocateCacheBuffer(cache.GetBufferManager(), object, length);
            std::memcpy(pin.GetDataMutable(), data, length);
            auto block = make_shared_ptr<CacheBlock>(offset, length);
            {
                const annotated_lock_guard<annotated_mutex> guard(block->mtx);
                block->block_handle = pin.GetBlockHandle();
                block->nr_bytes = length;
                block->state = CacheBlockState::LOADED;
#ifdef DEBUG
                block->checksum = Checksum(pin.Ptr(), length);
#endif
            }
            auto file = cache.GetOrCreateCachedFile(object);
            {
                const annotated_lock_guard<annotated_mutex> guard(file->meta_lock);
                file->validation_info.version_tag = VERSION_TAG;
                file->validation_info.file_size = MaxValue<idx_t>(file->validation_info.file_size, offset + length);
            }
            // The new span replaces every block it overlaps (readers keep the blocks they hold).
            const annotated_lock_guard<annotated_mutex> guard(file->map_lock);
            auto it = file->blocks.upper_bound(offset);
            if (it != file->blocks.begin() && std::prev(it)->first + std::prev(it)->second->size > offset)
                --it;
            while (it != file->blocks.end() && it->first < offset + length)
                it = file->blocks.erase(it);
            file->blocks.emplace(offset, std::move(block));
        } catch (const std::exception &) {
            // Out of memory for the cache: the read itself already succeeded.
        }
    }

    DatabaseInstance &db;
    std::mutex lifecycle_mutex; // serializes Retain/Release (worker start and join)
    std::mutex mutex;
    std::condition_variable wake;
    std::deque<Pending> pending;
    std::unique_ptr<Pending> current; // being inserted by the worker
    uint64_t pending_bytes = 0;       // queued and current
    uint64_t users = 0;
    bool stopping = false;
    std::thread worker;
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
    return DBConfig::ParseMemoryLimitOrMaximum(text);
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
