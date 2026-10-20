// Remote garbage collection of an S3 location.
//
// Exported data is immutable and shared: index pages reference packs of any database in the
// location, and several repositories (shared databases, clones, copies) export into it. Collection
// therefore marks from roots in S3 itself (branch heads and commit records without an expiry
// record) plus this repository's own retained exports and S3-only objects, and deletes in two
// phases so that it never races an export:
//
//  1. Unreachable data objects older than the grace cutoff are condemned: their keys are written to
//     a create-only record, gc/condemned/<microseconds>-<sha256>.
//  2. A later collection deletes what was condemned before the cutoff and is still unreachable:
//     packs; index pages all of whose packs are condemned that long and unreachable (no export can
//     produce such a page again); descriptors with a deleted or missing index page.
//
// Every export learns the records first and once more just before it publishes its descriptor, and
// never references a condemned pack (an identical new pack is laid out differently). A pack
// condemned after that last check is reachable through the export's commit record by the time a
// collection could delete it. Repositories whose live files read objects only from S3 (shared
// writers' base snapshots) copy objects of condemned packs locally when they learn the record.
#include "s3_internal.hpp"
#include "differential_s3.hpp"
#include <openssl/evp.h>
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <sstream>

namespace differential {
namespace {
constexpr const char *RECORDS = "gc/condemned/";
constexpr const char *RECORD_FORMAT = "differential-condemned-v1";
constexpr size_t RECORD_KEYS = 8000;            // keys per record: below 1 MiB
constexpr uint64_t RECORD_LIMIT = 1024 * 1024;
constexpr uint64_t DESCRIPTOR_LIMIT = 1024 * 1024;
std::atomic<uint64_t> condemned_objects{0}, deleted_objects{0}, deleted_bytes{0}, rescued_objects{0},
    rescue_failures{0};
std::mutex error_mutex;
std::string last_error;

std::string Sha256(const std::vector<unsigned char> &bytes) {
    unsigned char digest[32];
    unsigned length = 0;
    if (EVP_Digest(bytes.data(), bytes.size(), digest, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("SHA256 failed");
    static const char *hex = "0123456789abcdef";
    std::string out;
    for (auto b : digest) {
        out += hex[b >> 4];
        out += hex[b & 15];
    }
    return out;
}
bool IsHash(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
bool IsUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
}
// Data objects collection may delete: descriptors/<sha256>, dbs/<uuid>/{packs,indexes}/<sha256>, and
// shipping epochs dbs/<uuid>/log/<branch name>/<branch uuid>/<10 digits>/<10 digits>.
enum class Kind { None, Descriptor, Index, Pack, Log };
bool Digits(const std::string &s, size_t n) {
    return s.size() == n && s.find_first_not_of("0123456789") == std::string::npos;
}
Kind DataKind(const std::string &key) {
    if (key.rfind("descriptors/", 0) == 0)
        return IsHash(key.substr(12)) ? Kind::Descriptor : Kind::None;
    if (key.rfind("dbs/", 0) != 0 || key.size() < 41 || !IsUUID(key.substr(4, 36)))
        return Kind::None;
    auto rest = key.substr(40);
    if (rest.rfind("/packs/", 0) == 0 && IsHash(rest.substr(7)))
        return Kind::Pack;
    if (rest.rfind("/indexes/", 0) == 0 && IsHash(rest.substr(9)))
        return Kind::Index;
    if (rest.rfind("/log/", 0) == 0) {
        // <branch name>/<branch uuid>/<inverted chain>/<inverted epoch>
        auto log = rest.substr(5);
        auto slash = log.find('/');
        if (slash == std::string::npos || slash == 0)
            return Kind::None;
        auto tail = log.substr(slash + 1);
        if (tail.size() == 36 + 1 + 10 + 1 + 10 && IsUUID(tail.substr(0, 36)) && tail[36] == '/' && tail[47] == '/' &&
            Digits(tail.substr(37, 10), 10) && Digits(tail.substr(48, 10), 10))
            return Kind::Log;
    }
    return Kind::None;
}
std::string HashOf(const std::string &key) { return key.substr(key.rfind('/') + 1); }
void ListAll(S3Provider &provider, const std::string &within,
             const std::function<void(const std::string &, uint64_t, int64_t)> &each) {
    std::string continuation;
    do {
        auto page = provider.List(continuation, 1000, within);
        for (size_t i = 0; i < page.keys.size(); i++)
            each(page.keys[i], page.sizes[i], page.modified[i]);
        continuation = page.continuation;
    } while (!continuation.empty());
}
std::vector<unsigned char> EncodeRecord(const std::vector<std::string> &keys) {
    std::string text = std::string(RECORD_FORMAT) + "\n";
    for (auto &key : keys)
        text += key + "\n";
    return {text.begin(), text.end()};
}
std::vector<std::string> DecodeRecord(const std::vector<unsigned char> &bytes) {
    std::istringstream in(std::string(bytes.begin(), bytes.end()));
    std::string line;
    if (!std::getline(in, line) || line != RECORD_FORMAT)
        throw std::runtime_error("[differential:incompatible_format] Unsupported remote collection record");
    std::vector<std::string> keys;
    while (std::getline(in, line)) {
        if (DataKind(line) == Kind::None)
            throw std::runtime_error("[differential:corrupt_data] Corrupt remote collection record");
        keys.push_back(line);
    }
    return keys;
}
// gc/condemned/<20-digit microseconds>-<sha256 of the record>.
bool RecordName(const std::string &key, std::string &hash) {
    auto name = key.substr(std::string(RECORDS).size());
    if (name.size() != 20 + 1 + 64 || name[20] != '-' || name.find_first_not_of("0123456789") != 20)
        return false;
    hash = name.substr(21);
    return IsHash(hash);
}
void RecordError(const std::string &error) {
    std::lock_guard<std::mutex> guard(error_mutex);
    last_error = error;
}
} // namespace

std::set<std::string> SyncCondemned(Control &control, const std::string &provider_id) {
    auto provider = S3Client(control, provider_id);
    std::set<std::string> learned;
    ListAll(*provider, RECORDS, [&](const std::string &key, uint64_t, int64_t modified) {
        std::string hash;
        if (!RecordName(key, hash) || control.KnownCondemnation(provider_id, key))
            return;
        auto keys = DecodeRecord(provider->ReadObject(key, RECORD_LIMIT, hash));
        control.RecordCondemnation(provider_id, key, modified, keys, false);
        learned.insert(keys.begin(), keys.end());
        // Local objects read only from S3 must not depend on a condemned pack: copy them first. On
        // failure the record is read again at the next synchronization.
        std::set<std::string> packs;
        for (auto &condemned : keys)
            if (DataKind(condemned) == Kind::Pack)
                packs.insert(condemned);
        auto objects = control.RemoteOnlyObjectsIn(provider_id, packs);
        try {
            if (!objects.empty()) {
                control.MaterializeRemoteObjects(FetchRemoteObjects(control, objects));
                rescued_objects += objects.size();
            }
        } catch (const std::exception &error) {
            ++rescue_failures;
            RecordError("copying " + std::to_string(objects.size()) +
                        " S3-only objects out of condemned packs failed: " + error.what());
            return;
        }
        control.RecordCondemnation(provider_id, key, modified, {}, true);
    });
    return learned;
}

RemoteCollection CollectRemote(Control &control, const std::string &provider_id, int64_t older_than, bool dry_run) {
    auto provider = S3Client(control, provider_id);
    auto width = provider->Concurrency();
    RemoteCollection result;
    SyncCondemned(control, provider_id);
    // Inventory first, roots after: an object uploaded meanwhile is not listed, and a root
    // published meanwhile is still marked.
    struct Object {
        uint64_t size;
        int64_t modified;
    };
    std::map<std::string, Object> data;
    std::vector<std::string> heads, commits;
    std::set<std::string> expired; // "<uuid>/<inverted number>"
    auto add = [&](const std::string &key, uint64_t size, int64_t modified) {
        if (DataKind(key) != Kind::None) {
            data[key] = {size, modified};
            return;
        }
        if (key.rfind("dbs/", 0) != 0 || key.size() < 41)
            return;
        auto rest = key.substr(40);
        auto record = key.substr(4, 36) + "/" + key.substr(key.rfind('/') + 1);
        if (rest.rfind("/branches/", 0) == 0)
            heads.push_back(key);
        else if (rest.rfind("/snapshots/", 0) == 0)
            commits.push_back(key);
        else if (rest.rfind("/expired/", 0) == 0)
            expired.insert(record);
    };
    ListAll(*provider, "descriptors/", add);
    ListAll(*provider, "dbs/", add);
    // References of immutable objects read before (and those learned now, stored before deleting).
    auto cache = control.LoadRemoteReferences(provider_id);
    Control::RemoteReferences learned;
    std::mutex mutex;
    auto pages_of = [&](const std::string &key) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            auto found = cache.descriptor_pages.find(key);
            if (found != cache.descriptor_pages.end())
                return found->second;
        }
        auto descriptor = DecodeDescriptor(provider->ReadObject(key, DESCRIPTOR_LIMIT, HashOf(key)));
        std::vector<std::pair<std::string, uint64_t>> pages;
        for (size_t page = 0; page < descriptor.pages.size(); page++)
            pages.emplace_back(descriptor.pool + "indexes/" + descriptor.pages[page], descriptor.page_sizes[page]);
        std::lock_guard<std::mutex> guard(mutex);
        cache.descriptor_pages[key] = learned.descriptor_pages[key] = pages;
        return pages;
    };
    auto packs_of = [&](const std::string &key, uint64_t size) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            auto found = cache.index_packs.find(key);
            if (found != cache.index_packs.end())
                return found->second;
        }
        auto packs = IndexPackKeys(provider->ReadObject(key, size, HashOf(key)));
        std::lock_guard<std::mutex> guard(mutex);
        cache.index_packs[key] = learned.index_packs[key] = packs;
        return packs;
    };
    // Roots: descriptors named by branch heads, unexpired commit records and retained local exports.
    std::set<std::string> roots;
    commits.erase(std::remove_if(commits.begin(), commits.end(),
                                 [&](const std::string &key) {
                                     return expired.count(key.substr(4, 36) + "/" + key.substr(key.rfind('/') + 1)) > 0;
                                 }),
                  commits.end());
    std::vector<std::string> records(heads);
    for (auto &key : commits) {
        auto found = cache.commit_descriptors.find(key);
        if (found != cache.commit_descriptors.end())
            roots.insert(found->second); // commit records never change
        else
            records.push_back(key);
    }
    ParallelTasks(records.size(), width, [&](size_t i) {
        auto object = provider->ReadMutable(records[i]);
        if (!object)
            return; // removed meanwhile: a head is never removed, so nothing it named is lost
        std::string descriptor;
        if (i < heads.size()) {
            auto head = DecodeHeadObject(object->first);
            if (!head)
                return; // a dropped branch
            descriptor = head->descriptor;
        } else {
            auto commit = DecodeCommitRecord(object->first);
            if (!commit)
                throw std::runtime_error("[differential:corrupt_data] Unreadable commit record " + records[i]);
            descriptor = commit->descriptor;
        }
        std::lock_guard<std::mutex> guard(mutex);
        roots.insert(descriptor);
        if (i >= heads.size())
            learned.commit_descriptors[records[i]] = descriptor;
    });
    for (auto &descriptor : control.LiveExportDescriptors(provider_id))
        roots.insert(descriptor);
    // Shipping (SHIP_WAL): the newest chain of every shipped branch incarnation is live, epochs and
    // base; superseded chains are collected like any other unreachable object.
    std::set<std::string> live_log;
    {
        std::map<std::string, std::string> newest; // "<db>/log/<name>/<uuid>/" -> newest chain directory
        for (auto &object : data) {
            if (DataKind(object.first) != Kind::Log)
                continue;
            auto chain_dir = object.first.substr(0, object.first.size() - 10);       // .../<inverted chain>/
            auto branch_dir = chain_dir.substr(0, chain_dir.size() - 11);            // .../<branch uuid>/
            auto &best = newest[branch_dir];
            if (best.empty() || chain_dir < best) // inverted numbers: the smallest is the newest chain
                best = chain_dir;
        }
        std::vector<std::string> chains;
        for (auto &entry : newest)
            chains.push_back(entry.second);
        for (auto &object : data)
            if (DataKind(object.first) == Kind::Log &&
                newest.count(object.first.substr(0, object.first.size() - 21)) &&
                newest.at(object.first.substr(0, object.first.size() - 21)) == object.first.substr(0, object.first.size() - 10))
                live_log.insert(object.first);
        // Each live chain's base: named in its first epoch ("base descriptors/<sha256>").
        ParallelTasks(chains.size(), width, [&](size_t i) {
            auto first = chains[i] + "9999999999";
            auto found = data.find(first);
            if (found == data.end())
                return;
            auto header = provider->ReadSpan(first, 0, std::min<uint64_t>(found->second.size, 4096));
            std::istringstream in(std::string(header.begin(), header.end()));
            std::string line;
            while (std::getline(in, line))
                if (line.rfind("base descriptors/", 0) == 0 && IsHash(line.substr(17))) {
                    std::lock_guard<std::mutex> guard(mutex);
                    roots.insert(line.substr(5));
                    break;
                }
        });
    }
    result.roots = int64_t(roots.size());
    // Mark: descriptors -> index pages -> packs.
    std::set<std::string> live(roots.begin(), roots.end());
    live.insert(live_log.begin(), live_log.end());
    std::map<std::string, uint64_t> indexes;
    std::vector<std::string> root_list(roots.begin(), roots.end());
    ParallelTasks(root_list.size(), width, [&](size_t i) {
        auto &key = root_list[i];
        if (!data.count(key))
            return; // never uploaded completely; nothing to keep
        auto pages = pages_of(key);
        std::lock_guard<std::mutex> guard(mutex);
        for (auto &page : pages)
            indexes[page.first] = page.second;
    });
    std::vector<std::pair<std::string, uint64_t>> index_list(indexes.begin(), indexes.end());
    ParallelTasks(index_list.size(), width, [&](size_t i) {
        auto &key = index_list[i].first;
        if (!data.count(key))
            return;
        auto packs = packs_of(key, index_list[i].second);
        std::lock_guard<std::mutex> guard(mutex);
        live.insert(key);
        live.insert(packs.begin(), packs.end());
    });
    for (auto &pack : control.RemoteOnlyPacks(provider_id))
        live.insert(pack);
    // Unreachable objects old enough to be outside every export still in progress.
    std::vector<std::string> unreachable;
    for (auto &object : data) {
        if (live.count(object.first)) {
            ++result.live_objects;
            continue;
        }
        if (object.second.modified >= older_than)
            continue;
        unreachable.push_back(object.first);
        result.unreachable_bytes += int64_t(object.second.size);
    }
    result.unreachable_objects = int64_t(unreachable.size());
    auto condemned = control.Condemnations(provider_id);
    auto graceful = [&](const std::string &key) {
        auto found = condemned.find(key);
        return found != condemned.end() && found->second < older_than && !live.count(key);
    };
    // Deletable: condemned before the cutoff and still unreachable. Index pages and descriptors only
    // when no export could produce them again (their packs are gone or going).
    std::set<std::string> deletable;
    std::vector<std::string> pages, descriptors, fresh;
    for (auto &key : unreachable) {
        if (!condemned.count(key))
            fresh.push_back(key);
        else if (graceful(key)) {
            auto kind = DataKind(key);
            if (kind == Kind::Pack || kind == Kind::Log)
                deletable.insert(key);
            else
                (kind == Kind::Index ? pages : descriptors).push_back(key);
        }
    }
    ParallelTasks(pages.size(), width, [&](size_t i) {
        auto packs = packs_of(pages[i], data.at(pages[i]).size);
        if (std::all_of(packs.begin(), packs.end(), graceful)) {
            std::lock_guard<std::mutex> guard(mutex);
            deletable.insert(pages[i]);
        }
    });
    ParallelTasks(descriptors.size(), width, [&](size_t i) {
        auto pages = pages_of(descriptors[i]);
        std::lock_guard<std::mutex> guard(mutex);
        for (auto &page : pages) {
            auto &key = page.first;
            if (deletable.count(key) || !data.count(key)) {
                deletable.insert(descriptors[i]);
                break;
            }
        }
    });
    result.condemned_objects = int64_t(fresh.size());
    for (auto &key : deletable)
        result.deleted_bytes += int64_t(data.at(key).size);
    result.deleted_objects = int64_t(deletable.size());
    control.StoreRemoteReferences(provider_id, learned); // before deletions forget their objects
    if (dry_run)
        return result;
    // Descriptors first, then index pages, then packs: a partial run never leaves a descriptor
    // whose pages are gone while it was still deletable on its own.
    for (auto kind : {Kind::Log, Kind::Descriptor, Kind::Index, Kind::Pack}) {
        std::vector<std::string> batch;
        for (auto &key : deletable)
            if (DataKind(key) == kind)
                batch.push_back(key);
        ParallelTasks(batch.size(), width, [&](size_t i) { provider->DeleteImmutable(batch[i]); });
        control.ForgetRemoteObjects(provider_id, batch);
        deleted_objects += batch.size();
    }
    deleted_bytes += uint64_t(result.deleted_bytes);
    // Condemn the newly unreachable objects; the records' S3 times start their grace periods.
    for (size_t i = 0; i < fresh.size(); i += RECORD_KEYS) {
        std::vector<std::string> keys(fresh.begin() + int64_t(i),
                                      fresh.begin() + int64_t(std::min(fresh.size(), i + RECORD_KEYS)));
        auto bytes = EncodeRecord(keys);
        auto now = std::to_string(Control::Now());
        UploadImmutable(control, *provider, RECORDS + std::string(20 - now.size(), '0') + now + "-" + Sha256(bytes),
                        bytes);
    }
    condemned_objects += fresh.size();
    SyncCondemned(control, provider_id);
    return result;
}
std::string LastRemoteCollectionError() {
    std::lock_guard<std::mutex> guard(error_mutex);
    return last_error;
}
std::vector<std::pair<std::string, uint64_t>> RemoteCollectionMetrics() {
    return {{"remote_gc_condemned_objects", condemned_objects.load()},
            {"remote_gc_deleted_objects", deleted_objects.load()},
            {"remote_gc_deleted_bytes", deleted_bytes.load()},
            {"remote_gc_rescued_objects", rescued_objects.load()},
            {"remote_gc_rescue_failures", rescue_failures.load()}};
}
} // namespace differential
