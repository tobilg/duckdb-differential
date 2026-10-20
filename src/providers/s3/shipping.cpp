// Continuous WAL shipping (differential_shipping.hpp): epochs, chains, the shipping worker,
// recovery and follower images.
#include "differential_shipping.hpp"
#include "differential_error.hpp"
#include "differential_metrics.hpp"
#include "differential_s3.hpp"
#include "differential_sql.hpp"
#include "differential_store.hpp"
#include "s3_internal.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

namespace differential {
namespace {
constexpr const char *EPOCH_FORMAT = "differential-epoch-v1";
constexpr int64_t KEY_MAX = 9999999999;
constexpr int64_t CHAIN_EPOCHS = 256;                    // a new chain after this many epochs ...
constexpr uint64_t CHAIN_BYTES = 64ull * 1024 * 1024;    // ... or this much epoch data
constexpr uint64_t EPOCH_LIMIT = 256ull * 1024 * 1024;   // one epoch's payload (else a new chain)
constexpr uint64_t HEADER_LIMIT = 16ull * 1024 * 1024;   // an epoch's text part
constexpr auto SYNC_TIMEOUT = std::chrono::minutes(5);
constexpr auto CLOSE_TIMEOUT = std::chrono::seconds(30);
std::atomic<uint64_t> epochs_shipped{0}, epoch_bytes{0}, chains_started{0}, ship_failures{0}, ship_rounds{0},
    epochs_followed{0};

std::string Sha256(const unsigned char *data, size_t n) {
    unsigned char digest[32];
    unsigned length = 0;
    if (EVP_Digest(data, n, digest, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("SHA256 failed");
    static const char *hex = "0123456789abcdef";
    std::string out;
    for (auto b : digest) {
        out += hex[b >> 4];
        out += hex[b & 15];
    }
    return out;
}
std::string Sha256(const std::vector<unsigned char> &bytes) { return Sha256(bytes.data(), bytes.size()); }
bool IsHash(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
bool IsUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
}
std::string Inverted(int64_t n) {
    auto text = std::to_string(KEY_MAX - n);
    return std::string(10 - text.size(), '0') + text;
}
int64_t FromInverted(const std::string &text) {
    if (text.size() != 10 || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("[differential:corrupt_data] Corrupt shipping key");
    return KEY_MAX - std::stoll(text);
}
std::string BranchPrefix(const std::string &database, const std::string &branch_name) {
    return "dbs/" + database + "/log/" + branch_name + "/";
}
std::string ChainPrefix(const std::string &database, const std::string &branch_name, const std::string &branch,
                        int64_t chain) {
    return BranchPrefix(database, branch_name) + branch + "/" + Inverted(chain) + "/";
}
std::string EpochKey(const std::string &database, const std::string &branch_name, const std::string &branch,
                     int64_t chain, int64_t epoch) {
    return ChainPrefix(database, branch_name, branch, chain) + Inverted(epoch);
}

// --- Epoch encoding -------------------------------------------------------------------------------
std::vector<unsigned char> Encode(const Epoch &e) {
    std::ostringstream out;
    out << EPOCH_FORMAT << '\n'
        << "database " << e.database << '\n'
        << "branch " << e.branch << '\n'
        << "branch_name " << e.branch_name << '\n'
        << "chain " << e.chain << '\n'
        << "epoch " << e.epoch << '\n'
        << "previous " << (e.previous.empty() ? "-" : e.previous) << '\n'
        << "base " << e.base << '\n'
        << "sequence " << e.sequence << '\n'
        << "published_at " << e.published_at << '\n'
        << "next " << (e.next_chain < 0 ? std::string("-") : std::to_string(e.next_chain)) << '\n'
        << "files " << e.files.size() << '\n';
    for (auto &f : e.files) {
        out << "file " << (f.suffix.empty() ? "-" : f.suffix) << ' ' << (f.full ? "full" : "delta") << ' '
            << (f.log ? "log" : "chunked") << ' ' << f.size << ' ' << (f.log ? 1 : f.chunks.size()) << '\n';
        if (f.log)
            out << "t " << f.tail_offset << ' ' << f.tail_length << ' ' << f.tail_payload << ' '
                << (f.tail_hash.empty() ? "-" : f.tail_hash) << '\n';
        else
            for (auto &c : f.chunks)
                out << "c " << c.index << ' ' << (c.hash.empty() ? "-" : c.hash) << ' ' << c.payload << '\n';
    }
    out << "removed " << e.removed.size() << '\n';
    for (auto &suffix : e.removed)
        out << suffix << '\n';
    out << "payload " << e.payload.size() << '\n';
    auto text = out.str();
    std::vector<unsigned char> bytes(text.begin(), text.end());
    bytes.insert(bytes.end(), e.payload.begin(), e.payload.end());
    return bytes;
}
Epoch Decode(const std::vector<unsigned char> &bytes) {
    auto fail = [] { throw std::runtime_error("[differential:corrupt_data] Corrupt shipping epoch"); };
    size_t position = 0;
    auto line = [&]() {
        auto end = std::find(bytes.begin() + int64_t(position), bytes.end(), '\n');
        if (end == bytes.end() || size_t(end - bytes.begin()) - position > HEADER_LIMIT)
            fail();
        std::string text(bytes.begin() + int64_t(position), end);
        position = size_t(end - bytes.begin()) + 1;
        return text;
    };
    auto words = [&](size_t count) {
        std::istringstream in(line());
        std::vector<std::string> out;
        std::string word;
        while (in >> word)
            out.push_back(word);
        if (out.size() != count)
            fail();
        return out;
    };
    auto number = [&](const std::string &text) -> uint64_t {
        if (text.empty() || text.size() > 19 || text.find_first_not_of("0123456789") != std::string::npos)
            fail();
        return std::stoull(text);
    };
    auto field = [&](const char *name) {
        auto w = words(2);
        if (w[0] != name)
            fail();
        return w[1];
    };
    if (line() != EPOCH_FORMAT)
        throw std::runtime_error("[differential:incompatible_format] Unsupported shipping epoch format");
    Epoch e;
    e.database = field("database");
    e.branch = field("branch");
    e.branch_name = field("branch_name");
    e.chain = int64_t(number(field("chain")));
    e.epoch = int64_t(number(field("epoch")));
    e.previous = field("previous");
    if (e.previous == "-")
        e.previous.clear();
    e.base = field("base");
    e.sequence = number(field("sequence"));
    e.published_at = int64_t(number(field("published_at")));
    auto next = field("next");
    e.next_chain = next == "-" ? -1 : int64_t(number(next));
    if (!IsUUID(e.database) || !IsUUID(e.branch) || (!e.previous.empty() && !IsHash(e.previous)) ||
        e.base.rfind("descriptors/", 0) != 0 || !IsHash(e.base.substr(12)) || (e.epoch == 0) != e.previous.empty())
        fail();
    auto files = number(field("files"));
    for (uint64_t i = 0; i < files; i++) {
        auto w = words(6);
        if (w[0] != "file" || (w[2] != "full" && w[2] != "delta") || (w[3] != "log" && w[3] != "chunked"))
            fail();
        Epoch::File f;
        f.suffix = w[1] == "-" ? "" : w[1];
        if (!f.suffix.empty() && (f.suffix[0] != '.' || f.suffix.find_first_of("/ ") != std::string::npos))
            fail();
        f.full = w[2] == "full";
        f.log = w[3] == "log";
        f.size = number(w[4]);
        auto items = number(w[5]);
        if (f.log) {
            if (items != 1)
                fail();
            auto t = words(5);
            if (t[0] != "t")
                fail();
            f.tail_offset = number(t[1]);
            f.tail_length = number(t[2]);
            f.tail_payload = number(t[3]);
            f.tail_hash = t[4] == "-" ? "" : t[4];
            if (f.tail_offset + f.tail_length != f.size || (f.tail_length && !IsHash(f.tail_hash)))
                fail();
        } else
            for (uint64_t k = 0; k < items; k++) {
                auto c = words(4);
                if (c[0] != "c")
                    fail();
                Epoch::File::Chunk chunk;
                chunk.index = number(c[1]);
                chunk.hash = c[2] == "-" ? "" : c[2];
                chunk.payload = number(c[3]);
                if ((!chunk.hash.empty() && !IsHash(chunk.hash)) || chunk.index >= Store::ChunkCount(f.size))
                    fail();
                f.chunks.push_back(chunk);
            }
        e.files.push_back(std::move(f));
    }
    auto removed = number(field("removed"));
    for (uint64_t i = 0; i < removed; i++) {
        auto suffix = line();
        if (suffix.empty() || suffix[0] != '.')
            fail();
        e.removed.push_back(suffix);
    }
    auto length = number(field("payload"));
    if (bytes.size() - position != length)
        fail();
    e.payload.assign(bytes.begin() + int64_t(position), bytes.end());
    // Every inline item lies inside the payload and matches its identity.
    for (auto &f : e.files) {
        if (f.log) {
            if (f.tail_payload + f.tail_length > e.payload.size() ||
                (f.tail_length && Sha256(e.payload.data() + f.tail_payload, f.tail_length) != f.tail_hash))
                fail();
            continue;
        }
        for (auto &c : f.chunks)
            if (!c.hash.empty() && (c.payload + Store::CHUNK > e.payload.size() ||
                                    Sha256(e.payload.data() + c.payload, Store::CHUNK) != c.hash))
                fail();
    }
    e.hash = Sha256(bytes);
    return e;
}

// --- Building an epoch from the store ---------------------------------------------------------------
struct ChainFull : std::runtime_error {
    ChainFull() : std::runtime_error("[differential:busy] Shipping epoch too large; a new chain starts") {}
};
struct Builder : Store::ShipSink {
    std::string group;
    uint64_t limit = EPOCH_LIMIT;
    Epoch epoch;
    std::vector<Store::ShipFile> files;
    std::map<std::string, size_t> index; // file name -> position in epoch.files
    void Reset() override {
        epoch.files.clear();
        epoch.removed.clear();
        epoch.payload.clear();
        files.clear();
        index.clear();
    }
    std::string Suffix(const std::string &name) const { return name.substr(group.size()); }
    void Group(const std::vector<Store::ShipFile> &group_files) override { files = group_files; }
    void File(const Store::ShipFile &file, bool full) override {
        Epoch::File f;
        f.suffix = Suffix(file.name);
        f.full = full;
        f.log = file.log;
        f.size = file.size;
        index[file.name] = epoch.files.size();
        epoch.files.push_back(std::move(f));
    }
    void Append(const unsigned char *data, size_t n) {
        if (epoch.payload.size() + n > limit)
            throw ChainFull();
        epoch.payload.insert(epoch.payload.end(), data, data + n);
    }
    void Chunk(const Store::ShipFile &file, uint64_t chunk, const std::string &hash, const SharedBytes &bytes) override {
        auto &f = epoch.files[index.at(file.name)];
        Epoch::File::Chunk c;
        c.index = chunk;
        c.hash = hash;
        if (bytes) {
            c.payload = epoch.payload.size();
            Append(bytes->data(), bytes->size());
        }
        f.chunks.push_back(c);
    }
    void Tail(const Store::ShipFile &file, uint64_t offset, const std::vector<unsigned char> &bytes) override {
        auto &f = epoch.files[index.at(file.name)];
        f.tail_offset = offset;
        f.tail_length = bytes.size();
        f.tail_payload = epoch.payload.size();
        f.tail_hash = bytes.empty() ? "" : Sha256(bytes);
        Append(bytes.data(), bytes.size());
    }
};
std::string EncodeKnown(const std::map<std::string, int64_t> &known) {
    std::string text;
    for (auto &entry : known)
        text += entry.first + "\t" + std::to_string(entry.second) + "\n";
    return text;
}
std::map<std::string, int64_t> DecodeKnown(const std::string &text) {
    std::map<std::string, int64_t> known;
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos)
            throw std::runtime_error("[differential:corrupt_data] Corrupt shipping state");
        known[line.substr(0, tab)] = std::stoll(line.substr(tab + 1));
    }
    return known;
}

// --- Local chain state (metadata store) -----------------------------------------------------------
struct ChainState {
    bool exists = false;
    int64_t chain = 0, epoch = -1;
    std::string hash, base;
    std::map<std::string, int64_t> known;
    uint64_t sequence = 0, bytes = 0;
    int64_t shipped_at = 0;
    std::string pending; // an epoch being uploaded: "<key>\n<hash>\n<state line>" (see Pending)
};
ChainState LoadState(Control &control, const std::string &branch) {
    ChainState state;
    auto row = control.ShipChainOf(branch);
    if (row) {
        state.exists = true;
        state.chain = row->chain;
        state.epoch = row->epoch;
        state.hash = row->hash;
        state.base = row->base;
        state.known = DecodeKnown(row->known);
        state.sequence = uint64_t(row->sequence);
        state.bytes = uint64_t(row->bytes);
        state.shipped_at = row->shipped_at;
        state.pending = row->pending;
    }
    return state;
}
void SaveState(Control &control, const std::string &database, const std::string &branch, const ChainState &state) {
    Control::ShipChainRow row;
    row.chain = state.chain;
    row.epoch = state.epoch;
    row.hash = state.hash;
    row.base = state.base;
    row.known = EncodeKnown(state.known);
    row.sequence = int64_t(state.sequence);
    row.bytes = int64_t(state.bytes);
    row.shipped_at = state.shipped_at;
    row.pending = state.pending;
    control.SetShipChainOf(database, branch, row);
}
// The state after an epoch, kept as an intent while its upload is in flight: a crash after the
// upload finds the epoch in S3 (same hash: the intent becomes the state) or not (it is discarded).
std::string EncodePending(const std::string &key, const ChainState &next) {
    std::ostringstream out;
    out << key << '\n'
        << next.hash << '\n'
        << next.chain << ' ' << next.epoch << ' ' << next.sequence << ' ' << next.bytes << ' ' << next.shipped_at << '\n'
        << next.base << '\n'
        << EncodeKnown(next.known);
    return out.str();
}
bool DecodePending(const std::string &text, std::string &key, ChainState &next) {
    std::istringstream in(text);
    std::string numbers;
    if (!std::getline(in, key) || !std::getline(in, next.hash) || !std::getline(in, numbers) ||
        !std::getline(in, next.base))
        return false;
    std::istringstream n(numbers);
    if (!(n >> next.chain >> next.epoch >> next.sequence >> next.bytes >> next.shipped_at))
        return false;
    std::string rest((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    next.known = DecodeKnown(rest);
    next.exists = true;
    return true;
}

// Upload one epoch, create-only: identical content already there is success, different content is
// another writer with this branch's identity.
void PutEpoch(Control &control, S3Provider &provider, const std::string &key, const std::vector<unsigned char> &bytes,
              const std::string &hash) {
    try {
        if (bytes.size() < provider.Config().multipart_threshold)
            provider.PutBytes(key, bytes, hash);
        else {
            // A large epoch (bulk changes): a multipart upload from a temporary file in the repository.
            unsigned char random[8];
            if (RAND_bytes(random, sizeof(random)) != 1)
                throw std::runtime_error("Cannot name a shipping staging file");
            std::string name = "ship-";
            for (auto b : random)
                name += "0123456789abcdef"[b >> 4], name += "0123456789abcdef"[b & 15];
            auto folder = std::filesystem::path(control.store->directory) / "staging";
            control.store->permissions.CreateDirectories(folder);
            auto path = (folder / name).string();
            int fd = control.store->permissions.Open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC);
            if (fd < 0)
                throw std::runtime_error("[differential:local_io] Cannot stage a shipping epoch");
            struct Cleanup {
                std::string path;
                ~Cleanup() { unlink(path.c_str()); }
            } cleanup {path};
            size_t done = 0;
            while (done < bytes.size()) {
                auto n = write(fd, bytes.data() + done, bytes.size() - done);
                if (n < 0 && errno == EINTR)
                    continue;
                if (n <= 0) {
                    close(fd);
                    throw std::runtime_error("[differential:local_io] Cannot stage a shipping epoch");
                }
                done += size_t(n);
            }
            close(fd);
            provider.PutFile(key, path, hash);
        }
    } catch (const std::exception &error) {
        if (!Is(error, ErrorCode::ObjectConflict))
            throw;
        throw Error(ErrorCode::HeadConflict,
                    "[differential:head_conflict] Shipping epoch " + key + " was already written by another writer, "
                    "for example a copy of this repository on another host. Shipping stopped for this branch; if this "
                    "repository is the only writer, run differential_claim_branch_heads and attach again.");
    }
    ++epochs_shipped;
    epoch_bytes += bytes.size();
}

// --- The shipping worker ----------------------------------------------------------------------------
struct Shipped {
    std::weak_ptr<Control> control;
    std::string database, branch, branch_name, group, provider;
    ShipConfig config;
    bool dirty = false, urgent = false, active = true, rotate = false;
    bool busy = false; // a round is running
    int failures = 0;
    std::chrono::steady_clock::time_point due = std::chrono::steady_clock::now();
    uint64_t shipped_sequence = 0;
    std::string error;
    uint64_t error_round = 0, round = 0;
};
// Never destroyed: the worker may still run while static objects are destroyed at exit (it is stopped
// and joined by Joiner first).
struct Worker {
    std::mutex mutex;
    std::condition_variable signal, shipped_signal;
    std::map<std::string, std::shared_ptr<Shipped>> shipped; // by repository directory and branch
    std::thread thread;
    bool running = false;
};
Worker &W() {
    static auto *worker = new Worker();
    return *worker;
}
#define worker_mutex W().mutex
#define worker_signal W().signal
#define shipped_signal W().shipped_signal
#define shipped W().shipped
#define worker W().thread
#define worker_running W().running
std::string EntryKey(Control &control, const std::string &branch) { return control.store->directory + "/" + branch; }

// One shipping round of a branch: finish an interrupted upload, then ship its changes as the next
// epoch, or start a new chain. Returns the shipping sequence now shipped.
uint64_t Round(Control &control, Shipped &entry) {
    ++ship_rounds;
    auto provider = S3Client(control, entry.provider);
    auto state = LoadState(control, entry.branch);
    if (!state.pending.empty()) {
        std::string key;
        ChainState next;
        if (!DecodePending(state.pending, key, next))
            throw std::runtime_error("[differential:corrupt_data] Corrupt shipping intent");
        auto stat = provider->TryHead(key);
        state.pending.clear();
        if (stat && stat->sha256 == next.hash) {
            state = next; // uploaded before a crash
            control.store->AckShipped(entry.group, state.sequence);
        } else if (stat)
            throw Error(ErrorCode::HeadConflict, "[differential:head_conflict] Shipping epoch " + key +
                                                     " was written by another writer");
        SaveState(control, entry.database, entry.branch, state);
    }
    auto branch = control.GetBranch(entry.database, entry.branch, true);
    auto main = branch.FileName();
    if (!control.store->Exists(main))
        return state.sequence; // DuckDB has not created the database file yet
    bool rotate;
    {
        std::lock_guard<std::mutex> lock(worker_mutex);
        rotate = entry.rotate;
        entry.rotate = false;
    }
    // A chain ends at a completed checkpoint (the WAL is short then), or when it grew long.
    bool new_chain = !state.exists || rotate || state.epoch + 1 >= CHAIN_EPOCHS || state.bytes >= CHAIN_BYTES;
    for (int attempt = 0; attempt < 2; attempt++) {
        ChainState next = state;
        Epoch epoch;
        Builder builder;
        builder.group = main;
        std::shared_ptr<Pin> pin;
        int64_t redirect_chain = -1, redirect_epoch = -1;
        std::string redirect_previous;
        if (new_chain) {
            // A new chain above every chain in S3 (another repository may have shipped this branch
            // before, e.g. before a failover), based on the main file as published now.
            auto newest = NewestEpoch(*provider, entry.database, entry.branch_name, entry.branch);
            next.chain = std::max(state.exists ? state.chain : 0, newest ? newest->chain : 0) + 1;
            if (newest) {
                redirect_chain = newest->chain;
                redirect_epoch = newest->epoch + 1;
                auto stat = provider->TryHead(EpochKey(entry.database, entry.branch_name, entry.branch,
                                                       newest->chain, newest->epoch));
                redirect_previous = stat ? stat->sha256 : "";
            }
            pin = control.NewPin();
            uint64_t captured = 0;
            auto root = control.store->Capture(control.store->FileId(main), pin->id, captured);
            next.base = ExportBase(control, entry.provider, branch, root, [] {});
            next.epoch = 0;
            next.hash.clear();
            next.bytes = 0;
            next.known = {{main, control.store->FileId(main)}};
            ++chains_started;
        } else
            next.epoch = state.epoch + 1;
        try {
            next.sequence = control.store->ReadShipDelta(main, next.known, builder);
        } catch (const ChainFull &) {
            if (new_chain)
                throw std::runtime_error("[differential:busy] A shipping epoch exceeds 256 MiB; checkpoint the "
                                         "database and retry");
            new_chain = true;
            continue;
        }
        // Removed files, and what the next epoch knows.
        std::map<std::string, int64_t> known;
        for (auto &file : builder.files)
            known[file.name] = file.id;
        for (auto &entry_known : next.known)
            if (!known.count(entry_known.first))
                builder.epoch.removed.push_back(entry_known.first.substr(main.size()));
        if (!new_chain && builder.epoch.files.empty() && builder.epoch.removed.empty()) {
            // Nothing to ship: the changes (if any) were of unnamed files.
            control.store->AckShipped(main, next.sequence);
            return next.sequence;
        }
        epoch = std::move(builder.epoch);
        epoch.database = entry.database;
        epoch.branch = entry.branch;
        epoch.branch_name = entry.branch_name;
        epoch.chain = next.chain;
        epoch.epoch = next.epoch;
        epoch.previous = new_chain ? "" : state.hash;
        epoch.base = next.base;
        epoch.sequence = next.sequence;
        epoch.published_at = control.Clock();
        auto bytes = Encode(epoch);
        auto hash = Sha256(bytes);
        auto key = EpochKey(entry.database, entry.branch_name, entry.branch, next.chain, next.epoch);
        next.hash = hash;
        next.known = known;
        next.bytes += bytes.size();
        next.shipped_at = epoch.published_at;
        next.exists = true;
        // Intent, upload, state: see EncodePending.
        ChainState intent = state;
        intent.pending = EncodePending(key, next);
        SaveState(control, entry.database, entry.branch, intent);
        PutEpoch(control, *provider, key, bytes, hash);
        next.pending.clear();
        SaveState(control, entry.database, entry.branch, next);
        control.store->AckShipped(main, next.sequence);
        // The superseded chain ends with a redirect to its successor, so followers move on without a
        // listing (best effort: recovery lists the newest chain anyway).
        if (redirect_chain >= 0 && !redirect_previous.empty()) {
            try {
                Epoch redirect;
                redirect.database = entry.database;
                redirect.branch = entry.branch;
                redirect.branch_name = entry.branch_name;
                redirect.chain = redirect_chain;
                redirect.epoch = redirect_epoch;
                redirect.previous = redirect_previous;
                redirect.base = next.base;
                redirect.sequence = next.sequence;
                redirect.published_at = epoch.published_at;
                redirect.next_chain = next.chain;
                auto encoded = Encode(redirect);
                PutEpoch(control, *provider,
                         EpochKey(entry.database, entry.branch_name, entry.branch, redirect_chain, redirect_epoch),
                         encoded, Sha256(encoded));
            } catch (const std::exception &) {
            }
        }
        return next.sequence;
    }
    throw std::runtime_error("[differential:busy] Shipping could not start a new chain");
}
void Work() {
    std::unique_lock<std::mutex> lock(worker_mutex);
    while (worker_running) {
        auto now = std::chrono::steady_clock::now();
        std::shared_ptr<Shipped> next;
        auto wake = now + std::chrono::seconds(60);
        for (auto &item : shipped) {
            auto &entry = *item.second;
            if (!entry.active || !entry.dirty)
                continue;
            auto due = entry.urgent ? now : entry.due;
            if (due <= now && !next)
                next = item.second;
            wake = std::min(wake, due);
        }
        if (!next) {
            worker_signal.wait_until(lock, wake);
            continue;
        }
        auto control = next->control.lock();
        next->dirty = false;
        next->urgent = false;
        next->busy = true;
        auto round = ++next->round;
        lock.unlock();
        uint64_t sequence = 0;
        std::string error;
        try {
            if (!control)
                throw std::runtime_error("repository closed");
            sequence = Round(*control, *next);
        } catch (const std::exception &failure) {
            error = failure.what();
            ++ship_failures;
        }
        lock.lock();
        next->busy = false;
        if (error.empty()) {
            next->shipped_sequence = std::max(next->shipped_sequence, sequence);
            next->error.clear();
            next->failures = 0;
            next->due = std::chrono::steady_clock::now() + std::chrono::milliseconds(next->config.interval_ms);
        } else {
            next->error = error;
            next->error_round = round;
            next->dirty = true; // retried with backoff (up to a minute)
            next->failures = std::min(next->failures + 1, 8);
            auto backoff = std::min<int64_t>(60000, std::max<int64_t>(next->config.interval_ms, 250) << next->failures);
            next->due = std::chrono::steady_clock::now() + std::chrono::milliseconds(backoff);
            if (error.find("[differential:head_conflict]") != std::string::npos)
                next->active = false; // a split brain: stop until the next ATTACH
        }
        shipped_signal.notify_all();
    }
}
std::shared_ptr<Shipped> Find(Control &control, const std::string &branch) {
    auto found = shipped.find(EntryKey(control, branch));
    return found == shipped.end() ? nullptr : found->second;
}
// Wait until the branch's shipped sequence reaches `target`; throws the shipping error of a round
// that started after the wait began.
void Await(Control &control, const std::string &branch, uint64_t target, std::chrono::steady_clock::duration timeout,
           bool raise) {
    std::unique_lock<std::mutex> lock(worker_mutex);
    auto entry = Find(control, branch);
    if (!entry)
        return;
    auto start_round = entry->round;
    entry->dirty = entry->urgent = true;
    worker_signal.notify_all();
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (entry->shipped_sequence < target) {
        if (!entry->error.empty() && entry->error_round > start_round) {
            if (raise)
                throw std::runtime_error("[differential:ship_failed] The commit is durable locally but could not be "
                                         "shipped to S3: " + entry->error);
            return;
        }
        if (!entry->active || shipped_signal.wait_until(lock, deadline) == std::cv_status::timeout) {
            if (raise)
                throw std::runtime_error("[differential:ship_failed] The commit is durable locally but was not "
                                         "shipped to S3 in time" + (entry->error.empty() ? "" : ": " + entry->error));
            return;
        }
    }
}
} // namespace

ShipConfig GetShipConfig(Control &control, const std::string &database) {
    ShipConfig config;
    if (auto row = control.ShipConfigOf(database)) {
        config.mode = row->first;
        config.interval_ms = row->second;
    }
    return config;
}
void SetShipConfig(Control &control, const std::string &database, const ShipConfig &config) {
    if (config.mode != "off" && config.mode != "async" && config.mode != "sync")
        throw std::runtime_error("SHIP_WAL must be 'off', 'async' or 'sync'");
    if (config.interval_ms < 0 || config.interval_ms > 3600 * 1000)
        throw std::runtime_error("SHIP_INTERVAL must be between 0 and 3600 seconds");
    if (config.mode != "off") {
        if (control.Writers(database) == "shared")
            throw std::runtime_error("SHIP_WAL applies to single-writer databases; shared databases publish "
                                     "through S3 already");
        if (control.DataPath(database).rfind("s3://", 0) != 0)
            throw std::runtime_error("SHIP_WAL needs an S3 DATA_PATH ('s3://bucket/prefix/')");
    }
    control.SetShipConfigOf(database, config.mode, config.interval_ms);
    if (config.mode == "off") {
        for (auto &branch : control.Branches(database))
            control.store->SetShipping(branch.FileName(), false);
        std::lock_guard<std::mutex> lock(worker_mutex);
        for (auto &branch : control.Branches(database)) {
            auto entry = Find(control, branch.id);
            if (entry)
                entry->active = false;
        }
        shipped_signal.notify_all();
    } else {
        // Attached writers of the database pick the new settings up.
        std::lock_guard<std::mutex> lock(worker_mutex);
        for (auto &branch : control.Branches(database))
            if (auto entry = Find(control, branch.id))
                entry->config = config;
    }
}
void StartShipping(const std::shared_ptr<Control> &control, const Branch &branch) {
    auto config = GetShipConfig(*control, branch.database);
    if (config.mode == "off")
        return;
    control->store->SetShipping(branch.FileName(), true);
    auto entry = std::make_shared<Shipped>();
    entry->control = control;
    entry->database = branch.database;
    entry->branch = branch.id;
    entry->branch_name = branch.name;
    entry->group = branch.FileName();
    entry->provider = control->Profile(branch.database);
    entry->config = config;
    entry->shipped_sequence = LoadState(*control, branch.id).sequence;
    // The S3 client (with the AWS SDK and OpenSSL) exists before the worker's exit handler is
    // registered, so at process exit the worker stops before they are torn down.
    S3Client(*control, entry->provider);
    std::lock_guard<std::mutex> lock(worker_mutex);
    shipped[EntryKey(*control, branch.id)] = entry;
    if (!worker_running) {
        if (worker.joinable())
            worker.join();
        worker_running = true;
        worker = std::thread(Work);
        // The worker outlives no process: joined at exit.
        static struct Joiner {
            ~Joiner() {
                {
                    std::lock_guard<std::mutex> guard(worker_mutex);
                    worker_running = false;
                }
                worker_signal.notify_all();
                if (worker.joinable())
                    worker.join();
            }
        } joiner;
    }
    worker_signal.notify_all();
}
void AfterSync(Control &control, const std::string &branch, bool checkpoint) {
    std::shared_ptr<Shipped> entry;
    {
        std::lock_guard<std::mutex> lock(worker_mutex);
        entry = Find(control, branch);
        if (!entry || !entry->active)
            return;
        entry->dirty = true;
        entry->rotate = entry->rotate || checkpoint;
        if (entry->config.mode != "sync") {
            worker_signal.notify_all();
            return;
        }
    }
    Await(control, branch, control.store->ShipSequence(), SYNC_TIMEOUT, true);
}
void ShipOnClose(Control &control, const std::string &branch) {
    {
        std::lock_guard<std::mutex> lock(worker_mutex);
        auto entry = Find(control, branch);
        if (!entry || !entry->active)
            return;
    }
    Await(control, branch, control.store->ShipSequence(), CLOSE_TIMEOUT, false);
    // No round of this branch may outlive the attachment (or the process's S3 runtime at exit).
    std::unique_lock<std::mutex> lock(worker_mutex);
    auto entry = Find(control, branch);
    if (entry) {
        entry->active = false;
        shipped_signal.wait_for(lock, CLOSE_TIMEOUT, [&] { return !entry->busy; });
    }
    shipped.erase(EntryKey(control, branch));
}
ShipStatus GetShipStatus(Control &control, const std::string &database, const std::string &branch) {
    ShipStatus status;
    status.mode = GetShipConfig(control, database).mode;
    auto state = LoadState(control, branch);
    if (state.exists) {
        status.chain = state.chain;
        status.epoch = state.epoch;
        status.shipped_sequence = state.sequence;
        status.shipped_at = state.shipped_at;
    }
    std::lock_guard<std::mutex> lock(worker_mutex);
    if (auto entry = Find(control, branch)) {
        status.error = entry->error;
        status.pending_sequence = entry->dirty ? control.store->ShipSequence() : status.shipped_sequence;
    }
    return status;
}
std::vector<std::pair<std::string, uint64_t>> ShippingMetrics() {
    return {{"ship_epochs", epochs_shipped.load()},       {"ship_epoch_bytes", epoch_bytes.load()},
            {"ship_chains_started", chains_started.load()}, {"ship_failures", ship_failures.load()},
            {"ship_rounds", ship_rounds.load()},           {"follow_epochs_applied", epochs_followed.load()}};
}

// --- Reading chains -------------------------------------------------------------------------------
std::optional<ChainPosition> NewestEpoch(S3Provider &provider, const std::string &database,
                                         const std::string &branch_name, const std::string &branch) {
    auto within = BranchPrefix(database, branch_name) + (branch.empty() ? "" : branch + "/");
    // Keys are <branch uuid>/<inverted chain>/<inverted epoch>: the first key of each incarnation is
    // its newest epoch.
    std::map<std::string, ChainPosition> newest;
    std::string continuation;
    do {
        auto page = provider.List(continuation, branch.empty() ? 1000 : 1, within);
        for (auto &key : page.keys) {
            auto rest = key.substr(BranchPrefix(database, branch_name).size());
            if (rest.size() != 36 + 1 + 10 + 1 + 10 || rest[36] != '/' || rest[47] != '/')
                continue;
            auto id = rest.substr(0, 36);
            if (!IsUUID(id) || newest.count(id))
                continue;
            newest[id] = {id, FromInverted(rest.substr(37, 10)), FromInverted(rest.substr(48, 10))};
        }
        continuation = branch.empty() ? page.continuation : "";
    } while (!continuation.empty());
    if (newest.empty())
        return std::nullopt;
    if (newest.size() == 1)
        return newest.begin()->second;
    // Several incarnations of the name (a dropped and recreated branch): the most recently shipped.
    std::optional<ChainPosition> best;
    int64_t best_time = -1;
    for (auto &item : newest) {
        auto epoch = ReadEpoch(provider, database, branch_name, item.first, item.second.chain, item.second.epoch);
        if (epoch && epoch->published_at > best_time) {
            best_time = epoch->published_at;
            best = item.second;
        }
    }
    return best;
}
std::optional<ChainPosition> NewestEpoch(Control &control, const std::string &provider, const std::string &database,
                                         const std::string &branch_name) {
    return NewestEpoch(*S3Client(control, provider), database, branch_name);
}
BranchLocation ResolveBranchLocation(Control &control, const std::string &provider_id, const std::string &path) {
    auto valid_name = [](const std::string &s) {
        return !s.empty() && s.size() <= 128 &&
               s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") ==
                   std::string::npos;
    };
    std::vector<std::string> parts;
    std::stringstream in(path);
    std::string part;
    while (std::getline(in, part, '/'))
        parts.push_back(part);
    if (parts.empty() || parts.size() > 3 || !valid_name(parts[0]) ||
        (parts.size() == 3 && (!IsUUID(parts[1]) || !valid_name(parts[2]))) ||
        (parts.size() == 2 && !valid_name(parts[1])))
        throw std::runtime_error("Expected heads/<database>[/<database uuid>][/<branch>]");
    BranchLocation location;
    location.database_name = parts[0];
    location.branch_name = parts.size() == 1 ? std::string("main") : parts.back();
    location.database = parts.size() == 3 ? parts[1] : "";
    if (!location.database.empty())
        return location;
    // The names index lists every database UUID published (exported or shipped) under this name;
    // only those with this branch (shipped or exported) count.
    auto provider = S3Client(control, provider_id);
    std::vector<std::string> found;
    std::string continuation;
    auto within = "names/" + location.database_name + "/";
    do {
        auto page = provider->List(continuation, 1000, within);
        for (auto &key : page.keys) {
            auto id = key.substr(within.size());
            if (!IsUUID(id))
                continue;
            if (provider->List("", 1, BranchPrefix(id, location.branch_name)).keys.empty() &&
                !provider->TryHead("dbs/" + id + "/branches/" + location.branch_name))
                continue;
            found.push_back(id);
        }
        continuation = page.continuation;
    } while (!continuation.empty());
    if (found.empty())
        throw std::runtime_error("[differential:not_found] No shipped or exported branch " + location.database_name +
                                 "/" + location.branch_name);
    if (found.size() > 1)
        throw std::runtime_error("Several databases named " + location.database_name +
                                 " publish to this location; use heads/" + location.database_name +
                                 "/<database uuid>/" + location.branch_name);
    location.database = found.front();
    return location;
}
std::string BranchHeadVersion(Control &control, const std::string &provider_id, const BranchLocation &location) {
    auto stat = S3Client(control, provider_id)->TryHead("dbs/" + location.database + "/branches/" + location.branch_name);
    return stat ? stat->etag : "";
}
std::optional<Epoch> ReadEpoch(S3Provider &provider, const std::string &database, const std::string &branch_name,
                               const std::string &branch, int64_t chain, int64_t epoch) {
    auto key = EpochKey(database, branch_name, branch, chain, epoch);
    auto stat = provider.TryHead(key);
    if (!stat)
        return std::nullopt;
    if (stat->size > EPOCH_LIMIT + HEADER_LIMIT || !IsHash(stat->sha256))
        throw std::runtime_error("[differential:corrupt_data] Corrupt shipping epoch " + key);
    std::vector<unsigned char> bytes;
    bytes.reserve(stat->size);
    auto step = std::max<uint64_t>(1, provider.Config().max_range_bytes);
    for (uint64_t offset = 0; offset < stat->size; offset += step) {
        auto part = provider.ReadSpan(key, offset, std::min(step, stat->size - offset));
        bytes.insert(bytes.end(), part.begin(), part.end());
    }
    if (Sha256(bytes) != stat->sha256)
        throw std::runtime_error("[differential:corrupt_data] Corrupt shipping epoch " + key);
    auto decoded = Decode(bytes);
    if (decoded.database != database || decoded.branch != branch || decoded.branch_name != branch_name ||
        decoded.chain != chain || decoded.epoch != epoch)
        throw std::runtime_error("[differential:corrupt_data] Shipping epoch does not match its key " + key);
    return decoded;
}
namespace {
// The verified, consecutive epochs of a chain from `from` on (stops at the first missing one).
std::vector<Epoch> ReadChain(S3Provider &provider, const std::string &database, const std::string &branch_name,
                             const std::string &branch, int64_t chain, int64_t from, const std::string &previous) {
    std::vector<Epoch> epochs;
    auto expected = previous;
    for (int64_t number = from;; number++) {
        auto epoch = ReadEpoch(provider, database, branch_name, branch, chain, number);
        if (!epoch)
            break;
        if (epoch->previous != expected)
            throw std::runtime_error("[differential:corrupt_data] Shipping chain " + std::to_string(chain) +
                                     " is broken at epoch " + std::to_string(number));
        expected = epoch->hash;
        bool redirect = epoch->next_chain >= 0;
        epochs.push_back(std::move(*epoch));
        if (redirect)
            break;
    }
    return epochs;
}
// The base export of a chain as a map to import.
RemoteHead BaseHead(Control &control, const std::string &provider, const std::string &descriptor) {
    RemoteSnapshot snapshot(control, provider, descriptor);
    RemoteHead head;
    head.descriptor = descriptor;
    head.root = snapshot.descriptor.root;
    head.size = snapshot.descriptor.size;
    for (uint64_t page = 0; page < snapshot.descriptor.pages.size(); page++)
        for (auto &entry : snapshot.Entries(page)) {
            head.locations[entry.first.object] = entry.second;
            head.chunks.push_back(std::move(entry.first));
        }
    return head;
}
} // namespace

RecoveredBranch RecoverBranch(Control &control, const std::string &provider_id, const std::string &database,
                              const std::string &branch_name, const std::string &name, bool same_identity) {
    auto provider = S3Client(control, provider_id);
    auto newest = NewestEpoch(*provider, database, branch_name);
    if (!newest)
        throw std::runtime_error("[differential:not_found] Branch " + branch_name + " of database " + database +
                                 " was never shipped to this S3 location");
    auto epochs = ReadChain(*provider, database, branch_name, newest->branch, newest->chain, 0, "");
    if (epochs.empty())
        throw std::runtime_error("[differential:not_found] The newest shipping chain of branch " + branch_name +
                                 " has no first epoch");
    auto base = BaseHead(control, provider_id, epochs.front().base);
    auto branch = control.CreateFromShippedBase(name, provider_id, same_identity ? database : "",
                                                same_identity ? newest->branch : "", branch_name, base);
    auto &store = *control.store;
    auto main = store.FileId(branch.FileName());
    for (auto &epoch : epochs) {
        if (epoch.next_chain >= 0)
            continue; // a redirect carries no changes
        for (auto &suffix : epoch.removed)
            store.Remove(branch.FileName() + suffix);
        for (auto &f : epoch.files) {
            auto name = branch.FileName() + f.suffix;
            auto file = f.suffix.empty() ? main : store.Open(name, true, false, false).id;
            if (f.log) {
                store.Truncate(file, f.full ? 0 : f.tail_offset);
                if (f.tail_length)
                    store.Write(file, epoch.payload.data() + f.tail_payload, f.tail_length, f.tail_offset);
                store.Truncate(file, f.size);
                continue;
            }
            if (f.full)
                store.Truncate(file, 0);
            static const Bytes zero(Store::CHUNK, 0);
            for (auto &c : f.chunks) {
                auto data = c.hash.empty() ? zero.data() : epoch.payload.data() + c.payload;
                auto offset = Store::ChunkOffset(c.index);
                auto length = std::min(Store::ChunkLength(c.index), f.size - offset);
                store.Write(file, data, length, offset);
            }
            store.Truncate(file, f.size);
        }
    }
    store.Sync();
    RecoveredBranch result;
    result.database = branch.database;
    result.branch = branch.id;
    result.chain = epochs.back().chain;
    result.epoch = epochs.back().epoch;
    result.sequence = epochs.back().sequence;
    return result;
}

// --- Followers --------------------------------------------------------------------------------------
uint64_t FollowImage::Size(const std::string &suffix) const {
    if (suffix.empty())
        return size;
    auto found = logs.find(suffix);
    return found == logs.end() ? 0 : found->second->size();
}
uint64_t FollowImage::Read(const std::string &suffix, void *data, uint64_t length, uint64_t offset) const {
    auto out = static_cast<unsigned char *>(data);
    if (!suffix.empty()) {
        auto found = logs.find(suffix);
        if (found == logs.end() || offset >= found->second->size())
            return 0;
        auto n = std::min(length, found->second->size() - offset);
        std::memcpy(out, found->second->data() + offset, n);
        return n;
    }
    if (offset >= size)
        return 0;
    length = std::min(length, size - offset);
    uint64_t done = 0;
    while (done < length) {
        auto position = offset + done, index = Store::ChunkIndex(position);
        auto take = std::min(length - done, Store::ChunkLength(index) - Store::ChunkPosition(position));
        auto changed = chunks.find(index);
        if (changed != chunks.end()) {
            if (changed->second)
                std::memcpy(out + done, changed->second->data() + Store::ChunkPosition(position), take);
            else
                std::memset(out + done, 0, take);
        } else if (position >= base->descriptor.size)
            std::memset(out + done, 0, take);
        else {
            auto available = std::min(take, base->descriptor.size - position);
            if (base->Read(out + done, available, position) != available)
                throw std::runtime_error("[differential:unavailable_object] Short read of a shipping base");
            std::memset(out + done + available, 0, take - available);
        }
        done += take;
    }
    return length;
}
namespace {
struct Images {
    std::mutex mutex;
    std::map<std::string, std::shared_ptr<const FollowImage>> images;
    std::atomic<uint64_t> counter{0};
};
Images &I() {
    static auto *images = new Images(); // never destroyed (followers may release images at exit)
    return *images;
}
} // namespace
std::string RegisterFollowImage(std::shared_ptr<const FollowImage> image) {
    auto id = "i" + std::to_string(++I().counter);
    std::lock_guard<std::mutex> guard(I().mutex);
    I().images[id] = std::move(image);
    return id;
}
std::shared_ptr<const FollowImage> FindFollowImage(const std::string &id) {
    std::lock_guard<std::mutex> guard(I().mutex);
    auto found = I().images.find(id);
    return found == I().images.end() ? nullptr : found->second;
}
void ReleaseFollowImage(const std::string &id) {
    std::lock_guard<std::mutex> guard(I().mutex);
    I().images.erase(id);
}

ChainFollower::ChainFollower(Control &control, std::string provider_id, std::string database, std::string branch_name,
                             std::string branch)
    : control(control), provider(S3Client(control, provider_id)), provider_id(std::move(provider_id)),
      database(std::move(database)), branch_name(std::move(branch_name)), branch(std::move(branch)) {
    auto newest = NewestEpoch(*provider, this->database, this->branch_name, this->branch);
    if (!newest)
        throw std::runtime_error("[differential:not_found] Branch " + this->branch_name + " is not shipped");
    this->branch = newest->branch;
    Start(newest->chain);
}
void ChainFollower::Start(int64_t chain) {
    auto epochs = ReadChain(*provider, database, branch_name, branch, chain, 0, "");
    if (epochs.empty())
        throw std::runtime_error("[differential:not_found] Shipping chain " + std::to_string(chain) + " has no epochs");
    auto fresh = std::make_shared<FollowImage>();
    fresh->base = std::make_shared<RemoteSnapshot>(control, provider_id, epochs.front().base);
    fresh->size = fresh->base->descriptor.size;
    fresh->chain = chain;
    image = fresh;
    last_hash.clear();
    for (auto &epoch : epochs) {
        if (epoch.next_chain >= 0) {
            Start(epoch.next_chain);
            return;
        }
        Apply(epoch);
    }
}
void ChainFollower::Apply(const Epoch &epoch) {
    // Images are immutable once published: build the next one from a copy.
    auto next = std::make_shared<FollowImage>(*image);
    for (auto &suffix : epoch.removed)
        next->logs.erase(suffix);
    for (auto &f : epoch.files) {
        if (f.log) {
            auto bytes = std::make_shared<Bytes>();
            if (!f.full) {
                auto found = next->logs.find(f.suffix);
                if (found != next->logs.end())
                    *bytes = *found->second;
            }
            bytes->resize(f.tail_offset, 0);
            bytes->insert(bytes->end(), epoch.payload.begin() + int64_t(f.tail_payload),
                          epoch.payload.begin() + int64_t(f.tail_payload + f.tail_length));
            next->logs[f.suffix] = bytes;
            continue;
        }
        if (!f.suffix.empty())
            continue; // only the main file is chunked
        if (f.full) {
            // A replaced main file: everything not listed is a hole.
            next->chunks.clear();
            for (uint64_t index = 0; index < Store::ChunkCount(std::max(f.size, next->base->descriptor.size)); index++)
                next->chunks[index] = nullptr;
        }
        for (auto &c : f.chunks)
            next->chunks[c.index] =
                c.hash.empty() ? nullptr
                               : std::make_shared<const Bytes>(epoch.payload.begin() + int64_t(c.payload),
                                                               epoch.payload.begin() + int64_t(c.payload + Store::CHUNK));
        // Chunks beyond the new end are gone; a later regrowth reads zeros there. (A truncation inside a
        // chunk ships that chunk with its tail zeroed, as the store keeps it.)
        for (auto index = Store::ChunkCount(f.size); index < Store::ChunkCount(next->size); index++)
            next->chunks[index] = nullptr;
        next->size = f.size;
    }
    next->epoch = epoch.epoch;
    next->sequence = epoch.sequence;
    next->published_at = epoch.published_at;
    last_hash = epoch.hash;
    image = next;
    ++epochs_followed;
}
bool ChainFollower::Advance() {
    bool changed = false;
    while (true) {
        auto number = image->epoch + (last_hash.empty() ? 0 : 1);
        auto epoch = ReadEpoch(*provider, database, branch_name, branch, image->chain, number);
        if (!epoch)
            return changed;
        if (epoch->previous != last_hash)
            throw std::runtime_error("[differential:corrupt_data] Shipping chain " + std::to_string(image->chain) +
                                     " is broken at epoch " + std::to_string(number));
        changed = true;
        if (epoch->next_chain >= 0) {
            Start(epoch->next_chain);
            continue;
        }
        Apply(*epoch);
    }
}
#undef worker_mutex
#undef worker_signal
#undef shipped_signal
#undef shipped
#undef worker
#undef worker_running
} // namespace differential
