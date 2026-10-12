// Phase-0 instrumented local pass-through. Not the differential storage backend.
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "differential_hooks.hpp"
#include <cstdlib>
#include <cstdio>
#include <mutex>
#include <unistd.h>
#include <filesystem>
#include <thread>
#include <chrono>

namespace duckdb {
namespace {
void Trace(const char *op, const string &path, int64_t n = 0, idx_t offset = 0) {
    const char *gate = differential::Hook("DIFFERENTIAL_PROBE_GATE");
    const char *gate_uri = differential::Hook("DIFFERENTIAL_PROBE_GATE_URI");
    if (gate && string(op) == "pwrite" && offset >= 12288 && path == (gate_uri ? gate_uri : "diff://probe")) {
        std::error_code error;
        std::filesystem::rename(string(gate) + ".arm", string(gate) + ".entered", error);
        if (!error) {
            for (unsigned i = 0; i < 1000; i++) {
                if (std::filesystem::exists(string(gate) + ".release"))
                    break;
                if (i == 999)
                    throw IOException("Probe gate timed out");
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    const char *fault = differential::Hook("DIFFERENTIAL_PROBE_CRASH");
    if (fault) {
        static std::mutex fault_lock;
        static unsigned hits = 0;
        std::lock_guard<std::mutex> guard(fault_lock);
        string spec(fault);
        auto sep = spec.rfind(':');
        if (sep != string::npos && spec.substr(0, sep) == string(op) + ":" + path) {
            if (++hits == std::stoul(spec.substr(sep + 1)))
                _exit(86);
        }
    }
    const char *trace = differential::Hook("DIFFERENTIAL_PROBE_TRACE");
    if (!trace)
        return;
    static std::mutex lock;
    std::lock_guard<std::mutex> guard(lock);
    auto f = std::fopen(trace, "a");
    if (!f)
        throw IOException("Cannot open differential probe trace");
    std::fprintf(f, "%s\t%s\t%lld\t%llu\n", op, path.c_str(), static_cast<long long>(n),
                 static_cast<unsigned long long>(offset));
    std::fclose(f);
}
struct ProbeHandle : FileHandle {
    unique_ptr<FileHandle> inner;
    ProbeHandle(FileSystem &fs, string path, FileOpenFlags flags, unique_ptr<FileHandle> inner)
        : FileHandle(fs, std::move(path), flags), inner(std::move(inner)) {}
    ~ProbeHandle() override { Close(); }
    void Close() override {
        if (inner) {
            inner->Close();
            inner.reset();
        }
    }
};
class ProbeFileSystem : public FileSystem {
    unique_ptr<FileSystem> local = FileSystem::CreateLocal();
    string root;
    string Map(const string &path) {
        if (path.rfind("diff://", 0) != 0)
            throw IOException("Not a differential probe URI");
        auto tail = path.substr(7);
        if (tail.empty() || tail.find("..") != string::npos ||
            tail.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./") !=
                string::npos ||
            tail[0] == '/')
            throw IOException("Invalid differential probe URI");
        return root + "/" + tail;
    }
    FileHandle &Inner(FileHandle &h) { return *static_cast<ProbeHandle &>(h).inner; }

  public:
    explicit ProbeFileSystem(string root) : root(std::move(root)) {
        if (this->root.empty() || this->root[0] != '/')
            throw IOException("Probe root must be absolute");
    }
    string GetName() const override { return "differential_probe"; }
    bool CanHandleFile(const string &p) override { return p.rfind("diff:", 0) == 0; }
    bool CanSeek() override { return true; }
    bool OnDiskFile(FileHandle &) override { return true; }
    bool IsPathAbsolute(const string &) override { return true; }
    string PathSeparator(const string &) override { return "/"; }
    string CanonicalizePath(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Map(p);
        Trace("canonical", p);
        return p;
    }
    unique_ptr<FileHandle> OpenFile(const string &p, FileOpenFlags flags,
                                    optional_ptr<FileOpener> = nullptr) override {
        if (p.find(".snapshot") != string::npos && flags.OpenForWriting())
            throw IOException("Immutable probe snapshot is read-only");
        Trace("open", p, flags.GetFlagsInternal(), static_cast<idx_t>(flags.Lock()));
        auto inner = local->OpenFile(Map(p), flags);
        if (!inner)
            return nullptr;
        return make_uniq<ProbeHandle>(*this, p, flags, std::move(inner));
    }
    void Read(FileHandle &h, void *b, int64_t n, idx_t pos) override {
        Trace("pread", h.path, n, pos);
        local->Read(Inner(h), b, n, pos);
    }
    void Write(FileHandle &h, void *b, int64_t n, idx_t pos) override {
        Trace("pwrite", h.path, n, pos);
        local->Write(Inner(h), b, n, pos);
        Trace("pwrite.end", h.path, n, pos);
    }
    int64_t Read(FileHandle &h, void *b, int64_t n) override {
        Trace("read", h.path, n);
        return local->Read(Inner(h), b, n);
    }
    int64_t Write(FileHandle &h, void *b, int64_t n) override {
        Trace("write", h.path, n);
        return local->Write(Inner(h), b, n);
    }
    int64_t GetFileSize(FileHandle &h) override {
        Trace("size", h.path);
        return local->GetFileSize(Inner(h));
    }
    FileType GetFileType(FileHandle &h) override { return local->GetFileType(Inner(h)); }
    timestamp_t GetLastModifiedTime(FileHandle &h) override { return local->GetLastModifiedTime(Inner(h)); }
    void FileSync(FileHandle &h) override {
        Trace("sync.begin", h.path);
        local->FileSync(Inner(h));
        Trace("sync.end", h.path);
    }
    void Truncate(FileHandle &h, int64_t n) override {
        Trace("truncate", h.path, n);
        local->Truncate(Inner(h), n);
    }
    bool Trim(FileHandle &h, idx_t p, idx_t n) override {
        Trace("trim", h.path, n, p);
        return local->Trim(Inner(h), p, n);
    }
    void Seek(FileHandle &h, idx_t p) override {
        Trace("seek", h.path, 0, p);
        local->Seek(Inner(h), p);
    }
    idx_t SeekPosition(FileHandle &h) override { return local->SeekPosition(Inner(h)); }
    void Reset(FileHandle &h) override { local->Reset(Inner(h)); }
    bool FileExists(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Trace("exists", p);
        return local->FileExists(Map(p));
    }
    bool DirectoryExists(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Trace("dir.exists", p);
        return local->DirectoryExists(Map(p));
    }
    void CreateDirectory(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Trace("mkdir", p);
        local->CreateDirectory(Map(p));
    }
    bool IsPipe(const string &, optional_ptr<FileOpener> = nullptr) override { return false; }
    void MoveFile(const string &a, const string &b, optional_ptr<FileOpener> = nullptr) override {
        Trace("move.from", a);
        Trace("move.to", b);
        local->MoveFile(Map(a), Map(b));
    }
    void RemoveFile(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Trace("remove", p);
        local->RemoveFile(Map(p));
    }
    bool TryRemoveFile(const string &p, optional_ptr<FileOpener> = nullptr) override {
        Trace("try_remove", p);
        return local->TryRemoveFile(Map(p));
    }
};
} // namespace
void ProbeEvent(const char *op, const string &path, int64_t n, idx_t p) { Trace(op, path, n, p); }
class UnconfiguredFileSystem : public FileSystem {
  public:
    string GetName() const override { return "differential_unconfigured"; }
    bool IsPathAbsolute(const string &) override { return true; }
    string CanonicalizePath(const string &path, optional_ptr<FileOpener> = nullptr) override { return path; }
    bool CanHandleFile(const string &path) override { return path.rfind("diff:", 0) == 0; }
    bool FileExists(const string &, optional_ptr<FileOpener> = nullptr) override {
        throw InvalidInputException(
            "Experimental differential backend: set an absolute DIFFERENTIAL_STORE_ROOT before LOAD; v1 "
            "management API is not implemented");
    }
    unique_ptr<FileHandle> OpenFile(const string &, FileOpenFlags,
                                    optional_ptr<FileOpener> = nullptr) override {
        throw InvalidInputException("Differential repository is not configured");
    }
};
void RegisterProbeFileSystem(ExtensionLoader &loader) {
    const char *root = differential::Hook("DIFFERENTIAL_PROBE_ROOT");
    if (!root) {
        FileSystem::GetFileSystem(loader.GetDatabaseInstance())
            .RegisterSubSystem(make_uniq<UnconfiguredFileSystem>());
        return;
    }
    if (root)
        FileSystem::GetFileSystem(loader.GetDatabaseInstance())
            .RegisterSubSystem(make_uniq<ProbeFileSystem>(root));
}
} // namespace duckdb
