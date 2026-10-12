#pragma once
#include "differential_permissions.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace differential {
// Cross-process fence between external-payload readers/writers (shared) and deletion
// (exclusive). External payloads have no SQLite MVCC. One flock per process is shared by
// all in-process holders, so acquiring it costs no system call after the first holder.
class PayloadFence {
  public:
    PayloadFence(const std::string &repository, bool exclusive);
    // Exclusive acquisition that gives up instead of waiting (bounded background work).
    static bool TryExclusive(const std::string &repository, std::unique_ptr<PayloadFence> &out);
    ~PayloadFence();
    PayloadFence(const PayloadFence &) = delete;
    PayloadFence &operator=(const PayloadFence &) = delete;
    struct State;

  private:
    PayloadFence() = default;
    State *state = nullptr;
    bool exclusive = false;
};
class LocalPool {
  public:
    // `permissions` are those of the owning repository (shared repositories share their pools).
    LocalPool(const std::string &path, const std::string &repository, const std::string &provider,
              const Permissions &permissions = {});
    // Durably publish immutable chunks: per-file data sync, atomic link, directory syncs and
    // one final full-device flush for the whole batch.
    void PutBatch(const std::vector<std::pair<std::string, const std::vector<unsigned char> *>> &objects);
    void Put(const std::string &hash, const std::vector<unsigned char> &bytes);
    std::vector<unsigned char> Get(const std::string &hash);
    void Remove(const std::string &entry);
    // A bounded sorted page of owned names after `after`. Names are hashes or staging files.
    // Work is bounded by the fan-out directories visited, not the whole namespace.
    std::vector<std::string> List(const std::string &after, uint64_t limit);
    static std::string Canonical(const std::string &path);
    const std::string directory;

  private:
    Permissions permissions;
    std::string Path(const std::string &hash) const;
};
} // namespace differential
