#pragma once
// File modes of a repository and everything it owns: journals, locks, pins, upload staging and
// local payload pools. A repository is created either private (0700 directories, 0600 files)
// or shared with the group of its directory (setgid 02770 directories, 0660 files), so several
// local users of that group can use it. Modes are set explicitly after creation, so the process
// umask cannot narrow them.
#include <cerrno>
#include <filesystem>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace differential {
struct Permissions {
    bool shared = false;
    mode_t Directory() const { return shared ? 02770 : 0700; }
    mode_t File() const { return shared ? 0660 : 0600; }

    // Create `path` and missing parents; every directory created here gets Directory().
    // Returns the created directories, outermost first.
    std::vector<std::filesystem::path> CreateDirectories(const std::filesystem::path &path) const {
        std::vector<std::filesystem::path> missing;
        for (auto p = path; !p.empty() && !std::filesystem::exists(p); p = p.parent_path()) {
            missing.push_back(p);
            if (p == p.parent_path())
                break;
        }
        std::vector<std::filesystem::path> created;
        for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
            if (mkdir(it->c_str(), Directory()) != 0) {
                if (errno == EEXIST)
                    continue;
                throw std::runtime_error("[differential:local_io] Cannot create directory " + it->string());
            }
            if (chmod(it->c_str(), Directory()) != 0)
                throw std::runtime_error("[differential:local_io] Cannot set directory permissions " + it->string());
            created.push_back(*it);
        }
        return created;
    }
    // Owner-only: a file this process created (or owns) gets File(); others' files are left alone.
    void Apply(int fd) const {
        struct stat st {};
        if (fd >= 0 && fstat(fd, &st) == 0 && st.st_uid == geteuid() && (st.st_mode & 07777) != File())
            fchmod(fd, File());
    }
    // open(path, flags | O_CREAT) with this repository's file mode.
    int Open(const std::filesystem::path &path, int flags) const {
        int fd = open(path.c_str(), flags | O_CREAT, File());
        Apply(fd);
        return fd;
    }

    // Permissions of the repository at `directory`, registered when its store opens
    // (private for directories that no open store has registered).
    static Permissions Of(const std::string &directory) {
        std::lock_guard<std::mutex> guard(RegistryMutex());
        auto found = Registry().find(directory);
        return found == Registry().end() ? Permissions{} : found->second;
    }
    static void Register(const std::string &directory, Permissions permissions) {
        std::lock_guard<std::mutex> guard(RegistryMutex());
        Registry()[directory] = permissions;
    }

  private:
    static std::map<std::string, Permissions> &Registry() {
        static std::map<std::string, Permissions> registry;
        return registry;
    }
    static std::mutex &RegistryMutex() {
        static std::mutex mutex;
        return mutex;
    }
};
} // namespace differential
