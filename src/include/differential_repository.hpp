#pragma once
#include "differential_control.hpp"
#include <functional>
#include <mutex>
#include <filesystem>
#include <stdexcept>
namespace differential {
// Configuration is cheap and side-effect free. Only execution/file access opens metadata.
class Repository {
  public:
    explicit Repository(std::string directory) : directory(std::move(directory)) {}
    // Relative paths resolve once, against the current working directory.
    void Select(const std::string &requested) {
        if (requested.empty())
            throw std::runtime_error("Repository path cannot be empty");
        auto path = std::filesystem::absolute(requested).lexically_normal().string();
        if (path.size() > 1 && path.back() == '/')
            path.pop_back();
        std::lock_guard<std::mutex> guard(mutex);
        if (control && path != directory)
            throw std::runtime_error("Select repository before its first use in this DuckDB instance");
        directory = path;
    }
    // Request a repository shared with other local users (group-writable). Only applies when the
    // repository is created; an existing repository keeps its recorded mode.
    void Share(bool shared) {
        std::lock_guard<std::mutex> guard(mutex);
        if (control && shared && !control->store->permissions.shared)
            throw std::runtime_error("The repository is already open for a single user; set "
                                     "differential_repository_shared before its first use");
        share = shared;
    }
    std::shared_ptr<Control> Get() {
        std::lock_guard<std::mutex> guard(mutex);
        if (!control) {
            control = std::make_shared<Control>(std::make_shared<Store>(directory, share));
            if (on_open)
                on_open(control);
        }
        return control;
    }
    struct State {
        std::string directory;
        bool open = false;
        bool shared = false; // the opened repository's mode, else the requested one
    };
    State Configuration() {
        std::lock_guard<std::mutex> guard(mutex);
        return {directory, bool(control), control ? control->store->permissions.shared : share};
    }
    std::string directory;
    // Runs once for the opened repository (the extension installs its reader of S3-only objects).
    std::function<void(const std::shared_ptr<Control> &)> on_open;

  private:
    bool share = false;
    std::mutex mutex;
    std::shared_ptr<Control> control;
};
} // namespace differential
