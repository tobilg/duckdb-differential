#pragma once
#include "duckdb/main/client_context_state.hpp"
#include "differential_control.hpp"
namespace duckdb {
struct DifferentialRecoveryState : ClientContextState {
    std::shared_ptr<differential::BranchLock> lease;
    explicit DifferentialRecoveryState(std::shared_ptr<differential::BranchLock> lease)
        : lease(std::move(lease)) {}
};
} // namespace duckdb
