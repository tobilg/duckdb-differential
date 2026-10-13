#pragma once
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
// Validation only: never create a directory or open the repository while planning.
inline void ValidateDifferentialTempDirectory(const string &path) {
    auto lower = StringUtil::Lower(path);
    if (lower.rfind("diff:", 0) == 0 || lower.rfind("differential:", 0) == 0 ||
        lower.find("://") != string::npos || FileSystem::IsRemoteFile(lower))
        throw InvalidInputException("[differential:local_temp_directory_required] "
                                    "DuckDB spill files require a local temp_directory; "
                                    "use SET temp_directory='/absolute/local/directory' "
                                    "(or an empty string to disable spilling)");
}
} // namespace duckdb
