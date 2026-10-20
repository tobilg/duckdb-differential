#pragma once
// Errors whose kind callers act on. The message keeps its user-visible "[differential:<category>]"
// prefix; the code is internal, so control flow never depends on message wording.
#include <stdexcept>
#include <string>

namespace differential {
enum class ErrorCode {
    WriterBusy,      // a branch's writer lock is held by a live attachment or management owner
    HeadConflict,    // a single-writer S3 branch head was written by another writer
    BranchConflict,  // a shared branch head moved in S3 since this repository's base
    ObjectConflict,  // a create-only S3 object exists with other content
};
class Error : public std::runtime_error {
  public:
    Error(ErrorCode code, const std::string &message) : std::runtime_error(message), code(code) {}
    const ErrorCode code;
};
// Whether `error` (or the differential error it is) has `code`.
inline bool Is(const std::exception &error, ErrorCode code) {
    auto typed = dynamic_cast<const Error *>(&error);
    return typed && typed->code == code;
}
} // namespace differential
