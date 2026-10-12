#pragma once
// Internal SQLite helpers shared by the byte store and the metadata authority.
#include "differential_hooks.hpp"
#include "differential_sqlite_error.hpp"
#include <sqlite3.h>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace differential {
inline void Execute(sqlite3 *db, const char *sql) {
    if (sqlite3_exec(db, sql, nullptr, nullptr, nullptr) != SQLITE_OK)
        throw std::runtime_error(SQLiteError(db));
}
struct Query {
    sqlite3 *db;
    sqlite3_stmt *s = nullptr;
    bool owned = true;
    Query(sqlite3 *db, const char *sql) : db(db) {
        if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK)
            Error();
    }
    // Borrow a cached statement; it is reset, not finalized, on destruction.
    Query(sqlite3 *db, sqlite3_stmt *cached) : db(db), s(cached), owned(false) {}
    Query(const Query &) = delete;
    Query &operator=(const Query &) = delete;
    ~Query() {
        if (owned)
            sqlite3_finalize(s);
        else {
            sqlite3_reset(s);
            sqlite3_clear_bindings(s);
        }
    }
    [[noreturn]] void Error() const { throw std::runtime_error(SQLiteError(db)); }
    Query &Text(int i, const std::string &v) {
        if (sqlite3_bind_text(s, i, v.data(), int(v.size()), SQLITE_TRANSIENT) != SQLITE_OK)
            Error();
        return *this;
    }
    Query &Int(int i, int64_t v) {
        if (sqlite3_bind_int64(s, i, v) != SQLITE_OK)
            Error();
        return *this;
    }
    Query &Blob(int i, const void *v, int n) {
        if (sqlite3_bind_blob(s, i, v, n, SQLITE_TRANSIENT) != SQLITE_OK)
            Error();
        return *this;
    }
    Query &Null(int i) {
        if (sqlite3_bind_null(s, i) != SQLITE_OK)
            Error();
        return *this;
    }
    bool Step() {
        auto rc = sqlite3_step(s);
        if (rc == SQLITE_ROW)
            return true;
        if (rc == SQLITE_DONE)
            return false;
        Error();
    }
    // Run to completion; returns sqlite3_changes for DML.
    int Run() {
        while (Step()) {
        }
        return sqlite3_changes(db);
    }
    int64_t Int(int i) const { return sqlite3_column_int64(s, i); }
    bool IsNull(int i) const { return sqlite3_column_type(s, i) == SQLITE_NULL; }
    std::string Text(int i) const {
        auto p = sqlite3_column_text(s, i);
        return p ? std::string(reinterpret_cast<const char *>(p), size_t(sqlite3_column_bytes(s, i))) : "";
    }
};
// BEGIN IMMEDIATE on the authority connection. A non-durable transaction commits with
// SQLite WAL synchronous=NORMAL: it survives process death and is made durable by the
// next FULL commit, which syncs the shared WAL including earlier frames.
struct Transaction {
    sqlite3 *db;
    const char *crash_variable;
    bool done = false, durable;
    explicit Transaction(sqlite3 *db, const char *crash_variable = "DIFFERENTIAL_CONTROL_CRASH",
                         bool durable = true)
        : db(db), crash_variable(crash_variable), durable(durable) {
        if (!durable)
            Execute(db, "PRAGMA synchronous=NORMAL");
        if (sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
            auto error = SQLiteError(db);
            if (!durable)
                sqlite3_exec(db, "PRAGMA synchronous=FULL", nullptr, nullptr, nullptr);
            throw std::runtime_error(error);
        }
    }
    Transaction(const Transaction &) = delete;
    Transaction &operator=(const Transaction &) = delete;
    void Commit() {
        Failpoint(crash_variable, "before_commit");
        Execute(db, "COMMIT");
        done = true;
        if (!durable)
            Execute(db, "PRAGMA synchronous=FULL");
        Failpoint(crash_variable, "after_commit");
    }
    ~Transaction() {
        if (!done)
            sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        if (!done && !durable)
            sqlite3_exec(db, "PRAGMA synchronous=FULL", nullptr, nullptr, nullptr);
    }
};
} // namespace differential
