#pragma once
#include <sqlite3.h>
#include <string>
namespace differential {
inline std::string SQLiteError(sqlite3 *db) {
    const char *category = "io_error";
    auto code = sqlite3_extended_errcode(db);
    if ((code & 255) == SQLITE_FULL)
        category = "disk_full";
    else if ((code & 255) == SQLITE_BUSY || (code & 255) == SQLITE_LOCKED)
        category = "writer_busy";
    else if ((code & 255) == SQLITE_CORRUPT || (code & 255) == SQLITE_NOTADB ||
             code == SQLITE_CONSTRAINT_FOREIGNKEY)
        category = "corrupt_data";
    else if (code == SQLITE_CONSTRAINT_UNIQUE || code == SQLITE_CONSTRAINT_PRIMARYKEY)
        category = "name_conflict";
    return std::string("[differential:") + category + "] " + sqlite3_errmsg(db);
}
} // namespace differential
