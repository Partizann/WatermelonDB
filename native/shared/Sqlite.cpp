#include "Sqlite.h"
#include "DatabasePlatform.h"
#include <cassert>
#include <algorithm>
#include <forward_list>

namespace watermelondb {

using platform::consoleError;
using platform::consoleLog;

std::string resolveDatabasePath(std::string path) {
    if (path == "" || path == ":memory:" || path.rfind("file:", 0) == 0 || path.rfind("/", 0) == 0) {
        // These seem like paths/sqlite path-like strings
        return path;
    } else {
        // path is a name to be resolved based on platform preferences
        return platform::resolveDatabasePath(path);
    }
}

SqliteDb::SqliteDb(std::string path) {
    consoleLog("Will open database...");
    platform::initializeSqlite();
    #ifndef ANDROID
    assert(sqlite3_threadsafe());
    #endif

    auto resolvedPath = resolveDatabasePath(path);
    int openResult = sqlite3_open(resolvedPath.c_str(), &sqlite);

    if (openResult != SQLITE_OK) {
        if (sqlite) {
            auto error = std::string(sqlite3_errmsg(sqlite));
            throw new std::runtime_error("Error while trying to open database - " + error);
        } else {
            // whoa, sqlite couldn't allocate memory
            throw new std::runtime_error("Error while trying to open database, sqlite is null - " + std::to_string(openResult));
        }
    }
    assert(sqlite != nullptr);

    consoleLog("Opened database at " + resolvedPath);
}

void SqliteDb::destroy() {
    if (isDestroyed_) {
        consoleLog("Database is already closed");
        return;
    }
    consoleLog("Closing database...");

    isDestroyed_ = true;
    assert(sqlite != nullptr);

    // Find and finalize all prepared statements
    std::forward_list<sqlite3_stmt*> list;
    sqlite3_stmt *stmt = nullptr;
    while ((stmt = sqlite3_next_stmt(sqlite, stmt))) {
        list.emplace_front(stmt);
    }
    std::for_each(list.begin(), list.end(), [](const auto &stmt) { sqlite3_finalize(stmt); });

    // Close connection
    // NOTE: Applications should finalize all prepared statements, close all BLOB handles, and finish all sqlite3_backup objects
    int closeResult = sqlite3_close(sqlite);

    if (closeResult != SQLITE_OK) {
        // NOTE: We're just gonna log an error. We can't throw an exception here. We could crash, but most likely we're
        // only leaking memory/resources
        consoleError("Failed to close sqlite database - " + std::string(sqlite3_errmsg(sqlite)));
    } else {
        consoleLog("Database closed.");
    }
}

SqliteDb::~SqliteDb() {
    destroy();
}

} // namespace watermelondb
