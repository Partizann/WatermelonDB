#pragma once

#import <memory>
#import <string>
#import <sqlite3.h>

namespace watermelondb {

// Lightweight wrapper for handling sqlite3 lifetime
class SqliteDb {
public:
    SqliteDb(std::string path);
    ~SqliteDb();
    void destroy();

    sqlite3 *sqlite;

    SqliteDb &operator=(const SqliteDb &) = delete;
    SqliteDb(const SqliteDb &) = delete;

private:
    std::atomic<bool> isDestroyed_;
};

} // namespace watermelondb

