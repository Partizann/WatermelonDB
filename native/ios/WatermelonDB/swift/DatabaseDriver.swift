import Foundation

class DatabaseDriver {
    typealias SchemaVersion = Int
    typealias Schema = (version: SchemaVersion, sql: Database.SQL)
    typealias MigrationSet = (from: SchemaVersion, to: SchemaVersion, sql: Database.SQL)

    struct SchemaNeededError: Error { }
    struct MigrationNeededError: Error {
        let databaseVersion: SchemaVersion
    }

    let database: Database

    convenience init(dbName: String, schemaVersion: SchemaVersion) throws {
        self.init(dbName: dbName)

        switch isCompatible(withVersion: schemaVersion) {
        case .compatible: break
        case .needsSetup:
            throw SchemaNeededError()
        case .needsMigration(fromVersion: let dbVersion):
            throw MigrationNeededError(databaseVersion: dbVersion)
        }
    }

    convenience init(dbName: String, setUpWithSchema schema: Schema) {
        self.init(dbName: dbName)

        do {
            try unsafeResetDatabase(schema: schema)
        } catch {
            fatalError("Error while setting up the database: \(error)")
        }
    }

    convenience init(dbName: String, setUpWithMigrations migrations: MigrationSet) throws {
        self.init(dbName: dbName)
        try migrate(with: migrations)
    }

    private init(dbName: String) {
        self.database = Database(path: getPath(dbName: dbName))
    }

    func find(table: Database.TableName, id: String) throws -> Any? {
        let results = try database.queryRaw("select * from `\(table)` where id == ? limit 1", [id])

        guard let record = results.next() else {
            return nil
        }

        return record.resultDictionary!
    }

    func cachedQuery(table: Database.TableName, query: Database.SQL, args: Database.QueryArgs = []) throws -> [Any] {
        return try database.queryRaw(query, args).map { row in
            return row.resultDictionary!
        }
    }

    func queryIds(query: Database.SQL, args: Database.QueryArgs = []) throws -> [String] {
        return try database.queryRaw(query, args).map { row in
            row.string(forColumn: "id")!
        }
    }

    func unsafeQueryRaw(query: Database.SQL, args: Database.QueryArgs = []) throws -> [Any] {
        return try database.queryRaw(query, args).map { row in
            row.resultDictionary!
        }
    }

    func count(_ query: Database.SQL, args: Database.QueryArgs = []) throws -> Int {
        return try database.count(query, args)
    }

    enum CacheBehavior {
        case ignore
        case addFirstArg(table: Database.TableName)
        case removeFirstArg(table: Database.TableName)
    }

    struct Operation {
        let cacheBehavior: CacheBehavior
        let sql: Database.SQL
        let argBatches: [Database.QueryArgs]
    }

    func batch(_ operations: [Operation]) throws {
        try database.inTransaction {
            for operation in operations {
                for args in operation.argBatches {
                    try database.execute(operation.sql, args)
                }
            }
        }
    }

// MARK: - LocalStorage

    func getLocal(key: String) throws -> String? {
        let results = try database.queryRaw("select `value` from `local_storage` where `key` = ?", [key])

        guard let record = results.next() else {
            return nil
        }

        return record.string(forColumn: "value")!
    }

// MARK: - Other private details

    private enum SchemaCompatibility {
        case compatible
        case needsSetup
        case needsMigration(fromVersion: SchemaVersion)
    }

    private func isCompatible(withVersion schemaVersion: SchemaVersion) -> SchemaCompatibility {
        let databaseVersion = database.userVersion

        switch databaseVersion {
        case schemaVersion: return .compatible
        case 0: return .needsSetup
        case (1..<schemaVersion): return .needsMigration(fromVersion: databaseVersion)
        default:
            consoleLog("Database has newer version (\(databaseVersion)) than what the " +
                "app supports (\(schemaVersion)). Will reset database.")
            return .needsSetup
        }
    }

    func unsafeResetDatabase(schema: Schema) throws {
        try database.unsafeDestroyEverything()

        try database.inTransaction {
            try database.executeStatements(schema.sql)
            database.userVersion = schema.version
        }
    }

    private func migrate(with migrations: MigrationSet) throws {
        precondition(
            database.userVersion == migrations.from,
            "Incompatbile migration set applied. DB: \(database.userVersion), migration: \(migrations.from)"
        )

        try database.inTransaction {
            try database.executeStatements(migrations.sql)
            database.userVersion = migrations.to
        }
    }
}

private func getPath(dbName: String) -> String {
    // If starts with `file:` or contains `/`, it's a path!
    if dbName.starts(with: "file:") || dbName.contains("/") {
        return dbName
    } else {
        // swiftlint:disable:next force_try
        return try! FileManager.default
            .url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
            .appendingPathComponent("\(dbName).db")
            .path
    }
}
