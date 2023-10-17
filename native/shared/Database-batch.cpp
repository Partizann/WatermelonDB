#include "Database.h"

namespace watermelondb {

using platform::consoleError;
using platform::consoleLog;

// TODO: Remove non-json batch once we can tell that there's no serious perf regression
void Database::batch(jsi::Array &operations) {
    auto &rt = getRt();
    beginTransaction();

    try {
        size_t operationsCount = operations.length(rt);
        for (size_t i = 0; i < operationsCount; i++) {
            jsi::Array operation = operations.getValueAtIndex(rt, i).getObject(rt).getArray(rt);

            auto sql = operation.getValueAtIndex(rt, 2).getString(rt).utf8(rt);

            jsi::Array argsBatches = operation.getValueAtIndex(rt, 3).getObject(rt).getArray(rt);
            size_t argsBatchesCount = argsBatches.length(rt);
            for (size_t j = 0; j < argsBatchesCount; j++) {
                jsi::Array args = argsBatches.getValueAtIndex(rt, j).getObject(rt).getArray(rt);
                executeUpdate(sql, args);
            }

        }
        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }
}

void Database::batchJSON(jsi::String &&jsiJson) {
    using namespace simdjson;

    auto &rt = getRt();
    beginTransaction();

    try {
        ondemand::parser parser;
        auto json = padded_string(jsiJson.utf8(rt));
        ondemand::document doc = parser.iterate(json);

        // NOTE: simdjson::ondemand processes forwards-only, hence the weird field enumeration
        // We can't use subscript or backtrack.
        for (ondemand::array operation : doc) {
            std::string sql;
            size_t fieldIdx = 0;
            for (auto field : operation) {
                if (fieldIdx == 2) {
                    sql = (std::string_view) field;
                } else if (fieldIdx == 3) {
                    ondemand::array argsBatches = field;
                    auto stmt = prepareQuery(sql);

                    for (ondemand::array args : argsBatches) {
                        executeUpdate(stmt);
                        sqlite3_reset(stmt);
                        sqlite3_clear_bindings(stmt);
                    }
                }
                fieldIdx++;
            }
        }

        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }
}

}
