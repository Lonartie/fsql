#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("MemoryStorage::column_index");

TEST_CASE("matches column names case-insensitively")
{
    sql::MemoryStorage storage;
    sql::Table table{"todos", std::nullopt, {"title", "done"}, {{"Buy milk", "false"}}};

    storage.save_table(table);
    const auto loaded = storage.load_table({sql::RelationReference::Kind::Identifier, "todos"});

    CHECK_EQ(storage.column_index(loaded, "DONE"), 1U);
}

TEST_SUITE_END();

