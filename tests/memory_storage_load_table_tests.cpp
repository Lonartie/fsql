#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("MemoryStorage::load_table");

TEST_CASE("stores and loads tables")
{
    sql::MemoryStorage storage;
    sql::Table table{"todos", std::nullopt, {"title", "done"}, {{"Buy milk", "false"}}};

    storage.save_table(table);
    const auto loaded = storage.load_table({sql::RelationReference::Kind::Identifier, "todos"});

    CHECK_EQ(loaded.name, "todos");
    REQUIRE_EQ(loaded.columns.size(), 2U);
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "Buy milk");
}

TEST_CASE("rejects missing tables")
{
    sql::MemoryStorage storage;
    CHECK_THROWS_AS(storage.load_table({sql::RelationReference::Kind::Identifier, "missing"}), std::runtime_error);
}

TEST_SUITE_END();

