set(SQL_TEST_SOURCES
    test_main.cpp
    tokenizer_tokenize_tests.cpp
    parser_parse_statement_tests.cpp
    parser_parse_expression_tests.cpp
    memory_storage_load_table_tests.cpp
    memory_storage_column_index_tests.cpp
    csv_storage_escape_csv_tests.cpp
    csv_storage_parse_csv_line_tests.cpp
    csv_storage_load_table_tests.cpp
    executor_execute_crud_tests.cpp
    executor_execute_select_tests.cpp
    executor_execute_expression_tests.cpp
    executor_execute_defaults_tests.cpp
    executor_execute_complex_sql_tests.cpp
)

add_executable(sql_tests
    ${SQL_TEST_SOURCES}
)

target_include_directories(sql_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/core/include
)

target_link_libraries(sql_tests PRIVATE sql_core)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET sql_tests PROPERTY CXX_STANDARD 20)
endif()

add_test(NAME sql_tests COMMAND $<TARGET_FILE:sql_tests>)
