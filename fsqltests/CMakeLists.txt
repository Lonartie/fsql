set(SQL_TEST_SOURCES
    test_main.cpp
    coroutine_stream_tests.cpp
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

add_executable(fsqltests
    ${SQL_TEST_SOURCES}
)

target_include_directories(fsqltests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/fsqllib/include
)

target_link_libraries(fsqltests PRIVATE fsqllib)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET fsqltests PROPERTY CXX_STANDARD 20)
endif()

add_test(NAME fsqltests COMMAND $<TARGET_FILE:fsqltests>)
