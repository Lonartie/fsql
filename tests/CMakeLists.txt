add_executable(sql_tests
    sql_tests.cpp
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
