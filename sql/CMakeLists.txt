# CMakeList.txt : CMake project for sql, include source and define
#
# project specific logic here.
#

# Create a reusable application library
add_library(sql_app_lib STATIC
    src/Application.cpp
)

target_include_directories(sql_app_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/core/include
)
target_link_libraries(sql_app_lib PUBLIC sql_core)

# Wrapper executable for the main sql application
add_executable(sql_app main.cpp)
target_link_libraries(sql_app PRIVATE sql_app_lib)
set_target_properties(sql_app PROPERTIES OUTPUT_NAME sql)

# List of SQL commands for which wrappers will be created
set(SQL_WRAPPER_COMMANDS
    select
    insert
    update
    delete
    create
    drop
)

# Create wrappers for each SQL command
foreach(command_name IN LISTS SQL_WRAPPER_COMMANDS)
    add_executable(${command_name}_wrapper main.cpp)
    target_link_libraries(${command_name}_wrapper PRIVATE sql_app_lib)
    set_target_properties(${command_name}_wrapper PROPERTIES OUTPUT_NAME ${command_name})
endforeach()

# Set C++ standard for targets
if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET sql_app_lib PROPERTY CXX_STANDARD 20)
  set_property(TARGET sql_app PROPERTY CXX_STANDARD 20)
  foreach(command_name IN LISTS SQL_WRAPPER_COMMANDS)
    set_property(TARGET ${command_name}_wrapper PROPERTY CXX_STANDARD 20)
  endforeach()
endif()

# TODO: Add tests and install targets if needed.
