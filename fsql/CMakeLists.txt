# CMakeList.txt : CMake project for fsql, include source and define
#
# project specific logic here.
#

# Create a reusable application library
add_library(fsqlapplib STATIC
    src/CliHelp.cpp
    src/Application.cpp
    src/QueryInput.cpp
)

target_include_directories(fsqlapplib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/fsqllib/include
)
target_link_libraries(fsqlapplib PUBLIC fsqllib)

# Wrapper executable for the main fsql application
add_executable(fsqlapp main.cpp)
target_link_libraries(fsqlapp PRIVATE fsqlapplib)
set_target_properties(fsqlapp PROPERTIES OUTPUT_NAME fsql)

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
    target_link_libraries(${command_name}_wrapper PRIVATE fsqlapplib)
    set_target_properties(${command_name}_wrapper PROPERTIES OUTPUT_NAME ${command_name})
endforeach()

# Set C++ standard for targets
if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET fsqlapplib PROPERTY CXX_STANDARD 20)
  set_property(TARGET fsqlapp PROPERTY CXX_STANDARD 20)
  foreach(command_name IN LISTS SQL_WRAPPER_COMMANDS)
    set_property(TARGET ${command_name}_wrapper PROPERTY CXX_STANDARD 20)
  endforeach()
endif()

# TODO: Add tests and install targets if needed.
