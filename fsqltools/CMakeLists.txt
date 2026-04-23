project(fsqltools)

set(SOURCES fsql_table_gen.cpp)

foreach (SOURCE ${SOURCES})
    get_filename_component(NAME ${SOURCE} NAME_WE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} fsqllib)
    target_include_directories(${NAME} PRIVATE ${CMAKE_SOURCE_DIR}/fsqllib)
endforeach()