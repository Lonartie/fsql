add_library(sql_core STATIC
    src/ConsoleOutputWriter.cpp
    src/CsvStorage.cpp
    src/Executor.cpp
    src/MemoryStorage.cpp
    src/Parser.cpp
    src/Tokenizer.cpp
)

target_include_directories(sql_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET sql_core PROPERTY CXX_STANDARD 20)
endif()
