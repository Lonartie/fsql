add_library(sql_core STATIC
    src/ColumnMetadata.cpp
    src/ConsoleOutputWriter.cpp
    src/CsvStorage.cpp
    src/ExpressionValue.cpp
    src/ExpressionEvaluation.cpp
    src/Executor.cpp
    src/MemoryStorage.cpp
    src/Parser.cpp
    src/ParserExpressions.cpp
    src/ParserStatements.cpp
    src/SelectAggregation.cpp
    src/SelectExecution.cpp
    src/SelectPlanning.cpp
    src/SqlSerialization.cpp
    src/Tokenizer.cpp
)

target_include_directories(sql_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET sql_core PROPERTY CXX_STANDARD 20)
endif()
