add_library(fsqllib STATIC
    src/ColumnMetadata.cpp
    src/ConsoleOutputWriter.cpp
    src/CsvStorage.cpp
    src/ExpressionValue.cpp
    src/ExpressionEvaluation.cpp
    src/Executor.cpp
    src/FileStorage.cpp
    src/FileStorageSupport.cpp
    src/JsonStorage.cpp
    src/MemoryStorage.cpp
    src/Parser.cpp
    src/ParserExpressions.cpp
    src/ParserStatements.cpp
    src/SelectAggregation.cpp
    src/SelectExecution.cpp
    src/SelectPlanning.cpp
    src/SqlSerialization.cpp
    src/StorageFormatUtils.cpp
    src/StorageRegistry.cpp
    src/TomlStorage.cpp
    src/Tokenizer.cpp
    src/XmlStorage.cpp
    src/YamlStorage.cpp
)

target_include_directories(fsqllib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET fsqllib PROPERTY CXX_STANDARD 20)
endif()
