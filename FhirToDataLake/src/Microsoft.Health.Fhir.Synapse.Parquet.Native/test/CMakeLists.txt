set(TEST_PROJECT_NAME
    ParquetTests
)
set(LIBRARY_TESTS_SOURCE
    SchemaManagerTests.cpp
    ParquetWriterTests.cpp
)

include_directories (${ArrowParquetNative_SOURCE_DIR}/src)

if (NOT (TARGET gtest AND TARGET gtest_main))
    message(FATAL_ERROR "gtest and gtest_main targets NOT found")
endif()

add_executable(${TEST_PROJECT_NAME} ${LIBRARY_TESTS_SOURCE})
target_link_libraries(${TEST_PROJECT_NAME}
    ParquetNative
    parquet
    arrow
    jsoncpp_object
    jsoncpp_static
    JsonCpp::JsonCpp
    gtest
    gtest_main
)

add_test(${TEST_PROJECT_NAME} ${TEST_PROJECT_NAME})
set_tests_properties(${TEST_PROJECT_NAME} PROPERTIES 
    ENVIRONMENT
    TESTDATADIR=${CMAKE_CURRENT_SOURCE_DIR}/data/)
