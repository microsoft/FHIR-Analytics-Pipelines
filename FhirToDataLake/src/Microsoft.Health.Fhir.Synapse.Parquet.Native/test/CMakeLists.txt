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

set(VCPKG_LIBRARY_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/../vcpkg_installed/${VCPKG_TARGET_TRIPLET}/${VCPKG_BUILD_TYPE}/lib
)

if ("${VCPKG_BUILD_TYPE}" STREQUAL "release")
    message("Build using release configuration.")
    set(VCPKG_LIBRARY_PATH
        ${CMAKE_CURRENT_BINARY_DIR}/../vcpkg_installed/${VCPKG_TARGET_TRIPLET}/lib
    )
endif()

message(${VCPKG_LIBRARY_PATH})
link_directories(${VCPKG_LIBRARY_PATH})

add_executable(${TEST_PROJECT_NAME} ${LIBRARY_TESTS_SOURCE})

set(ARROW_DEPENDANTS
    ${PARQUET_LIBRARIES}
    ${ARROW_LIBRARIES})

if ("${VCPKG_TARGET_TRIPLET}" STREQUAL "x64-linux")
    message("Build for Linux.")
    set(ARROW_DEPENDANTS
    ${PARQUET_LIBRARIES}
    ${ARROW_LIBRARIES}
    snappy
    re2
    utf8proc
    zstd
    bz2
    crypto
    lz4
    z
    thriftd
    thriftnbd
    thriftzd
    brotlidec-static
    brotlienc-static
    brotlicommon-static)
endif()

target_link_libraries(${TEST_PROJECT_NAME}
    PRIVATE
    ParquetNative_static
    ${ARROW_DEPENDANTS}
    gtest
    gtest_main
    JsonCpp::JsonCpp)

add_test(${TEST_PROJECT_NAME} ${TEST_PROJECT_NAME})
set_tests_properties(${TEST_PROJECT_NAME} PROPERTIES 
    ENVIRONMENT
    TESTDATADIR=${CMAKE_CURRENT_SOURCE_DIR}/data/)
