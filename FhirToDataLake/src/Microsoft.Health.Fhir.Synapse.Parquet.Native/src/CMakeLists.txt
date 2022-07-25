set(LIBRARY_NAME
    ParquetNative
)

add_library(${LIBRARY_NAME}
    SHARED
    SchemaManager.h
    SchemaManager.cpp
    ParquetOptions.h
    ParquetWriter.h
    ParquetWriter.cpp
    ParquetLib.h
    ParquetLib.cpp)
target_link_libraries(${LIBRARY_NAME}
    ${PARQUET_LIBRARIES}
    ${ARROW_LIBRARIES}
    jsoncpp_lib
    jsoncpp_object
    JsonCpp::JsonCpp)

add_library(${LIBRARY_NAME}_static
    SchemaManager.h
    SchemaManager.cpp
    ParquetOptions.h
    ParquetWriter.h
    ParquetWriter.cpp
    ParquetLib.h
    ParquetLib.cpp)

target_link_libraries(${LIBRARY_NAME}_static
    PRIVATE
    ${PARQUET_LIBRARIES}
    ${ARROW_LIBRARIES}
    jsoncpp_lib
    jsoncpp_object
    JsonCpp::JsonCpp)
