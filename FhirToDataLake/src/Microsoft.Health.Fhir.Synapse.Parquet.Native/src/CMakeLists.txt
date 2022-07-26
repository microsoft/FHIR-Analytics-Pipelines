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
    JsonCpp::JsonCpp)

# Currently work only in windows platform. https://cmake.org/cmake/help/git-stage/manual/cmake-generator-expressions.7.html#genex:TARGET_RUNTIME_DLLS
add_custom_command(TARGET ${LIBRARY_NAME} POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_RUNTIME_DLLS:${LIBRARY_NAME}> $<TARGET_FILE_DIR:${LIBRARY_NAME}> 
  COMMAND_EXPAND_LISTS
)


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
    JsonCpp::JsonCpp)
