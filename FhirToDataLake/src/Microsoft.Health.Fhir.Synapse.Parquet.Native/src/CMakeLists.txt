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
	  parquet
	  arrow
      jsoncpp_object
      jsoncpp_static
      JsonCpp::JsonCpp
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
	parquet
	arrow
        jsoncpp_object
        jsoncpp_static
        JsonCpp::JsonCpp
        )
