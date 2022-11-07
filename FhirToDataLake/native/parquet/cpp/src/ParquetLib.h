#if defined(_MSC_VER)
    //  Microsoft
    #define EXPORT __declspec(dllexport)
    #define IMPORT __declspec(dllimport)
#elif defined(__GNUC__)
    //  GCC
    #define EXPORT __attribute__((visibility("default")))
    #define IMPORT
#else
    #define EXPORT
    #define IMPORT
    #pragma warning Unknown dynamic link import/export semantics.
#endif

#include <iostream>
#include <string>
#include "ParquetWriter.h"

using namespace std;

// Create a parquet writer.
extern "C" EXPORT ParquetWriter* CreateParquetWriter();
// Detroy the parquet writer and release memory.
extern "C" EXPORT void DestroyParquetWriter(ParquetWriter* writer);

// Register json schema.
extern "C" EXPORT int RegisterParquetSchema(ParquetWriter* writer, const char* schemaKey, const char* schemaData);
// Convert input json to parquet bytes.
extern "C" EXPORT int ConvertJsonToParquet(ParquetWriter* writer, const char* schemaKey, const char* inputJson, int inputLength, byte** outputData, int* outputLength, char* errorMessage);
// Release memory of parquet bytes.
extern "C" EXPORT int TryReleaseUnmanagedData(byte** data);