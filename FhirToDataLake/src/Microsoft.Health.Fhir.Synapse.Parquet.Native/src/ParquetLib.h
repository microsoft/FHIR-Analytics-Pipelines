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

extern "C" int RegisterParquetSchema(char* schemaKey, char* schemaData);
extern "C" int ReleaseUnmanagedData(byte** data);
extern "C" int ConvertJsonToParquet(char* schemaKey, const char* inputJson, int inputLength, byte** outputData, int *outputLength);
