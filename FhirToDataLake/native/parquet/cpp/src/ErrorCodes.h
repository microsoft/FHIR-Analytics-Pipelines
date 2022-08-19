#pragma once
#include <string>

using namespace std;

enum ErrorCodes : int
{
    // Error parsing input json.
    ReadInputJsonError = 10001,
    // Error when converting to parquet.
    WriteToParquetError = 10002,
    // Error when parsing schema files.
    ParseParquetSchemaError = 11001,
    // Specfied schema file (for resource) not found.
    SchemaNotFound = 11002,
};