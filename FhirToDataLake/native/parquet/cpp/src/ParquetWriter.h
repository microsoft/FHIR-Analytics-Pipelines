#pragma once
#include <arrow/api.h>
#include <unordered_map>
#include <string>
#include "SchemaManager.h"
#include "ParquetOptions.h"
#include "ErrorCodes.h"

using namespace std;

typedef unsigned char byte;

void WriteToParquet(const shared_ptr<arrow::Table> table, byte** outputData, size_t outputSize);
void WriteErrorMessage(const string& errorMessage, char* outputErrorMessage);

class ParquetWriter
{
    private:
        SchemaManager _schemaManager;
        ParquetOptions _parquetOptions;
        arrow::json::ReadOptions _readOptions;

    public:
        ParquetWriter();
        ParquetWriter(const unordered_map<string, string>& schemaData);

        // Register schema for schemaKey, will overwrite if current key exists.
        int RegisterSchema(const string& schemaKey, const string& schemaData);

        // Write input json of resource type to parquet bytes, will try get schema from schema manager.
        int Write(const string& resourceType, const char* inputJson, int inSize, byte** outputData, int* outSize, char* errorMessage=nullptr);
};
