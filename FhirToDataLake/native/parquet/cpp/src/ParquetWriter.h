#pragma once
#include <arrow/api.h>
#include <unordered_map>
#include <string>
#include "SchemaManager.h"
#include "ParquetOptions.h"
#include "ErrorCodes.h"

using namespace std;

typedef unsigned char byte;

int WriteToParquet(const shared_ptr<arrow::Table> table, byte** outputData, int* outputSize, char* errorMessage, const shared_ptr<parquet::WriterProperties> writeProperties);
void WriteErrorMessage(const string& errorMessage, char* outputErrorMessage);

class ParquetWriter
{
    private:
        SchemaManager _schemaManager;
        arrow::json::ReadOptions _readOptions;
        arrow::json::UnexpectedFieldBehavior _unexpectedFieldBehavior;
        shared_ptr<parquet::WriterProperties> _writeProperties;

    public:
        ParquetWriter();
        ParquetWriter(const unordered_map<string, string>& schemaData);

        // Register schema for schemaKey, will overwrite if current key exists.
        int RegisterSchema(const string& schemaKey, const string& schemaData);

        // Write input json of resource type to parquet bytes, will try get schema from schema manager.
        int Write(const string& resourceType, const char* inputJson, int inSize, byte** outputData, int* outSize, char* errorMessage=nullptr);
};
