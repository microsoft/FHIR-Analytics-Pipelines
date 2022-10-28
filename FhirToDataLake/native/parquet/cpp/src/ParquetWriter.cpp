#include "ParquetWriter.h"
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <fstream>

int WriteToParquet(const shared_ptr<arrow::Table> table, byte** outputData, int* outputSize, char* errorMessage, const shared_ptr<parquet::WriterProperties> writeProperties)
{
    const shared_ptr<arrow::io::BufferOutputStream> outputStream = parquet::CreateOutputStream();
    const auto status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outputStream, table->num_rows(), writeProperties);
    if (!status.ok())
    {
        string errorDetail = status.ToString();
        WriteErrorMessage(errorDetail, errorMessage);
        return WriteToParquetError;
    }

    shared_ptr<arrow::Buffer> outputBuffer = move(outputStream->Finish()).ValueOrDie();
    const byte* bytes = reinterpret_cast<const byte*>(outputBuffer->data());
    *outputSize = outputBuffer->size();
    *outputData = new byte[*outputSize];
    memcpy(*outputData, bytes, *outputSize);

    return 0;
}

ParquetWriter::ParquetWriter()
{
    _readOptions = arrow::json::ReadOptions::Defaults();
    _readOptions.block_size = ParquetOptions::BlockSize;
    _readOptions.use_threads = ParquetOptions::UseThreads;
    _unexpectedFieldBehavior = ParquetOptions::UnexpectedFieldBehavior;

    _writeProperties = parquet::WriterProperties::Builder()
        .write_batch_size(ParquetOptions::WriteBatchSize)->compression(ParquetOptions::Compression)->build();
}

ParquetWriter::ParquetWriter(const unordered_map<string, string>& schemaData)
{
    for (auto itr = schemaData.begin(); itr != schemaData.end(); itr ++)
    {
        _schemaManager.AddSchema(itr->first, itr->second);
    }
}

int ParquetWriter::RegisterSchema(const string& schemaKey, const string& schemaData)
{
    return _schemaManager.AddSchema(schemaKey, schemaData);
}

int ParquetWriter::Write(const string& resourceType, const char* inputJson, int inputLength, byte** outputData, int* outputLength, char* errorMessage)
{
    if (outputData == nullptr)
    {
        WriteErrorMessage("Output data pointer is null.", errorMessage);
        return WriteToParquetError;
    }

    if (outputLength == nullptr)
    {
        WriteErrorMessage("Output data size pointer is null.", errorMessage);
        return WriteToParquetError;
    }

    if (inputJson == nullptr)
    {
        WriteErrorMessage("Input Json data is null.", errorMessage);
        return ReadInputJsonError;
    }
    
    auto schema = _schemaManager.GetSchema(resourceType);
    if (schema == nullptr)
    {
        WriteErrorMessage("Schema not found for '" + resourceType + "'.", errorMessage);
        return SchemaNotFound;
    }

    arrow::json::ParseOptions parseOptions = arrow::json::ParseOptions::Defaults();
    parseOptions.explicit_schema = move(schema);
    parseOptions.unexpected_field_behavior = _unexpectedFieldBehavior;

    const auto bufferReader = make_shared<arrow::io::BufferReader>(reinterpret_cast<const uint8_t*>(inputJson), static_cast<int64_t>(inputLength));
    arrow::Result<shared_ptr<arrow::json::TableReader>> tableReaderResult = arrow::json::TableReader::Make(arrow::default_memory_pool(), bufferReader, _readOptions, parseOptions);
    
    if (!tableReaderResult.ok())
    {
        string errorDetail = tableReaderResult.status().ToString();
        WriteErrorMessage(errorDetail, errorMessage);
        return ReadInputJsonError;
    }
    const shared_ptr<arrow::json::TableReader> tableReader = tableReaderResult.ValueOrDie();
    arrow::Result<shared_ptr<arrow::Table>> tableResult = move(tableReader->Read());

    if (!tableResult.ok())
    {
        string errorDetail = tableResult.status().ToString();
        WriteErrorMessage(errorDetail, errorMessage);
        return ReadInputJsonError;
    }

    const shared_ptr<arrow::Table> table = tableResult.ValueOrDie();
    return WriteToParquet(table, outputData, outputLength, errorMessage, _writeProperties);
}

void WriteErrorMessage(const string& errorMessage, char* outputErrorMessage)
{
    if (outputErrorMessage != nullptr)
    {
        strncpy(outputErrorMessage, errorMessage.c_str(), 200);
    }
}
