#include "ParquetConverter.h"
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

using namespace Microsoft::Health::Fhir::Synapse::Parquet::Native;

ParquetConverter::ParquetConverter()
{
    _arrowParquetOption = ArrowParquetOption();
    _readOptions = arrow::json::ReadOptions{ _arrowParquetOption.UseThreads, _arrowParquetOption.BlockSize };
}

ParquetConverter::ParquetConverter(const ArrowParquetOption& arrowParquetOption)
{
    _arrowParquetOption = arrowParquetOption;
    _readOptions = arrow::json::ReadOptions{ _arrowParquetOption.UseThreads, _arrowParquetOption.BlockSize };
}

Status ParquetConverter::WriteTableToBuffer(const std::shared_ptr<arrow::Table>& table, std::shared_ptr<arrow::Buffer>& outputBuffer) const
{
    const std::shared_ptr<::arrow::io::BufferOutputStream> sink = parquet::CreateOutputStream();
    const std::shared_ptr<parquet::WriterProperties> writeProps = parquet::WriterProperties::Builder().
        write_batch_size(_arrowParquetOption.WriteBatchSize)->
        compression(_arrowParquetOption.Compression)->
        build();

    // TODO: adjust chunk size after performance test
    // parquet write rows by group, and chunk size is the rows write at one time
    const auto status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, table->num_rows(), writeProps);
    if (!status.ok())
    {
        return Status(StatusCode::TableToParquetBufferFailed, status.ToString());
    }

    outputBuffer = std::move(sink->Finish()).ValueOrDie();
    return Status(StatusCode::OK);
}

Status ParquetConverter::ConvertToParquetBuffer(const char* data, const int length, std::shared_ptr<arrow::Schema> explicitSchema, std::shared_ptr<arrow::Buffer>& outputBuffer) const
{
    if (explicitSchema == nullptr)
    {
        return Status(StatusCode::InvalidSchema, "The input schema should not be null.");
    }

    try
    {
        arrow::json::ParseOptions parseOptions = arrow::json::ParseOptions::Defaults();
        parseOptions.explicit_schema = std::move(explicitSchema);
        parseOptions.unexpected_field_behavior = _arrowParquetOption.UnexpectedFieldBehavior;

        const auto bufferReader = std::make_shared<arrow::io::BufferReader>(reinterpret_cast<const uint8_t*>(data), static_cast<int64_t>(length));
        arrow::Result<std::shared_ptr<arrow::json::TableReader>> tableReaderResult = arrow::json::TableReader::Make(
            arrow::default_memory_pool(), bufferReader, _readOptions, parseOptions);

        if (!tableReaderResult.ok())
        {
            return Status(StatusCode::CreateTableReaderFailed, tableReaderResult.status().ToString());
        }

        const std::shared_ptr<arrow::json::TableReader> tableReader = tableReaderResult.ValueOrDie();
        arrow::Result<std::shared_ptr<arrow::Table>> tableResult = std::move(tableReader->Read());

        if (!tableResult.ok())
        {
            return Status(StatusCode::ReadToTableFailed, tableResult.status().ToString());
        }

        const std::shared_ptr<arrow::Table> table = tableResult.ValueOrDie();

        return WriteTableToBuffer(table, outputBuffer);
    }
    catch (const std::exception& ex)
    {
        return Status(StatusCode::UnhandledException, ex.what());
    }
    catch (...)
    {
        return Status(StatusCode::UnknownError, "Unknown error occurs.");
    }
}

