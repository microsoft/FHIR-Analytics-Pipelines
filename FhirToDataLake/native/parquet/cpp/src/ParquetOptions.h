#pragma once
#include <arrow/json/api.h>
#include <parquet/arrow/reader.h>

namespace ParquetOptions
{
    const bool UseThreads = true;

    const int BlockSize = 1 << 30;
    
    const arrow::json::UnexpectedFieldBehavior UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Ignore;

    const int WriteBatchSize = 100;

    const arrow::Compression::type Compression = arrow::Compression::SNAPPY;
};
