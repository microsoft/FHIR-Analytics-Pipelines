#pragma once
#include <arrow/json/api.h>
#include <parquet/arrow/reader.h>

struct ParquetOptions
{
	bool UseThreads = true;

	int BlockSize = 1 << 20;
	
	arrow::json::UnexpectedFieldBehavior UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Ignore;

	int WriteBatchSize = 100;

	arrow::Compression::type Compression = arrow::Compression::SNAPPY;
};
