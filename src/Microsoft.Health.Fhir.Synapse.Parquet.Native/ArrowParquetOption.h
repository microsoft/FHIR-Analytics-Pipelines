#pragma once
#include <arrow/json/api.h>
#include <parquet/arrow/reader.h>

namespace Microsoft
{
    namespace Health
    {
        namespace Fhir
        {
            namespace Synapse
            {
                namespace Parquet
                {
                    namespace Native
                    {
                        // TODO: performance test to decide the option value (especially blockSize, WriteBatchSize)
                        /// <summary>
                        /// Option for arrow parquet
                        /// </summary>
                        struct ArrowParquetOption
                        {
                            /// <summary>
                            /// one of the arrow::json::ReadOptions,
                            /// whether to use the global CPU thread pool,
                            /// the default value is true.
                            /// </summary>
                            bool UseThreads = true;

                            /// <summary>
                            /// one of the arrow::json::ReadOptions,
                            /// block size that request from the IO layer;
                            /// also determines the size of chunks when use_threads is true,
                            /// the default value is 1MB (1 << 20).
                            /// </summary>
                            int BlockSize = 1 << 20;

                            /// <summary>
                            /// one of the arrow::json::ParseOptions,
                            /// How JSON fields outside of explicit_schema (if given) are treated,
                            /// there are three options: Ignore, Error, InferType,
                            /// the default value is Ignore
                            /// </summary>
                            arrow::json::UnexpectedFieldBehavior UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Ignore;

                            /// <summary>
                            /// one of the parquet::WriterProperties,
                            /// the write batch size,
                            /// the default value is 100
                            /// </summary>
                            int WriteBatchSize = 100;

                            /// <summary>
                            /// one of the parquet::WriterProperties,
                            /// the compression algorithm used to convert arrow table to parquet stream
                            /// there are two options, SNAPPY or UNCOMPRESSED,
                            /// the default value is SNAPPY
                            /// </summary>
                            arrow::Compression::type Compression = arrow::Compression::SNAPPY;
                        };
                    }
                }
            }
        }
    }
}