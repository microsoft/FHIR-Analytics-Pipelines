#pragma once
#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include "ArrowParquetOption.h"
#include "Status.h"

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
                        class ParquetConverter
                        {
                        public:
                            /// <summary>
                            /// Create a parquet instance.
                            /// </summary>
                            ParquetConverter();

                            /// <summary>
                            /// Create a parquet instance with specified ArrowParquetOption
                            /// </summary>
                            /// <param name="arrowParquetOption">The ArrowParquetOption.</param>
                            ParquetConverter(const ArrowParquetOption& arrowParquetOption);

                            /// <summary>
                            /// Convert in-memory stream to parquet buffer with specified schema.
                            /// </summary>
                            /// <param name="data">The input data.</param>
                            /// <param name="length">The length of input data.</param>
                            /// <param name="schema">The specified schema used for parsing input data to arrow table.</param>
                            /// <param name="outputBuffer">The converted result, which is a parquet buffer.</param>
                            /// <returns>Process status with detail message.</returns>
                            Status ConvertToParquetBuffer(const char* data, const int length, std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Buffer>& outputBuffer) const;

                        private:
                            Status WriteTableToBuffer(const std::shared_ptr<arrow::Table>& table, std::shared_ptr<arrow::Buffer>& outputBuffer) const;

                            ArrowParquetOption _arrowParquetOption;

                            // the size of one resource should be smaller than the size of two blocks,
                            // otherwise an exception will throw with message "Invalid: straddling object straddles two block boundaries (try to increase block size?)" 
                            arrow::json::ReadOptions _readOptions;
                        };
                    }
                }
            }
        }
    }
}