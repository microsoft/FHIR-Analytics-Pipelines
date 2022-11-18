// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Parquet
{
    public class ParquetConverterErrorCodes
    {
        public const int ReadInputJsonError = 10001;

        public const int WriteToParquetError = 10002;

        public const int ParseParquetSchemaError = 11001;

        public const int SchemaNotFound = 11002;
    }
}
