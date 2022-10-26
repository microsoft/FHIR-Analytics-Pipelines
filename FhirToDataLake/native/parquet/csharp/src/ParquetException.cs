// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Parquet
{
    public class ParquetException : Exception
    {
        public ParquetException(string message)
            : base(message)
        {
        }

        public ParquetException(int status)
            : base(GetParquetErrorMessage(status))
        {
        }

        public ParquetException(int status, string message)
            : base(GetParquetErrorMessage(status) + " " + message)
        {
        }

        private static string GetParquetErrorMessage(int status)
        {
            switch (status)
            {
                case ParquetConverterErrorCodes.ReadInputJsonError:
                    return Resources.ReadInputJsonError;
                case ParquetConverterErrorCodes.WriteToParquetError:
                    return Resources.WriteToParquetError;
                case ParquetConverterErrorCodes.ParseParquetSchemaError:
                    return Resources.ParseParquetSchemaError;
                case ParquetConverterErrorCodes.SchemaNotFound:
                    return Resources.SchemaNotFound;
                default:
                    return "Unknown error.";
            }
        }
    }
}
