// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

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

    public ParquetException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    private static string GetParquetErrorMessage(int status)
    {
        switch (status)
        {
            case 10001:
                return "Input json is invalid.";
            case 10002:
                return "Write to parquet failed.";
            case 11001:
                return "Parse given schema failed.";
            case 11002:
                return "Target schema is not found.";
            default:
                return "Unknown error.";
        }
    }
}