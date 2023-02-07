﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions
{
    public class GenerateFhirParquetSchemaNodeException : FhirSchemaException
    {
        public GenerateFhirParquetSchemaNodeException(string message)
            : base(message)
        {
        }

        public GenerateFhirParquetSchemaNodeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
