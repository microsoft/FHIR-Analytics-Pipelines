// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Exceptions
{
    public class DataLakeSearchException : SynapsePipelineExternalException
    {
        public DataLakeSearchException(string message)
            : base(message)
        {
        }

        public DataLakeSearchException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
