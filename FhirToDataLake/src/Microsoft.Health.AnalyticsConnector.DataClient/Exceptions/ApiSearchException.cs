// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Exceptions
{
    public class ApiSearchException : SynapsePipelineExternalException
    {
        public ApiSearchException(string message)
            : base(message)
        {
        }

        public ApiSearchException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
