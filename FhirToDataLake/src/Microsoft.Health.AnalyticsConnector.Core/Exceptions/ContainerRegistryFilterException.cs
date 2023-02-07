// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.Core.Exceptions
{
    public class ContainerRegistryFilterException : SynapsePipelineExternalException
    {
        public ContainerRegistryFilterException(string message)
            : base(message)
        {
        }

        public ContainerRegistryFilterException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
