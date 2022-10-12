// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Exceptions
{
    public class SynapsePipelineRetriableException : Exception
    {
        public SynapsePipelineRetriableException(string message)
            : base(message)
        {
        }

        public SynapsePipelineRetriableException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
