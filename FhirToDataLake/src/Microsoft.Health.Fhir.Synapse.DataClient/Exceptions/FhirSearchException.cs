// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Exceptions
{
    public class FhirSearchException : SynapsePipelineRetriableException
    {
        public FhirSearchException(string message)
            : base(message)
        {
        }

        public FhirSearchException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
