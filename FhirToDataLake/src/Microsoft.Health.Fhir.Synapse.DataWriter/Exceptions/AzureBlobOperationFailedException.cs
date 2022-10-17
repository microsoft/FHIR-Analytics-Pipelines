// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions
{
    public class AzureBlobOperationFailedException : SynapsePipelineRetriableException
    {
        public AzureBlobOperationFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
